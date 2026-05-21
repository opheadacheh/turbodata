package turbodata

import (
	"bytes"
	"errors"
	"io"
	"math"
	"sync/atomic"
	"testing"
	"time"
)

// slowReadAtSource wraps a ReadSource and sleeps for slow on each ReadAt so
// the parallelism test can actually observe concurrent in-flight reads.
type slowReadAtSource struct {
	rs   ReadSource
	slow time.Duration
}

func (s *slowReadAtSource) Read(p []byte) (int, error)            { return s.rs.Read(p) }
func (s *slowReadAtSource) Seek(o int64, w int) (int64, error)    { return s.rs.Seek(o, w) }
func (s *slowReadAtSource) ReadAt(p []byte, off int64) (int, error) {
	time.Sleep(s.slow)
	return s.rs.ReadAt(p, off)
}

// trackingSource counts every Read/Seek/ReadAt and the bytes they read.
// All counters are atomic because ReadAt is called concurrently by Fetcher.
type trackingSource struct {
	rs              ReadSource
	readBytes       atomic.Int64
	readCalls       atomic.Int64
	seekCalls       atomic.Int64
	readAtBytes     atomic.Int64
	readAtCalls     atomic.Int64
	readAtInFlight  atomic.Int64
	readAtMaxFlight atomic.Int64
}

func newTrackingSource(rs ReadSource) *trackingSource {
	return &trackingSource{rs: rs}
}

func (t *trackingSource) Read(p []byte) (int, error) {
	n, err := t.rs.Read(p)
	t.readBytes.Add(int64(n))
	t.readCalls.Add(1)
	return n, err
}

func (t *trackingSource) Seek(off int64, w int) (int64, error) {
	t.seekCalls.Add(1)
	return t.rs.Seek(off, w)
}

func (t *trackingSource) ReadAt(p []byte, off int64) (int, error) {
	cur := t.readAtInFlight.Add(1)
	defer t.readAtInFlight.Add(-1)
	for {
		prev := t.readAtMaxFlight.Load()
		if cur <= prev || t.readAtMaxFlight.CompareAndSwap(prev, cur) {
			break
		}
	}
	n, err := t.rs.ReadAt(p, off)
	t.readAtBytes.Add(int64(n))
	t.readAtCalls.Add(1)
	return n, err
}

func roundTripWithTracking(t *testing.T, setup func(*Writer)) (*bytes.Reader, []byte) {
	t.Helper()
	buf := &bytes.Buffer{}
	w := NewWriter(buf)
	setup(w)
	data := buf.Bytes()
	return bytes.NewReader(data), data
}

func collectFromReader(t *testing.T, r *Reader, opts ...ReadOption) []testMsg {
	t.Helper()
	it, err := r.ReadMessages(opts...)
	if err != nil {
		t.Fatalf("ReadMessages: %v", err)
	}
	rb := NewReusableBuffer()
	var out []testMsg
	for {
		ts, name, err := it.NextInto(rb)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("NextInto: %v", err)
		}
		d := make([]byte, len(rb.Data))
		copy(d, rb.Data)
		out = append(out, testMsg{ts, name, d})
	}
	return out
}

// TestDefaultPathRegressionGuard verifies that without WithReadStrategy, the
// reader still uses Seek+Read exclusively and doesn't accidentally touch
// ReadAt (which would indicate the cost-aware path leaked into the default).
func TestDefaultPathRegressionGuard(t *testing.T) {
	bodyReader, _ := roundTripWithTracking(t, func(w *Writer) {
		mustOpenTopics(t, w, []string{"cam"}, []map[string]any{{}})
		mustWriteMessage(t, w, "cam", []byte("frame0"), 10)
		mustWriteMessage(t, w, "cam", []byte("frame1"), 20)
		mustWriteMessage(t, w, "cam", []byte("frame2"), 30)
		mustCloseTopic(t, w)
		mustClose(t, w)
	})
	tracking := newTrackingSource(bodyReader)
	r, err := NewReader(tracking)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	out := collectFromReader(t, r)
	if len(out) != 3 {
		t.Fatalf("len=%d", len(out))
	}
	if tracking.readAtCalls.Load() != 0 {
		t.Errorf("default path used ReadAt %d times; expected 0", tracking.readAtCalls.Load())
	}
}

// TestCostAwareSemanticEquivalence compares the default path and the
// cost-aware path on a non-trivial file and asserts the message sequences are
// byte-identical. Both compressed and uncompressed groups; multiple chunks.
func TestCostAwareSemanticEquivalence(t *testing.T) {
	build := func(w *Writer) {
		mustOpenTopics(t, w, []string{"img"}, []map[string]any{{}}, WithCompression(), WithChunkConfig(&ChunkConfig{Mode: ChunkThresholdModeCount, Count: 2}))
		mustWriteMessage(t, w, "img", []byte("img-frame-0"), 10)
		mustWriteMessage(t, w, "img", []byte("img-frame-1"), 20)
		mustWriteMessage(t, w, "img", []byte("img-frame-2"), 30)
		mustWriteMessage(t, w, "img", []byte("img-frame-3"), 40)
		mustWriteMessage(t, w, "img", []byte("img-frame-4"), 50)
		mustCloseTopic(t, w)
		mustOpenTopics(t, w, []string{"imu"}, []map[string]any{{}}, WithChunkConfig(&ChunkConfig{Mode: ChunkThresholdModeCount, Count: 3}))
		mustWriteMessage(t, w, "imu", []byte("imu-sample-15"), 15)
		mustWriteMessage(t, w, "imu", []byte("imu-sample-25"), 25)
		mustWriteMessage(t, w, "imu", []byte("imu-sample-35"), 35)
		mustWriteMessage(t, w, "imu", []byte("imu-sample-45"), 45)
		mustCloseTopic(t, w)
		mustClose(t, w)
	}

	rsDefault, raw := roundTripWithTracking(t, build)
	r1, err := NewReader(rsDefault)
	if err != nil {
		t.Fatalf("NewReader default: %v", err)
	}
	want := collectFromReader(t, r1)

	r2, err := NewReader(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("NewReader cost-aware: %v", err)
	}
	strat := ReadStrategy{CoalesceGap: 1 << 20, SplitThreshold: math.MaxInt64, MaxConcurrency: 4}
	got := collectFromReader(t, r2, WithReadStrategy(strat))

	if len(got) != len(want) {
		t.Fatalf("count mismatch: cost-aware=%d default=%d", len(got), len(want))
	}
	for i := range want {
		if got[i].ts != want[i].ts || got[i].name != want[i].name || string(got[i].data) != string(want[i].data) {
			t.Errorf("[%d] want %+v got %+v", i, want[i], got[i])
		}
	}
}

// TestCostAwareSemanticEquivalenceReverse runs the same comparison but with
// reverse-time order to verify chunk + message reverse iteration on both paths.
func TestCostAwareSemanticEquivalenceReverse(t *testing.T) {
	build := func(w *Writer) {
		mustOpenTopics(t, w, []string{"x"}, []map[string]any{{}}, WithChunkConfig(&ChunkConfig{Mode: ChunkThresholdModeCount, Count: 2}))
		for i := int64(0); i < 6; i++ {
			mustWriteMessage(t, w, "x", []byte{byte('a' + i)}, (i+1)*10)
		}
		mustCloseTopic(t, w)
		mustClose(t, w)
	}
	rsDefault, raw := roundTripWithTracking(t, build)
	r1, err := NewReader(rsDefault)
	if err != nil {
		t.Fatalf("NewReader default: %v", err)
	}
	want := collectFromReader(t, r1, WithOrder(ReverseTimeOrder))

	r2, err := NewReader(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("NewReader cost-aware: %v", err)
	}
	got := collectFromReader(t, r2, WithOrder(ReverseTimeOrder), WithReadStrategy(ReadStrategy{CoalesceGap: 1 << 20, SplitThreshold: math.MaxInt64, MaxConcurrency: 2}))

	if len(got) != len(want) {
		t.Fatalf("count mismatch: cost-aware=%d default=%d", len(got), len(want))
	}
	for i := range want {
		if got[i].ts != want[i].ts || string(got[i].data) != string(want[i].data) {
			t.Errorf("[%d] want %+v got %+v", i, want[i], got[i])
		}
	}
}

// TestSelectiveUncompressedBytesSavings writes an uncompressed multi-topic
// chunk, then reads with a topic filter that drops most messages. The
// cost-aware path's message-level granularity must read strictly fewer bytes
// than the default path (which loads the whole chunk).
func TestSelectiveUncompressedBytesSavings(t *testing.T) {
	build := func(w *Writer) {
		// Single uncompressed group with two topics in the same chunk; each
		// message is fat enough that fetching only some of them saves bytes.
		mustOpenTopics(t, w, []string{"kept", "dropped"}, []map[string]any{{}, {}})
		big := bytes.Repeat([]byte("X"), 1024)
		small := bytes.Repeat([]byte("k"), 16)
		// Interleave: dropped's payload dominates the chunk's bytes.
		mustWriteMessage(t, w, "kept", small, 10)
		mustWriteMessage(t, w, "dropped", big, 20)
		mustWriteMessage(t, w, "kept", small, 30)
		mustWriteMessage(t, w, "dropped", big, 40)
		mustWriteMessage(t, w, "kept", small, 50)
		mustWriteMessage(t, w, "dropped", big, 60)
		mustCloseTopic(t, w)
		mustClose(t, w)
	}

	rsDefault, raw := roundTripWithTracking(t, build)

	trkDefault := newTrackingSource(rsDefault)
	rDef, err := NewReader(trkDefault)
	if err != nil {
		t.Fatalf("NewReader default: %v", err)
	}
	defaultOut := collectFromReader(t, rDef, WithTopicNames([]string{"kept"}))

	trkCost := newTrackingSource(bytes.NewReader(raw))
	rCost, err := NewReader(trkCost)
	if err != nil {
		t.Fatalf("NewReader cost-aware: %v", err)
	}
	strat := ReadStrategy{CoalesceGap: 0, SplitThreshold: math.MaxInt64, MaxConcurrency: 4}
	costOut := collectFromReader(t, rCost, WithTopicNames([]string{"kept"}), WithReadStrategy(strat))

	if len(defaultOut) != 3 || len(costOut) != 3 {
		t.Fatalf("expected 3 kept messages on both paths; default=%d cost=%d", len(defaultOut), len(costOut))
	}
	for i := range defaultOut {
		if string(defaultOut[i].data) != string(costOut[i].data) {
			t.Errorf("[%d] payload mismatch: default=%q cost=%q", i, defaultOut[i].data, costOut[i].data)
		}
	}

	// The default path streams the full chunk via Read; the cost-aware path
	// fetches only the kept messages via ReadAt. Compare bytes pulled for
	// data (Read on default vs. ReadAt on cost-aware). The default path's
	// Read also includes the index chunk, so we compare the cost-aware
	// ReadAt total (which includes both index+data ranges) against the
	// default path's Read total (which is the dominant byte source).
	if trkCost.readAtBytes.Load() >= trkDefault.readBytes.Load() {
		t.Errorf("expected cost-aware to read fewer bytes than default; cost-aware ReadAt=%d default Read=%d",
			trkCost.readAtBytes.Load(), trkDefault.readBytes.Load())
	}
}

// TestCostAwareParallelism verifies that with high concurrency, the fetcher
// actually issues simultaneous ReadAts.
func TestCostAwareParallelism(t *testing.T) {
	build := func(w *Writer) {
		// Many small chunks so Phase B has many ops to parallelize.
		mustOpenTopics(t, w, []string{"t"}, []map[string]any{{}}, WithChunkConfig(&ChunkConfig{Mode: ChunkThresholdModeCount, Count: 1}))
		for i := int64(0); i < 32; i++ {
			mustWriteMessage(t, w, "t", []byte{byte(i)}, (i+1)*10)
		}
		mustCloseTopic(t, w)
		mustClose(t, w)
	}
	_, raw := roundTripWithTracking(t, build)
	// In-memory ReadAts complete in nanoseconds, so workers don't overlap
	// observably without a small per-call delay. Slow each ReadAt by 5ms so
	// the parallelism is visible.
	slow := &slowReadAtSource{rs: bytes.NewReader(raw), slow: 5 * time.Millisecond}
	tracking := newTrackingSource(slow)
	r, err := NewReader(tracking)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	// CoalesceGap=0 keeps each chunk in its own op; concurrency=8 lets the
	// fetcher actually fan out.
	strat := ReadStrategy{CoalesceGap: 0, SplitThreshold: math.MaxInt64, MaxConcurrency: 8}
	out := collectFromReader(t, r, WithReadStrategy(strat))
	if len(out) != 32 {
		t.Fatalf("len=%d", len(out))
	}
	if tracking.readAtMaxFlight.Load() < 2 {
		t.Errorf("expected concurrent ReadAt observed; max in-flight = %d", tracking.readAtMaxFlight.Load())
	}
}

// TestCostAwareWithTopicAndTimeFilters verifies filters compose with the
// cost-aware path the same way they do with the default path.
func TestCostAwareWithTopicAndTimeFilters(t *testing.T) {
	build := func(w *Writer) {
		mustOpenTopics(t, w, []string{"a"}, []map[string]any{{}}, WithChunkConfig(&ChunkConfig{Mode: ChunkThresholdModeCount, Count: 1}))
		for ts := int64(10); ts <= 50; ts += 10 {
			mustWriteMessage(t, w, "a", []byte{byte(ts)}, ts)
		}
		mustCloseTopic(t, w)
		mustOpenTopics(t, w, []string{"b"}, []map[string]any{{}})
		for ts := int64(15); ts <= 45; ts += 10 {
			mustWriteMessage(t, w, "b", []byte{byte(ts)}, ts)
		}
		mustCloseTopic(t, w)
		mustClose(t, w)
	}
	_, raw := roundTripWithTracking(t, build)

	r1, _ := NewReader(bytes.NewReader(raw))
	wantOpts := []ReadOption{WithTopicNames([]string{"a"}), WithStartTimestamp(20), WithEndTimestamp(40)}
	want := collectFromReader(t, r1, wantOpts...)

	r2, _ := NewReader(bytes.NewReader(raw))
	got := collectFromReader(t, r2, append(wantOpts, WithReadStrategy(ReadStrategy{CoalesceGap: 1 << 20, SplitThreshold: math.MaxInt64, MaxConcurrency: 4}))...)

	if len(want) != len(got) || len(want) != 3 {
		t.Fatalf("counts mismatch want=%d got=%d", len(want), len(got))
	}
	for i := range want {
		if want[i].ts != got[i].ts || want[i].name != got[i].name || string(want[i].data) != string(got[i].data) {
			t.Errorf("[%d] want %+v got %+v", i, want[i], got[i])
		}
	}
}
