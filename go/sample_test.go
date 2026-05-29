package turbodata

import (
	"bytes"
	"math"
	"strings"
	"testing"
	"time"
)

// buildSampleReader is a tiny adapter that builds a file, wraps it in a
// trackingSource, and returns the Reader plus the tracker so individual tests
// can assert ReadAt counts.
func buildSampleReader(t *testing.T, setup func(*Writer)) (*Reader, *trackingSource) {
	t.Helper()
	raw := buildFile(t, setup)
	tracking := newTrackingSource(bytes.NewReader(raw))
	r, err := NewReader(tracking)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	return r, tracking
}

// TestSampleFloorOnSingleTopic exercises the four core floor cases (exact
// hit, between-message floor, T at the first message, T at the last message)
// in one strictly-increasing Timestamps slice.
func TestSampleFloorOnSingleTopic(t *testing.T) {
	r, _ := buildSampleReader(t, func(w *Writer) {
		mustOpenTopics(t, w, []string{"cam"}, []map[string]any{{}})
		mustWriteMessage(t, w, "cam", []byte("f10"), 10)
		mustWriteMessage(t, w, "cam", []byte("f20"), 20)
		mustWriteMessage(t, w, "cam", []byte("f30"), 30)
		mustWriteMessage(t, w, "cam", []byte("f40"), 40)
		mustWriteMessage(t, w, "cam", []byte("f50"), 50)
		mustCloseTopic(t, w)
		mustClose(t, w)
	})

	out, err := r.Sample([]SampleQuery{
		{Topic: "cam", Timestamps: []int64{10, 15, 20, 50}},
	})
	if err != nil {
		t.Fatalf("Sample: %v", err)
	}
	want := []SampleResult{
		{Found: true, Timestamp: 10, Data: []byte("f10")},
		{Found: true, Timestamp: 10, Data: []byte("f10")},
		{Found: true, Timestamp: 20, Data: []byte("f20")},
		{Found: true, Timestamp: 50, Data: []byte("f50")},
	}
	assertResultsEqual(t, out[0], want)
}

func TestSampleAfterLastMessage(t *testing.T) {
	r, _ := buildSampleReader(t, func(w *Writer) {
		mustOpenTopics(t, w, []string{"x"}, []map[string]any{{}})
		mustWriteMessage(t, w, "x", []byte("a"), 10)
		mustWriteMessage(t, w, "x", []byte("b"), 20)
		mustWriteMessage(t, w, "x", []byte("c"), 30)
		mustCloseTopic(t, w)
		mustClose(t, w)
	})
	out, err := r.Sample([]SampleQuery{{Topic: "x", Timestamps: []int64{100}}})
	if err != nil {
		t.Fatalf("Sample: %v", err)
	}
	got := out[0][0]
	if !got.Found || got.Timestamp != 30 || string(got.Data) != "c" {
		t.Errorf("T past last message want Found,30,c got Found=%v ts=%d data=%q", got.Found, got.Timestamp, got.Data)
	}
}

func TestSampleBeforeFirstMessage(t *testing.T) {
	r, _ := buildSampleReader(t, func(w *Writer) {
		mustOpenTopics(t, w, []string{"x"}, []map[string]any{{}})
		mustWriteMessage(t, w, "x", []byte("a"), 10)
		mustWriteMessage(t, w, "x", []byte("b"), 20)
		mustCloseTopic(t, w)
		mustClose(t, w)
	})
	out, err := r.Sample([]SampleQuery{{Topic: "x", Timestamps: []int64{5}}})
	if err != nil {
		t.Fatalf("Sample: %v", err)
	}
	if out[0][0].Found {
		t.Errorf("expected Found=false for T < first message; got %+v", out[0][0])
	}
}

// TestSampleSameChunkBatching writes five messages into a single compressed
// chunk and samples four timestamps that all land in it. The engine should
// deduplicate to one chunk per phase, so the data-and-index I/O after summary
// load is bounded: at most one ReadAt per phase (Phase A index chunk + Phase
// B data chunk) for two ReadAts total. This guards against accidental
// per-timestamp fetching.
func TestSampleSameChunkBatching(t *testing.T) {
	r, tracking := buildSampleReader(t, func(w *Writer) {
		mustOpenTopics(t, w, []string{"cam"}, []map[string]any{{}}, WithCompression())
		mustWriteMessage(t, w, "cam", []byte("f10"), 10)
		mustWriteMessage(t, w, "cam", []byte("f20"), 20)
		mustWriteMessage(t, w, "cam", []byte("f30"), 30)
		mustWriteMessage(t, w, "cam", []byte("f40"), 40)
		mustWriteMessage(t, w, "cam", []byte("f50"), 50)
		mustCloseTopic(t, w)
		mustClose(t, w)
	})
	if _, err := r.Summary(); err != nil {
		t.Fatalf("Summary: %v", err)
	}
	before := tracking.readAtCalls.Load()

	out, err := r.Sample([]SampleQuery{
		{Topic: "cam", Timestamps: []int64{15, 25, 35, 45}},
	})
	if err != nil {
		t.Fatalf("Sample: %v", err)
	}
	want := []SampleResult{
		{Found: true, Timestamp: 10, Data: []byte("f10")},
		{Found: true, Timestamp: 20, Data: []byte("f20")},
		{Found: true, Timestamp: 30, Data: []byte("f30")},
		{Found: true, Timestamp: 40, Data: []byte("f40")},
	}
	assertResultsEqual(t, out[0], want)

	diff := tracking.readAtCalls.Load() - before
	// One Phase A op (the single index chunk), one Phase B op (the single
	// data chunk). Coalescing might merge them when they are < 1 MiB apart,
	// but Plan runs separately per phase, so the minimum is 2 and we allow a
	// small slack for any extra defensive read.
	if diff > 3 {
		t.Errorf("expected <= 3 ReadAt calls for 4 same-chunk samples, got %d", diff)
	}
}

// TestSampleDuplicateFloorDedupesReads samples several timestamps that all
// floor to the SAME uncompressed message. The duplicate floors must collapse
// to a single data read: identical ranges have a negative gap and never
// coalesce, so without the per-message dedup in runPhaseB the fetcher would
// issue one ReadAt per duplicate. Asserting on the read count (not just the
// results) is what proves the dedup is actually in effect.
func TestSampleDuplicateFloorDedupesReads(t *testing.T) {
	const dupCount = 5
	r, tracking := buildSampleReader(t, func(w *Writer) {
		mustOpenTopics(t, w, []string{"cam"}, []map[string]any{{}})
		mustWriteMessage(t, w, "cam", []byte("f100"), 100)
		mustWriteMessage(t, w, "cam", []byte("f200"), 200)
		mustWriteMessage(t, w, "cam", []byte("f300"), 300)
		mustCloseTopic(t, w)
		mustClose(t, w)
	})
	if _, err := r.Summary(); err != nil {
		t.Fatalf("Summary: %v", err)
	}
	before := tracking.readAtCalls.Load()

	// 101..105 all floor to the single message at ts=100.
	out, err := r.Sample([]SampleQuery{
		{Topic: "cam", Timestamps: []int64{101, 102, 103, 104, 105}},
	})
	if err != nil {
		t.Fatalf("Sample: %v", err)
	}
	want := make([]SampleResult, dupCount)
	for i := range want {
		want[i] = SampleResult{Found: true, Timestamp: 100, Data: []byte("f100")}
	}
	assertResultsEqual(t, out[0], want)

	diff := tracking.readAtCalls.Load() - before
	// Phase A reads the single index chunk (1 op); Phase B reads the single
	// floored message (1 op after dedup). Without dedup, Phase B would issue
	// dupCount separate reads of identical bytes -> 1 + dupCount total.
	if diff != 2 {
		t.Errorf("expected 2 ReadAt calls (1 index + 1 deduped data); got %d (no-dedup would be %d)", diff, 1+dupCount)
	}
}

// TestSampleMixedCompressedUncompressed exercises cross-group sampling with
// one compressed and one uncompressed group, asserting both Phase B
// granularities work end-to-end.
func TestSampleMixedCompressedUncompressed(t *testing.T) {
	r, _ := buildSampleReader(t, func(w *Writer) {
		mustOpenTopics(t, w, []string{"img"}, []map[string]any{{}}, WithCompression())
		mustWriteMessage(t, w, "img", []byte("img10"), 10)
		mustWriteMessage(t, w, "img", []byte("img20"), 20)
		mustWriteMessage(t, w, "img", []byte("img30"), 30)
		mustCloseTopic(t, w)
		mustOpenTopics(t, w, []string{"imu"}, []map[string]any{{}})
		mustWriteMessage(t, w, "imu", []byte("imu15"), 15)
		mustWriteMessage(t, w, "imu", []byte("imu25"), 25)
		mustWriteMessage(t, w, "imu", []byte("imu35"), 35)
		mustCloseTopic(t, w)
		mustClose(t, w)
	})

	out, err := r.Sample([]SampleQuery{
		{Topic: "img", Timestamps: []int64{12, 22, 32}},
		{Topic: "imu", Timestamps: []int64{20, 30}},
	})
	if err != nil {
		t.Fatalf("Sample: %v", err)
	}
	assertResultsEqual(t, out[0], []SampleResult{
		{Found: true, Timestamp: 10, Data: []byte("img10")},
		{Found: true, Timestamp: 20, Data: []byte("img20")},
		{Found: true, Timestamp: 30, Data: []byte("img30")},
	})
	assertResultsEqual(t, out[1], []SampleResult{
		{Found: true, Timestamp: 15, Data: []byte("imu15")},
		{Found: true, Timestamp: 25, Data: []byte("imu25")},
	})
}

// TestSampleFallbackToPreviousChunk forces the rare case where the
// timestamp's natural candidate chunk has no message of the requested topic
// (because two topics in the same group write into alternating chunks). The
// engine should fall back to chunk c-1 in a second Phase A wave and still
// return the correct floor.
func TestSampleFallbackToPreviousChunk(t *testing.T) {
	r, _ := buildSampleReader(t, func(w *Writer) {
		// Count=1 chunk config means each WriteMessage produces a chunk
		// containing only that one message. Interleaving topics "a" and "b"
		// therefore produces chunks: [a@5], [b@10], [a@30], [b@40].
		cfg := &ChunkConfig{Mode: ChunkThresholdModeCount, Count: 1}
		mustOpenTopics(t, w, []string{"a", "b"}, []map[string]any{{}, {}}, WithChunkConfig(cfg))
		mustWriteMessage(t, w, "a", []byte("a5"), 5)
		mustWriteMessage(t, w, "b", []byte("b10"), 10)
		mustWriteMessage(t, w, "a", []byte("a30"), 30)
		mustWriteMessage(t, w, "b", []byte("b40"), 40)
		mustCloseTopic(t, w)
		mustClose(t, w)
	})

	// T=15 lands in chunk index 1 (StartTs=10, contains only b@10). Topic
	// "a" has no message <= 15 in that chunk, so the engine must fall back
	// to chunk index 0 ([a@5]) to find the floor.
	out, err := r.Sample([]SampleQuery{{Topic: "a", Timestamps: []int64{15}}})
	if err != nil {
		t.Fatalf("Sample: %v", err)
	}
	got := out[0][0]
	if !got.Found || got.Timestamp != 5 || string(got.Data) != "a5" {
		t.Errorf("fallback case: want Found,5,a5 got Found=%v ts=%d data=%q", got.Found, got.Timestamp, got.Data)
	}

	// And a T that falls back past chunk 0 (T=1 < first chunk's StartTs=5
	// for topic "a" - actually first chunk for the GROUP is at 5, so T=3
	// has no candidate at all -> Found=false straight from initial assignment).
	out2, err := r.Sample([]SampleQuery{{Topic: "a", Timestamps: []int64{3}}})
	if err != nil {
		t.Fatalf("Sample: %v", err)
	}
	if out2[0][0].Found {
		t.Errorf("T < first chunk's StartTs should be Found=false; got %+v", out2[0][0])
	}
}

// TestSampleValidationAggregatesAllViolations packs every validation rule
// into a single Sample call and checks that the returned errors.Join names
// each one. After summary load, no further ReadAt calls should happen.
func TestSampleValidationAggregatesAllViolations(t *testing.T) {
	r, tracking := buildSampleReader(t, func(w *Writer) {
		mustOpenTopics(t, w, []string{"a", "b"}, []map[string]any{{}, {}})
		mustWriteMessage(t, w, "a", []byte("a1"), 10)
		mustWriteMessage(t, w, "b", []byte("b1"), 20)
		mustCloseTopic(t, w)
		mustClose(t, w)
	})
	if _, err := r.Summary(); err != nil {
		t.Fatalf("Summary: %v", err)
	}
	before := tracking.readAtCalls.Load()

	queries := []SampleQuery{
		{Topic: "ghost", Timestamps: []int64{10}},      // unknown topic
		{Topic: "a", Timestamps: []int64{30, 20}},      // not strictly increasing
		{Topic: "a", Timestamps: []int64{40}},          // duplicate topic
		{Topic: "b", Timestamps: []int64{10, 10, 20}},  // duplicate (== not strict)
	}
	_, err := r.Sample(queries)
	if err == nil {
		t.Fatalf("expected validation error, got nil")
	}

	// Each violation should produce a sub-error whose text is identifiable
	// via the offending query index.
	wantSubstrings := []string{
		`queries[0]: unknown topic "ghost"`,
		`queries[1].Timestamps not strictly increasing at position 1 (20 <= 30)`,
		`queries[2]: duplicate topic "a" already used by queries[1]`,
		`queries[3].Timestamps not strictly increasing at position 1 (10 <= 10)`,
	}
	for _, s := range wantSubstrings {
		if !strings.Contains(err.Error(), s) {
			t.Errorf("aggregated error missing substring %q\nfull error:\n%s", s, err.Error())
		}
	}

	// errors.Join produces an error whose Unwrap() returns []error. Verify
	// every sub-error is still individually addressable so callers can
	// inspect them programmatically.
	type unwrapper interface{ Unwrap() []error }
	u, ok := err.(unwrapper)
	if !ok {
		t.Fatalf("Sample error is not an errors.Join: %T", err)
	}
	if got := len(u.Unwrap()); got != 4 {
		t.Errorf("expected 4 sub-errors, got %d", got)
	}

	if diff := tracking.readAtCalls.Load() - before; diff != 0 {
		t.Errorf("validation failure should not issue further ReadAt calls; observed %d additional calls", diff)
	}
}

func TestSampleEmptyTimestamps(t *testing.T) {
	r, tracking := buildSampleReader(t, func(w *Writer) {
		mustOpenTopics(t, w, []string{"a"}, []map[string]any{{}})
		mustWriteMessage(t, w, "a", []byte("x"), 10)
		mustCloseTopic(t, w)
		mustClose(t, w)
	})
	if _, err := r.Summary(); err != nil {
		t.Fatalf("Summary: %v", err)
	}
	before := tracking.readAtCalls.Load()

	out, err := r.Sample([]SampleQuery{{Topic: "a", Timestamps: nil}})
	if err != nil {
		t.Fatalf("Sample with empty Timestamps: %v", err)
	}
	if len(out) != 1 || len(out[0]) != 0 {
		t.Errorf("empty Timestamps should produce one empty result row, got %v", out)
	}
	if diff := tracking.readAtCalls.Load() - before; diff != 0 {
		t.Errorf("empty Timestamps should not issue further ReadAt calls; observed %d", diff)
	}
}

func TestLinSpaceTimestamps(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		got := LinSpaceTimestamps(100, 10, 5)
		want := []int64{100, 110, 120, 130, 140}
		if len(got) != len(want) {
			t.Fatalf("len got %d want %d", len(got), len(want))
		}
		for i, v := range want {
			if got[i] != v {
				t.Errorf("[%d] got %d want %d", i, got[i], v)
			}
		}
	})
	t.Run("strictly_increasing_contract", func(t *testing.T) {
		got := LinSpaceTimestamps(1000, 1, 1000)
		for i := 1; i < len(got); i++ {
			if got[i] <= got[i-1] {
				t.Errorf("LinSpaceTimestamps must produce strictly increasing values; failed at %d", i)
				break
			}
		}
	})
	t.Run("zero_count_is_nil", func(t *testing.T) {
		if got := LinSpaceTimestamps(0, 1, 0); got != nil {
			t.Errorf("expected nil for count=0, got %v", got)
		}
		if got := LinSpaceTimestamps(0, 1, -5); got != nil {
			t.Errorf("expected nil for count<0, got %v", got)
		}
	})
	t.Run("non_positive_stride_panics", func(t *testing.T) {
		for _, stride := range []int64{0, -1} {
			func() {
				defer func() {
					if recover() == nil {
						t.Errorf("expected panic for stride=%d", stride)
					}
				}()
				LinSpaceTimestamps(0, stride, 3)
			}()
		}
	})
}

// TestSampleConcurrency builds many small chunks and verifies that Sample
// actually issues concurrent ReadAt calls under a strategy that defeats
// coalescing.
func TestSampleConcurrency(t *testing.T) {
	raw := buildFile(t, func(w *Writer) {
		cfg := &ChunkConfig{Mode: ChunkThresholdModeCount, Count: 1}
		mustOpenTopics(t, w, []string{"t"}, []map[string]any{{}}, WithChunkConfig(cfg))
		for i := int64(0); i < 32; i++ {
			mustWriteMessage(t, w, "t", []byte{byte(i)}, (i+1)*10)
		}
		mustCloseTopic(t, w)
		mustClose(t, w)
	})
	// In-memory ReadAts return in nanoseconds; slow them so the fetcher's
	// concurrency is observable.
	slow := &slowReadAtSource{rs: bytes.NewReader(raw), slow: 5 * time.Millisecond}
	tracking := newTrackingSource(slow)
	r, err := NewReader(tracking)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	// Sample one T per chunk, so Phase A has 32 distinct ranges and Phase B
	// has 32 more. CoalesceGap=0 + SplitThreshold=MaxInt64 means one ReadAt
	// per range; MaxConcurrency=8 lets the fetcher fan out.
	timestamps := make([]int64, 32)
	for i := range timestamps {
		timestamps[i] = (int64(i) + 1) * 10
	}
	strat := ReadStrategy{CoalesceGap: 0, SplitThreshold: math.MaxInt64, MaxConcurrency: 8}

	out, err := r.Sample([]SampleQuery{{Topic: "t", Timestamps: timestamps}}, WithSampleReadStrategy(strat))
	if err != nil {
		t.Fatalf("Sample: %v", err)
	}
	if len(out[0]) != 32 {
		t.Fatalf("expected 32 results, got %d", len(out[0]))
	}
	for i, res := range out[0] {
		if !res.Found || res.Timestamp != (int64(i)+1)*10 || len(res.Data) != 1 || res.Data[0] != byte(i) {
			t.Errorf("[%d] want ts=%d data=%d got %+v", i, (int64(i)+1)*10, i, res)
		}
	}
	if tracking.readAtMaxFlight.Load() < 2 {
		t.Errorf("expected concurrent ReadAt observed; max in-flight = %d", tracking.readAtMaxFlight.Load())
	}
}

// assertResultsEqual compares slices of SampleResult with helpful diffs. The
// existing test helpers in this package compare iterator outputs (testMsg);
// SampleResult has a different shape so it gets its own comparator.
func assertResultsEqual(t *testing.T, got, want []SampleResult) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("result count: got %d want %d (got=%+v want=%+v)", len(got), len(want), got, want)
	}
	for i := range want {
		if got[i].Found != want[i].Found {
			t.Errorf("[%d] Found got %v want %v", i, got[i].Found, want[i].Found)
		}
		if got[i].Timestamp != want[i].Timestamp {
			t.Errorf("[%d] Timestamp got %d want %d", i, got[i].Timestamp, want[i].Timestamp)
		}
		if !bytes.Equal(got[i].Data, want[i].Data) {
			t.Errorf("[%d] Data got %q want %q", i, got[i].Data, want[i].Data)
		}
	}
}
