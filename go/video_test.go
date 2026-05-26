package turbodata

import (
	"bytes"
	"errors"
	"testing"

	"turbodata/format"
	"turbodata/internal/buffer"
	"turbodata/internal/compress"
)

// ---- Test helpers ---------------------------------------------------------

// mustWriteVideoMessage calls WriteVideoMessage and fatals on error.
func mustWriteVideoMessage(t *testing.T, w *Writer, name string, data []byte, ts int64, isKeyFrame bool) {
	t.Helper()
	if err := w.WriteVideoMessage(name, data, ts, isKeyFrame); err != nil {
		t.Fatalf("WriteVideoMessage(ts=%d, kf=%v): %v", ts, isKeyFrame, err)
	}
}

// frame is a single (timestamp, isKeyFrame, payload) triple for synthesizing
// test video streams. Synthetic GOPs of K=keyframe, P=delta are constructed
// by repeating these.
type frame struct {
	ts         int64
	isKeyFrame bool
	data       []byte
}

// writeVideo writes a sequence of synthetic video frames into one video topic.
func writeVideo(t *testing.T, w *Writer, topic string, frames []frame) {
	t.Helper()
	for _, f := range frames {
		mustWriteVideoMessage(t, w, topic, f.data, f.ts, f.isKeyFrame)
	}
}

// ---- WithVideoTopic option validation ------------------------------------

func TestWithVideoTopicOption(t *testing.T) {
	t.Run("unknown_codec_rejected", func(t *testing.T) {
		w := NewWriter(&bytes.Buffer{})
		err := w.OpenTopics([]string{"cam"}, []map[string]any{{}}, WithVideoTopic("vp10"))
		if !errors.Is(err, ErrUnknownVideoCodec) {
			t.Errorf("expected ErrUnknownVideoCodec, got %v", err)
		}
	})

	t.Run("h264_accepted", func(t *testing.T) {
		w := NewWriter(&bytes.Buffer{})
		if err := w.OpenTopics([]string{"cam"}, []map[string]any{{}}, WithVideoTopic("h264")); err != nil {
			t.Fatalf("OpenTopics(WithVideoTopic h264): %v", err)
		}
		if !w.writerConfig.isVideo {
			t.Error("expected isVideo=true")
		}
		if w.writerConfig.codec != "h264" {
			t.Errorf("expected codec=h264, got %q", w.writerConfig.codec)
		}
	})

	t.Run("compression_then_video_rejected", func(t *testing.T) {
		w := NewWriter(&bytes.Buffer{})
		// Options apply in order; WithCompression sets is_compressed first,
		// then WithVideoTopic rejects because the metadata already says
		// compressed.
		err := w.OpenTopics([]string{"cam"}, []map[string]any{{}},
			WithCompression(), WithVideoTopic("h264"))
		if !errors.Is(err, ErrVideoTopicCannotBeCompressed) {
			t.Errorf("expected ErrVideoTopicCannotBeCompressed, got %v", err)
		}
	})

	t.Run("video_then_compression_rejected_at_open", func(t *testing.T) {
		// Opposite ordering: WithVideoTopic sets is_video; WithCompression
		// later flips is_compressed. OpenTopics's own check catches this.
		w := NewWriter(&bytes.Buffer{})
		err := w.OpenTopics([]string{"cam"}, []map[string]any{{}},
			WithVideoTopic("h264"), WithCompression())
		if !errors.Is(err, ErrVideoTopicCannotBeCompressed) {
			t.Errorf("expected ErrVideoTopicCannotBeCompressed, got %v", err)
		}
	})
}

// ---- Group constraints ---------------------------------------------------

func TestVideoGroupMustBeSingleTopic(t *testing.T) {
	w := NewWriter(&bytes.Buffer{})
	err := w.OpenTopics(
		[]string{"cam", "stereo"},
		[]map[string]any{{}, {}},
		WithVideoTopic("h264"),
	)
	if !errors.Is(err, ErrVideoGroupMustBeSingleTopic) {
		t.Errorf("expected ErrVideoGroupMustBeSingleTopic, got %v", err)
	}
}

// ---- API gating: WriteMessage vs WriteVideoMessage -----------------------

func TestWriteMessageRejectedOnVideoTopic(t *testing.T) {
	w := NewWriter(&bytes.Buffer{})
	mustOpenTopics(t, w, []string{"cam"}, []map[string]any{{}}, WithVideoTopic("h264"))
	err := w.WriteMessage("cam", []byte("x"), 1)
	if !errors.Is(err, ErrWriteMessageOnVideoTopic) {
		t.Errorf("expected ErrWriteMessageOnVideoTopic, got %v", err)
	}
}

func TestWriteVideoMessageRejectedOnNonVideoTopic(t *testing.T) {
	w := NewWriter(&bytes.Buffer{})
	mustOpenTopics(t, w, []string{"imu"}, []map[string]any{{}})
	err := w.WriteVideoMessage("imu", []byte("x"), 1, true)
	if !errors.Is(err, ErrWriteVideoMessageOnNonVideoTopic) {
		t.Errorf("expected ErrWriteVideoMessageOnNonVideoTopic, got %v", err)
	}
}

// ---- First-message-must-be-keyframe --------------------------------------

func TestFirstVideoMessageMustBeKeyFrame(t *testing.T) {
	w := NewWriter(&bytes.Buffer{})
	mustOpenTopics(t, w, []string{"cam"}, []map[string]any{{}}, WithVideoTopic("h264"))
	err := w.WriteVideoMessage("cam", []byte("p0"), 1, false)
	if !errors.Is(err, ErrFirstVideoMessageMustBeKeyFrame) {
		t.Errorf("expected ErrFirstVideoMessageMustBeKeyFrame, got %v", err)
	}
}

// ---- GOP integrity at chunk boundaries -----------------------------------

// Stream of 6 frames K P P K P K with a tiny size threshold (1 byte) forces
// the writer to consider flushing on every keyframe. Each chunk must end
// just before a keyframe; every chunk must begin with KeyFrameIndexes[0]==0.
func TestVideoGOPIntegrityChunkBoundary(t *testing.T) {
	// 1-byte payloads + size threshold = 1: writer is "eager" to flush, so
	// the threshold is met before every key frame (except the very first).
	cfg := &ChunkConfig{Mode: ChunkThresholdModeSize, Size: 1}
	frames := []frame{
		{ts: 10, isKeyFrame: true, data: []byte("K0")},
		{ts: 20, isKeyFrame: false, data: []byte("P1")},
		{ts: 30, isKeyFrame: false, data: []byte("P2")},
		{ts: 40, isKeyFrame: true, data: []byte("K3")},
		{ts: 50, isKeyFrame: false, data: []byte("P4")},
		{ts: 60, isKeyFrame: true, data: []byte("K5")},
	}

	r := writerRoundTrip(t, func(w *Writer) {
		mustOpenTopics(t, w, []string{"cam"}, []map[string]any{{}},
			WithChunkConfig(cfg), WithVideoTopic("h264"))
		writeVideo(t, w, "cam", frames)
		mustCloseTopic(t, w)
		mustClose(t, w)
	})

	summary, err := r.Summary()
	if err != nil {
		t.Fatalf("Summary: %v", err)
	}
	if len(summary.TopicsInfos) != 1 {
		t.Fatalf("expected 1 TopicsInfo, got %d", len(summary.TopicsInfos))
	}

	// Three GOPs: [K0 P1 P2], [K3 P4], [K5]. Three chunks expected.
	infos := summary.TopicsInfos[0].IndexChunkInfoList
	if len(infos) != 3 {
		t.Fatalf("expected 3 chunks (one per GOP), got %d", len(infos))
	}

	// Walk each chunk via the iterator path: ReadMessages yields all frames
	// in storage order, partitioned by the IndexChunk boundaries we just
	// asserted. We exhaustively verify GOP integrity by re-walking the
	// summary's index chunks.
	for ci := range infos {
		ic := mustLoadIndexChunk(t, r, summary.TopicsInfos[0], ci)
		if len(ic.TopicIndexes) != 1 {
			t.Fatalf("chunk %d: expected 1 topic, got %d", ci, len(ic.TopicIndexes))
		}
		ti := ic.TopicIndexes[0]
		if len(ti.KeyFrameIndexes) == 0 {
			t.Errorf("chunk %d: expected at least one key frame, got 0", ci)
		}
		if ti.KeyFrameIndexes[0] != 0 {
			t.Errorf("chunk %d: KeyFrameIndexes[0]=%d, want 0 (chunk must begin with a key frame)", ci, ti.KeyFrameIndexes[0])
		}
	}

	// Round-trip the data and verify frame order is preserved.
	msgs := collect(t, r)
	if len(msgs) != len(frames) {
		t.Fatalf("expected %d messages round-trip, got %d", len(frames), len(msgs))
	}
	for i, m := range msgs {
		if !bytes.Equal(m.data, frames[i].data) {
			t.Errorf("msg[%d]: data %q, want %q", i, m.data, frames[i].data)
		}
		if m.ts != frames[i].ts {
			t.Errorf("msg[%d]: ts %d, want %d", i, m.ts, frames[i].ts)
		}
	}
}

// Threshold met inside a GOP should not flush mid-GOP. With size threshold of
// 1 byte and a 5-frame GOP [K P P P P], the threshold is met after every
// frame but we must wait for the next K (or CloseTopic) to flush.
func TestVideoNoFlushMidGOP(t *testing.T) {
	cfg := &ChunkConfig{Mode: ChunkThresholdModeSize, Size: 1}
	frames := []frame{
		{ts: 1, isKeyFrame: true, data: []byte("K")},
		{ts: 2, isKeyFrame: false, data: []byte("P")},
		{ts: 3, isKeyFrame: false, data: []byte("P")},
		{ts: 4, isKeyFrame: false, data: []byte("P")},
		{ts: 5, isKeyFrame: false, data: []byte("P")},
	}

	r := writerRoundTrip(t, func(w *Writer) {
		mustOpenTopics(t, w, []string{"cam"}, []map[string]any{{}},
			WithChunkConfig(cfg), WithVideoTopic("h264"))
		writeVideo(t, w, "cam", frames)
		mustCloseTopic(t, w)
		mustClose(t, w)
	})
	summary, err := r.Summary()
	if err != nil {
		t.Fatalf("Summary: %v", err)
	}
	// One GOP, never broken, regardless of how aggressive the threshold is.
	if got := len(summary.TopicsInfos[0].IndexChunkInfoList); got != 1 {
		t.Errorf("expected 1 chunk for the one GOP, got %d", got)
	}
}

// ---- Sample: single video query, full GOP prefix --------------------------

func TestSampleVideoSingleQueryReturnsGOPPrefix(t *testing.T) {
	// One GOP [K10, P20, P30, P40]. Query for the P30 timestamp should
	// return ResetDecoder=true and Frames=[K10, P20, P30] in storage order.
	frames := []frame{
		{ts: 10, isKeyFrame: true, data: []byte("K10")},
		{ts: 20, isKeyFrame: false, data: []byte("P20")},
		{ts: 30, isKeyFrame: false, data: []byte("P30")},
		{ts: 40, isKeyFrame: false, data: []byte("P40")},
	}
	r, _ := buildSampleReader(t, func(w *Writer) {
		mustOpenTopics(t, w, []string{"cam"}, []map[string]any{{}}, WithVideoTopic("h264"))
		writeVideo(t, w, "cam", frames)
		mustCloseTopic(t, w)
		mustClose(t, w)
	})

	out, err := r.Sample([]SampleQuery{{Topic: "cam", Timestamps: []int64{30}}})
	if err != nil {
		t.Fatalf("Sample: %v", err)
	}
	row := out[0]
	if len(row) != 1 {
		t.Fatalf("expected 1 result, got %d", len(row))
	}
	res := row[0]
	if !res.Found {
		t.Fatal("Found=false; want true")
	}
	if !res.ResetDecoder {
		t.Error("expected ResetDecoder=true for first row result")
	}
	wantFrames := []frame{frames[0], frames[1], frames[2]}
	assertFramesMatch(t, res.Frames, wantFrames)
	if string(res.Data) != "P30" || res.Timestamp != 30 {
		t.Errorf("target frame Data/Timestamp wrong: got data=%q ts=%d", res.Data, res.Timestamp)
	}
}

// ---- Sample: query lands on the key frame itself --------------------------

func TestSampleVideoQueryOnKeyFrame(t *testing.T) {
	frames := []frame{
		{ts: 10, isKeyFrame: true, data: []byte("K10")},
		{ts: 20, isKeyFrame: false, data: []byte("P20")},
	}
	r, _ := buildSampleReader(t, func(w *Writer) {
		mustOpenTopics(t, w, []string{"cam"}, []map[string]any{{}}, WithVideoTopic("h264"))
		writeVideo(t, w, "cam", frames)
		mustCloseTopic(t, w)
		mustClose(t, w)
	})
	out, err := r.Sample([]SampleQuery{{Topic: "cam", Timestamps: []int64{10}}})
	if err != nil {
		t.Fatalf("Sample: %v", err)
	}
	res := out[0][0]
	if !res.Found || !res.ResetDecoder {
		t.Fatalf("want Found+Reset, got Found=%v Reset=%v", res.Found, res.ResetDecoder)
	}
	if len(res.Frames) != 1 {
		t.Fatalf("query exactly on key frame: expected len(Frames)=1, got %d", len(res.Frames))
	}
	if !res.Frames[0].IsKeyFrame {
		t.Error("Frames[0].IsKeyFrame should be true for an exact-keyframe query")
	}
}

// ---- Sample: three queries in same GOP get incremental Frames -------------

func TestSampleVideoIncrementalSameGOP(t *testing.T) {
	// One GOP with 6 frames: K0 at ts=10, P1..P5 at ts=20..60.
	frames := []frame{
		{ts: 10, isKeyFrame: true, data: []byte("K0")},
		{ts: 20, isKeyFrame: false, data: []byte("P1")},
		{ts: 30, isKeyFrame: false, data: []byte("P2")},
		{ts: 40, isKeyFrame: false, data: []byte("P3")},
		{ts: 50, isKeyFrame: false, data: []byte("P4")},
		{ts: 60, isKeyFrame: false, data: []byte("P5")},
	}
	r, _ := buildSampleReader(t, func(w *Writer) {
		mustOpenTopics(t, w, []string{"cam"}, []map[string]any{{}}, WithVideoTopic("h264"))
		writeVideo(t, w, "cam", frames)
		mustCloseTopic(t, w)
		mustClose(t, w)
	})

	// Three queries: 25 (-> P1), 45 (-> P3), 60 (-> P5). All same GOP.
	out, err := r.Sample([]SampleQuery{{Topic: "cam", Timestamps: []int64{25, 45, 60}}})
	if err != nil {
		t.Fatalf("Sample: %v", err)
	}
	row := out[0]
	if len(row) != 3 {
		t.Fatalf("expected 3 results, got %d", len(row))
	}

	// First result: ResetDecoder=true, full prefix [K0, P1].
	r0 := row[0]
	if !r0.ResetDecoder {
		t.Error("row[0].ResetDecoder should be true (first in row)")
	}
	assertFramesMatch(t, r0.Frames, []frame{frames[0], frames[1]})

	// Second result: ResetDecoder=false, only new frames [P2, P3].
	r1 := row[1]
	if r1.ResetDecoder {
		t.Error("row[1].ResetDecoder should be false (same GOP)")
	}
	assertFramesMatch(t, r1.Frames, []frame{frames[2], frames[3]})

	// Third result: ResetDecoder=false, only new frames [P4, P5].
	r2 := row[2]
	if r2.ResetDecoder {
		t.Error("row[2].ResetDecoder should be false (same GOP)")
	}
	assertFramesMatch(t, r2.Frames, []frame{frames[4], frames[5]})

	// Total bytes across all Frames must equal the size of the full GOP
	// prefix up to the last query's target (no duplicates).
	totalGot := 0
	for _, r := range row {
		for _, f := range r.Frames {
			totalGot += len(f.Data)
		}
	}
	wantTotal := 0
	for _, f := range frames {
		wantTotal += len(f.data)
	}
	if totalGot != wantTotal {
		t.Errorf("dedup check: total Frames bytes = %d, want %d (= sum of K0..P5 payloads)", totalGot, wantTotal)
	}
}

// ---- Sample: two queries spanning different GOPs --------------------------

func TestSampleVideoSpanningTwoGOPs(t *testing.T) {
	// Two GOPs forced by a tiny chunk size; the writer emits one chunk per
	// GOP because of the keyframe-gated boundary rule.
	cfg := &ChunkConfig{Mode: ChunkThresholdModeSize, Size: 1}
	frames := []frame{
		// GOP A
		{ts: 10, isKeyFrame: true, data: []byte("KA")},
		{ts: 20, isKeyFrame: false, data: []byte("A1")},
		{ts: 30, isKeyFrame: false, data: []byte("A2")},
		// GOP B
		{ts: 40, isKeyFrame: true, data: []byte("KB")},
		{ts: 50, isKeyFrame: false, data: []byte("B1")},
		{ts: 60, isKeyFrame: false, data: []byte("B2")},
	}
	r, _ := buildSampleReader(t, func(w *Writer) {
		mustOpenTopics(t, w, []string{"cam"}, []map[string]any{{}},
			WithChunkConfig(cfg), WithVideoTopic("h264"))
		writeVideo(t, w, "cam", frames)
		mustCloseTopic(t, w)
		mustClose(t, w)
	})

	// Two queries: 25 (-> A1 in GOP A), 55 (-> B1 in GOP B).
	out, err := r.Sample([]SampleQuery{{Topic: "cam", Timestamps: []int64{25, 55}}})
	if err != nil {
		t.Fatalf("Sample: %v", err)
	}
	row := out[0]

	// First result: GOP A prefix [KA, A1], ResetDecoder=true.
	if !row[0].ResetDecoder {
		t.Error("row[0].ResetDecoder should be true")
	}
	assertFramesMatch(t, row[0].Frames, []frame{frames[0], frames[1]})

	// Second result: different GOP, so ResetDecoder=true again and full
	// prefix from GOP B's key frame.
	if !row[1].ResetDecoder {
		t.Error("row[1].ResetDecoder should be true (different GOP)")
	}
	assertFramesMatch(t, row[1].Frames, []frame{frames[3], frames[4]})
}

// ---- Sample: two queries that resolve to the same target frame -----------

func TestSampleVideoTwoQueriesSameTarget(t *testing.T) {
	frames := []frame{
		{ts: 10, isKeyFrame: true, data: []byte("K")},
		{ts: 20, isKeyFrame: false, data: []byte("P1")},
		{ts: 30, isKeyFrame: false, data: []byte("P2")},
	}
	r, _ := buildSampleReader(t, func(w *Writer) {
		mustOpenTopics(t, w, []string{"cam"}, []map[string]any{{}}, WithVideoTopic("h264"))
		writeVideo(t, w, "cam", frames)
		mustCloseTopic(t, w)
		mustClose(t, w)
	})
	// 21 and 22 both floor to P1 (ts=20).
	out, err := r.Sample([]SampleQuery{{Topic: "cam", Timestamps: []int64{21, 22}}})
	if err != nil {
		t.Fatalf("Sample: %v", err)
	}
	row := out[0]

	// First: K, P1 with reset.
	if !row[0].ResetDecoder || len(row[0].Frames) != 2 {
		t.Errorf("row[0]: want Reset+len(Frames)=2; got Reset=%v len=%d", row[0].ResetDecoder, len(row[0].Frames))
	}

	// Second: same target frame. Frames is empty (nothing new to feed),
	// ResetDecoder=false, but Data/Timestamp still describe the target.
	if row[1].ResetDecoder {
		t.Error("row[1].ResetDecoder should be false")
	}
	if len(row[1].Frames) != 0 {
		t.Errorf("row[1].Frames should be empty (same target), got %d", len(row[1].Frames))
	}
	if string(row[1].Data) != "P1" || row[1].Timestamp != 20 {
		t.Errorf("row[1] target mismatch: Data=%q Ts=%d", row[1].Data, row[1].Timestamp)
	}
}

// ---- Sample: timestamp older than any keyframe gets Found=false ---------

func TestSampleVideoBeforeFirstKeyFrame(t *testing.T) {
	frames := []frame{
		{ts: 10, isKeyFrame: true, data: []byte("K")},
		{ts: 20, isKeyFrame: false, data: []byte("P")},
	}
	r, _ := buildSampleReader(t, func(w *Writer) {
		mustOpenTopics(t, w, []string{"cam"}, []map[string]any{{}}, WithVideoTopic("h264"))
		writeVideo(t, w, "cam", frames)
		mustCloseTopic(t, w)
		mustClose(t, w)
	})
	out, err := r.Sample([]SampleQuery{{Topic: "cam", Timestamps: []int64{5}}})
	if err != nil {
		t.Fatalf("Sample: %v", err)
	}
	if out[0][0].Found {
		t.Error("expected Found=false for T < first key frame")
	}
}

// ---- ReadMessages: WithVideoDecodable snaps StartTimestamp back ----------

func TestReadMessagesVideoDecodableSnapBack(t *testing.T) {
	frames := []frame{
		{ts: 10, isKeyFrame: true, data: []byte("K10")},
		{ts: 20, isKeyFrame: false, data: []byte("P20")},
		{ts: 30, isKeyFrame: false, data: []byte("P30")},
		{ts: 40, isKeyFrame: false, data: []byte("P40")},
	}
	r := writerRoundTrip(t, func(w *Writer) {
		mustOpenTopics(t, w, []string{"cam"}, []map[string]any{{}}, WithVideoTopic("h264"))
		writeVideo(t, w, "cam", frames)
		mustCloseTopic(t, w)
		mustClose(t, w)
	})

	// Without snap-back, requesting StartTimestamp=25 skips K10 and P20.
	{
		msgs := collect(t, r, WithStartTimestamp(25))
		if len(msgs) != 2 || string(msgs[0].data) != "P30" {
			t.Errorf("plain start=25: expected [P30, P40], got %v", msgPayloads(msgs))
		}
	}

	// With snap-back, StartTimestamp=25 should snap back to ts=10 (the key
	// frame) and the iterator should yield K10, P20, P30, P40 in order.
	{
		msgs := collect(t, r, WithStartTimestamp(25), WithVideoDecodable())
		want := []string{"K10", "P20", "P30", "P40"}
		got := msgPayloads(msgs)
		if !stringSliceEqual(got, want) {
			t.Errorf("snap-back start=25: want %v, got %v", want, got)
		}
	}
}

// Snap-back behaves correctly when the requested start lies mid-GOP in a
// later chunk: the iterator must reach back into THIS chunk's key frame, not
// to the prior chunk's last key frame.
func TestReadMessagesVideoDecodableSnapBackMultipleGOPs(t *testing.T) {
	cfg := &ChunkConfig{Mode: ChunkThresholdModeSize, Size: 1}
	frames := []frame{
		{ts: 10, isKeyFrame: true, data: []byte("KA")},
		{ts: 20, isKeyFrame: false, data: []byte("A1")},
		{ts: 30, isKeyFrame: true, data: []byte("KB")},
		{ts: 40, isKeyFrame: false, data: []byte("B1")},
		{ts: 50, isKeyFrame: false, data: []byte("B2")},
	}
	r := writerRoundTrip(t, func(w *Writer) {
		mustOpenTopics(t, w, []string{"cam"}, []map[string]any{{}},
			WithChunkConfig(cfg), WithVideoTopic("h264"))
		writeVideo(t, w, "cam", frames)
		mustCloseTopic(t, w)
		mustClose(t, w)
	})

	// Start=45 mid-GOP B; snap back should land on KB (ts=30), not KA.
	msgs := collect(t, r, WithStartTimestamp(45), WithVideoDecodable())
	want := []string{"KB", "B1", "B2"}
	got := msgPayloads(msgs)
	if !stringSliceEqual(got, want) {
		t.Errorf("snap-back over GOP boundary: want %v, got %v", want, got)
	}
}

// Without WithVideoDecodable, ReadMessages must NOT snap back. The caller
// gets exactly the time-filtered messages (possibly mid-GOP).
func TestReadMessagesVideoNoSnapBackByDefault(t *testing.T) {
	frames := []frame{
		{ts: 10, isKeyFrame: true, data: []byte("K")},
		{ts: 20, isKeyFrame: false, data: []byte("P1")},
		{ts: 30, isKeyFrame: false, data: []byte("P2")},
	}
	r := writerRoundTrip(t, func(w *Writer) {
		mustOpenTopics(t, w, []string{"cam"}, []map[string]any{{}}, WithVideoTopic("h264"))
		writeVideo(t, w, "cam", frames)
		mustCloseTopic(t, w)
		mustClose(t, w)
	})
	msgs := collect(t, r, WithStartTimestamp(25))
	want := []string{"P2"}
	got := msgPayloads(msgs)
	if !stringSliceEqual(got, want) {
		t.Errorf("no snap-back: want %v, got %v", want, got)
	}
}

// ---- Helpers --------------------------------------------------------------

// mustLoadIndexChunk fetches, decompresses, and parses one index chunk of a
// TopicsInfo via the reader's underlying ReadSource. Used by GOP-integrity
// tests that need direct access to KeyFrameIndexes on the wire.
func mustLoadIndexChunk(t *testing.T, r *Reader, ti *format.TopicsInfo, ci int) *format.IndexChunk {
	t.Helper()
	infos := ti.IndexChunkInfoList
	info := infos[ci]
	var ln int64
	if ci < len(infos)-1 {
		ln = infos[ci+1].Offset - info.Offset
	} else {
		ln = ti.TotalLen - info.Offset + infos[0].Offset
	}
	raw := make([]byte, ln)
	if _, err := r.rs.ReadAt(raw, info.Offset); err != nil {
		t.Fatalf("ReadAt index chunk %d: %v", ci, err)
	}
	decompBuf := buffer.NewReusableBuffer()
	if err := compress.DecompressInto(raw, decompBuf); err != nil {
		t.Fatalf("decompress index chunk %d: %v", ci, err)
	}
	ic, err := format.ReadIndexChunk(bytes.NewReader(decompBuf.Data))
	if err != nil {
		t.Fatalf("ReadIndexChunk %d: %v", ci, err)
	}
	return ic
}

func assertFramesMatch(t *testing.T, got []Frame, want []frame) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("Frames length: got %d want %d (got payloads %v want %v)",
			len(got), len(want), framePayloads(got), wantPayloads(want))
		return
	}
	for i := range want {
		if !bytes.Equal(got[i].Data, want[i].data) {
			t.Errorf("Frames[%d].Data: got %q want %q", i, got[i].Data, want[i].data)
		}
		if got[i].Timestamp != want[i].ts {
			t.Errorf("Frames[%d].Timestamp: got %d want %d", i, got[i].Timestamp, want[i].ts)
		}
		if got[i].IsKeyFrame != want[i].isKeyFrame {
			t.Errorf("Frames[%d].IsKeyFrame: got %v want %v", i, got[i].IsKeyFrame, want[i].isKeyFrame)
		}
	}
}

func framePayloads(fs []Frame) []string {
	out := make([]string, len(fs))
	for i, f := range fs {
		out[i] = string(f.Data)
	}
	return out
}

func wantPayloads(fs []frame) []string {
	out := make([]string, len(fs))
	for i, f := range fs {
		out[i] = string(f.data)
	}
	return out
}

func msgPayloads(ms []testMsg) []string {
	out := make([]string, len(ms))
	for i, m := range ms {
		out[i] = string(m.data)
	}
	return out
}

func stringSliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
