package turbodata

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"testing"
)

// buildOneTopic builds a single-topic file with one message per timestamp.
// Message payload is "<topic>@<ts>" so callers can assert provenance.
func buildOneTopic(t *testing.T, topic string, tss ...int64) []byte {
	t.Helper()
	return buildFile(t, func(w *Writer) {
		mustOpenTopics(t, w, []string{topic}, []map[string]any{{}})
		for _, ts := range tss {
			mustWriteMessage(t, w, topic, []byte(fmt.Sprintf("%s@%d", topic, ts)), ts)
		}
		mustCloseTopic(t, w)
		mustClose(t, w)
	})
}

// collectMulti drains a MultiReader into a slice of testMsgs.
func collectMulti(t *testing.T, mr *MultiReader, opts ...ReadOption) []testMsg {
	t.Helper()
	it, err := mr.ReadMessages(opts...)
	if err != nil {
		t.Fatalf("ReadMessages: %v", err)
	}
	rb := NewReusableBuffer()
	var msgs []testMsg
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
		msgs = append(msgs, testMsg{ts, name, d})
	}
	return msgs
}

// TestMultiReadTimeSplit: two files holding the same topic over disjoint time
// ranges merge into one fully ordered stream, forward and reverse.
func TestMultiReadTimeSplit(t *testing.T) {
	a := buildOneTopic(t, "cam", 10, 30, 50)
	b := buildOneTopic(t, "cam", 20, 40, 60)
	mr, err := NewMultiReader(
		NewReader(bytes.NewReader(a)),
		NewReader(bytes.NewReader(b)),
	)
	if err != nil {
		t.Fatalf("NewMultiReader: %v", err)
	}

	msgs := collectMulti(t, mr)
	wantTs := []int64{10, 20, 30, 40, 50, 60}
	if len(msgs) != len(wantTs) {
		t.Fatalf("forward count got %d want %d (%+v)", len(msgs), len(wantTs), msgs)
	}
	for i, ts := range wantTs {
		if msgs[i].ts != ts || msgs[i].name != "cam" {
			t.Errorf("forward[%d] got (%d,%q) want (%d,cam)", i, msgs[i].ts, msgs[i].name, ts)
		}
	}

	rev := collectMulti(t, mr, WithOrder(ReverseTimeOrder))
	wantRev := []int64{60, 50, 40, 30, 20, 10}
	if len(rev) != len(wantRev) {
		t.Fatalf("reverse count got %d want %d (%+v)", len(rev), len(wantRev), rev)
	}
	for i, ts := range wantRev {
		if rev[i].ts != ts {
			t.Errorf("reverse[%d] ts got %d want %d", i, rev[i].ts, ts)
		}
	}
}

// TestMultiReadAugmentation: two files with disjoint topics over overlapping
// time merge into the time-ordered union of both topics.
func TestMultiReadAugmentation(t *testing.T) {
	a := buildOneTopic(t, "lidar", 10, 20, 30)
	b := buildOneTopic(t, "radar", 15, 25, 35)
	mr, err := NewMultiReader(
		NewReader(bytes.NewReader(a)),
		NewReader(bytes.NewReader(b)),
	)
	if err != nil {
		t.Fatalf("NewMultiReader: %v", err)
	}

	msgs := collectMulti(t, mr)
	type tn struct {
		ts   int64
		name string
	}
	want := []tn{{10, "lidar"}, {15, "radar"}, {20, "lidar"}, {25, "radar"}, {30, "lidar"}, {35, "radar"}}
	if len(msgs) != len(want) {
		t.Fatalf("count got %d want %d (%+v)", len(msgs), len(want), msgs)
	}
	for i, w := range want {
		if msgs[i].ts != w.ts || msgs[i].name != w.name {
			t.Errorf("[%d] got (%d,%q) want (%d,%q)", i, msgs[i].ts, msgs[i].name, w.ts, w.name)
		}
	}
}

// TestMultiReadUnionVsSplit: a shared in-file name unions across files, while
// a per-Reader remap keeps the colliding name distinct.
func TestMultiReadUnionVsSplit(t *testing.T) {
	a := buildOneTopic(t, "cam", 10, 30)
	b := buildOneTopic(t, "cam", 20, 40)

	union, err := NewMultiReader(
		NewReader(bytes.NewReader(a)),
		NewReader(bytes.NewReader(b)),
	)
	if err != nil {
		t.Fatalf("NewMultiReader union: %v", err)
	}
	um := collectMulti(t, union)
	if len(um) != 4 {
		t.Fatalf("union count got %d want 4", len(um))
	}
	for i, m := range um {
		if m.name != "cam" {
			t.Errorf("union[%d] name got %q want cam", i, m.name)
		}
	}

	split, err := NewMultiReader(
		NewReader(bytes.NewReader(a)),
		NewReader(bytes.NewReader(b), WithTopicRemap(map[string]string{"cam": "cam_b"})),
	)
	if err != nil {
		t.Fatalf("NewMultiReader split: %v", err)
	}
	sm := collectMulti(t, split)
	names := map[string]int{}
	for _, m := range sm {
		names[m.name]++
	}
	if names["cam"] != 2 || names["cam_b"] != 2 {
		t.Errorf("split names got %v want cam:2 cam_b:2", names)
	}
}

// buildVideoFile builds a single video-topic file from the given frames.
func buildVideoFile(t *testing.T, topic string, frames []frame) []byte {
	t.Helper()
	return buildFile(t, func(w *Writer) {
		mustOpenTopics(t, w, []string{topic}, []map[string]any{{}}, WithVideoTopic())
		writeVideo(t, w, topic, frames)
		mustCloseTopic(t, w)
		mustClose(t, w)
	})
}

// TestMultiSampleLatestFloor: the same non-video topic split across two files
// resolves each timestamp to the latest floor across the union.
func TestMultiSampleLatestFloor(t *testing.T) {
	a := buildOneTopic(t, "cam", 10, 20)
	b := buildOneTopic(t, "cam", 30, 40)
	mr, err := NewMultiReader(
		NewReader(bytes.NewReader(a)),
		NewReader(bytes.NewReader(b)),
	)
	if err != nil {
		t.Fatalf("NewMultiReader: %v", err)
	}

	out, err := mr.Sample([]SampleQuery{
		{Topic: "cam", Timestamps: []int64{5, 15, 35, 50}},
	})
	if err != nil {
		t.Fatalf("Sample: %v", err)
	}
	want := []SampleResult{
		{Found: false},
		{Found: true, Timestamp: 10, Data: []byte("cam@10")},
		{Found: true, Timestamp: 30, Data: []byte("cam@30")},
		{Found: true, Timestamp: 40, Data: []byte("cam@40")},
	}
	assertResultsEqual(t, out[0], want)
}

// TestMultiSampleVideoTimeDisjoint: a video topic spread across two
// time-disjoint files samples correctly, with each picked cell carrying its
// reader's GOP prefix (ResetDecoder=true at the switch boundary).
func TestMultiSampleVideoTimeDisjoint(t *testing.T) {
	aFrames := []frame{
		{ts: 10, isKeyFrame: true, data: []byte("K10")},
		{ts: 20, isKeyFrame: false, data: []byte("P20")},
		{ts: 30, isKeyFrame: false, data: []byte("P30")},
	}
	bFrames := []frame{
		{ts: 100, isKeyFrame: true, data: []byte("K100")},
		{ts: 110, isKeyFrame: false, data: []byte("P110")},
		{ts: 120, isKeyFrame: false, data: []byte("P120")},
	}
	a := buildVideoFile(t, "cam", aFrames)
	b := buildVideoFile(t, "cam", bFrames)
	mr, err := NewMultiReader(
		NewReader(bytes.NewReader(a)),
		NewReader(bytes.NewReader(b)),
	)
	if err != nil {
		t.Fatalf("NewMultiReader: %v", err)
	}

	out, err := mr.Sample([]SampleQuery{
		{Topic: "cam", Timestamps: []int64{25, 115}},
	}, WithSampleVideoDecodable())
	if err != nil {
		t.Fatalf("Sample: %v", err)
	}
	row := out[0]
	if len(row) != 2 {
		t.Fatalf("expected 2 results, got %d", len(row))
	}

	// T=25 -> file A, floor P20, full GOP prefix [K10, P20].
	if !row[0].Found || !row[0].IsVideo || !row[0].ResetDecoder || row[0].Timestamp != 20 {
		t.Errorf("row[0]: got Found=%v IsVideo=%v Reset=%v ts=%d; want true,true,true,20",
			row[0].Found, row[0].IsVideo, row[0].ResetDecoder, row[0].Timestamp)
	}
	assertFramesMatch(t, row[0].Frames, []frame{aFrames[0], aFrames[1]})

	// T=115 -> file B wins (latest floor), first Found cell of B's row so
	// ResetDecoder=true with B's GOP prefix [K100, P110].
	if !row[1].Found || !row[1].IsVideo || !row[1].ResetDecoder || row[1].Timestamp != 110 {
		t.Errorf("row[1]: got Found=%v IsVideo=%v Reset=%v ts=%d; want true,true,true,110",
			row[1].Found, row[1].IsVideo, row[1].ResetDecoder, row[1].Timestamp)
	}
	assertFramesMatch(t, row[1].Frames, []frame{bFrames[0], bFrames[1]})
}

// TestMultiSampleVideoOverlapError: overlapping video sources are rejected
// under WithSampleVideoDecodable, but allowed (plain floor) without it.
func TestMultiSampleVideoOverlapError(t *testing.T) {
	aFrames := []frame{
		{ts: 10, isKeyFrame: true, data: []byte("K10")},
		{ts: 30, isKeyFrame: false, data: []byte("P30")},
		{ts: 50, isKeyFrame: false, data: []byte("P50")},
	}
	bFrames := []frame{
		{ts: 40, isKeyFrame: true, data: []byte("K40")},
		{ts: 60, isKeyFrame: false, data: []byte("P60")},
		{ts: 80, isKeyFrame: false, data: []byte("P80")},
	}
	a := buildVideoFile(t, "cam", aFrames)
	b := buildVideoFile(t, "cam", bFrames)
	mr, err := NewMultiReader(
		NewReader(bytes.NewReader(a)),
		NewReader(bytes.NewReader(b)),
	)
	if err != nil {
		t.Fatalf("NewMultiReader: %v", err)
	}

	// Overlapping [10,50] and [40,80] under the decodable option is rejected.
	_, err = mr.Sample([]SampleQuery{
		{Topic: "cam", Timestamps: []int64{45}},
	}, WithSampleVideoDecodable())
	if !errors.Is(err, ErrVideoSourcesOverlap) {
		t.Errorf("expected ErrVideoSourcesOverlap, got %v", err)
	}

	// Without the decodable option, overlap is fine: plain floor semantics.
	out, err := mr.Sample([]SampleQuery{
		{Topic: "cam", Timestamps: []int64{45}},
	})
	if err != nil {
		t.Fatalf("Sample (plain): %v", err)
	}
	// Floor of 45 across both: A=P30(30), B=K40(40) -> latest 40.
	got := out[0][0]
	if !got.Found || got.Timestamp != 40 || string(got.Data) != "K40" {
		t.Errorf("plain floor: got Found=%v ts=%d data=%q; want true,40,K40", got.Found, got.Timestamp, got.Data)
	}
}

// buildVideoPlusTopic builds a file with one video topic and one plain topic.
func buildVideoPlusTopic(t *testing.T, videoTopic string, vframes []frame, plainTopic string, plainTs []int64) []byte {
	t.Helper()
	return buildFile(t, func(w *Writer) {
		mustOpenTopics(t, w, []string{videoTopic}, []map[string]any{{}}, WithVideoTopic())
		writeVideo(t, w, videoTopic, vframes)
		mustCloseTopic(t, w)
		mustOpenTopics(t, w, []string{plainTopic}, []map[string]any{{}})
		for _, ts := range plainTs {
			mustWriteMessage(t, w, plainTopic, []byte(fmt.Sprintf("%s@%d", plainTopic, ts)), ts)
		}
		mustCloseTopic(t, w)
		mustClose(t, w)
	})
}

// TestMultiReadVideoTimeDisjoint: a video topic split across two time-disjoint
// files merges fine under WithVideoDecodable.
func TestMultiReadVideoTimeDisjoint(t *testing.T) {
	aFrames := []frame{
		{ts: 10, isKeyFrame: true, data: []byte("K10")},
		{ts: 20, isKeyFrame: false, data: []byte("P20")},
		{ts: 30, isKeyFrame: false, data: []byte("P30")},
	}
	bFrames := []frame{
		{ts: 100, isKeyFrame: true, data: []byte("K100")},
		{ts: 110, isKeyFrame: false, data: []byte("P110")},
		{ts: 120, isKeyFrame: false, data: []byte("P120")},
	}
	mr, err := NewMultiReader(
		NewReader(bytes.NewReader(buildVideoFile(t, "cam", aFrames))),
		NewReader(bytes.NewReader(buildVideoFile(t, "cam", bFrames))),
	)
	if err != nil {
		t.Fatalf("NewMultiReader: %v", err)
	}

	msgs := collectMulti(t, mr, WithVideoDecodable())
	wantTs := []int64{10, 20, 30, 100, 110, 120}
	if len(msgs) != len(wantTs) {
		t.Fatalf("count got %d want %d (%+v)", len(msgs), len(wantTs), msgs)
	}
	for i, ts := range wantTs {
		if msgs[i].ts != ts || msgs[i].name != "cam" {
			t.Errorf("[%d] got (%d,%q) want (%d,cam)", i, msgs[i].ts, msgs[i].name, ts)
		}
	}
}

// TestMultiReadVideoOverlapError: under WithVideoDecodable, overlapping video
// sources are rejected (merging them would interleave GOP chains); without the
// option the merge is allowed.
func TestMultiReadVideoOverlapError(t *testing.T) {
	aFrames := []frame{
		{ts: 10, isKeyFrame: true, data: []byte("K10")},
		{ts: 30, isKeyFrame: false, data: []byte("P30")},
		{ts: 50, isKeyFrame: false, data: []byte("P50")},
	}
	bFrames := []frame{
		{ts: 40, isKeyFrame: true, data: []byte("K40")},
		{ts: 60, isKeyFrame: false, data: []byte("P60")},
		{ts: 80, isKeyFrame: false, data: []byte("P80")},
	}
	mr, err := NewMultiReader(
		NewReader(bytes.NewReader(buildVideoFile(t, "cam", aFrames))),
		NewReader(bytes.NewReader(buildVideoFile(t, "cam", bFrames))),
	)
	if err != nil {
		t.Fatalf("NewMultiReader: %v", err)
	}

	// Overlapping [10,50] and [40,80] under WithVideoDecodable is rejected.
	if _, err := mr.ReadMessages(WithVideoDecodable()); !errors.Is(err, ErrVideoSourcesOverlap) {
		t.Errorf("expected ErrVideoSourcesOverlap, got %v", err)
	}

	// Without the option, the merge is allowed (no decode contract).
	if _, err := mr.ReadMessages(); err != nil {
		t.Errorf("plain ReadMessages should not error, got %v", err)
	}
}

// TestMultiReadVideoOverlapOutOfScope: an overlapping video topic that is
// filtered out via WithTopicNames must not trigger the disjoint check.
func TestMultiReadVideoOverlapOutOfScope(t *testing.T) {
	aFrames := []frame{
		{ts: 10, isKeyFrame: true, data: []byte("K10")},
		{ts: 50, isKeyFrame: false, data: []byte("P50")},
	}
	bFrames := []frame{
		{ts: 40, isKeyFrame: true, data: []byte("K40")},
		{ts: 80, isKeyFrame: false, data: []byte("P80")},
	}
	mr, err := NewMultiReader(
		NewReader(bytes.NewReader(buildVideoPlusTopic(t, "cam", aFrames, "imu", []int64{15, 25}))),
		NewReader(bytes.NewReader(buildVideoPlusTopic(t, "cam", bFrames, "imu", []int64{45, 55}))),
	)
	if err != nil {
		t.Fatalf("NewMultiReader: %v", err)
	}

	// "cam" overlaps, but it is out of scope: only "imu" is requested.
	msgs := collectMulti(t, mr, WithVideoDecodable(), WithTopicNames([]string{"imu"}))
	wantTs := []int64{15, 25, 45, 55}
	if len(msgs) != len(wantTs) {
		t.Fatalf("count got %d want %d (%+v)", len(msgs), len(wantTs), msgs)
	}
	for i, ts := range wantTs {
		if msgs[i].ts != ts || msgs[i].name != "imu" {
			t.Errorf("[%d] got (%d,%q) want (%d,imu)", i, msgs[i].ts, msgs[i].name, ts)
		}
	}

	// Bringing "cam" into scope re-triggers the overlap error.
	if _, err := mr.ReadMessages(WithVideoDecodable(), WithTopicNames([]string{"cam"})); !errors.Is(err, ErrVideoSourcesOverlap) {
		t.Errorf("in-scope cam: expected ErrVideoSourcesOverlap, got %v", err)
	}
}
