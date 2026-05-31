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
