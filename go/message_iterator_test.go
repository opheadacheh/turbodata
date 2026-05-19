package turbodata

import (
	"errors"
	"io"
	"testing"
)

// collectWithOpts drains a Reader into a slice of testMsgs using the given ReadOptions.
func collectWithOpts(t *testing.T, r *Reader, opts ...ReadOption) []testMsg {
	t.Helper()
	it, err := r.ReadMessages(opts...)
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

func TestNextIntoEmpty(t *testing.T) {
	r := writerRoundTrip(t, func(w *Writer) {
		mustClose(t, w)
	})

	it, err := r.ReadMessages()
	if err != nil {
		t.Fatalf("ReadMessages: %v", err)
	}
	rb := NewReusableBuffer()
	_, _, err = it.NextInto(rb)
	if !errors.Is(err, io.EOF) {
		t.Errorf("expected io.EOF for empty file, got %v", err)
	}
}

func TestNextIntoSingleTopicOrder(t *testing.T) {
	r := writerRoundTrip(t, func(w *Writer) {
		mustOpenTopics(t, w, []string{"cam"}, []map[string]any{{}})
		mustWriteMessage(t, w, "cam", []byte("frame0"), 10)
		mustWriteMessage(t, w, "cam", []byte("frame1"), 20)
		mustWriteMessage(t, w, "cam", []byte("frame2"), 30)
		mustCloseTopic(t, w)
		mustClose(t, w)
	})

	msgs := collectWithOpts(t, r)
	if len(msgs) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(msgs))
	}
	want := []testMsg{
		{10, "cam", []byte("frame0")},
		{20, "cam", []byte("frame1")},
		{30, "cam", []byte("frame2")},
	}
	for i, m := range msgs {
		if m.ts != want[i].ts {
			t.Errorf("msg[%d]: timestamp want %d, got %d", i, want[i].ts, m.ts)
		}
		if m.name != want[i].name {
			t.Errorf("msg[%d]: name want %q, got %q", i, want[i].name, m.name)
		}
		if string(m.data) != string(want[i].data) {
			t.Errorf("msg[%d]: data want %q, got %q", i, want[i].data, m.data)
		}
	}
}

func TestNextIntoMultiTopicMerge(t *testing.T) {
	// Two topics in the same group; messages interleaved by timestamp.
	// The writer enforces non-decreasing timestamps across all topics in a group,
	// so messages are written in global timestamp order.
	r := writerRoundTrip(t, func(w *Writer) {
		mustOpenTopics(t, w, []string{"a", "b"}, []map[string]any{{}, {}})
		mustWriteMessage(t, w, "a", []byte("a1"), 10)
		mustWriteMessage(t, w, "b", []byte("b1"), 20)
		mustWriteMessage(t, w, "a", []byte("a2"), 30)
		mustWriteMessage(t, w, "b", []byte("b2"), 40)
		mustCloseTopic(t, w)
		mustClose(t, w)
	})

	msgs := collectWithOpts(t, r)
	if len(msgs) != 4 {
		t.Fatalf("expected 4 messages, got %d", len(msgs))
	}
	want := []testMsg{
		{10, "a", []byte("a1")},
		{20, "b", []byte("b1")},
		{30, "a", []byte("a2")},
		{40, "b", []byte("b2")},
	}
	for i, m := range msgs {
		if m.ts != want[i].ts {
			t.Errorf("msg[%d]: timestamp want %d, got %d", i, want[i].ts, m.ts)
		}
		if m.name != want[i].name {
			t.Errorf("msg[%d]: name want %q, got %q", i, want[i].name, m.name)
		}
		if string(m.data) != string(want[i].data) {
			t.Errorf("msg[%d]: data want %q, got %q", i, want[i].data, m.data)
		}
	}
}

func TestNextIntoReverseOrder(t *testing.T) {
	// Two topics in the same group, same setup as TestNextIntoMultiTopicMerge.
	// WithOrder(ReverseTimeOrder) should deliver messages in descending timestamp order.
	r := writerRoundTrip(t, func(w *Writer) {
		mustOpenTopics(t, w, []string{"a", "b"}, []map[string]any{{}, {}})
		mustWriteMessage(t, w, "a", []byte("a1"), 10)
		mustWriteMessage(t, w, "b", []byte("b1"), 20)
		mustWriteMessage(t, w, "a", []byte("a2"), 30)
		mustWriteMessage(t, w, "b", []byte("b2"), 40)
		mustCloseTopic(t, w)
		mustClose(t, w)
	})

	msgs := collectWithOpts(t, r, WithOrder(ReverseTimeOrder))
	if len(msgs) != 4 {
		t.Fatalf("expected 4 messages, got %d", len(msgs))
	}
	wantTs := []int64{40, 30, 20, 10}
	for i, m := range msgs {
		if m.ts != wantTs[i] {
			t.Errorf("msg[%d]: timestamp want %d, got %d", i, wantTs[i], m.ts)
		}
	}
}

func TestNextIntoTopicNameFilter(t *testing.T) {
	// Two separate groups (separate OpenTopics calls), each with one topic.
	// Using separate groups avoids per-message topic filtering within a shared chunk.
	r := writerRoundTrip(t, func(w *Writer) {
		mustOpenTopics(t, w, []string{"topic_a"}, []map[string]any{{}})
		mustWriteMessage(t, w, "topic_a", []byte("a1"), 10)
		mustWriteMessage(t, w, "topic_a", []byte("a2"), 20)
		mustCloseTopic(t, w)
		mustOpenTopics(t, w, []string{"topic_b"}, []map[string]any{{}})
		mustWriteMessage(t, w, "topic_b", []byte("b1"), 30)
		mustWriteMessage(t, w, "topic_b", []byte("b2"), 40)
		mustCloseTopic(t, w)
		mustClose(t, w)
	})

	msgs := collectWithOpts(t, r, WithTopicNames([]string{"topic_a"}))
	if len(msgs) != 2 {
		t.Fatalf("expected 2 messages for topic_a, got %d", len(msgs))
	}
	for _, m := range msgs {
		if m.name != "topic_a" {
			t.Errorf("expected only topic_a messages, got name %q", m.name)
		}
	}
}

func TestNextIntoTimestampRange(t *testing.T) {
	// One message per chunk (Count=1) so the chunk-level filter skips ts=10 and
	// ts=50 entirely, and the three remaining chunks are each read individually.
	chunkCfg := &ChunkConfig{Mode: ChunkThresholdModeCount, Count: 1}
	r := writerRoundTrip(t, func(w *Writer) {
		mustOpenTopics(t, w, []string{"t"}, []map[string]any{{}}, WithChunkConfig(chunkCfg))
		mustWriteMessage(t, w, "t", []byte("m10"), 10)
		mustWriteMessage(t, w, "t", []byte("m20"), 20)
		mustWriteMessage(t, w, "t", []byte("m30"), 30)
		mustWriteMessage(t, w, "t", []byte("m40"), 40)
		mustWriteMessage(t, w, "t", []byte("m50"), 50)
		mustCloseTopic(t, w)
		mustClose(t, w)
	})

	msgs := collectWithOpts(t, r, WithStartTimestamp(20), WithEndTimestamp(40))
	if len(msgs) != 3 {
		t.Fatalf("expected 3 messages (ts=20,30,40), got %d", len(msgs))
	}
	wantTs := []int64{20, 30, 40}
	for i, m := range msgs {
		if m.ts != wantTs[i] {
			t.Errorf("msg[%d]: timestamp want %d, got %d", i, wantTs[i], m.ts)
		}
	}
}

func TestNextIntoMultipleChunks(t *testing.T) {
	// Force a new chunk per message; verify all 5 messages are recovered in order.
	chunkCfg := &ChunkConfig{Mode: ChunkThresholdModeCount, Count: 1}
	r := writerRoundTrip(t, func(w *Writer) {
		mustOpenTopics(t, w, []string{"t"}, []map[string]any{{}}, WithChunkConfig(chunkCfg))
		for i := int64(0); i < 5; i++ {
			mustWriteMessage(t, w, "t", []byte{byte(i)}, i*10)
		}
		mustCloseTopic(t, w)
		mustClose(t, w)
	})

	msgs := collectWithOpts(t, r)
	if len(msgs) != 5 {
		t.Fatalf("expected 5 messages, got %d", len(msgs))
	}
	for i, m := range msgs {
		if m.ts != int64(i)*10 {
			t.Errorf("msg[%d]: timestamp want %d, got %d", i, int64(i)*10, m.ts)
		}
		if m.data[0] != byte(i) {
			t.Errorf("msg[%d]: data want %d, got %d", i, i, m.data[0])
		}
	}
}

func TestNextIntoMultiGroupMerge(t *testing.T) {
	// Two separate topic groups with interleaved timestamps.
	// Verifies that MessageIterator's top-level heap merges across groups.
	r := writerRoundTrip(t, func(w *Writer) {
		mustOpenTopics(t, w, []string{"x"}, []map[string]any{{}})
		mustWriteMessage(t, w, "x", []byte("x10"), 10)
		mustWriteMessage(t, w, "x", []byte("x30"), 30)
		mustWriteMessage(t, w, "x", []byte("x50"), 50)
		mustCloseTopic(t, w)
		mustOpenTopics(t, w, []string{"y"}, []map[string]any{{}})
		mustWriteMessage(t, w, "y", []byte("y20"), 20)
		mustWriteMessage(t, w, "y", []byte("y40"), 40)
		mustCloseTopic(t, w)
		mustClose(t, w)
	})

	msgs := collectWithOpts(t, r)
	if len(msgs) != 5 {
		t.Fatalf("expected 5 messages, got %d", len(msgs))
	}
	wantTs := []int64{10, 20, 30, 40, 50}
	wantNames := []string{"x", "y", "x", "y", "x"}
	for i, m := range msgs {
		if m.ts != wantTs[i] {
			t.Errorf("msg[%d]: timestamp want %d, got %d", i, wantTs[i], m.ts)
		}
		if m.name != wantNames[i] {
			t.Errorf("msg[%d]: name want %q, got %q", i, wantNames[i], m.name)
		}
	}
}
