// Copyright 2026 Wanjia He
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package iter_test

import (
	"testing"

	"github.com/opheadacheh/turbodata/go/turbodata"
)

// TestTopicsGroupIterator_IntraGroupTopicFilter verifies the topicIds filter inside
// sortAndFilter. Unlike TestNextIntoTopicNameFilter (which puts each
// topic in its own separate group), this test places both topics in the same group
// so the per-message topic filter is actually exercised.
func TestTopicsGroupIterator_IntraGroupTopicFilter(t *testing.T) {
	r := writerRoundTrip(t, func(w *turbodata.Writer) {
		mustOpenTopics(t, w, []string{"a", "b"}, []map[string]any{{}, {}})
		mustWriteMessage(t, w, "a", []byte("a1"), 10)
		mustWriteMessage(t, w, "b", []byte("b1"), 20)
		mustWriteMessage(t, w, "a", []byte("a2"), 30)
		mustWriteMessage(t, w, "b", []byte("b2"), 40)
		mustCloseTopic(t, w)
		mustClose(t, w)
	})

	msgs := collect(t, r, turbodata.WithTopicNames([]string{"a"}))
	if len(msgs) != 2 {
		t.Fatalf("expected 2 messages for topic a, got %d", len(msgs))
	}
	want := []testMsg{
		{10, "a", []byte("a1")},
		{30, "a", []byte("a2")},
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

// TestTopicsGroupIterator_PerMessageTimestampFilter verifies the per-message timestamp
// checks inside sortAndFilter. TestNextIntoTimestampRange uses one message
// per chunk so the chunk-level filter handles everything; here all messages land in a
// single chunk so the boundary messages must be dropped at the message level.
func TestTopicsGroupIterator_PerMessageTimestampFilter(t *testing.T) {
	r := writerRoundTrip(t, func(w *turbodata.Writer) {
		mustOpenTopics(t, w, []string{"t"}, []map[string]any{{}})
		mustWriteMessage(t, w, "t", []byte("m5"), 5)
		mustWriteMessage(t, w, "t", []byte("m10"), 10)
		mustWriteMessage(t, w, "t", []byte("m20"), 20)
		mustWriteMessage(t, w, "t", []byte("m25"), 25)
		mustCloseTopic(t, w)
		mustClose(t, w)
	})

	msgs := collect(t, r, turbodata.WithStartTimestamp(10), turbodata.WithEndTimestamp(20))
	if len(msgs) != 2 {
		t.Fatalf("expected 2 messages (ts=10,20), got %d", len(msgs))
	}
	wantTs := []int64{10, 20}
	for i, m := range msgs {
		if m.ts != wantTs[i] {
			t.Errorf("msg[%d]: timestamp want %d, got %d", i, wantTs[i], m.ts)
		}
	}
}

// TestTopicsGroupIterator_EmptyChunkAfterFilter verifies that Next() correctly skips
// a chunk whose messages are all filtered out and continues to the next chunk.
// No existing test creates this scenario (prior tests either have 1 msg per chunk or
// use range filters that the chunk-level filter in newTopicsGroupIterator already handles).
func TestTopicsGroupIterator_EmptyChunkAfterFilter(t *testing.T) {
	chunkCfg := &turbodata.ChunkConfig{Mode: turbodata.ChunkThresholdModeCount, Count: 2}
	r := writerRoundTrip(t, func(w *turbodata.Writer) {
		mustOpenTopics(t, w, []string{"t"}, []map[string]any{{}}, turbodata.WithChunkConfig(chunkCfg))
		mustWriteMessage(t, w, "t", []byte("m1"), 1)
		mustWriteMessage(t, w, "t", []byte("m2"), 2)
		mustWriteMessage(t, w, "t", []byte("m100"), 100)
		mustWriteMessage(t, w, "t", []byte("m200"), 200)
		mustCloseTopic(t, w)
		mustClose(t, w)
	})

	msgs := collect(t, r, turbodata.WithStartTimestamp(100))
	if len(msgs) != 2 {
		t.Fatalf("expected 2 messages (ts=100,200), got %d", len(msgs))
	}
	wantTs := []int64{100, 200}
	for i, m := range msgs {
		if m.ts != wantTs[i] {
			t.Errorf("msg[%d]: timestamp want %d, got %d", i, wantTs[i], m.ts)
		}
	}
}

// TestTopicsGroupIterator_CompressedData verifies the loadDataChunk compressed path.
// WithCompression() sets is_compressed=true in the topic metadata, causing loadDataChunk
// to decompress the data chunk before reading messages from it.
func TestTopicsGroupIterator_CompressedData(t *testing.T) {
	r := writerRoundTrip(t, func(w *turbodata.Writer) {
		mustOpenTopics(t, w, []string{"t"}, []map[string]any{{}}, turbodata.WithCompression())
		mustWriteMessage(t, w, "t", []byte("hello"), 10)
		mustWriteMessage(t, w, "t", []byte("world"), 20)
		mustWriteMessage(t, w, "t", []byte("compressed"), 30)
		mustCloseTopic(t, w)
		mustClose(t, w)
	})

	msgs := collect(t, r)
	if len(msgs) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(msgs))
	}
	want := []testMsg{
		{10, "t", []byte("hello")},
		{20, "t", []byte("world")},
		{30, "t", []byte("compressed")},
	}
	for i, m := range msgs {
		if m.ts != want[i].ts {
			t.Errorf("msg[%d]: timestamp want %d, got %d", i, want[i].ts, m.ts)
		}
		if string(m.data) != string(want[i].data) {
			t.Errorf("msg[%d]: data want %q, got %q", i, want[i].data, m.data)
		}
	}
}
