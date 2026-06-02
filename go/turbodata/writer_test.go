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

package turbodata

import (
	"bytes"
	"errors"
	"testing"
)

// errWriter always returns a write error, used to test error propagation.
type errWriter struct{}

func (e *errWriter) Write(p []byte) (int, error) {
	return 0, errors.New("injected write error")
}

// mustOpenTopics calls OpenTopics and fatals on error.
func mustOpenTopics(t *testing.T, w *Writer, names []string, metadatas []map[string]any, opts ...WriteOption) {
	t.Helper()
	if err := w.OpenTopics(names, metadatas, opts...); err != nil {
		t.Fatalf("OpenTopics: %v", err)
	}
}

// mustWriteMessage calls WriteMessage and fatals on error.
func mustWriteMessage(t *testing.T, w *Writer, name string, data []byte, ts int64) {
	t.Helper()
	if err := w.WriteMessage(name, data, ts); err != nil {
		t.Fatalf("WriteMessage: %v", err)
	}
}

// mustCloseTopic calls CloseTopic and fatals on error.
func mustCloseTopic(t *testing.T, w *Writer) {
	t.Helper()
	if err := w.CloseTopic(); err != nil {
		t.Fatalf("CloseTopic: %v", err)
	}
}

// mustClose calls Close and fatals on error.
func mustClose(t *testing.T, w *Writer) {
	t.Helper()
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// ---- NewWriter ----

func TestNewWriter(t *testing.T) {
	t.Run("returns_non_nil", func(t *testing.T) {
		if w := NewWriter(&bytes.Buffer{}); w == nil {
			t.Fatal("expected non-nil writer")
		}
	})

	t.Run("footer_magic_initialized", func(t *testing.T) {
		w := NewWriter(&bytes.Buffer{})
		want := [5]byte{'7', 'U', 'R', 'B', '0'}
		if w.footer.Magic != want {
			t.Errorf("footer.Magic = %v, want %v", w.footer.Magic, want)
		}
	})

	t.Run("internal_buffers_initialized", func(t *testing.T) {
		w := NewWriter(&bytes.Buffer{})
		if w.buf == nil {
			t.Error("buf is nil")
		}
		if w.compressBuf == nil {
			t.Error("compressBuf is nil")
		}
	})
}

// ---- OpenTopics ----

func TestOpenTopics(t *testing.T) {
	t.Run("error_when_already_open", func(t *testing.T) {
		w := NewWriter(&bytes.Buffer{})
		mustOpenTopics(t, w, []string{"a"}, []map[string]any{{}})
		err := w.OpenTopics([]string{"b"}, []map[string]any{{}})
		if !errors.Is(err, ErrTopicAlreadyOpen) {
			t.Errorf("expected ErrTopicAlreadyOpen, got %v", err)
		}
	})

	t.Run("error_names_metadatas_length_mismatch", func(t *testing.T) {
		w := NewWriter(&bytes.Buffer{})
		err := w.OpenTopics([]string{"a", "b"}, []map[string]any{{}})
		if !errors.Is(err, ErrNamesMetadatasMismatch) {
			t.Errorf("expected ErrNamesMetadatasMismatch, got %v", err)
		}
	})

	t.Run("error_no_topics", func(t *testing.T) {
		w := NewWriter(&bytes.Buffer{})
		err := w.OpenTopics([]string{}, []map[string]any{})
		if !errors.Is(err, ErrNoTopicsToOpen) {
			t.Errorf("expected ErrNoTopicsToOpen, got %v", err)
		}
	})

	t.Run("success_single_topic_default_chunk_config", func(t *testing.T) {
		w := NewWriter(&bytes.Buffer{})
		if err := w.OpenTopics([]string{"t1"}, []map[string]any{{}}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !w.isTopicOpen {
			t.Error("isTopicOpen should be true")
		}
		if w.writerConfig.chunkConfig.Mode != ChunkThresholdModeSize {
			t.Errorf("default mode should be size, got %v", w.writerConfig.chunkConfig.Mode)
		}
		if w.writerConfig.chunkConfig.Size != 1024*1024 {
			t.Errorf("default size should be 1 MiB, got %d", w.writerConfig.chunkConfig.Size)
		}
	})

	t.Run("success_multiple_topics", func(t *testing.T) {
		w := NewWriter(&bytes.Buffer{})
		if err := w.OpenTopics([]string{"a", "b"}, []map[string]any{{}, {}}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(w.writerConfig.topicIds) != 2 {
			t.Errorf("expected 2 topic IDs, got %d", len(w.writerConfig.topicIds))
		}
	})

	t.Run("write_option_applied_to_all_metadatas", func(t *testing.T) {
		cfg := &ChunkConfig{Mode: ChunkThresholdModeCount, Count: 5}
		w := NewWriter(&bytes.Buffer{})
		if err := w.OpenTopics([]string{"a", "b"}, []map[string]any{{}, {}}, WithChunkConfig(cfg)); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if w.writerConfig.chunkConfig != cfg {
			t.Error("custom chunk config not applied")
		}
	})

	t.Run("write_option_error_propagates", func(t *testing.T) {
		optErr := errors.New("option error")
		badOpt := WriteOption(func(m map[string]any) error { return optErr })
		w := NewWriter(&bytes.Buffer{})
		err := w.OpenTopics([]string{"a"}, []map[string]any{{}}, badOpt)
		if !errors.Is(err, optErr) {
			t.Errorf("expected option error, got %v", err)
		}
	})

	t.Run("with_compression_sets_compressed", func(t *testing.T) {
		w := NewWriter(&bytes.Buffer{})
		if err := w.OpenTopics([]string{"t"}, []map[string]any{{}}, WithCompression()); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !w.writerConfig.isCompressed {
			t.Error("expected isCompressed to be true")
		}
	})

	t.Run("topic_ids_increment_across_cycles", func(t *testing.T) {
		w := NewWriter(&bytes.Buffer{})
		mustOpenTopics(t, w, []string{"t1"}, []map[string]any{{}})
		firstId := w.currentTopicId
		mustCloseTopic(t, w)
		mustOpenTopics(t, w, []string{"t2"}, []map[string]any{{}})
		secondId := w.currentTopicId
		if secondId <= firstId {
			t.Errorf("expected topic ID to increment: first=%d second=%d", firstId, secondId)
		}
	})

	t.Run("last_timestamp_reset_on_open", func(t *testing.T) {
		w := NewWriter(&bytes.Buffer{})
		mustOpenTopics(t, w, []string{"t1"}, []map[string]any{{}})
		mustWriteMessage(t, w, "t1", []byte("a"), 100)
		mustCloseTopic(t, w)
		mustOpenTopics(t, w, []string{"t2"}, []map[string]any{{}})
		if w.lastTimestamp != 0 {
			t.Errorf("lastTimestamp should be reset to 0, got %d", w.lastTimestamp)
		}
	})
}

// ---- WriteMessage ----

func TestWriteMessage(t *testing.T) {
	t.Run("error_when_topic_not_open", func(t *testing.T) {
		w := NewWriter(&bytes.Buffer{})
		err := w.WriteMessage("t", []byte("hi"), 1)
		if !errors.Is(err, ErrTopicNotOpened) {
			t.Errorf("expected ErrTopicNotOpened, got %v", err)
		}
	})

	t.Run("error_when_timestamp_decreases", func(t *testing.T) {
		w := NewWriter(&bytes.Buffer{})
		mustOpenTopics(t, w, []string{"t"}, []map[string]any{{}})
		mustWriteMessage(t, w, "t", []byte("a"), 10)
		err := w.WriteMessage("t", []byte("b"), 5)
		if !errors.Is(err, ErrTimestampDecreases) {
			t.Errorf("expected ErrTimestampDecreases, got %v", err)
		}
	})

	t.Run("equal_timestamp_is_allowed", func(t *testing.T) {
		w := NewWriter(&bytes.Buffer{})
		mustOpenTopics(t, w, []string{"t"}, []map[string]any{{}})
		mustWriteMessage(t, w, "t", []byte("a"), 10)
		if err := w.WriteMessage("t", []byte("b"), 10); err != nil {
			t.Errorf("equal timestamp should be allowed, got: %v", err)
		}
	})

	t.Run("error_unknown_topic_name", func(t *testing.T) {
		w := NewWriter(&bytes.Buffer{})
		mustOpenTopics(t, w, []string{"t"}, []map[string]any{{}})
		err := w.WriteMessage("unknown", []byte("data"), 1)
		if !errors.Is(err, ErrTopicNotRegistered) {
			t.Errorf("expected ErrTopicNotRegistered, got %v", err)
		}
	})

	t.Run("size_mode_no_flush_below_threshold", func(t *testing.T) {
		w := NewWriter(&bytes.Buffer{})
		cfg := &ChunkConfig{Mode: ChunkThresholdModeSize, Size: 100}
		mustOpenTopics(t, w, []string{"t"}, []map[string]any{{}}, WithChunkConfig(cfg))
		mustWriteMessage(t, w, "t", []byte("small"), 1)
		if len(w.indexChunks) != 0 {
			t.Errorf("expected no chunks flushed yet, got %d", len(w.indexChunks))
		}
	})

	t.Run("size_mode_flush_at_threshold", func(t *testing.T) {
		w := NewWriter(&bytes.Buffer{})
		cfg := &ChunkConfig{Mode: ChunkThresholdModeSize, Size: 5}
		mustOpenTopics(t, w, []string{"t"}, []map[string]any{{}}, WithChunkConfig(cfg))
		mustWriteMessage(t, w, "t", []byte("hello"), 1) // exactly 5 bytes == threshold
		if len(w.indexChunks) != 1 {
			t.Errorf("expected 1 chunk after threshold reached, got %d", len(w.indexChunks))
		}
	})

	t.Run("duration_mode_sets_start_timestamp_and_no_flush_below_threshold", func(t *testing.T) {
		w := NewWriter(&bytes.Buffer{})
		cfg := &ChunkConfig{Mode: ChunkThresholdModeDuration, Duration: 100}
		mustOpenTopics(t, w, []string{"t"}, []map[string]any{{}}, WithChunkConfig(cfg))
		mustWriteMessage(t, w, "t", []byte("a"), 0)
		if w.chunkStatus.startTimestamp != 0 {
			t.Errorf("startTimestamp should be 0, got %d", w.chunkStatus.startTimestamp)
		}
		mustWriteMessage(t, w, "t", []byte("b"), 50) // duration 50 < 100
		if len(w.indexChunks) != 0 {
			t.Errorf("expected no flush yet, got %d chunk(s)", len(w.indexChunks))
		}
	})

	t.Run("duration_mode_flush_at_threshold", func(t *testing.T) {
		w := NewWriter(&bytes.Buffer{})
		cfg := &ChunkConfig{Mode: ChunkThresholdModeDuration, Duration: 100}
		mustOpenTopics(t, w, []string{"t"}, []map[string]any{{}}, WithChunkConfig(cfg))
		mustWriteMessage(t, w, "t", []byte("a"), 0)
		mustWriteMessage(t, w, "t", []byte("b"), 100) // 100-0 >= 100, flush
		if len(w.indexChunks) != 1 {
			t.Errorf("expected 1 chunk after duration threshold, got %d", len(w.indexChunks))
		}
	})

	t.Run("count_mode_no_flush_below_threshold", func(t *testing.T) {
		w := NewWriter(&bytes.Buffer{})
		cfg := &ChunkConfig{Mode: ChunkThresholdModeCount, Count: 3}
		mustOpenTopics(t, w, []string{"t"}, []map[string]any{{}}, WithChunkConfig(cfg))
		mustWriteMessage(t, w, "t", []byte("a"), 1)
		mustWriteMessage(t, w, "t", []byte("b"), 2)
		if len(w.indexChunks) != 0 {
			t.Errorf("expected no flush yet, got %d chunk(s)", len(w.indexChunks))
		}
	})

	t.Run("count_mode_flush_at_threshold", func(t *testing.T) {
		w := NewWriter(&bytes.Buffer{})
		cfg := &ChunkConfig{Mode: ChunkThresholdModeCount, Count: 3}
		mustOpenTopics(t, w, []string{"t"}, []map[string]any{{}}, WithChunkConfig(cfg))
		mustWriteMessage(t, w, "t", []byte("a"), 1)
		mustWriteMessage(t, w, "t", []byte("b"), 2)
		mustWriteMessage(t, w, "t", []byte("c"), 3) // 3rd message hits threshold
		if len(w.indexChunks) != 1 {
			t.Errorf("expected 1 chunk after count threshold, got %d", len(w.indexChunks))
		}
	})

	t.Run("multi_topic_messages_routed_to_correct_topic_indexes", func(t *testing.T) {
		// Count=3 forces a flush after writing 2 messages to "a" and 1 to "b".
		cfg := &ChunkConfig{Mode: ChunkThresholdModeCount, Count: 3}
		w := NewWriter(&bytes.Buffer{})
		mustOpenTopics(t, w, []string{"a", "b"}, []map[string]any{{}, {}}, WithChunkConfig(cfg))
		mustWriteMessage(t, w, "a", []byte("a1"), 1)
		mustWriteMessage(t, w, "b", []byte("b1"), 2)
		mustWriteMessage(t, w, "a", []byte("a2"), 3) // triggers flush
		if len(w.indexChunks) != 1 {
			t.Fatalf("expected 1 chunk, got %d", len(w.indexChunks))
		}
		chunk := w.indexChunks[0]
		idA := w.writerConfig.namesToIds["a"]
		idB := w.writerConfig.namesToIds["b"]
		counts := map[uint16]int{}
		for _, ti := range chunk.TopicIndexes {
			counts[ti.Id] = len(ti.MessageIndexes)
		}
		if counts[idA] != 2 {
			t.Errorf("topic a: expected 2 message indexes, got %d", counts[idA])
		}
		if counts[idB] != 1 {
			t.Errorf("topic b: expected 1 message index, got %d", counts[idB])
		}
	})

	t.Run("chunk_state_reset_after_flush", func(t *testing.T) {
		cfg := &ChunkConfig{Mode: ChunkThresholdModeCount, Count: 2}
		w := NewWriter(&bytes.Buffer{})
		mustOpenTopics(t, w, []string{"t"}, []map[string]any{{}}, WithChunkConfig(cfg))
		mustWriteMessage(t, w, "t", []byte("a"), 1)
		mustWriteMessage(t, w, "t", []byte("b"), 2) // triggers flush
		if w.chunkStatus.count != 0 {
			t.Errorf("count should be 0 after flush, got %d", w.chunkStatus.count)
		}
		if w.chunkStatus.size != 0 {
			t.Errorf("size should be 0 after flush, got %d", w.chunkStatus.size)
		}
		if w.chunkStatus.startTimestamp != -1 {
			t.Errorf("startTimestamp should be -1 after flush, got %d", w.chunkStatus.startTimestamp)
		}
	})
}

// ---- CloseTopic ----

func TestCloseTopic(t *testing.T) {
	t.Run("error_when_already_closed", func(t *testing.T) {
		w := NewWriter(&bytes.Buffer{})
		err := w.CloseTopic()
		if !errors.Is(err, ErrTopicAlreadyClosed) {
			t.Errorf("expected ErrTopicAlreadyClosed, got %v", err)
		}
	})

	t.Run("flushes_remaining_buffer_as_chunk", func(t *testing.T) {
		w := NewWriter(&bytes.Buffer{})
		// Large threshold so no auto-flush during WriteMessage.
		cfg := &ChunkConfig{Mode: ChunkThresholdModeSize, Size: 1024 * 1024}
		mustOpenTopics(t, w, []string{"t"}, []map[string]any{{}}, WithChunkConfig(cfg))
		mustWriteMessage(t, w, "t", []byte("leftover"), 1)
		mustCloseTopic(t, w)
		if len(w.indexChunksList) != 1 || len(w.indexChunksList[0]) != 1 {
			t.Errorf("expected 1 chunk flushed at close, got indexChunksList=%v", w.indexChunksList)
		}
	})

	t.Run("no_extra_flush_when_buffer_already_empty", func(t *testing.T) {
		w := NewWriter(&bytes.Buffer{})
		// Threshold of 1 byte forces an immediate flush after each message.
		cfg := &ChunkConfig{Mode: ChunkThresholdModeSize, Size: 1}
		mustOpenTopics(t, w, []string{"t"}, []map[string]any{{}}, WithChunkConfig(cfg))
		mustWriteMessage(t, w, "t", []byte("x"), 1) // auto-flushes
		if w.buf.Len() != 0 {
			t.Fatal("expected empty buf after auto-flush")
		}
		mustCloseTopic(t, w)
		// Exactly 1 chunk (from the auto-flush), not 2.
		if len(w.indexChunksList[0]) != 1 {
			t.Errorf("expected 1 chunk, got %d", len(w.indexChunksList[0]))
		}
	})

	t.Run("appends_to_index_chunks_list_and_resets_index_chunks", func(t *testing.T) {
		w := NewWriter(&bytes.Buffer{})
		mustOpenTopics(t, w, []string{"t"}, []map[string]any{{}})
		mustWriteMessage(t, w, "t", []byte("a"), 1)
		mustCloseTopic(t, w)
		if len(w.indexChunksList) != 1 {
			t.Errorf("expected 1 entry in indexChunksList, got %d", len(w.indexChunksList))
		}
		if len(w.indexChunks) != 0 {
			t.Errorf("indexChunks should be reset to empty, len=%d", len(w.indexChunks))
		}
	})
}

// ---- Close ----

func TestClose(t *testing.T) {
	t.Run("error_when_topic_still_open", func(t *testing.T) {
		w := NewWriter(&bytes.Buffer{})
		mustOpenTopics(t, w, []string{"t"}, []map[string]any{{}})
		err := w.Close()
		if !errors.Is(err, ErrTopicNotClosed) {
			t.Errorf("expected ErrTopicNotClosed, got %v", err)
		}
	})

	t.Run("write_error_propagates_from_underlying_writer", func(t *testing.T) {
		// errWriter always fails. The bufio layer buffers writes but errors on Flush.
		w := NewWriter(&errWriter{})
		mustOpenTopics(t, w, []string{"t"}, []map[string]any{{}})
		_ = w.WriteMessage("t", []byte("data"), 1)
		_ = w.CloseTopic()
		if err := w.Close(); err == nil {
			t.Error("expected error from failing underlying writer, got nil")
		}
	})

	t.Run("successful_close_produces_valid_file", func(t *testing.T) {
		buf := &bytes.Buffer{}
		w := NewWriter(buf)
		mustOpenTopics(t, w, []string{"t"}, []map[string]any{{}})
		mustWriteMessage(t, w, "t", []byte("msg"), 42)
		mustCloseTopic(t, w)
		mustClose(t, w)
		if buf.Len() == 0 {
			t.Error("expected non-empty output after Close")
		}
	})
}

// ---- Integration / round-trip ----

func TestRoundTrip(t *testing.T) {
	t.Run("single_topic_single_message_uncompressed", func(t *testing.T) {
		payload := []byte("hello world")
		r := writerRoundTrip(t, func(w *Writer) {
			mustOpenTopics(t, w, []string{"cam"}, []map[string]any{{}})
			mustWriteMessage(t, w, "cam", payload, 42)
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
		if summary.TopicsInfos[0].TopicMetadatas[0].Name != "cam" {
			t.Errorf("topic name mismatch: got %q", summary.TopicsInfos[0].TopicMetadatas[0].Name)
		}

		msgs := collect(t, r)
		if len(msgs) != 1 {
			t.Fatalf("expected 1 message, got %d", len(msgs))
		}
		if msgs[0].ts != 42 {
			t.Errorf("timestamp: want 42, got %d", msgs[0].ts)
		}
		if string(msgs[0].data) != string(payload) {
			t.Errorf("data: want %q, got %q", payload, msgs[0].data)
		}
	})

	t.Run("size_based_chunk_boundary", func(t *testing.T) {
		cfg := &ChunkConfig{Mode: ChunkThresholdModeSize, Size: 5}
		r := writerRoundTrip(t, func(w *Writer) {
			mustOpenTopics(t, w, []string{"t"}, []map[string]any{{}}, WithChunkConfig(cfg))
			mustWriteMessage(t, w, "t", []byte("hello"), 1) // flushes at 5 bytes
			mustWriteMessage(t, w, "t", []byte("world"), 2) // flushes at 5 bytes
			mustWriteMessage(t, w, "t", []byte("!"), 3)     // leftover, flushed by CloseTopic
			mustCloseTopic(t, w)
			mustClose(t, w)
		})

		msgs := collect(t, r)
		if len(msgs) != 3 {
			t.Fatalf("expected 3 messages, got %d", len(msgs))
		}
		want := []string{"hello", "world", "!"}
		for i, m := range msgs {
			if string(m.data) != want[i] {
				t.Errorf("msg[%d]: want %q, got %q", i, want[i], m.data)
			}
		}
	})

	t.Run("duration_based_chunk_boundary", func(t *testing.T) {
		cfg := &ChunkConfig{Mode: ChunkThresholdModeDuration, Duration: 10}
		r := writerRoundTrip(t, func(w *Writer) {
			mustOpenTopics(t, w, []string{"t"}, []map[string]any{{}}, WithChunkConfig(cfg))
			mustWriteMessage(t, w, "t", []byte("a"), 0)
			mustWriteMessage(t, w, "t", []byte("b"), 10) // 10-0 >= 10, flushes
			mustWriteMessage(t, w, "t", []byte("c"), 15) // leftover
			mustCloseTopic(t, w)
			mustClose(t, w)
		})

		msgs := collect(t, r)
		if len(msgs) != 3 {
			t.Fatalf("expected 3 messages, got %d", len(msgs))
		}
		want := []string{"a", "b", "c"}
		for i, m := range msgs {
			if string(m.data) != want[i] {
				t.Errorf("msg[%d]: want %q, got %q", i, want[i], m.data)
			}
		}
	})

	t.Run("count_based_chunk_boundary", func(t *testing.T) {
		cfg := &ChunkConfig{Mode: ChunkThresholdModeCount, Count: 2}
		r := writerRoundTrip(t, func(w *Writer) {
			mustOpenTopics(t, w, []string{"t"}, []map[string]any{{}}, WithChunkConfig(cfg))
			mustWriteMessage(t, w, "t", []byte("a"), 1)
			mustWriteMessage(t, w, "t", []byte("b"), 2) // flushes at count 2
			mustWriteMessage(t, w, "t", []byte("c"), 3) // leftover
			mustCloseTopic(t, w)
			mustClose(t, w)
		})

		msgs := collect(t, r)
		if len(msgs) != 3 {
			t.Fatalf("expected 3 messages, got %d", len(msgs))
		}
		want := []string{"a", "b", "c"}
		for i, m := range msgs {
			if string(m.data) != want[i] {
				t.Errorf("msg[%d]: want %q, got %q", i, want[i], m.data)
			}
		}
	})

	t.Run("multiple_topics_in_single_open_call", func(t *testing.T) {
		r := writerRoundTrip(t, func(w *Writer) {
			mustOpenTopics(t, w, []string{"a", "b"}, []map[string]any{{}, {}})
			mustWriteMessage(t, w, "a", []byte("msg-a"), 1)
			mustWriteMessage(t, w, "b", []byte("msg-b"), 2)
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
		if len(summary.TopicsInfos[0].TopicMetadatas) != 2 {
			t.Fatalf("expected 2 topic metadatas, got %d", len(summary.TopicsInfos[0].TopicMetadatas))
		}

		msgs := collect(t, r)
		if len(msgs) != 2 {
			t.Fatalf("expected 2 messages, got %d", len(msgs))
		}
	})

	t.Run("two_successive_open_close_cycles_produce_two_topics_infos", func(t *testing.T) {
		r := writerRoundTrip(t, func(w *Writer) {
			mustOpenTopics(t, w, []string{"first"}, []map[string]any{{}})
			mustWriteMessage(t, w, "first", []byte("d1"), 1)
			mustCloseTopic(t, w)
			mustOpenTopics(t, w, []string{"second"}, []map[string]any{{}})
			mustWriteMessage(t, w, "second", []byte("d2"), 1)
			mustCloseTopic(t, w)
			mustClose(t, w)
		})

		summary, err := r.Summary()
		if err != nil {
			t.Fatalf("Summary: %v", err)
		}
		if len(summary.TopicsInfos) != 2 {
			t.Fatalf("expected 2 TopicsInfos, got %d", len(summary.TopicsInfos))
		}
	})

	t.Run("with_compression_data_roundtrips_correctly", func(t *testing.T) {
		payload := []byte("compressed payload content")
		r := writerRoundTrip(t, func(w *Writer) {
			mustOpenTopics(t, w, []string{"c"}, []map[string]any{{}}, WithCompression())
			mustWriteMessage(t, w, "c", payload, 99)
			mustCloseTopic(t, w)
			mustClose(t, w)
		})

		msgs := collect(t, r)
		if len(msgs) != 1 {
			t.Fatalf("expected 1 message, got %d", len(msgs))
		}
		if string(msgs[0].data) != string(payload) {
			t.Errorf("data mismatch: want %q, got %q", payload, msgs[0].data)
		}
	})
}
