package turbodata

import (
	"bytes"
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func writeAndSummarize(t *testing.T, setup func(*Writer) error, write func(*Writer) error) *Summary {
	t.Helper()
	buf := bytes.NewBuffer(nil)
	writer := NewWriter(buf)
	if err := setup(writer); err != nil {
		t.Fatalf("setup: %v", err)
	}
	if write != nil {
		if err := write(writer); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	if err := writer.CloseTopic(); err != nil {
		t.Fatalf("close topic: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	reader, err := NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("create reader: %v", err)
	}
	summary, err := reader.Summary()
	if err != nil {
		t.Fatalf("get summary: %v", err)
	}
	return summary
}

func write100ToTest(w *Writer) error {
	for i := 0; i < 100; i++ {
		if err := w.WriteMessage("test", []byte("test"), int64(i)); err != nil {
			return err
		}
	}
	return nil
}

var fourChunkIndexChunkInfoList = []*IndexChunkInfo{
	{StartTimestamp: 0, EndTimestamp: 24, Offset: 400},
	{StartTimestamp: 25, EndTimestamp: 49, Offset: 521},
	{StartTimestamp: 50, EndTimestamp: 74, Offset: 651},
	{StartTimestamp: 75, EndTimestamp: 99, Offset: 798},
}

func TestWriterBasic(t *testing.T) {
	summary := writeAndSummarize(t,
		func(w *Writer) error {
			return w.OpenTopics([]string{"test"}, []map[string]any{{"foo": "bar"}})
		},
		write100ToTest,
	)

	wanted := &Summary{
		TopicsInfos: []*TopicsInfo{
			{
				TopicMetadatas: []*TopicMetadata{
					{Id: 1, Name: "test", Metadata: map[string]any{"foo": "bar"}},
				},
				IndexChunkInfoList: []*IndexChunkInfo{
					{StartTimestamp: 0, EndTimestamp: 99, Offset: 400},
				},
				TotalLen: 408,
			},
		},
	}

	if diff := cmp.Diff(summary, wanted); diff != "" {
		t.Fatalf("summary mismatch: %s", diff)
	}
}

func TestWriterWithCompression(t *testing.T) {
	summary := writeAndSummarize(t,
		func(w *Writer) error {
			return w.OpenTopics([]string{"test"}, []map[string]any{{"foo": "bar"}}, WithCompression())
		},
		write100ToTest,
	)

	wanted := &Summary{
		TopicsInfos: []*TopicsInfo{
			{
				TopicMetadatas: []*TopicMetadata{
					{Id: 1, Name: "test", Metadata: map[string]any{"foo": "bar", "is_compressed": true}},
				},
				IndexChunkInfoList: []*IndexChunkInfo{
					{StartTimestamp: 0, EndTimestamp: 99, Offset: 27},
				},
				TotalLen: 406,
			},
		},
	}

	if diff := cmp.Diff(summary, wanted); diff != "" {
		t.Fatalf("summary mismatch: %s", diff)
	}
}

func TestWriterChunking(t *testing.T) {
	tests := []struct {
		name      string
		chunkCfg  *ChunkConfig
		chunkMeta map[string]any
	}{
		{
			name:     "size",
			chunkCfg: &ChunkConfig{Mode: ChunkThresholdModeSize, Size: 100},
			chunkMeta: map[string]any{
				"Mode": uint8(0), "Size": int64(100), "Duration": int64(0), "Count": uint32(0),
			},
		},
		{
			name:     "count",
			chunkCfg: &ChunkConfig{Mode: ChunkThresholdModeCount, Count: 25},
			chunkMeta: map[string]any{
				"Mode": uint8(2), "Size": int64(0), "Duration": int64(0), "Count": uint32(25),
			},
		},
		{
			name:     "duration",
			chunkCfg: &ChunkConfig{Mode: ChunkThresholdModeDuration, Duration: 24},
			chunkMeta: map[string]any{
				"Mode": uint8(1), "Size": int64(0), "Duration": int64(24), "Count": uint32(0),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			summary := writeAndSummarize(t,
				func(w *Writer) error {
					return w.OpenTopics([]string{"test"}, []map[string]any{{"foo": "bar"}}, WithChunkConfig(tt.chunkCfg))
				},
				write100ToTest,
			)

			wanted := &Summary{
				TopicsInfos: []*TopicsInfo{
					{
						TopicMetadatas: []*TopicMetadata{
							{Id: 1, Name: "test", Metadata: map[string]any{
								"foo":          "bar",
								"chunk_config": tt.chunkMeta,
							}},
						},
						IndexChunkInfoList: fourChunkIndexChunkInfoList,
						TotalLen:           539,
					},
				},
			}

			if diff := cmp.Diff(summary, wanted); diff != "" {
				t.Fatalf("summary mismatch: %s", diff)
			}
		})
	}
}

func TestWriterWithMultipleTopics(t *testing.T) {
	summary := writeAndSummarize(t,
		func(w *Writer) error {
			return w.OpenTopics([]string{"test", "test2"}, []map[string]any{{"foo": "bar"}, {"foo": "bar2"}})
		},
		func(w *Writer) error {
			for i := 0; i < 100; i++ {
				if err := w.WriteMessage("test", []byte("test"), int64(i)); err != nil {
					return err
				}
				if err := w.WriteMessage("test2", []byte("test2"), int64(i)); err != nil {
					return err
				}
			}
			return nil
		},
	)

	wanted := &Summary{
		TopicsInfos: []*TopicsInfo{
			{
				TopicMetadatas: []*TopicMetadata{
					{Id: 1, Name: "test", Metadata: map[string]any{"foo": "bar"}},
					{Id: 2, Name: "test2", Metadata: map[string]any{"foo": "bar2"}},
				},
				IndexChunkInfoList: []*IndexChunkInfo{
					{StartTimestamp: 0, EndTimestamp: 99, Offset: 900},
				},
				TotalLen: 621,
			},
		},
	}
	if diff := cmp.Diff(summary, wanted); diff != "" {
		t.Fatalf("summary mismatch: %s", diff)
	}
}

func TestWriterWithEmptyTopic(t *testing.T) {
	summary := writeAndSummarize(t,
		func(w *Writer) error {
			return w.OpenTopics([]string{"test"}, []map[string]any{{"foo": "bar"}})
		},
		nil,
	)

	wanted := &Summary{
		TopicsInfos: []*TopicsInfo{
			{
				TopicMetadatas: []*TopicMetadata{
					{Id: 1, Name: "test", Metadata: map[string]any{"foo": "bar"}},
				},
				IndexChunkInfoList: []*IndexChunkInfo{},
			},
		},
	}
	if diff := cmp.Diff(summary, wanted); diff != "" {
		t.Fatalf("summary mismatch: %s", diff)
	}
}

func TestWriterOpenTopicsErrors(t *testing.T) {
	t.Run("topic_already_open", func(t *testing.T) {
		writer := NewWriter(bytes.NewBuffer(nil))
		if err := writer.OpenTopics([]string{"test"}, []map[string]any{{}}); err != nil {
			t.Fatalf("first OpenTopics: %v", err)
		}
		err := writer.OpenTopics([]string{"test2"}, []map[string]any{{}})
		if !errors.Is(err, ErrTopicAlreadyOpen) {
			t.Fatalf("expected ErrTopicAlreadyOpen, got: %v", err)
		}
	})

	t.Run("names_metadatas_mismatch", func(t *testing.T) {
		writer := NewWriter(bytes.NewBuffer(nil))
		err := writer.OpenTopics([]string{"a", "b"}, []map[string]any{{}})
		if !errors.Is(err, ErrNamesMetadatasMismatch) {
			t.Fatalf("expected ErrNamesMetadatasMismatch, got: %v", err)
		}
	})

	t.Run("no_topics", func(t *testing.T) {
		writer := NewWriter(bytes.NewBuffer(nil))
		err := writer.OpenTopics(nil, nil)
		if !errors.Is(err, ErrNoTopicsToOpen) {
			t.Fatalf("expected ErrNoTopicsToOpen, got: %v", err)
		}
	})
}

func TestWriterWriteMessageErrors(t *testing.T) {
	t.Run("topic_not_opened", func(t *testing.T) {
		writer := NewWriter(bytes.NewBuffer(nil))
		err := writer.WriteMessage("test", []byte("x"), 0)
		if !errors.Is(err, ErrTopicNotOpened) {
			t.Fatalf("expected ErrTopicNotOpened, got: %v", err)
		}
	})

	t.Run("topic_not_registered", func(t *testing.T) {
		writer := NewWriter(bytes.NewBuffer(nil))
		if err := writer.OpenTopics([]string{"test"}, []map[string]any{{"foo": "bar"}}); err != nil {
			t.Fatalf("OpenTopics: %v", err)
		}
		err := writer.WriteMessage("test2", []byte("test2"), 0)
		if !errors.Is(err, ErrTopicNotRegistered) {
			t.Fatalf("expected ErrTopicNotRegistered, got: %v", err)
		}
	})

	t.Run("timestamp_decreases", func(t *testing.T) {
		writer := NewWriter(bytes.NewBuffer(nil))
		if err := writer.OpenTopics([]string{"test"}, []map[string]any{{"foo": "bar"}}); err != nil {
			t.Fatalf("OpenTopics: %v", err)
		}
		if err := writer.WriteMessage("test", []byte("test"), 10); err != nil {
			t.Fatalf("first WriteMessage: %v", err)
		}
		err := writer.WriteMessage("test", []byte("test"), 9)
		if !errors.Is(err, ErrTimestampDecreases) {
			t.Fatalf("expected ErrTimestampDecreases, got: %v", err)
		}
	})
}

func TestWriterCloseTopicErrors(t *testing.T) {
	writer := NewWriter(bytes.NewBuffer(nil))
	if err := writer.OpenTopics([]string{"test"}, []map[string]any{{"foo": "bar"}}); err != nil {
		t.Fatalf("OpenTopics: %v", err)
	}
	if err := writer.CloseTopic(); err != nil {
		t.Fatalf("first CloseTopic: %v", err)
	}
	err := writer.CloseTopic()
	if !errors.Is(err, ErrTopicAlreadyClosed) {
		t.Fatalf("expected ErrTopicAlreadyClosed, got: %v", err)
	}
}

func TestWriterCloseErrors(t *testing.T) {
	writer := NewWriter(bytes.NewBuffer(nil))
	if err := writer.OpenTopics([]string{"test"}, []map[string]any{{"foo": "bar"}}); err != nil {
		t.Fatalf("OpenTopics: %v", err)
	}
	err := writer.Close()
	if !errors.Is(err, ErrTopicNotClosed) {
		t.Fatalf("expected ErrTopicNotClosed, got: %v", err)
	}
}
