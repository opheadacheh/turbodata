package turbodata

import (
	"bytes"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestReader(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	writer := NewWriter(buf)
	writer.OpenTopics([]string{"test"}, []map[string]any{{"foo": "bar"}})
	writer.WriteMessage("test", []byte("test"), 1)
	writer.CloseTopic()
	writer.Close()

	reader, err := NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("failed to create reader: %v", err)
	}

	summary, err := reader.Summary()
	if err != nil {
		t.Fatalf("failed to get summary: %v", err)
	}

	wanted := &Summary{
		TopicsInfos: []*TopicsInfo{
			{
				TopicMetadatas: []*TopicMetadata{
					{Id: 1, Name: "test", Metadata: map[string]any{"foo": "bar"}},
				},
				IndexChunkInfoList: []*IndexChunkInfo{
					{StartTimestamp: 1, EndTimestamp: 1, Offset: 4},
				},
				TotalLen: 40,
			},
		},
	}

	if diff := cmp.Diff(summary, wanted); diff != "" {
		t.Fatalf("summary mismatch: %s", diff)
	}
}
