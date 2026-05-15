package turbodata

import (
	"bytes"
	"testing"
)

func TestWriter(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	writer := NewWriter(buf)
	writer.OpenTopics([]string{"test"}, []map[string]any{{"foo": "bar"}})
	writer.WriteMessage("test", []byte("test"), 1)
	writer.WriteMessage("test", []byte("test2"), 2)
	writer.WriteMessage("test", []byte("test3"), 3)
	writer.WriteMessage("test", []byte("test4"), 4)
	writer.CloseTopic()
	writer.Close()

	_, err := NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("failed to create reader: %v", err)
	}
}
