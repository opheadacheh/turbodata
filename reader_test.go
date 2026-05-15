package turbodata

import (
	"bytes"
	"testing"
)

func TestReader(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	writer := NewWriter(buf)
	writer.OpenTopic(1, "test", map[string]any{})
	writer.WriteMessage("test", []byte("test"), 1, false)
	writer.CloseTopic()
	writer.Close()

	_, err := NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("failed to create reader: %v", err)
	}
}
