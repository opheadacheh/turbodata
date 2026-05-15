package turbodata

import (
	"bytes"
	"fmt"
	"io"
	"testing"
)

func TestMessageIterator(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	writer := NewWriter(buf)
	writer.OpenTopic(1, "test", map[string]any{})
	writer.WriteMessage("test", []byte("test"), 1, false)
	writer.WriteMessage("test", []byte("test2"), 2, false)
	writer.WriteMessage("test", []byte("test3"), 3, false)
	writer.WriteMessage("test", []byte("test4"), 4, false)
	writer.CloseTopic()
	writer.OpenTopic(2, "testTopic", map[string]any{})
	writer.WriteMessage("testTopic", []byte("test"), 1, false)
	writer.WriteMessage("testTopic", []byte("test2"), 2, false)
	writer.WriteMessage("testTopic", []byte("test3"), 3, false)
	writer.WriteMessage("testTopic", []byte("test4"), 4, false)
	writer.CloseTopic()
	writer.Close()

	reader, err := NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("failed to create reader: %v", err)
	}

	it, err := reader.ReadMessages(WithOrder(TimeOrder))
	if err != nil {
		t.Fatalf("failed to create message iterator: %v", err)
	}

	for {
		data, timestamp, name, err := it.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("failed to read message: %v", err)
		}

		fmt.Println("got", string(data), timestamp, name)
	}
}
