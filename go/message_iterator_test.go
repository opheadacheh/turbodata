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
	writer.OpenTopics([]string{"test"}, []map[string]any{{"foo": "bar"}})
	writer.WriteMessage("test", []byte("test"), 1)
	writer.WriteMessage("test", []byte("test2"), 2)
	writer.WriteMessage("test", []byte("test3"), 3)
	writer.WriteMessage("test", []byte("test4"), 4)
	writer.CloseTopic()
	writer.OpenTopics([]string{"testTopic"}, []map[string]any{{"foo": "bar"}})
	writer.WriteMessage("testTopic", []byte("test"), 1)
	writer.WriteMessage("testTopic", []byte("test2"), 2)
	writer.WriteMessage("testTopic", []byte("test3"), 3)
	writer.WriteMessage("testTopic", []byte("test4"), 4)
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
	Print(summary)

	it, err := reader.ReadMessages(WithOrder(TimeOrder))
	if err != nil {
		t.Fatalf("failed to create message iterator: %v", err)
	}

	fmt.Println("len", len(it.topicsGroupIterators))

	messageBuf := NewReusableBuffer()
	for {
		topicName, err := it.NextInto(messageBuf)
		fmt.Println("err", err)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("failed to read message: %v", err)
		}

		fmt.Println("got", topicName, string(messageBuf.Data))
	}
}
