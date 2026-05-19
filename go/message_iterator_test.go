package turbodata

import (
	"io"
	"testing"
)

type gotMessage struct {
	Timestamp int64
	Topic     string
	Payload   []byte
}

func collectMessages(t *testing.T, it *MessageIterator) []gotMessage {
	t.Helper()
	buf := NewReusableBuffer()
	var out []gotMessage
	for {
		ts, topic, err := it.NextInto(buf)
		if err == io.EOF {
			return out
		}
		if err != nil {
			t.Fatalf("NextInto: %v", err)
		}
		payload := make([]byte, len(buf.Data))
		copy(payload, buf.Data)
		out = append(out, gotMessage{Timestamp: ts, Topic: topic, Payload: payload})
	}
}

func readMessages(t *testing.T, reader *Reader, opts ...ReadOption) []gotMessage {
	t.Helper()
	it, err := reader.ReadMessages(opts...)
	if err != nil {
		t.Fatalf("ReadMessages: %v", err)
	}
	return collectMessages(t, it)
}

func readerFromFixture(t *testing.T, data []byte) *Reader {
	t.Helper()
	return newReaderFromBytes(t, data)
}

func minimalFixture(t *testing.T) ([]byte, []gotMessage) {
	t.Helper()
	data := writeReaderFixture(t, minimalReaderSetup(false), minimalReaderWrite)
	want := []gotMessage{{Timestamp: 1, Topic: "test", Payload: []byte("test")}}
	return data, want
}

func twoTopicGroupsFixture(t *testing.T) ([]byte, []gotMessage) {
	t.Helper()
	payloads := [][]byte{[]byte("test"), []byte("test2"), []byte("test3"), []byte("test4")}
	data := writeReaderFixture(t, nil, func(w *Writer) error {
		if err := w.OpenTopics([]string{"test"}, []map[string]any{{"foo": "bar"}}); err != nil {
			return err
		}
		for i, p := range payloads {
			if err := w.WriteMessage("test", p, int64(i+1)); err != nil {
				return err
			}
		}
		if err := w.CloseTopic(); err != nil {
			return err
		}
		if err := w.OpenTopics([]string{"testTopic"}, []map[string]any{{"foo": "bar"}}); err != nil {
			return err
		}
		for i, p := range payloads {
			if err := w.WriteMessage("testTopic", p, int64(i+1)); err != nil {
				return err
			}
		}
		return nil
	})
	var want []gotMessage
	for i, p := range payloads {
		want = append(want, gotMessage{Timestamp: int64(i + 1), Topic: "test", Payload: append([]byte(nil), p...)})
	}
	for i, p := range payloads {
		want = append(want, gotMessage{Timestamp: int64(i + 1), Topic: "testTopic", Payload: append([]byte(nil), p...)})
	}
	return data, want
}

func interleavedTwoTopicsFixture(t *testing.T) ([]byte, []gotMessage) {
	t.Helper()
	const n = 4
	data := writeReaderFixture(t,
		func(w *Writer) error {
			return w.OpenTopics([]string{"a", "b"}, []map[string]any{{}, {}})
		},
		func(w *Writer) error {
			for i := 0; i < n; i++ {
				if err := w.WriteMessage("a", []byte("a"), int64(2*i)); err != nil {
					return err
				}
				if err := w.WriteMessage("b", []byte("b"), int64(2*i+1)); err != nil {
					return err
				}
			}
			return nil
		},
	)
	var want []gotMessage
	for i := 0; i < n; i++ {
		want = append(want, gotMessage{Timestamp: int64(2 * i), Topic: "a", Payload: []byte("a")})
		want = append(want, gotMessage{Timestamp: int64(2*i + 1), Topic: "b", Payload: []byte("b")})
	}
	return data, want
}

func chunkedFixture(t *testing.T) ([]byte, []gotMessage) {
	t.Helper()
	data := writeReaderFixture(t,
		func(w *Writer) error {
			return w.OpenTopics([]string{"test"}, []map[string]any{{"foo": "bar"}},
				WithChunkConfig(&ChunkConfig{Mode: ChunkThresholdModeCount, Count: 25}))
		},
		write100ToTest,
	)
	want := make([]gotMessage, 100)
	for i := range want {
		want[i] = gotMessage{Timestamp: int64(i), Topic: "test", Payload: []byte("test")}
	}
	return data, want
}
