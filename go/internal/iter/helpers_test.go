package iter_test

// This file mirrors the test helpers in the root package's test_helpers_test.go
// and writer_test.go. The mirror exists because:
//   - These tests live in the internal/iter directory but run as the external
//     test package "iter_test" so they can import turbodata.
//   - Test files (*_test.go) are not visible across packages, so we cannot
//     import the helpers from the root test package.
//
// If these helpers change, update both copies. The root copies stay because
// writer_test.go and cost_aware_integration_test.go still use them.

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"turbodata"
)

// testMsg holds the fields returned by MessageIterator.NextInto.
type testMsg struct {
	ts   int64
	name string
	data []byte
}

// writerRoundTrip runs setup against a Writer, then returns a Reader over the result.
func writerRoundTrip(t *testing.T, setup func(*turbodata.Writer)) *turbodata.Reader {
	t.Helper()
	buf := &bytes.Buffer{}
	w := turbodata.NewWriter(buf)
	setup(w)
	r := turbodata.NewReader(bytes.NewReader(buf.Bytes()))
	return r
}

// collect drains a Reader into a slice of testMsgs.
func collect(t *testing.T, r *turbodata.Reader, opts ...turbodata.ReadOption) []testMsg {
	t.Helper()
	it, err := r.ReadMessages(opts...)
	if err != nil {
		t.Fatalf("ReadMessages: %v", err)
	}
	rb := turbodata.NewReusableBuffer()
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

func mustOpenTopics(t *testing.T, w *turbodata.Writer, names []string, metadatas []map[string]any, opts ...turbodata.WriteOption) {
	t.Helper()
	if err := w.OpenTopics(names, metadatas, opts...); err != nil {
		t.Fatalf("OpenTopics: %v", err)
	}
}

func mustWriteMessage(t *testing.T, w *turbodata.Writer, name string, data []byte, ts int64) {
	t.Helper()
	if err := w.WriteMessage(name, data, ts); err != nil {
		t.Fatalf("WriteMessage: %v", err)
	}
}

func mustCloseTopic(t *testing.T, w *turbodata.Writer) {
	t.Helper()
	if err := w.CloseTopic(); err != nil {
		t.Fatalf("CloseTopic: %v", err)
	}
}

func mustClose(t *testing.T, w *turbodata.Writer) {
	t.Helper()
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
