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
	"io"
	"testing"
)

// testMsg holds the fields returned by MessageIterator.NextInto.
type testMsg struct {
	ts   int64
	name string
	data []byte
}

// buildFile runs setup against a Writer and returns the resulting bytes.
// Use this when you need direct access to the raw file bytes (e.g. to wrap
// them in a custom ReadSource). For the common case of immediately reading
// the file back, prefer writerRoundTrip.
func buildFile(t *testing.T, setup func(*Writer)) []byte {
	t.Helper()
	buf := &bytes.Buffer{}
	w := NewWriter(buf)
	setup(w)
	return buf.Bytes()
}

// writerRoundTrip runs setup against a Writer, then returns a Reader over the result.
func writerRoundTrip(t *testing.T, setup func(*Writer)) *Reader {
	t.Helper()
	r := NewReader(bytes.NewReader(buildFile(t, setup)))
	return r
}

// collect drains a Reader into a slice of testMsgs.
// Pass ReadOptions to control filtering/order.
func collect(t *testing.T, r *Reader, opts ...ReadOption) []testMsg {
	t.Helper()
	it, err := r.ReadMessages(opts...)
	if err != nil {
		t.Fatalf("ReadMessages: %v", err)
	}
	rb := NewReusableBuffer()
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
