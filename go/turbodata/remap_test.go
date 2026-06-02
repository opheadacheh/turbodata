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
	"strings"
	"testing"
)

// buildRemapReader builds a two-topic file ("a", "b") and returns a Reader
// over it with the given remap applied.
func buildRemapReader(t *testing.T, remap map[string]string) *Reader {
	t.Helper()
	raw := buildFile(t, func(w *Writer) {
		mustOpenTopics(t, w, []string{"a", "b"}, []map[string]any{{}, {}})
		mustWriteMessage(t, w, "a", []byte("a1"), 10)
		mustWriteMessage(t, w, "b", []byte("b1"), 20)
		mustWriteMessage(t, w, "a", []byte("a2"), 30)
		mustWriteMessage(t, w, "b", []byte("b2"), 40)
		mustCloseTopic(t, w)
		mustClose(t, w)
	})
	return NewReader(bytes.NewReader(raw), WithTopicRemap(remap))
}

func TestReaderRemapOutputNames(t *testing.T) {
	r := buildRemapReader(t, map[string]string{"a": "a_v2"})
	msgs := collect(t, r)

	want := []testMsg{
		{10, "a_v2", []byte("a1")},
		{20, "b", []byte("b1")},
		{30, "a_v2", []byte("a2")},
		{40, "b", []byte("b2")},
	}
	if len(msgs) != len(want) {
		t.Fatalf("message count: got %d want %d", len(msgs), len(want))
	}
	for i := range want {
		if msgs[i].ts != want[i].ts || msgs[i].name != want[i].name || !bytes.Equal(msgs[i].data, want[i].data) {
			t.Errorf("[%d] got (%d,%q,%q) want (%d,%q,%q)",
				i, msgs[i].ts, msgs[i].name, msgs[i].data, want[i].ts, want[i].name, want[i].data)
		}
	}
}

func TestReaderRemapTopicNamesFilter(t *testing.T) {
	r := buildRemapReader(t, map[string]string{"a": "a_v2"})

	// Filtering must accept the exposed name and return only that topic's
	// messages, labeled with the exposed name.
	msgs := collect(t, r, WithTopicNames([]string{"a_v2"}))
	want := []testMsg{
		{10, "a_v2", []byte("a1")},
		{30, "a_v2", []byte("a2")},
	}
	if len(msgs) != len(want) {
		t.Fatalf("message count: got %d want %d (%+v)", len(msgs), len(want), msgs)
	}
	for i := range want {
		if msgs[i].ts != want[i].ts || msgs[i].name != want[i].name || !bytes.Equal(msgs[i].data, want[i].data) {
			t.Errorf("[%d] got (%d,%q,%q) want (%d,%q,%q)",
				i, msgs[i].ts, msgs[i].name, msgs[i].data, want[i].ts, want[i].name, want[i].data)
		}
	}
}

func TestReaderRemapSummaryExposedNames(t *testing.T) {
	r := buildRemapReader(t, map[string]string{"a": "a_v2"})
	summary, err := r.Summary()
	if err != nil {
		t.Fatalf("Summary: %v", err)
	}
	got := make(map[string]bool)
	for _, ti := range summary.TopicsInfos {
		for _, tm := range ti.TopicMetadatas {
			got[tm.Name] = true
		}
	}
	if !got["a_v2"] || !got["b"] {
		t.Errorf("summary names: got %v want a_v2 and b", got)
	}
	if got["a"] {
		t.Errorf("summary still exposes in-file name %q", "a")
	}
}

func TestReaderRemapCollisionError(t *testing.T) {
	// Renaming "a" onto the untouched in-file name "b" collapses two topics
	// onto one exposed name and must error on first use.
	r := buildRemapReader(t, map[string]string{"a": "b"})

	if _, err := r.Summary(); err == nil {
		t.Errorf("Summary: expected collision error, got nil")
	} else if !strings.Contains(err.Error(), "duplicate exposed name") {
		t.Errorf("Summary: error %q should mention duplicate exposed name", err)
	}

	if _, err := r.ReadMessages(); err == nil {
		t.Errorf("ReadMessages: expected collision error, got nil")
	}
}

func TestReaderRemapSample(t *testing.T) {
	r := buildRemapReader(t, map[string]string{"a": "a_v2"})

	out, err := r.Sample([]SampleQuery{
		{Topic: "a_v2", Timestamps: []int64{10, 25, 30}},
	})
	if err != nil {
		t.Fatalf("Sample: %v", err)
	}
	want := []SampleResult{
		{Found: true, Timestamp: 10, Data: []byte("a1")},
		{Found: true, Timestamp: 10, Data: []byte("a1")},
		{Found: true, Timestamp: 30, Data: []byte("a2")},
	}
	assertResultsEqual(t, out[0], want)
}
