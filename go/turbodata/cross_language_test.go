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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
)

// pythonDemoRelPath is the .td file produced by py/examples/write_demo.py,
// relative to this package directory (go/turbodata). It is gitignored and
// regenerated in CI; see the Python job in .github/workflows/ci.yml.
//
// This test closes the Python-writes / Go-reads cross-language gap: the Python
// SDK writes the file, the Go reference SDK must decode it back byte-for-byte
// identically. It is the mirror image of py/tests/test_cross_language.py's
// TestReadGoFile (Go writes, Python reads) and skips, rather than fails, when
// the file is absent so a plain `go test ./...` without Python still passes.
const pythonDemoRelPath = "../../py/examples/demo.td"

type expectedMessage struct {
	ts   int64
	data []byte
}

// pythonDemoExpected reconstructs, grouped by topic, the exact messages that
// py/examples/write_demo.py emits. Keep it in lockstep with that script (and
// its mirror go/examples/write/main.go): an intentional change to the demo
// content must update this alongside it.
func pythonDemoExpected() map[string][]expectedMessage {
	exp := map[string][]expectedMessage{}

	// Group A: /imu, uncompressed, ts 100..1000.
	for i := 1; i <= 10; i++ {
		exp["/imu"] = append(exp["/imu"], expectedMessage{
			ts:   int64(i) * 100,
			data: []byte(fmt.Sprintf("imu-%03d", i)),
		})
	}

	// Group B: /cam/front, compressed, ts 150..1050, padded payload.
	for i := 0; i < 10; i++ {
		exp["/cam/front"] = append(exp["/cam/front"], expectedMessage{
			ts:   int64(150 + i*100),
			data: append([]byte(fmt.Sprintf("cam-%03d-", i)), make([]byte, 64)...),
		})
	}

	// Group C: /odom + /gps, one compressed group, interleaved by timestamp.
	entries := []struct {
		topic string
		ts    int64
	}{
		{"/odom", 120}, {"/odom", 220}, {"/gps", 300}, {"/odom", 320},
		{"/odom", 420}, {"/odom", 520}, {"/gps", 600}, {"/odom", 620},
		{"/odom", 720}, {"/gps", 900},
	}
	for i, e := range entries {
		exp[e.topic] = append(exp[e.topic], expectedMessage{
			ts:   e.ts,
			data: []byte(fmt.Sprintf("%s-%03d", e.topic[1:], i)),
		})
	}

	// Group D: /cam/h264 video, two GOPs (K@100..P@400, K@500..P@800).
	frames := []struct {
		ts    int64
		isKey bool
	}{
		{100, true}, {200, false}, {300, false}, {400, false},
		{500, true}, {600, false}, {700, false}, {800, false},
	}
	for _, f := range frames {
		kind := "P"
		if f.isKey {
			kind = "K"
		}
		exp["/cam/h264"] = append(exp["/cam/h264"], expectedMessage{
			ts:   f.ts,
			data: append([]byte(fmt.Sprintf("%s%d-", kind, f.ts)), make([]byte, 32)...),
		})
	}

	return exp
}

func TestReadPythonDemo(t *testing.T) {
	if _, err := os.Stat(pythonDemoRelPath); err != nil {
		t.Skipf("python demo not built (%s); run `python examples/write_demo.py` in py/ first",
			filepath.Clean(pythonDemoRelPath))
	}

	f, err := os.Open(pythonDemoRelPath)
	if err != nil {
		t.Fatalf("open %s: %v", pythonDemoRelPath, err)
	}
	defer f.Close()

	r := NewReader(f)

	// The summary must expose every topic the Python writer declared.
	summary, err := r.Summary()
	if err != nil {
		t.Fatalf("Summary: %v", err)
	}
	gotTopics := map[string]bool{}
	for _, ti := range summary.TopicsInfos {
		for _, tm := range ti.TopicMetadatas {
			gotTopics[tm.Name] = true
		}
	}
	for _, name := range []string{"/imu", "/cam/front", "/odom", "/gps", "/cam/h264"} {
		if !gotTopics[name] {
			t.Errorf("summary is missing topic %q written by Python", name)
		}
	}

	// Drain the message stream and bucket by topic. The default TimeOrder read
	// emits non-decreasing timestamps, so per-topic order matches write order.
	it, err := r.ReadMessages()
	if err != nil {
		t.Fatalf("ReadMessages: %v", err)
	}
	got := map[string][]expectedMessage{}
	buf := NewReusableBuffer()
	total := 0
	for {
		ts, name, err := it.NextInto(buf)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("NextInto: %v", err)
		}
		data := make([]byte, len(buf.Data))
		copy(data, buf.Data)
		got[name] = append(got[name], expectedMessage{ts: ts, data: data})
		total++
	}

	want := pythonDemoExpected()
	wantTotal := 0
	for _, msgs := range want {
		wantTotal += len(msgs)
	}
	if total != wantTotal {
		t.Errorf("decoded %d messages, want %d", total, wantTotal)
	}

	for topic, wantMsgs := range want {
		gotMsgs := got[topic]
		if len(gotMsgs) != len(wantMsgs) {
			t.Errorf("topic %q: decoded %d messages, want %d", topic, len(gotMsgs), len(wantMsgs))
			continue
		}
		for i := range wantMsgs {
			if gotMsgs[i].ts != wantMsgs[i].ts {
				t.Errorf("topic %q message %d: ts = %d, want %d", topic, i, gotMsgs[i].ts, wantMsgs[i].ts)
			}
			if !bytes.Equal(gotMsgs[i].data, wantMsgs[i].data) {
				t.Errorf("topic %q message %d: data = %q, want %q", topic, i, gotMsgs[i].data, wantMsgs[i].data)
			}
		}
	}
}
