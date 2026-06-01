// Example: read several turbodata files as one time-ordered stream.
//
// MultiReader merges per-file streams. Topic-name collisions across files are
// resolved per Reader with WithTopicRemap: names meant to union share an
// exposed name; names meant to stay distinct are remapped apart.
//
// This example builds two small files in memory (no examples/write needed) and
// demonstrates:
//
//	union read       — same topic in both files, time-split, merged as one
//	split read       — one file's colliding topic remapped to a distinct name
//	multi sample     — floor across files picks the latest floor (the union floor)
//
// Run:
//
//	cd go
//	go run ./examples/multiread
package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"

	"turbodata"
)

func main() {
	fileA := buildFile(map[string][]int64{
		"/cam": {100, 300, 500},
		"/imu": {150, 350},
	})
	fileB := buildFile(map[string][]int64{
		"/cam":   {200, 400, 600},
		"/lidar": {250, 450},
	})

	demoUnion(fileA, fileB)
	demoSplit(fileA, fileB)
	demoSample(fileA, fileB)
}

// demoUnion: no remap. "/cam" appears in both files over disjoint time, so it
// unions into a single time-ordered stream; "/imu" and "/lidar" augment it.
func demoUnion(fileA, fileB []byte) {
	section("union read (shared /cam, augmented by /imu + /lidar)")
	mr, err := turbodata.NewMultiReader(
		turbodata.NewReader(bytes.NewReader(fileA)),
		turbodata.NewReader(bytes.NewReader(fileB)),
	)
	if err != nil {
		log.Fatalf("NewMultiReader: %v", err)
	}
	it, err := mr.ReadMessages()
	if err != nil {
		log.Fatalf("ReadMessages: %v", err)
	}
	iterate(it)
}

// demoSplit: remap file B's "/cam" to "/cam_b" so the two cameras stay
// distinct instead of unioning.
func demoSplit(fileA, fileB []byte) {
	section("split read (file B /cam remapped to /cam_b)")
	mr, err := turbodata.NewMultiReader(
		turbodata.NewReader(bytes.NewReader(fileA)),
		turbodata.NewReader(bytes.NewReader(fileB),
			turbodata.WithTopicRemap(map[string]string{"/cam": "/cam_b"})),
	)
	if err != nil {
		log.Fatalf("NewMultiReader: %v", err)
	}
	it, err := mr.ReadMessages()
	if err != nil {
		log.Fatalf("ReadMessages: %v", err)
	}
	iterate(it)
}

// demoSample: floor sample of "/cam" across both files. For each timestamp the
// result is the latest floor across the union of A and B.
func demoSample(fileA, fileB []byte) {
	section("multi sample of /cam (latest floor across files)")
	mr, err := turbodata.NewMultiReader(
		turbodata.NewReader(bytes.NewReader(fileA)),
		turbodata.NewReader(bytes.NewReader(fileB)),
	)
	if err != nil {
		log.Fatalf("NewMultiReader: %v", err)
	}
	ts := []int64{150, 350, 550, 700}
	out, err := mr.Sample([]turbodata.SampleQuery{{Topic: "/cam", Timestamps: ts}})
	if err != nil {
		log.Fatalf("Sample: %v", err)
	}
	for j, res := range out[0] {
		if !res.Found {
			fmt.Printf("  T=%4d -> (no floor)\n", ts[j])
			continue
		}
		fmt.Printf("  T=%4d -> ts=%4d data=%q\n", ts[j], res.Timestamp, res.Data)
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// buildFile writes one topic per map entry, one message per timestamp, and
// returns the resulting file bytes. Payloads are "<topic>@<ts>".
func buildFile(topics map[string][]int64) []byte {
	buf := &bytes.Buffer{}
	w := turbodata.NewWriter(buf)
	for topic, tss := range topics {
		if err := w.OpenTopics([]string{topic}, []map[string]any{{}}); err != nil {
			log.Fatalf("OpenTopics %s: %v", topic, err)
		}
		for _, ts := range tss {
			msg := []byte(fmt.Sprintf("%s@%d", topic, ts))
			if err := w.WriteMessage(topic, msg, ts); err != nil {
				log.Fatalf("WriteMessage %s: %v", topic, err)
			}
		}
		if err := w.CloseTopic(); err != nil {
			log.Fatalf("CloseTopic %s: %v", topic, err)
		}
	}
	if err := w.Close(); err != nil {
		log.Fatalf("Close writer: %v", err)
	}
	return buf.Bytes()
}

func iterate(it interface {
	NextInto(buf *turbodata.ReusableBuffer) (int64, string, error)
}) {
	buf := turbodata.NewReusableBuffer()
	for {
		ts, name, err := it.NextInto(buf)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			log.Fatalf("NextInto: %v", err)
		}
		fmt.Printf("  ts=%4d topic=%-8s data=%q\n", ts, name, buf.Data)
	}
}

func section(title string) {
	fmt.Printf("\n--- %s ---\n", title)
}
