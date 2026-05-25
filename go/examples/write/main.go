// Example: write a turbodata file.
//
// Produces examples/demo.td. The read/ and sample/ examples consume this same
// file, so run this one first:
//
//	cd go
//	go run ./examples/write
//	go run ./examples/read
//	go run ./examples/sample
//
// The demo file deliberately contains three topic groups with mixed
// compression and chunk configs so the other examples have something
// interesting to query:
//
//	Group A  /imu                 uncompressed, size-based default chunking
//	Group B  /cam/front           compressed,   count=3 chunks (forces multiple chunks)
//	Group C  /odom, /gps          compressed,   count=4 chunks (two-topic group)
package main

import (
	"fmt"
	"log"
	"os"
	"turbodata"
)

const outPath = "examples/demo.td"

func main() {
	f, err := os.Create(outPath)
	if err != nil {
		log.Fatalf("create %s: %v", outPath, err)
	}
	defer f.Close()

	w := turbodata.NewWriter(f)

	writeImuGroup(w)
	writeCamGroup(w)
	writeOdomGpsGroup(w)

	if err := w.Close(); err != nil {
		log.Fatalf("close writer: %v", err)
	}

	info, _ := f.Stat()
	fmt.Printf("wrote %s (%d bytes)\n", outPath, info.Size())
}

// Group A: a single uncompressed topic with default size-based chunking. With
// only ten ~10-byte messages the entire group lives in one chunk.
func writeImuGroup(w *turbodata.Writer) {
	if err := w.OpenTopics(
		[]string{"/imu"},
		[]map[string]any{{"hz": int64(100)}},
	); err != nil {
		log.Fatalf("open /imu: %v", err)
	}
	// Timestamps: 100, 200, ..., 1000.
	for i := 1; i <= 10; i++ {
		ts := int64(i) * 100
		msg := []byte(fmt.Sprintf("imu-%03d", i))
		if err := w.WriteMessage("/imu", msg, ts); err != nil {
			log.Fatalf("write /imu: %v", err)
		}
	}
	if err := w.CloseTopic(); err != nil {
		log.Fatalf("close /imu: %v", err)
	}
}

// Group B: a single compressed topic, count-mode chunks of 3. Ten messages
// therefore land in four chunks (3+3+3+1), so time-range queries and
// sampling have multiple candidate chunks to choose from.
func writeCamGroup(w *turbodata.Writer) {
	chunkCfg := &turbodata.ChunkConfig{
		Mode:  turbodata.ChunkThresholdModeCount,
		Count: 3,
	}
	if err := w.OpenTopics(
		[]string{"/cam/front"},
		[]map[string]any{{"hz": int64(10), "encoding": "jpeg"}},
		turbodata.WithCompression(),
		turbodata.WithChunkConfig(chunkCfg),
	); err != nil {
		log.Fatalf("open /cam/front: %v", err)
	}
	// Timestamps: 150, 250, ..., 1050. Offset by 50 from /imu so floor
	// sampling at e.g. T=300 returns different timestamps per topic.
	// Padded payload keeps compression non-trivial.
	for i := 0; i < 10; i++ {
		ts := int64(150 + i*100)
		msg := append([]byte(fmt.Sprintf("cam-%03d-", i)), make([]byte, 64)...)
		if err := w.WriteMessage("/cam/front", msg, ts); err != nil {
			log.Fatalf("write /cam/front: %v", err)
		}
	}
	if err := w.CloseTopic(); err != nil {
		log.Fatalf("close /cam/front: %v", err)
	}
}

// Group C: two topics in one compressed group, count-mode chunks of 4.
// Messages are interleaved by timestamp across the two topics, demonstrating
// that a single group can hold multiple topics that share storage.
func writeOdomGpsGroup(w *turbodata.Writer) {
	chunkCfg := &turbodata.ChunkConfig{
		Mode:  turbodata.ChunkThresholdModeCount,
		Count: 4,
	}
	if err := w.OpenTopics(
		[]string{"/odom", "/gps"},
		[]map[string]any{
			{"hz": int64(20), "frame": "base_link"},
			{"hz": int64(1), "frame": "wgs84"},
		},
		turbodata.WithCompression(),
		turbodata.WithChunkConfig(chunkCfg),
	); err != nil {
		log.Fatalf("open /odom + /gps: %v", err)
	}
	// /odom at ts 120, 220, ..., 720 (7 msgs).
	// /gps  at ts 300, 600, 900            (3 msgs).
	// Merge-sorted into a single non-decreasing sequence of 10 messages.
	type entry struct {
		topic string
		ts    int64
	}
	entries := []entry{
		{"/odom", 120}, {"/odom", 220}, {"/gps", 300}, {"/odom", 320},
		{"/odom", 420}, {"/odom", 520}, {"/gps", 600}, {"/odom", 620},
		{"/odom", 720}, {"/gps", 900},
	}
	for i, e := range entries {
		msg := []byte(fmt.Sprintf("%s-%03d", e.topic[1:], i))
		if err := w.WriteMessage(e.topic, msg, e.ts); err != nil {
			log.Fatalf("write %s: %v", e.topic, err)
		}
	}
	if err := w.CloseTopic(); err != nil {
		log.Fatalf("close /odom + /gps: %v", err)
	}
}
