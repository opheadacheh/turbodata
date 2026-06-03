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
// The demo file deliberately contains four topic groups with mixed
// compression, chunk configs, and a video group so the other examples have
// something interesting to query:
//
//	Group A  /imu                 uncompressed, size-based default chunking
//	Group B  /cam/front           compressed,   count=3 chunks (forces multiple chunks)
//	Group C  /odom, /gps          compressed,   count=4 chunks (two-topic group)
//	Group D  /cam/h264            video,        two GOPs (key-frame gated chunking)
package main

import (
	"fmt"
	"log"
	"os"
	"github.com/opheadacheh/turbodata/go/turbodata"
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
	writeVideoGroup(w)

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

// Group D: a single video topic. A video group must hold exactly one topic
// and must not be compressed (the codec already compresses the bytes). Frames
// are written with WriteVideoMessage (NOT WriteMessage), which carries the
// isKeyFrame flag; the first frame MUST be a key frame. The writer gates chunk
// boundaries on key frames, so a GOP is never split across two chunks.
//
// Two GOPs are written so the read/ and sample/ examples can demonstrate
// key-frame snap-back and GOP-prefix sampling:
//
//	GOP A  K@100, P@200, P@300, P@400
//	GOP B  K@500, P@600, P@700, P@800
func writeVideoGroup(w *turbodata.Writer) {
	if err := w.OpenTopics(
		[]string{"/cam/h264"},
		[]map[string]any{{"hz": int64(10), "codec": "h264"}},
		turbodata.WithVideoTopic(),
	); err != nil {
		log.Fatalf("open /cam/h264: %v", err)
	}
	type vframe struct {
		ts         int64
		isKeyFrame bool
	}
	frames := []vframe{
		{100, true}, {200, false}, {300, false}, {400, false}, // GOP A
		{500, true}, {600, false}, {700, false}, {800, false}, // GOP B
	}
	for _, f := range frames {
		kind := "P"
		if f.isKeyFrame {
			kind = "K"
		}
		// Pad the payload so it looks like a coded frame rather than a label.
		msg := append([]byte(fmt.Sprintf("%s%d-", kind, f.ts)), make([]byte, 32)...)
		if err := w.WriteVideoMessage("/cam/h264", msg, f.ts, f.isKeyFrame); err != nil {
			log.Fatalf("write /cam/h264: %v", err)
		}
	}
	if err := w.CloseTopic(); err != nil {
		log.Fatalf("close /cam/h264: %v", err)
	}
}
