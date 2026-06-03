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

// Example: sample messages at specific timestamps.
//
// Reader.Sample returns the floor message per (topic, timestamp) pair: the
// most-recent message at or before each requested timestamp. Compared to
// ReadMessages, Sample is the right primitive when you need "the state at
// time T" rather than "every message in [t0, t1]".
//
// Reads examples/demo.td (produced by examples/write). Run:
//
//	cd go
//	go run ./examples/write
//	go run ./examples/sample
package main

import (
	"fmt"
	"log"
	"os"
	"github.com/opheadacheh/turbodata/go/turbodata"
)

const inPath = "examples/demo.td"

func main() {
	demoSingleTopic()
	demoFloorSemantics()
	demoLinSpace()
	demoMultiTopic()
	demoEdgeCases()
	demoWithReadStrategy()
	demoVideoDecodable()
}

// demoSingleTopic asks for /imu at three concrete timestamps. /imu was
// written at ts 100, 200, ..., 1000.
func demoSingleTopic() {
	section("single topic, three timestamps")
	r := openReader()
	out, err := r.Sample([]turbodata.SampleQuery{
		{Topic: "/imu", Timestamps: []int64{200, 500, 1000}},
	})
	if err != nil {
		log.Fatalf("Sample: %v", err)
	}
	printResults("/imu", []int64{200, 500, 1000}, out[0])
}

// demoFloorSemantics shows what happens between data points: T=250 is
// between /imu@200 and /imu@300, so Sample returns the floor (200, "imu-002").
func demoFloorSemantics() {
	section("floor semantics: T=250 between /imu@200 and /imu@300")
	r := openReader()
	out, err := r.Sample([]turbodata.SampleQuery{
		{Topic: "/imu", Timestamps: []int64{250}},
	})
	if err != nil {
		log.Fatalf("Sample: %v", err)
	}
	printResults("/imu", []int64{250}, out[0])
}

// demoLinSpace constructs "N samples at a given frequency from T" without
// hand-rolling a loop. LinSpaceTimestamps satisfies the strictly-increasing
// contract on SampleQuery.Timestamps.
func demoLinSpace() {
	section("LinSpaceTimestamps: 6 samples at 5 Hz starting at T=100")
	// 5 Hz on this file's timestamp scale = stride 200 (timestamps are
	// dimensionless integers here; in a real file they'd be nanoseconds).
	ts := turbodata.LinSpaceTimestamps(100, 200, 6)
	r := openReader()
	out, err := r.Sample([]turbodata.SampleQuery{
		{Topic: "/imu", Timestamps: ts},
	})
	if err != nil {
		log.Fatalf("Sample: %v", err)
	}
	printResults("/imu", ts, out[0])
}

// demoMultiTopic queries three topics in a single Sample call. The engine
// fetches the necessary index chunks and data chunks concurrently for all
// topics — much faster than three sequential single-topic calls when
// storage latency is non-trivial.
//
// Rules enforced before any I/O:
//   - each Topic must appear in at most one SampleQuery (no duplicates)
//   - each Timestamps slice must be strictly increasing
//   - every Topic must exist in the file's summary
func demoMultiTopic() {
	section("multi-topic in one Sample call")
	r := openReader()
	out, err := r.Sample([]turbodata.SampleQuery{
		{Topic: "/imu", Timestamps: []int64{350, 750}},
		{Topic: "/cam/front", Timestamps: []int64{350, 750}},
		{Topic: "/gps", Timestamps: []int64{350, 750}},
	})
	if err != nil {
		log.Fatalf("Sample: %v", err)
	}
	printResults("/imu", []int64{350, 750}, out[0])
	printResults("/cam/front", []int64{350, 750}, out[1])
	printResults("/gps", []int64{350, 750}, out[2])
}

// demoEdgeCases shows the two not-found-worthy cases:
//
//   T before the topic's first message → Found=false
//   T after the topic's last message   → Found=true, returns the last message
func demoEdgeCases() {
	section("edge cases: before-first and after-last")
	r := openReader()
	out, err := r.Sample([]turbodata.SampleQuery{
		// /imu's first message is at ts=100; T=50 has no floor.
		// /imu's last message is at ts=1000; T=9999 floors to it.
		{Topic: "/imu", Timestamps: []int64{50, 9999}},
	})
	if err != nil {
		log.Fatalf("Sample: %v", err)
	}
	printResults("/imu", []int64{50, 9999}, out[0])
}

// demoWithReadStrategy overrides the default sample strategy. The default
// (turbodata.DefaultSampleStrategy) is sized for cloud object storage; on
// local disk a smaller CoalesceGap + serial fetch is fine too.
//
// Same three knobs as ReadMessages's ReadStrategy:
//
//   CoalesceGap     — merge adjacent ranges within this many bytes
//   SplitThreshold  — slice large ranges into parallel sub-reads
//   MaxConcurrency  — cap on in-flight ReadAt calls
func demoWithReadStrategy() {
	section("custom sample strategy")
	r := openReader()
	out, err := r.Sample(
		[]turbodata.SampleQuery{
			{Topic: "/imu", Timestamps: []int64{200, 500, 800}},
		},
		turbodata.WithSampleReadStrategy(turbodata.ReadStrategy{
			CoalesceGap:    256 * 1024,
			SplitThreshold: 2 * 1024 * 1024,
			MaxConcurrency: 4,
		}),
	)
	if err != nil {
		log.Fatalf("Sample: %v", err)
	}
	printResults("/imu", []int64{200, 500, 800}, out[0])
}

// demoVideoDecodable samples the video topic /cam/h264 (GOP A at ts 100..400,
// GOP B at ts 500..800) with WithSampleVideoDecodable.
//
// Without the option, a video topic samples like any other topic: each result
// is the single floor frame in Data. That frame may be a non-key frame that a
// decoder can't start from. With the option, each result instead carries a
// decoder-ready GOP sequence in Frames (and Data is nil):
//
//   - The first found result in each GOP sets ResetDecoder=true and Frames is
//     the whole prefix [keyframe ... target].
//   - Subsequent results in the same GOP set ResetDecoder=false and Frames is
//     only the new frames since the previous query (carry the decoder state
//     forward; don't reset).
//
// Here T=350 lands in GOP A and T=650 lands in GOP B, so both reset.
func demoVideoDecodable() {
	section("video: GOP-prefix sampling (WithSampleVideoDecodable)")
	r := openReader()
	ts := []int64{350, 650}
	out, err := r.Sample(
		[]turbodata.SampleQuery{{Topic: "/cam/h264", Timestamps: ts}},
		turbodata.WithSampleVideoDecodable(),
	)
	if err != nil {
		log.Fatalf("Sample: %v", err)
	}
	for i, res := range out[0] {
		if !res.Found {
			fmt.Printf("  /cam/h264@T=%-4d  (no key frame at or before T)\n", ts[i])
			continue
		}
		labels := make([]string, len(res.Frames))
		for j, fr := range res.Frames {
			labels[j] = fmt.Sprintf("ts=%d", fr.Timestamp)
		}
		fmt.Printf("  /cam/h264@T=%-4d  -> target ts=%d reset=%-5v frames=%v\n",
			ts[i], res.Timestamp, res.ResetDecoder, labels)
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func openReader() *turbodata.Reader {
	f, err := os.Open(inPath)
	if err != nil {
		log.Fatalf("open %s: %v (did you run examples/write first?)", inPath, err)
	}
	r := turbodata.NewReader(f)
	return r
}

func printResults(topic string, queriedAt []int64, results []turbodata.SampleResult) {
	for i, res := range results {
		if !res.Found {
			fmt.Printf("  %s@T=%-4d  (no message at or before T)\n", topic, queriedAt[i])
			continue
		}
		fmt.Printf("  %s@T=%-4d  -> ts=%-4d data=%q\n",
			topic, queriedAt[i], res.Timestamp, snippet(res.Data))
	}
}

func snippet(b []byte) string {
	const max = 24
	if len(b) <= max {
		return string(b)
	}
	return string(b[:max]) + "..."
}

func section(title string) {
	fmt.Printf("\n--- %s ---\n", title)
}
