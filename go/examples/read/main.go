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

// Example: read a turbodata file with each ReadOption.
//
// Reads examples/demo.td (produced by examples/write). Each demoXxx function
// is self-contained so you can copy a single one into your code.
//
// Run:
//
//	cd go
//	go run ./examples/write   # produces examples/demo.td
//	go run ./examples/read
//
// ReadOptions covered:
//
//	WithTopicNames       — restrict to a subset of topics
//	WithStartTimestamp   — inclusive lower bound on message timestamps
//	WithEndTimestamp     — inclusive upper bound on message timestamps
//	WithOrder            — TimeOrder (default) or ReverseTimeOrder
//	WithReadStrategy     — switch onto the cost-aware concurrent-I/O path
//	WithTailPrefetch     — single ReadAt for footer + summary on small files
//	WithVideoDecodable   — snap a video topic's start back to its key frame
//
// ReadStrategy gets a dedicated walkthrough below; it's the most-asked-about
// knob because it changes the I/O shape of the reader (lazy vs concurrent),
// not just the filter.
package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"time"
	"github.com/opheadacheh/turbodata/go/turbodata"
)

const inPath = "examples/demo.td"

func main() {
	demoSummary()
	demoReadAll()
	demoReverseOrder()
	demoTopicFilter()
	demoTimeRange()
	demoTailPrefetch()
	demoReadStrategyDefault()
	demoReadStrategyLatency()
	demoReadStrategyMoney()
	demoReadStrategyBlended()
	demoVideoDecodable()
}

// demoSummary prints the file's summary without reading any messages. The
// summary load is lazy; this is the cheapest possible inspection.
func demoSummary() {
	section("summary only (no message I/O)")
	r := openReader()
	summary, err := r.Summary()
	if err != nil {
		log.Fatalf("summary: %v", err)
	}
	for _, ti := range summary.TopicsInfos {
		names := make([]string, 0, len(ti.TopicMetadatas))
		for _, tm := range ti.TopicMetadatas {
			names = append(names, tm.Name)
		}
		fmt.Printf("  group: topics=%v chunks=%d totalLen=%d\n",
			names, len(ti.IndexChunkInfoList), ti.TotalLen)
	}
}

// demoReadAll iterates every message with default options: time order, no
// filter, default (lazy) reader path.
func demoReadAll() {
	section("read all messages (defaults)")
	r := openReader()
	it, err := r.ReadMessages()
	if err != nil {
		log.Fatalf("ReadMessages: %v", err)
	}
	iterate(it, 5)
}

// demoTopicFilter restricts iteration to a subset of topics. Topics not
// listed are skipped entirely; their groups are not even opened if no other
// requested topic shares the group.
func demoTopicFilter() {
	section("only /imu and /gps")
	r := openReader()
	it, err := r.ReadMessages(
		turbodata.WithTopicNames([]string{"/imu", "/gps"}),
	)
	if err != nil {
		log.Fatalf("ReadMessages: %v", err)
	}
	iterate(it, 0)
}

// demoTimeRange restricts iteration to messages with timestamp in [start, end]
// inclusive. The filter is applied per chunk in the summary, so chunks fully
// outside the window are never fetched from disk.
func demoTimeRange() {
	section("time range [300, 700]")
	r := openReader()
	it, err := r.ReadMessages(
		turbodata.WithStartTimestamp(300),
		turbodata.WithEndTimestamp(700),
	)
	if err != nil {
		log.Fatalf("ReadMessages: %v", err)
	}
	iterate(it, 0)
}

// demoReverseOrder iterates messages from newest to oldest. The reader still
// merges across groups; only the comparison direction changes.
func demoReverseOrder() {
	section("reverse time order (last 5)")
	r := openReader()
	it, err := r.ReadMessages(
		turbodata.WithOrder(turbodata.ReverseTimeOrder),
	)
	if err != nil {
		log.Fatalf("ReadMessages: %v", err)
	}
	iterate(it, 5)
}

// demoTailPrefetch hints how many trailing bytes to read speculatively when
// loading the file's summary. If the hint covers footer + compressed
// summary, the load costs one ReadAt instead of two. Useful when latency to
// the storage backend dominates (object storage). Safe to over-hint; the
// extra bytes are simply discarded.
func demoTailPrefetch() {
	section("tail prefetch hint of 64 KiB")
	r := openReader()
	it, err := r.ReadMessages(
		turbodata.WithTailPrefetch(64 * 1024),
	)
	if err != nil {
		log.Fatalf("ReadMessages: %v", err)
	}
	iterate(it, 3)
}

// ---------------------------------------------------------------------------
// ReadStrategy walkthrough.
//
// Without WithReadStrategy, the reader walks one chunk at a time using
// serial Seek+Read. That path uses minimal memory and is fine for local
// disk, but it cannot overlap I/O or amortize per-request latency.
//
// WithReadStrategy switches onto the cost-aware path:
//
//   1. Phase A: plan and concurrently fetch every in-scope index chunk.
//   2. Decode the index chunks to learn which message bytes are needed.
//   3. Phase B: plan and concurrently fetch the message data, either at
//      chunk granularity (compressed groups) or message granularity
//      (uncompressed groups).
//
// Planning takes three knobs:
//
//   CoalesceGap     — merge two adjacent needed ranges into a single ReadAt
//                     when the gap between them is smaller than this many
//                     bytes. Trades wasted bytes for fewer requests.
//
//   SplitThreshold  — slice a planned read larger than this into parallel
//                     sub-reads. Trades extra requests for lower per-read
//                     wall-clock.
//
//   MaxConcurrency  — cap on in-flight ReadAt calls. 1 = serial.
//
// You can construct ReadStrategy values directly when you know the right
// numbers for your environment, or use the helper constructors below for
// the common cost models.
// ---------------------------------------------------------------------------

// demoReadStrategyDefault: cost-aware path with hand-tuned values. Use this
// when you know your storage backend's request cost and want explicit
// control.
func demoReadStrategyDefault() {
	section("cost-aware path, hand-tuned strategy")
	strategy := turbodata.ReadStrategy{
		CoalesceGap:    1 << 20, // 1 MiB: merge adjacent reads within 1 MiB
		SplitThreshold: 4 << 20, // 4 MiB: split single reads above 4 MiB
		MaxConcurrency: 8,
	}
	r := openReader()
	it, err := r.ReadMessages(turbodata.WithReadStrategy(strategy))
	if err != nil {
		log.Fatalf("ReadMessages: %v", err)
	}
	iterate(it, 0)
}

// demoReadStrategyLatency: minimize wall-clock for object storage. Pick a
// CoalesceGap and SplitThreshold equal to the bandwidth-latency product
// (BDP = RTT * per-stream bandwidth) so each parallel sub-read keeps a
// stream saturated for roughly one RTT.
func demoReadStrategyLatency() {
	section("strategy: minimize wall-clock (object storage)")
	// 20 ms RTT, ~125 MB/s per stream, 16 parallel streams.
	strategy := turbodata.StrategyForLatency(
		20*time.Millisecond,
		125*1024*1024,
		16,
	)
	r := openReader()
	it, err := r.ReadMessages(turbodata.WithReadStrategy(strategy))
	if err != nil {
		log.Fatalf("ReadMessages: %v", err)
	}
	iterate(it, 0)
}

// demoReadStrategyMoney: minimize total spend for paid-request storage
// (e.g. S3 GET = $0.0004 / 1000 requests + per-byte egress). Aggressively
// coalesces (every avoided request is money saved) and never splits (extra
// splits = extra paid requests).
func demoReadStrategyMoney() {
	section("strategy: minimize spend (paid-request storage)")
	// S3-style: $0.0000004 per GET, $0.00000000009 per byte (in-region GET).
	// reqPrice / bytePrice ≈ 4.4 MiB — gaps up to that size are cheaper to
	// pave than to issue an extra request.
	strategy := turbodata.StrategyForMoney(
		0.0000004,
		0.00000000009,
		8,
	)
	r := openReader()
	it, err := r.ReadMessages(turbodata.WithReadStrategy(strategy))
	if err != nil {
		log.Fatalf("ReadMessages: %v", err)
	}
	iterate(it, 0)
}

// demoReadStrategyBlended: minimize spend (as money) but cap how long any
// single read may take by splitting reads larger than maxReadTime *
// perStreamBW. Useful when egress isn't free but tail latency still
// matters.
func demoReadStrategyBlended() {
	section("strategy: blended cost + latency cap")
	strategy := turbodata.StrategyForBlended(
		0.0000004,            // reqPrice ($)
		0.00000000009,        // bytePrice ($/byte)
		100*time.Millisecond, // cap any single read to ~100 ms
		125*1024*1024,        // perStreamBW (B/s)
		8,                    // concurrency
	)
	r := openReader()
	it, err := r.ReadMessages(turbodata.WithReadStrategy(strategy))
	if err != nil {
		log.Fatalf("ReadMessages: %v", err)
	}
	iterate(it, 0)
}

// demoVideoDecodable shows the video-aware read path on /cam/h264 (GOP A at
// ts 100..400, GOP B at ts 500..800).
//
// A video topic stores compressed frames where a non-key frame is only
// decodable after its GOP's key frame. A plain time filter that starts
// mid-GOP therefore yields bytes a decoder can't cold-start on. WithVideo-
// Decodable snaps the effective StartTimestamp back to the latest key frame
// at or before the requested start, so the emitted sequence begins with a
// key frame and feeds a decoder correctly. Non-video topics are unaffected.
func demoVideoDecodable() {
	section("video: start=650 mid-GOP, plain vs WithVideoDecodable")

	// Plain: start=650 lands inside GOP B (after K@500), so the first emitted
	// frame is a non-key frame the decoder cannot start from.
	r := openReader()
	plain, err := r.ReadMessages(
		turbodata.WithTopicNames([]string{"/cam/h264"}),
		turbodata.WithStartTimestamp(650),
	)
	if err != nil {
		log.Fatalf("ReadMessages: %v", err)
	}
	fmt.Println("  plain (undecodable, starts mid-GOP):")
	iterate(plain, 0)

	// Decodable: snaps back to K@500 so the sequence starts on a key frame.
	r = openReader()
	decodable, err := r.ReadMessages(
		turbodata.WithTopicNames([]string{"/cam/h264"}),
		turbodata.WithStartTimestamp(650),
		turbodata.WithVideoDecodable(),
	)
	if err != nil {
		log.Fatalf("ReadMessages: %v", err)
	}
	fmt.Println("  decodable (snapped back to the key frame at ts=500):")
	iterate(decodable, 0)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func openReader() *turbodata.Reader {
	f, err := os.Open(inPath)
	if err != nil {
		log.Fatalf("open %s: %v (did you run examples/write first?)", inPath, err)
	}
	// Each demo leaks its file handle on purpose: the example is short and
	// closing the file would close the underlying ReadSource the iterator
	// uses. A real program would manage the lifetime explicitly.
	r := turbodata.NewReader(f)
	return r
}

// iterate pulls every message from the iterator and prints up to limit lines.
// limit=0 means print everything.
func iterate(it interface {
	NextInto(buf *turbodata.ReusableBuffer) (int64, string, error)
}, limit int) {
	buf := turbodata.NewReusableBuffer()
	total := 0
	for {
		ts, name, err := it.NextInto(buf)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			log.Fatalf("NextInto: %v", err)
		}
		total++
		if limit == 0 || total <= limit {
			fmt.Printf("  ts=%4d topic=%-12s data=%q\n", ts, name, snippet(buf.Data))
		}
	}
	if limit > 0 && total > limit {
		fmt.Printf("  ... (%d more messages)\n", total-limit)
	}
	fmt.Printf("  total: %d messages\n", total)
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
