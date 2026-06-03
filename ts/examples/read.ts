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
// The TS SDK is read-only and browser-targeted, but this example runs under
// Node (via tsx) by wrapping a file's bytes in a Blob and the standard
// BlobReadSource. It reads the committed Go-produced test fixtures, so no
// writer step is needed:
//
//   cd ts
//   npm run example:read
//
// ReadOptions covered: defaults, order, topic + time filters, strategy, and
// the video-decodable key-frame snap-back. Each demoXxx is self-contained.

import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import {
  BlobReadSource,
  Reader,
  StrategyForLatency,
  type Message,
  type ReadOptions,
} from "../src/index.js";

const FIXTURES = join(dirname(fileURLToPath(import.meta.url)), "..", "test", "fixtures");
const TXT = new TextDecoder();

// loadReader wraps a fixture's bytes in a Blob-backed source. In a browser
// you'd pass a File from <input type="file"> or an HttpRangeReadSource instead.
function loadReader(fixture: string): Reader {
  const bytes = new Uint8Array(readFileSync(join(FIXTURES, fixture)));
  return new Reader(new BlobReadSource(new Blob([bytes])));
}

function section(title: string): void {
  console.log(`\n--- ${title} ---`);
}

function snippet(data: Uint8Array, max = 24): string {
  const s = TXT.decode(data.subarray(0, max));
  return data.length > max ? `${s}...` : s;
}

async function iterate(reader: Reader, opts: ReadOptions, limit = 0): Promise<void> {
  let total = 0;
  for await (const msg of reader.readMessages(opts)) {
    total += 1;
    if (limit === 0 || total <= limit) {
      const m: Message = msg;
      console.log(`  ts=${m.timestamp} topic=${m.topicName} data=${JSON.stringify(snippet(m.data))}`);
    }
  }
  if (limit > 0 && total > limit) {
    console.log(`  ... (${total - limit} more messages)`);
  }
  console.log(`  total: ${total} messages`);
}

// summary() loads only the file's index, no message bytes.
async function demoSummary(): Promise<void> {
  section("summary only (no message I/O)");
  const reader = loadReader("multi-topic-compressed.td");
  const summary = await reader.summary();
  for (const ti of summary.topicsInfos) {
    const names = ti.topicMetadatas.map((tm) => tm.name);
    console.log(`  group: topics=${JSON.stringify(names)} chunks=${ti.indexChunkInfoList.length}`);
  }
}

async function demoReadAll(): Promise<void> {
  section("read all messages (defaults)");
  await iterate(loadReader("multi-topic-compressed.td"), {}, 5);
}

async function demoReverseOrder(): Promise<void> {
  section("reverse time order (last 5)");
  await iterate(loadReader("multi-topic-compressed.td"), { order: "reverse-time" }, 5);
}

// Topic and time filters combine: only these topics, only this [start, end]
// window (inclusive). Note bigint literals (the SDK uses int64 = bigint).
async function demoFilters(): Promise<void> {
  section("only /imu + /cam, time range [3_000_000, 3_001_000]");
  await iterate(loadReader("multi-topic-compressed.td"), {
    topicNames: ["/imu", "/cam"],
    startTimestamp: 3_000_000n,
    endTimestamp: 3_001_000n,
  });
}

// Pass a strategy to switch onto the cost-aware concurrent path (HTTP Range /
// object storage). Without it, the reader uses the default lazy path.
async function demoStrategy(): Promise<void> {
  section("cost-aware path (StrategyForLatency)");
  await iterate(loadReader("multi-topic-compressed.td"), {
    strategy: StrategyForLatency({
      rttSeconds: 0.05,
      perStreamBytesPerSec: 8_000_000,
      concurrency: 8,
    }),
  }, 0);
}

// Video: video-single-gop.td holds one GOP (K@10, P@20..P@60). A plain start
// of 25 lands after the key frame, so the first emitted frame can't cold-start
// a decoder. videoDecodable snaps the start back to the key frame at ts=10.
async function demoVideoDecodable(): Promise<void> {
  section("video: start=25 mid-GOP, plain vs videoDecodable");
  console.log("  plain (undecodable, starts mid-GOP):");
  await iterate(loadReader("video-single-gop.td"), { startTimestamp: 25n });
  console.log("  decodable (snapped back to the key frame at ts=10):");
  await iterate(loadReader("video-single-gop.td"), { startTimestamp: 25n, videoDecodable: true });
}

async function main(): Promise<void> {
  await demoSummary();
  await demoReadAll();
  await demoReverseOrder();
  await demoFilters();
  await demoStrategy();
  await demoVideoDecodable();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
