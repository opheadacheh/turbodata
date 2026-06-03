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
// Reader.sample returns the floor message per (topic, timestamp): the most
// recent message at or before each requested timestamp. Use it for "the state
// at time T" rather than "every message in [t0, t1]".
//
//   cd ts
//   npm run example:sample
//
// Covers single/multi-topic floor lookup, a custom strategy, and the
// video-decodable GOP-prefix semantics. Runs under Node via tsx by wrapping a
// committed fixture's bytes in a BlobReadSource.

import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import {
  BlobReadSource,
  Reader,
  StrategyForLatency,
  type SampleResult,
} from "../src/index.js";

const FIXTURES = join(dirname(fileURLToPath(import.meta.url)), "..", "test", "fixtures");
const TXT = new TextDecoder();

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

function printResults(topic: string, queriedAt: bigint[], results: SampleResult[]): void {
  results.forEach((res, i) => {
    if (!res.found) {
      console.log(`  ${topic}@T=${queriedAt[i]}  (no message at or before T)`);
      return;
    }
    console.log(`  ${topic}@T=${queriedAt[i]}  -> ts=${res.timestamp} data=${JSON.stringify(snippet(res.data))}`);
  });
}

// /cam in multi-topic-compressed.td is written at ts 3_000_000, 3_000_300, ...
async function demoSingleTopic(): Promise<void> {
  section("single topic, three timestamps");
  const reader = loadReader("multi-topic-compressed.td");
  const ts = [3_000_000n, 3_001_000n, 3_003_300n];
  const out = await reader.sample([{ topic: "/cam", timestamps: ts }]);
  printResults("/cam", ts, out[0]!);
}

async function demoMultiTopic(): Promise<void> {
  section("multi-topic in one sample call");
  const reader = loadReader("multi-topic-compressed.td");
  const ts = [3_001_500n, 3_002_500n];
  const out = await reader.sample([
    { topic: "/cam", timestamps: ts },
    { topic: "/imu", timestamps: ts },
    { topic: "/lidar", timestamps: ts },
  ]);
  printResults("/cam", ts, out[0]!);
  printResults("/imu", ts, out[1]!);
  printResults("/lidar", ts, out[2]!);
}

// Pass a strategy to override the default sample cost model for your backend.
async function demoStrategy(): Promise<void> {
  section("custom sample strategy");
  const reader = loadReader("multi-topic-compressed.td");
  const ts = [3_000_000n, 3_001_500n, 3_003_000n];
  const out = await reader.sample([{ topic: "/imu", timestamps: ts }], {
    strategy: StrategyForLatency({ rttSeconds: 0.05, perStreamBytesPerSec: 8_000_000, concurrency: 4 }),
  });
  printResults("/imu", ts, out[0]!);
}

// Video: video-multi-gop-chunks.td holds two GOPs (A: KA@10,A1@20,A2@30;
// B: KB@40,B1@50,B2@60). With videoDecodable, each result carries a
// decoder-ready GOP sequence in `frames` (data is empty): the first result in
// each GOP sets resetDecoder=true with the full prefix [keyframe ... target];
// later results in the same GOP carry only the new frames.
async function demoVideoDecodable(): Promise<void> {
  section("video: GOP-prefix sampling (videoDecodable)");
  const reader = loadReader("video-multi-gop-chunks.td");
  const ts = [25n, 55n]; // 25 -> GOP A (floor A1@20), 55 -> GOP B (floor B1@50)
  const out = await reader.sample([{ topic: "/cam", timestamps: ts }], { videoDecodable: true });
  out[0]!.forEach((res, i) => {
    if (!res.found) {
      console.log(`  /cam@T=${ts[i]}  (no key frame at or before T)`);
      return;
    }
    const frameTs = res.frames.map((f) => f.timestamp.toString());
    console.log(`  /cam@T=${ts[i]}  -> target ts=${res.timestamp} reset=${res.resetDecoder} frames=[${frameTs.join(", ")}]`);
  });
}

async function main(): Promise<void> {
  await demoSingleTopic();
  await demoMultiTopic();
  await demoStrategy();
  await demoVideoDecodable();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
