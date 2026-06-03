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

// Example: read several turbodata files as one time-ordered stream.
//
//   cd ts
//   npm run example:multiread
//
// MultiReader merges per-file Readers. Topic-name collisions across files are
// resolved per Reader with topicRemap: names meant to union share an exposed
// name; names meant to stay distinct are remapped apart.
//
// Both committed fixtures expose a "/sensor" topic over disjoint time:
//   single-topic-uncompressed.td  -> /sensor at ts 1_000_000 .. 1_029_000
//   single-topic-compressed.td    -> /sensor at ts 2_000_000 .. 2_029_000

import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { BlobReadSource, MultiReader, Reader } from "../src/index.js";

const FIXTURES = join(dirname(fileURLToPath(import.meta.url)), "..", "test", "fixtures");

function loadSource(fixture: string): BlobReadSource {
  const bytes = new Uint8Array(readFileSync(join(FIXTURES, fixture)));
  return new BlobReadSource(new Blob([bytes]));
}

function section(title: string): void {
  console.log(`\n--- ${title} ---`);
}

// Union: no remap. "/sensor" appears in both files over disjoint time, so it
// unions into a single time-ordered stream.
async function demoUnion(): Promise<void> {
  section("union read (shared /sensor across two files)");
  const mr = new MultiReader([
    new Reader(loadSource("single-topic-uncompressed.td")),
    new Reader(loadSource("single-topic-compressed.td")),
  ]);
  let total = 0;
  let first: bigint | undefined;
  let last: bigint | undefined;
  for await (const msg of mr.readMessages()) {
    if (first === undefined) first = msg.timestamp;
    last = msg.timestamp;
    total += 1;
  }
  console.log(`  /sensor: ${total} messages, ts ${first} .. ${last} (file A then file B, merged)`);
}

// Split: remap file B's "/sensor" to "/sensor_v2" so the two stay distinct.
async function demoSplit(): Promise<void> {
  section("split read (file B /sensor remapped to /sensor_v2)");
  const mr = new MultiReader([
    new Reader(loadSource("single-topic-uncompressed.td")),
    new Reader(loadSource("single-topic-compressed.td"), { topicRemap: { "/sensor": "/sensor_v2" } }),
  ]);
  const counts = new Map<string, number>();
  for await (const msg of mr.readMessages()) {
    counts.set(msg.topicName, (counts.get(msg.topicName) ?? 0) + 1);
  }
  for (const [name, n] of counts) {
    console.log(`  topic=${name} messages=${n}`);
  }
}

// Sample across files: for each timestamp the result is the latest floor
// across the union of both files.
async function demoSample(): Promise<void> {
  section("multi sample of /sensor (latest floor across files)");
  const mr = new MultiReader([
    new Reader(loadSource("single-topic-uncompressed.td")),
    new Reader(loadSource("single-topic-compressed.td")),
  ]);
  const ts = [1_005_000n, 2_005_000n, 2_999_999n];
  const out = await mr.sample([{ topic: "/sensor", timestamps: ts }]);
  out[0]!.forEach((res, i) => {
    if (!res.found) {
      console.log(`  T=${ts[i]} -> (no floor)`);
      return;
    }
    console.log(`  T=${ts[i]} -> ts=${res.timestamp}`);
  });
}

async function main(): Promise<void> {
  await demoUnion();
  await demoSplit();
  await demoSample();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
