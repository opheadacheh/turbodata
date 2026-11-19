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

// Reader integration tests against the Go-produced fixtures.
//
// Verification strategy (Karpathy rule 4):
//   - The Go gen-fixtures program emitted each .td file plus a .golden.txt
//     containing a stream sha256 over the full default-path iteration. We
//     re-compute that same hash here and assert equality. Bit-for-bit
//     correctness of every yielded (timestamp, topicName, data) is captured
//     in one number; if any field is wrong, the hash diverges.
//   - We also assert message count, first/last lines (richer error messages
//     when the hash mismatches), topic list parity.

import { describe, expect, it } from "vitest";

import { HttpRangeReadSource, Reader, StrategyForLatency, type IndexEntry } from "../src/index.js";

import {
  BytesReadSource,
  iterateAndHash,
  listFixtures,
  readFixture,
  readGolden,
} from "./helpers.js";

describe("Reader on Go fixtures", () => {
  const fixtures = listFixtures();

  it("at least one fixture exists", () => {
    expect(fixtures.length).toBeGreaterThan(0);
  });

  for (const name of fixtures) {
    describe(name, () => {
      it("summary topics match golden", async () => {
        const bytes = readFixture(name);
        const golden = readGolden(name);
        const reader = new Reader(new BytesReadSource(bytes));
        const summary = await reader.summary();

        const flat: { id: number; name: string }[] = [];
        for (const ti of summary.topicsInfos) {
          for (const tm of ti.topicMetadatas) {
            flat.push({ id: tm.id, name: tm.name });
          }
        }
        flat.sort((a, b) => a.id - b.id);

        expect(flat.length).toBe(golden.topics.length);
        for (let i = 0; i < flat.length; i++) {
          expect(flat[i]!.id).toBe(golden.topics[i]!.id);
          expect(flat[i]!.name).toBe(golden.topics[i]!.name);
        }
      });

      it("default-path stream hash matches golden", async () => {
        const bytes = readFixture(name);
        const golden = readGolden(name);
        const reader = new Reader(new BytesReadSource(bytes));
        const stats = await iterateAndHash(reader);
        expect(stats.count).toBe(golden.messageCount);
        expect(stats.firstLine).toBe(golden.firstLine);
        expect(stats.lastLine).toBe(golden.lastLine);
        expect(stats.hash).toBe(golden.streamSha256);
      });

      it("cost-aware path produces the same hash as default path", async () => {
        const bytes = readFixture(name);
        const golden = readGolden(name);
        const reader = new Reader(new BytesReadSource(bytes));
        // Use a moderate strategy: enough coalescing to merge index+data
        // in some groups, splitting tuned tiny so the split path is also
        // exercised on larger fixtures.
        const strat = StrategyForLatency({
          rttSeconds: 0.001,
          perStreamBytesPerSec: 1_000_000,
          concurrency: 4,
        });
        const stats = await iterateAndHash(reader, { strategy: strat });
        expect(stats.count).toBe(golden.messageCount);
        expect(stats.hash).toBe(golden.streamSha256);
      });

      it("copy:true yields independent buffers", async () => {
        const bytes = readFixture(name);
        const reader = new Reader(new BytesReadSource(bytes));
        const collected: Uint8Array[] = [];
        for await (const msg of reader.readMessages({ copy: true })) {
          collected.push(msg.data);
          if (collected.length >= 5) {
            break;
          }
        }
        // After breaking, all collected buffers must still be readable and
        // distinct (no two share underlying memory).
        for (let i = 0; i < collected.length; i++) {
          for (let j = i + 1; j < collected.length; j++) {
            expect(collected[i]!.buffer).not.toBe(collected[j]!.buffer);
          }
        }
      });

      it("startTimestamp filter keeps only matching messages", async () => {
        const bytes = readFixture(name);
        const reader = new Reader(new BytesReadSource(bytes));

        // Pick a cutoff at the second-to-last message timestamp via a first pass.
        const all: bigint[] = [];
        for await (const m of reader.readMessages()) {
          all.push(m.timestamp);
        }
        if (all.length < 2) {
          return;
        }
        const cutoff = all[Math.floor(all.length / 2)]!;

        const reader2 = new Reader(new BytesReadSource(bytes));
        let kept = 0;
        for await (const m of reader2.readMessages({ startTimestamp: cutoff })) {
          expect(m.timestamp).toBeGreaterThanOrEqual(cutoff);
          kept++;
        }
        const expected = all.filter((t) => t >= cutoff).length;
        expect(kept).toBe(expected);
      });

      it("topicNames filter restricts output to listed topics", async () => {
        const bytes = readFixture(name);
        const reader = new Reader(new BytesReadSource(bytes));
        const summary = await reader.summary();
        const allNames: string[] = [];
        for (const ti of summary.topicsInfos) {
          for (const tm of ti.topicMetadatas) {
            allNames.push(tm.name);
          }
        }
        if (allNames.length === 0) {
          return;
        }
        const wanted = [allNames[0]!];

        const reader2 = new Reader(new BytesReadSource(bytes));
        for await (const m of reader2.readMessages({ topicNames: wanted })) {
          expect(wanted).toContain(m.topicName);
        }
      });
    });
  }
});

describe("readIndex", () => {
  for (const [file, topic, startTimestamp, endTimestamp, videoDecodable] of [
    ["multi-group-mixed.td", "/groupB/y", 4000000n, 4004700n, true],
    ["video-multi-gop-chunks.td", "/cam", 20n, 60n, false],
    ["video-multi-gop-chunks.td", "/cam", 20n, 60n, true],
  ] as const) {
    it(`matches message metadata without payload reads on ${file} (videoDecodable=${videoDecodable})`, async () => {
      const bytes = readFixture(file);
      const remap = { topicRemap: { [topic]: "renamed" } };
      const reference = new Reader(new BytesReadSource(bytes), remap);
      const summary = await reference.summary();
      const indexStart = summary.topicsInfos[0]!.indexChunkInfoList[0]!.offset;
      class IndexRegionSource extends BytesReadSource {
        override async read(offset: bigint, length: bigint) {
          expect(offset).toBeGreaterThanOrEqual(indexStart);
          return super.read(offset, length);
        }
      }
      for (const order of ["time", "reverse-time"] as const) {
        for (const topicNames of [undefined, ["renamed"], ["missing"]]) {
          const query = { order, topicNames, startTimestamp, endTimestamp, videoDecodable };
          const expected: IndexEntry[] = [];
          for await (const { topicName, timestamp, isKeyFrame } of reference.readMessages(query)) {
            expected.push({ topicName, timestamp, isKeyFrame });
          }
          for (const strategy of [undefined, { coalesceGap: 1n << 20n, splitThreshold: 64n, maxConcurrency: 3 }]) {
            const reader = new Reader(new IndexRegionSource(bytes), remap);
            const entries: IndexEntry[] = [];
            for await (const entry of reader.readIndex({ ...query, strategy, copy: true })) {
              entries.push(entry);
            }
            expect(entries).toEqual(expected);
          }
        }
      }
    });
  }
});

it("tailPrefetch loads the index scan's footer and summary in one request", async () => {
  const bytes = readFixture("video-multi-gop-chunks.td");
  for (const [tailPrefetch, expectedReads] of [[0n, 2], [BigInt(bytes.length), 1]] as const) {
    const source = new BytesReadSource(bytes);
    const entries = new Reader(source).readIndex({ tailPrefetch, topicNames: ["missing"] });
    expect((await entries.next()).done).toBe(true);
    expect(source.reads).toBe(expectedReads);
  }
});

it("HTTP index scan stays in the metadata region when HEAD is unavailable", async () => {
  const bytes = readFixture("video-multi-gop-chunks.td");
  const reference = new Reader(new BytesReadSource(bytes));
  const summary = await reference.summary();
  const indexStart = Number(summary.topicsInfos[0]!.indexChunkInfoList[0]!.offset);
  const source = new HttpRangeReadSource("https://example.test/video.td", {
    async fetch(_input, init) {
      if (init?.method === "HEAD") {
        return new Response(null, { status: 405 });
      }
      const range = new Headers(init?.headers).get("Range");
      if (range === "bytes=-1") {
        const last = bytes.length - 1;
        return new Response(bytes.slice(last), {
          status: 206, headers: { "Content-Range": `bytes ${last}-${last}/${bytes.length}` },
        });
      }
      const [, start, end] = /^bytes=(\d+)-(\d+)$/.exec(range ?? "")!;
      expect(Number(start)).toBeGreaterThanOrEqual(indexStart);
      return new Response(bytes.slice(Number(start), Number(end) + 1), { status: 206 });
    },
  });
  const timestamps: bigint[] = [];
  for await (const entry of new Reader(source).readIndex()) timestamps.push(entry.timestamp);
  expect(timestamps).toEqual([10n, 20n, 30n, 40n, 50n, 60n]);
});

it("stopping at an index chunk boundary leaves later chunks unread", async () => {
  const source = new BytesReadSource(readFixture("video-multi-gop-chunks.td"));
  const reader = new Reader(source);
  await reader.summary();
  const before = source.reads;
  const entries = reader.readIndex();
  expect((await entries.next()).value.timestamp).toBe(10n);
  expect((await entries.next()).value.timestamp).toBe(20n);
  await entries.return?.();
  expect(source.reads - before).toBe(1);
});
