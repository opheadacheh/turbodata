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

import { Reader, StrategyForLatency } from "../src/index.js";

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
