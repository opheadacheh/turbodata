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

// Sample tests.
//
// Verification strategy (Karpathy rule 4): the TS SDK has no Writer, so we
// can't synthesize tailored files like the Go/Python sample tests do. Instead
// we drive correctness off the Go-produced fixtures:
//
//   - Full-read each fixture into per-topic ascending (ts -> data) lists. That
//     list is the ground truth for floor lookups.
//   - Build a strictly-increasing query timestamp set per topic that covers
//     before-first (not found), exact hits, between-message floors, and
//     after-last. Compare Reader.sample against a reference floor computed
//     from the ground-truth list.
//   - Run under several strategies (default, serial, tiny-coalesce/high-fanout)
//     so the Phase A multi-wave + Phase B coalescing/concurrency paths all
//     execute and must agree.
//
// Plus focused unit tests for validation aggregation and linSpaceTimestamps.

import { createHash } from "node:crypto";

import { describe, expect, it } from "vitest";

import {
  DEFAULT_SAMPLE_STRATEGY,
  Reader,
  SampleValidationError,
  linSpaceTimestamps,
  type ReadStrategy,
  type SampleQuery,
} from "../src/index.js";

import { BytesReadSource, listFixtures, readFixture } from "./helpers.js";

interface Entry {
  ts: bigint;
  data: Uint8Array;
}

function hex(data: Uint8Array): string {
  return createHash("sha256").update(data).digest("hex");
}

async function collectByTopic(
  bytes: Uint8Array,
): Promise<Map<string, Entry[]>> {
  const reader = new Reader(new BytesReadSource(bytes));
  const out = new Map<string, Entry[]>();
  for await (const msg of reader.readMessages({ copy: true })) {
    let arr = out.get(msg.topicName);
    if (arr === undefined) {
      arr = [];
      out.set(msg.topicName, arr);
    }
    arr.push({ ts: msg.timestamp, data: msg.data });
  }
  return out;
}

/** Reference floor: greatest entry with ts <= T (last one, to match dup-ts). */
function referenceFloor(entries: Entry[], T: bigint): Entry | undefined {
  let res: Entry | undefined;
  for (const e of entries) {
    if (e.ts <= T) {
      res = e;
    } else {
      break; // entries are ascending by timestamp.
    }
  }
  return res;
}

/** A strictly-increasing query set covering the interesting boundaries. */
function queryTimestamps(entries: Entry[]): bigint[] {
  const set = new Set<bigint>();
  set.add(entries[0]!.ts - 1n); // before first -> not found
  for (let i = 0; i < entries.length; i++) {
    set.add(entries[i]!.ts); // exact hits
    if (i + 1 < entries.length) {
      const gap = entries[i + 1]!.ts - entries[i]!.ts;
      if (gap >= 2n) {
        set.add(entries[i]!.ts + 1n); // between -> floors to entries[i]
      }
    }
  }
  set.add(entries[entries.length - 1]!.ts + 100n); // after last -> last msg
  return [...set].sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
}

const strategies: { name: string; strategy: ReadStrategy }[] = [
  { name: "default", strategy: DEFAULT_SAMPLE_STRATEGY },
  {
    name: "serial-no-coalesce",
    strategy: { coalesceGap: 0n, splitThreshold: 0n, maxConcurrency: 1 },
  },
  {
    name: "tiny-coalesce-high-fanout",
    strategy: {
      coalesceGap: 8n,
      splitThreshold: 16n,
      maxConcurrency: 8,
    },
  },
];

describe("Reader.sample on Go fixtures", () => {
  const fixtures = listFixtures();

  it("at least one fixture exists", () => {
    expect(fixtures.length).toBeGreaterThan(0);
  });

  for (const name of fixtures) {
    for (const { name: stratName, strategy } of strategies) {
      it(`${name} [${stratName}]: floor results match a full-read reference`, async () => {
        const bytes = readFixture(name);
        const byTopic = await collectByTopic(bytes);

        // One query per topic, batched into a single sample() call.
        const topics = [...byTopic.keys()];
        const queries: SampleQuery[] = topics.map((topic) => ({
          topic,
          timestamps: queryTimestamps(byTopic.get(topic)!),
        }));

        const reader = new Reader(new BytesReadSource(bytes));
        const out = await reader.sample(queries, { strategy });

        expect(out.length).toBe(queries.length);
        for (let qi = 0; qi < queries.length; qi++) {
          const q = queries[qi]!;
          const entries = byTopic.get(q.topic)!;
          const row = out[qi]!;
          expect(row.length).toBe(q.timestamps.length);
          for (let j = 0; j < q.timestamps.length; j++) {
            const got = row[j]!;
            const want = referenceFloor(entries, q.timestamps[j]!);
            if (want === undefined) {
              expect(got.found).toBe(false);
            } else {
              expect(got.found).toBe(true);
              expect(got.timestamp).toBe(want.ts);
              expect(hex(got.data)).toBe(hex(want.data));
            }
          }
        }
      });
    }
  }

  it("same-chunk timestamps don't re-read the same bytes", async () => {
    // Pick the fixture's first topic, query several timestamps that all fall
    // on existing messages. Reads after summary priming should be small
    // (a couple index-chunk + data ops), never one-read-per-timestamp.
    const name = listFixtures()[0]!;
    const bytes = readFixture(name);
    const byTopic = await collectByTopic(bytes);
    const [topic, entries] = [...byTopic.entries()][0]!;

    const src = new BytesReadSource(bytes);
    const reader = new Reader(src);
    await reader.summary(); // prime summary load
    const before = src.reads;

    const timestamps = entries.map((e) => e.ts); // every existing timestamp
    const out = await reader.sample([{ topic, timestamps }]);

    expect(out[0]!.length).toBe(timestamps.length);
    for (let j = 0; j < timestamps.length; j++) {
      expect(out[0]![j]!.found).toBe(true);
      expect(out[0]![j]!.timestamp).toBe(entries[j]!.ts);
    }
    // Way fewer reads than timestamps: dedup + coalescing collapse them.
    const used = src.reads - before;
    expect(used).toBeLessThanOrEqual(Math.max(4, timestamps.length));
    expect(used).toBeLessThan(timestamps.length + 1);
  });
});

describe("Reader.sample validation", () => {
  it("aggregates every precondition violation", async () => {
    const name = listFixtures()[0]!;
    const bytes = readFixture(name);
    const reader = new Reader(new BytesReadSource(bytes));
    const summary = await reader.summary();
    const realTopic = summary.topicsInfos[0]!.topicMetadatas[0]!.name;

    const queries: SampleQuery[] = [
      { topic: "ghost-topic", timestamps: [10n] }, // unknown
      { topic: realTopic, timestamps: [30n, 20n] }, // not increasing
      { topic: realTopic, timestamps: [40n] }, // duplicate topic
    ];

    const src = new BytesReadSource(bytes);
    const reader2 = new Reader(src);
    await reader2.summary();
    const before = src.reads;

    await expect(reader2.sample(queries)).rejects.toBeInstanceOf(
      SampleValidationError,
    );

    let caught: SampleValidationError | undefined;
    try {
      await reader2.sample(queries);
    } catch (err) {
      caught = err as SampleValidationError;
    }
    expect(caught).toBeInstanceOf(SampleValidationError);
    const v = caught!.violations;
    expect(v.some((s) => s.includes("queries[0]") && s.includes("unknown"))).toBe(
      true,
    );
    expect(
      v.some(
        (s) => s.includes("queries[1]") && s.includes("strictly increasing"),
      ),
    ).toBe(true);
    expect(
      v.some((s) => s.includes("queries[2]") && s.includes("duplicate")),
    ).toBe(true);

    // Validation must not trigger any data reads beyond the summary load.
    expect(src.reads).toBe(before);
  });

  it("empty timestamps yields an empty row and reads no data", async () => {
    const name = listFixtures()[0]!;
    const bytes = readFixture(name);
    const reader = new Reader(new BytesReadSource(bytes));
    const summary = await reader.summary();
    const topic = summary.topicsInfos[0]!.topicMetadatas[0]!.name;

    const src = new BytesReadSource(bytes);
    const reader2 = new Reader(src);
    await reader2.summary();
    const before = src.reads;

    const out = await reader2.sample([{ topic, timestamps: [] }]);
    expect(out.length).toBe(1);
    expect(out[0]).toEqual([]);
    expect(src.reads).toBe(before);
  });
});

describe("linSpaceTimestamps", () => {
  it("produces count values stride apart", () => {
    expect(linSpaceTimestamps(100n, 10n, 5)).toEqual([
      100n,
      110n,
      120n,
      130n,
      140n,
    ]);
  });

  it("is strictly increasing", () => {
    const out = linSpaceTimestamps(1000n, 1n, 1000);
    for (let i = 1; i < out.length; i++) {
      expect(out[i]! > out[i - 1]!).toBe(true);
    }
  });

  it("returns [] for count <= 0", () => {
    expect(linSpaceTimestamps(0n, 1n, 0)).toEqual([]);
    expect(linSpaceTimestamps(0n, 1n, -5)).toEqual([]);
  });

  it("throws for stride <= 0", () => {
    expect(() => linSpaceTimestamps(0n, 0n, 3)).toThrow();
    expect(() => linSpaceTimestamps(0n, -1n, 3)).toThrow();
  });
});
