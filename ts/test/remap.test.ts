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

// Topic remap tests. Mirrors go/remap_test.go.
//
// The TS SDK has no Writer, so these are driven off the Go-produced
// multi-group-mixed fixture (topics: "/groupA/data", "/groupB/x",
// "/groupB/y"). We verify that remap only rewrites exposed names without
// changing message order/payloads, that topicNames filtering and sample()
// operate in exposed-name space, and that colliding remaps throw.

import { createHash } from "node:crypto";

import { describe, expect, it } from "vitest";

import { Reader, TopicRemapError, type SampleQuery } from "../src/index.js";

import { BytesReadSource, readFixture } from "./helpers.js";

const FIXTURE = "multi-group-mixed.td";
const REMAP = { "/groupA/data": "/groupA/data_v2" };

function hex(data: Uint8Array): string {
  return createHash("sha256").update(data).digest("hex");
}

interface Row {
  ts: bigint;
  name: string;
  data: string;
}

async function collect(reader: Reader, opts = {}): Promise<Row[]> {
  const out: Row[] = [];
  for await (const msg of reader.readMessages({ copy: true, ...opts })) {
    out.push({ ts: msg.timestamp, name: msg.topicName, data: hex(msg.data) });
  }
  return out;
}

describe("topic remap", () => {
  it("rewrites only exposed names, preserving order and payloads", async () => {
    const bytes = readFixture(FIXTURE);
    const base = await collect(new Reader(new BytesReadSource(bytes)));
    const remapped = await collect(
      new Reader(new BytesReadSource(bytes), { topicRemap: REMAP }),
    );

    expect(remapped.length).toBe(base.length);
    for (let i = 0; i < base.length; i++) {
      const expectedName =
        base[i]!.name === "/groupA/data" ? "/groupA/data_v2" : base[i]!.name;
      expect(remapped[i]!.name).toBe(expectedName);
      expect(remapped[i]!.ts).toBe(base[i]!.ts);
      expect(remapped[i]!.data).toBe(base[i]!.data);
    }
    // The in-file name must no longer appear.
    expect(remapped.some((r) => r.name === "/groupA/data")).toBe(false);
  });

  it("topicNames filter accepts the exposed name", async () => {
    const bytes = readFixture(FIXTURE);
    const reader = new Reader(new BytesReadSource(bytes), { topicRemap: REMAP });
    const rows = await collect(reader, { topicNames: ["/groupA/data_v2"] });

    expect(rows.length).toBeGreaterThan(0);
    expect(rows.every((r) => r.name === "/groupA/data_v2")).toBe(true);

    // An exposed name that maps to nothing matches nothing.
    const reader2 = new Reader(new BytesReadSource(bytes), {
      topicRemap: REMAP,
    });
    const none = await collect(reader2, { topicNames: ["/nope"] });
    expect(none.length).toBe(0);
  });

  it("summary reports exposed names", async () => {
    const bytes = readFixture(FIXTURE);
    const reader = new Reader(new BytesReadSource(bytes), { topicRemap: REMAP });
    const summary = await reader.summary();
    const names = new Set<string>();
    for (const ti of summary.topicsInfos) {
      for (const tm of ti.topicMetadatas) {
        names.add(tm.name);
      }
    }
    expect(names.has("/groupA/data_v2")).toBe(true);
    expect(names.has("/groupB/x")).toBe(true);
    expect(names.has("/groupA/data")).toBe(false);
  });

  it("throws on a colliding remap (lazily, on first use)", async () => {
    const bytes = readFixture(FIXTURE);
    // Rename "/groupB/x" onto the untouched in-file name "/groupB/y".
    const reader = new Reader(new BytesReadSource(bytes), {
      topicRemap: { "/groupB/x": "/groupB/y" },
    });
    await expect(reader.summary()).rejects.toThrow(TopicRemapError);
    await expect(reader.summary()).rejects.toThrow(/duplicate exposed name/);

    const reader2 = new Reader(new BytesReadSource(bytes), {
      topicRemap: { "/groupB/x": "/groupB/y" },
    });
    await expect(
      (async () => {
        for await (const _ of reader2.readMessages()) {
          // drain
        }
      })(),
    ).rejects.toThrow(TopicRemapError);
  });

  it("sample() accepts exposed names and matches the in-file result", async () => {
    const bytes = readFixture(FIXTURE);
    // Ground truth: read /groupA/data messages with no remap.
    const base = await collect(new Reader(new BytesReadSource(bytes)));
    const first = base.find((r) => r.name === "/groupA/data");
    expect(first).toBeDefined();

    const reader = new Reader(new BytesReadSource(bytes), { topicRemap: REMAP });
    const query: SampleQuery = {
      topic: "/groupA/data_v2",
      timestamps: [first!.ts],
    };
    const out = await reader.sample([query]);
    expect(out[0]!.length).toBe(1);
    expect(out[0]![0]!.found).toBe(true);
    expect(out[0]![0]!.timestamp).toBe(first!.ts);
    expect(hex(out[0]![0]!.data)).toBe(first!.data);
  });

  it("no remap is identity", async () => {
    const bytes = readFixture(FIXTURE);
    const base = await collect(new Reader(new BytesReadSource(bytes)));
    const identity = await collect(
      new Reader(new BytesReadSource(bytes), { topicRemap: {} }),
    );
    expect(identity).toEqual(base);
  });
});
