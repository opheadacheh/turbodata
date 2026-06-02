// MultiReader tests. Mirrors go/multi_reader_test.go and
// py/tests/test_multi_reader.py.
//
// The TS SDK has no Writer, so these are driven off the Go-produced fixtures.
// Two readers over time-disjoint /sensor files (single-topic-uncompressed.td
// at ts 1.00M..1.029M and single-topic-compressed.td at ts 2.00M..2.029M)
// stand in for the synthetic "same topic, disjoint time" files the Go/Python
// tests build in memory. The /cam video fixtures (all ts 10..60) exercise the
// overlap path; remap splits them apart for the disjoint case.

import { createHash } from "node:crypto";

import { describe, expect, it } from "vitest";

import {
  MultiReader,
  Reader,
  SampleValidationError,
  VideoSourcesOverlapError,
  type Message,
  type ReadOptions,
  type SampleResult,
} from "../src/index.js";

import { BytesReadSource, readFixture } from "./helpers.js";

const SENSOR_A = "single-topic-uncompressed.td"; // /sensor, ts 1000000..1029000
const SENSOR_B = "single-topic-compressed.td"; // /sensor, ts 2000000..2029000
const MULTI = "multi-topic-compressed.td"; // /cam, /imu, /lidar, ts ~3000000
const VIDEO_A = "video-single-gop.td"; // /cam, ts 10..60
const VIDEO_B = "video-multi-gop-chunks.td"; // /cam, ts 10..60

function hex(data: Uint8Array): string {
  return createHash("sha256").update(data).digest("hex");
}

function reader(name: string, opts = {}): Reader {
  return new Reader(new BytesReadSource(readFixture(name)), opts);
}

interface Row {
  ts: bigint;
  name: string;
  data: string;
}

async function drain(
  it: AsyncIterableIterator<Message>,
): Promise<Row[]> {
  const out: Row[] = [];
  for await (const msg of it) {
    out.push({ ts: msg.timestamp, name: msg.topicName, data: hex(msg.data) });
  }
  return out;
}

function collectMulti(mr: MultiReader, opts: ReadOptions = {}): Promise<Row[]> {
  return drain(mr.readMessages({ copy: true, ...opts }));
}

describe("MultiReader construction", () => {
  it("requires at least one reader", () => {
    expect(() => new MultiReader([])).toThrow(/at least one reader/);
  });
});

describe("MultiReader readMessages", () => {
  it("merges the same topic over disjoint time into one ordered stream", async () => {
    const mr = new MultiReader([reader(SENSOR_A), reader(SENSOR_B)]);
    const rows = await collectMulti(mr);

    expect(rows.length).toBe(60);
    expect(rows.every((r) => r.name === "/sensor")).toBe(true);
    // Strictly ascending across the merge boundary.
    for (let i = 1; i < rows.length; i++) {
      expect(rows[i]!.ts > rows[i - 1]!.ts).toBe(true);
    }
    // First half from A (~1M), second half from B (~2M).
    expect(rows[0]!.ts).toBe(1000000n);
    expect(rows[29]!.ts).toBe(1029000n);
    expect(rows[30]!.ts).toBe(2000000n);
    expect(rows[59]!.ts).toBe(2029000n);
  });

  it("merges disjoint topics into the time-ordered union", async () => {
    const mr = new MultiReader([reader(SENSOR_A), reader(MULTI)]);
    const merged = await collectMulti(mr);

    // Ground truth: read each reader alone and merge by timestamp in JS.
    const a = await drain(reader(SENSOR_A).readMessages({ copy: true }));
    const b = await drain(reader(MULTI).readMessages({ copy: true }));
    const expected = [...a, ...b].sort((x, y) =>
      x.ts < y.ts ? -1 : x.ts > y.ts ? 1 : 0,
    );

    expect(merged.length).toBe(a.length + b.length);
    expect(merged).toEqual(expected);
  });

  it("unions a shared name but a per-reader remap keeps it distinct", async () => {
    const union = new MultiReader([reader(SENSOR_A), reader(SENSOR_B)]);
    const um = await collectMulti(union);
    expect(um.length).toBe(60);
    expect(um.every((r) => r.name === "/sensor")).toBe(true);

    const split = new MultiReader([
      reader(SENSOR_A),
      reader(SENSOR_B, { topicRemap: { "/sensor": "/sensor_b" } }),
    ]);
    const sm = await collectMulti(split);
    const names: Record<string, number> = {};
    for (const r of sm) {
      names[r.name] = (names[r.name] ?? 0) + 1;
    }
    expect(names).toEqual({ "/sensor": 30, "/sensor_b": 30 });
  });

  it("merges in reverse-time order", async () => {
    const mr = new MultiReader([reader(SENSOR_A), reader(SENSOR_B)]);
    const rows = await collectMulti(mr, { order: "reverse-time" });

    expect(rows.length).toBe(60);
    expect(rows[0]!.ts).toBe(2029000n);
    expect(rows[59]!.ts).toBe(1000000n);
    for (let i = 1; i < rows.length; i++) {
      expect(rows[i]!.ts < rows[i - 1]!.ts).toBe(true);
    }
  });

  it("forwards topicNames filtering (by exposed name) to each reader", async () => {
    const mr = new MultiReader([reader(SENSOR_A), reader(MULTI)]);
    const rows = await collectMulti(mr, { topicNames: ["/imu"] });
    expect(rows.length).toBeGreaterThan(0);
    expect(rows.every((r) => r.name === "/imu")).toBe(true);
  });
});

describe("MultiReader sample", () => {
  it("resolves each timestamp to the latest floor across the union", async () => {
    const mr = new MultiReader([reader(SENSOR_A), reader(SENSOR_B)]);
    const ts = [1000000n, 1500000n, 2000000n, 2050000n];
    const out = await mr.sample([{ topic: "/sensor", timestamps: ts }]);
    const row = out[0]!;

    expect(row.map((r) => r.found)).toEqual([true, true, true, true]);
    // 1.00M -> A exact; 1.5M -> A's last (no B floor); 2.00M -> B exact wins
    // over A's last; 2.05M -> B's last wins.
    expect(row.map((r) => r.timestamp)).toEqual([
      1000000n,
      1029000n,
      2000000n,
      2029000n,
    ]);

    // Provenance: the B-winning cells carry B's data, not A's.
    const bAlone = await reader(SENSOR_B).sample([
      { topic: "/sensor", timestamps: [2000000n, 2050000n] },
    ]);
    expect(hex(row[2]!.data)).toBe(hex(bAlone[0]![0]!.data));
    expect(hex(row[3]!.data)).toBe(hex(bAlone[0]![1]!.data));
  });

  it("returns not-found before any reader's first message", async () => {
    const mr = new MultiReader([reader(SENSOR_A), reader(SENSOR_B)]);
    const out = await mr.sample([{ topic: "/sensor", timestamps: [1n] }]);
    expect(out[0]![0]!.found).toBe(false);
  });

  it("rejects a topic held by no reader", async () => {
    const mr = new MultiReader([reader(SENSOR_A)]);
    await expect(
      mr.sample([{ topic: "/nope", timestamps: [1n] }]),
    ).rejects.toThrow(SampleValidationError);
  });

  it("rejects non-strictly-increasing timestamps", async () => {
    const mr = new MultiReader([reader(SENSOR_A)]);
    await expect(
      mr.sample([{ topic: "/sensor", timestamps: [10n, 10n] }]),
    ).rejects.toThrow(SampleValidationError);
  });
});

describe("MultiReader video", () => {
  it("rejects overlapping video sources under videoDecodable (read)", async () => {
    const mr = new MultiReader([reader(VIDEO_A), reader(VIDEO_B)]);
    // /cam is video in both files over the same ts range [10,60] -> overlap.
    await expect(collectMulti(mr, { videoDecodable: true })).rejects.toThrow(
      VideoSourcesOverlapError,
    );

    // Without the option the merge is allowed (plain interleave, no contract).
    const plain = await collectMulti(new MultiReader([reader(VIDEO_A), reader(VIDEO_B)]));
    expect(plain.length).toBe(12);
    expect(plain.every((r) => r.name === "/cam")).toBe(true);
  });

  it("rejects overlapping video sources under videoDecodable (sample)", async () => {
    const mr = new MultiReader([reader(VIDEO_A), reader(VIDEO_B)]);
    await expect(
      mr.sample([{ topic: "/cam", timestamps: [35n] }], { videoDecodable: true }),
    ).rejects.toThrow(VideoSourcesOverlapError);

    // Without the decodable option, plain floor semantics across the union.
    const out = await new MultiReader([reader(VIDEO_A), reader(VIDEO_B)]).sample([
      { topic: "/cam", timestamps: [35n] },
    ]);
    expect(out[0]![0]!.found).toBe(true);
  });

  it("does not check an overlapping video topic that is out of scope", async () => {
    const mr = new MultiReader([reader(VIDEO_A), reader(VIDEO_B)]);
    // /cam overlaps, but only a non-existent topic is requested: no error,
    // empty stream.
    const rows = await collectMulti(mr, {
      videoDecodable: true,
      topicNames: ["/not-cam"],
    });
    expect(rows.length).toBe(0);

    // Bringing /cam into scope re-triggers the overlap error.
    await expect(
      collectMulti(new MultiReader([reader(VIDEO_A), reader(VIDEO_B)]), {
        videoDecodable: true,
        topicNames: ["/cam"],
      }),
    ).rejects.toThrow(VideoSourcesOverlapError);
  });

  it("allows decodable video when remap splits the topics apart", async () => {
    // Distinct exposed names => each video topic is held by one reader, so the
    // disjoint check is a no-op and decodable read/sample succeed.
    const mr = new MultiReader([
      reader(VIDEO_A, { topicRemap: { "/cam": "/camA" } }),
      reader(VIDEO_B, { topicRemap: { "/cam": "/camB" } }),
    ]);

    const rows = await collectMulti(mr, { videoDecodable: true });
    expect(rows.length).toBe(12);
    const names = new Set(rows.map((r) => r.name));
    expect(names).toEqual(new Set(["/camA", "/camB"]));

    const out = await mr.sample(
      [
        { topic: "/camA", timestamps: [35n] },
        { topic: "/camB", timestamps: [35n] },
      ],
      { videoDecodable: true },
    );
    for (const row of out as SampleResult[][]) {
      expect(row[0]!.found).toBe(true);
      expect(row[0]!.isVideo).toBe(true);
      expect(row[0]!.frames.length).toBeGreaterThan(0);
    }
  });
});
