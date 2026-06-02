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

// Video-decodable read/sample tests.
//
// Mirrors go/video_test.go and py/tests/test_video.py (the sample GOP-prefix
// semantics + readMessages key-frame snap-back). The TS SDK has no Writer, so
// these run against dedicated Go-produced fixtures whose frame layouts are
// fixed and documented in go/cmd/gen-fixtures/main.go. We hardcode the same
// layouts here as ground truth (the Go/Python tests hardcode frames too).

import { describe, expect, it } from "vitest";

import {
  Reader,
  type Frame,
  type ReadStrategy,
  type SampleResult,
} from "../src/index.js";

import { BytesReadSource, readFixture } from "./helpers.js";

const TXT = new TextDecoder();

interface FrameSpec {
  ts: bigint;
  label: string;
  key: boolean;
}

// Layouts mirror the gen-fixtures video builders exactly.
const SINGLE_GOP: FrameSpec[] = [
  { ts: 10n, label: "K10", key: true },
  { ts: 20n, label: "P20", key: false },
  { ts: 30n, label: "P30", key: false },
  { ts: 40n, label: "P40", key: false },
  { ts: 50n, label: "P50", key: false },
  { ts: 60n, label: "P60", key: false },
];

function newReader(fixture: string): Reader {
  return new Reader(new BytesReadSource(readFixture(fixture)));
}

function assertFrames(got: Frame[], want: FrameSpec[]): void {
  expect(got.map((f) => TXT.decode(f.data))).toEqual(want.map((w) => w.label));
  expect(got.length).toBe(want.length);
  for (let i = 0; i < want.length; i++) {
    expect(got[i]!.timestamp).toBe(want[i]!.ts);
    expect(got[i]!.isKeyFrame).toBe(want[i]!.key);
    expect(TXT.decode(got[i]!.data)).toBe(want[i]!.label);
  }
}

async function collectLabels(
  reader: Reader,
  opts: Parameters<Reader["readMessages"]>[0],
): Promise<string[]> {
  const out: string[] = [];
  for await (const m of reader.readMessages(opts)) {
    out.push(TXT.decode(m.data));
  }
  return out;
}

describe("Reader.sample video-decodable (single GOP fixture)", () => {
  const fixture = "video-single-gop.td";

  it("single query returns the GOP prefix with resetDecoder", async () => {
    const out = await newReader(fixture).sample(
      [{ topic: "/cam", timestamps: [30n] }],
      { videoDecodable: true },
    );
    const res = out[0]![0]!;
    expect(res.found).toBe(true);
    expect(res.isVideo).toBe(true);
    expect(res.resetDecoder).toBe(true);
    assertFrames(res.frames, [SINGLE_GOP[0]!, SINGLE_GOP[1]!, SINGLE_GOP[2]!]);
    expect(res.data.byteLength).toBe(0); // bytes live in frames for video
    expect(res.timestamp).toBe(30n);
    expect(TXT.decode(res.frames[res.frames.length - 1]!.data)).toBe("P30");
  });

  it("query exactly on the key frame returns a single key frame", async () => {
    const out = await newReader(fixture).sample(
      [{ topic: "/cam", timestamps: [10n] }],
      { videoDecodable: true },
    );
    const res = out[0]![0]!;
    expect(res.found && res.resetDecoder).toBe(true);
    expect(res.frames.length).toBe(1);
    expect(res.frames[0]!.isKeyFrame).toBe(true);
  });

  it("incremental queries in the same GOP feed only new frames", async () => {
    const out = await newReader(fixture).sample(
      [{ topic: "/cam", timestamps: [25n, 45n, 60n] }],
      { videoDecodable: true },
    );
    const row = out[0]!;
    expect(row.length).toBe(3);

    expect(row[0]!.resetDecoder).toBe(true);
    assertFrames(row[0]!.frames, [SINGLE_GOP[0]!, SINGLE_GOP[1]!]); // K10, P20

    expect(row[1]!.resetDecoder).toBe(false);
    assertFrames(row[1]!.frames, [SINGLE_GOP[2]!, SINGLE_GOP[3]!]); // P30, P40

    expect(row[2]!.resetDecoder).toBe(false);
    assertFrames(row[2]!.frames, [SINGLE_GOP[4]!, SINGLE_GOP[5]!]); // P50, P60

    // Dedup: total bytes across the row equal the full prefix to the last
    // target (no frame fed twice).
    const totalGot = row.reduce(
      (s, r) => s + r.frames.reduce((a, f) => a + f.data.byteLength, 0),
      0,
    );
    const totalWant = SINGLE_GOP.reduce((a, f) => a + f.label.length, 0);
    expect(totalGot).toBe(totalWant);
  });

  it("two queries resolving to the same target feed nothing the second time", async () => {
    const out = await newReader(fixture).sample(
      [{ topic: "/cam", timestamps: [21n, 22n] }],
      { videoDecodable: true },
    );
    const row = out[0]!;
    expect(row[0]!.resetDecoder).toBe(true);
    expect(row[0]!.frames.length).toBe(2); // K10, P20

    expect(row[1]!.resetDecoder).toBe(false);
    expect(row[1]!.isVideo).toBe(true);
    expect(row[1]!.frames.length).toBe(0); // same target, nothing new
    expect(row[1]!.data.byteLength).toBe(0);
    expect(row[1]!.timestamp).toBe(20n);
    expect(TXT.decode(row[0]!.frames[row[0]!.frames.length - 1]!.data)).toBe(
      "P20",
    );
  });

  it("timestamp before the first key frame is not found", async () => {
    const out = await newReader(fixture).sample(
      [{ topic: "/cam", timestamps: [5n] }],
      { videoDecodable: true },
    );
    expect(out[0]![0]!.found).toBe(false);
  });

  it("without videoDecodable, a video topic samples like a normal topic", async () => {
    const out = await newReader(fixture).sample([
      { topic: "/cam", timestamps: [10n, 35n] },
    ]);
    const row = out[0]!;
    const wantData = ["K10", "P30"]; // 35 floors to P30
    const wantTs = [10n, 30n];
    for (let i = 0; i < row.length; i++) {
      const res = row[i] as SampleResult;
      expect(res.found).toBe(true);
      expect(res.isVideo).toBe(false);
      expect(res.frames).toEqual([]);
      expect(res.resetDecoder).toBe(false);
      expect(TXT.decode(res.data)).toBe(wantData[i]);
      expect(res.timestamp).toBe(wantTs[i]);
    }
  });
});

describe("Reader.sample video-decodable (multi-GOP across chunks)", () => {
  const fixture = "video-multi-gop-chunks.td";

  it("guard: frames split into two index chunks", async () => {
    const summary = await newReader(fixture).summary();
    expect(summary.topicsInfos[0]!.indexChunkInfoList.length).toBe(2);
  });

  it("queries spanning two GOPs each reset with their own prefix", async () => {
    const out = await newReader(fixture).sample(
      [{ topic: "/cam", timestamps: [25n, 55n] }],
      { videoDecodable: true },
    );
    const row = out[0]!;
    expect(row[0]!.resetDecoder).toBe(true);
    assertFrames(row[0]!.frames, [
      { ts: 10n, label: "KA", key: true },
      { ts: 20n, label: "A1", key: false },
    ]);
    // Different GOP -> reset again, full prefix from GOP B's key frame.
    expect(row[1]!.resetDecoder).toBe(true);
    assertFrames(row[1]!.frames, [
      { ts: 40n, label: "KB", key: true },
      { ts: 50n, label: "B1", key: false },
    ]);
  });
});

const SNAP_STRATEGIES: { name: string; strategy?: ReadStrategy }[] = [
  { name: "default", strategy: undefined },
  {
    name: "cost-aware",
    strategy: { coalesceGap: 1n << 20n, splitThreshold: 4n << 20n, maxConcurrency: 4 },
  },
];

describe("Reader.readMessages video-decodable snap-back", () => {
  for (const { name, strategy } of SNAP_STRATEGIES) {
    it(`[${name}] single GOP: start mid-GOP snaps back to the key frame`, async () => {
      // Without snap-back, start=25 skips K10 and P20.
      const plain = await collectLabels(newReader("video-single-gop.td"), {
        startTimestamp: 25n,
        strategy,
      });
      expect(plain).toEqual(["P30", "P40", "P50", "P60"]);

      // With snap-back, start=25 snaps to the key frame at ts=10.
      const snapped = await collectLabels(newReader("video-single-gop.td"), {
        startTimestamp: 25n,
        videoDecodable: true,
        strategy,
      });
      expect(snapped).toEqual(["K10", "P20", "P30", "P40", "P50", "P60"]);
    });

    it(`[${name}] multi-GOP across chunks: snaps back within the right chunk`, async () => {
      // start=45 is in chunk 1 (GOP B); snap lands on KB@40, not KA@10.
      const snapped = await collectLabels(
        newReader("video-multi-gop-chunks.td"),
        { startTimestamp: 45n, videoDecodable: true, strategy },
      );
      expect(snapped).toEqual(["KB", "B1", "B2"]);
    });

    it(`[${name}] multi-GOP in one chunk: snaps to the GOP, not the chunk's first key frame`, async () => {
      // start=45 is mid-GOP B (KB@30); must not over-reach to KA@10.
      const snapped = await collectLabels(
        newReader("video-multi-gop-one-chunk.td"),
        { startTimestamp: 45n, videoDecodable: true, strategy },
      );
      expect(snapped).toEqual(["KB", "B1", "KC", "C1"]);
    });

    it(`[${name}] no snap-back without the option`, async () => {
      const plain = await collectLabels(
        newReader("video-multi-gop-one-chunk.td"),
        { startTimestamp: 45n, strategy },
      );
      expect(plain).toEqual(["KC", "C1"]); // ts>=45: KC@50, C1@60
    });
  }
});
