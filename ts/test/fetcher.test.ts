// Unit tests for the concurrency-bounded fetcher.

import { describe, expect, it } from "vitest";

import { fetchAll } from "../src/read_fetcher.js";
import type { ReadOp } from "../src/read_planner.js";
import type { ReadSource } from "../src/read_source.js";

class InstrumentedSource implements ReadSource {
  inFlight = 0;
  maxInFlight = 0;
  private readonly bytes: Uint8Array;
  private readonly delayMs: number;

  constructor(bytes: Uint8Array, delayMs: number) {
    this.bytes = bytes;
    this.delayMs = delayMs;
  }

  async size(): Promise<bigint> {
    return BigInt(this.bytes.byteLength);
  }

  async read(offset: bigint, length: bigint): Promise<Uint8Array> {
    this.inFlight++;
    if (this.inFlight > this.maxInFlight) {
      this.maxInFlight = this.inFlight;
    }
    try {
      await new Promise((res) => setTimeout(res, this.delayMs));
      return new Uint8Array(
        this.bytes.subarray(Number(offset), Number(offset + length)),
      );
    } finally {
      this.inFlight--;
    }
  }
}

describe("fetchAll", () => {
  it("returns one buffer per op, in input order", async () => {
    const bytes = new Uint8Array(100);
    for (let i = 0; i < bytes.length; i++) {
      bytes[i] = i;
    }
    const src = new InstrumentedSource(bytes, 0);
    const ops: ReadOp[] = [
      { offset: 0n, length: 5n },
      { offset: 50n, length: 10n },
      { offset: 90n, length: 4n },
    ];
    const out = await fetchAll(src, ops, 4);
    expect(out.length).toBe(3);
    expect(Array.from(out[0]!)).toEqual([0, 1, 2, 3, 4]);
    expect(Array.from(out[1]!)).toEqual([50, 51, 52, 53, 54, 55, 56, 57, 58, 59]);
    expect(Array.from(out[2]!)).toEqual([90, 91, 92, 93]);
  });

  it("never exceeds maxConcurrency in-flight", async () => {
    const bytes = new Uint8Array(1000);
    const src = new InstrumentedSource(bytes, 5);
    const ops: ReadOp[] = [];
    for (let i = 0; i < 20; i++) {
      ops.push({ offset: BigInt(i * 10), length: 10n });
    }
    await fetchAll(src, ops, 3);
    expect(src.maxInFlight).toBeLessThanOrEqual(3);
    expect(src.maxInFlight).toBeGreaterThan(0);
  });

  it("propagates the first error", async () => {
    const failing: ReadSource = {
      async size() {
        return 0n;
      },
      async read() {
        throw new Error("boom");
      },
    };
    await expect(
      fetchAll(failing, [{ offset: 0n, length: 1n }], 1),
    ).rejects.toThrow(/boom/);
  });

  it("zero ops returns empty", async () => {
    const bytes = new Uint8Array(0);
    const src = new InstrumentedSource(bytes, 0);
    const out = await fetchAll(src, [], 4);
    expect(out).toEqual([]);
  });
});
