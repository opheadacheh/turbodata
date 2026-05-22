// Unit tests for the read planner.

import { describe, expect, it } from "vitest";

import { plan, type Range } from "../src/read_planner.js";
import type { ReadStrategy } from "../src/read_strategy.js";

const r = (offset: bigint, length: bigint): Range => ({ offset, length });

describe("plan", () => {
  it("empty input produces no ops", () => {
    const out = plan([], { coalesceGap: 0n, splitThreshold: 0n, maxConcurrency: 1 });
    expect(out.ops).toEqual([]);
    expect(out.locations).toEqual([]);
  });

  it("no coalescing: each range becomes its own op", () => {
    const ranges = [r(0n, 10n), r(20n, 5n), r(50n, 7n)];
    const s: ReadStrategy = { coalesceGap: 0n, splitThreshold: 0n, maxConcurrency: 1 };
    const out = plan(ranges, s);
    expect(out.ops.length).toBe(3);
    expect(out.ops[0]).toEqual({ offset: 0n, length: 10n });
    expect(out.ops[1]).toEqual({ offset: 20n, length: 5n });
    expect(out.ops[2]).toEqual({ offset: 50n, length: 7n });
    expect(out.locations[0]).toEqual({ opIndex: 0, inOpOff: 0, length: 10 });
    expect(out.locations[1]).toEqual({ opIndex: 1, inOpOff: 0, length: 5 });
    expect(out.locations[2]).toEqual({ opIndex: 2, inOpOff: 0, length: 7 });
  });

  it("coalesces adjacent ranges within gap", () => {
    // Gap from end of r0 (10) to start of r1 (15) is 5; gap from end of merged
    // [0..20) to start of r2 (50) is 30. Both are < 100, so all three merge.
    const ranges = [r(0n, 10n), r(15n, 5n), r(50n, 5n)];
    const s: ReadStrategy = { coalesceGap: 100n, splitThreshold: 0n, maxConcurrency: 1 };
    const out = plan(ranges, s);
    expect(out.ops.length).toBe(1);
    expect(out.ops[0]).toEqual({ offset: 0n, length: 55n });
    expect(out.locations[0]).toEqual({ opIndex: 0, inOpOff: 0, length: 10 });
    expect(out.locations[1]).toEqual({ opIndex: 0, inOpOff: 15, length: 5 });
    expect(out.locations[2]).toEqual({ opIndex: 0, inOpOff: 50, length: 5 });
  });

  it("does not coalesce across a too-large gap", () => {
    const ranges = [r(0n, 10n), r(15n, 5n), r(50n, 5n)];
    // Gap of 30 from [0..20) to r2 exceeds coalesceGap=10; r0+r1 still merge (gap=5).
    const s: ReadStrategy = { coalesceGap: 10n, splitThreshold: 0n, maxConcurrency: 1 };
    const out = plan(ranges, s);
    expect(out.ops.length).toBe(2);
    expect(out.ops[0]).toEqual({ offset: 0n, length: 20n });
    expect(out.ops[1]).toEqual({ offset: 50n, length: 5n });
  });

  it("does NOT coalesce when gap == coalesceGap (strict <)", () => {
    // Gap of exactly 5; coalesceGap = 5 means no merge.
    const ranges = [r(0n, 10n), r(15n, 5n)];
    const s: ReadStrategy = { coalesceGap: 5n, splitThreshold: 0n, maxConcurrency: 1 };
    const out = plan(ranges, s);
    expect(out.ops.length).toBe(2);
  });

  it("splits a coalesced group at range boundaries when over threshold", () => {
    // Coalesce into one big op (gap=0), then split at threshold.
    const ranges = [r(0n, 30n), r(30n, 30n), r(60n, 30n)];
    const s: ReadStrategy = { coalesceGap: 1000n, splitThreshold: 50n, maxConcurrency: 1 };
    const out = plan(ranges, s);
    // First op holds r0 (30 bytes). r0+r1 = 60 > 50, so r1 starts a new op.
    // r1+r2 = 60 > 50 again, so r2 starts a new op.
    expect(out.ops.length).toBe(3);
    expect(out.ops[0]).toEqual({ offset: 0n, length: 30n });
    expect(out.ops[1]).toEqual({ offset: 30n, length: 30n });
    expect(out.ops[2]).toEqual({ offset: 60n, length: 30n });
  });

  it("never splits a single oversize range", () => {
    const ranges = [r(0n, 1000n)];
    const s: ReadStrategy = { coalesceGap: 0n, splitThreshold: 50n, maxConcurrency: 1 };
    const out = plan(ranges, s);
    expect(out.ops.length).toBe(1);
    expect(out.ops[0]).toEqual({ offset: 0n, length: 1000n });
  });
});
