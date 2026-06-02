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

// Read planner: groups Ranges into ReadOps according to a ReadStrategy.
// Mirrors go/read_planner.go.

import type { ReadStrategy } from "./read_strategy.js";

export interface Range {
  /** int64. Absolute file offset. */
  offset: bigint;
  /** int64. Length in bytes. */
  length: bigint;
}

export interface ReadOp {
  offset: bigint;
  length: bigint;
}

export interface RangeLocation {
  /** Index into the produced ReadOp[]. */
  opIndex: number;
  /** Byte offset within that op's buffer (must fit in number; ops are bounded). */
  inOpOff: number;
  /** Length of this original range, in bytes. */
  length: number;
}

interface CoalesceGroup {
  offset: bigint;
  end: bigint; // exclusive
  members: number[]; // indices into input ranges, in offset order
}

/**
 * Plan groups Ranges into ReadOps.
 *
 * Precondition: ranges must be in non-decreasing offset order. All call sites
 * in this package satisfy this by construction (writer lays out groups and
 * chunks at monotonically increasing file offsets).
 *
 * Algorithm:
 *  1. Coalesce: merge adjacent ranges whose gap is < coalesceGap.
 *  2. Split: a coalesced op whose length exceeds splitThreshold is sliced at
 *     internal range boundaries via greedy packing. A single range larger
 *     than splitThreshold stays as one oversize op (atomic units never break).
 */
export function plan(
  ranges: Range[],
  s: ReadStrategy,
): { ops: ReadOp[]; locations: RangeLocation[] } {
  const locations: RangeLocation[] = new Array(ranges.length);
  if (ranges.length === 0) {
    return { ops: [], locations };
  }

  const groups: CoalesceGroup[] = [];
  for (let idx = 0; idx < ranges.length; idx++) {
    const r = ranges[idx]!;
    if (groups.length === 0) {
      groups.push({
        offset: r.offset,
        end: r.offset + r.length,
        members: [idx],
      });
      continue;
    }
    const cur = groups[groups.length - 1]!;
    const gap = r.offset - cur.end;
    if (gap >= 0n && gap < s.coalesceGap) {
      const newEnd = r.offset + r.length;
      if (newEnd > cur.end) {
        cur.end = newEnd;
      }
      cur.members.push(idx);
      continue;
    }
    groups.push({
      offset: r.offset,
      end: r.offset + r.length,
      members: [idx],
    });
  }

  const ops: ReadOp[] = [];
  const threshold = s.splitThreshold;
  const splitDisabled = threshold <= 0n;

  for (const g of groups) {
    if (splitDisabled || g.end - g.offset <= threshold) {
      const opIndex = ops.length;
      ops.push({ offset: g.offset, length: g.end - g.offset });
      for (const mi of g.members) {
        const r = ranges[mi]!;
        locations[mi] = {
          opIndex,
          inOpOff: Number(r.offset - g.offset),
          length: Number(r.length),
        };
      }
      continue;
    }

    // Greedy split at range boundaries.
    let opStartIdx = 0;
    let opStartOffset = ranges[g.members[0]!]!.offset;
    let opEndOffset =
      ranges[g.members[0]!]!.offset + ranges[g.members[0]!]!.length;

    const flushOp = (lastMember: number): void => {
      const opIndex = ops.length;
      ops.push({ offset: opStartOffset, length: opEndOffset - opStartOffset });
      for (let k = opStartIdx; k <= lastMember; k++) {
        const mi = g.members[k]!;
        const r = ranges[mi]!;
        locations[mi] = {
          opIndex,
          inOpOff: Number(r.offset - opStartOffset),
          length: Number(r.length),
        };
      }
    };

    for (let k = 1; k < g.members.length; k++) {
      const mi = g.members[k]!;
      const r = ranges[mi]!;
      const nextEnd = r.offset + r.length;
      if (nextEnd - opStartOffset > threshold) {
        flushOp(k - 1);
        opStartIdx = k;
        opStartOffset = r.offset;
        opEndOffset = nextEnd;
        continue;
      }
      opEndOffset = nextEnd;
    }
    flushOp(g.members.length - 1);
  }

  return { ops, locations };
}
