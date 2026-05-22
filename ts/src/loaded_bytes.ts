// LoadedBytes: bridge between the planner+fetcher (which think in coalesced /
// split byte ranges) and the consumers (which think in their original
// per-range terms). Mirrors go/loaded_bytes.go.

import type { Range, RangeLocation } from "./read_planner.js";

interface BytesLocation {
  bufferIdx: number;
  inBufOff: number;
  length: number;
}

export class LoadedBytes {
  private readonly buffers: Uint8Array[];
  private readonly index: Map<bigint, BytesLocation>;

  constructor(
    ranges: Range[],
    locations: RangeLocation[],
    buffers: Uint8Array[],
  ) {
    this.buffers = buffers;
    this.index = new Map();
    for (let i = 0; i < ranges.length; i++) {
      const r = ranges[i]!;
      const loc = locations[i]!;
      this.index.set(r.offset, {
        bufferIdx: loc.opIndex,
        inBufOff: loc.inOpOff,
        length: loc.length,
      });
    }
  }

  /**
   * Return the slice for the range registered at offset. The returned
   * Uint8Array aliases the underlying buffer; callers must not mutate it.
   */
  get(offset: bigint): Uint8Array | undefined {
    const loc = this.index.get(offset);
    if (loc === undefined) {
      return undefined;
    }
    const buf = this.buffers[loc.bufferIdx]!;
    return buf.subarray(loc.inBufOff, loc.inBufOff + loc.length);
  }
}
