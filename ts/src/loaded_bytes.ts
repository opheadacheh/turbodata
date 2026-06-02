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
