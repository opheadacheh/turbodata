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

// Read fetcher: executes ReadOps with bounded concurrency. Mirrors
// go/read_fetcher.go (promise-pool instead of goroutines + token channel).

import type { ReadOp } from "./read_planner.js";
import type { ReadSource } from "./read_source.js";

/**
 * Execute ops against rs with at most maxConcurrency in-flight reads. Returns
 * one Uint8Array per op, in input order. Throws on the first error
 * encountered; in-flight reads may still settle in the background.
 */
export async function fetchAll(
  rs: ReadSource,
  ops: ReadOp[],
  maxConcurrency: number,
): Promise<Uint8Array[]> {
  if (ops.length === 0) {
    return [];
  }
  const workers = maxConcurrency > 0 ? maxConcurrency : 1;
  const out = new Array<Uint8Array>(ops.length);

  let nextIdx = 0;
  let firstErr: unknown = undefined;

  const runOne = async (): Promise<void> => {
    while (firstErr === undefined) {
      const i = nextIdx++;
      if (i >= ops.length) {
        return;
      }
      const op = ops[i]!;
      try {
        const buf = await rs.read(op.offset, op.length);
        if (BigInt(buf.byteLength) !== op.length) {
          throw new Error(
            `fetcher: short read for op ${i}: got ${buf.byteLength} want ${op.length}`,
          );
        }
        out[i] = buf;
      } catch (err) {
        if (firstErr === undefined) {
          firstErr = err;
        }
        return;
      }
    }
  };

  const pool: Promise<void>[] = new Array(Math.min(workers, ops.length));
  for (let w = 0; w < pool.length; w++) {
    pool[w] = runOne();
  }
  await Promise.all(pool);

  if (firstErr !== undefined) {
    throw firstErr;
  }
  return out;
}
