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

/**
 * ReadSource is the storage abstraction the Reader uses. It is intentionally
 * minimal: every Go ReadAt/Read+Seek call site can be expressed as `read(off,
 * len)`.
 *
 * Concurrency contract: implementations MUST be safe for concurrent `read`
 * calls. The cost-aware reader path issues many in-flight reads bounded only
 * by `maxConcurrency`.
 */
export interface ReadSource {
  /** Total length of the underlying file/blob/resource, in bytes. */
  size(): Promise<bigint>;

  /**
   * Read exactly `length` bytes starting at `offset`. Implementations should
   * throw if fewer bytes are available; the Reader assumes a full read.
   */
  read(offset: bigint, length: bigint): Promise<Uint8Array>;
}
