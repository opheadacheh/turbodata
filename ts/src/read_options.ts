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

// Read options for Reader.readMessages(). Mirrors go/read_option.go.

import type { ReadStrategy } from "./read_strategy.js";

export type Order = "time" | "reverse-time";

export interface ReadOptions {
  /** When set, only messages from these topic names are emitted. */
  topicNames?: string[];
  /** int64. Inclusive lower bound on message timestamp. Default: 0n. */
  startTimestamp?: bigint;
  /** int64. Inclusive upper bound on message timestamp. Default: int64-max. */
  endTimestamp?: bigint;
  /** Iteration order. Default: "time". */
  order?: Order;
  /**
   * When set, switches Reader.readMessages onto the cost-aware path. Without
   * a strategy, the reader uses the default lazy path (one chunk at a time).
   */
  strategy?: ReadStrategy;
  /**
   * int64. Bytes to read speculatively from the file tail when loading the
   * summary. When the (footer + compressed summary) fits within the prefetch
   * window, summary loading costs one read instead of two. Default: 0n
   * (footer-only read).
   */
  tailPrefetch?: bigint;
  /**
   * If true, the iterator hands out a freshly-allocated Uint8Array per
   * message (safe to retain). If false (default), `data` aliases an internal
   * reusable buffer that becomes invalid on the next iteration step.
   */
  copy?: boolean;
  /**
   * If true, for any video topic in scope the effective per-group
   * startTimestamp is snapped back to the latest key frame whose timestamp is
   * <= startTimestamp, so the emitted sequence can be fed to a decoder cold.
   * Non-video topics are unaffected. Default: false.
   */
  videoDecodable?: boolean;
}

/** Default value for endTimestamp: matches Go's math.MaxInt64. */
export const MAX_INT64 = 0x7fffffffffffffffn;
