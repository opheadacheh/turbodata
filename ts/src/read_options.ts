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
}

/** Default value for endTimestamp: matches Go's math.MaxInt64. */
export const MAX_INT64 = 0x7fffffffffffffffn;
