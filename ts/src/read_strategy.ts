// ReadStrategy: knobs for the cost-aware reader path. Mirrors
// go/read_strategy.go.

export interface ReadStrategy {
  /**
   * int64. Byte gap below which two adjacent needed ranges are merged into
   * a single read. 0n disables coalescing.
   */
  coalesceGap: bigint;

  /**
   * int64. Size above which a merged read is sliced into parallel sub-reads
   * at internal range boundaries. A single range larger than splitThreshold
   * stays as one oversized op (atomic units are never split). 0n or
   * int64-max disables splitting.
   */
  splitThreshold: bigint;

  /** Max number of in-flight reads. <= 0 is clamped to 1 (serial). */
  maxConcurrency: number;
}

const MAX_INT64 = 0x7fffffffffffffffn;

/**
 * Strategy that minimizes wall-clock by amortizing per-request RTT and
 * splitting reads at the bandwidth-latency product so each parallel sub-read
 * keeps a stream saturated for ~one RTT.
 */
export function StrategyForLatency(opts: {
  rttSeconds: number;
  perStreamBytesPerSec: number;
  concurrency: number;
}): ReadStrategy {
  let bdp = BigInt(Math.trunc(opts.rttSeconds * opts.perStreamBytesPerSec));
  if (bdp <= 0n) {
    bdp = 1n;
  }
  return {
    coalesceGap: bdp,
    splitThreshold: bdp,
    maxConcurrency: opts.concurrency,
  };
}

/**
 * Strategy that minimizes total spend by aggressively coalescing (to save
 * requests) and never splitting (extra splits = extra paid requests).
 *
 * Set bytePrice to 0 for free in-region egress; then coalescing happens
 * regardless of gap.
 */
export function StrategyForMoney(opts: {
  reqPrice: number;
  bytePrice: number;
  concurrency: number;
}): ReadStrategy {
  let gap = MAX_INT64;
  if (opts.bytePrice > 0) {
    gap = BigInt(Math.trunc(opts.reqPrice / opts.bytePrice));
  }
  return {
    coalesceGap: gap,
    splitThreshold: MAX_INT64,
    maxConcurrency: opts.concurrency,
  };
}

/**
 * Strategy that minimizes spend (via the same money-driven coalesceGap as
 * StrategyForMoney) but caps how long any single read may take by splitting
 * reads larger than maxReadTimeSeconds * perStreamBytesPerSec into parallel
 * sub-reads.
 */
export function StrategyForBlended(opts: {
  reqPrice: number;
  bytePrice: number;
  maxReadTimeSeconds: number;
  perStreamBytesPerSec: number;
  concurrency: number;
}): ReadStrategy {
  let gap = MAX_INT64;
  if (opts.bytePrice > 0) {
    gap = BigInt(Math.trunc(opts.reqPrice / opts.bytePrice));
  }
  let split = BigInt(
    Math.trunc(opts.maxReadTimeSeconds * opts.perStreamBytesPerSec),
  );
  if (split <= 0n) {
    split = MAX_INT64;
  }
  return {
    coalesceGap: gap,
    splitThreshold: split,
    maxConcurrency: opts.concurrency,
  };
}
