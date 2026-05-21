package turbodata

import (
	"math"
	"time"
)

// ReadStrategy controls how the cost-aware reader path plans and executes I/O.
// The default reader path (no WithReadStrategy passed) ignores this entirely.
//
// The three knobs together let any cost model collapse to operational settings:
//   - CoalesceGap answers "is it worth reading wasted bytes to save a request?"
//   - SplitThreshold answers "is it worth extra requests to parallelize a big read?"
//   - MaxConcurrency caps total in-flight reads.
//
// Users compute these from whatever cost model they care about (wall-clock,
// money, public-egress money, blended). The helper constructors below cover
// the common cases.
type ReadStrategy struct {
	// CoalesceGap is the byte gap below which two adjacent needed ranges are
	// merged into a single read. 0 disables coalescing.
	CoalesceGap int64

	// SplitThreshold is the size above which a merged read is sliced into
	// parallel sub-reads at internal range boundaries. A single range larger
	// than SplitThreshold stays as one oversized op — atomic units are never
	// split. 0 or math.MaxInt64 disables splitting.
	SplitThreshold int64

	// MaxConcurrency is the max number of in-flight ReadAt calls. 1 = serial.
	MaxConcurrency int
}

// StrategyForLatency returns a strategy that minimizes wall-clock by amortizing
// per-request RTT and splitting reads at the bandwidth-latency product so each
// parallel sub-read keeps a stream saturated for ~one RTT.
//
// rtt is the per-request fixed cost (network round-trip or syscall latency).
// perStreamBW is the steady-state bandwidth per concurrent stream, in bytes/sec.
// concurrency is the max in-flight reads.
func StrategyForLatency(rtt time.Duration, perStreamBW int64, concurrency int) ReadStrategy {
	bdp := int64(rtt.Seconds() * float64(perStreamBW))
	if bdp <= 0 {
		bdp = 1
	}
	return ReadStrategy{
		CoalesceGap:    bdp,
		SplitThreshold: bdp,
		MaxConcurrency: concurrency,
	}
}

// StrategyForMoney returns a strategy that minimizes total spend by aggressively
// coalescing (to save requests) and never splitting (extra splits = extra paid
// requests). reqPrice is the per-request cost; bytePrice is the per-byte cost
// in the same currency unit. Set bytePrice to 0 for free in-region egress;
// then coalescing happens regardless of gap.
func StrategyForMoney(reqPrice, bytePrice float64, concurrency int) ReadStrategy {
	gap := int64(math.MaxInt64)
	if bytePrice > 0 {
		gap = int64(reqPrice / bytePrice)
	}
	return ReadStrategy{
		CoalesceGap:    gap,
		SplitThreshold: math.MaxInt64,
		MaxConcurrency: concurrency,
	}
}

// StrategyForBlended returns a strategy that minimizes spend (via the same
// money-driven CoalesceGap as StrategyForMoney) but caps how long any single
// read may take by splitting reads larger than maxReadTime * perStreamBW into
// parallel sub-reads. Useful when egress isn't free but tail latency matters.
func StrategyForBlended(reqPrice, bytePrice float64, maxReadTime time.Duration, perStreamBW int64, concurrency int) ReadStrategy {
	gap := int64(math.MaxInt64)
	if bytePrice > 0 {
		gap = int64(reqPrice / bytePrice)
	}
	split := int64(maxReadTime.Seconds() * float64(perStreamBW))
	if split <= 0 {
		split = math.MaxInt64
	}
	return ReadStrategy{
		CoalesceGap:    gap,
		SplitThreshold: split,
		MaxConcurrency: concurrency,
	}
}
