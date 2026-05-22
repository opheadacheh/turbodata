package benchmark

import (
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"turbodata"

	"github.com/foxglove/mcap/go/mcap"
)

// BenchmarkStrategy compares TD read variants against MCAP under synthetic
// network latency. All TD variants read from the same .td fixture
// (uncompressed image chunks, compressed non-image chunks, 4MB chunks).
// The MCAP reference reads from the original .mcap file.
//
// Sub-benchmark labels: {scenario}/{variant}/{rtt}.
//   - scenario: all | img | range
//   - variant:  mcap | td_default | td_latency_serial | td_latency_parallel | td_money
//   - rtt:      0ms | 1ms | 10ms (latency injected on every Read/ReadAt)
//
// Latency is charged as `rtt + n/perStreamBW` per Read or ReadAt call; Seek
// is free. perStreamBW is fixed at 100 MB/s. The MCAP reference is wrapped in
// the same latency model for a like-for-like comparison.
func BenchmarkStrategy(b *testing.B) {
	if *mcapFlag == "" {
		b.Skip("no -mcap flag provided")
	}
	if strategyTdPath == "" {
		b.Skip("strategy fixture not built")
	}

	const perStreamBW int64 = 100 << 20 // 100 MB/s

	rtts := []struct {
		label string
		d     time.Duration
	}{
		{"0ms", 0},
		{"1ms", 1 * time.Millisecond},
		{"10ms", 10 * time.Millisecond},
	}

	scenarios := []string{"all", "img", "range"}

	for _, scenario := range scenarios {
		scenario := scenario
		for _, rtt := range rtts {
			rtt := rtt

			// MCAP reference (no strategy concept for MCAP).
			b.Run(fmt.Sprintf("%s/mcap/%s", scenario, rtt.label), func(b *testing.B) {
				runStrategyBench(b, *mcapFlag, rtt.d, perStreamBW, func(lat *LatencyReadSource) error {
					return runMcapScenario(scenario, lat)
				})
			})

			// TD default: no strategy.
			b.Run(fmt.Sprintf("%s/td_default/%s", scenario, rtt.label), func(b *testing.B) {
				runStrategyBench(b, strategyTdPath, rtt.d, perStreamBW, func(lat *LatencyReadSource) error {
					_, err := tdReadAll(lat, scenarioOpts(scenario, nil))
					return err
				})
			})

			// Strategy tuning RTT: at rtt=0 we still want the cost-aware path
			// active so its overhead is visible; use 1µs as a stand-in BDP
			// generator (CoalesceGap and SplitThreshold both = 1µs * BW ≈ 100B,
			// which produces minimal coalescing/splitting).
			tuneRTT := rtt.d
			if tuneRTT == 0 {
				tuneRTT = 1 * time.Microsecond
			}

			b.Run(fmt.Sprintf("%s/td_latency_serial/%s", scenario, rtt.label), func(b *testing.B) {
				strat := turbodata.StrategyForLatency(tuneRTT, perStreamBW, 1)
				runStrategyBench(b, strategyTdPath, rtt.d, perStreamBW, func(lat *LatencyReadSource) error {
					_, err := tdReadAll(lat, scenarioOpts(scenario, []turbodata.ReadOption{turbodata.WithReadStrategy(strat)}))
					return err
				})
			})

			b.Run(fmt.Sprintf("%s/td_latency_parallel/%s", scenario, rtt.label), func(b *testing.B) {
				strat := turbodata.StrategyForLatency(tuneRTT, perStreamBW, 8)
				runStrategyBench(b, strategyTdPath, rtt.d, perStreamBW, func(lat *LatencyReadSource) error {
					_, err := tdReadAll(lat, scenarioOpts(scenario, []turbodata.ReadOption{turbodata.WithReadStrategy(strat)}))
					return err
				})
			})

			// Money: free egress (bytePrice=0) → infinite coalesce gap, no
			// split, serial. One big merged read per chunk.
			b.Run(fmt.Sprintf("%s/td_money/%s", scenario, rtt.label), func(b *testing.B) {
				strat := turbodata.StrategyForMoney(0.0004, 0, 1)
				runStrategyBench(b, strategyTdPath, rtt.d, perStreamBW, func(lat *LatencyReadSource) error {
					_, err := tdReadAll(lat, scenarioOpts(scenario, []turbodata.ReadOption{turbodata.WithReadStrategy(strat)}))
					return err
				})
			})
		}
	}
}

// runStrategyBench opens path, wraps it with TrackingReadSeeker and
// LatencyReadSource, runs body for b.N iterations, and reports IO metrics.
func runStrategyBench(
	b *testing.B,
	path string,
	rtt time.Duration,
	perStreamBW int64,
	body func(lat *LatencyReadSource) error,
) {
	b.Helper()

	f, err := os.Open(path)
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()

	tracker := NewTrackingReadSeeker(f)
	lat := NewLatencyReadSource(tracker, rtt, perStreamBW)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			b.Fatal(err)
		}
		if err := body(lat); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	reportIOMetrics(b, tracker)
}

// scenarioOpts builds TD ReadOptions for a scenario, with optional extras.
func scenarioOpts(scenario string, extra []turbodata.ReadOption) []turbodata.ReadOption {
	switch scenario {
	case "all":
		return extra
	case "img":
		return append([]turbodata.ReadOption{turbodata.WithTopicNames([]string{imgTopicName})}, extra...)
	case "range":
		return append([]turbodata.ReadOption{
			turbodata.WithTopicNames(timeRangeTopics),
			turbodata.WithStartTimestamp(midStartTs),
			turbodata.WithEndTimestamp(midEndTs),
		}, extra...)
	default:
		return extra
	}
}

// tdReadAll opens a TD reader, applies opts, drains all messages.
func tdReadAll(rs turbodata.ReadSource, opts []turbodata.ReadOption) (int, error) {
	r, err := turbodata.NewReader(rs)
	if err != nil {
		return 0, err
	}
	it, err := r.ReadMessages(opts...)
	if err != nil {
		return 0, err
	}
	return drainTd(it)
}

// runMcapScenario runs the matching MCAP read path against rs (the
// latency-wrapped source).
func runMcapScenario(scenario string, rs *LatencyReadSource) error {
	r, err := mcap.NewReader(rs)
	if err != nil {
		return err
	}
	defer r.Close()

	var it mcap.MessageIterator
	switch scenario {
	case "all":
		it, err = r.Messages(mcap.InOrder(mcap.LogTimeOrder))
	case "img":
		it, err = r.Messages(mcap.InOrder(mcap.LogTimeOrder), mcap.WithTopics([]string{imgTopicName}))
	case "range":
		it, err = r.Messages(
			mcap.InOrder(mcap.LogTimeOrder),
			mcap.WithTopics(timeRangeTopics),
			mcap.AfterNanos(uint64(midStartTs)),
			mcap.BeforeNanos(uint64(midEndTs)),
		)
	default:
		return nil
	}
	if err != nil {
		return err
	}
	_, err = drainMcap(it)
	return err
}
