package benchmark

import (
	"io"
	"os"
	"testing"
	"time"

	"turbodata"
)

// BenchmarkUseCase (Obj 4) compares the default reader against
// WithReadStrategy across realistic workflows. It uses the improved .td
// fixture (per-image-topic groups uncompressed, non-image topics co-located
// with 64 KiB chunks) — the configuration we proposed in Obj 3.
//
// Storage behavior is modeled by LatencyReadSource, which sleeps
// rtt + len(p)/perStreamBW per Read/ReadAt. ns/op under any non-trivial
// profile is therefore *modeled* wall-clock, not measured CPU. The numbers
// that always tell the truth are bytes/op, reads/op, seeks/op, and uUSD/op.
//
// Sub-benchmarks: BenchmarkUseCase/{usecase}/{scenario}/{profile}/{variant}.
//   - usecase:  labeling | training | viewing
//   - scenario: depends on usecase (see below)
//   - profile:  local_nvme | local_hdd | cloud_obj_internal | cloud_obj_public
//   - variant:  td_default | td_strategy
//
// Each (usecase, profile) pair has *one* matching strategy variant chosen
// to optimize what that profile cares about (latency, throughput, money).
// We do not benchmark mismatched strategies — picking a wrong strategy
// would just demonstrate that yes, picking wrong is wrong.
func BenchmarkUseCase(b *testing.B) {
	if mcapInfo == nil {
		b.Skip("no -mcap flag provided")
	}
	if pickedImageTopic == "" || pickedNonImageTopic == "" {
		b.Skip("could not derive scenario picks from MCAP")
	}
	if tdImprovedPath == "" {
		b.Skip("improved .td fixture not built")
	}

	b.Run("labeling", func(b *testing.B) {
		runLabeling(b)
	})
	b.Run("training", func(b *testing.B) {
		runTraining(b)
	})
	b.Run("viewing", func(b *testing.B) {
		runViewing(b)
	})
}

// runLabeling: data labeling reads one image topic (or one image + the
// small co-located topics) over the entire timeline, served from a public
// S3-class object store where bytes are expensive. The strategy that fits
// is StrategyForMoney with the profile's prices.
func runLabeling(b *testing.B) {
	scenarios := []struct {
		name string
		sc   Scenario
	}{
		{"img_only", Scenario{
			Name:   "img_only",
			Topics: []string{pickedImageTopic},
		}},
		{"img_plus_small", Scenario{
			Name:   "img_plus_small",
			Topics: pickedRangeTopics, // 1 image + busy non-image + 1 more
		}},
	}
	prof := ProfileCloudObjPublic
	strat := strategyForMoneyProfile(prof, 4)

	for _, sc := range scenarios {
		sc := sc
		runUseCaseRow(b, sc.name, prof, "td_default", sc.sc, nil)
		runUseCaseRow(b, sc.name, prof, "td_strategy", sc.sc, []turbodata.ReadOption{turbodata.WithReadStrategy(strat)})
	}
}

// runTraining: model training reads selected topics in a 1 s window. We
// run across all four profiles to show how the right strategy depends on
// storage class.
func runTraining(b *testing.B) {
	sc := Scenario{
		Name:           "range",
		Topics:         pickedRangeTopics,
		StartTimestamp: rangeStartTs,
		EndTimestamp:   rangeEndTs,
	}

	profiles := []struct {
		prof  StorageProfile
		strat turbodata.ReadStrategy
	}{
		// nvme: latency tiny, bandwidth huge → split reads at the BDP and
		// fan out to 8 in-flight ReadAts.
		{ProfileLocalNVMe, turbodata.StrategyForLatency(ProfileLocalNVMe.RTT, ProfileLocalNVMe.PerStreamBW, 8)},

		// hdd: seeks are expensive, parallel I/O thrashes the head → keep
		// concurrency=1 and let the BDP-driven coalescing collapse small
		// adjacent reads.
		{ProfileLocalHDD, turbodata.StrategyForLatency(ProfileLocalHDD.RTT, ProfileLocalHDD.PerStreamBW, 1)},

		// cloud_obj_internal: bytes are free in-region, but in a *selective*
		// read (training reads ~few topics in a small window) the
		// "coalesce everything" policy of StrategyForMoney(byte=0) merges
		// disjoint small ranges into one giant request — over-fetching the
		// whole file. Latency-driven coalescing is the right tradeoff for
		// selective reads on RPS-bound storage; we keep concurrency high
		// since per-request cost is the only real budget.
		{ProfileCloudObjInternal, turbodata.StrategyForLatency(ProfileCloudObjInternal.RTT, ProfileCloudObjInternal.PerStreamBW, 16)},

		// cloud_obj_public: bytes are expensive enough that
		// StrategyForMoney's CoalesceGap = reqPrice/bytePrice (~12 KiB
		// here) tightly bounds wasted reads while still amortizing
		// per-request cost.
		{ProfileCloudObjPublic, strategyForMoneyProfile(ProfileCloudObjPublic, 4)},
	}
	for _, p := range profiles {
		p := p
		b.Run("range/"+p.prof.Name, func(b *testing.B) {
			runUseCaseRow(b, "", p.prof, "td_default", sc, nil)
			runUseCaseRow(b, "", p.prof, "td_strategy", sc, []turbodata.ReadOption{turbodata.WithReadStrategy(p.strat)})
		})
	}
}

// runViewing: interactive sample preview over a 100 ms window. Latency
// dominates, so StrategyForLatency with high concurrency is the fit on
// every profile we run here.
func runViewing(b *testing.B) {
	mid := mcapInfo.StartTimestamp + (mcapInfo.EndTimestamp-mcapInfo.StartTimestamp)/2
	end := mid + 100*int64(time.Millisecond/time.Nanosecond)
	if end > mcapInfo.EndTimestamp {
		end = mcapInfo.EndTimestamp
	}
	sc := Scenario{
		Name:           "range_short",
		Topics:         pickedRangeTopics,
		StartTimestamp: mid,
		EndTimestamp:   end,
	}

	profiles := []struct {
		prof  StorageProfile
		strat turbodata.ReadStrategy
	}{
		{ProfileLocalNVMe, turbodata.StrategyForLatency(ProfileLocalNVMe.RTT, ProfileLocalNVMe.PerStreamBW, 8)},
		{ProfileCloudObjInternal, turbodata.StrategyForLatency(ProfileCloudObjInternal.RTT, ProfileCloudObjInternal.PerStreamBW, 8)},
	}
	for _, p := range profiles {
		p := p
		b.Run("range_short/"+p.prof.Name, func(b *testing.B) {
			runUseCaseRow(b, "", p.prof, "td_default", sc, nil)
			runUseCaseRow(b, "", p.prof, "td_strategy", sc, []turbodata.ReadOption{turbodata.WithReadStrategy(p.strat)})
		})
	}
}

// strategyForMoneyProfile maps a storage profile's prices to a
// money-minimizing strategy. We convert BytePriceUSDPerGB to per-byte and
// feed it to StrategyForMoney; if BytePrice is 0 (free in-region egress),
// StrategyForMoney's CoalesceGap goes to MaxInt64 — coalesce everything.
func strategyForMoneyProfile(p StorageProfile, concurrency int) turbodata.ReadStrategy {
	const bytesPerGiB = 1 << 30
	bytePrice := 0.0
	if p.BytePriceUSDPerGB > 0 {
		bytePrice = p.BytePriceUSDPerGB / float64(bytesPerGiB)
	}
	return turbodata.StrategyForMoney(p.ReqPriceUSD, bytePrice, concurrency)
}

// runUseCaseRow executes one bench row. If parentScenario is non-empty
// the row is run as a nested b.Run with name "<parent>/<profile.Name>/<variant>";
// otherwise we expect the caller is already inside an outer b.Run and we
// add only "/td_default" or "/td_strategy".
func runUseCaseRow(
	b *testing.B,
	parentScenario string,
	prof StorageProfile,
	variant string,
	sc Scenario,
	extra []turbodata.ReadOption,
) {
	subName := variant
	if parentScenario != "" {
		subName = parentScenario + "/" + prof.Name + "/" + variant
	}
	b.Run(subName, func(b *testing.B) {
		runUseCaseLoop(b, prof, sc, extra)
	})
}

// runUseCaseLoop opens the improved .td fixture, wraps it in a tracker +
// LatencyReadSource, runs the scenario b.N times, and reports IO + money
// + heap metrics. Math.MaxInt64 splits and the parallel ReadAt path mean
// we must not Seek between iterations the way runReadBench does — the
// strategy reader uses ReadAt, which doesn't depend on the underlying
// Seek position. We still rewind the file as a safety net for the
// default reader, which does call Seek.
func runUseCaseLoop(b *testing.B, prof StorageProfile, sc Scenario, extra []turbodata.ReadOption) {
	f, err := os.Open(tdImprovedPath)
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()
	tracker := NewTrackingReadSeeker(f)
	lat := NewLatencyReadSource(tracker, prof)

	var hs *HeapSampler
	if HeapEnabled() {
		hs = StartHeapSampler()
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			b.Fatal(err)
		}
		if _, err := runTd(lat, sc, extra...); err != nil {
			b.Fatal(err)
		}
		if hs != nil {
			b.StopTimer()
			hs.Sample()
			b.StartTimer()
		}
	}
	b.StopTimer()
	reportReadIO(b, tracker)
	reportMoney(b, tracker, prof)
	if hs != nil {
		hs.Stop()
		hs.Report(b)
	}
}
