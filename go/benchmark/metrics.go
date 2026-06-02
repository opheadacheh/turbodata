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

package benchmark

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// reportReadIO divides accumulated tracker stats by b.N and reports them as
// custom metrics on b. ReadAt counters are folded into reads/op so callers
// don't need to know which path the reader used.
func reportReadIO(b *testing.B, tracker *TrackingReadSeeker) {
	b.Helper()
	n := float64(b.N)
	totalBytes := tracker.ReadBytes + tracker.ReadAtBytes
	totalCalls := tracker.ReadCalls + tracker.ReadAtCalls
	b.ReportMetric(float64(totalBytes)/n, "bytes/op")
	b.ReportMetric(float64(totalCalls)/n, "reads/op")
	b.ReportMetric(float64(tracker.SeekCalls)/n, "seeks/op")
}

// reportWriteIO reports per-op write IO metrics from a TrackingWriter.
func reportWriteIO(b *testing.B, tw *TrackingWriter) {
	b.Helper()
	n := float64(b.N)
	b.ReportMetric(float64(tw.WriteBytes)/n, "write_bytes/op")
	b.ReportMetric(float64(tw.WriteCalls)/n, "write_calls/op")
}

// reportMoney reports a per-op cost in USD using the storage profile's per-
// request and per-byte prices and the IO counters accumulated in tracker. It
// is informational only — when prices are zero (local profiles) the metric is
// also zero. We report micro-USD (µUSD) per op because real per-op spend is
// fractions of a cent and floats lose precision at fractional cents.
func reportMoney(b *testing.B, tracker *TrackingReadSeeker, p StorageProfile) {
	b.Helper()
	n := float64(b.N)
	totalBytes := float64(tracker.ReadBytes + tracker.ReadAtBytes)
	totalCalls := float64(tracker.ReadCalls + tracker.ReadAtCalls)
	const bytesPerGiB = 1 << 30
	usdPerOp := (totalCalls*p.ReqPriceUSD + totalBytes*p.BytePriceUSDPerGB/float64(bytesPerGiB)) / n
	b.ReportMetric(usdPerOp*1e6, "uUSD/op")
}

// HeapSampler measures heap usage of a benchmark in two complementary ways:
//
//  1. A background goroutine polls runtime.MemStats.HeapInuse every
//     samplePeriod and tracks the max. This captures the *peak* live heap
//     during the bench, including transient allocations that the GC frees
//     before the next iteration boundary. The polling has no synchronization
//     with the bench loop, so it perturbs nothing.
//
//  2. At iteration boundaries (Sample), it forces a GC and reads HeapInuse.
//     This captures *retained* heap — memory still alive after a full GC.
//     Useful for finding leaks across iterations.
//
// Reported metrics:
//   - peak_heap_B         absolute peak HeapInuse (from background poll)
//   - peak_heap_delta_B   peak minus the baseline HeapInuse at start
//   - retained_heap_B     post-GC HeapInuse at the last iteration boundary
//   - total_alloc_B/op    cumulative allocation bytes / b.N
//
// `delta` and `retained` together tell the story: large `delta` with small
// `retained` = transient working set that GCs cleanly; both large = leak or
// data held until Close.
type HeapSampler struct {
	baselineHeapInuse uint64
	startTotalAlloc   uint64

	// Updated by the background goroutine, read at Report time.
	peakHeapInuse atomic.Uint64

	// Updated only at Sample().
	retainedHeapInuse uint64
	endTotalAlloc     uint64

	stop chan struct{}
	done chan struct{}
}

const heapSamplePeriod = 1 * time.Millisecond

// StartHeapSampler initializes a sampler and launches the background poller.
// Call Sample() at the end of every iteration to refresh the retained-heap
// snapshot, and Stop() before Report() to terminate the poller.
func StartHeapSampler() *HeapSampler {
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	hs := &HeapSampler{
		baselineHeapInuse: ms.HeapInuse,
		startTotalAlloc:   ms.TotalAlloc,
		retainedHeapInuse: ms.HeapInuse,
		stop:              make(chan struct{}),
		done:              make(chan struct{}),
	}
	hs.peakHeapInuse.Store(ms.HeapInuse)

	go hs.poll()
	return hs
}

func (h *HeapSampler) poll() {
	defer close(h.done)
	t := time.NewTicker(heapSamplePeriod)
	defer t.Stop()
	for {
		select {
		case <-h.stop:
			return
		case <-t.C:
			var ms runtime.MemStats
			runtime.ReadMemStats(&ms)
			for {
				cur := h.peakHeapInuse.Load()
				if ms.HeapInuse <= cur {
					break
				}
				if h.peakHeapInuse.CompareAndSwap(cur, ms.HeapInuse) {
					break
				}
			}
		}
	}
}

// Sample records retained-heap state at an iteration boundary. Forces a GC
// so the resulting HeapInuse reflects only objects still alive across
// iterations.
func (h *HeapSampler) Sample() {
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	h.retainedHeapInuse = ms.HeapInuse
	h.endTotalAlloc = ms.TotalAlloc
}

// Stop terminates the background poller. Must be called before Report.
func (h *HeapSampler) Stop() {
	close(h.stop)
	<-h.done
}

// Report writes heap metrics to b. Stop must have been called first.
func (h *HeapSampler) Report(b *testing.B) {
	b.Helper()
	n := float64(b.N)
	if n <= 0 {
		n = 1
	}
	peak := h.peakHeapInuse.Load()
	b.ReportMetric(float64(peak), "peak_heap_B")
	delta := int64(peak) - int64(h.baselineHeapInuse)
	if delta < 0 {
		delta = 0
	}
	b.ReportMetric(float64(delta), "peak_heap_delta_B")
	b.ReportMetric(float64(h.retainedHeapInuse), "retained_heap_B")
	b.ReportMetric(float64(h.endTotalAlloc-h.startTotalAlloc)/n, "total_alloc_B/op")
}

// heapEnabled is set once by TestMain when -heap is passed. Atomic isn't
// needed: TestMain runs to completion before any Benchmark.
var (
	heapEnabledMu sync.RWMutex
	heapEnabled   bool
)

func setHeapEnabled(v bool) {
	heapEnabledMu.Lock()
	heapEnabled = v
	heapEnabledMu.Unlock()
}

// HeapEnabled reports whether heap sampling is on. Benchmarks call this to
// decide whether to wrap iterations in a HeapSampler.
func HeapEnabled() bool {
	heapEnabledMu.RLock()
	defer heapEnabledMu.RUnlock()
	return heapEnabled
}
