package benchmark

import (
	"io"
	"os"
	"testing"
)

// BenchmarkRead measures sequential full-scan read throughput of TD vs MCAP
// on equivalent fixtures (canonical 1MB-compressed). Per Obj 2, only the
// "read all messages" scenario is exercised here; chunk-size sweeps and
// strategy variations live in their own benchmarks.
//
// Sub-benchmarks: BenchmarkRead/all/{td,mcap}.
func BenchmarkRead(b *testing.B) {
	if mcapInfo == nil {
		b.Skip("no -mcap flag provided")
	}

	scenario := Scenario{Name: "all"}

	b.Run("all/td", func(b *testing.B) {
		runReadBench(b, tdCanonicalPath, func(rs *TrackingReadSeeker) error {
			_, err := runTd(rs, scenario)
			return err
		})
	})
	b.Run("all/mcap", func(b *testing.B) {
		runReadBench(b, mcapCanonicalPath, func(rs *TrackingReadSeeker) error {
			_, err := runMcap(rs, scenario)
			return err
		})
	})
}

// runReadBench opens path, wraps in TrackingReadSeeker, runs body for b.N
// iterations, and reports IO + (optionally) heap metrics. The file is
// rewound to offset 0 between iterations so each iteration is a fresh scan.
func runReadBench(b *testing.B, path string, body func(rs *TrackingReadSeeker) error) {
	b.Helper()
	f, err := os.Open(path)
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()
	tracker := NewTrackingReadSeeker(f)

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
		if err := body(tracker); err != nil {
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
	if hs != nil {
		hs.Stop()
		hs.Report(b)
	}
}
