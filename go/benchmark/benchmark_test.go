package benchmark

import (
	"io"
	"os"
	"testing"
)

// reportIOMetrics divides accumulated tracker stats by b.N and reports them as custom metrics.
func reportIOMetrics(b *testing.B, tracker *TrackingReadSeeker) {
	b.Helper()
	n := float64(b.N)
	b.ReportMetric(float64(tracker.ReadBytes)/n, "bytes/op")
	b.ReportMetric(float64(tracker.ReadCalls)/n, "reads/op")
	b.ReportMetric(float64(tracker.SeekCalls)/n, "seeks/op")
}

// BenchmarkReadAllMessages benchmarks a full sequential scan of all messages.
// Sub-benchmarks: mcap, td/<config>.
func BenchmarkReadAllMessages(b *testing.B) {
	if *mcapFlag == "" {
		b.Skip("no -mcap flag provided")
	}

	b.Run("mcap", func(b *testing.B) {
		f, err := os.Open(*mcapFlag)
		if err != nil {
			b.Fatal(err)
		}
		defer f.Close()
		tracker := NewTrackingReadSeeker(f)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			f.Seek(0, io.SeekStart) //nolint:errcheck
			if _, err := mcapReadAllMessages(tracker); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		reportIOMetrics(b, tracker)
	})

	for _, pair := range configPairs {
		b.Run("td/"+pair.Label, func(b *testing.B) {
			f, err := os.Open(tdFilePaths[pair.Label])
			if err != nil {
				b.Fatal(err)
			}
			defer f.Close()
			tracker := NewTrackingReadSeeker(f)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				f.Seek(0, io.SeekStart) //nolint:errcheck
				if _, err := tdReadAllMessages(tracker); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			reportIOMetrics(b, tracker)
		})
	}
}

// BenchmarkReadSingleTopic benchmarks reading all messages from one topic.
// Sub-benchmarks: img/mcap, img/td/<config>, nonimg/mcap, nonimg/td/<config>.
func BenchmarkReadSingleTopic(b *testing.B) {
	if *mcapFlag == "" {
		b.Skip("no -mcap flag provided")
	}

	for _, tc := range []struct {
		name  string
		topic string
	}{
		{"img", imgTopicName},
		{"nonimg", nonImgTopicName},
	} {
		tc := tc
		if tc.topic == "" {
			continue
		}

		b.Run(tc.name+"/mcap", func(b *testing.B) {
			f, err := os.Open(*mcapFlag)
			if err != nil {
				b.Fatal(err)
			}
			defer f.Close()
			tracker := NewTrackingReadSeeker(f)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				f.Seek(0, io.SeekStart) //nolint:errcheck
				if _, err := mcapReadSingleTopic(tracker, tc.topic); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			reportIOMetrics(b, tracker)
		})

		for _, pair := range configPairs {
			b.Run(tc.name+"/td/"+pair.Label, func(b *testing.B) {
				f, err := os.Open(tdFilePaths[pair.Label])
				if err != nil {
					b.Fatal(err)
				}
				defer f.Close()
				tracker := NewTrackingReadSeeker(f)

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					f.Seek(0, io.SeekStart) //nolint:errcheck
					if _, err := tdReadSingleTopic(tracker, tc.topic); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				reportIOMetrics(b, tracker)
			})
		}
	}
}

// BenchmarkReadTimeRange benchmarks reading messages across multiple topics in the middle-third
// time window of the file.
// Sub-benchmarks: mcap, td/<config>.
func BenchmarkReadTimeRange(b *testing.B) {
	if *mcapFlag == "" {
		b.Skip("no -mcap flag provided")
	}
	if midStartTs == 0 && midEndTs == 0 {
		b.Skip("no timestamp info available (MCAP file has no statistics)")
	}

	b.Run("mcap", func(b *testing.B) {
		f, err := os.Open(*mcapFlag)
		if err != nil {
			b.Fatal(err)
		}
		defer f.Close()
		tracker := NewTrackingReadSeeker(f)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			f.Seek(0, io.SeekStart) //nolint:errcheck
			if _, err := mcapReadTimeRange(tracker, timeRangeTopics, uint64(midStartTs), uint64(midEndTs)); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		reportIOMetrics(b, tracker)
	})

	for _, pair := range configPairs {
		b.Run("td/"+pair.Label, func(b *testing.B) {
			f, err := os.Open(tdFilePaths[pair.Label])
			if err != nil {
				b.Fatal(err)
			}
			defer f.Close()
			tracker := NewTrackingReadSeeker(f)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				f.Seek(0, io.SeekStart) //nolint:errcheck
				if _, err := tdReadTimeRange(tracker, timeRangeTopics, midStartTs, midEndTs); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			reportIOMetrics(b, tracker)
		})
	}
}

// BenchmarkWrite benchmarks writing all pre-loaded messages to io.Discard.
// MCAP has no equivalent chunk config parameter, so only TD variants are benchmarked here.
// Sub-benchmarks: td/<config>.
func BenchmarkWrite(b *testing.B) {
	if *mcapFlag == "" {
		b.Skip("no -mcap flag provided")
	}

	for _, pair := range configPairs {
		b.Run("td/"+pair.Label, func(b *testing.B) {
			tw := NewTrackingWriter(io.Discard)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				tw.Reset()
				if _, err := tdWriteGroups(tw, pair); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()

			n := float64(b.N)
			b.ReportMetric(float64(tw.WriteBytes)/n, "write_bytes/op")
			b.ReportMetric(float64(tw.WriteCalls)/n, "write_calls/op")
		})
	}
}
