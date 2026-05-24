package benchmark

import (
	"testing"
)

// BenchmarkChunkConfig (Obj 3) compares the canonical fixture against the
// improved fixture across four read scenarios. Both fixtures hold the same
// messages; they differ only in chunk size, compression, and topic
// grouping.
//
// Sub-benchmarks: BenchmarkChunkConfig/{scenario}/{config}.
//   - scenario: all | single_image | single_nonimage | selected_range
//   - config:   canonical | improved
//
// Canonical fixture: all topics in one group, 1 MiB chunks, all compressed.
// Improved fixture: per-image-topic groups (uncompressed, 1 MiB chunks),
// all non-image topics in one group (compressed, 64 KiB chunks).
//
// What we expect to learn:
//   - "all": sequential full scan; chunk config differences should be small.
//   - "single_image": improved should reduce bytes/op because image data
//     is no longer interleaved with other topics in the same chunk.
//   - "single_nonimage": improved may pull *more* bytes/op for /tf because
//     /tf shares chunks with calibration + torque topics, but with very
//     small chunks the overhead is bounded.
//   - "selected_range": improved should win across the board because the
//     non-image topics are co-located in the same chunks.
func BenchmarkChunkConfig(b *testing.B) {
	if mcapInfo == nil {
		b.Skip("no -mcap flag provided")
	}
	if pickedImageTopic == "" || pickedNonImageTopic == "" {
		b.Skip("could not derive scenario picks from MCAP")
	}

	scenarios := []struct {
		name     string
		scenario Scenario
	}{
		{
			"all",
			Scenario{Name: "all"},
		},
		{
			"single_image",
			Scenario{Name: "single_image", Topics: []string{pickedImageTopic}},
		},
		{
			"single_nonimage",
			Scenario{Name: "single_nonimage", Topics: []string{pickedNonImageTopic}},
		},
		{
			"selected_range",
			Scenario{
				Name:           "selected_range",
				Topics:         pickedRangeTopics,
				StartTimestamp: rangeStartTs,
				EndTimestamp:   rangeEndTs,
			},
		},
	}

	configs := []struct {
		name string
		path string
	}{
		{"canonical", tdCanonicalPath},
		{"improved", tdImprovedPath},
	}

	for _, sc := range scenarios {
		for _, cfg := range configs {
			sc, cfg := sc, cfg
			b.Run(sc.name+"/"+cfg.name, func(b *testing.B) {
				runReadBench(b, cfg.path, func(rs *TrackingReadSeeker) error {
					_, err := runTd(rs, sc.scenario)
					return err
				})
			})
		}
	}
}
