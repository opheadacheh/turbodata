// mcap-analyze walks an MCAP file and prints per-topic statistics needed to
// inform turbodata chunk-config decisions: message count, size distribution
// (mean / p50 / p99 / max), message rate, total bytes, and total share of
// the file. Topics are split into image (foxglove.RawImage) and non-image
// groups for separate analysis.
//
// Usage:
//
//	go run ./benchmark/cmd/mcap-analyze -mcap /path/to/file.mcap
//
// Output is plain text on stdout, designed to be human-readable.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/foxglove/mcap/go/mcap"
)

type topicStats struct {
	topic      string
	schemaName string
	count      int
	totalBytes int64
	startTs    int64
	endTs      int64
	sizes      []int // every message size, for percentile computation
}

func (s *topicStats) durationNs() int64 {
	if s.endTs <= s.startTs {
		return 0
	}
	return s.endTs - s.startTs
}

func (s *topicStats) durationSec() float64 {
	return float64(s.durationNs()) / 1e9
}

func (s *topicStats) ratePerSec() float64 {
	d := s.durationSec()
	if d == 0 {
		return 0
	}
	return float64(s.count) / d
}

func (s *topicStats) bytesPerSec() float64 {
	d := s.durationSec()
	if d == 0 {
		return 0
	}
	return float64(s.totalBytes) / d
}

func (s *topicStats) meanSize() float64 {
	if s.count == 0 {
		return 0
	}
	return float64(s.totalBytes) / float64(s.count)
}

// percentile returns the size at the given percentile (0..100). Caller must
// have sorted s.sizes ascending.
func (s *topicStats) percentile(p float64) int {
	if len(s.sizes) == 0 {
		return 0
	}
	idx := int(float64(len(s.sizes)-1) * p / 100)
	return s.sizes[idx]
}

func main() {
	mcapPath := flag.String("mcap", "", "path to MCAP file (required)")
	flag.Parse()
	if *mcapPath == "" {
		fmt.Fprintln(os.Stderr, "error: -mcap is required")
		os.Exit(2)
	}
	if err := run(*mcapPath); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	fi, _ := f.Stat()

	r, err := mcap.NewReader(f)
	if err != nil {
		return err
	}
	defer r.Close()

	info, err := r.Info()
	if err != nil {
		return err
	}

	stats := make(map[string]*topicStats, len(info.Channels))
	for _, ch := range info.Channels {
		schemaName := ""
		if s := info.Schemas[ch.SchemaID]; s != nil {
			schemaName = s.Name
		}
		stats[ch.Topic] = &topicStats{
			topic:      ch.Topic,
			schemaName: schemaName,
			startTs:    -1,
		}
	}

	it, err := r.Messages(mcap.InOrder(mcap.LogTimeOrder))
	if err != nil {
		return err
	}
	msg := &mcap.Message{}
	channelToTopic := make(map[uint16]string, len(info.Channels))
	for _, ch := range info.Channels {
		channelToTopic[ch.ID] = ch.Topic
	}

	for {
		_, _, _, err := it.NextInto(msg)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		topic := channelToTopic[msg.ChannelID]
		st := stats[topic]
		if st == nil {
			continue
		}
		st.count++
		size := len(msg.Data)
		st.totalBytes += int64(size)
		st.sizes = append(st.sizes, size)
		ts := int64(msg.LogTime)
		if st.startTs == -1 || ts < st.startTs {
			st.startTs = ts
		}
		if ts > st.endTs {
			st.endTs = ts
		}
	}

	// Sort sizes per topic for percentile computation.
	for _, st := range stats {
		sort.Ints(st.sizes)
	}

	// Compute totals across all topics.
	var grandTotalBytes int64
	for _, st := range stats {
		grandTotalBytes += st.totalBytes
	}

	// Partition.
	var imageTopics, nonImageTopics []*topicStats
	for _, st := range stats {
		if st.schemaName == "foxglove.RawImage" {
			imageTopics = append(imageTopics, st)
		} else {
			nonImageTopics = append(nonImageTopics, st)
		}
	}
	// Sort each group by total bytes desc (largest first).
	sort.Slice(imageTopics, func(i, j int) bool {
		return imageTopics[i].totalBytes > imageTopics[j].totalBytes
	})
	sort.Slice(nonImageTopics, func(i, j int) bool {
		return nonImageTopics[i].totalBytes > nonImageTopics[j].totalBytes
	})

	// Header.
	fmt.Printf("MCAP: %s\n", path)
	fmt.Printf("File size: %s\n", humanBytes(fi.Size()))
	if info.Statistics != nil {
		fmt.Printf("Messages : %d\n", info.Statistics.MessageCount)
		fmt.Printf("Channels : %d\n", info.Statistics.ChannelCount)
		fmt.Printf("Time span: %.3fs\n",
			float64(info.Statistics.MessageEndTime-info.Statistics.MessageStartTime)/1e9)
	}
	fmt.Printf("Uncompressed payload: %s\n", humanBytes(grandTotalBytes))
	fmt.Println()

	printGroup("IMAGE TOPICS", imageTopics, grandTotalBytes)
	printGroup("NON-IMAGE TOPICS", nonImageTopics, grandTotalBytes)

	printSummary(imageTopics, nonImageTopics, grandTotalBytes)
	return nil
}

func printGroup(label string, group []*topicStats, grandTotal int64) {
	fmt.Printf("== %s (%d) ==\n", label, len(group))
	if len(group) == 0 {
		fmt.Println("  (none)")
		fmt.Println()
		return
	}
	fmt.Printf("%-50s  %-22s  %8s  %10s  %10s  %10s  %10s  %10s  %8s  %8s\n",
		"topic", "schema", "count", "mean", "p50", "p99", "max", "total", "rate/s", "share")
	fmt.Println(repeatStr("-", 170))
	for _, st := range group {
		share := 0.0
		if grandTotal > 0 {
			share = float64(st.totalBytes) / float64(grandTotal) * 100
		}
		fmt.Printf("%-50s  %-22s  %8d  %10s  %10s  %10s  %10s  %10s  %8.1f  %7.2f%%\n",
			truncate(st.topic, 50),
			truncate(st.schemaName, 22),
			st.count,
			humanBytes(int64(st.meanSize())),
			humanBytes(int64(st.percentile(50))),
			humanBytes(int64(st.percentile(99))),
			humanBytes(int64(st.percentile(100))),
			humanBytes(st.totalBytes),
			st.ratePerSec(),
			share,
		)
	}
	fmt.Println()
}

// printSummary computes group-level totals to inform chunking decisions.
func printSummary(image, nonImage []*topicStats, grandTotal int64) {
	fmt.Println("== SUMMARY ==")

	groupTotal := func(g []*topicStats) (count int, bytes int64) {
		for _, st := range g {
			count += st.count
			bytes += st.totalBytes
		}
		return
	}
	imgCount, imgBytes := groupTotal(image)
	niCount, niBytes := groupTotal(nonImage)

	fmt.Printf("  image    : %d topics, %d msgs, %s (%.1f%% of payload)\n",
		len(image), imgCount, humanBytes(imgBytes),
		shareOf(imgBytes, grandTotal))
	fmt.Printf("  non-image: %d topics, %d msgs, %s (%.1f%% of payload)\n",
		len(nonImage), niCount, humanBytes(niBytes),
		shareOf(niBytes, grandTotal))
	fmt.Println()

	// Suggest chunk sizes: for each group, what 1MB / 4MB chunks would mean.
	fmt.Println("  Chunking hints (size-based, per group):")
	fmt.Println("    A chunk of size S holds ~S/mean_msg_size messages of that group.")
	for _, line := range chunkHints("image    ", image) {
		fmt.Println("   ", line)
	}
	for _, line := range chunkHints("non-image", nonImage) {
		fmt.Println("   ", line)
	}
}

func chunkHints(label string, group []*topicStats) []string {
	if len(group) == 0 {
		return nil
	}
	var totalBytes int64
	var totalCount int
	for _, st := range group {
		totalBytes += st.totalBytes
		totalCount += st.count
	}
	if totalCount == 0 {
		return nil
	}
	mean := float64(totalBytes) / float64(totalCount)
	out := make([]string, 0, 4)
	for _, sz := range []int64{256 << 10, 1 << 20, 4 << 20, 16 << 20} {
		msgsPerChunk := float64(sz) / mean
		chunks := float64(totalBytes) / float64(sz)
		out = append(out, fmt.Sprintf(
			"%s @ %s chunk: ~%.0f msgs/chunk, ~%.0f chunks total",
			label, humanBytes(sz), msgsPerChunk, chunks,
		))
	}
	return out
}

func shareOf(part, total int64) float64 {
	if total == 0 {
		return 0
	}
	return float64(part) / float64(total) * 100
}

func humanBytes(n int64) string {
	const (
		KiB = 1 << 10
		MiB = 1 << 20
		GiB = 1 << 30
	)
	v := float64(n)
	switch {
	case v >= GiB:
		return fmt.Sprintf("%.2fGiB", v/GiB)
	case v >= MiB:
		return fmt.Sprintf("%.2fMiB", v/MiB)
	case v >= KiB:
		return fmt.Sprintf("%.2fKiB", v/KiB)
	default:
		return fmt.Sprintf("%dB", n)
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	if n <= 3 {
		return s[:n]
	}
	return s[:n-3] + "..."
}

func repeatStr(s string, n int) string {
	out := make([]byte, 0, len(s)*n)
	for i := 0; i < n; i++ {
		out = append(out, s...)
	}
	return string(out)
}
