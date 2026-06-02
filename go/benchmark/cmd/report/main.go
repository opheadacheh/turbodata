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

// report parses `go test -bench` output and emits a Markdown summary grouped
// by top-level Benchmark name.
//
// The benchmark suite produces two complementary kinds of metrics:
//
//   - Timing + IO metrics (ns/op, bytes/op, reads/op, ...) come from a run
//     WITHOUT -heap. The -heap flag perturbs timings (background poll +
//     forced GCs at iteration boundaries), so timing data from -heap runs
//     is not trustworthy.
//
//   - Heap metrics (peak_heap_B, peak_heap_delta_B, retained_heap_B,
//     total_alloc_B/op) come from a run WITH -heap.
//
// Run both, then merge them into one report:
//
//	go test ./benchmark/ -bench=. -benchmem -count=3 \
//	    -args -mcap=$PWD/test.mcap > bench_time.txt
//	go test ./benchmark/ -bench=. -benchmem -count=1 \
//	    -args -mcap=$PWD/test.mcap -heap > bench_heap.txt
//	go run ./benchmark/cmd/report \
//	    -time bench_time.txt -heap bench_heap.txt > REPORT.md
//
// Either flag may be omitted; if both are omitted the tool reads from stdin
// and treats it as a single run (back-compat for ad-hoc use).
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
)

// benchLine is a single parsed `BenchmarkXxx/.../...   N   <metrics>` row.
type benchLine struct {
	full    string             // full name e.g. BenchmarkRead/all/td-12
	bench   string             // top-level group e.g. BenchmarkRead
	sub     string             // sub-name e.g. all/td (with the GOMAXPROCS suffix stripped)
	metrics map[string]float64 // metric label -> value
}

// metricSources lists which metrics belong to which run. Metrics not listed
// here are passed through from whichever file provides them.
var (
	timingMetrics = map[string]bool{
		"ns/op":          true,
		"bytes/op":       true,
		"reads/op":       true,
		"seeks/op":       true,
		"write_bytes/op": true,
		"write_calls/op": true,
		"uUSD/op":        true,
		"B/op":           true,
		"allocs/op":      true,
	}
	heapMetrics = map[string]bool{
		"peak_heap_B":       true,
		"peak_heap_delta_B": true,
		"retained_heap_B":   true,
		"total_alloc_B/op":  true,
	}
)

func main() {
	var (
		timePath = flag.String("time", "", "path to bench output from a run WITHOUT -heap (timing metrics)")
		heapPath = flag.String("heap", "", "path to bench output from a run WITH -heap (heap metrics)")
	)
	flag.Parse()

	if err := run(*timePath, *heapPath, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "report:", err)
		os.Exit(1)
	}
}

func run(timePath, heapPath string, out io.Writer) error {
	var timeLines, heapLines []benchLine

	switch {
	case timePath == "" && heapPath == "":
		// Back-compat: read stdin as a single run.
		ls, err := parseFrom(os.Stdin)
		if err != nil {
			return err
		}
		if len(ls) == 0 {
			return fmt.Errorf("no benchmark lines found in stdin")
		}
		emitMarkdown(out, ls, ls)
		return nil
	default:
		if timePath != "" {
			ls, err := parseFile(timePath)
			if err != nil {
				return fmt.Errorf("read %s: %w", timePath, err)
			}
			timeLines = ls
		}
		if heapPath != "" {
			ls, err := parseFile(heapPath)
			if err != nil {
				return fmt.Errorf("read %s: %w", heapPath, err)
			}
			heapLines = ls
		}
	}
	if len(timeLines) == 0 && len(heapLines) == 0 {
		return fmt.Errorf("no benchmark lines found")
	}
	emitMarkdown(out, timeLines, heapLines)
	return nil
}

func parseFile(path string) ([]benchLine, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return parseFrom(f)
}

// parseFrom reads `go test -bench` output and returns one benchLine per row.
// Lines starting with "Benchmark" and matching the standard format are
// accepted; everything else is ignored. When the same (bench, sub) appears
// multiple times (e.g. -count=N), later rows overwrite earlier ones — we
// keep the last sample.
func parseFrom(in io.Reader) ([]benchLine, error) {
	var out []benchLine
	sc := bufio.NewScanner(in)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "Benchmark") {
			continue
		}
		bl, ok := parseLine(line)
		if !ok {
			continue
		}
		out = append(out, bl)
	}
	return out, sc.Err()
}

// parseLine parses a single bench output row. Format (whitespace-separated):
//
//	BenchmarkName/sub-N   iters   value unit   value unit   ...
//
// The iteration count at fields[1] is dropped from metrics.
func parseLine(s string) (benchLine, bool) {
	fields := strings.Fields(s)
	if len(fields) < 4 {
		return benchLine{}, false
	}
	full := fields[0]
	bl := benchLine{
		full:    full,
		metrics: make(map[string]float64),
	}
	bl.bench, bl.sub = splitName(full)

	rest := fields[2:]
	for i := 0; i+1 < len(rest); i += 2 {
		var v float64
		if _, err := fmt.Sscanf(rest[i], "%g", &v); err != nil {
			continue
		}
		bl.metrics[rest[i+1]] = v
	}
	return bl, true
}

// splitName splits "BenchmarkRead/all/td-12" into ("BenchmarkRead", "all/td").
func splitName(full string) (bench, sub string) {
	name := full
	if i := strings.LastIndex(name, "-"); i > 0 {
		tail := name[i+1:]
		isNum := tail != ""
		for _, c := range tail {
			if c < '0' || c > '9' {
				isNum = false
				break
			}
		}
		if isNum {
			name = name[:i]
		}
	}
	if i := strings.Index(name, "/"); i > 0 {
		return name[:i], name[i+1:]
	}
	return name, ""
}

// rowKey identifies a row across runs.
type rowKey struct{ bench, sub string }

// merge combines timeLines and heapLines into a per-row metric map. For
// metrics in timingMetrics, the value comes from timeLines; for heapMetrics
// it comes from heapLines; otherwise either source is taken (timing first).
// When -count>1, we average duplicates (per (bench, sub)).
func merge(timeLines, heapLines []benchLine) map[rowKey]map[string]float64 {
	timeAvg := averageBySubKey(timeLines)
	heapAvg := averageBySubKey(heapLines)

	keys := make(map[rowKey]bool)
	for k := range timeAvg {
		keys[k] = true
	}
	for k := range heapAvg {
		keys[k] = true
	}

	merged := make(map[rowKey]map[string]float64, len(keys))
	for k := range keys {
		m := make(map[string]float64)
		// Pull each metric from its canonical source, falling back to the
		// other if missing.
		for name, v := range timeAvg[k] {
			if heapMetrics[name] {
				continue // heap source preferred
			}
			m[name] = v
		}
		for name, v := range heapAvg[k] {
			if timingMetrics[name] {
				if _, ok := m[name]; ok {
					continue // timing source preferred
				}
			}
			m[name] = v
		}
		merged[k] = m
	}
	return merged
}

// averageBySubKey collapses repeated samples (e.g. -count=N) by averaging.
func averageBySubKey(lines []benchLine) map[rowKey]map[string]float64 {
	type acc struct {
		sum   map[string]float64
		count map[string]int
	}
	groups := make(map[rowKey]*acc)
	for _, l := range lines {
		k := rowKey{l.bench, l.sub}
		a, ok := groups[k]
		if !ok {
			a = &acc{sum: make(map[string]float64), count: make(map[string]int)}
			groups[k] = a
		}
		for name, v := range l.metrics {
			a.sum[name] += v
			a.count[name]++
		}
	}
	out := make(map[rowKey]map[string]float64, len(groups))
	for k, a := range groups {
		m := make(map[string]float64, len(a.sum))
		for name, s := range a.sum {
			c := a.count[name]
			if c == 0 {
				continue
			}
			m[name] = s / float64(c)
		}
		out[k] = m
	}
	return out
}

// emitMarkdown writes one section per top-level benchmark.
func emitMarkdown(out io.Writer, timeLines, heapLines []benchLine) {
	merged := merge(timeLines, heapLines)

	// Group by bench name, preserving first-seen order from timeLines then heapLines.
	groups := make(map[string][]rowKey)
	var order []string
	seenBench := make(map[string]bool)
	addKey := func(k rowKey) {
		if !seenBench[k.bench] {
			seenBench[k.bench] = true
			order = append(order, k.bench)
		}
		groups[k.bench] = append(groups[k.bench], k)
	}
	seenKey := make(map[rowKey]bool)
	for _, l := range append(append([]benchLine{}, timeLines...), heapLines...) {
		k := rowKey{l.bench, l.sub}
		if seenKey[k] {
			continue
		}
		seenKey[k] = true
		addKey(k)
	}

	fmt.Fprintln(out, "# Benchmark report")
	fmt.Fprintln(out)

	for _, name := range order {
		keys := groups[name]
		fmt.Fprintf(out, "## %s\n\n", name)

		cols := collectColumns(keys, merged)

		fmt.Fprint(out, "| sub |")
		for _, c := range cols {
			fmt.Fprintf(out, " %s |", c)
		}
		fmt.Fprintln(out)
		fmt.Fprint(out, "|---|")
		for range cols {
			fmt.Fprint(out, "---:|")
		}
		fmt.Fprintln(out)

		sort.SliceStable(keys, func(i, j int) bool { return keys[i].sub < keys[j].sub })
		for _, k := range keys {
			row := merged[k]
			fmt.Fprintf(out, "| %s |", k.sub)
			for _, c := range cols {
				v, ok := row[c]
				if !ok {
					fmt.Fprint(out, " — |")
					continue
				}
				fmt.Fprintf(out, " %s |", formatVal(v, c))
			}
			fmt.Fprintln(out)
		}
		fmt.Fprintln(out)
	}
}

// collectColumns returns the union of metric names across the rows in this
// group, in a preferred order.
func collectColumns(keys []rowKey, merged map[rowKey]map[string]float64) []string {
	seen := make(map[string]bool)
	for _, k := range keys {
		for name := range merged[k] {
			seen[name] = true
		}
	}
	preferred := []string{
		"ns/op",
		"bytes/op", "write_bytes/op",
		"reads/op", "seeks/op", "write_calls/op",
		"uUSD/op",
		"B/op", "allocs/op",
		"total_alloc_B/op", "peak_heap_B", "peak_heap_delta_B", "retained_heap_B",
	}
	var out []string
	used := make(map[string]bool)
	for _, p := range preferred {
		if seen[p] {
			out = append(out, p)
			used[p] = true
		}
	}
	var rest []string
	for k := range seen {
		if !used[k] {
			rest = append(rest, k)
		}
	}
	sort.Strings(rest)
	out = append(out, rest...)
	return out
}

// formatVal renders a metric value with units appropriate for the column.
func formatVal(v float64, col string) string {
	switch col {
	case "ns/op":
		if v >= 1e9 {
			return fmt.Sprintf("%.2fs", v/1e9)
		}
		if v >= 1e6 {
			return fmt.Sprintf("%.2fms", v/1e6)
		}
		if v >= 1e3 {
			return fmt.Sprintf("%.2fµs", v/1e3)
		}
		return fmt.Sprintf("%.0fns", v)
	case "bytes/op", "write_bytes/op", "B/op", "total_alloc_B/op", "peak_heap_B", "peak_heap_delta_B", "retained_heap_B":
		return humanBytes(v)
	case "uUSD/op":
		// Render in the most natural unit. Below 1 µUSD, use scientific notation
		// for visibility. Above 1 USD/op (would be bizarre), show dollars.
		switch {
		case v == 0:
			return "0"
		case v >= 1e6:
			return fmt.Sprintf("$%.2f", v/1e6)
		case v >= 1e3:
			return fmt.Sprintf("%.2f mUSD", v/1e3)
		case v >= 1:
			return fmt.Sprintf("%.2f µUSD", v)
		default:
			return fmt.Sprintf("%.3g µUSD", v)
		}
	default:
		if v >= 1e6 {
			return fmt.Sprintf("%.2fM", v/1e6)
		}
		if v >= 1e3 {
			return fmt.Sprintf("%.2fk", v/1e3)
		}
		return fmt.Sprintf("%.0f", v)
	}
}

func humanBytes(v float64) string {
	const (
		KiB = 1 << 10
		MiB = 1 << 20
		GiB = 1 << 30
	)
	switch {
	case v >= GiB:
		return fmt.Sprintf("%.2f GiB", v/GiB)
	case v >= MiB:
		return fmt.Sprintf("%.2f MiB", v/MiB)
	case v >= KiB:
		return fmt.Sprintf("%.2f KiB", v/KiB)
	default:
		return fmt.Sprintf("%.0f B", v)
	}
}
