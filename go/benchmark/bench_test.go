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
	"flag"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/opheadacheh/turbodata/go/benchmark/mockcompress"
)

var (
	mcapFlag          = flag.String("mcap", "", "path to input MCAP file (required to run benchmarks)")
	heapFlag          = flag.Bool("heap", false, "enable heap sampling (perturbs timings; use in a separate run)")
	mockImageSizeFlag = flag.String("mock-image-size", "60KB",
		"if non-empty, replace foxglove.RawImage payloads with this many bytes of pseudo-random data. "+
			"Mimics how files would look if images were stored already-compressed (JPEG/H.264). "+
			"The mocked file is built once at <input>.mock.mcap and reused. Set to empty to disable.")
)

// Package-level state populated by TestMain. Bench files read these directly.
var (
	// sourcePath is the path actually used by all benchmarks. It is either
	// the user-supplied -mcap path verbatim, or the auto-built mocked
	// version next to it (when -mock-image-size is non-empty).
	sourcePath string

	// mcapInfo describes sourcePath (not the original -mcap if mocked).
	mcapInfo *MCAPInfo

	// Path to the canonical 1MB-compressed .td fixture. Used by objectives 1 & 2.
	tdCanonicalPath string

	// Path to the canonical 1MB-compressed .mcap fixture (re-encoded from the
	// source MCAP with our standard chunk size/compression). Read benchmarks
	// use this — not the raw source — so MCAP and TD are compared on the
	// same encoding pipeline.
	mcapCanonicalPath string

	// Path to the improved .td fixture: per-image-topic groups (uncompressed,
	// 1 MiB chunks), all non-image topics in one group (compressed, 64 KiB
	// chunks). Used by objectives 3 and 4.
	tdImprovedPath string

	// Scenario picks derived from mcapInfo at TestMain time. These drive
	// BenchmarkChunkConfig (Obj 3) and BenchmarkUseCase (Obj 4).
	pickedImageTopic    string   // first image topic
	pickedNonImageTopic string   // /tf if present, else first non-image topic
	pickedRangeTopics   []string // [pickedImageTopic, pickedNonImageTopic, one extra non-image]
	rangeStartTs        int64    // middle of file, inclusive
	rangeEndTs          int64    // start + 1s

	// Phase D will populate:
	//   tdUseCasePath = tdImprovedPath (we reuse the same fixture)
)

func TestMain(m *testing.M) {
	flag.Parse()

	if *heapFlag {
		setHeapEnabled(true)
	}

	if *mcapFlag == "" {
		fmt.Fprintln(os.Stderr, "no -mcap flag provided; benchmarks will be skipped")
		os.Exit(m.Run())
	}

	src, err := ensureMockSource(*mcapFlag, *mockImageSizeFlag)
	if err != nil {
		log.Fatalf("ensure mock source: %v", err)
	}
	sourcePath = src

	mi, err := loadMCAPInfo(sourcePath)
	if err != nil {
		log.Fatalf("benchmark setup: %v", err)
	}
	mcapInfo = mi
	fmt.Printf("mcap: %s\n", mi.Path)
	fmt.Printf("  image topics    : %d\n", len(mi.ImageTopics))
	fmt.Printf("  non-image topics: %d\n", len(mi.NonImageTopics))
	fmt.Printf("  time span       : [%d, %d]\n", mi.StartTimestamp, mi.EndTimestamp)

	pickScenarioInputs(mi)
	fmt.Printf("scenarios: img=%q nonimg=%q range_topics=%v range=[%d, %d]\n",
		pickedImageTopic, pickedNonImageTopic, pickedRangeTopics, rangeStartTs, rangeEndTs)

	tdPath, err := ensureCanonicalTd(mi)
	if err != nil {
		log.Fatalf("ensure canonical td: %v", err)
	}
	tdCanonicalPath = tdPath

	mcapPath, err := ensureCanonicalMcap(mi)
	if err != nil {
		log.Fatalf("ensure canonical mcap: %v", err)
	}
	mcapCanonicalPath = mcapPath

	improvedPath, err := ensureImprovedTd(mi)
	if err != nil {
		log.Fatalf("ensure improved td: %v", err)
	}
	tdImprovedPath = improvedPath

	os.Exit(m.Run())
}

// pickScenarioInputs derives deterministic topic and time-range picks from
// the MCAP info. Used by Obj 3 and Obj 4 sub-benchmarks.
func pickScenarioInputs(mi *MCAPInfo) {
	if len(mi.ImageTopics) > 0 {
		pickedImageTopic = mi.ImageTopics[0]
	}
	// Prefer /tf since it has the most non-image volume in robotics MCAPs;
	// fall back to the first non-image topic.
	for _, t := range mi.NonImageTopics {
		if t == "/tf" {
			pickedNonImageTopic = t
			break
		}
	}
	if pickedNonImageTopic == "" && len(mi.NonImageTopics) > 0 {
		pickedNonImageTopic = mi.NonImageTopics[0]
	}

	// Range topics: 1 image + the picked non-image + one extra non-image
	// (a torque/sensor topic if present, else the second non-image).
	if pickedImageTopic != "" {
		pickedRangeTopics = append(pickedRangeTopics, pickedImageTopic)
	}
	if pickedNonImageTopic != "" {
		pickedRangeTopics = append(pickedRangeTopics, pickedNonImageTopic)
	}
	for _, t := range mi.NonImageTopics {
		if t == pickedNonImageTopic {
			continue
		}
		pickedRangeTopics = append(pickedRangeTopics, t)
		break
	}

	// Middle 1s window.
	if mi.EndTimestamp > mi.StartTimestamp {
		mid := mi.StartTimestamp + (mi.EndTimestamp-mi.StartTimestamp)/2
		rangeStartTs = mid
		rangeEndTs = mid + 1_000_000_000
		if rangeEndTs > mi.EndTimestamp {
			rangeEndTs = mi.EndTimestamp
		}
	}
}

// ensureMockSource returns the path benchmarks should treat as the source
// MCAP. When sizeStr is empty, the original is used. Otherwise a sibling
// "<input>.mock.mcap" is built once with images replaced by random bytes
// (mimicking already-compressed image data) and reused on subsequent runs.
func ensureMockSource(origPath, sizeStr string) (string, error) {
	if sizeStr == "" {
		return origPath, nil
	}
	size, err := mockcompress.ParseSize(sizeStr)
	if err != nil {
		return "", fmt.Errorf("parse -mock-image-size: %w", err)
	}
	dst := origPath + ".mock.mcap"
	if fi, err := os.Stat(dst); err == nil {
		fmt.Printf("mock-source: reusing %s (%d bytes; image payload=%d bytes)\n", dst, fi.Size(), size)
		return dst, nil
	}
	fmt.Printf("mock-source: building %s (image payload=%d bytes)\n", dst, size)
	res, err := mockcompress.Run(origPath, dst, size, 42)
	if err != nil {
		return "", err
	}
	fmt.Printf("mock-source: rewrote %d images, copied %d others; %d -> %d bytes\n",
		res.ImagesRewritten, res.OthersCopied, res.InputBytes, res.OutputBytes)
	return dst, nil
}
