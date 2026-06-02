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

// mcap-mock-compress is a thin CLI wrapper around the mockcompress package.
// See the package doc for what it does.
//
// Usage:
//
//	mcap-mock-compress -in input.mcap -out output.mcap [-size 60KB] [-seed 42]
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/opheadacheh/turbodata/go/benchmark/mockcompress"
)

func main() {
	var (
		inPath  = flag.String("in", "", "path to input MCAP file (required)")
		outPath = flag.String("out", "", "path to output MCAP file (required)")
		sizeStr = flag.String("size", "60KB", "replacement payload size for foxglove.RawImage messages (e.g. 60KB, 200KB, 1MB)")
		seed    = flag.Uint64("seed", 42, "PRNG seed for replacement bytes")
	)
	flag.Parse()

	if *inPath == "" || *outPath == "" {
		fmt.Fprintln(os.Stderr, "error: -in and -out are required")
		flag.Usage()
		os.Exit(2)
	}

	size, err := mockcompress.ParseSize(*sizeStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: invalid -size: %v\n", err)
		os.Exit(2)
	}

	res, err := mockcompress.Run(*inPath, *outPath, size, *seed)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("rewrote %d image messages (payload=%d bytes each)\n", res.ImagesRewritten, size)
	fmt.Printf("copied  %d non-image messages\n", res.OthersCopied)
	fmt.Printf("input:  %s (%d bytes)\n", *inPath, res.InputBytes)
	fmt.Printf("output: %s (%d bytes)\n", *outPath, res.OutputBytes)
}
