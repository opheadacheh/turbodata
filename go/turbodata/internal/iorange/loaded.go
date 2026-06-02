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

package iorange

// LoadedBytes holds bytes that were pre-fetched in one or more ReadOps and
// provides flat offset-based lookup back to the original requested ranges.
//
// It is the bridge between the planner+fetcher (which think in coalesced and
// split byte ranges) and the consumers (which think in their original
// per-range terms — index chunks, data chunks, individual messages).
type LoadedBytes struct {
	buffers [][]byte
	index   map[int64]bytesLocation
}

type bytesLocation struct {
	bufferIdx int
	inBufOff  int
	length    int
}

// NewLoadedBytes assembles a LoadedBytes from a planner's locations and the
// fetcher's per-op buffers. ranges is the original input slice passed to Plan,
// in the same order; locations[i] describes where ranges[i] lives inside one
// of bufs. len(bufs) must equal the number of ops the planner produced.
func NewLoadedBytes(ranges []Range, locations []RangeLocation, bufs [][]byte) *LoadedBytes {
	lb := &LoadedBytes{
		buffers: bufs,
		index:   make(map[int64]bytesLocation, len(ranges)),
	}
	for i, r := range ranges {
		lb.index[r.Offset] = bytesLocation{
			bufferIdx: locations[i].OpIndex,
			inBufOff:  locations[i].InOpOff,
			length:    locations[i].Length,
		}
	}
	return lb
}

// Get returns the slice for the range registered at offset, or nil if no such
// range was registered. The returned slice aliases the underlying buffer; the
// caller must not mutate it.
func (lb *LoadedBytes) Get(offset int64) []byte {
	loc, ok := lb.index[offset]
	if !ok {
		return nil
	}
	return lb.buffers[loc.bufferIdx][loc.inBufOff : loc.inBufOff+loc.length]
}
