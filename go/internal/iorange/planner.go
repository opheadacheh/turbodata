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

// Range is a single atomic byte range requested by the caller. After planning,
// each input range lives entirely inside exactly one ReadOp.
type Range struct {
	Offset int64
	Length int64
}

// ReadOp is one I/O request the fetcher will execute. It is the result of
// coalescing zero-or-more input ranges into a contiguous read (possibly
// reading wasted bytes between them) and optionally splitting an
// over-threshold coalesced range at internal range boundaries.
type ReadOp struct {
	Offset int64
	Length int64
}

// RangeLocation describes where a single original input range ended up after
// planning. The caller uses these to assemble a LoadedBytes once the fetcher
// has returned buffers.
type RangeLocation struct {
	OpIndex int // index into the returned []ReadOp
	InOpOff int // byte offset within that op's buffer
	Length  int
}

// Plan groups ranges into ReadOps. The returned locations slice is parallel
// to the input ranges: locations[i] tells where ranges[i] lives in the
// produced ops.
//
// coalesceGap is the byte gap below which two adjacent ranges are merged into
// a single read; 0 disables coalescing. splitThreshold is the size above which
// a merged op is sliced into parallel sub-ops at internal range boundaries;
// 0 or math.MaxInt64 disables splitting. These two values typically come from
// a ReadStrategy in the public readstrategy package; Plan itself stays
// strategy-agnostic so it can be reused for other planning callers.
//
// Precondition: ranges must be in non-decreasing Offset order. Callers in this
// module satisfy this by construction (the writer lays out groups and chunks
// in increasing file offset, and range builders walk them in the same order).
// Behavior is undefined if this precondition is violated.
//
// Algorithm:
//  1. Coalesce: merge adjacent ranges whose gap is < coalesceGap.
//  2. Split: a merged op whose length exceeds splitThreshold is sliced at
//     internal range boundaries via greedy packing. A single range larger
//     than splitThreshold stays as one oversize op — atomic units are never
//     broken.
func Plan(ranges []Range, coalesceGap, splitThreshold int64) (ops []ReadOp, locations []RangeLocation) {
	locations = make([]RangeLocation, len(ranges))
	if len(ranges) == 0 {
		return nil, locations
	}

	// Coalesce pass: produce groups of original indices, each group becoming
	// a single contiguous read at this stage.
	type group struct {
		offset  int64
		end     int64 // exclusive
		members []int // indices into ranges, in offset order
	}
	groups := make([]group, 0, len(ranges))
	for idx, r := range ranges {
		if len(groups) == 0 {
			groups = append(groups, group{
				offset:  r.Offset,
				end:     r.Offset + r.Length,
				members: []int{idx},
			})
			continue
		}
		cur := &groups[len(groups)-1]
		gap := r.Offset - cur.end
		if gap >= 0 && gap < coalesceGap {
			if r.Offset+r.Length > cur.end {
				cur.end = r.Offset + r.Length
			}
			cur.members = append(cur.members, idx)
			continue
		}
		groups = append(groups, group{
			offset:  r.Offset,
			end:     r.Offset + r.Length,
			members: []int{idx},
		})
	}

	// Split pass: for each group, emit one or more ReadOps. If
	// splitThreshold is non-positive, the whole group becomes one op.
	threshold := splitThreshold
	splitDisabled := threshold <= 0

	for _, g := range groups {
		if splitDisabled || (g.end-g.offset) <= threshold {
			// Single op covers the whole group.
			opIndex := len(ops)
			ops = append(ops, ReadOp{Offset: g.offset, Length: g.end - g.offset})
			for _, mi := range g.members {
				locations[mi] = RangeLocation{
					OpIndex: opIndex,
					InOpOff: int(ranges[mi].Offset - g.offset),
					Length:  int(ranges[mi].Length),
				}
			}
			continue
		}

		// Greedy split at range boundaries.
		opStart := g.members[0]
		opStartIdx := 0
		opStartOffset := ranges[opStart].Offset
		opEndOffset := ranges[opStart].Offset + ranges[opStart].Length

		flushOp := func(lastMember int) {
			opIndex := len(ops)
			ops = append(ops, ReadOp{Offset: opStartOffset, Length: opEndOffset - opStartOffset})
			for k := opStartIdx; k <= lastMember; k++ {
				mi := g.members[k]
				locations[mi] = RangeLocation{
					OpIndex: opIndex,
					InOpOff: int(ranges[mi].Offset - opStartOffset),
					Length:  int(ranges[mi].Length),
				}
			}
		}

		for k := 1; k < len(g.members); k++ {
			mi := g.members[k]
			nextEnd := ranges[mi].Offset + ranges[mi].Length
			if nextEnd-opStartOffset > threshold {
				// Including this member would push past threshold; close
				// the current op without it.
				flushOp(k - 1)
				opStartIdx = k
				opStartOffset = ranges[mi].Offset
				opEndOffset = nextEnd
				continue
			}
			opEndOffset = nextEnd
		}
		flushOp(len(g.members) - 1)
	}

	return ops, locations
}
