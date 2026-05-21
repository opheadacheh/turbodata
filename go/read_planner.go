package turbodata

import "sort"

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

// Plan groups ranges into ReadOps according to s. The returned locations slice
// is parallel to the input ranges: locations[i] tells where ranges[i] lives in
// the produced ops.
//
// Algorithm:
//  1. Sort ranges by Offset, remembering original indices.
//  2. Coalesce: merge adjacent ranges whose gap is < CoalesceGap.
//  3. Split: a merged op whose length exceeds SplitThreshold is sliced at
//     internal range boundaries via greedy packing. A single range larger
//     than SplitThreshold stays as one oversize op — atomic units are never
//     broken.
func Plan(ranges []Range, s ReadStrategy) (ops []ReadOp, locations []RangeLocation) {
	locations = make([]RangeLocation, len(ranges))
	if len(ranges) == 0 {
		return nil, locations
	}

	// Sort by offset while remembering original indices.
	sorted := make([]int, len(ranges))
	for i := range sorted {
		sorted[i] = i
	}
	sort.SliceStable(sorted, func(i, j int) bool {
		return ranges[sorted[i]].Offset < ranges[sorted[j]].Offset
	})

	// Coalesce pass: produce groups of original indices, each group becoming
	// a single contiguous read at this stage.
	type group struct {
		offset  int64
		end     int64 // exclusive
		members []int // indices into ranges, in offset order
	}
	groups := make([]group, 0, len(ranges))
	for _, idx := range sorted {
		r := ranges[idx]
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
		if gap >= 0 && gap < s.CoalesceGap {
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
	// SplitThreshold is non-positive or larger than int64's range, the
	// whole group becomes one op.
	threshold := s.SplitThreshold
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
