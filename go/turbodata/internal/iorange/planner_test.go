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

import (
	"math"
	"reflect"
	"testing"
)

func TestPlanEmpty(t *testing.T) {
	ops, locs := Plan(nil, 16, math.MaxInt64)
	if len(ops) != 0 {
		t.Fatalf("expected no ops, got %d", len(ops))
	}
	if len(locs) != 0 {
		t.Fatalf("expected no locations, got %d", len(locs))
	}
}

func TestPlanSingleRange(t *testing.T) {
	ranges := []Range{{Offset: 100, Length: 50}}
	ops, locs := Plan(ranges, 0, math.MaxInt64)
	if len(ops) != 1 {
		t.Fatalf("expected 1 op, got %d", len(ops))
	}
	if ops[0] != (ReadOp{Offset: 100, Length: 50}) {
		t.Errorf("op = %+v", ops[0])
	}
	if locs[0] != (RangeLocation{OpIndex: 0, InOpOff: 0, Length: 50}) {
		t.Errorf("loc = %+v", locs[0])
	}
}

func TestPlanCoalesceTable(t *testing.T) {
	tests := []struct {
		name        string
		ranges      []Range
		coalesceGap int64
		wantOps     []ReadOp
		wantLocs    []RangeLocation
	}{
		{
			name:        "no coalesce when gap >= threshold",
			ranges:      []Range{{Offset: 0, Length: 10}, {Offset: 20, Length: 10}},
			coalesceGap: 10, // gap=10 NOT < 10 → don't merge
			wantOps: []ReadOp{
				{Offset: 0, Length: 10},
				{Offset: 20, Length: 10},
			},
			wantLocs: []RangeLocation{
				{OpIndex: 0, InOpOff: 0, Length: 10},
				{OpIndex: 1, InOpOff: 0, Length: 10},
			},
		},
		{
			name:        "coalesce when gap < threshold",
			ranges:      []Range{{Offset: 0, Length: 10}, {Offset: 20, Length: 10}},
			coalesceGap: 11, // gap=10 < 11 → merge
			wantOps:     []ReadOp{{Offset: 0, Length: 30}},
			wantLocs: []RangeLocation{
				{OpIndex: 0, InOpOff: 0, Length: 10},
				{OpIndex: 0, InOpOff: 20, Length: 10},
			},
		},
		{
			name:        "coalesce adjacent (gap=0)",
			ranges:      []Range{{Offset: 0, Length: 10}, {Offset: 10, Length: 5}},
			coalesceGap: 1,
			wantOps:     []ReadOp{{Offset: 0, Length: 15}},
			wantLocs: []RangeLocation{
				{OpIndex: 0, InOpOff: 0, Length: 10},
				{OpIndex: 0, InOpOff: 10, Length: 5},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ops, locs := Plan(tc.ranges, tc.coalesceGap, math.MaxInt64)
			if !reflect.DeepEqual(ops, tc.wantOps) {
				t.Errorf("ops mismatch:\n got %+v\nwant %+v", ops, tc.wantOps)
			}
			if !reflect.DeepEqual(locs, tc.wantLocs) {
				t.Errorf("locs mismatch:\n got %+v\nwant %+v", locs, tc.wantLocs)
			}
		})
	}
}

func TestPlanSplitTable(t *testing.T) {
	tests := []struct {
		name           string
		ranges         []Range
		coalesceGap    int64
		splitThreshold int64
		wantOps        []ReadOp
	}{
		{
			name:           "no split below threshold",
			ranges:         []Range{{Offset: 0, Length: 40}, {Offset: 40, Length: 40}},
			coalesceGap:    1,
			splitThreshold: 100,
			wantOps:        []ReadOp{{Offset: 0, Length: 80}},
		},
		{
			name:           "split greedy at member boundary",
			ranges:         []Range{{Offset: 0, Length: 40}, {Offset: 40, Length: 40}, {Offset: 80, Length: 40}},
			coalesceGap:    1,
			splitThreshold: 100,
			// pack 0+40, including next (80) would push to 120 > 100; flush.
			// Then 80+40 alone, next would push to 160 > 100; flush.
			// Then 120+40.
			wantOps: []ReadOp{
				{Offset: 0, Length: 80},
				{Offset: 80, Length: 40},
			},
		},
		{
			name:           "single oversize range stays as one op",
			ranges:         []Range{{Offset: 0, Length: 250}},
			coalesceGap:    1,
			splitThreshold: 100,
			wantOps:        []ReadOp{{Offset: 0, Length: 250}},
		},
		{
			name:           "first member oversize then small members trail",
			ranges:         []Range{{Offset: 0, Length: 250}, {Offset: 250, Length: 10}, {Offset: 260, Length: 10}},
			coalesceGap:    1,
			splitThreshold: 100,
			// 250 stays as own op (atomic), then 10+10.
			wantOps: []ReadOp{
				{Offset: 0, Length: 250},
				{Offset: 250, Length: 20},
			},
		},
		{
			name:           "exact threshold boundary keeps one op",
			ranges:         []Range{{Offset: 0, Length: 50}, {Offset: 50, Length: 50}},
			coalesceGap:    1,
			splitThreshold: 100,
			// 50+50 = exactly 100, not strictly > threshold → one op.
			wantOps: []ReadOp{{Offset: 0, Length: 100}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ops, locs := Plan(tc.ranges, tc.coalesceGap, tc.splitThreshold)
			if !reflect.DeepEqual(ops, tc.wantOps) {
				t.Errorf("ops mismatch:\n got %+v\nwant %+v", ops, tc.wantOps)
			}
			// Sanity check: every original range, when read out of its op's
			// buffer at locs[i], spans the original Offset..Offset+Length.
			for i, r := range tc.ranges {
				op := ops[locs[i].OpIndex]
				if op.Offset+int64(locs[i].InOpOff) != r.Offset {
					t.Errorf("range[%d]: location maps to absolute %d, want %d", i, op.Offset+int64(locs[i].InOpOff), r.Offset)
				}
				if int64(locs[i].Length) != r.Length {
					t.Errorf("range[%d]: location length %d, want %d", i, locs[i].Length, r.Length)
				}
			}
		})
	}
}

func TestPlanSplitDisabled(t *testing.T) {
	ranges := []Range{
		{Offset: 0, Length: 100},
		{Offset: 100, Length: 100},
		{Offset: 200, Length: 100},
	}
	// splitThreshold=0 means "no splitting"; the merged group emits as one op.
	ops, _ := Plan(ranges, 1, 0)
	if len(ops) != 1 {
		t.Fatalf("expected 1 op when splits disabled, got %d", len(ops))
	}
	if ops[0] != (ReadOp{Offset: 0, Length: 300}) {
		t.Errorf("op = %+v", ops[0])
	}
}
