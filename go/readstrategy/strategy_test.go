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

package readstrategy

import (
	"math"
	"testing"
	"time"
)

func TestStrategyForLatency(t *testing.T) {
	// 50ms RTT, 100 MB/s per stream → bandwidth-latency product = 5 MB.
	s := StrategyForLatency(50*time.Millisecond, 100*1024*1024, 8)
	wantBDP := int64(50*time.Millisecond.Seconds() * float64(100*1024*1024))
	if s.CoalesceGap != wantBDP {
		t.Errorf("CoalesceGap = %d, want %d", s.CoalesceGap, wantBDP)
	}
	if s.SplitThreshold != wantBDP {
		t.Errorf("SplitThreshold = %d, want %d", s.SplitThreshold, wantBDP)
	}
	if s.MaxConcurrency != 8 {
		t.Errorf("MaxConcurrency = %d, want 8", s.MaxConcurrency)
	}
}

func TestStrategyForLatencyZeroProduct(t *testing.T) {
	// Degenerate case: rtt=0 → product=0 → clamp to 1 so we still have a sane bound.
	s := StrategyForLatency(0, 100, 1)
	if s.CoalesceGap != 1 || s.SplitThreshold != 1 {
		t.Errorf("expected clamp to 1, got CoalesceGap=%d SplitThreshold=%d", s.CoalesceGap, s.SplitThreshold)
	}
}

func TestStrategyForMoney(t *testing.T) {
	// $0.0004 per request, $0.00000009 per byte → coalesceGap ≈ 4444 bytes.
	reqPrice, bytePrice := 0.0004, 0.00000009
	s := StrategyForMoney(reqPrice, bytePrice, 4)
	wantGap := int64(reqPrice / bytePrice)
	if s.CoalesceGap != wantGap {
		t.Errorf("CoalesceGap = %d, want %d", s.CoalesceGap, wantGap)
	}
	if s.SplitThreshold != math.MaxInt64 {
		t.Errorf("SplitThreshold = %d, want MaxInt64", s.SplitThreshold)
	}
	if s.MaxConcurrency != 4 {
		t.Errorf("MaxConcurrency = %d, want 4", s.MaxConcurrency)
	}
}

func TestStrategyForMoneyFreeEgress(t *testing.T) {
	// bytePrice=0 → always coalesce regardless of gap.
	s := StrategyForMoney(0.0004, 0, 4)
	if s.CoalesceGap != math.MaxInt64 {
		t.Errorf("expected MaxInt64 gap for free egress, got %d", s.CoalesceGap)
	}
}

func TestStrategyForBlended(t *testing.T) {
	// reqPrice / bytePrice -> gap; maxReadTime * BW -> split.
	rtt := 100 * time.Millisecond
	bw := int64(50 * 1024 * 1024)
	reqPrice, bytePrice := 0.0004, 0.00000009
	s := StrategyForBlended(reqPrice, bytePrice, rtt, bw, 16)
	wantGap := int64(reqPrice / bytePrice)
	wantSplit := int64(rtt.Seconds() * float64(bw))
	if s.CoalesceGap != wantGap {
		t.Errorf("CoalesceGap = %d, want %d", s.CoalesceGap, wantGap)
	}
	if s.SplitThreshold != wantSplit {
		t.Errorf("SplitThreshold = %d, want %d", s.SplitThreshold, wantSplit)
	}
}
