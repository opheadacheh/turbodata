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

package turbodata

import (
	"github.com/opheadacheh/turbodata/go/turbodata/internal/iter"
	"github.com/opheadacheh/turbodata/go/turbodata/readstrategy"
)

// ReadOption configures a read invocation. The underlying parameter type is
// intentionally opaque (an internal iterator); use the WithXxx helpers below
// to construct options.
type ReadOption func(it *iter.MessageIterator) error

// Order controls the timestamp direction in which messages are produced.
type Order = iter.Order

const (
	TimeOrder        = iter.TimeOrder
	ReverseTimeOrder = iter.ReverseTimeOrder
)

// ReadStrategy controls how the cost-aware reader path plans and executes I/O.
// The canonical definition lives in the public readstrategy package; this
// alias keeps the existing turbodata.ReadStrategy spelling working for
// callers that don't want to import readstrategy directly.
// See the helper constructors (StrategyForLatency, StrategyForMoney,
// StrategyForBlended) for the common cases.
type ReadStrategy = readstrategy.ReadStrategy

var (
	StrategyForLatency = readstrategy.StrategyForLatency
	StrategyForMoney   = readstrategy.StrategyForMoney
	StrategyForBlended = readstrategy.StrategyForBlended
)

func WithTopicNames(names []string) ReadOption {
	return func(it *iter.MessageIterator) error {
		it.TopicNames = names
		return nil
	}
}

func WithStartTimestamp(timestamp int64) ReadOption {
	return func(it *iter.MessageIterator) error {
		it.StartTimestamp = timestamp
		return nil
	}
}

func WithEndTimestamp(timestamp int64) ReadOption {
	return func(it *iter.MessageIterator) error {
		it.EndTimestamp = timestamp
		return nil
	}
}

func WithOrder(order Order) ReadOption {
	return func(it *iter.MessageIterator) error {
		it.Order = order
		return nil
	}
}

// WithReadStrategy switches ReadMessages onto the cost-aware reader path with
// the given strategy. Without this option, the reader uses the default
// memory-minimal path (lazy Seek+Read, one chunk at a time).
func WithReadStrategy(s ReadStrategy) ReadOption {
	return func(it *iter.MessageIterator) error {
		it.Strategy = &s
		return nil
	}
}

// WithTailPrefetch hints how many trailing bytes to read speculatively when
// loading the file's summary. When the summary fits within the prefetch window,
// it costs one ReadAt instead of two reads. Benefits both the default and
// cost-aware paths. size <= 0 disables the hint (today's exact-sized reads).
func WithTailPrefetch(size int64) ReadOption {
	return func(it *iter.MessageIterator) error {
		it.TailPrefetch = size
		return nil
	}
}

// WithVideoDecodable makes the iterator return a decoder-ready sequence for
// any video topic in scope: per video group, the effective StartTimestamp is
// snapped backwards to the latest key frame whose timestamp is <=
// StartTimestamp. The caller can then feed the produced messages to a video
// decoder cold and reach the requested time with correct state.
//
// Without this option, video topics behave like any other topic (messages
// start at the literal StartTimestamp, which may land mid-GOP and be
// undecodable on its own). Use the bare iterator when you only want to scan
// the file, not decode it.
//
// Non-video topics are unaffected.
func WithVideoDecodable() ReadOption {
	return func(it *iter.MessageIterator) error {
		it.VideoDecodable = true
		return nil
	}
}
