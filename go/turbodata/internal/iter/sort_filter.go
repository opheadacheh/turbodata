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

package iter

import (
	"container/heap"
	"sync"

	"github.com/opheadacheh/turbodata/go/turbodata/format"
)

// sortAndFilterMergeScratch bundles the reusable working state for
// sortAndFilter. A single scratch can be shared across many calls; it
// owns no per-call data once a call returns.
type sortAndFilterMergeScratch struct {
	sortHeap    *MessageIndexHeap
	idToIndexes map[uint16][]format.MessageIndex
	idToCursor  map[uint16]int

	// Sorted-but-not-yet-filtered working slices. Capacity grows; never shrinks.
	sortedItems []*messageIndexWithTopicId
	sortedLens  []int64

	// Pool of *messageIndexWithTopicId. Reused for both heap-internal items
	// and emitted output items. Items handed to the caller via outMsgs are
	// "out on loan"; they return to the pool either via the next call's
	// prologue (when the caller reuses the same outMsgs slice) or get GC'd.
	itemPool sync.Pool
}

func newSortAndFilterMergeScratch() *sortAndFilterMergeScratch {
	s := &sortAndFilterMergeScratch{
		sortHeap:    &MessageIndexHeap{},
		idToIndexes: make(map[uint16][]format.MessageIndex),
		idToCursor:  make(map[uint16]int),
	}
	heap.Init(s.sortHeap)
	s.itemPool.New = func() any { return &messageIndexWithTopicId{} }
	return s
}

// sortAndFilter computes the kept-message list for a single decoded
// IndexChunk in one pass:
//   - sorts MessageIndexes from all topics in offset order (matches the
//     writer's interleaving, which equals timestamp order),
//   - computes per-message lengths from differences between successive offsets
//     (with totalLen closing out the last one),
//   - drops messages whose topicId isn't in topicIds or whose timestamp falls
//     outside [startTimestamp, endTimestamp].
//
// The caller owns scratch (reused across calls) and the output slices (also
// reusable). At entry, items currently in *outMsgs are returned to the pool;
// pass a fresh empty slice to opt out of that recycling.
//
// When videoDecodable is true, the effective lower bound is snapped back from
// startTimestamp to the timestamp of the latest key frame whose timestamp is
// <= startTimestamp within this chunk (using each topic's KeyFrameIndexes), so
// the caller receives a sequence a decoder can consume cold. See
// keyFrameStart for why this is correct without resurrecting older frames.
//
// This single function serves both the per-Next() hot path in
// TopicsGroupIterator and the per-chunk setup in prepareCostAware.
func sortAndFilter(
	topicIndexes []*format.TopicIndex,
	totalLen int64,
	topicIds map[uint16]struct{},
	startTimestamp, endTimestamp int64,
	videoDecodable bool,
	scratch *sortAndFilterMergeScratch,
	outMsgs *[]*messageIndexWithTopicId,
	outLens *[]int64,
) {
	// Return any previously-emitted items in *outMsgs to the pool; reset.
	for _, item := range *outMsgs {
		item.messageIndex = nil
		scratch.itemPool.Put(item)
	}
	*outMsgs = (*outMsgs)[:0]
	*outLens = (*outLens)[:0]

	totalMessages := 0
	for _, ti := range topicIndexes {
		totalMessages += len(ti.MessageIndexes)
	}
	if totalMessages == 0 {
		return
	}

	// Grow / reset scratch slices.
	if cap(scratch.sortedItems) < totalMessages {
		scratch.sortedItems = make([]*messageIndexWithTopicId, 0, totalMessages)
	} else {
		scratch.sortedItems = scratch.sortedItems[:0]
	}
	if cap(scratch.sortedLens) < totalMessages {
		scratch.sortedLens = make([]int64, totalMessages)
	} else {
		scratch.sortedLens = scratch.sortedLens[:totalMessages]
	}

	if len(topicIndexes) == 1 {
		// Fast path: single topic, messages are already in offset order.
		ti := topicIndexes[0]
		for i := range ti.MessageIndexes {
			item := scratch.itemPool.Get().(*messageIndexWithTopicId)
			item.topicId = ti.Id
			item.messageIndex = &ti.MessageIndexes[i]
			scratch.sortedItems = append(scratch.sortedItems, item)
		}
	} else {
		// K-way merge by offsetInChunk via min-heap.
		for k := range scratch.idToIndexes {
			delete(scratch.idToIndexes, k)
		}
		for k := range scratch.idToCursor {
			delete(scratch.idToCursor, k)
		}
		for _, ti := range topicIndexes {
			if len(ti.MessageIndexes) == 0 {
				continue
			}
			scratch.idToIndexes[ti.Id] = ti.MessageIndexes
			item := scratch.itemPool.Get().(*messageIndexWithTopicId)
			item.topicId = ti.Id
			item.messageIndex = &ti.MessageIndexes[0]
			heap.Push(scratch.sortHeap, item)
			scratch.idToCursor[ti.Id] = 1
		}

		for scratch.sortHeap.Len() > 0 {
			item, _ := heap.Pop(scratch.sortHeap).(*messageIndexWithTopicId)
			scratch.sortedItems = append(scratch.sortedItems, item)
			tid := item.topicId
			cursor := scratch.idToCursor[tid]
			if cursor >= len(scratch.idToIndexes[tid]) {
				continue
			}
			next := scratch.itemPool.Get().(*messageIndexWithTopicId)
			next.topicId = tid
			next.messageIndex = &scratch.idToIndexes[tid][cursor]
			heap.Push(scratch.sortHeap, next)
			scratch.idToCursor[tid]++
		}
	}

	// Compute per-entry lengths from offset deltas.
	for i := 0; i < len(scratch.sortedItems)-1; i++ {
		scratch.sortedLens[i] = scratch.sortedItems[i+1].messageIndex.OffsetInChunk - scratch.sortedItems[i].messageIndex.OffsetInChunk
	}
	scratch.sortedLens[len(scratch.sortedItems)-1] = totalLen - scratch.sortedItems[len(scratch.sortedItems)-1].messageIndex.OffsetInChunk

	// For video-decodable groups, snap the lower bound back to the anchoring
	// key frame so the kept sequence is decodable cold.
	effectiveStart := startTimestamp
	if videoDecodable {
		effectiveStart = keyFrameStart(topicIndexes, startTimestamp)
	}

	// Filter by topic + timestamp range. Dropped items go back to the pool;
	// kept items are appended to *outMsgs / *outLens.
	for i, item := range scratch.sortedItems {
		if _, ok := topicIds[item.topicId]; !ok {
			item.messageIndex = nil
			scratch.itemPool.Put(item)
			continue
		}
		ts := item.messageIndex.Timestamp
		if ts < effectiveStart || ts > endTimestamp {
			item.messageIndex = nil
			scratch.itemPool.Put(item)
			continue
		}
		*outMsgs = append(*outMsgs, item)
		*outLens = append(*outLens, scratch.sortedLens[i])
	}
}

// keyFrameStart returns the timestamp of the latest key frame whose timestamp
// is <= startTimestamp, or startTimestamp unchanged if no such key frame
// exists in this chunk.
//
// Video-decodable groups always hold exactly one topic, so a single topic
// index suffices. KeyFrameIndexes are ascending positions into that topic's
// timestamp-ordered MessageIndexes. The caller relies on chunk-level filtering
// having already dropped chunks that end before startTimestamp, so the only
// chunk holding a key frame <= startTimestamp is the one containing
// startTimestamp; that key frame is the GOP anchor needed to decode the first
// in-range frame, and it never resurrects frames from earlier chunks.
func keyFrameStart(topicIndexes []*format.TopicIndex, startTimestamp int64) int64 {
	if len(topicIndexes) == 0 {
		return startTimestamp
	}
	ti := topicIndexes[0]
	anchor := startTimestamp
	for _, kfIdx := range ti.KeyFrameIndexes {
		ts := ti.MessageIndexes[kfIdx].Timestamp
		if ts > startTimestamp {
			break
		}
		anchor = ts
	}
	return anchor
}
