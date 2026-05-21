package turbodata

import (
	"container/heap"
	"sync"
)

// messageRef is a kept message after sortAndFilter — enough to yield a result
// (timestamp, topicId, byte slice) without holding onto the original
// *MessageIndex pointer.
type messageRef struct {
	Timestamp     int64
	TopicId       uint16
	OffsetInChunk int64
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
// Pure / allocating; suitable for the cost-aware setup path which runs this
// once per chunk at iterator construction. The per-Next() hot path in
// TopicsGroupIterator keeps its in-place, pool-reusing variant unchanged.
func sortAndFilter(
	topicIndexes []*TopicIndex,
	totalLen int64,
	topicIds map[uint16]struct{},
	startTimestamp, endTimestamp int64,
) (msgs []messageRef, lens []int64) {
	total := 0
	for _, ti := range topicIndexes {
		total += len(ti.MessageIndexes)
	}
	if total == 0 {
		return nil, nil
	}

	sorted := make([]messageIndexWithTopicId, 0, total)

	if len(topicIndexes) == 1 {
		ti := topicIndexes[0]
		for _, mi := range ti.MessageIndexes {
			sorted = append(sorted, messageIndexWithTopicId{topicId: ti.Id, messageIndex: mi})
		}
	} else {
		h := &MessageIndexHeap{}
		heap.Init(h)
		idToIndexes := make(map[uint16][]*MessageIndex, len(topicIndexes))
		idToCursor := make(map[uint16]int, len(topicIndexes))
		pool := sync.Pool{New: func() any { return &messageIndexWithTopicId{} }}

		for _, ti := range topicIndexes {
			if len(ti.MessageIndexes) == 0 {
				continue
			}
			idToIndexes[ti.Id] = ti.MessageIndexes
			item := pool.Get().(*messageIndexWithTopicId)
			item.topicId = ti.Id
			item.messageIndex = ti.MessageIndexes[0]
			heap.Push(h, item)
			idToCursor[ti.Id] = 1
		}

		for h.Len() > 0 {
			item, _ := heap.Pop(h).(*messageIndexWithTopicId)
			sorted = append(sorted, messageIndexWithTopicId{topicId: item.topicId, messageIndex: item.messageIndex})
			tid := item.topicId
			cursor := idToCursor[tid]
			item.messageIndex = nil
			pool.Put(item)
			if cursor >= len(idToIndexes[tid]) {
				continue
			}
			next := pool.Get().(*messageIndexWithTopicId)
			next.topicId = tid
			next.messageIndex = idToIndexes[tid][cursor]
			heap.Push(h, next)
			idToCursor[tid]++
		}
	}

	// Compute per-entry lengths from offset deltas.
	allLens := make([]int64, len(sorted))
	for i := 0; i < len(sorted)-1; i++ {
		allLens[i] = sorted[i+1].messageIndex.OffsetInChunk - sorted[i].messageIndex.OffsetInChunk
	}
	allLens[len(allLens)-1] = totalLen - sorted[len(sorted)-1].messageIndex.OffsetInChunk

	// Filter by topic + timestamp range.
	msgs = make([]messageRef, 0, len(sorted))
	lens = make([]int64, 0, len(sorted))
	for i, item := range sorted {
		if _, ok := topicIds[item.topicId]; !ok {
			continue
		}
		ts := item.messageIndex.Timestamp
		if ts < startTimestamp || ts > endTimestamp {
			continue
		}
		msgs = append(msgs, messageRef{
			Timestamp:     ts,
			TopicId:       item.topicId,
			OffsetInChunk: item.messageIndex.OffsetInChunk,
		})
		lens = append(lens, allLens[i])
	}
	return msgs, lens
}
