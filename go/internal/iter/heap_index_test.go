package iter

import (
	"container/heap"
	"testing"

	"turbodata/format"
)

func TestMessageIndexHeap(t *testing.T) {
	h := &MessageIndexHeap{}
	heap.Init(h)
	heap.Push(h, &messageIndexWithTopicId{topicId: 1, messageIndex: &format.MessageIndex{Timestamp: 1, OffsetInChunk: 3}})
	heap.Push(h, &messageIndexWithTopicId{topicId: 1, messageIndex: &format.MessageIndex{Timestamp: 2, OffsetInChunk: 2}})
	heap.Push(h, &messageIndexWithTopicId{topicId: 1, messageIndex: &format.MessageIndex{Timestamp: 3, OffsetInChunk: 1}})

	if h.Len() != 3 {
		t.Errorf("heap length should be 3, but got %d", h.Len())
	}

	len := h.Len()
	for i := 0; i < len; i++ {
		offsetInChunk := heap.Pop(h).(*messageIndexWithTopicId).messageIndex.OffsetInChunk
		if offsetInChunk != int64(i+1) {
			t.Errorf("heap pop should return %d, but got %d", i+1, offsetInChunk)
		}
	}
}
