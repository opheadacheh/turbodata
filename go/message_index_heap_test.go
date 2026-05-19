package turbodata

import (
	"container/heap"
	"testing"
)

func TestMessageIndexHeap(t *testing.T) {
	h := &MessageIndexHeap{}
	heap.Init(h)
	heap.Push(h, &messageIndexWithTopicId{topicId: 1, messageIndex: &MessageIndex{Timestamp: 3}})
	heap.Push(h, &messageIndexWithTopicId{topicId: 1, messageIndex: &MessageIndex{Timestamp: 2}})
	heap.Push(h, &messageIndexWithTopicId{topicId: 1, messageIndex: &MessageIndex{Timestamp: 1}})

	if h.Len() != 3 {
		t.Errorf("heap length should be 3, but got %d", h.Len())
	}

	len := h.Len()
	for i := 0; i < len; i++ {
		timestamp := heap.Pop(h).(*messageIndexWithTopicId).messageIndex.Timestamp
		if timestamp != int64(i+1) {
			t.Errorf("heap pop should return %d, but got %d", i+1, timestamp)
		}
	}
}
