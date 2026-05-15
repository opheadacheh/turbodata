package turbodata

import (
	"container/heap"
	"testing"
)

func TestTimeOrderHeap(t *testing.T) {
	h := &TimeOrderHeap{}
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
		if timestamp != uint64(i+1) {
			t.Errorf("heap pop should return %d, but got %d", i+1, timestamp)
		}
	}
}

func TestReverseTimeOrderHeap(t *testing.T) {
	h := &ReverseTimeOrderHeap{}
	heap.Init(h)
	heap.Push(h, &messageIndexWithTopicId{topicId: 1, messageIndex: &MessageIndex{Timestamp: 1}})
	heap.Push(h, &messageIndexWithTopicId{topicId: 1, messageIndex: &MessageIndex{Timestamp: 2}})
	heap.Push(h, &messageIndexWithTopicId{topicId: 1, messageIndex: &MessageIndex{Timestamp: 3}})

	if h.Len() != 3 {
		t.Errorf("heap length should be 3, but got %d", h.Len())
	}

	len := h.Len()
	for i := 0; i < len; i++ {
		timestamp := heap.Pop(h).(*messageIndexWithTopicId).messageIndex.Timestamp
		if timestamp != uint64(len-i) {
			t.Errorf("heap pop should return %d, but got %d", len-i, timestamp)
		}
	}
}
