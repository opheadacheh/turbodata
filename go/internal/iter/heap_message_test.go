package iter

import (
	"container/heap"
	"testing"
)

func TestMessageHeap(t *testing.T) {
	h := &MessageHeap{}
	heap.Init(h)
	heap.Push(h, &message{timestamp: 3})
	heap.Push(h, &message{timestamp: 1})
	heap.Push(h, &message{timestamp: 2})

	if h.Len() != 3 {
		t.Errorf("heap length should be 3, but got %d", h.Len())
	}

	len := h.Len()
	for i := 0; i < len; i++ {
		timestamp := heap.Pop(h).(*message).timestamp
		if timestamp != int64(i+1) {
			t.Errorf("heap pop should return %d, but got %d", i+1, timestamp)
		}
	}
}

func TestReverseMessageHeap(t *testing.T) {
	h := &ReverseMessageHeap{}
	heap.Init(h)
	heap.Push(h, &message{timestamp: 2})
	heap.Push(h, &message{timestamp: 1})
	heap.Push(h, &message{timestamp: 3})

	if h.Len() != 3 {
		t.Errorf("heap length should be 3, but got %d", h.Len())
	}

	len := h.Len()
	for i := 0; i < len; i++ {
		timestamp := heap.Pop(h).(*message).timestamp
		if timestamp != int64(len-i) {
			t.Errorf("heap pop should return %d, but got %d", len-i, timestamp)
		}
	}
}
