package turbodata

type messageIndexWithTopicId struct {
	topicId      uint16
	messageIndex *MessageIndex
}

// Time ordered message index heap.
type MessageIndexHeap []*messageIndexWithTopicId

func (h MessageIndexHeap) Len() int { return len(h) }
func (h MessageIndexHeap) Less(i, j int) bool {
	return h[i].messageIndex.Timestamp < h[j].messageIndex.Timestamp
}
func (h MessageIndexHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *MessageIndexHeap) Push(x any) {
	*h = append(*h, x.(*messageIndexWithTopicId))
}

func (h *MessageIndexHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// Reverse time ordered message index heap.
type ReverseMessageIndexHeap []*messageIndexWithTopicId

func (h ReverseMessageIndexHeap) Len() int { return len(h) }
func (h ReverseMessageIndexHeap) Less(i, j int) bool {
	return h[i].messageIndex.Timestamp > h[j].messageIndex.Timestamp
}
func (h ReverseMessageIndexHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *ReverseMessageIndexHeap) Push(x any) {
	*h = append(*h, x.(*messageIndexWithTopicId))
}

func (h *ReverseMessageIndexHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
