package turbodata

type messageIndexWithTopicId struct {
	topicId      uint16
	messageIndex *MessageIndex
}

// Time ordered message index heap.
type MessageIndexHeap []*messageIndexWithTopicId

func (h MessageIndexHeap) Len() int { return len(h) }
func (h MessageIndexHeap) Less(i, j int) bool {
	return h[i].messageIndex.OffsetInChunk < h[j].messageIndex.OffsetInChunk
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
