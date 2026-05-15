package turbodata

type messageIndexWithTopicId struct {
	topicId      uint16
	messageIndex *MessageIndex
}

type TimeOrderHeap []*messageIndexWithTopicId

func (h TimeOrderHeap) Len() int { return len(h) }
func (h TimeOrderHeap) Less(i, j int) bool {
	return h[i].messageIndex.Timestamp < h[j].messageIndex.Timestamp
}
func (h TimeOrderHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *TimeOrderHeap) Push(x any) {
	*h = append(*h, x.(*messageIndexWithTopicId))
}

func (h *TimeOrderHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type ReverseTimeOrderHeap []*messageIndexWithTopicId

func (h ReverseTimeOrderHeap) Len() int { return len(h) }
func (h ReverseTimeOrderHeap) Less(i, j int) bool {
	return h[i].messageIndex.Timestamp > h[j].messageIndex.Timestamp
}
func (h ReverseTimeOrderHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *ReverseTimeOrderHeap) Push(x any) {
	*h = append(*h, x.(*messageIndexWithTopicId))
}

func (h *ReverseTimeOrderHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
