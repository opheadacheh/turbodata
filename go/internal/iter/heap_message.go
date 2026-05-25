package iter

type message struct {
	topicId    uint16
	data       []byte
	groupIndex int
	timestamp  int64
}

// Time ordered message heap.
type MessageHeap []*message

func (h MessageHeap) Len() int { return len(h) }
func (h MessageHeap) Less(i, j int) bool {
	return h[i].timestamp < h[j].timestamp
}
func (h MessageHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *MessageHeap) Push(x any) {
	*h = append(*h, x.(*message))
}

func (h *MessageHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// Reverse time ordered message heap.
type ReverseMessageHeap []*message

func (h ReverseMessageHeap) Len() int { return len(h) }
func (h ReverseMessageHeap) Less(i, j int) bool {
	return h[i].timestamp > h[j].timestamp
}
func (h ReverseMessageHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *ReverseMessageHeap) Push(x any) {
	*h = append(*h, x.(*message))
}

func (h *ReverseMessageHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
