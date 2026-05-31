package iter

import (
	"container/heap"
	"errors"
	"io"

	"turbodata/internal/buffer"
)

// multiEntry is one in-flight head from a sub-iterator. The message bytes are
// not carried here; they live in the owning sub's buffer until that sub is
// advanced again. Only the ordering key, the exposed name, and the sub index
// travel through the heap.
type multiEntry struct {
	timestamp int64
	name      string
	subIndex  int
}

// multiHeap orders entries by timestamp. When reverse is true the ordering is
// flipped (latest timestamp first), matching ReverseTimeOrder sub-iterators.
type multiHeap struct {
	entries []multiEntry
	reverse bool
}

func (h multiHeap) Len() int { return len(h.entries) }
func (h multiHeap) Less(i, j int) bool {
	if h.reverse {
		return h.entries[i].timestamp > h.entries[j].timestamp
	}
	return h.entries[i].timestamp < h.entries[j].timestamp
}
func (h multiHeap) Swap(i, j int) { h.entries[i], h.entries[j] = h.entries[j], h.entries[i] }
func (h *multiHeap) Push(x any)   { h.entries = append(h.entries, x.(multiEntry)) }
func (h *multiHeap) Pop() any {
	old := h.entries
	n := len(old)
	x := old[n-1]
	h.entries = old[:n-1]
	return x
}

// MultiMessageIterator merges already-time-ordered sub-iterators into one
// globally time-ordered stream via a k-way heap merge. Each sub owns one
// buffer holding its current head message; the heap carries only
// (timestamp, name, subIndex), so there is exactly one in-flight message per
// sub and no per-message copy beyond what each sub's NextInto already does.
type MultiMessageIterator struct {
	subs    []*MessageIterator
	bufs    []*buffer.ReusableBuffer // one per sub, holds that sub's current head
	h       *multiHeap
	reverse bool
	loaded  bool
}

// NewMultiMessageIterator wraps subs into a single merged iterator. reverse
// must match the Order the subs were prepared with so the merge direction
// agrees with each sub's own ordering.
func NewMultiMessageIterator(subs []*MessageIterator, reverse bool) *MultiMessageIterator {
	bufs := make([]*buffer.ReusableBuffer, len(subs))
	for i := range bufs {
		bufs[i] = buffer.NewReusableBuffer()
	}
	return &MultiMessageIterator{
		subs:    subs,
		bufs:    bufs,
		h:       &multiHeap{reverse: reverse},
		reverse: reverse,
	}
}

// pull advances sub i into its own buffer and, unless the sub is exhausted,
// pushes the resulting head entry onto the heap.
func (m *MultiMessageIterator) pull(i int) error {
	ts, name, err := m.subs[i].NextInto(m.bufs[i])
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err != nil {
		return err
	}
	heap.Push(m.h, multiEntry{timestamp: ts, name: name, subIndex: i})
	return nil
}

func (m *MultiMessageIterator) initLoad() error {
	heap.Init(m.h)
	for i := range m.subs {
		if err := m.pull(i); err != nil {
			return err
		}
	}
	return nil
}

// NextInto copies the next message (in merged time order) into buf and returns
// its timestamp and exposed topic name. Returns io.EOF when every sub is
// drained.
func (m *MultiMessageIterator) NextInto(buf *buffer.ReusableBuffer) (int64, string, error) {
	if !m.loaded {
		if err := m.initLoad(); err != nil {
			return 0, "", err
		}
		m.loaded = true
	}

	if m.h.Len() == 0 {
		return 0, "", io.EOF
	}

	// The popped entry's bytes still live in its sub's buffer; copy them out
	// before advancing that sub (which overwrites the buffer).
	top := heap.Pop(m.h).(multiEntry)
	src := m.bufs[top.subIndex].Data
	buf.Prepare(len(src))
	copy(buf.Data, src)

	if err := m.pull(top.subIndex); err != nil {
		return 0, "", err
	}
	return top.timestamp, top.name, nil
}
