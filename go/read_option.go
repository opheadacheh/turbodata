package turbodata

type ReadOption func(it *MessageIterator) error

type Order uint8

const (
	TimeOrder Order = iota
	ReverseTimeOrder
)

func WithTopicNames(names []string) ReadOption {
	return func(it *MessageIterator) error {
		it.topicNames = names
		return nil
	}
}

func WithStartTimestamp(timestamp int64) ReadOption {
	return func(it *MessageIterator) error {
		it.startTimestamp = timestamp
		return nil
	}
}

func WithEndTimestamp(timestamp int64) ReadOption {
	return func(it *MessageIterator) error {
		it.endTimestamp = timestamp
		return nil
	}
}

func WithOrder(order Order) ReadOption {
	return func(it *MessageIterator) error {
		it.order = order
		return nil
	}
}

// WithReadStrategy switches ReadMessages onto the cost-aware reader path with
// the given strategy. Without this option, the reader uses the default
// memory-minimal path (lazy Seek+Read, one chunk at a time).
func WithReadStrategy(s ReadStrategy) ReadOption {
	return func(it *MessageIterator) error {
		it.strategy = &s
		return nil
	}
}

// WithTailPrefetch hints how many trailing bytes to read speculatively when
// loading the file's summary. When the summary fits within the prefetch window,
// it costs one ReadAt instead of two reads. Benefits both the default and
// cost-aware paths. size <= 0 disables the hint (today's exact-sized reads).
func WithTailPrefetch(size int64) ReadOption {
	return func(it *MessageIterator) error {
		it.tailPrefetch = size
		return nil
	}
}
