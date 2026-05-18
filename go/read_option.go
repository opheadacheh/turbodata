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
