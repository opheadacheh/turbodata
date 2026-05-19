package turbodata

import "errors"

var (
	ErrTopicAlreadyOpen       = errors.New("topic already open")
	ErrNamesMetadatasMismatch = errors.New("names and metadatas length mismatch")
	ErrNoTopicsToOpen         = errors.New("no topics to open")

	ErrTopicNotOpened     = errors.New("topic not opened, call OpenTopics first")
	ErrTopicNotRegistered = errors.New("topic not registered with OpenTopics")
	ErrTimestampDecreases = errors.New("timestamp cannot decrease")

	ErrTopicAlreadyClosed = errors.New("topic is already closed")
	ErrTopicNotClosed     = errors.New("topic not closed, call CloseTopic first")
)
