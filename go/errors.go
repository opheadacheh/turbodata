package turbodata

import "errors"

var (
	ErrNilReadSource = errors.New("read source is nil")

	ErrTopicAlreadyOpen       = errors.New("topic already open")
	ErrNamesMetadatasMismatch = errors.New("names and metadatas length mismatch")
	ErrNoTopicsToOpen         = errors.New("no topics to open")

	ErrTopicNotOpened     = errors.New("topic not opened, call OpenTopics first")
	ErrTopicNotRegistered = errors.New("topic not registered with OpenTopics")
	ErrTimestampDecreases = errors.New("timestamp cannot decrease")

	ErrTopicAlreadyClosed = errors.New("topic is already closed")
	ErrTopicNotClosed     = errors.New("topic not closed, call CloseTopic first")

	// Video-topic constraints. See WithVideoTopic / WriteVideoMessage.
	ErrVideoGroupMustBeSingleTopic      = errors.New("a video topic must be opened alone in its group")
	ErrVideoTopicCannotBeCompressed     = errors.New("video topics cannot also be compressed (the codec already compresses the bytes)")
	ErrFirstVideoMessageMustBeKeyFrame  = errors.New("first message of a video topic must be a key frame")
	ErrWriteMessageOnVideoTopic         = errors.New("use WriteVideoMessage for video topics")
	ErrWriteVideoMessageOnNonVideoTopic = errors.New("WriteVideoMessage requires a topic opened with WithVideoTopic")
)
