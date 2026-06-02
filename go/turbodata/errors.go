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

	// ErrVideoSourcesOverlap is returned by MultiReader.Sample/ReadMessages under
	// WithSampleVideoDecodable when a queried video topic is provided by more
	// than one reader whose time ranges overlap. Decodable multi-file video
	// sampling requires each video topic's source files to be time-disjoint.
	ErrVideoSourcesOverlap = errors.New("video topic source files overlap in time")

	// Video-topic constraints. See WithVideoTopic / WriteVideoMessage.
	ErrVideoGroupMustBeSingleTopic      = errors.New("a video topic must be opened alone in its group")
	ErrVideoTopicCannotBeCompressed     = errors.New("video topics cannot also be compressed (the codec already compresses the bytes)")
	ErrFirstVideoMessageMustBeKeyFrame  = errors.New("first message of a video topic must be a key frame")
	ErrWriteMessageOnVideoTopic         = errors.New("use WriteVideoMessage for video topics")
	ErrWriteVideoMessageOnNonVideoTopic = errors.New("WriteVideoMessage requires a topic opened with WithVideoTopic")
)
