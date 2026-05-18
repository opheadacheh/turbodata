package turbodata

type TypeCode uint8

const (
	TypeCodeUnknown TypeCode = iota
	TypeCodeTopicInfo
	TypeCodeGroupTopicsInfo
)

type Footer struct {
	SummaryLen int64
	Magic      [5]byte
}

type Summary struct {
	TopicsInfos []*TopicsInfo
}

type TopicsInfo struct {
	TopicMetadatas     []*TopicMetadata
	IndexChunkInfoList []*IndexChunkInfo
	TotalLen           int64
}

type TopicMetadata struct {
	Id       uint16
	Name     string
	Metadata map[string]any
}

type IndexChunkInfo struct {
	StartTimestamp int64
	EndTimestamp   int64
	Offset         int64
}

type IndexChunk struct {
	TopicIndexes    []*TopicIndex
	ChunkOffset     int64
	ChunkLen        int64
	UncompressedLen int64
}

type TopicIndex struct {
	Id              uint16
	MessageIndexes  []*MessageIndex
	KeyFrameIndexes []uint32
}

type MessageIndex struct {
	Timestamp     int64
	OffsetInChunk int64
}
