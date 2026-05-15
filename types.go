package turbodata

type TypeCode uint8

const (
	TypeCodeUnknown TypeCode = iota
	TypeCodeTopicInfo
	TypeCodeGroupTopicsInfo
)

type Footer struct {
	SummaryLen uint64
	Magic      [5]byte
}

type Summary struct {
	TopicsInfos []*TopicsInfo
}

type TopicsInfo struct {
	TopicMetadatas     []*TopicMetadata
	IndexChunkInfoList []*IndexChunkInfo
	TotalLen           uint64
}

type TopicMetadata struct {
	Id       uint16
	Name     string
	Metadata map[string]any
}

type IndexChunkInfo struct {
	StartTimestamp uint64
	EndTimestamp   uint64
	Offset         uint64
}

type IndexChunk struct {
	TopicIndexes    []*TopicIndex
	ChunkOffset     uint64
	ChunkLen        uint64
	UncompressedLen uint64
}

type TopicIndex struct {
	Id              uint16
	MessageIndexes  []*MessageIndex
	KeyFrameIndexes []uint32
}

type MessageIndex struct {
	Timestamp     uint64
	OffsetInChunk uint64
}
