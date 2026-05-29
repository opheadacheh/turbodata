package format

type Footer struct {
	SummaryLen int64
	Magic      [5]byte
}

// FooterLen is the on-disk size of a Footer (int64 + [5]byte).
const FooterLen = 13

// Magic is the file's trailing magic identifier.
var Magic = [5]byte{'7', 'U', 'R', 'B', '0'}

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
