// Copyright 2026 Wanjia He
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package format

// Common per-topic schema metadata keys. SchemaData holds raw schema bytes.
const (
	MetaKeySchemaName     = "schema_name"
	MetaKeySchemaEncoding = "schema_encoding"
	MetaKeySchemaData     = "schema_data"
)

// Internal per-topic metadata keys used to carry write-format control flags
// alongside caller-supplied metadata. The __td_ prefix namespaces them away
// from user keys. MetaKeyCompressed and MetaKeyVideo are persisted into the
// summary (readers depend on them); MetaKeyChunkConfig is write-time-only and
// is stripped before serialization.
const (
	MetaKeyCompressed  = "__td_is_compressed"
	MetaKeyVideo       = "__td_is_video"
	MetaKeyChunkConfig = "__td_chunk_config"
)

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

// IndexChunkLen returns the on-disk byte length of the i-th index chunk: the
// distance to the next chunk's offset, or for the last chunk the distance that
// wraps around via TotalLen to the first chunk's offset.
func (ti *TopicsInfo) IndexChunkLen(i int) int64 {
	info := ti.IndexChunkInfoList[i]
	if i < len(ti.IndexChunkInfoList)-1 {
		return ti.IndexChunkInfoList[i+1].Offset - info.Offset
	}
	return ti.TotalLen - info.Offset + ti.IndexChunkInfoList[0].Offset
}

type TopicMetadata struct {
	Id       uint16
	Name     string
	Metadata map[string]any
	// MessageCount is the number of messages written into this topic.
	MessageCount uint32
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
	MessageIndexes  []MessageIndex
	KeyFrameIndexes []uint32
}

type MessageIndex struct {
	Timestamp     int64
	OffsetInChunk int64
}
