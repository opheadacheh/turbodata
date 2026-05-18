package turbodata

import (
	"bytes"
	"testing"
)

var (
	testTopicMetadata1 = &TopicMetadata{
		Id:       1,
		Name:     "test",
		Metadata: map[string]any{"foo": "bar"},
	}
	testTopicMetadata2 = &TopicMetadata{
		Id:       2,
		Name:     "test2",
		Metadata: map[string]any{"foo": "bar"},
	}

	testIndexChunkInfo1 = &IndexChunkInfo{
		StartTimestamp: 1,
		EndTimestamp:   2,
		Offset:         3,
	}
	testIndexChunkInfo2 = &IndexChunkInfo{
		StartTimestamp: 4,
		EndTimestamp:   5,
		Offset:         6,
	}

	testTopicsInfo1 = &TopicsInfo{
		TopicMetadatas:     []*TopicMetadata{testTopicMetadata1, testTopicMetadata2},
		IndexChunkInfoList: []*IndexChunkInfo{testIndexChunkInfo1, testIndexChunkInfo2},
		TotalLen:           100,
	}
	testTopicsInfo2 = &TopicsInfo{
		TopicMetadatas:     []*TopicMetadata{testTopicMetadata1, testTopicMetadata2},
		IndexChunkInfoList: []*IndexChunkInfo{testIndexChunkInfo1, testIndexChunkInfo2},
		TotalLen:           100,
	}

	testSummary = &Summary{
		TopicsInfos: []*TopicsInfo{testTopicsInfo1, testTopicsInfo2},
	}

	testMessageIndex1 = &MessageIndex{
		Timestamp:     1,
		OffsetInChunk: 2,
	}
	testMessageIndex2 = &MessageIndex{
		Timestamp:     3,
		OffsetInChunk: 4,
	}

	testTopicIndex1 = &TopicIndex{
		Id:              1,
		MessageIndexes:  []*MessageIndex{testMessageIndex1, testMessageIndex2},
		KeyFrameIndexes: []uint32{1, 2},
	}
	testTopicIndex2 = &TopicIndex{
		Id:              2,
		MessageIndexes:  []*MessageIndex{testMessageIndex1, testMessageIndex2},
		KeyFrameIndexes: []uint32{1, 2},
	}

	testIndexChunk = &IndexChunk{
		TopicIndexes:    []*TopicIndex{testTopicIndex1, testTopicIndex2},
		ChunkOffset:     1,
		ChunkLen:        2,
		UncompressedLen: 3,
	}
)

func TestWriteReadString(t *testing.T) {
	str := "hello"
	buf := bytes.NewBuffer(nil)
	err := writeString(buf, str)
	if err != nil {
		t.Fatalf("failed to write string: %v", err)
	}

	if buf.Len() != 9 {
		t.Fatalf("string length mismatch: expected %d, got %d", 9, buf.Len())
	}

	readStr, err := readString(buf)
	if err != nil {
		t.Fatalf("failed to read string: %v", err)
	}

	if readStr != str {
		t.Fatalf("string mismatch: expected %s, got %s", str, readStr)
	}
}

func TestWriteReadMap(t *testing.T) {
	mp := map[string]any{
		"hello": "world",
		"foo":   123,
	}
	buf := bytes.NewBuffer(nil)
	err := writeMap(buf, mp)
	if err != nil {
		t.Fatalf("failed to write map: %v", err)
	}

	if buf.Len() != 22 {
		t.Fatalf("map length mismatch: expected %d, got %d", 22, buf.Len())
	}

	readMap, err := readMap(buf)
	if err != nil {
		t.Fatalf("failed to read map: %v", err)
	}

	if readMap["hello"].(string) != "world" {
		t.Fatalf("map mismatch: expected %s, got %s", "world", readMap["hello"])
	}

	if readMap["foo"].(int8) != 123 {
		t.Fatalf("map mismatch: expected %d, got %d", 123, readMap["foo"])
	}
}

func TestWriteReadFooter(t *testing.T) {
	footer := &Footer{
		SummaryLen: 100,
		Magic:      [5]byte{'7', 'U', 'R', 'B', '0'},
	}

	buf := bytes.NewBuffer(nil)
	err := WriteFooter(buf, footer)
	if err != nil {
		t.Fatalf("failed to write footer: %v", err)
	}

	if buf.Len() != 13 {
		t.Fatalf("footer length mismatch: expected %d, got %d", 13, buf.Len())
	}

	readFooter, err := ReadFooter(buf)
	if err != nil {
		t.Fatalf("failed to read footer: %v", err)
	}

	if readFooter.SummaryLen != footer.SummaryLen {
		t.Fatalf("summary len mismatch: expected %d, got %d", footer.SummaryLen, readFooter.SummaryLen)
	}

	if readFooter.Magic != footer.Magic {
		t.Fatalf("Magic mismatch: expected %v, got %v", footer.Magic, readFooter.Magic)
	}
}

func TestWriteReadTopicMetadata(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	err := WriteTopicMetadata(buf, testTopicMetadata1)
	if err != nil {
		t.Fatalf("failed to write topic Metadata: %v", err)
	}

	readTopicMetadata, err := ReadTopicMetadata(buf)
	if err != nil {
		t.Fatalf("failed to read topic Metadata: %v", err)
	}

	if readTopicMetadata.Id != testTopicMetadata1.Id {
		t.Fatalf("Id mismatch: expected %d, got %d", testTopicMetadata1.Id, readTopicMetadata.Id)
	}

	if readTopicMetadata.Name != testTopicMetadata1.Name {
		t.Fatalf("Name mismatch: expected %s, got %s", testTopicMetadata1.Name, readTopicMetadata.Name)
	}

	if readTopicMetadata.Metadata["foo"].(string) != "bar" {
		t.Fatalf("Metadata mismatch: expected %s, got %s", "bar", readTopicMetadata.Metadata["foo"])
	}
}

func TestWriteReadIndexChunkInfo(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	err := WriteIndexChunkInfo(buf, testIndexChunkInfo1)
	if err != nil {
		t.Fatalf("failed to write index chunk info: %v", err)
	}

	readIndexChunkInfo, err := ReadIndexChunkInfo(buf)
	if err != nil {
		t.Fatalf("failed to read index chunk info: %v", err)
	}

	if readIndexChunkInfo.StartTimestamp != testIndexChunkInfo1.StartTimestamp {
		t.Fatalf("start timestamp mismatch: expected %d, got %d", testIndexChunkInfo1.StartTimestamp, readIndexChunkInfo.StartTimestamp)
	}

	if readIndexChunkInfo.EndTimestamp != testIndexChunkInfo1.EndTimestamp {
		t.Fatalf("end timestamp mismatch: expected %d, got %d", testIndexChunkInfo1.EndTimestamp, readIndexChunkInfo.EndTimestamp)
	}

	if readIndexChunkInfo.Offset != testIndexChunkInfo1.Offset {
		t.Fatalf("offset mismatch: expected %d, got %d", testIndexChunkInfo1.Offset, readIndexChunkInfo.Offset)
	}
}

func TestWriteReadTopicsInfo(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	err := WriteTopicsInfo(buf, testTopicsInfo1)
	if err != nil {
		t.Fatalf("failed to write topics info: %v", err)
	}

	readTopicsInfo, err := ReadTopicsInfo(buf)
	if err != nil {
		t.Fatalf("failed to read topics info: %v", err)
	}

	if len(readTopicsInfo.TopicMetadatas) != len(testTopicsInfo1.TopicMetadatas) {
		t.Fatalf("topic metadatas length mismatch: expected %d, got %d", len(testTopicsInfo1.TopicMetadatas), len(readTopicsInfo.TopicMetadatas))
	}

	for i := range readTopicsInfo.TopicMetadatas {
		if readTopicsInfo.TopicMetadatas[i].Id != testTopicsInfo1.TopicMetadatas[i].Id {
			t.Fatalf("topic metadata id mismatch: expected %d, got %d", testTopicsInfo1.TopicMetadatas[i].Id, readTopicsInfo.TopicMetadatas[i].Id)
		}

		if readTopicsInfo.TopicMetadatas[i].Name != testTopicsInfo1.TopicMetadatas[i].Name {
			t.Fatalf("topic metadata name mismatch: expected %s, got %s", testTopicsInfo1.TopicMetadatas[i].Name, readTopicsInfo.TopicMetadatas[i].Name)
		}

		if readTopicsInfo.TopicMetadatas[i].Metadata["foo"].(string) != "bar" {
			t.Fatalf("topic metadata metadata mismatch: expected %s, got %s", "bar", readTopicsInfo.TopicMetadatas[i].Metadata["foo"])
		}
	}

	if len(readTopicsInfo.IndexChunkInfoList) != len(testTopicsInfo1.IndexChunkInfoList) {
		t.Fatalf("index chunk info list length mismatch: expected %d, got %d", len(testTopicsInfo1.IndexChunkInfoList), len(readTopicsInfo.IndexChunkInfoList))
	}

	for i := range readTopicsInfo.IndexChunkInfoList {
		if readTopicsInfo.IndexChunkInfoList[i].StartTimestamp != testTopicsInfo1.IndexChunkInfoList[i].StartTimestamp {
			t.Fatalf("index chunk info start timestamp mismatch: expected %d, got %d", testTopicsInfo1.IndexChunkInfoList[i].StartTimestamp, readTopicsInfo.IndexChunkInfoList[i].StartTimestamp)
		}

		if readTopicsInfo.IndexChunkInfoList[i].EndTimestamp != testTopicsInfo1.IndexChunkInfoList[i].EndTimestamp {
			t.Fatalf("index chunk info end timestamp mismatch: expected %d, got %d", testTopicsInfo1.IndexChunkInfoList[i].EndTimestamp, readTopicsInfo.IndexChunkInfoList[i].EndTimestamp)
		}

		if readTopicsInfo.IndexChunkInfoList[i].Offset != testTopicsInfo1.IndexChunkInfoList[i].Offset {
			t.Fatalf("index chunk info offset mismatch: expected %d, got %d", testTopicsInfo1.IndexChunkInfoList[i].Offset, readTopicsInfo.IndexChunkInfoList[i].Offset)
		}
	}

	if readTopicsInfo.TotalLen != testTopicsInfo1.TotalLen {
		t.Fatalf("total len mismatch: expected %d, got %d", testTopicsInfo1.TotalLen, readTopicsInfo.TotalLen)
	}
}

func TestWriteReadSummary(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	err := WriteSummary(buf, testSummary)
	if err != nil {
		t.Fatalf("failed to write summary: %v", err)
	}

	readSummary, err := ReadSummary(buf)
	if err != nil {
		t.Fatalf("failed to read summary: %v", err)
	}

	if len(readSummary.TopicsInfos) != len(testSummary.TopicsInfos) {
		t.Fatalf("topics infos length mismatch: expected %d, got %d", len(testSummary.TopicsInfos), len(readSummary.TopicsInfos))
	}

	for i := range readSummary.TopicsInfos {
		for j := range readSummary.TopicsInfos[i].TopicMetadatas {
			if readSummary.TopicsInfos[i].TopicMetadatas[j].Id != testSummary.TopicsInfos[i].TopicMetadatas[j].Id {
				t.Fatalf("topic metadata id mismatch: expected %d, got %d", testSummary.TopicsInfos[i].TopicMetadatas[j].Id, readSummary.TopicsInfos[i].TopicMetadatas[j].Id)
			}

			if readSummary.TopicsInfos[i].TopicMetadatas[j].Name != testSummary.TopicsInfos[i].TopicMetadatas[j].Name {
				t.Fatalf("topic metadata name mismatch: expected %s, got %s", testSummary.TopicsInfos[i].TopicMetadatas[j].Name, readSummary.TopicsInfos[i].TopicMetadatas[j].Name)
			}

			if readSummary.TopicsInfos[i].TopicMetadatas[j].Metadata["foo"].(string) != "bar" {
				t.Fatalf("topic metadata metadata mismatch: expected %s, got %s", "bar", readSummary.TopicsInfos[i].TopicMetadatas[j].Metadata["foo"])
			}
		}

		if len(readSummary.TopicsInfos[i].IndexChunkInfoList) != len(testSummary.TopicsInfos[i].IndexChunkInfoList) {
			t.Fatalf("index chunk info list length mismatch: expected %d, got %d", len(testSummary.TopicsInfos[i].IndexChunkInfoList), len(readSummary.TopicsInfos[i].IndexChunkInfoList))
		}

		for j := range readSummary.TopicsInfos[i].IndexChunkInfoList {
			if readSummary.TopicsInfos[i].IndexChunkInfoList[j].StartTimestamp != testSummary.TopicsInfos[i].IndexChunkInfoList[j].StartTimestamp {
				t.Fatalf("index chunk info start timestamp mismatch: expected %d, got %d", testSummary.TopicsInfos[i].IndexChunkInfoList[j].StartTimestamp, readSummary.TopicsInfos[i].IndexChunkInfoList[j].StartTimestamp)
			}

			if readSummary.TopicsInfos[i].IndexChunkInfoList[j].EndTimestamp != testSummary.TopicsInfos[i].IndexChunkInfoList[j].EndTimestamp {
				t.Fatalf("index chunk info end timestamp mismatch: expected %d, got %d", testSummary.TopicsInfos[i].IndexChunkInfoList[j].EndTimestamp, readSummary.TopicsInfos[i].IndexChunkInfoList[j].EndTimestamp)
			}

			if readSummary.TopicsInfos[i].IndexChunkInfoList[j].Offset != testSummary.TopicsInfos[i].IndexChunkInfoList[j].Offset {
				t.Fatalf("index chunk info offset mismatch: expected %d, got %d", testSummary.TopicsInfos[i].IndexChunkInfoList[j].Offset, readSummary.TopicsInfos[i].IndexChunkInfoList[j].Offset)
			}
		}

		if readSummary.TopicsInfos[i].TotalLen != testSummary.TopicsInfos[i].TotalLen {
			t.Fatalf("total len mismatch: expected %d, got %d", testSummary.TopicsInfos[i].TotalLen, readSummary.TopicsInfos[i].TotalLen)
		}
	}
}

func TestWriteReadMessageIndex(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	err := WriteMessageIndex(buf, testMessageIndex1)
	if err != nil {
		t.Fatalf("failed to write message index: %v", err)
	}

	if buf.Len() != 16 {
		t.Fatalf("message index length mismatch: expected %d, got %d", 16, buf.Len())
	}

	readMessageIndex, err := ReadMessageIndex(buf)
	if err != nil {
		t.Fatalf("failed to read message index: %v", err)
	}

	if readMessageIndex.Timestamp != testMessageIndex1.Timestamp {
		t.Fatalf("timestamp mismatch: expected %d, got %d", testMessageIndex1.Timestamp, readMessageIndex.Timestamp)
	}

	if readMessageIndex.OffsetInChunk != testMessageIndex1.OffsetInChunk {
		t.Fatalf("offset in chunk mismatch: expected %d, got %d", testMessageIndex1.OffsetInChunk, readMessageIndex.OffsetInChunk)
	}
}

func TestWriteReadTopicIndex(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	err := WriteTopicIndex(buf, testTopicIndex1)
	if err != nil {
		t.Fatalf("failed to write topic index: %v", err)
	}

	readTopicIndex, err := ReadTopicIndex(buf)
	if err != nil {
		t.Fatalf("failed to read topic index: %v", err)
	}

	if readTopicIndex.Id != testTopicIndex1.Id {
		t.Fatalf("id mismatch: expected %d, got %d", testTopicIndex1.Id, readTopicIndex.Id)
	}

	if len(readTopicIndex.MessageIndexes) != len(testTopicIndex1.MessageIndexes) {
		t.Fatalf("message indexes length mismatch: expected %d, got %d", len(testTopicIndex1.MessageIndexes), len(readTopicIndex.MessageIndexes))
	}

	for i := range readTopicIndex.MessageIndexes {
		if readTopicIndex.MessageIndexes[i].Timestamp != testTopicIndex1.MessageIndexes[i].Timestamp {
			t.Fatalf("message index timestamp mismatch: expected %d, got %d", testTopicIndex1.MessageIndexes[i].Timestamp, readTopicIndex.MessageIndexes[i].Timestamp)
		}

		if readTopicIndex.MessageIndexes[i].OffsetInChunk != testTopicIndex1.MessageIndexes[i].OffsetInChunk {
			t.Fatalf("message index offset in chunk mismatch: expected %d, got %d", testTopicIndex1.MessageIndexes[i].OffsetInChunk, readTopicIndex.MessageIndexes[i].OffsetInChunk)
		}
	}

	if len(readTopicIndex.KeyFrameIndexes) != len(testTopicIndex1.KeyFrameIndexes) {
		t.Fatalf("key frame indexes length mismatch: expected %d, got %d", len(testTopicIndex1.KeyFrameIndexes), len(readTopicIndex.KeyFrameIndexes))
	}

	for i := range readTopicIndex.KeyFrameIndexes {
		if readTopicIndex.KeyFrameIndexes[i] != testTopicIndex1.KeyFrameIndexes[i] {
			t.Fatalf("key frame index mismatch: expected %d, got %d", testTopicIndex1.KeyFrameIndexes[i], readTopicIndex.KeyFrameIndexes[i])
		}
	}
}

func TestWriteReadIndexChunk(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	err := WriteIndexChunk(buf, testIndexChunk)
	if err != nil {
		t.Fatalf("failed to write index chunk: %v", err)
	}

	readIndexChunk, err := ReadIndexChunk(buf)
	if err != nil {
		t.Fatalf("failed to read index chunk: %v", err)
	}

	if len(readIndexChunk.TopicIndexes) != len(testIndexChunk.TopicIndexes) {
		t.Fatalf("topic indexes length mismatch: expected %d, got %d", len(testIndexChunk.TopicIndexes), len(readIndexChunk.TopicIndexes))
	}

	for i := range readIndexChunk.TopicIndexes {
		if readIndexChunk.TopicIndexes[i].Id != testIndexChunk.TopicIndexes[i].Id {
			t.Fatalf("topic index id mismatch: expected %d, got %d", testIndexChunk.TopicIndexes[i].Id, readIndexChunk.TopicIndexes[i].Id)
		}
	}

	if readIndexChunk.ChunkOffset != testIndexChunk.ChunkOffset {
		t.Fatalf("chunk offset mismatch: expected %d, got %d", testIndexChunk.ChunkOffset, readIndexChunk.ChunkOffset)
	}

	if readIndexChunk.ChunkLen != testIndexChunk.ChunkLen {
		t.Fatalf("chunk len mismatch: expected %d, got %d", testIndexChunk.ChunkLen, readIndexChunk.ChunkLen)
	}

	if readIndexChunk.UncompressedLen != testIndexChunk.UncompressedLen {
		t.Fatalf("uncompressed len mismatch: expected %d, got %d", testIndexChunk.UncompressedLen, readIndexChunk.UncompressedLen)
	}
}
