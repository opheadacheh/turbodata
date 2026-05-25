package format

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/vmihailenco/msgpack/v5"
)

const maxStringLen = 64 * 1024 * 1024 // 64 MiB

func readString(r io.Reader) (string, error) {
	var strLen uint32
	if err := binary.Read(r, binary.BigEndian, &strLen); err != nil {
		return "", err
	}
	if strLen > maxStringLen {
		return "", fmt.Errorf("string length %d exceeds maximum allowed %d", strLen, maxStringLen)
	}

	buf := make([]byte, strLen)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}

	return string(buf), nil
}

func writeString(w io.Writer, s string) error {
	strBytes := []byte(s)
	if err := binary.Write(w, binary.BigEndian, uint32(len(strBytes))); err != nil {
		return err
	}

	return binary.Write(w, binary.BigEndian, strBytes)
}

const maxMapLen = 64 * 1024 * 1024 // 64 MiB

func readMap(r io.Reader) (map[string]any, error) {
	var mapLen uint32
	if err := binary.Read(r, binary.BigEndian, &mapLen); err != nil {
		return nil, err
	}
	if mapLen > maxMapLen {
		return nil, fmt.Errorf("map length %d exceeds maximum allowed %d", mapLen, maxMapLen)
	}

	buf := make([]byte, mapLen)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}

	m := make(map[string]any)
	if err := msgpack.Unmarshal(buf, &m); err != nil {
		return nil, err
	}

	return m, nil
}

func writeMap(w io.Writer, m map[string]any) error {
	bytes, err := msgpack.Marshal(m)
	if err != nil {
		return err
	}

	mapLen := uint32(len(bytes))
	if err := binary.Write(w, binary.BigEndian, mapLen); err != nil {
		return err
	}

	return binary.Write(w, binary.BigEndian, bytes)
}

func ReadFooter(r io.Reader) (*Footer, error) {
	footer := &Footer{}
	if err := binary.Read(r, binary.BigEndian, footer); err != nil {
		return nil, err
	}

	return footer, nil
}

func WriteFooter(w io.Writer, footer *Footer) error {
	return binary.Write(w, binary.BigEndian, footer)
}

func ReadTopicMetadata(r io.Reader) (*TopicMetadata, error) {
	topicMetadata := &TopicMetadata{}

	if err := binary.Read(r, binary.BigEndian, &topicMetadata.Id); err != nil {
		return nil, err
	}

	var err error
	if topicMetadata.Name, err = readString(r); err != nil {
		return nil, err
	}
	if topicMetadata.Metadata, err = readMap(r); err != nil {
		return nil, err
	}
	return topicMetadata, nil
}

func WriteTopicMetadata(w io.Writer, topicMetadata *TopicMetadata) error {
	if err := binary.Write(w, binary.BigEndian, topicMetadata.Id); err != nil {
		return err
	}
	if err := writeString(w, topicMetadata.Name); err != nil {
		return err
	}
	return writeMap(w, topicMetadata.Metadata)
}

func ReadIndexChunkInfo(r io.Reader) (*IndexChunkInfo, error) {
	indexChunkInfo := &IndexChunkInfo{}
	if err := binary.Read(r, binary.BigEndian, indexChunkInfo); err != nil {
		return nil, err
	}

	return indexChunkInfo, nil
}

func WriteIndexChunkInfo(w io.Writer, indexChunkInfo *IndexChunkInfo) error {
	return binary.Write(w, binary.BigEndian, indexChunkInfo)
}

func ReadTopicsInfo(r io.Reader) (*TopicsInfo, error) {
	topicsInfo := &TopicsInfo{}

	var topicMetadataLen uint32
	if err := binary.Read(r, binary.BigEndian, &topicMetadataLen); err != nil {
		return nil, err
	}

	var err error
	topicsInfo.TopicMetadatas = make([]*TopicMetadata, topicMetadataLen)
	for i := 0; i < int(topicMetadataLen); i++ {
		if topicsInfo.TopicMetadatas[i], err = ReadTopicMetadata(r); err != nil {
			return nil, err
		}
	}

	var indexChunkInfoLen uint32
	if err := binary.Read(r, binary.BigEndian, &indexChunkInfoLen); err != nil {
		return nil, err
	}

	topicsInfo.IndexChunkInfoList = make([]*IndexChunkInfo, indexChunkInfoLen)
	for i := 0; i < int(indexChunkInfoLen); i++ {
		if topicsInfo.IndexChunkInfoList[i], err = ReadIndexChunkInfo(r); err != nil {
			return nil, err
		}
	}

	if err := binary.Read(r, binary.BigEndian, &topicsInfo.TotalLen); err != nil {
		return nil, err
	}

	return topicsInfo, nil
}

func WriteTopicsInfo(w io.Writer, topicsInfo *TopicsInfo) error {
	if err := binary.Write(w, binary.BigEndian, uint32(len(topicsInfo.TopicMetadatas))); err != nil {
		return err
	}

	for _, topicMetadata := range topicsInfo.TopicMetadatas {
		if err := WriteTopicMetadata(w, topicMetadata); err != nil {
			return err
		}
	}

	if err := binary.Write(w, binary.BigEndian, uint32(len(topicsInfo.IndexChunkInfoList))); err != nil {
		return err
	}

	for _, indexChunkInfo := range topicsInfo.IndexChunkInfoList {
		if err := WriteIndexChunkInfo(w, indexChunkInfo); err != nil {
			return err
		}
	}

	return binary.Write(w, binary.BigEndian, topicsInfo.TotalLen)
}

func ReadSummary(r io.Reader) (*Summary, error) {
	summary := &Summary{}
	var topicsInfoLen uint32
	if err := binary.Read(r, binary.BigEndian, &topicsInfoLen); err != nil {
		return nil, err
	}

	var err error
	summary.TopicsInfos = make([]*TopicsInfo, topicsInfoLen)
	for i := 0; i < int(topicsInfoLen); i++ {
		if summary.TopicsInfos[i], err = ReadTopicsInfo(r); err != nil {
			return nil, err
		}
	}

	return summary, nil
}

func WriteSummary(w io.Writer, summary *Summary) error {
	if err := binary.Write(w, binary.BigEndian, uint32(len(summary.TopicsInfos))); err != nil {
		return err
	}
	for _, topicsInfo := range summary.TopicsInfos {
		if err := WriteTopicsInfo(w, topicsInfo); err != nil {
			return err
		}
	}

	return nil
}

func ReadMessageIndex(r io.Reader) (*MessageIndex, error) {
	messageIndex := &MessageIndex{}
	if err := binary.Read(r, binary.BigEndian, messageIndex); err != nil {
		return nil, err
	}
	return messageIndex, nil
}

func WriteMessageIndex(w io.Writer, messageIndex *MessageIndex) error {
	if err := binary.Write(w, binary.BigEndian, messageIndex); err != nil {
		return err
	}
	return nil
}

func ReadTopicIndex(r io.Reader) (*TopicIndex, error) {
	topicIndex := &TopicIndex{}
	if err := binary.Read(r, binary.BigEndian, &topicIndex.Id); err != nil {
		return nil, err
	}

	var messageIndexLen uint32
	if err := binary.Read(r, binary.BigEndian, &messageIndexLen); err != nil {
		return nil, err
	}

	var err error
	topicIndex.MessageIndexes = make([]*MessageIndex, messageIndexLen)
	for i := 0; i < int(messageIndexLen); i++ {
		if topicIndex.MessageIndexes[i], err = ReadMessageIndex(r); err != nil {
			return nil, err
		}
	}

	var keyFrameIndexLen uint32
	if err := binary.Read(r, binary.BigEndian, &keyFrameIndexLen); err != nil {
		return nil, err
	}

	topicIndex.KeyFrameIndexes = make([]uint32, keyFrameIndexLen)
	for i := 0; i < int(keyFrameIndexLen); i++ {
		if err := binary.Read(r, binary.BigEndian, &topicIndex.KeyFrameIndexes[i]); err != nil {
			return nil, err
		}
	}

	return topicIndex, nil
}

func WriteTopicIndex(w io.Writer, topicIndex *TopicIndex) error {
	if err := binary.Write(w, binary.BigEndian, topicIndex.Id); err != nil {
		return err
	}

	if err := binary.Write(w, binary.BigEndian, uint32(len(topicIndex.MessageIndexes))); err != nil {
		return err
	}

	for _, messageIndex := range topicIndex.MessageIndexes {
		if err := WriteMessageIndex(w, messageIndex); err != nil {
			return err
		}
	}

	if err := binary.Write(w, binary.BigEndian, uint32(len(topicIndex.KeyFrameIndexes))); err != nil {
		return err
	}

	for _, keyFrameIndex := range topicIndex.KeyFrameIndexes {
		if err := binary.Write(w, binary.BigEndian, keyFrameIndex); err != nil {
			return err
		}
	}

	return nil
}

func ReadIndexChunk(r io.Reader) (*IndexChunk, error) {
	indexChunk := &IndexChunk{}

	var topicIndexesLen uint32
	if err := binary.Read(r, binary.BigEndian, &topicIndexesLen); err != nil {
		return nil, err
	}

	var err error
	indexChunk.TopicIndexes = make([]*TopicIndex, topicIndexesLen)
	for i := 0; i < int(topicIndexesLen); i++ {
		if indexChunk.TopicIndexes[i], err = ReadTopicIndex(r); err != nil {
			return nil, err
		}
	}

	if err := binary.Read(r, binary.BigEndian, &indexChunk.ChunkOffset); err != nil {
		return nil, err
	}

	if err := binary.Read(r, binary.BigEndian, &indexChunk.ChunkLen); err != nil {
		return nil, err
	}

	if err := binary.Read(r, binary.BigEndian, &indexChunk.UncompressedLen); err != nil {
		return nil, err
	}

	return indexChunk, nil
}

func WriteIndexChunk(w io.Writer, indexChunk *IndexChunk) error {
	if err := binary.Write(w, binary.BigEndian, uint32(len(indexChunk.TopicIndexes))); err != nil {
		return err
	}

	for _, topicIndex := range indexChunk.TopicIndexes {
		if err := WriteTopicIndex(w, topicIndex); err != nil {
			return err
		}
	}

	if err := binary.Write(w, binary.BigEndian, indexChunk.ChunkOffset); err != nil {
		return err
	}

	if err := binary.Write(w, binary.BigEndian, indexChunk.ChunkLen); err != nil {
		return err
	}

	return binary.Write(w, binary.BigEndian, indexChunk.UncompressedLen)
}
