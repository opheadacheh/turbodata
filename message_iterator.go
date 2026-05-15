package turbodata

import (
	"bytes"
	"container/heap"
	"fmt"
	"io"
	"math"
)

type indexChunkInfoWithLen struct {
	indexChunkInfo *IndexChunkInfo
	len            uint64
}

type TopicState struct {
	name         string
	isCompressed bool

	indexChunkInfoList         []*indexChunkInfoWithLen
	currentIndexChunkInfoIndex int

	messageIndexes      []*MessageIndex
	messageIndexLens    []uint64
	currentMessageIndex int

	buf *ReusableBuffer
}

type ReusableBuffer struct {
	Data []byte
}

type MessageIterator struct {
	rs io.ReadSeeker

	heap        *TimeOrderHeap
	reverseHeap *ReverseTimeOrderHeap
	loaded      bool

	topicNames     []string
	startTimestamp uint64
	endTimestamp   uint64
	order          Order

	topicState    map[uint16]*TopicState
	loadBuf       *ReusableBuffer
	decompressBuf *ReusableBuffer
}

func prepareReusableBuf(buf *ReusableBuffer, len uint64) {
	if cap(buf.Data) < int(len) {
		buf.Data = make([]byte, len)
		return
	}

	buf.Data = buf.Data[:len]
}

func getMessageIndexLens(messageIndexes []*MessageIndex, totalLen uint64) []uint64 {
	messageIndexLens := make([]uint64, len(messageIndexes))
	for i, messageIndex := range messageIndexes {
		if i == 0 {
			continue
		}
		messageIndexLens[i-1] = messageIndex.OffsetInChunk - messageIndexes[i-1].OffsetInChunk
	}

	messageIndexLens[len(messageIndexLens)-1] = totalLen - messageIndexes[len(messageIndexes)-1].OffsetInChunk

	return messageIndexLens
}

func newMessageIterator(rs io.ReadSeeker) *MessageIterator {
	return &MessageIterator{
		rs:             rs,
		order:          TimeOrder,
		startTimestamp: 0,
		endTimestamp:   math.MaxUint64,
		topicNames:     []string{},
		topicState:     make(map[uint16]*TopicState),
		loadBuf:        &ReusableBuffer{Data: make([]byte, 0)},
		decompressBuf:  &ReusableBuffer{Data: make([]byte, 0)},
	}
}

func (it *MessageIterator) loadIndexChunk(info *indexChunkInfoWithLen) (*IndexChunk, error) {
	it.rs.Seek(int64(info.indexChunkInfo.Offset), io.SeekStart)
	prepareReusableBuf(it.loadBuf, info.len)

	_, err := it.rs.Read(it.loadBuf.Data)
	if err != nil {
		return nil, err
	}

	err = decompressInto(it.loadBuf.Data, it.decompressBuf)
	if err != nil {
		return nil, err
	}

	return ReadIndexChunk(bytes.NewReader(it.decompressBuf.Data))
}

func (it *MessageIterator) loadDataChunk(rbuf *ReusableBuffer, topicId uint16, indexChunk *IndexChunk) error {
	it.rs.Seek(int64(indexChunk.ChunkOffset), io.SeekStart)

	if it.topicState[topicId].isCompressed {
		prepareReusableBuf(it.loadBuf, indexChunk.ChunkLen)
		_, err := it.rs.Read(it.loadBuf.Data)
		if err != nil {
			return err
		}

		if err := decompressInto(it.loadBuf.Data, rbuf); err != nil {
			return err
		}
		return nil
	}

	prepareReusableBuf(rbuf, indexChunk.ChunkLen)
	_, err := it.rs.Read(rbuf.Data)
	if err != nil {
		return err
	}

	return nil
}

func (it *MessageIterator) initLoad() error {
	switch it.order {
	case TimeOrder:
		for topicId, topicState := range it.topicState {
			if len(topicState.indexChunkInfoList) == 0 {
				continue
			}

			indexChunk, err := it.loadIndexChunk(topicState.indexChunkInfoList[0])
			if err != nil {
				return err
			}
			it.topicState[topicId].currentIndexChunkInfoIndex = 0

			it.loadDataChunk(it.topicState[topicId].buf, topicId, indexChunk)

			it.topicState[topicId].messageIndexes = indexChunk.TopicIndexes[0].MessageIndexes
			for i, messageIndex := range indexChunk.TopicIndexes[0].MessageIndexes {
				if messageIndex.Timestamp < it.startTimestamp {
					continue
				}

				heap.Push(it.heap, &messageIndexWithTopicId{topicId: topicId, messageIndex: messageIndex})
				it.topicState[topicId].currentMessageIndex = i
				break
			}
			it.topicState[topicId].messageIndexLens = getMessageIndexLens(indexChunk.TopicIndexes[0].MessageIndexes, indexChunk.UncompressedLen)
		}
	case ReverseTimeOrder:
		for topicId, topicState := range it.topicState {
			if len(topicState.indexChunkInfoList) == 0 {
				continue
			}

			indexChunk, err := it.loadIndexChunk(topicState.indexChunkInfoList[len(topicState.indexChunkInfoList)-1])
			if err != nil {
				continue
			}
			it.topicState[topicId].currentIndexChunkInfoIndex = len(topicState.indexChunkInfoList) - 1

			it.loadDataChunk(it.topicState[topicId].buf, topicId, indexChunk)

			for i := len(indexChunk.TopicIndexes[0].MessageIndexes) - 1; i >= 0; i-- {
				messageIndex := indexChunk.TopicIndexes[0].MessageIndexes[i]
				if messageIndex.Timestamp > it.endTimestamp {
					continue
				}

				heap.Push(it.reverseHeap, &messageIndexWithTopicId{topicId: topicId, messageIndex: messageIndex})
				it.topicState[topicId].currentMessageIndex = i
				break
			}
			it.topicState[topicId].messageIndexLens = getMessageIndexLens(indexChunk.TopicIndexes[0].MessageIndexes, indexChunk.UncompressedLen)
		}
	}
	return nil
}

func (it *MessageIterator) readData(topicId uint16, messageIndex *MessageIndex) ([]byte, error) {
	len := it.topicState[topicId].messageIndexLens[it.topicState[topicId].currentMessageIndex]
	return it.topicState[topicId].buf.Data[messageIndex.OffsetInChunk : messageIndex.OffsetInChunk+len], nil
}

func (it *MessageIterator) pushNextMessageIndex(topicId uint16) {
	topicState := it.topicState[topicId]

	if topicState.currentMessageIndex < len(topicState.messageIndexes)-1 {
		topicState.currentMessageIndex++
		messageIndex := topicState.messageIndexes[topicState.currentMessageIndex]

		if messageIndex.Timestamp > it.endTimestamp {
			return
		}

		heap.Push(it.heap, &messageIndexWithTopicId{topicId: topicId, messageIndex: messageIndex})
		return
	}

	if topicState.currentIndexChunkInfoIndex < len(topicState.indexChunkInfoList)-1 {
		topicState.currentIndexChunkInfoIndex++

		indexChunk, err := it.loadIndexChunk(topicState.indexChunkInfoList[topicState.currentIndexChunkInfoIndex])
		if err != nil {
			return
		}

		topicState.messageIndexes = indexChunk.TopicIndexes[0].MessageIndexes

		topicState.currentMessageIndex = 0
		messageIndex := topicState.messageIndexes[topicState.currentMessageIndex]

		if messageIndex.Timestamp > it.endTimestamp {
			return
		}

		heap.Push(it.heap, &messageIndexWithTopicId{topicId: topicId, messageIndex: messageIndex})
		return
	}
}

func (it *MessageIterator) Next() ([]byte, uint64, string, error) {
	if !it.loaded {
		if err := it.initLoad(); err != nil {
			return nil, 0, "", err
		}
		it.loaded = true
	}

	switch it.order {
	case TimeOrder:
		if it.heap.Len() == 0 {
			return nil, 0, "", io.EOF
		}

		messageIndexWithTopicId, ok := heap.Pop(it.heap).(*messageIndexWithTopicId)
		if !ok {
			return nil, 0, "", fmt.Errorf("failed to pop message index with topic id")
		}

		topicId := messageIndexWithTopicId.topicId
		messageIndex := messageIndexWithTopicId.messageIndex

		data, err := it.readData(topicId, messageIndex)
		if err != nil {
			return nil, 0, "", err
		}

		it.pushNextMessageIndex(topicId)

		return data, messageIndex.Timestamp, it.topicState[topicId].name, nil
	case ReverseTimeOrder:
		if it.reverseHeap.Len() == 0 {
			return nil, 0, "", io.EOF
		}

		for {
			messageIndex := it.reverseHeap.Pop().(*messageIndexWithTopicId).messageIndex
			if messageIndex.Timestamp < it.startTimestamp {
				return nil, 0, "", io.EOF
			}
		}

		// TODO: implement reverse time order
	}

	return nil, 0, "", fmt.Errorf("invalid read order: %d", it.order)
}
