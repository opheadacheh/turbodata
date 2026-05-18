package turbodata

import (
	"bytes"
	"container/heap"
	"fmt"
	"io"
)

type TopicsGroupIterator struct {
	rs io.ReadSeeker

	sortHeap heap.Interface

	loadBuf    *ReusableBuffer
	IndexBuf   *ReusableBuffer
	messageBuf *ReusableBuffer

	topicIds       map[uint16]struct{}
	startTimestamp int64
	endTimestamp   int64
	order          Order
	isCompressed   bool

	indexChunkInfoList         []*IndexChunkInfo
	currentIndexChunkInfoIndex int
	indexChunkInfoTotalLen     int64

	messageIndexes      []*messageIndexWithTopicId
	messageLens         []int64
	currentMessageIndex int
}

func newTopicsGroupIterator(it *MessageIterator, topicIds map[uint16]struct{}, topicsInfo *TopicsInfo) *TopicsGroupIterator {
	indexChunkInfoList := []*IndexChunkInfo{}
	for _, indexChunkInfo := range topicsInfo.IndexChunkInfoList {
		if indexChunkInfo.EndTimestamp < it.startTimestamp {
			continue
		}

		if indexChunkInfo.StartTimestamp > it.endTimestamp {
			break
		}

		indexChunkInfoList = append(indexChunkInfoList, indexChunkInfo)
	}

	var sortHeap heap.Interface
	switch it.order {
	case TimeOrder:
		sortHeap = &MessageIndexHeap{}
	case ReverseTimeOrder:
		sortHeap = &ReverseMessageIndexHeap{}
	}
	heap.Init(sortHeap)

	isCompressed, ok := topicsInfo.TopicMetadatas[0].Metadata["is_compressed"].(bool)
	if !ok {
		isCompressed = false
	}

	return &TopicsGroupIterator{
		rs: it.rs,

		sortHeap: sortHeap,

		loadBuf:    NewReusableBuffer(),
		IndexBuf:   NewReusableBuffer(),
		messageBuf: NewReusableBuffer(),

		topicIds:       topicIds,
		startTimestamp: it.startTimestamp,
		endTimestamp:   it.endTimestamp,
		order:          it.order,
		isCompressed:   isCompressed,

		indexChunkInfoList:         indexChunkInfoList,
		currentIndexChunkInfoIndex: 0,
		indexChunkInfoTotalLen:     topicsInfo.TotalLen,
	}
}

func (it *TopicsGroupIterator) Next() (uint16, []byte, error) {
	for it.currentMessageIndex >= len(it.messageIndexes) {
		if it.currentIndexChunkInfoIndex >= len(it.indexChunkInfoList) {
			return 0, nil, io.EOF
		}

		currentIndexChunkInfoLen := int64(0)
		if it.currentIndexChunkInfoIndex < len(it.indexChunkInfoList)-1 {
			currentIndexChunkInfoLen = it.indexChunkInfoList[it.currentIndexChunkInfoIndex+1].Offset - it.indexChunkInfoList[it.currentIndexChunkInfoIndex].Offset
		} else {
			currentIndexChunkInfoLen = it.indexChunkInfoTotalLen - it.indexChunkInfoList[it.currentIndexChunkInfoIndex].Offset + it.indexChunkInfoList[0].Offset
		}

		indexChunk, err := it.loadIndexChunk(it.indexChunkInfoList[it.currentIndexChunkInfoIndex], currentIndexChunkInfoLen)
		if err != nil {
			return 0, nil, err
		}
		it.currentIndexChunkInfoIndex++

		it.messageIndexes, it.messageLens = it.sortAndFilterMessageIndexes(indexChunk.TopicIndexes, indexChunk.UncompressedLen)
		it.currentMessageIndex = 0

		if len(it.messageIndexes) > 0 {
			it.loadDataChunk(indexChunk.ChunkOffset, indexChunk.ChunkLen)
		}
	}

	fmt.Println("currentMessageIndex, messageIndexes len", it.currentMessageIndex, len(it.messageIndexes))
	messageIndex := it.messageIndexes[it.currentMessageIndex]
	rangeEnd := messageIndex.messageIndex.OffsetInChunk + it.messageLens[it.currentMessageIndex]
	it.currentMessageIndex++

	return messageIndex.topicId, it.messageBuf.Data[messageIndex.messageIndex.OffsetInChunk:rangeEnd], nil
}

func (it *TopicsGroupIterator) sortAndFilterMessageIndexes(topicIndexes []*TopicIndex, totalLen int64) ([]*messageIndexWithTopicId, []int64) {
	totalMessageIndexes := 0
	for _, topicIndex := range topicIndexes {
		totalMessageIndexes += len(topicIndex.MessageIndexes)
	}
	messageIndexes := make([]*messageIndexWithTopicId, 0, totalMessageIndexes)
	messageLens := make([]int64, totalMessageIndexes)

	idToMessageIndexes := make(map[uint16][]*MessageIndex)
	idToIndex := make(map[uint16]int)
	for _, topicIndex := range topicIndexes {
		if len(topicIndex.MessageIndexes) == 0 {
			continue
		}

		idToMessageIndexes[topicIndex.Id] = topicIndex.MessageIndexes
		heap.Push(it.sortHeap, &messageIndexWithTopicId{topicId: topicIndex.Id, messageIndex: topicIndex.MessageIndexes[0]})
		idToIndex[topicIndex.Id] = 1
	}
	fmt.Println("sortHeap len", it.sortHeap.Len())

	for it.sortHeap.Len() > 0 {
		item, _ := heap.Pop(it.sortHeap).(*messageIndexWithTopicId)

		messageIndexes = append(messageIndexes, item)

		topicId := item.topicId
		if idToIndex[topicId] >= len(idToMessageIndexes[topicId]) {
			continue
		}

		messageIndex := idToMessageIndexes[topicId][idToIndex[topicId]]
		heap.Push(it.sortHeap, &messageIndexWithTopicId{topicId: topicId, messageIndex: messageIndex})
		idToIndex[topicId]++
	}
	fmt.Println("messageIndexes len", len(messageIndexes))
	for i := 0; i < len(messageIndexes)-1; i++ {
		messageLens[i] = messageIndexes[i+1].messageIndex.OffsetInChunk - messageIndexes[i].messageIndex.OffsetInChunk
	}
	messageLens[len(messageLens)-1] = totalLen - messageIndexes[len(messageIndexes)-1].messageIndex.OffsetInChunk

	filteredMessageIndexes := []*messageIndexWithTopicId{}
	filteredMessageLens := []int64{}
	for i := 0; i < len(messageIndexes); i++ {
		if _, ok := it.topicIds[messageIndexes[i].topicId]; !ok {
			continue
		}

		if messageIndexes[i].messageIndex.Timestamp < it.startTimestamp {
			continue
		}
		if messageIndexes[i].messageIndex.Timestamp > it.endTimestamp {
			break
		}

		filteredMessageIndexes = append(filteredMessageIndexes, messageIndexes[i])
		filteredMessageLens = append(filteredMessageLens, messageLens[i])
	}

	return filteredMessageIndexes, filteredMessageLens
}

func (it *TopicsGroupIterator) loadIndexChunk(info *IndexChunkInfo, len int64) (*IndexChunk, error) {
	fmt.Println("loadIndexChunk", info.Offset, len)
	it.rs.Seek(info.Offset, io.SeekStart)
	it.loadBuf.Prepare(int(len))

	_, err := it.rs.Read(it.loadBuf.Data)
	if err != nil {
		return nil, err
	}

	fmt.Println("loadIndexChunk read done", info.Offset, len)

	err = decompressInto(it.loadBuf.Data, it.IndexBuf)
	if err != nil {
		return nil, err
	}

	fmt.Println("loadIndexChunk done", info.Offset, len)

	return ReadIndexChunk(bytes.NewReader(it.IndexBuf.Data))
}

func (it *TopicsGroupIterator) loadDataChunk(offset int64, len int64) error {
	fmt.Println("loadDataChunk", offset, len)
	it.rs.Seek(offset, io.SeekStart)

	if it.isCompressed {
		it.loadBuf.Prepare(int(len))
		_, err := it.rs.Read(it.loadBuf.Data)
		if err != nil {
			return err
		}

		if err := decompressInto(it.loadBuf.Data, it.messageBuf); err != nil {
			return err
		}
		return nil
	}

	it.messageBuf.Prepare(int(len))
	_, err := it.rs.Read(it.messageBuf.Data)
	if err != nil {
		return err
	}

	return nil
}
