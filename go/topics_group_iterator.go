package turbodata

import (
	"bytes"
	"container/heap"
	"io"
	"sync"
)

type TopicsGroupIterator struct {
	rs io.ReadSeeker

	// For sorting message indexes.
	sortHeap           heap.Interface
	idToMessageIndexes map[uint16][]*MessageIndex
	idToIndex          map[uint16]int

	// Memory buffers.
	loadBuf        *ReusableBuffer
	IndexBuf       *ReusableBuffer
	messageBuf     *ReusableBuffer
	sortItemPool   sync.Pool
	messageIndexes []*messageIndexWithTopicId
	messageLens    []int64
	bytesReader    *bytes.Reader

	// Read config.
	topicIds       map[uint16]struct{}
	startTimestamp int64
	endTimestamp   int64
	order          Order
	isCompressed   bool

	// Index chunk queue.
	indexChunkInfoList         []*IndexChunkInfo
	currentIndexChunkInfoIndex int
	indexChunkInfoTotalLen     int64

	// Message index queue.
	sortedMessageIndexes []*messageIndexWithTopicId
	sortedMessageLens    []int64
	currentMessageIndex  int
}

func newTopicsGroupIterator(it *MessageIterator, topicIds map[uint16]struct{}, topicsInfo *TopicsInfo) *TopicsGroupIterator {
	indexChunkInfoList := make([]*IndexChunkInfo, 0, len(topicsInfo.IndexChunkInfoList))
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

	tgi := &TopicsGroupIterator{
		rs: it.rs,

		sortHeap: sortHeap,

		loadBuf:        NewReusableBuffer(),
		IndexBuf:       NewReusableBuffer(),
		messageBuf:     NewReusableBuffer(),
		messageIndexes: make([]*messageIndexWithTopicId, 0),
		messageLens:    make([]int64, 0),
		bytesReader:    bytes.NewReader(nil),

		idToMessageIndexes: make(map[uint16][]*MessageIndex),
		idToIndex:          make(map[uint16]int),

		topicIds:       topicIds,
		startTimestamp: it.startTimestamp,
		endTimestamp:   it.endTimestamp,
		order:          it.order,
		isCompressed:   isCompressed,

		indexChunkInfoList:         indexChunkInfoList,
		currentIndexChunkInfoIndex: 0,
		indexChunkInfoTotalLen:     topicsInfo.TotalLen,
	}
	tgi.sortItemPool.New = func() any { return &messageIndexWithTopicId{} }
	return tgi
}

func (it *TopicsGroupIterator) Next() (int64, uint16, []byte, error) {
	for it.currentMessageIndex >= len(it.messageIndexes) {
		if it.currentIndexChunkInfoIndex >= len(it.indexChunkInfoList) {
			return 0, 0, nil, io.EOF
		}

		currentIndexChunkInfoLen := int64(0)
		if it.currentIndexChunkInfoIndex < len(it.indexChunkInfoList)-1 {
			currentIndexChunkInfoLen = it.indexChunkInfoList[it.currentIndexChunkInfoIndex+1].Offset - it.indexChunkInfoList[it.currentIndexChunkInfoIndex].Offset
		} else {
			currentIndexChunkInfoLen = it.indexChunkInfoTotalLen - it.indexChunkInfoList[it.currentIndexChunkInfoIndex].Offset + it.indexChunkInfoList[0].Offset
		}

		indexChunk, err := it.loadIndexChunk(it.indexChunkInfoList[it.currentIndexChunkInfoIndex], currentIndexChunkInfoLen)
		if err != nil {
			return 0, 0, nil, err
		}
		it.currentIndexChunkInfoIndex++

		it.sortAndFilterMessageIndexes(indexChunk.TopicIndexes, indexChunk.UncompressedLen)
		it.currentMessageIndex = 0

		if len(it.messageIndexes) > 0 {
			if err := it.loadDataChunk(indexChunk.ChunkOffset, indexChunk.ChunkLen); err != nil {
				return 0, 0, nil, err
			}
		}
	}

	messageIndex := it.messageIndexes[it.currentMessageIndex]
	rangeEnd := messageIndex.messageIndex.OffsetInChunk + it.messageLens[it.currentMessageIndex]
	it.currentMessageIndex++

	return messageIndex.messageIndex.Timestamp, messageIndex.topicId, it.messageBuf.Data[messageIndex.messageIndex.OffsetInChunk:rangeEnd], nil
}

func (it *TopicsGroupIterator) sortAndFilterMessageIndexes(topicIndexes []*TopicIndex, totalLen int64) {
	// Return items from the previous chunk back to the pool — they've been fully consumed.
	for _, item := range it.messageIndexes {
		item.messageIndex = nil
		it.sortItemPool.Put(item)
	}

	totalMessageIndexes := 0
	for _, topicIndex := range topicIndexes {
		totalMessageIndexes += len(topicIndex.MessageIndexes)
	}
	if cap(it.messageIndexes) < totalMessageIndexes {
		it.messageIndexes = make([]*messageIndexWithTopicId, 0, totalMessageIndexes)
	} else {
		it.messageIndexes = it.messageIndexes[:0]
	}
	if cap(it.messageLens) < totalMessageIndexes {
		it.messageLens = make([]int64, totalMessageIndexes)
	} else {
		it.messageLens = it.messageLens[:totalMessageIndexes]
	}

	if len(topicIndexes) == 1 {
		// Fast path: single topic, messages are already in timestamp order.
		topicIndex := topicIndexes[0]
		for _, messageIndex := range topicIndex.MessageIndexes {
			item := it.sortItemPool.Get().(*messageIndexWithTopicId)
			item.topicId = topicIndex.Id
			item.messageIndex = messageIndex
			it.messageIndexes = append(it.messageIndexes, item)
		}
	} else {
		for k := range it.idToMessageIndexes {
			delete(it.idToMessageIndexes, k)
		}
		for k := range it.idToIndex {
			delete(it.idToIndex, k)
		}
		for _, topicIndex := range topicIndexes {
			if len(topicIndex.MessageIndexes) == 0 {
				continue
			}

			it.idToMessageIndexes[topicIndex.Id] = topicIndex.MessageIndexes
			sortItem := it.sortItemPool.Get().(*messageIndexWithTopicId)
			sortItem.topicId = topicIndex.Id
			sortItem.messageIndex = topicIndex.MessageIndexes[0]
			heap.Push(it.sortHeap, sortItem)
			it.idToIndex[topicIndex.Id] = 1
		}

		for it.sortHeap.Len() > 0 {
			item, _ := heap.Pop(it.sortHeap).(*messageIndexWithTopicId)

			it.messageIndexes = append(it.messageIndexes, item)

			topicId := item.topicId
			if it.idToIndex[topicId] >= len(it.idToMessageIndexes[topicId]) {
				continue
			}

			nextItem := it.sortItemPool.Get().(*messageIndexWithTopicId)
			nextItem.topicId = topicId
			nextItem.messageIndex = it.idToMessageIndexes[topicId][it.idToIndex[topicId]]
			heap.Push(it.sortHeap, nextItem)
			it.idToIndex[topicId]++
		}
	}
	for i := 0; i < len(it.messageIndexes)-1; i++ {
		it.messageLens[i] = it.messageIndexes[i+1].messageIndex.OffsetInChunk - it.messageIndexes[i].messageIndex.OffsetInChunk
	}
	it.messageLens[len(it.messageLens)-1] = totalLen - it.messageIndexes[len(it.messageIndexes)-1].messageIndex.OffsetInChunk

	if cap(it.sortedMessageIndexes) < len(it.messageIndexes) {
		it.sortedMessageIndexes = make([]*messageIndexWithTopicId, 0, len(it.messageIndexes))
	} else {
		it.sortedMessageIndexes = it.sortedMessageIndexes[:0]
	}
	if cap(it.sortedMessageLens) < len(it.messageIndexes) {
		it.sortedMessageLens = make([]int64, 0, len(it.messageIndexes))
	} else {
		it.sortedMessageLens = it.sortedMessageLens[:0]
	}

	for i := 0; i < len(it.messageIndexes); i++ {
		if _, ok := it.topicIds[it.messageIndexes[i].topicId]; !ok {
			it.messageIndexes[i].messageIndex = nil
			it.sortItemPool.Put(it.messageIndexes[i])
			continue
		}

		if it.messageIndexes[i].messageIndex.Timestamp < it.startTimestamp {
			it.messageIndexes[i].messageIndex = nil
			it.sortItemPool.Put(it.messageIndexes[i])
			continue
		}
		if it.messageIndexes[i].messageIndex.Timestamp > it.endTimestamp {
			it.messageIndexes[i].messageIndex = nil
			it.sortItemPool.Put(it.messageIndexes[i])
			continue
		}

		it.sortedMessageIndexes = append(it.sortedMessageIndexes, it.messageIndexes[i])
		it.sortedMessageLens = append(it.sortedMessageLens, it.messageLens[i])
	}
}

func (it *TopicsGroupIterator) loadIndexChunk(info *IndexChunkInfo, len int64) (*IndexChunk, error) {
	if _, err := it.rs.Seek(info.Offset, io.SeekStart); err != nil {
		return nil, err
	}
	it.loadBuf.Prepare(int(len))

	if _, err := io.ReadFull(it.rs, it.loadBuf.Data); err != nil {
		return nil, err
	}

	if err := decompressInto(it.loadBuf.Data, it.IndexBuf); err != nil {
		return nil, err
	}

	it.bytesReader.Reset(it.IndexBuf.Data)
	return ReadIndexChunk(it.bytesReader)
}

func (it *TopicsGroupIterator) loadDataChunk(offset int64, len int64) error {
	if _, err := it.rs.Seek(offset, io.SeekStart); err != nil {
		return err
	}

	if it.isCompressed {
		it.loadBuf.Prepare(int(len))
		if _, err := io.ReadFull(it.rs, it.loadBuf.Data); err != nil {
			return err
		}

		if err := decompressInto(it.loadBuf.Data, it.messageBuf); err != nil {
			return err
		}
		return nil
	}

	it.messageBuf.Prepare(int(len))
	if _, err := io.ReadFull(it.rs, it.messageBuf.Data); err != nil {
		return err
	}

	return nil
}
