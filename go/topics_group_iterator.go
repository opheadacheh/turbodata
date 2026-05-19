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
	loadBuf              *ReusableBuffer
	IndexBuf             *ReusableBuffer
	messageBuf           *ReusableBuffer
	sortItemPool         sync.Pool
	sortedMessageIndexes []*messageIndexWithTopicId
	sortedMessageLens    []int64
	bytesReader          *bytes.Reader

	// Read config.
	topicIds        map[uint16]struct{}
	startTimestamp  int64
	endTimestamp    int64
	order           Order
	isCompressed    bool
	incrementFactor int

	// Index chunk queue.
	indexChunkInfoList         []*IndexChunkInfo
	indexChunkInfoLens         []int64
	currentIndexChunkInfoIndex int

	// Message index queue.
	filteredMessageIndexes []*messageIndexWithTopicId
	filteredMessageLens    []int64
	currentMessageIndex    int
}

func newTopicsGroupIterator(it *MessageIterator, topicIds map[uint16]struct{}, topicsInfo *TopicsInfo) *TopicsGroupIterator {
	indexChunkInfoList := make([]*IndexChunkInfo, 0, len(topicsInfo.IndexChunkInfoList))
	indexChunkInfoLens := make([]int64, 0, len(topicsInfo.IndexChunkInfoList))
	for i, indexChunkInfo := range topicsInfo.IndexChunkInfoList {
		if indexChunkInfo.EndTimestamp < it.startTimestamp {
			continue
		}

		if indexChunkInfo.StartTimestamp > it.endTimestamp {
			break
		}

		indexChunkInfoList = append(indexChunkInfoList, indexChunkInfo)
		if i < len(topicsInfo.IndexChunkInfoList)-1 {
			indexChunkInfoLens = append(indexChunkInfoLens, topicsInfo.IndexChunkInfoList[i+1].Offset-indexChunkInfo.Offset)
		} else {
			indexChunkInfoLens = append(indexChunkInfoLens, topicsInfo.TotalLen-indexChunkInfo.Offset+topicsInfo.IndexChunkInfoList[0].Offset)
		}
	}

	sortHeap := &MessageIndexHeap{}
	heap.Init(sortHeap)

	isCompressed, ok := topicsInfo.TopicMetadatas[0].Metadata["is_compressed"].(bool)
	if !ok {
		isCompressed = false
	}

	incrementFactor := 1
	currentIndexChunkInfoIndex := 0
	if it.order == ReverseTimeOrder {
		incrementFactor = -1
		currentIndexChunkInfoIndex = len(indexChunkInfoList) - 1
	}

	tgi := &TopicsGroupIterator{
		rs: it.rs,

		sortHeap: sortHeap,

		loadBuf:              NewReusableBuffer(),
		IndexBuf:             NewReusableBuffer(),
		messageBuf:           NewReusableBuffer(),
		sortedMessageIndexes: make([]*messageIndexWithTopicId, 0),
		sortedMessageLens:    make([]int64, 0),
		bytesReader:          bytes.NewReader(nil),

		idToMessageIndexes: make(map[uint16][]*MessageIndex),
		idToIndex:          make(map[uint16]int),

		topicIds:        topicIds,
		startTimestamp:  it.startTimestamp,
		endTimestamp:    it.endTimestamp,
		order:           it.order,
		isCompressed:    isCompressed,
		incrementFactor: incrementFactor,

		indexChunkInfoList:         indexChunkInfoList,
		currentIndexChunkInfoIndex: currentIndexChunkInfoIndex,
		indexChunkInfoLens:         indexChunkInfoLens,
	}
	tgi.sortItemPool.New = func() any { return &messageIndexWithTopicId{} }
	return tgi
}

func (it *TopicsGroupIterator) Next() (int64, uint16, []byte, error) {
	for it.currentMessageIndex >= len(it.filteredMessageIndexes) || it.currentMessageIndex < 0 {
		if it.currentIndexChunkInfoIndex >= len(it.indexChunkInfoList) || it.currentIndexChunkInfoIndex < 0 {
			return 0, 0, nil, io.EOF
		}

		indexChunk, err := it.loadIndexChunk(it.indexChunkInfoList[it.currentIndexChunkInfoIndex], it.indexChunkInfoLens[it.currentIndexChunkInfoIndex])
		if err != nil {
			return 0, 0, nil, err
		}
		it.currentIndexChunkInfoIndex += it.incrementFactor

		it.sortAndFilterMessageIndexes(indexChunk.TopicIndexes, indexChunk.UncompressedLen)

		it.currentMessageIndex = 0
		if it.order == ReverseTimeOrder {
			it.currentMessageIndex = len(it.filteredMessageIndexes) - 1
		}

		if len(it.filteredMessageIndexes) > 0 {
			if err := it.loadDataChunk(indexChunk.ChunkOffset, indexChunk.ChunkLen); err != nil {
				return 0, 0, nil, err
			}
		}
	}

	messageIndex := it.filteredMessageIndexes[it.currentMessageIndex]
	rangeEnd := messageIndex.messageIndex.OffsetInChunk + it.filteredMessageLens[it.currentMessageIndex]
	it.currentMessageIndex += it.incrementFactor

	return messageIndex.messageIndex.Timestamp, messageIndex.topicId, it.messageBuf.Data[messageIndex.messageIndex.OffsetInChunk:rangeEnd], nil
}

func (it *TopicsGroupIterator) sortAndFilterMessageIndexes(topicIndexes []*TopicIndex, totalLen int64) {
	// Return items from the previous chunk back to the pool — they've been fully consumed.
	for _, item := range it.filteredMessageIndexes {
		item.messageIndex = nil
		it.sortItemPool.Put(item)
	}

	totalMessageIndexes := 0
	for _, topicIndex := range topicIndexes {
		totalMessageIndexes += len(topicIndex.MessageIndexes)
	}
	if cap(it.sortedMessageIndexes) < totalMessageIndexes {
		it.sortedMessageIndexes = make([]*messageIndexWithTopicId, 0, totalMessageIndexes)
	} else {
		it.sortedMessageIndexes = it.sortedMessageIndexes[:0]
	}
	if cap(it.sortedMessageLens) < totalMessageIndexes {
		it.sortedMessageLens = make([]int64, totalMessageIndexes)
	} else {
		it.sortedMessageLens = it.sortedMessageLens[:totalMessageIndexes]
	}

	if len(topicIndexes) == 1 {
		// Fast path: single topic, messages are already in timestamp order.
		topicIndex := topicIndexes[0]
		for _, messageIndex := range topicIndex.MessageIndexes {
			item := it.sortItemPool.Get().(*messageIndexWithTopicId)
			item.topicId = topicIndex.Id
			item.messageIndex = messageIndex
			it.sortedMessageIndexes = append(it.sortedMessageIndexes, item)
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

			it.sortedMessageIndexes = append(it.sortedMessageIndexes, item)

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
	for i := 0; i < len(it.sortedMessageIndexes)-1; i++ {
		it.sortedMessageLens[i] = it.sortedMessageIndexes[i+1].messageIndex.OffsetInChunk - it.sortedMessageIndexes[i].messageIndex.OffsetInChunk
	}
	it.sortedMessageLens[len(it.sortedMessageLens)-1] = totalLen - it.sortedMessageIndexes[len(it.sortedMessageIndexes)-1].messageIndex.OffsetInChunk

	if cap(it.filteredMessageIndexes) < len(it.sortedMessageIndexes) {
		it.filteredMessageIndexes = make([]*messageIndexWithTopicId, 0, len(it.sortedMessageIndexes))
	} else {
		it.filteredMessageIndexes = it.filteredMessageIndexes[:0]
	}
	if cap(it.filteredMessageLens) < len(it.sortedMessageIndexes) {
		it.filteredMessageLens = make([]int64, 0, len(it.sortedMessageIndexes))
	} else {
		it.filteredMessageLens = it.filteredMessageLens[:0]
	}

	for i := 0; i < len(it.sortedMessageIndexes); i++ {
		if _, ok := it.topicIds[it.sortedMessageIndexes[i].topicId]; !ok {
			it.sortedMessageIndexes[i].messageIndex = nil
			it.sortItemPool.Put(it.sortedMessageIndexes[i])
			continue
		}

		if it.sortedMessageIndexes[i].messageIndex.Timestamp < it.startTimestamp {
			it.sortedMessageIndexes[i].messageIndex = nil
			it.sortItemPool.Put(it.sortedMessageIndexes[i])
			continue
		}
		if it.sortedMessageIndexes[i].messageIndex.Timestamp > it.endTimestamp {
			it.sortedMessageIndexes[i].messageIndex = nil
			it.sortItemPool.Put(it.sortedMessageIndexes[i])
			continue
		}

		it.filteredMessageIndexes = append(it.filteredMessageIndexes, it.sortedMessageIndexes[i])
		it.filteredMessageLens = append(it.filteredMessageLens, it.sortedMessageLens[i])
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
