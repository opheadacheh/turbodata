package turbodata

import (
	"bytes"
	"io"
)

type TopicsGroupIterator struct {
	rs io.ReadSeeker

	// Memory buffers.
	loadBuf     *ReusableBuffer
	IndexBuf    *ReusableBuffer
	messageBuf  *ReusableBuffer
	bytesReader *bytes.Reader

	// Sort/filter scratch + outputs (reused per chunk).
	scratch                *sortAndFilterMergeScratch
	filteredMessageIndexes []*messageIndexWithTopicId
	filteredMessageLens    []int64

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

	// Message index cursor.
	currentMessageIndex int
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

	return &TopicsGroupIterator{
		rs: it.rs,

		loadBuf:     NewReusableBuffer(),
		IndexBuf:    NewReusableBuffer(),
		messageBuf:  NewReusableBuffer(),
		bytesReader: bytes.NewReader(nil),

		scratch:                newSortAndFilterMergeScratch(),
		filteredMessageIndexes: make([]*messageIndexWithTopicId, 0),
		filteredMessageLens:    make([]int64, 0),

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

		sortAndFilterMerge(
			indexChunk.TopicIndexes,
			indexChunk.UncompressedLen,
			it.topicIds,
			it.startTimestamp,
			it.endTimestamp,
			it.scratch,
			&it.filteredMessageIndexes,
			&it.filteredMessageLens,
		)

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
