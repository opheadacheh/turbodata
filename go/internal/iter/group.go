package iter

import (
	"bytes"
	"io"

	"turbodata/format"
	"turbodata/internal/buffer"
	"turbodata/internal/compress"
)

type TopicsGroupIterator struct {
	rs io.ReadSeeker

	// Memory buffers.
	loadBuf     *buffer.ReusableBuffer
	indexBuf    *buffer.ReusableBuffer
	messageBuf  *buffer.ReusableBuffer
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
	indexChunkInfoList         []*format.IndexChunkInfo
	indexChunkInfoLens         []int64
	currentIndexChunkInfoIndex int

	// Message index cursor.
	currentMessageIndex int
}

func newTopicsGroupIterator(it *MessageIterator, topicIds map[uint16]struct{}, topicsInfo *format.TopicsInfo) *TopicsGroupIterator {
	return newTopicsGroupIteratorWithStart(it, topicIds, topicsInfo, it.StartTimestamp)
}

// newTopicsGroupIteratorWithStart is the variant used when the per-group
// start timestamp differs from the iterator's overall StartTimestamp (e.g.
// after snap-back to a key frame for a video topic under WithVideoDecodable).
func newTopicsGroupIteratorWithStart(it *MessageIterator, topicIds map[uint16]struct{}, topicsInfo *format.TopicsInfo, startTimestamp int64) *TopicsGroupIterator {
	indexChunkInfoList := make([]*format.IndexChunkInfo, 0, len(topicsInfo.IndexChunkInfoList))
	indexChunkInfoLens := make([]int64, 0, len(topicsInfo.IndexChunkInfoList))
	for i, indexChunkInfo := range topicsInfo.IndexChunkInfoList {
		if indexChunkInfo.EndTimestamp < startTimestamp {
			continue
		}

		if indexChunkInfo.StartTimestamp > it.EndTimestamp {
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
	if it.Order == ReverseTimeOrder {
		incrementFactor = -1
		currentIndexChunkInfoIndex = len(indexChunkInfoList) - 1
	}

	return &TopicsGroupIterator{
		rs: it.rs,

		loadBuf:     buffer.NewReusableBuffer(),
		indexBuf:    buffer.NewReusableBuffer(),
		messageBuf:  buffer.NewReusableBuffer(),
		bytesReader: bytes.NewReader(nil),

		scratch:                newSortAndFilterMergeScratch(),
		filteredMessageIndexes: make([]*messageIndexWithTopicId, 0),
		filteredMessageLens:    make([]int64, 0),

		topicIds:        topicIds,
		startTimestamp:  startTimestamp,
		endTimestamp:    it.EndTimestamp,
		order:           it.Order,
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

		sortAndFilter(
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

func (it *TopicsGroupIterator) loadIndexChunk(info *format.IndexChunkInfo, length int64) (*format.IndexChunk, error) {
	if _, err := it.rs.Seek(info.Offset, io.SeekStart); err != nil {
		return nil, err
	}
	it.loadBuf.Prepare(int(length))

	if _, err := io.ReadFull(it.rs, it.loadBuf.Data); err != nil {
		return nil, err
	}

	if err := compress.DecompressInto(it.loadBuf.Data, it.indexBuf); err != nil {
		return nil, err
	}

	it.bytesReader.Reset(it.indexBuf.Data)
	return format.ReadIndexChunk(it.bytesReader)
}

func (it *TopicsGroupIterator) loadDataChunk(offset int64, length int64) error {
	if _, err := it.rs.Seek(offset, io.SeekStart); err != nil {
		return err
	}

	if it.isCompressed {
		it.loadBuf.Prepare(int(length))
		if _, err := io.ReadFull(it.rs, it.loadBuf.Data); err != nil {
			return err
		}

		if err := compress.DecompressInto(it.loadBuf.Data, it.messageBuf); err != nil {
			return err
		}
		return nil
	}

	it.messageBuf.Prepare(int(length))
	if _, err := io.ReadFull(it.rs, it.messageBuf.Data); err != nil {
		return err
	}

	return nil
}
