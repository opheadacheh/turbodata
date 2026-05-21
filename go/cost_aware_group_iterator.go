package turbodata

import "io"

// costAwareGroupIterator yields messages from one topic group when the reader
// runs on the cost-aware path. All bytes the group needs have been pre-fetched
// into loadedData, and the per-chunk kept-message lists have been computed at
// setup time, so this type just walks forward (or backward) through them.
type costAwareGroupIterator struct {
	isCompressed bool
	order        Order

	indexChunks   []*IndexChunk
	chunkMessages [][]messageRef // parallel to indexChunks
	chunkMsgLens  [][]int64      // parallel to indexChunks

	loadedData *LoadedBytes

	// Reused across chunks; only allocated when isCompressed.
	decompressBuf *ReusableBuffer

	// Iteration state.
	chunkIdx       int // current chunk being consumed
	msgIdx         int // current message within current chunk
	loadedChunkIdx int // which chunk's data is currently in decompressBuf (-1 = none)
}

func newCostAwareGroupIterator(
	isCompressed bool,
	order Order,
	indexChunks []*IndexChunk,
	chunkMessages [][]messageRef,
	chunkMsgLens [][]int64,
	loadedData *LoadedBytes,
) *costAwareGroupIterator {
	it := &costAwareGroupIterator{
		isCompressed:   isCompressed,
		order:          order,
		indexChunks:    indexChunks,
		chunkMessages:  chunkMessages,
		chunkMsgLens:   chunkMsgLens,
		loadedData:     loadedData,
		loadedChunkIdx: -1,
	}
	if isCompressed {
		it.decompressBuf = NewReusableBuffer()
	}
	if order == ReverseTimeOrder {
		it.chunkIdx = len(indexChunks) - 1
		if it.chunkIdx >= 0 {
			it.msgIdx = len(chunkMessages[it.chunkIdx]) - 1
		}
	} else {
		it.chunkIdx = 0
		if len(indexChunks) > 0 {
			it.msgIdx = 0
		}
	}
	return it
}

// Next returns the next (timestamp, topicId, data) record, or io.EOF when the
// group is exhausted. Returned data aliases loadedData / decompressBuf; the
// caller must copy if it needs to outlive the next call.
func (it *costAwareGroupIterator) Next() (int64, uint16, []byte, error) {
	for {
		if it.chunkIdx < 0 || it.chunkIdx >= len(it.indexChunks) {
			return 0, 0, nil, io.EOF
		}
		msgs := it.chunkMessages[it.chunkIdx]
		if it.msgIdx < 0 || it.msgIdx >= len(msgs) {
			it.advanceChunk()
			continue
		}

		// Materialize this chunk's compressed buffer on first touch.
		if it.isCompressed && it.loadedChunkIdx != it.chunkIdx {
			compressed := it.loadedData.Get(it.indexChunks[it.chunkIdx].ChunkOffset)
			if err := decompressInto(compressed, it.decompressBuf); err != nil {
				return 0, 0, nil, err
			}
			it.loadedChunkIdx = it.chunkIdx
		}

		msg := msgs[it.msgIdx]
		length := it.chunkMsgLens[it.chunkIdx][it.msgIdx]
		chunkOffset := it.indexChunks[it.chunkIdx].ChunkOffset

		var data []byte
		if it.isCompressed {
			end := msg.OffsetInChunk + length
			data = it.decompressBuf.Data[msg.OffsetInChunk:end]
		} else {
			data = it.loadedData.Get(chunkOffset + msg.OffsetInChunk)
		}

		it.advanceMessage()
		return msg.Timestamp, msg.TopicId, data, nil
	}
}

func (it *costAwareGroupIterator) advanceMessage() {
	if it.order == ReverseTimeOrder {
		it.msgIdx--
	} else {
		it.msgIdx++
	}
}

func (it *costAwareGroupIterator) advanceChunk() {
	if it.order == ReverseTimeOrder {
		it.chunkIdx--
		if it.chunkIdx >= 0 {
			it.msgIdx = len(it.chunkMessages[it.chunkIdx]) - 1
		}
	} else {
		it.chunkIdx++
		if it.chunkIdx < len(it.indexChunks) {
			it.msgIdx = 0
		}
	}
}
