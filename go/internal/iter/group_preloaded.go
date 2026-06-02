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

package iter

import (
	"io"

	"github.com/opheadacheh/turbodata/go/format"
	"github.com/opheadacheh/turbodata/go/internal/buffer"
	"github.com/opheadacheh/turbodata/go/internal/compress"
	"github.com/opheadacheh/turbodata/go/internal/iorange"
)

// preloadedTopicsGroupIterator yields messages from one topic group when the reader
// runs on the cost-aware path. All bytes the group needs have been pre-fetched
// into loadedData, and the per-chunk kept-message lists have been computed at
// setup time, so this type just walks forward (or backward) through them.
type preloadedTopicsGroupIterator struct {
	isCompressed bool
	order        Order

	indexChunks   []*format.IndexChunk
	chunkMessages [][]*messageIndexWithTopicId // parallel to indexChunks
	chunkMsgLens  [][]int64                    // parallel to indexChunks

	loadedData *iorange.LoadedBytes

	// Reused across chunks; only allocated when isCompressed.
	decompressBuf *buffer.ReusableBuffer

	// Iteration state.
	chunkIdx       int // current chunk being consumed
	msgIdx         int // current message within current chunk
	loadedChunkIdx int // which chunk's data is currently in decompressBuf (-1 = none)
}

func newPreloadedTopicsGroupIterator(
	isCompressed bool,
	order Order,
	indexChunks []*format.IndexChunk,
	chunkMessages [][]*messageIndexWithTopicId,
	chunkMsgLens [][]int64,
	loadedData *iorange.LoadedBytes,
) *preloadedTopicsGroupIterator {
	it := &preloadedTopicsGroupIterator{
		isCompressed:   isCompressed,
		order:          order,
		indexChunks:    indexChunks,
		chunkMessages:  chunkMessages,
		chunkMsgLens:   chunkMsgLens,
		loadedData:     loadedData,
		loadedChunkIdx: -1,
	}
	if isCompressed {
		it.decompressBuf = buffer.NewReusableBuffer()
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
func (it *preloadedTopicsGroupIterator) Next() (int64, uint16, []byte, error) {
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
			if err := compress.DecompressInto(compressed, it.decompressBuf); err != nil {
				return 0, 0, nil, err
			}
			it.loadedChunkIdx = it.chunkIdx
		}

		msg := msgs[it.msgIdx]
		length := it.chunkMsgLens[it.chunkIdx][it.msgIdx]
		chunkOffset := it.indexChunks[it.chunkIdx].ChunkOffset

		var data []byte
		if it.isCompressed {
			end := msg.messageIndex.OffsetInChunk + length
			data = it.decompressBuf.Data[msg.messageIndex.OffsetInChunk:end]
		} else {
			data = it.loadedData.Get(chunkOffset + msg.messageIndex.OffsetInChunk)
		}

		it.advanceMessage()
		return msg.messageIndex.Timestamp, msg.topicId, data, nil
	}
}

func (it *preloadedTopicsGroupIterator) advanceMessage() {
	if it.order == ReverseTimeOrder {
		it.msgIdx--
	} else {
		it.msgIdx++
	}
}

func (it *preloadedTopicsGroupIterator) advanceChunk() {
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
