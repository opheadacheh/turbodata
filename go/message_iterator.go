package turbodata

import (
	"bytes"
	"container/heap"
	"context"
	"io"
	"math"
)

type MessageIterator struct {
	rs ReadSource

	heap   heap.Interface
	loaded bool

	topicNames     []string
	startTimestamp int64
	endTimestamp   int64
	order          Order
	summary        *Summary

	// Default-path state.
	topicsGroupIterators []*TopicsGroupIterator
	topicIdToNames       map[uint16]string

	// Set via ReadOptions.
	strategy     *ReadStrategy // nil = default lazy path
	tailPrefetch int64

	// Cost-aware path state (only populated when strategy != nil).
	costAwareGroups []*costAwareGroupIterator
}

func newMessageIterator(rs ReadSource, summary *Summary) *MessageIterator {
	return &MessageIterator{
		rs:             rs,
		summary:        summary,
		order:          TimeOrder,
		startTimestamp: 0,
		endTimestamp:   math.MaxInt64,
		topicNames:     []string{},
		topicIdToNames: make(map[uint16]string),
	}
}

func (it *MessageIterator) prepare() error {
	switch it.order {
	case TimeOrder:
		it.heap = &MessageHeap{}
	case ReverseTimeOrder:
		it.heap = &ReverseMessageHeap{}
	}
	heap.Init(it.heap)

	// Build the set of topic names the caller wants to read, defaulting to all.
	topicNamesMap := make(map[string]struct{})
	if len(it.topicNames) > 0 {
		for _, topicName := range it.topicNames {
			topicNamesMap[topicName] = struct{}{}
		}
	} else {
		for _, topicsInfo := range it.summary.TopicsInfos {
			for _, topicMetadata := range topicsInfo.TopicMetadatas {
				topicNamesMap[topicMetadata.Name] = struct{}{}
			}
		}
	}

	// Translate names to ids; remember the id -> name mapping for NextInto.
	topicIds := make(map[uint16]struct{})
	for _, topicsInfo := range it.summary.TopicsInfos {
		for _, topicMetadata := range topicsInfo.TopicMetadatas {
			if _, ok := topicNamesMap[topicMetadata.Name]; !ok {
				continue
			}
			topicIds[topicMetadata.Id] = struct{}{}
			it.topicIdToNames[topicMetadata.Id] = topicMetadata.Name
		}
	}

	if it.strategy != nil {
		return it.prepareCostAware(topicIds, topicNamesMap)
	}

	for _, topicsInfo := range it.summary.TopicsInfos {
		for _, topicMetadata := range topicsInfo.TopicMetadatas {
			if _, ok := topicNamesMap[topicMetadata.Name]; !ok {
				continue
			}

			topicsGroupIt := newTopicsGroupIterator(it, topicIds, topicsInfo)
			if topicsGroupIt != nil {
				it.topicsGroupIterators = append(it.topicsGroupIterators, topicsGroupIt)
			}

			break
		}
	}
	return nil
}

// prepareCostAware sets up the cost-aware reader path: pre-fetch all index
// chunks across all in-scope groups (Phase A), decode + sortAndFilter to learn
// each chunk's kept messages, then pre-fetch all data ranges (Phase B; chunk-
// level for compressed groups, message-level for uncompressed groups), and
// build a costAwareGroupIterator per group.
func (it *MessageIterator) prepareCostAware(topicIds map[uint16]struct{}, topicNamesMap map[string]struct{}) error {
	ctx := context.Background()
	strategy := *it.strategy
	fetcher := NewFetcher(it.rs, strategy.MaxConcurrency)

	// ---- Determine the in-scope groups and per-group filtered index chunks.
	type scopedGroup struct {
		topicsInfo     *TopicsInfo
		isCompressed   bool
		filteredInfos  []*IndexChunkInfo // chunks within [startTs, endTs]
		filteredInfoLs []int64           // byte lengths of those index chunks
	}
	scoped := make([]*scopedGroup, 0)
	for _, topicsInfo := range it.summary.TopicsInfos {
		// Skip groups that don't have any topic the caller cares about.
		anyMatch := false
		for _, tm := range topicsInfo.TopicMetadatas {
			if _, ok := topicNamesMap[tm.Name]; ok {
				anyMatch = true
				break
			}
		}
		if !anyMatch {
			continue
		}

		isCompressed, _ := topicsInfo.TopicMetadatas[0].Metadata["is_compressed"].(bool)

		// Time-range filter at the chunk level (same logic newTopicsGroupIterator uses).
		filteredInfos := make([]*IndexChunkInfo, 0, len(topicsInfo.IndexChunkInfoList))
		filteredLens := make([]int64, 0, len(topicsInfo.IndexChunkInfoList))
		for i, info := range topicsInfo.IndexChunkInfoList {
			if info.EndTimestamp < it.startTimestamp {
				continue
			}
			if info.StartTimestamp > it.endTimestamp {
				break
			}
			filteredInfos = append(filteredInfos, info)

			// Length of this compressed index chunk: distance to next chunk, or wrap to first chunk for last.
			var ln int64
			if i < len(topicsInfo.IndexChunkInfoList)-1 {
				ln = topicsInfo.IndexChunkInfoList[i+1].Offset - info.Offset
			} else {
				ln = topicsInfo.TotalLen - info.Offset + topicsInfo.IndexChunkInfoList[0].Offset
			}
			filteredLens = append(filteredLens, ln)
		}
		if len(filteredInfos) == 0 {
			continue
		}
		scoped = append(scoped, &scopedGroup{
			topicsInfo:     topicsInfo,
			isCompressed:   isCompressed,
			filteredInfos:  filteredInfos,
			filteredInfoLs: filteredLens,
		})
	}

	if len(scoped) == 0 {
		return nil
	}

	// ---- Phase A: plan + fetch all index chunk ranges across all groups.
	var rangesA []Range
	type rangeAssign struct {
		groupIdx int
		chunkIdx int
	}
	rangeMetaA := make([]rangeAssign, 0)
	for gi, g := range scoped {
		for ci, info := range g.filteredInfos {
			rangesA = append(rangesA, Range{Offset: info.Offset, Length: g.filteredInfoLs[ci]})
			rangeMetaA = append(rangeMetaA, rangeAssign{groupIdx: gi, chunkIdx: ci})
		}
	}
	opsA, locA := Plan(rangesA, strategy)
	bufsA, err := fetcher.Execute(ctx, opsA)
	if err != nil {
		return err
	}
	loadedIndex := NewLoadedBytes(rangesA, locA, bufsA)

	// ---- Decode each index chunk; run sortAndFilter to learn kept messages.
	type decodedChunk struct {
		indexChunk    *IndexChunk
		messages      []messageRef
		messageLens   []int64
	}
	perGroupChunks := make([][]decodedChunk, len(scoped))
	for gi, g := range scoped {
		perGroupChunks[gi] = make([]decodedChunk, len(g.filteredInfos))
		for ci, info := range g.filteredInfos {
			raw := loadedIndex.Get(info.Offset)
			decompressed, err := decompress(raw)
			if err != nil {
				return err
			}
			ic, err := ReadIndexChunk(bytes.NewReader(decompressed))
			if err != nil {
				return err
			}
			msgs, lens := sortAndFilter(ic.TopicIndexes, ic.UncompressedLen, topicIds, it.startTimestamp, it.endTimestamp)
			perGroupChunks[gi][ci] = decodedChunk{indexChunk: ic, messages: msgs, messageLens: lens}
		}
	}

	// ---- Phase B: plan + fetch data ranges, chunk-level for compressed,
	// message-level for uncompressed (with on-the-fly contiguous merging).
	// For each message we register both its run and its individual offset,
	// so the iterator can look up the message directly.
	type msgRunRef struct {
		msgOffset int64
		msgLen    int64
		runIdx    int   // index into rangesB
		inRunOff  int64 // byte offset within the run
	}
	var rangesB []Range
	var msgRefs []msgRunRef // populated for uncompressed groups
	for gi, g := range scoped {
		if g.isCompressed {
			for _, dc := range perGroupChunks[gi] {
				if len(dc.messages) == 0 {
					continue
				}
				rangesB = append(rangesB, Range{
					Offset: dc.indexChunk.ChunkOffset,
					Length: dc.indexChunk.ChunkLen,
				})
			}
			continue
		}
		// Uncompressed: emit one range per contiguous run of kept messages,
		// recording each message's location inside its run for later lookup
		// in the LoadedBytes index.
		for _, dc := range perGroupChunks[gi] {
			if len(dc.messages) == 0 {
				continue
			}
			runStart := dc.indexChunk.ChunkOffset + dc.messages[0].OffsetInChunk
			runEnd := runStart + dc.messageLens[0]
			runIdx := len(rangesB)
			msgRefs = append(msgRefs, msgRunRef{
				msgOffset: runStart,
				msgLen:    dc.messageLens[0],
				runIdx:    runIdx,
				inRunOff:  0,
			})
			for k := 1; k < len(dc.messages); k++ {
				msgStart := dc.indexChunk.ChunkOffset + dc.messages[k].OffsetInChunk
				if msgStart == runEnd {
					// Same run; record this message's location within it.
					msgRefs = append(msgRefs, msgRunRef{
						msgOffset: msgStart,
						msgLen:    dc.messageLens[k],
						runIdx:    runIdx,
						inRunOff:  msgStart - runStart,
					})
					runEnd += dc.messageLens[k]
					continue
				}
				// Close the current run, start a new one.
				rangesB = append(rangesB, Range{Offset: runStart, Length: runEnd - runStart})
				runStart = msgStart
				runEnd = runStart + dc.messageLens[k]
				runIdx = len(rangesB)
				msgRefs = append(msgRefs, msgRunRef{
					msgOffset: msgStart,
					msgLen:    dc.messageLens[k],
					runIdx:    runIdx,
					inRunOff:  0,
				})
			}
			rangesB = append(rangesB, Range{Offset: runStart, Length: runEnd - runStart})
		}
	}
	opsB, locB := Plan(rangesB, strategy)
	bufsB, err := fetcher.Execute(ctx, opsB)
	if err != nil {
		return err
	}
	loadedData := NewLoadedBytes(rangesB, locB, bufsB)

	// For uncompressed groups: widen the LoadedBytes index so each kept
	// message has its own (offset → bytes) entry, sub-located inside its run.
	for _, mr := range msgRefs {
		if _, ok := loadedData.index[mr.msgOffset]; ok {
			// First message of each run already gets the run's entry, which
			// has the correct length only by coincidence when the run is one
			// message. Override unconditionally to ensure length matches the
			// individual message.
			runLoc := locB[mr.runIdx]
			loadedData.index[mr.msgOffset] = bytesLocation{
				bufferIdx: runLoc.OpIndex,
				inBufOff:  runLoc.InOpOff + int(mr.inRunOff),
				length:    int(mr.msgLen),
			}
			continue
		}
		runLoc := locB[mr.runIdx]
		loadedData.index[mr.msgOffset] = bytesLocation{
			bufferIdx: runLoc.OpIndex,
			inBufOff:  runLoc.InOpOff + int(mr.inRunOff),
			length:    int(mr.msgLen),
		}
	}

	// ---- Build a costAwareGroupIterator per scoped group.
	it.costAwareGroups = make([]*costAwareGroupIterator, 0, len(scoped))
	for gi, g := range scoped {
		indexChunks := make([]*IndexChunk, 0, len(perGroupChunks[gi]))
		chunkMsgs := make([][]messageRef, 0, len(perGroupChunks[gi]))
		chunkLens := make([][]int64, 0, len(perGroupChunks[gi]))
		for _, dc := range perGroupChunks[gi] {
			if len(dc.messages) == 0 {
				continue
			}
			indexChunks = append(indexChunks, dc.indexChunk)
			chunkMsgs = append(chunkMsgs, dc.messages)
			chunkLens = append(chunkLens, dc.messageLens)
		}
		if len(indexChunks) == 0 {
			continue
		}
		groupIt := newCostAwareGroupIterator(g.isCompressed, it.order, indexChunks, chunkMsgs, chunkLens, loadedData)
		it.costAwareGroups = append(it.costAwareGroups, groupIt)
	}
	return nil
}

func (it *MessageIterator) NextInto(buf *ReusableBuffer) (int64, string, error) {
	if !it.loaded {
		if err := it.initLoad(); err != nil {
			return 0, "", err
		}
	}
	it.loaded = true

	if it.heap.Len() == 0 {
		return 0, "", io.EOF
	}

	item, _ := heap.Pop(it.heap).(*message)
	buf.Prepare(len(item.data))
	copy(buf.Data, item.data)

	retTimestamp := item.timestamp
	retTopicId := item.topicId

	timestamp, topicId, data, err := it.groupNext(item.groupIndex)
	if err == nil {
		item.timestamp = timestamp
		item.topicId = topicId
		item.data = data
		heap.Push(it.heap, item)
	}
	if err != io.EOF && err != nil {
		return 0, "", err
	}

	return retTimestamp, it.topicIdToNames[retTopicId], nil
}

func (it *MessageIterator) groupNext(groupIndex int) (int64, uint16, []byte, error) {
	if it.strategy != nil {
		return it.costAwareGroups[groupIndex].Next()
	}
	return it.topicsGroupIterators[groupIndex].Next()
}

func (it *MessageIterator) initLoad() error {
	groupCount := len(it.topicsGroupIterators)
	if it.strategy != nil {
		groupCount = len(it.costAwareGroups)
	}
	for i := 0; i < groupCount; i++ {
		timestamp, topicId, data, err := it.groupNext(i)
		if err == io.EOF {
			continue
		}
		if err != nil {
			return err
		}
		heap.Push(it.heap, &message{timestamp: timestamp, topicId: topicId, data: data, groupIndex: i})
	}
	return nil
}
