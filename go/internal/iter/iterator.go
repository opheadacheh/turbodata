package iter

import (
	"bytes"
	"container/heap"
	"context"
	"io"
	"math"

	"turbodata/format"
	"turbodata/internal/buffer"
	"turbodata/internal/compress"
	"turbodata/internal/iorange"
	"turbodata/readstrategy"
)

// MessageIterator drives reads across all in-scope topic groups, merging them
// into a single timestamp-ordered stream. The exported fields are configuration
// the caller (root package) sets before invoking Prepare; everything else is
// runtime state.
type MessageIterator struct {
	rs ReadSource

	heap   heap.Interface
	loaded bool

	// Configuration set by the caller before Prepare().
	TopicNames     []string
	StartTimestamp int64
	EndTimestamp   int64
	Order          Order
	Summary        *format.Summary
	Strategy       *readstrategy.ReadStrategy // nil = default lazy path
	TailPrefetch   int64
	// VideoDecodable: when true, for any video topic in scope the effective
	// per-group StartTimestamp is snapped backwards to the latest key frame
	// whose timestamp is <= StartTimestamp. Lets the caller hand the
	// resulting message sequence to a decoder cold.
	VideoDecodable bool

	// Default-path runtime state.
	topicsGroupIterators []*TopicsGroupIterator
	topicIdToNames       map[uint16]string

	// Cost-aware runtime state.
	preloadedTopicsGroups []*preloadedTopicsGroupIterator
}

// NewMessageIterator returns an iterator with default configuration. The caller
// is expected to set fields (e.g. TopicNames, Order) and Summary before calling
// Prepare.
func NewMessageIterator(rs ReadSource) *MessageIterator {
	return &MessageIterator{
		rs:             rs,
		Order:          TimeOrder,
		StartTimestamp: 0,
		EndTimestamp:   math.MaxInt64,
		TopicNames:     []string{},
		topicIdToNames: make(map[uint16]string),
	}
}

func (it *MessageIterator) Prepare() error {
	switch it.Order {
	case TimeOrder:
		it.heap = &MessageHeap{}
	case ReverseTimeOrder:
		it.heap = &ReverseMessageHeap{}
	}
	heap.Init(it.heap)

	// Build the set of topic names the caller wants to read, defaulting to all.
	topicNamesMap := make(map[string]struct{})
	if len(it.TopicNames) > 0 {
		for _, topicName := range it.TopicNames {
			topicNamesMap[topicName] = struct{}{}
		}
	} else {
		for _, topicsInfo := range it.Summary.TopicsInfos {
			for _, topicMetadata := range topicsInfo.TopicMetadatas {
				topicNamesMap[topicMetadata.Name] = struct{}{}
			}
		}
	}

	// Translate names to ids; remember the id -> name mapping for NextInto.
	topicIds := make(map[uint16]struct{})
	for _, topicsInfo := range it.Summary.TopicsInfos {
		for _, topicMetadata := range topicsInfo.TopicMetadatas {
			if _, ok := topicNamesMap[topicMetadata.Name]; !ok {
				continue
			}
			topicIds[topicMetadata.Id] = struct{}{}
			it.topicIdToNames[topicMetadata.Id] = topicMetadata.Name
		}
	}

	if it.Strategy != nil {
		return it.prepareCostAware(topicIds, topicNamesMap)
	}

	for _, topicsInfo := range it.Summary.TopicsInfos {
		for _, topicMetadata := range topicsInfo.TopicMetadatas {
			if _, ok := topicNamesMap[topicMetadata.Name]; !ok {
				continue
			}

			groupStart := it.StartTimestamp
			if it.VideoDecodable && isVideoTopicsInfo(topicsInfo) {
				groupStart = snapStartToKeyFrame(topicsInfo, it.StartTimestamp)
			}

			topicsGroupIt := newTopicsGroupIteratorWithStart(it, topicIds, topicsInfo, groupStart)
			if topicsGroupIt != nil {
				it.topicsGroupIterators = append(it.topicsGroupIterators, topicsGroupIt)
			}

			break
		}
	}
	return nil
}

// isVideoTopicsInfo reports whether the (single) topic in this group was
// opened with WithVideoTopic. Video groups always have exactly one topic, so
// inspecting the first topic's metadata is sufficient.
func isVideoTopicsInfo(ti *format.TopicsInfo) bool {
	if len(ti.TopicMetadatas) == 0 {
		return false
	}
	v, _ := ti.TopicMetadatas[0].Metadata["is_video"].(bool)
	return v
}

// snapStartToKeyFrame returns the timestamp of the key frame anchoring the
// GOP that contains origStart. Zero I/O: it walks the in-memory
// IndexChunkInfoList only.
//
// This works because of the writer's GOP-integrity invariant: every chunk for
// a video topic begins with a key frame, and IndexChunkInfo.StartTimestamp is
// the timestamp of that chunk's first message. So the chunk whose
// StartTimestamp is the largest still <= origStart is the GOP that contains
// origStart, and its StartTimestamp is exactly the anchoring key frame's
// timestamp.
//
// If origStart precedes every chunk in the topic, returns origStart unchanged
// (no snap-back possible).
func snapStartToKeyFrame(topicsInfo *format.TopicsInfo, origStart int64) int64 {
	infos := topicsInfo.IndexChunkInfoList
	snap := origStart
	for _, info := range infos {
		if info.StartTimestamp > origStart {
			break
		}
		snap = info.StartTimestamp
	}
	return snap
}

// prepareCostAware sets up the cost-aware reader path: pre-fetch all index
// chunks across all in-scope groups (Phase A), decode + sortAndFilter to learn
// each chunk's kept messages, then pre-fetch all data ranges (Phase B; chunk-
// level for compressed groups, message-level for uncompressed groups), and
// build a preloadedTopicsGroupIterator per group.
func (it *MessageIterator) prepareCostAware(topicIds map[uint16]struct{}, topicNamesMap map[string]struct{}) error {
	ctx := context.Background()
	strategy := *it.Strategy
	fetcher := iorange.NewFetcher(it.rs, strategy.MaxConcurrency)

	// ---- Determine the in-scope groups and per-group filtered index chunks.
	type scopedGroup struct {
		topicsInfo     *format.TopicsInfo
		isCompressed   bool
		startTimestamp int64                    // per-group start (snapped for video)
		filteredInfos  []*format.IndexChunkInfo // chunks within [startTs, endTs]
		filteredInfoLs []int64                  // byte lengths of those index chunks
	}
	scoped := make([]*scopedGroup, 0)
	for _, topicsInfo := range it.Summary.TopicsInfos {
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

		// Per-group start: snapped back to a key frame for video topics when
		// WithVideoDecodable is set; otherwise the iterator's StartTimestamp.
		groupStart := it.StartTimestamp
		if it.VideoDecodable && isVideoTopicsInfo(topicsInfo) {
			groupStart = snapStartToKeyFrame(topicsInfo, it.StartTimestamp)
		}

		// Time-range filter at the chunk level (same logic newTopicsGroupIterator uses).
		filteredInfos := make([]*format.IndexChunkInfo, 0, len(topicsInfo.IndexChunkInfoList))
		filteredLens := make([]int64, 0, len(topicsInfo.IndexChunkInfoList))
		for i, info := range topicsInfo.IndexChunkInfoList {
			if info.EndTimestamp < groupStart {
				continue
			}
			if info.StartTimestamp > it.EndTimestamp {
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
			startTimestamp: groupStart,
			filteredInfos:  filteredInfos,
			filteredInfoLs: filteredLens,
		})
	}

	if len(scoped) == 0 {
		return nil
	}

	// ---- Phase A: plan + fetch all index chunk ranges across all groups.
	var rangesA []iorange.Range
	type rangeAssign struct {
		groupIdx int
		chunkIdx int
	}
	rangeMetaA := make([]rangeAssign, 0)
	for gi, g := range scoped {
		for ci, info := range g.filteredInfos {
			rangesA = append(rangesA, iorange.Range{Offset: info.Offset, Length: g.filteredInfoLs[ci]})
			rangeMetaA = append(rangeMetaA, rangeAssign{groupIdx: gi, chunkIdx: ci})
		}
	}
	// rangesA is offset-sorted by construction: the writer lays out groups
	// in summary order and each group's index chunks in append order, both
	// at monotonically increasing file offsets. Plan requires this.
	opsA, locA := iorange.Plan(rangesA, strategy.CoalesceGap, strategy.SplitThreshold)
	bufsA, err := fetcher.Execute(ctx, opsA)
	if err != nil {
		return err
	}
	loadedIndex := iorange.NewLoadedBytes(rangesA, locA, bufsA)

	// ---- Decode each index chunk; run sortAndFilter to learn kept messages.
	type decodedChunk struct {
		indexChunk  *format.IndexChunk
		messages    []*messageIndexWithTopicId
		messageLens []int64
	}
	perGroupChunks := make([][]decodedChunk, len(scoped))
	// Reused across all index chunks: ReadIndexChunk + sortAndFilter copy
	// every value they need out of the decompressed bytes (no aliasing into
	// the buffer), so a single scratch buffer suffices. The merge scratch is
	// also shared across chunks; output slices are allocated fresh per chunk
	// because they're retained for the iterator's lifetime.
	indexDecompressBuf := buffer.NewReusableBuffer()
	mergeScratch := newSortAndFilterMergeScratch()
	for gi, g := range scoped {
		perGroupChunks[gi] = make([]decodedChunk, len(g.filteredInfos))
		for ci, info := range g.filteredInfos {
			raw := loadedIndex.Get(info.Offset)
			if err := compress.DecompressInto(raw, indexDecompressBuf); err != nil {
				return err
			}
			ic, err := format.ReadIndexChunk(bytes.NewReader(indexDecompressBuf.Data))
			if err != nil {
				return err
			}
			var msgs []*messageIndexWithTopicId
			var lens []int64
			sortAndFilter(
				ic.TopicIndexes,
				ic.UncompressedLen,
				topicIds,
				g.startTimestamp,
				it.EndTimestamp,
				mergeScratch,
				&msgs,
				&lens,
			)
			perGroupChunks[gi][ci] = decodedChunk{indexChunk: ic, messages: msgs, messageLens: lens}
		}
	}

	// ---- Phase B: plan + fetch data ranges, chunk-level for compressed,
	// message-level for uncompressed. We register every kept message as its
	// own Range; Plan coalesces contiguous (and near-contiguous, per
	// strategy.CoalesceGap) messages into single ReadOps, and NewLoadedBytes
	// indexes each message offset individually for direct lookup.
	var rangesB []iorange.Range
	for gi, g := range scoped {
		if g.isCompressed {
			for _, dc := range perGroupChunks[gi] {
				if len(dc.messages) == 0 {
					continue
				}
				rangesB = append(rangesB, iorange.Range{
					Offset: dc.indexChunk.ChunkOffset,
					Length: dc.indexChunk.ChunkLen,
				})
			}
			continue
		}
		for _, dc := range perGroupChunks[gi] {
			for k, msg := range dc.messages {
				rangesB = append(rangesB, iorange.Range{
					Offset: dc.indexChunk.ChunkOffset + msg.messageIndex.OffsetInChunk,
					Length: dc.messageLens[k],
				})
			}
		}
	}
	// rangesB is offset-sorted by construction: data chunks (and messages
	// within them) are appended in writer order, which is increasing file
	// offset. Plan requires this.
	opsB, locB := iorange.Plan(rangesB, strategy.CoalesceGap, strategy.SplitThreshold)
	bufsB, err := fetcher.Execute(ctx, opsB)
	if err != nil {
		return err
	}
	loadedData := iorange.NewLoadedBytes(rangesB, locB, bufsB)

	// ---- Build a preloadedTopicsGroupIterator per scoped group.
	it.preloadedTopicsGroups = make([]*preloadedTopicsGroupIterator, 0, len(scoped))
	for gi, g := range scoped {
		indexChunks := make([]*format.IndexChunk, 0, len(perGroupChunks[gi]))
		chunkMsgs := make([][]*messageIndexWithTopicId, 0, len(perGroupChunks[gi]))
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
		groupIt := newPreloadedTopicsGroupIterator(g.isCompressed, it.Order, indexChunks, chunkMsgs, chunkLens, loadedData)
		it.preloadedTopicsGroups = append(it.preloadedTopicsGroups, groupIt)
	}
	return nil
}

func (it *MessageIterator) NextInto(buf *buffer.ReusableBuffer) (int64, string, error) {
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
	if it.Strategy != nil {
		return it.preloadedTopicsGroups[groupIndex].Next()
	}
	return it.topicsGroupIterators[groupIndex].Next()
}

func (it *MessageIterator) initLoad() error {
	groupCount := len(it.topicsGroupIterators)
	if it.Strategy != nil {
		groupCount = len(it.preloadedTopicsGroups)
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
