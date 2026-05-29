package iter

import (
	"bytes"
	"context"
	"fmt"
	"sort"

	"turbodata/format"
	"turbodata/internal/buffer"
	"turbodata/internal/compress"
	"turbodata/internal/iorange"
	"turbodata/readstrategy"
)

// SampleSpec is one engine-level sampling query: ask for the floor message of
// Topic at each timestamp in Timestamps.
//
// The engine assumes Timestamps is strictly increasing, that Topic is unique
// across all SampleSpecs in a single Sample call, and that Topic exists in
// the summary. The public turbodata.Reader.Sample enforces these.
type SampleSpec struct {
	Topic      string
	Timestamps []int64
}

// SampleHit is the engine-level result for one (spec, timestamp): the floor
// message bytes, or Found=false when no message with ts <= the requested T
// exists for the topic. Data is an owned copy; safe to retain.
//
// For video topics, Frames and ResetDecoder carry GOP-prefix semantics
// described on the public turbodata.SampleResult.
type SampleHit struct {
	Found        bool
	Timestamp    int64
	Data         []byte
	IsVideo      bool
	Frames       []SampleFrame
	ResetDecoder bool
}

// SampleFrame is the engine-level equivalent of turbodata.Frame. Kept
// separate to avoid an import cycle between the public turbodata package and
// the internal iter package.
type SampleFrame struct {
	Timestamp  int64
	IsKeyFrame bool
	Data       []byte
}

// Sample resolves floor messages for the given specs in two concurrent I/O
// waves and returns hits in the same shape as the input: out[i][j]
// corresponds to specs[i].Timestamps[j].
func Sample(rs ReadSource, summary *format.Summary, specs []SampleSpec, strategy readstrategy.ReadStrategy, videoDecodable bool) ([][]SampleHit, error) {
	if len(specs) == 0 {
		return nil, nil
	}
	e := newSampleEngine(rs, summary, specs, strategy, videoDecodable)
	e.assignInitialCandidates()
	if err := e.runPhaseA(); err != nil {
		return nil, err
	}
	if err := e.runPhaseB(); err != nil {
		return nil, err
	}
	return e.collect(), nil
}

type chunkKey struct {
	group int
	chunk int
}

type pendingItem struct {
	tIdx     int // index in queryState.spec.Timestamps
	chunkIdx int // current candidate chunk index in the query's group
}

type resolvedItem struct {
	tIdx          int
	chunkKey      chunkKey
	msgOffInChunk int64
	msgLen        int64
	timestamp     int64

	// Video-only: positions into the topic's MessageIndexes in the cached
	// IndexChunk. keyframeMsgIdx is the anchor key frame for the GOP that
	// contains the target; targetMsgIdx is the target's position. Both are
	// set by resolveItemsInChunk for video queries; unused otherwise.
	keyframeMsgIdx int
	targetMsgIdx   int
}

type queryState struct {
	spec         SampleSpec
	groupIdx     int
	topicId      uint16
	isCompressed bool
	isVideo      bool
	results      []SampleHit
	pending      []pendingItem
	resolved     []resolvedItem
}

// chunkCache holds a decoded index chunk plus per-topic message lengths
// (computed once when the chunk is first decoded; messages from different
// topics within the chunk are interleaved by offset, so per-topic lengths
// can't be computed locally from a single topic's MessageIndexes).
type chunkCache struct {
	ic           *format.IndexChunk
	perTopicLens map[uint16][]int64
}

type sampleEngine struct {
	rs       ReadSource
	summary  *format.Summary
	strategy readstrategy.ReadStrategy
	fetcher  *iorange.Fetcher
	ctx      context.Context

	queryStates    []*queryState
	chunks         map[chunkKey]*chunkCache // decoded across all Phase A waves
	videoDecodable bool                     // gates GOP-prefix treatment of video topics
}

func newSampleEngine(rs ReadSource, summary *format.Summary, specs []SampleSpec, strategy readstrategy.ReadStrategy, videoDecodable bool) *sampleEngine {
	e := &sampleEngine{
		rs:             rs,
		summary:        summary,
		strategy:       strategy,
		fetcher:        iorange.NewFetcher(rs, strategy.MaxConcurrency),
		ctx:            context.Background(),
		chunks:         make(map[chunkKey]*chunkCache),
		videoDecodable: videoDecodable,
	}
	e.queryStates = make([]*queryState, len(specs))
	for i, spec := range specs {
		qs := &queryState{
			spec:    spec,
			results: make([]SampleHit, len(spec.Timestamps)),
		}
		e.resolveTopic(qs)
		// Video decoding is opt-in. Without it, a video topic is sampled like
		// any other topic (its floor frame is returned in Data), so clear the
		// flag that drives the GOP-prefix path.
		qs.isVideo = qs.isVideo && e.videoDecodable
		e.queryStates[i] = qs
	}
	return e
}

// resolveTopic locates the topic's group index and topic id in the summary.
// Assumes the public layer validated that the topic exists; if a misuse slips
// through it leaves zero values, which produces empty results downstream.
func (e *sampleEngine) resolveTopic(qs *queryState) {
	for gi, ti := range e.summary.TopicsInfos {
		for _, tm := range ti.TopicMetadatas {
			if tm.Name == qs.spec.Topic {
				qs.groupIdx = gi
				qs.topicId = tm.Id
				compressed, _ := ti.TopicMetadatas[0].Metadata["is_compressed"].(bool)
				qs.isCompressed = compressed
				video, _ := ti.TopicMetadatas[0].Metadata["is_video"].(bool)
				qs.isVideo = video
				return
			}
		}
	}
}

// assignInitialCandidates walks each query's strictly-increasing Timestamps
// alongside its group's IndexChunkInfoList with a single forward chunk cursor.
// For each T, the candidate chunk is the one with the largest StartTimestamp
// still <= T. T values that precede the group's first chunk's StartTimestamp
// are marked Found=false immediately.
func (e *sampleEngine) assignInitialCandidates() {
	for _, qs := range e.queryStates {
		chunks := e.summary.TopicsInfos[qs.groupIdx].IndexChunkInfoList
		if len(chunks) == 0 {
			continue
		}
		c := -1
		for t, T := range qs.spec.Timestamps {
			for c+1 < len(chunks) && chunks[c+1].StartTimestamp <= T {
				c++
			}
			if c < 0 {
				continue
			}
			qs.pending = append(qs.pending, pendingItem{tIdx: t, chunkIdx: c})
		}
	}
}

// runPhaseA loops: fetch any not-yet-cached candidate chunks concurrently,
// decode them, walk each query's pending items (already T-sorted) against
// the candidate chunk's topic MessageIndexes with a single forward cursor,
// recording resolved items and re-queuing fallbacks against chunk c-1 when
// the candidate doesn't actually have a message of the topic with ts <= T.
// Terminates when no pending items remain. Wave count is bounded by chunk
// count.
func (e *sampleEngine) runPhaseA() error {
	decompBuf := buffer.NewReusableBuffer()
	for {
		if allPendingEmpty(e.queryStates) {
			return nil
		}

		needs := e.gatherChunkNeeds()
		if len(needs) > 0 {
			if err := e.fetchAndDecodeIndexChunks(needs, decompBuf); err != nil {
				return err
			}
		}

		anyProgress := false
		for _, qs := range e.queryStates {
			if len(qs.pending) == 0 {
				continue
			}
			var stillPending []pendingItem
			i := 0
			for i < len(qs.pending) {
			j := i + 1
			for j < len(qs.pending) && qs.pending[j].chunkIdx == qs.pending[i].chunkIdx {
				j++
			}
				c := qs.pending[i].chunkIdx
				cache, ok := e.chunks[chunkKey{qs.groupIdx, c}]
				if !ok {
					stillPending = append(stillPending, qs.pending[i:j]...)
					i = j
					continue
				}
				anyProgress = true
				stillPending = append(stillPending, e.resolveItemsInChunk(qs, c, cache, qs.pending[i:j])...)
				i = j
			}
			qs.pending = stillPending
		}

		if !anyProgress {
			// All pending items reference chunks that weren't fetched this
			// wave AND weren't already cached. gatherChunkNeeds should have
			// surfaced them, so this would indicate an internal bug.
			return fmt.Errorf("sample: phase A made no progress with %d items still pending", countPending(e.queryStates))
		}
	}
}

type chunkNeed struct {
	key    chunkKey
	offset int64
	length int64
}

func (e *sampleEngine) gatherChunkNeeds() []chunkNeed {
	seen := make(map[chunkKey]struct{})
	var needs []chunkNeed
	for _, qs := range e.queryStates {
		for _, p := range qs.pending {
			k := chunkKey{qs.groupIdx, p.chunkIdx}
			if _, ok := e.chunks[k]; ok {
				continue
			}
			if _, ok := seen[k]; ok {
				continue
			}
			seen[k] = struct{}{}
			ti := e.summary.TopicsInfos[k.group]
			info := ti.IndexChunkInfoList[k.chunk]
			var ln int64
			if k.chunk < len(ti.IndexChunkInfoList)-1 {
				ln = ti.IndexChunkInfoList[k.chunk+1].Offset - info.Offset
			} else {
				ln = ti.TotalLen - info.Offset + ti.IndexChunkInfoList[0].Offset
			}
			needs = append(needs, chunkNeed{key: k, offset: info.Offset, length: ln})
		}
	}
	sort.Slice(needs, func(i, j int) bool { return needs[i].offset < needs[j].offset })
	return needs
}

func (e *sampleEngine) fetchAndDecodeIndexChunks(needs []chunkNeed, decompBuf *buffer.ReusableBuffer) error {
	ranges := make([]iorange.Range, len(needs))
	for i, n := range needs {
		ranges[i] = iorange.Range{Offset: n.offset, Length: n.length}
	}
	ops, locs := iorange.Plan(ranges, e.strategy.CoalesceGap, e.strategy.SplitThreshold)
	bufs, err := e.fetcher.Execute(e.ctx, ops)
	if err != nil {
		return err
	}
	loaded := iorange.NewLoadedBytes(ranges, locs, bufs)
	for _, n := range needs {
		raw := loaded.Get(n.offset)
		if err := compress.DecompressInto(raw, decompBuf); err != nil {
			return err
		}
		ic, err := format.ReadIndexChunk(bytes.NewReader(decompBuf.Data))
		if err != nil {
			return err
		}
		e.chunks[n.key] = buildChunkCache(ic)
	}
	return nil
}

// resolveItemsInChunk processes a contiguous run of pending items that all
// share the same candidate chunk index c. Items are in strictly-increasing T
// order, so a single forward cursor over the topic's MessageIndexes assigns
// each item its floor. Items the cursor can't satisfy (topic absent or its
// first message in this chunk is > T) become fallback entries against c-1.
func (e *sampleEngine) resolveItemsInChunk(qs *queryState, c int, cache *chunkCache, items []pendingItem) []pendingItem {
	var fallback []pendingItem
	ti := findTopicIndex(cache.ic, qs.topicId)
	if ti == nil || len(ti.MessageIndexes) == 0 {
		for _, p := range items {
			if c == 0 {
				qs.results[p.tIdx].Found = false
			} else {
				fallback = append(fallback, pendingItem{tIdx: p.tIdx, chunkIdx: c - 1})
			}
		}
		return fallback
	}
	mis := ti.MessageIndexes
	lens := cache.perTopicLens[qs.topicId]
	// For video, kf tracks the most recent key frame index <= k. The writer's
	// GOP-integrity rule ensures every chunk for a video topic begins with a
	// key frame (KeyFrameIndexes[0] == 0), so kf is always findable when k is.
	kf := 0
	kfPos := 0 // position within ti.KeyFrameIndexes corresponding to kf
	k := 0
	for _, p := range items {
		T := qs.spec.Timestamps[p.tIdx]
		for k+1 < len(mis) && mis[k+1].Timestamp <= T {
			k++
		}
		if mis[k].Timestamp > T {
			if c == 0 {
				qs.results[p.tIdx].Found = false
			} else {
				fallback = append(fallback, pendingItem{tIdx: p.tIdx, chunkIdx: c - 1})
			}
			continue
		}
		item := resolvedItem{
			tIdx:          p.tIdx,
			chunkKey:      chunkKey{qs.groupIdx, c},
			msgOffInChunk: mis[k].OffsetInChunk,
			msgLen:        lens[k],
			timestamp:     mis[k].Timestamp,
		}
		if qs.isVideo {
			kfs := ti.KeyFrameIndexes
			if len(kfs) == 0 || int(kfs[0]) > k {
				// No anchor key frame for this target in this chunk: violates
				// the writer's GOP-integrity invariant. Treat as not found
				// rather than emit an undecodable single frame.
				qs.results[p.tIdx].Found = false
				continue
			}
			for kfPos+1 < len(kfs) && int(kfs[kfPos+1]) <= k {
				kfPos++
			}
			kf = int(kfs[kfPos])
			item.keyframeMsgIdx = kf
			item.targetMsgIdx = k
		}
		qs.resolved = append(qs.resolved, item)
	}
	return fallback
}

func findTopicIndex(ic *format.IndexChunk, topicId uint16) *format.TopicIndex {
	for _, ti := range ic.TopicIndexes {
		if ti.Id == topicId {
			return ti
		}
	}
	return nil
}

// buildChunkCache flattens all topics' MessageIndexes into one offset-sorted
// list to derive per-message lengths (next message's OffsetInChunk minus
// this one's, with UncompressedLen closing the last), then projects the
// result back into per-topic length slices that align with each topic's
// MessageIndexes.
func buildChunkCache(ic *format.IndexChunk) *chunkCache {
	type item struct {
		topicId uint16
		offset  int64
		idx     int
	}
	total := 0
	for _, ti := range ic.TopicIndexes {
		total += len(ti.MessageIndexes)
	}
	items := make([]item, 0, total)
	for _, ti := range ic.TopicIndexes {
		for i, mi := range ti.MessageIndexes {
			items = append(items, item{topicId: ti.Id, offset: mi.OffsetInChunk, idx: i})
		}
	}
	sort.Slice(items, func(i, j int) bool { return items[i].offset < items[j].offset })

	perTopicLens := make(map[uint16][]int64, len(ic.TopicIndexes))
	for _, ti := range ic.TopicIndexes {
		perTopicLens[ti.Id] = make([]int64, len(ti.MessageIndexes))
	}
	for i := 0; i < len(items)-1; i++ {
		perTopicLens[items[i].topicId][items[i].idx] = items[i+1].offset - items[i].offset
	}
	if len(items) > 0 {
		last := items[len(items)-1]
		perTopicLens[last.topicId][last.idx] = ic.UncompressedLen - last.offset
	}
	return &chunkCache{ic: ic, perTopicLens: perTopicLens}
}

func allPendingEmpty(states []*queryState) bool {
	for _, qs := range states {
		if len(qs.pending) > 0 {
			return false
		}
	}
	return true
}

func countPending(states []*queryState) int {
	n := 0
	for _, qs := range states {
		n += len(qs.pending)
	}
	return n
}

// runPhaseB issues data reads for every resolved item in one concurrent wave
// and copies the message bytes into the result slots. Compressed groups
// register one Range per unique chunk (whole compressed data chunk, slicing
// out individual messages after a single decompression). Uncompressed
// non-video groups register one Range per message; iorange.Plan coalesces
// neighbors per the strategy. Video groups register one Range per resolved
// item that spans the GOP prefix [keyframe..target], so iorange.Plan can
// coalesce adjacent prefixes and the materialization step can build
// incremental Frames slices keyed off the prefix boundaries.
func (e *sampleEngine) runPhaseB() error {
	seenChunk := make(map[chunkKey]struct{})
	// Uncompressed non-video: dedupe per absolute message offset. Multiple
	// query timestamps in one spec can floor to the same message; without
	// this each would register an identical Range, and iorange.Plan turns
	// exact duplicates into separate (negative-gap) ops, i.e. redundant
	// reads of the same bytes. The offset is globally unique per message.
	seenMsg := make(map[int64]struct{})

	// Video: dedupe per (chunk, keyframe). All queries that share a GOP
	// register a single range from the keyframe to the FURTHEST target
	// across those queries. Reads aren't duplicated; shorter-target queries
	// just slice less of the same loaded bytes.
	type videoKey struct {
		chunkKey       chunkKey
		keyframeMsgIdx int
	}
	type videoExtent struct {
		topicId        uint16
		startInChunk   int64
		endInChunkExcl int64
	}
	videoExtents := make(map[videoKey]*videoExtent)

	var ranges []iorange.Range
	for _, qs := range e.queryStates {
		for _, r := range qs.resolved {
			ic := e.chunks[r.chunkKey].ic
			switch {
			case qs.isCompressed:
				if _, dup := seenChunk[r.chunkKey]; dup {
					continue
				}
				seenChunk[r.chunkKey] = struct{}{}
				ranges = append(ranges, iorange.Range{Offset: ic.ChunkOffset, Length: ic.ChunkLen})
			case qs.isVideo:
				cc := e.chunks[r.chunkKey]
				// Video groups are single-topic by writer invariant
				// (ErrVideoGroupMustBeSingleTopic), so the sole topic index
				// is TopicIndexes[0].
				ti := cc.ic.TopicIndexes[0]
				mis := ti.MessageIndexes
				lens := cc.perTopicLens[qs.topicId]
				start := mis[r.keyframeMsgIdx].OffsetInChunk
				endExcl := mis[r.targetMsgIdx].OffsetInChunk + lens[r.targetMsgIdx]
				vk := videoKey{r.chunkKey, r.keyframeMsgIdx}
				if ve, ok := videoExtents[vk]; ok {
					if endExcl > ve.endInChunkExcl {
						ve.endInChunkExcl = endExcl
					}
				} else {
					videoExtents[vk] = &videoExtent{
						topicId:        qs.topicId,
						startInChunk:   start,
						endInChunkExcl: endExcl,
					}
				}
			default:
				off := ic.ChunkOffset + r.msgOffInChunk
				if _, dup := seenMsg[off]; dup {
					continue
				}
				seenMsg[off] = struct{}{}
				ranges = append(ranges, iorange.Range{Offset: off, Length: r.msgLen})
			}
		}
	}
	for vk, ve := range videoExtents {
		ic := e.chunks[vk.chunkKey].ic
		ranges = append(ranges, iorange.Range{
			Offset: ic.ChunkOffset + ve.startInChunk,
			Length: ve.endInChunkExcl - ve.startInChunk,
		})
	}
	if len(ranges) == 0 {
		return nil
	}
	sort.Slice(ranges, func(i, j int) bool { return ranges[i].Offset < ranges[j].Offset })

	ops, locs := iorange.Plan(ranges, e.strategy.CoalesceGap, e.strategy.SplitThreshold)
	bufs, err := e.fetcher.Execute(e.ctx, ops)
	if err != nil {
		return err
	}
	loaded := iorange.NewLoadedBytes(ranges, locs, bufs)

	// ---- Compressed (non-video) materialization: per-chunk one-time decompress.
	type byChunkItem struct {
		qs *queryState
		r  resolvedItem
	}
	byChunk := make(map[chunkKey][]byChunkItem)
	for _, qs := range e.queryStates {
		if !qs.isCompressed {
			continue
		}
		for _, r := range qs.resolved {
			byChunk[r.chunkKey] = append(byChunk[r.chunkKey], byChunkItem{qs: qs, r: r})
		}
	}
	decompBuf := buffer.NewReusableBuffer()
	for ck, items := range byChunk {
		ic := e.chunks[ck].ic
		compressed := loaded.Get(ic.ChunkOffset)
		if err := compress.DecompressInto(compressed, decompBuf); err != nil {
			return err
		}
		for _, it := range items {
			data := make([]byte, it.r.msgLen)
			copy(data, decompBuf.Data[it.r.msgOffInChunk:it.r.msgOffInChunk+it.r.msgLen])
			it.qs.results[it.r.tIdx].Found = true
			it.qs.results[it.r.tIdx].Timestamp = it.r.timestamp
			it.qs.results[it.r.tIdx].Data = data
		}
	}

	// ---- Uncompressed non-video materialization: per-message copy.
	for _, qs := range e.queryStates {
		if qs.isCompressed || qs.isVideo {
			continue
		}
		for _, r := range qs.resolved {
			ic := e.chunks[r.chunkKey].ic
			raw := loaded.Get(ic.ChunkOffset + r.msgOffInChunk)
			data := make([]byte, len(raw))
			copy(data, raw)
			qs.results[r.tIdx].Found = true
			qs.results[r.tIdx].Timestamp = r.timestamp
			qs.results[r.tIdx].Data = data
		}
	}

	// ---- Video materialization: incremental Frames + ResetDecoder.
	for _, qs := range e.queryStates {
		if !qs.isVideo {
			continue
		}
		e.materializeVideo(qs, loaded)
	}
	return nil
}

// materializeVideo walks one query's resolved items in target-timestamp order
// and emits incremental Frames + ResetDecoder per the public contract on
// SampleResult. See sampler.go for the user-facing semantics.
func (e *sampleEngine) materializeVideo(qs *queryState, loaded *iorange.LoadedBytes) {
	if len(qs.resolved) == 0 {
		return
	}
	// Resolved items can arrive out of tIdx order if Phase A required
	// fallbacks. Sort by tIdx so the row's natural processing order matches
	// the caller's expectation.
	resolved := make([]resolvedItem, len(qs.resolved))
	copy(resolved, qs.resolved)
	sort.Slice(resolved, func(i, j int) bool { return resolved[i].tIdx < resolved[j].tIdx })

	// Decoder state continuity is per-row, scoped to a single GOP (= same
	// chunkKey + same keyframeMsgIdx). Track the previous found item's GOP
	// and target so consecutive same-GOP items emit only the new tail frames.
	var (
		havePrev         bool
		prevChunk        chunkKey
		prevKeyframeIdx  int
		prevTargetMsgIdx int
	)

	for _, r := range resolved {
		cc := e.chunks[r.chunkKey]
		ic := cc.ic
		// Video groups are single-topic by writer invariant
		// (ErrVideoGroupMustBeSingleTopic), so the sole topic index is
		// TopicIndexes[0].
		ti := ic.TopicIndexes[0]
		mis := ti.MessageIndexes
		lens := cc.perTopicLens[qs.topicId]

		// Same GOP as previous resolved item in this row?
		sameGOP := havePrev && prevChunk == r.chunkKey && prevKeyframeIdx == r.keyframeMsgIdx

		var startIdx int
		var reset bool
		if !sameGOP {
			startIdx = r.keyframeMsgIdx
			reset = true
		} else {
			startIdx = prevTargetMsgIdx + 1
			reset = false
		}

		// The Phase B registered range starts at the GOP's keyframe in this
		// chunk and is long enough to cover the furthest target across all
		// queries in this GOP. We slice into it by per-frame offset.
		prefixRangeOffset := ic.ChunkOffset + mis[r.keyframeMsgIdx].OffsetInChunk
		raw := loaded.Get(prefixRangeOffset)
		baseInChunk := mis[r.keyframeMsgIdx].OffsetInChunk

		// startIdx > targetMsgIdx happens when two queries resolve to the
		// exact same target frame within a GOP: Frames is empty, no reset.
		var frames []SampleFrame
		if startIdx <= r.targetMsgIdx {
			frames = make([]SampleFrame, 0, r.targetMsgIdx-startIdx+1)
			for i := startIdx; i <= r.targetMsgIdx; i++ {
				frameStart := mis[i].OffsetInChunk - baseInChunk
				frameLen := lens[i]
				data := make([]byte, frameLen)
				copy(data, raw[frameStart:frameStart+frameLen])
				frames = append(frames, SampleFrame{
					Timestamp: mis[i].Timestamp,
					// keyframeMsgIdx is the greatest key frame index <=
					// targetMsgIdx, so within [keyframeMsgIdx..targetMsgIdx]
					// it is the only key frame.
					IsKeyFrame: i == r.keyframeMsgIdx,
					Data:       data,
				})
			}
			// Data stays nil for video: the bytes live in Frames, whose last
			// element is the target. Timestamp names that target frame (the
			// last new frame in this incremental slice, by construction).
			qs.results[r.tIdx].Timestamp = frames[len(frames)-1].Timestamp
		} else {
			// Empty Frames: the target was already emitted by the previous
			// in-row result (same target frame). Data stays nil; Timestamp
			// still names the target so the caller knows which frame it is.
			qs.results[r.tIdx].Timestamp = mis[r.targetMsgIdx].Timestamp
		}
		qs.results[r.tIdx].Found = true
		qs.results[r.tIdx].Frames = frames
		qs.results[r.tIdx].ResetDecoder = reset

		havePrev = true
		prevChunk = r.chunkKey
		prevKeyframeIdx = r.keyframeMsgIdx
		prevTargetMsgIdx = r.targetMsgIdx
	}
}

func (e *sampleEngine) collect() [][]SampleHit {
	out := make([][]SampleHit, len(e.queryStates))
	for i, qs := range e.queryStates {
		if qs.isVideo {
			for j := range qs.results {
				qs.results[j].IsVideo = true
			}
		}
		out[i] = qs.results
	}
	return out
}
