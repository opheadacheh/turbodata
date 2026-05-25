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
type SampleHit struct {
	Found     bool
	Timestamp int64
	Data      []byte
}

// Sample resolves floor messages for the given specs in two concurrent I/O
// waves and returns hits in the same shape as the input: out[i][j]
// corresponds to specs[i].Timestamps[j].
func Sample(rs ReadSource, summary *format.Summary, specs []SampleSpec, strategy readstrategy.ReadStrategy) ([][]SampleHit, error) {
	if len(specs) == 0 {
		return nil, nil
	}
	e := newSampleEngine(rs, summary, specs, strategy)
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
}

type queryState struct {
	spec         SampleSpec
	groupIdx     int
	topicId      uint16
	isCompressed bool
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

	queryStates []*queryState
	chunks      map[chunkKey]*chunkCache // decoded across all Phase A waves
}

func newSampleEngine(rs ReadSource, summary *format.Summary, specs []SampleSpec, strategy readstrategy.ReadStrategy) *sampleEngine {
	e := &sampleEngine{
		rs:       rs,
		summary:  summary,
		strategy: strategy,
		fetcher:  iorange.NewFetcher(rs, strategy.MaxConcurrency),
		ctx:      context.Background(),
		chunks:   make(map[chunkKey]*chunkCache),
	}
	e.queryStates = make([]*queryState, len(specs))
	for i, spec := range specs {
		qs := &queryState{
			spec:    spec,
			results: make([]SampleHit, len(spec.Timestamps)),
		}
		e.resolveTopic(qs)
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
				j := i
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
		qs.resolved = append(qs.resolved, resolvedItem{
			tIdx:          p.tIdx,
			chunkKey:      chunkKey{qs.groupIdx, c},
			msgOffInChunk: mis[k].OffsetInChunk,
			msgLen:        lens[k],
			timestamp:     mis[k].Timestamp,
		})
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
// out individual messages after a single decompression). Uncompressed groups
// register one Range per message; iorange.Plan coalesces neighbors per the
// strategy.
func (e *sampleEngine) runPhaseB() error {
	type planEntry struct {
		key    chunkKey
		offset int64
		length int64
	}
	seenChunk := make(map[chunkKey]struct{})
	var entries []planEntry
	for _, qs := range e.queryStates {
		for _, r := range qs.resolved {
			ic := e.chunks[r.chunkKey].ic
			if qs.isCompressed {
				if _, dup := seenChunk[r.chunkKey]; dup {
					continue
				}
				seenChunk[r.chunkKey] = struct{}{}
				entries = append(entries, planEntry{key: r.chunkKey, offset: ic.ChunkOffset, length: ic.ChunkLen})
			} else {
				entries = append(entries, planEntry{key: r.chunkKey, offset: ic.ChunkOffset + r.msgOffInChunk, length: r.msgLen})
			}
		}
	}
	if len(entries) == 0 {
		return nil
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].offset < entries[j].offset })

	ranges := make([]iorange.Range, len(entries))
	for i, pe := range entries {
		ranges[i] = iorange.Range{Offset: pe.offset, Length: pe.length}
	}
	ops, locs := iorange.Plan(ranges, e.strategy.CoalesceGap, e.strategy.SplitThreshold)
	bufs, err := e.fetcher.Execute(e.ctx, ops)
	if err != nil {
		return err
	}
	loaded := iorange.NewLoadedBytes(ranges, locs, bufs)

	// Group compressed resolved items by chunk so each chunk is decompressed
	// exactly once and its decompressed bytes can be reused across messages
	// without a per-chunk persistent copy.
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

	for _, qs := range e.queryStates {
		if qs.isCompressed {
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
	return nil
}

func (e *sampleEngine) collect() [][]SampleHit {
	out := make([][]SampleHit, len(e.queryStates))
	for i, qs := range e.queryStates {
		out[i] = qs.results
	}
	return out
}
