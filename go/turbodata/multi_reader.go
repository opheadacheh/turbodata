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

package turbodata

import (
	"errors"
	"fmt"

	"github.com/opheadacheh/turbodata/go/turbodata/format"
	"github.com/opheadacheh/turbodata/go/turbodata/internal/iter"
)

// MultiReader presents several single-file Readers as one time-ordered stream.
// Topic-name collisions across files are the caller's responsibility: configure
// per-Reader WithTopicRemap so that names meant to union share an exposed name
// and names meant to stay distinct do not. ReadMessages and Sample operate
// entirely in each Reader's exposed-name space.
type MultiReader struct {
	readers []*Reader
}

// NewMultiReader groups readers into a MultiReader. At least one reader is
// required.
func NewMultiReader(readers ...*Reader) (*MultiReader, error) {
	if len(readers) == 0 {
		return nil, errors.New("turbodata: NewMultiReader requires at least one reader")
	}
	return &MultiReader{readers: readers}, nil
}

// ReadMessages returns a single iterator that merges every reader's stream in
// timestamp order. Options pass through to each underlying Reader.ReadMessages
// unchanged, so order, time bounds, topic filtering (by exposed name), and
// strategy apply per file before the merge.
//
// Under WithVideoDecodable, any in-scope video topic supplied by more than one
// reader must have time-disjoint source ranges; overlapping sources return
// ErrVideoSourcesOverlap. Merging overlapping decodable video sources would
// interleave frames from different GOP chains into an undecodable stream.
func (m *MultiReader) ReadMessages(opts ...ReadOption) (*iter.MultiMessageIterator, error) {
	subs := make([]*iter.MessageIterator, len(m.readers))
	for i, r := range m.readers {
		it, err := r.ReadMessages(opts...)
		if err != nil {
			return nil, err
		}
		subs[i] = it
	}

	if subs[0].VideoDecodable {
		bounds := make([]map[string]topicBound, len(m.readers))
		for i, r := range m.readers {
			b, err := r.topicBounds()
			if err != nil {
				return nil, err
			}
			bounds[i] = b
		}
		if err := enforceVideoDisjoint(readScopeTopics(opts, bounds), bounds); err != nil {
			return nil, err
		}
	}

	reverse := subs[0].Order == iter.ReverseTimeOrder
	return iter.NewMultiMessageIterator(subs, reverse), nil
}

// readScopeTopics returns the exposed topic names a ReadMessages call covers:
// the names passed to WithTopicNames, or every exposed topic across all
// readers when no topic filter is set. opts are applied to a throwaway
// iterator so the requested names are read before per-reader remap
// translation rewrites them to in-file names.
func readScopeTopics(opts []ReadOption, bounds []map[string]topicBound) []string {
	probe := iter.NewMessageIterator(nil)
	for _, opt := range opts {
		// Errors here also surface from the real per-reader ReadMessages; the
		// probe only needs the resulting field values.
		_ = opt(probe)
	}
	if len(probe.TopicNames) > 0 {
		return probe.TopicNames
	}
	seen := make(map[string]struct{})
	var all []string
	for _, b := range bounds {
		for name := range b {
			if _, ok := seen[name]; !ok {
				seen[name] = struct{}{}
				all = append(all, name)
			}
		}
	}
	return all
}

// Sample resolves floor messages across every reader. For each
// (queries[i].Topic, queries[i].Timestamps[j]) pair, out[i][j] is the floor
// result with the latest Found timestamp among all readers that hold the
// topic (by exposed name) - the true floor over the union. Ties resolve
// arbitrarily.
//
// Preconditions, enforced before any data I/O:
//   - For every i, queries[i].Timestamps must be strictly increasing.
//   - queries[i].Topic must be unique across all i.
//   - Every queries[i].Topic must exist in at least one reader.
//
// Under WithSampleVideoDecodable, a video topic supplied by more than one
// reader must have time-disjoint source ranges; overlapping sources return
// ErrVideoSourcesOverlap. Time-disjointness keeps each row's decoder carry-
// over chain intact: because query timestamps strictly increase and ranges do
// not overlap, the first cell drawn from any reader is that reader's first
// Found cell (ResetDecoder=true).
func (m *MultiReader) Sample(queries []SampleQuery, opts ...SampleOption) ([][]SampleResult, error) {
	cfg := &sampleConfig{strategy: DefaultSampleStrategy}
	for _, opt := range opts {
		if err := opt(cfg); err != nil {
			return nil, err
		}
	}

	bounds := make([]map[string]topicBound, len(m.readers))
	for i, r := range m.readers {
		b, err := r.topicBounds()
		if err != nil {
			return nil, err
		}
		bounds[i] = b
	}

	if err := validateMultiSampleQueries(queries, bounds); err != nil {
		return nil, err
	}

	if cfg.videoDecodable {
		topics := make([]string, len(queries))
		for i, q := range queries {
			topics[i] = q.Topic
		}
		if err := enforceVideoDisjoint(topics, bounds); err != nil {
			return nil, err
		}
	}

	out := make([][]SampleResult, len(queries))
	for qi, q := range queries {
		out[qi] = make([]SampleResult, len(q.Timestamps))
	}

	// Dispatch to each reader only the queries whose topic it holds (avoids
	// the engine's unknown-topic error), then merge by latest Found floor.
	for ri, r := range m.readers {
		var subset []SampleQuery
		var origIdx []int
		for qi, q := range queries {
			if _, ok := bounds[ri][q.Topic]; ok {
				subset = append(subset, q)
				origIdx = append(origIdx, qi)
			}
		}
		if len(subset) == 0 {
			continue
		}
		res, err := r.Sample(subset, opts...)
		if err != nil {
			return nil, err
		}
		for sub, qi := range origIdx {
			row := res[sub]
			for j := range row {
				cand := row[j]
				if !cand.Found {
					continue
				}
				if cur := out[qi][j]; !cur.Found || cand.Timestamp > cur.Timestamp {
					out[qi][j] = cand
				}
			}
		}
	}
	return out, nil
}

// validateMultiSampleQueries mirrors the single-reader checks but treats a
// topic as known if any reader holds it. All violations are aggregated.
func validateMultiSampleQueries(queries []SampleQuery, bounds []map[string]topicBound) error {
	seen := make(map[string]int)
	var errs []error
	for i, q := range queries {
		known := false
		for _, b := range bounds {
			if _, ok := b[q.Topic]; ok {
				known = true
				break
			}
		}
		if !known {
			errs = append(errs, fmt.Errorf("queries[%d]: unknown topic %q (not in any reader)", i, q.Topic))
		}
		if prev, dup := seen[q.Topic]; dup {
			errs = append(errs, fmt.Errorf("queries[%d]: duplicate topic %q already used by queries[%d]", i, q.Topic, prev))
		} else {
			seen[q.Topic] = i
		}
		for j := 1; j < len(q.Timestamps); j++ {
			if q.Timestamps[j] <= q.Timestamps[j-1] {
				errs = append(errs, fmt.Errorf("queries[%d].Timestamps not strictly increasing at position %d (%d <= %d)",
					i, j, q.Timestamps[j], q.Timestamps[j-1]))
			}
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// enforceVideoDisjoint returns ErrVideoSourcesOverlap when any of the given
// exposed video topics is provided by readers with overlapping inclusive time
// ranges. Topics that are not video, or are held by fewer than two readers,
// are skipped.
func enforceVideoDisjoint(topics []string, bounds []map[string]topicBound) error {
	for _, topic := range topics {
		type rng struct{ minTs, maxTs int64 }
		var ranges []rng
		isVideo := false
		for _, b := range bounds {
			bound, ok := b[topic]
			if !ok {
				continue
			}
			if bound.isVideo {
				isVideo = true
			}
			ranges = append(ranges, rng{bound.minTs, bound.maxTs})
		}
		if !isVideo || len(ranges) < 2 {
			continue
		}
		for a := 0; a < len(ranges); a++ {
			for c := a + 1; c < len(ranges); c++ {
				if ranges[a].minTs <= ranges[c].maxTs && ranges[c].minTs <= ranges[a].maxTs {
					return fmt.Errorf("turbodata: video topic %q sources [%d,%d] and [%d,%d] overlap: %w",
						topic, ranges[a].minTs, ranges[a].maxTs, ranges[c].minTs, ranges[c].maxTs, ErrVideoSourcesOverlap)
				}
			}
		}
	}
	return nil
}

// topicBound captures, per exposed topic, whether it is a video topic and the
// inclusive [minTs, maxTs] span of its messages. For video topics (always
// single-topic groups) the span is exact; for multi-topic groups it is the
// group's span, which is only used as a coarse bound and never for video
// enforcement.
type topicBound struct {
	isVideo bool
	minTs   int64
	maxTs   int64
}

// topicBounds returns per-exposed-topic bounds, computed once from the cached
// summary. Honors any configured remap so keys are exposed names.
func (r *Reader) topicBounds() (map[string]topicBound, error) {
	if r.boundsCache != nil {
		return r.boundsCache, nil
	}
	summary, err := r.summaryWithHint(0)
	if err != nil {
		return nil, err
	}
	if _, err := r.prepareRename(summary); err != nil {
		return nil, err
	}
	out := make(map[string]topicBound)
	for _, ti := range summary.TopicsInfos {
		if len(ti.IndexChunkInfoList) == 0 {
			continue
		}
		minTs := ti.IndexChunkInfoList[0].StartTimestamp
		maxTs := ti.IndexChunkInfoList[0].EndTimestamp
		for _, info := range ti.IndexChunkInfoList {
			if info.StartTimestamp < minTs {
				minTs = info.StartTimestamp
			}
			if info.EndTimestamp > maxTs {
				maxTs = info.EndTimestamp
			}
		}
		isVideo := false
		if len(ti.TopicMetadatas) > 0 {
			isVideo, _ = ti.TopicMetadatas[0].Metadata[format.MetaKeyVideo].(bool)
		}
		for _, tm := range ti.TopicMetadatas {
			exposed := tm.Name
			if v, ok := r.topicRemap[tm.Name]; ok {
				exposed = v
			}
			out[exposed] = topicBound{isVideo: isVideo, minTs: minTs, maxTs: maxTs}
		}
	}
	r.boundsCache = out
	return out, nil
}
