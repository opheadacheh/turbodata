package turbodata

import (
	"errors"
	"fmt"

	"turbodata/format"
	"turbodata/internal/iter"
)

// SampleQuery requests floor messages from a single topic at the given
// timestamps. Within Timestamps, values must be strictly increasing. The same
// Topic must not appear in more than one SampleQuery within a single
// Reader.Sample call. Both rules are enforced by Reader.Sample before any I/O.
type SampleQuery struct {
	Topic      string
	Timestamps []int64
}

// SampleResult is one returned message. Out[i][j] from Reader.Sample
// corresponds to queries[i].Timestamps[j]. Found is false when there is no
// message with timestamp <= Timestamps[j] for the topic (e.g. T precedes the
// topic's first message). Data is an owned copy and is safe to retain after
// the call returns.
//
// Video topics (opened with WithVideoTopic) also populate Frames and
// ResetDecoder. The conventions, within a single Out[i] row (one topic,
// strictly increasing query timestamps):
//
//   - For the first found result in each GOP encountered in the row:
//     ResetDecoder = true, Frames = [keyframe ... target] in storage
//     (decode) order. The caller's decoder must Reset() before feeding.
//
//   - For subsequent found results within the same GOP: ResetDecoder =
//     false, Frames = [previous_target+1 ... target] (only the new frames
//     since the previous query in this row). Frames is empty when two
//     queries resolve to the same target frame.
//
//   - Data and Timestamp always describe the target frame.
//
// Process results within a row in order to benefit from decoder-state
// carry-over. Skipping or reordering forces the caller to find the nearest
// preceding ResetDecoder=true entry and feed forward from there.
//
// For non-video topics, Frames is nil and ResetDecoder is false.
type SampleResult struct {
	Found     bool
	Timestamp int64
	Data      []byte

	// Video-only.
	Frames       []Frame
	ResetDecoder bool
}

// Frame is one coded video frame surfaced as part of a SampleResult's GOP
// prefix or incremental tail. Data is an owned copy and is safe to retain
// after Sample returns.
type Frame struct {
	Timestamp  int64
	IsKeyFrame bool
	Data       []byte
}

// DefaultSampleStrategy is the ReadStrategy Reader.Sample uses when the
// caller does not pass WithSampleReadStrategy. It is sized for a cloud-object
// / in-region profile and behaves acceptably on local disk; callers SHOULD
// override via WithSampleReadStrategy for their specific environment.
var DefaultSampleStrategy = ReadStrategy{
	CoalesceGap:    1 << 20, // 1 MiB
	SplitThreshold: 4 << 20, // 4 MiB
	MaxConcurrency: 16,
}

// SampleOption configures a Reader.Sample invocation.
type SampleOption func(*sampleConfig) error

type sampleConfig struct {
	strategy     ReadStrategy
	tailPrefetch int64
}

// WithSampleReadStrategy overrides DefaultSampleStrategy for this Sample call.
func WithSampleReadStrategy(s ReadStrategy) SampleOption {
	return func(c *sampleConfig) error {
		c.strategy = s
		return nil
	}
}

// WithSampleTailPrefetch hints how many trailing bytes to read speculatively
// when loading the file's summary. Same semantics as WithTailPrefetch on
// ReadMessages.
func WithSampleTailPrefetch(n int64) SampleOption {
	return func(c *sampleConfig) error {
		c.tailPrefetch = n
		return nil
	}
}

// LinSpaceTimestamps returns count timestamps starting at start, each stride
// apart. Useful for "N samples at a given frequency from T" patterns:
//
//	// 10 samples at 10 Hz starting at T:
//	ts := turbodata.LinSpaceTimestamps(T, int64(100*time.Millisecond), 10)
//
// Returns nil when count <= 0. Panics when stride <= 0 (the strict-increasing
// contract on SampleQuery.Timestamps requires positive strides).
func LinSpaceTimestamps(start, stride int64, count int) []int64 {
	if count <= 0 {
		return nil
	}
	if stride <= 0 {
		panic("turbodata: LinSpaceTimestamps requires stride > 0")
	}
	out := make([]int64, count)
	for i := 0; i < count; i++ {
		out[i] = start + int64(i)*stride
	}
	return out
}

// Sample returns floor messages for every (queries[i].Topic,
// queries[i].Timestamps[j]) pair. The result is shaped to mirror the input:
// out[i][j] corresponds to queries[i].Timestamps[j].
//
// Preconditions, enforced before any data I/O:
//   - For every i, queries[i].Timestamps must be strictly increasing.
//   - queries[i].Topic must be unique across all i.
//   - Every queries[i].Topic must exist in the file's summary.
//
// All violations across all queries are aggregated via errors.Join and
// returned as a single error. The summary is loaded (one to two ReadAt
// calls) before validation runs; data and index-chunk I/O only happen after
// validation passes.
//
// All data I/O is concurrent under the hood, governed by
// WithSampleReadStrategy. DefaultSampleStrategy applies when the option is
// omitted.
func (r *Reader) Sample(queries []SampleQuery, opts ...SampleOption) ([][]SampleResult, error) {
	cfg := &sampleConfig{strategy: DefaultSampleStrategy}
	for _, opt := range opts {
		if err := opt(cfg); err != nil {
			return nil, err
		}
	}

	summary, err := r.summaryWithHint(cfg.tailPrefetch)
	if err != nil {
		return nil, err
	}

	if err := validateSampleQueries(queries, summary); err != nil {
		return nil, err
	}

	specs := make([]iter.SampleSpec, len(queries))
	for i, q := range queries {
		specs[i] = iter.SampleSpec{Topic: q.Topic, Timestamps: q.Timestamps}
	}
	hits, err := iter.Sample(r.rs, summary, specs, cfg.strategy)
	if err != nil {
		return nil, err
	}

	out := make([][]SampleResult, len(queries))
	for i := range queries {
		if i < len(hits) {
			row := hits[i]
			out[i] = make([]SampleResult, len(row))
			for j, h := range row {
				var frames []Frame
				if len(h.Frames) > 0 {
					frames = make([]Frame, len(h.Frames))
					for k, f := range h.Frames {
						frames[k] = Frame{
							Timestamp:  f.Timestamp,
							IsKeyFrame: f.IsKeyFrame,
							Data:       f.Data,
						}
					}
				}
				out[i][j] = SampleResult{
					Found:        h.Found,
					Timestamp:    h.Timestamp,
					Data:         h.Data,
					Frames:       frames,
					ResetDecoder: h.ResetDecoder,
				}
			}
		} else {
			out[i] = []SampleResult{}
		}
	}
	return out, nil
}

// validateSampleQueries returns nil when all preconditions hold; otherwise it
// returns an errors.Join of every violation it observed. Sub-errors name the
// offending query index, timestamp position, and topic so callers can locate
// the problem in a large batch.
func validateSampleQueries(queries []SampleQuery, summary *format.Summary) error {
	knownTopics := make(map[string]struct{})
	for _, ti := range summary.TopicsInfos {
		for _, tm := range ti.TopicMetadatas {
			knownTopics[tm.Name] = struct{}{}
		}
	}
	seenTopics := make(map[string]int)
	var errs []error
	for i, q := range queries {
		if _, ok := knownTopics[q.Topic]; !ok {
			errs = append(errs, fmt.Errorf("queries[%d]: unknown topic %q", i, q.Topic))
		}
		if prev, dup := seenTopics[q.Topic]; dup {
			errs = append(errs, fmt.Errorf("queries[%d]: duplicate topic %q already used by queries[%d]", i, q.Topic, prev))
		} else {
			seenTopics[q.Topic] = i
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
