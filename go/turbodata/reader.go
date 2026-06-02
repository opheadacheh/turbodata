package turbodata

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"turbodata/format"
	"turbodata/internal/compress"
	"turbodata/internal/iter"
)

type Reader struct {
	rs ReadSource

	size int64

	summary *format.Summary
	footer  *format.Footer

	// topicRemap maps in-file topic names to the exposed names this Reader
	// presents to callers. nil/empty means identity (zero behavior change).
	topicRemap map[string]string

	// Cached prepareRename outputs (computed lazily once the summary is
	// known). renameInverse maps exposed -> in-file for query/filter
	// translation; renameErr captures a remap collision detected against the
	// actual summary.
	renamePrepared bool
	renameInverse  map[string]string
	renameErr      error

	// Cached per-topic bounds (exposed name -> bound), computed lazily from
	// the summary by topicBounds.
	boundsCache map[string]topicBound
}

// ReaderOption configures a Reader at construction time.
type ReaderOption func(*Reader)

// WithTopicRemap presents in-file topic names under different exposed names.
// The map is keyed by in-file name and valued by the exposed name emitted from
// ReadMessages, accepted by WithTopicNames and SampleQuery.Topic, and reported
// by Summary. Names absent from the map pass through unchanged.
//
// The remap is validated lazily against the file's summary on first use: it is
// an error for two topics to collapse onto the same exposed name (whether two
// in-file names map to the same target, or a renamed name collides with an
// untouched in-file name).
func WithTopicRemap(m map[string]string) ReaderOption {
	return func(r *Reader) {
		r.topicRemap = m
	}
}

// NewReader creates a Reader backed by rs. No I/O is performed; the footer
// and summary are loaded lazily on the first call to Summary or ReadMessages.
func NewReader(rs ReadSource, opts ...ReaderOption) *Reader {
	r := &Reader{rs: rs}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// prepareRename builds the exposed -> in-file inverse map once the summary is
// known and validates that no two topics collapse onto the same exposed name.
// Results (including any error) are cached. Returns (nil, nil) when no remap is
// configured.
func (r *Reader) prepareRename(summary *format.Summary) (map[string]string, error) {
	if r.renamePrepared {
		return r.renameInverse, r.renameErr
	}
	r.renamePrepared = true
	if len(r.topicRemap) == 0 {
		return nil, nil
	}
	inverse := make(map[string]string)
	for _, ti := range summary.TopicsInfos {
		for _, tm := range ti.TopicMetadatas {
			exposed := tm.Name
			if v, ok := r.topicRemap[tm.Name]; ok {
				exposed = v
			}
			if prev, dup := inverse[exposed]; dup {
				r.renameErr = fmt.Errorf("turbodata: topic remap produces duplicate exposed name %q (from in-file topics %q and %q)", exposed, prev, tm.Name)
				return nil, r.renameErr
			}
			inverse[exposed] = tm.Name
		}
	}
	r.renameInverse = inverse
	return r.renameInverse, nil
}

// exposedSummary returns a shallow copy of summary with TopicMetadata.Name
// rewritten to exposed names. The IndexChunkInfoList and Metadata maps are
// shared with the cached in-file summary (read-only here). When no remap is
// configured the original summary is returned unchanged.
func (r *Reader) exposedSummary(summary *format.Summary) (*format.Summary, error) {
	if len(r.topicRemap) == 0 {
		return summary, nil
	}
	if _, err := r.prepareRename(summary); err != nil {
		return nil, err
	}
	out := &format.Summary{TopicsInfos: make([]*format.TopicsInfo, len(summary.TopicsInfos))}
	for i, ti := range summary.TopicsInfos {
		nti := &format.TopicsInfo{
			TopicMetadatas:     make([]*format.TopicMetadata, len(ti.TopicMetadatas)),
			IndexChunkInfoList: ti.IndexChunkInfoList,
			TotalLen:           ti.TotalLen,
		}
		for j, tm := range ti.TopicMetadatas {
			name := tm.Name
			if v, ok := r.topicRemap[name]; ok {
				name = v
			}
			nti.TopicMetadatas[j] = &format.TopicMetadata{
				Id:       tm.Id,
				Name:     name,
				Metadata: tm.Metadata,
			}
		}
		out.TopicsInfos[i] = nti
	}
	return out, nil
}

// Summary returns the parsed summary, loading it lazily on the first call.
// Subsequent calls return the cached value. When a topic remap is configured,
// TopicMetadata.Name reports exposed names.
func (r *Reader) Summary() (*format.Summary, error) {
	summary, err := r.summaryWithHint(0)
	if err != nil {
		return nil, err
	}
	return r.exposedSummary(summary)
}

// summaryWithHint loads the footer and summary, optionally via a single
// speculative ReadAt of prefetch bytes from the file tail. When prefetch is
// large enough to cover footer + compressed summary, only one ReadAt is issued.
// When prefetch <= 0 or the tail window is too small for the summary, two
// sequential Seek+Read calls are used instead (one for the footer, one for the
// summary).
//
// Result is cached in r.summary; subsequent calls return that cached value
// regardless of the prefetch hint.
func (r *Reader) summaryWithHint(prefetch int64) (*format.Summary, error) {
	if r.summary != nil {
		return r.summary, nil
	}

	if r.size == 0 {
		size, err := r.rs.Seek(0, io.SeekEnd)
		if err != nil {
			return nil, err
		}
		r.size = size
	}

	if r.size < format.FooterLen {
		return nil, fmt.Errorf("file is too small to contain a footer")
	}

	var compressed []byte

	if prefetch <= 0 {
		prefetch = format.FooterLen
	}
	if prefetch > r.size {
		prefetch = r.size
	}

	tail := make([]byte, prefetch)
	if _, err := r.rs.ReadAt(tail, r.size-prefetch); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}

	footer, err := format.ReadFooter(bytes.NewReader(tail[int64(len(tail))-format.FooterLen:]))
	if err != nil {
		return nil, err
	}
	if footer.Magic != format.Magic {
		return nil, fmt.Errorf("invalid magic number")
	}
	r.footer = footer

	if int64(len(tail)) >= footer.SummaryLen+format.FooterLen {
		start := int64(len(tail)) - format.FooterLen - footer.SummaryLen
		compressed = tail[start : start+footer.SummaryLen]
	} else {
		compressed = make([]byte, footer.SummaryLen)
		if _, err := r.rs.ReadAt(compressed, r.size-footer.SummaryLen-format.FooterLen); err != nil {
			return nil, err
		}
	}

	decompressed, err := compress.Decompress(compressed)
	if err != nil {
		return nil, err
	}

	summary, err := format.ReadSummary(bytes.NewReader(decompressed))
	if err != nil {
		return nil, err
	}
	r.summary = summary
	return summary, nil
}

func (r *Reader) ReadMessages(opts ...ReadOption) (*iter.MessageIterator, error) {
	// Parse options before loading summary so that WithTailPrefetch can take
	// effect on the summary read, and WithReadStrategy can be detected before
	// the iterator is prepared.
	it := iter.NewMessageIterator(r.rs)
	for _, opt := range opts {
		if err := opt(it); err != nil {
			return nil, err
		}
	}

	summary, err := r.summaryWithHint(it.TailPrefetch)
	if err != nil {
		return nil, err
	}
	it.Summary = summary

	if len(r.topicRemap) > 0 {
		inverse, err := r.prepareRename(summary)
		if err != nil {
			return nil, err
		}
		// Translate caller-supplied exposed names to in-file names; internal
		// matching stays in in-file space. Unknown names pass through and
		// simply match nothing.
		if len(it.TopicNames) > 0 {
			translated := make([]string, len(it.TopicNames))
			for i, n := range it.TopicNames {
				if infile, ok := inverse[n]; ok {
					translated[i] = infile
				} else {
					translated[i] = n
				}
			}
			it.TopicNames = translated
		}
		it.TopicRename = r.topicRemap
	}

	if err := it.Prepare(); err != nil {
		return nil, err
	}
	return it, nil
}
