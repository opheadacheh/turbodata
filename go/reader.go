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
}

// NewReader creates a Reader backed by rs. No I/O is performed; the footer
// and summary are loaded lazily on the first call to Summary or ReadMessages.
func NewReader(rs ReadSource) (*Reader, error) {
	if rs == nil {
		return nil, ErrNilReadSource
	}
	return &Reader{rs: rs}, nil
}

// Summary returns the parsed summary, loading it lazily on the first call.
// Subsequent calls return the cached value.
func (r *Reader) Summary() (*format.Summary, error) {
	return r.summaryWithHint(0)
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

	if err := it.Prepare(); err != nil {
		return nil, err
	}
	return it, nil
}
