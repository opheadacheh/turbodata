package turbodata

import (
	"bytes"
	"fmt"
	"io"
)

type Reader struct {
	rs ReadSource

	size int64

	summary *Summary
	footer  *Footer
}

const footerLen = 13

// NewReader creates a Reader backed by rs. No I/O is performed; the footer
// and summary are loaded lazily on the first call to Summary or ReadMessages.
func NewReader(rs ReadSource) (*Reader, error) {
	return &Reader{rs: rs}, nil
}

// Summary returns the parsed summary, loading it lazily on the first call.
// Subsequent calls return the cached value.
func (r *Reader) Summary() (*Summary, error) {
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
func (r *Reader) summaryWithHint(prefetch int64) (*Summary, error) {
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

	if r.size < footerLen {
		return nil, fmt.Errorf("file is too small to contain a footer")
	}

	var compressed []byte

	if prefetch <= 0 {
		prefetch = footerLen
	}
	if prefetch > r.size {
		prefetch = r.size
	}

	tail := make([]byte, prefetch)
	if _, err := r.rs.ReadAt(tail, r.size-prefetch); err != nil && err != io.EOF {
		return nil, err
	}

	footer, err := ReadFooter(bytes.NewReader(tail[int64(len(tail))-footerLen:]))
	if err != nil {
		return nil, err
	}
	if footer.Magic != [5]byte{'7', 'U', 'R', 'B', '0'} {
		return nil, fmt.Errorf("invalid magic number")
	}
	r.footer = footer

	if int64(len(tail)) >= footer.SummaryLen+footerLen {
		start := int64(len(tail)) - footerLen - footer.SummaryLen
		compressed = tail[start : start+footer.SummaryLen]
	} else {
		compressed = make([]byte, footer.SummaryLen)
		if _, err := r.rs.ReadAt(compressed, r.size-footer.SummaryLen-footerLen); err != nil {
			return nil, err
		}
	}

	decompressed, err := decompress(compressed)
	if err != nil {
		return nil, err
	}

	summary, err := ReadSummary(bytes.NewReader(decompressed))
	if err != nil {
		return nil, err
	}
	r.summary = summary
	return summary, nil
}

func (r *Reader) ReadMessages(opts ...ReadOption) (*MessageIterator, error) {
	// Parse options before loading summary so that WithTailPrefetch can take
	// effect on the summary read, and WithReadStrategy can be detected before
	// the iterator is prepared.
	it := newMessageIterator(r.rs, nil)
	for _, opt := range opts {
		if err := opt(it); err != nil {
			return nil, err
		}
	}

	summary, err := r.summaryWithHint(it.tailPrefetch)
	if err != nil {
		return nil, err
	}
	it.summary = summary

	if err := it.prepare(); err != nil {
		return nil, err
	}
	return it, nil
}
