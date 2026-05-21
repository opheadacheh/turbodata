package turbodata

import (
	"bytes"
	"fmt"
	"io"
)

type Reader struct {
	rs ReadSource

	summary *Summary
	footer  *Footer
}

const footerLen = 13

func NewReader(rs ReadSource) (*Reader, error) {
	if _, err := rs.Seek(-footerLen, io.SeekEnd); err != nil {
		return nil, err
	}

	footer, err := ReadFooter(rs)
	if err != nil {
		return nil, err
	}

	if footer.Magic != [5]byte{'7', 'U', 'R', 'B', '0'} {
		return nil, fmt.Errorf("invalid magic number")
	}

	return &Reader{
		rs:     rs,
		footer: footer,
	}, nil
}

// Summary returns the parsed summary, loading it lazily on the first call.
// Subsequent calls return the cached value. Uses today's Seek+Read flow.
func (r *Reader) Summary() (*Summary, error) {
	return r.summaryWithHint(0)
}

// summaryWithHint loads the summary, optionally taking a single speculative
// ReadAt at the file's tail of size prefetch (which should include enough
// trailing bytes to cover the footer + the compressed summary). On prefetch
// hit, only one ReadAt is needed; on miss, a second exact-sized ReadAt is
// issued for the summary.
//
// prefetch <= 0 disables the hint and falls back to today's Seek+Read flow.
// Result is cached in r.summary; subsequent calls return that cached value
// regardless of the prefetch hint.
func (r *Reader) summaryWithHint(prefetch int64) (*Summary, error) {
	if r.summary != nil {
		return r.summary, nil
	}

	var compressed []byte

	if prefetch > 0 && prefetch >= r.footer.SummaryLen+footerLen {
		// One speculative ReadAt covering [size-prefetch, size). If the
		// summary fits in there, we're done in one request.
		size, err := r.rs.Seek(0, io.SeekEnd)
		if err != nil {
			return nil, err
		}
		readLen := prefetch
		if readLen > size {
			readLen = size
		}
		tail := make([]byte, readLen)
		if _, err := r.rs.ReadAt(tail, size-readLen); err != nil && err != io.EOF {
			return nil, err
		}
		// The compressed summary sits at the end-of-tail minus the footer
		// minus the summary length.
		if int64(len(tail)) >= r.footer.SummaryLen+footerLen {
			start := int64(len(tail)) - footerLen - r.footer.SummaryLen
			compressed = tail[start : start+r.footer.SummaryLen]
		}
	}

	if compressed == nil {
		// Default path: today's Seek+Read flow.
		if _, err := r.rs.Seek(-r.footer.SummaryLen-footerLen, io.SeekEnd); err != nil {
			return nil, err
		}
		compressed = make([]byte, r.footer.SummaryLen)
		if _, err := io.ReadFull(r.rs, compressed); err != nil {
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
