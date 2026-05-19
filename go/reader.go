package turbodata

import (
	"bytes"
	"fmt"
	"io"
)

type Reader struct {
	rs io.ReadSeeker

	summary *Summary
	footer  *Footer
}

const footerLen = 13

func NewReader(rs io.ReadSeeker) (*Reader, error) {
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

func (r *Reader) Summary() (*Summary, error) {
	if r.summary != nil {
		return r.summary, nil
	}

	if _, err := r.rs.Seek(-r.footer.SummaryLen-footerLen, io.SeekEnd); err != nil {
		return nil, err
	}

	compressed := make([]byte, r.footer.SummaryLen)
	if _, err := io.ReadFull(r.rs, compressed); err != nil {
		return nil, err
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
	summary, err := r.Summary()
	if err != nil {
		return nil, err
	}

	it := newMessageIterator(r.rs, summary)
	for _, opt := range opts {
		if err := opt(it); err != nil {
			return nil, err
		}
	}

	it.prepare()

	return it, nil
}
