package turbodata

import (
	"errors"

	"turbodata/internal/iter"
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
func (m *MultiReader) ReadMessages(opts ...ReadOption) (*iter.MultiMessageIterator, error) {
	subs := make([]*iter.MessageIterator, len(m.readers))
	for i, r := range m.readers {
		it, err := r.ReadMessages(opts...)
		if err != nil {
			return nil, err
		}
		subs[i] = it
	}
	reverse := subs[0].Order == iter.ReverseTimeOrder
	return iter.NewMultiMessageIterator(subs, reverse), nil
}
