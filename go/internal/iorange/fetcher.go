package iorange

import (
	"context"
	"fmt"
	"io"
	"sync"
)

// Fetcher executes a slice of ReadOps against an io.ReaderAt concurrently and
// returns one byte buffer per op, in input order. It is bound by a fixed
// MaxConcurrency.
type Fetcher struct {
	rr      io.ReaderAt
	workers int
}

// NewFetcher returns a Fetcher backed by rr. maxConcurrency <= 0 is clamped
// to 1 (serial execution).
func NewFetcher(rr io.ReaderAt, maxConcurrency int) *Fetcher {
	if maxConcurrency <= 0 {
		maxConcurrency = 1
	}
	return &Fetcher{rr: rr, workers: maxConcurrency}
}

// Execute fetches every op in ops. The returned [][]byte is parallel to ops:
// bufs[i] holds the bytes for ops[i] (length ops[i].Length). On the first
// error encountered, Execute returns it and stops dispatching new ops;
// in-flight workers may still complete.
func (f *Fetcher) Execute(ctx context.Context, ops []ReadOp) ([][]byte, error) {
	if len(ops) == 0 {
		return nil, nil
	}
	bufs := make([][]byte, len(ops))

	// Pre-allocate result buffers so workers can write to bufs[i] without
	// further synchronization.
	for i, op := range ops {
		bufs[i] = make([]byte, op.Length)
	}

	tokens := make(chan struct{}, f.workers)
	var wg sync.WaitGroup
	var firstErrOnce sync.Once
	var firstErr error
	setErr := func(err error) {
		firstErrOnce.Do(func() { firstErr = err })
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for i, op := range ops {
		select {
		case <-ctx.Done():
			setErr(ctx.Err())
			wg.Wait()
			return nil, firstErr
		case tokens <- struct{}{}:
		}

		wg.Add(1)
		go func(idx int, op ReadOp) {
			defer wg.Done()
			defer func() { <-tokens }()
			n, err := f.rr.ReadAt(bufs[idx], op.Offset)
			if err != nil && !(err == io.EOF && int64(n) == op.Length) {
				setErr(fmt.Errorf("read op %d at offset %d length %d: %w", idx, op.Offset, op.Length, err))
				cancel()
				return
			}
			if int64(n) != op.Length {
				setErr(fmt.Errorf("read op %d short read: got %d want %d", idx, n, op.Length))
				cancel()
				return
			}
		}(i, op)
	}
	wg.Wait()
	if firstErr != nil {
		return nil, firstErr
	}
	return bufs, nil
}
