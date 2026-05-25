package iorange

import (
	"bytes"
	"context"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestFetcherExecuteEmpty(t *testing.T) {
	f := NewFetcher(bytes.NewReader([]byte("hello")), 4)
	bufs, err := f.Execute(context.Background(), nil)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(bufs) != 0 {
		t.Fatalf("expected 0 bufs, got %d", len(bufs))
	}
}

func TestFetcherExecuteCorrectness(t *testing.T) {
	data := []byte("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")
	f := NewFetcher(bytes.NewReader(data), 4)
	ops := []ReadOp{
		{Offset: 0, Length: 5},
		{Offset: 10, Length: 3},
		{Offset: 33, Length: 3},
	}
	bufs, err := f.Execute(context.Background(), ops)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(bufs) != 3 {
		t.Fatalf("expected 3 bufs, got %d", len(bufs))
	}
	if string(bufs[0]) != "01234" {
		t.Errorf("bufs[0] = %q", bufs[0])
	}
	if string(bufs[1]) != "ABC" {
		t.Errorf("bufs[1] = %q", bufs[1])
	}
	if string(bufs[2]) != "XYZ" {
		t.Errorf("bufs[2] = %q", bufs[2])
	}
}

// concurrencyObserver wraps a ReaderAt and tracks the max simultaneous
// in-flight ReadAt calls observed during the test, so we can verify the
// fetcher honors MaxConcurrency.
type concurrencyObserver struct {
	r        io.ReaderAt
	inFlight atomic.Int64
	maxSeen  atomic.Int64
}

func (c *concurrencyObserver) ReadAt(p []byte, off int64) (int, error) {
	cur := c.inFlight.Add(1)
	for {
		prev := c.maxSeen.Load()
		if cur <= prev || c.maxSeen.CompareAndSwap(prev, cur) {
			break
		}
	}
	// Hold the slot briefly so concurrent calls actually overlap in time.
	time.Sleep(5 * time.Millisecond)
	n, err := c.r.ReadAt(p, off)
	c.inFlight.Add(-1)
	return n, err
}

func TestFetcherRespectsMaxConcurrency(t *testing.T) {
	data := bytes.Repeat([]byte("x"), 1024)
	obs := &concurrencyObserver{r: bytes.NewReader(data)}
	f := NewFetcher(obs, 3)

	const numOps = 16
	ops := make([]ReadOp, numOps)
	for i := range ops {
		ops[i] = ReadOp{Offset: int64(i * 4), Length: 4}
	}
	if _, err := f.Execute(context.Background(), ops); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if max := obs.maxSeen.Load(); max > 3 {
		t.Errorf("observed in-flight = %d, want <= 3", max)
	}
	// Concurrency=3 with 16 ops and 5ms each should have observed > 1 in-flight
	// at some point. If not, the limiter is too eager / there's a goroutine bug.
	if max := obs.maxSeen.Load(); max < 2 {
		t.Errorf("observed in-flight = %d, expected > 1 to confirm parallelism", max)
	}
}

func TestFetcherSerialWhenConcurrencyClamped(t *testing.T) {
	data := []byte("hello world")
	obs := &concurrencyObserver{r: bytes.NewReader(data)}
	f := NewFetcher(obs, 0) // clamps to 1

	ops := []ReadOp{
		{Offset: 0, Length: 5},
		{Offset: 6, Length: 5},
	}
	if _, err := f.Execute(context.Background(), ops); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if max := obs.maxSeen.Load(); max != 1 {
		t.Errorf("expected serial execution (in-flight = 1), got %d", max)
	}
}

// failingReaderAt fails after the first call to simulate an I/O error
// mid-fetch and verify error propagation.
type failingReaderAt struct {
	calls atomic.Int64
	mu    sync.Mutex
	err   error
}

func (f *failingReaderAt) ReadAt(p []byte, off int64) (int, error) {
	n := f.calls.Add(1)
	if n >= 2 {
		f.mu.Lock()
		defer f.mu.Unlock()
		return 0, f.err
	}
	// First call succeeds (return zeros).
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}

func TestFetcherPropagatesError(t *testing.T) {
	wantErr := io.ErrUnexpectedEOF
	f := NewFetcher(&failingReaderAt{err: wantErr}, 1)
	ops := []ReadOp{
		{Offset: 0, Length: 4},
		{Offset: 4, Length: 4},
	}
	_, err := f.Execute(context.Background(), ops)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}
