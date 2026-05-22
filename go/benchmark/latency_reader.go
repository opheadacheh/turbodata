package benchmark

import (
	"io"
	"time"

	"turbodata"
)

// LatencyReadSource wraps a turbodata.ReadSource and injects synthetic
// per-call latency to simulate remote storage (e.g. S3 / network FS).
//
// Each Read or ReadAt call sleeps for:
//
//	rtt + len(p)/perStreamBW
//
// Seek does not sleep — real object stores have no seek operation; cost is
// charged at the next Read/ReadAt as part of its RTT.
//
// Concurrency: ReadAt calls each sleep independently (no shared mutex), so N
// concurrent ReadAts complete in ~max(latency_i) wall time. This matches the
// "per-stream" bandwidth model used by ReadStrategy.StrategyForLatency.
type LatencyReadSource struct {
	rs          turbodata.ReadSource
	rtt         time.Duration
	perStreamBW int64 // bytes/sec; 0 disables bandwidth charging
}

// NewLatencyReadSource builds a wrapper. Pass rtt=0 and perStreamBW=0 for a
// pass-through (the default benchmark variant uses this so every variant
// shares the same code path).
func NewLatencyReadSource(rs turbodata.ReadSource, rtt time.Duration, perStreamBW int64) *LatencyReadSource {
	return &LatencyReadSource{rs: rs, rtt: rtt, perStreamBW: perStreamBW}
}

func (l *LatencyReadSource) sleep(n int) {
	if l.rtt > 0 {
		time.Sleep(l.rtt)
	}
	if l.perStreamBW > 0 && n > 0 {
		d := time.Duration(float64(n) / float64(l.perStreamBW) * float64(time.Second))
		if d > 0 {
			time.Sleep(d)
		}
	}
}

func (l *LatencyReadSource) Read(p []byte) (int, error) {
	n, err := l.rs.Read(p)
	l.sleep(n)
	return n, err
}

func (l *LatencyReadSource) Seek(offset int64, whence int) (int64, error) {
	return l.rs.Seek(offset, whence)
}

func (l *LatencyReadSource) ReadAt(p []byte, off int64) (int, error) {
	n, err := l.rs.ReadAt(p, off)
	l.sleep(n)
	return n, err
}

// Compile-time check.
var _ interface {
	io.ReadSeeker
	io.ReaderAt
} = (*LatencyReadSource)(nil)
