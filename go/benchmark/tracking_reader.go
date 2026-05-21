package benchmark

import "io"

// TrackingReadSeeker wraps an io.ReadSeeker and counts reads, seeks, and bytes transferred.
// Stats accumulate across calls; call Reset to clear them.
type TrackingReadSeeker struct {
	rs        io.ReadSeeker
	ReadBytes int64
	ReadCalls int64
	SeekCalls int64
}

func NewTrackingReadSeeker(rs io.ReadSeeker) *TrackingReadSeeker {
	return &TrackingReadSeeker{rs: rs}
}

func (t *TrackingReadSeeker) Read(p []byte) (int, error) {
	n, err := t.rs.Read(p)
	t.ReadBytes += int64(n)
	t.ReadCalls++
	return n, err
}

func (t *TrackingReadSeeker) Seek(offset int64, whence int) (int64, error) {
	t.SeekCalls++
	return t.rs.Seek(offset, whence)
}

func (t *TrackingReadSeeker) Reset() {
	t.ReadBytes = 0
	t.ReadCalls = 0
	t.SeekCalls = 0
}

// TrackingWriter wraps an io.Writer and counts write calls and bytes transferred.
// Stats accumulate across calls; call Reset to clear them.
type TrackingWriter struct {
	w          io.Writer
	WriteBytes int64
	WriteCalls int64
}

func NewTrackingWriter(w io.Writer) *TrackingWriter {
	return &TrackingWriter{w: w}
}

func (t *TrackingWriter) Write(p []byte) (int, error) {
	n, err := t.w.Write(p)
	t.WriteBytes += int64(n)
	t.WriteCalls++
	return n, err
}

func (t *TrackingWriter) Reset() {
	t.WriteBytes = 0
	t.WriteCalls = 0
}
