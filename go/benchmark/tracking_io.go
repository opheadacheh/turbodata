// Copyright 2026 Wanjia He
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package benchmark

import "io"

// TrackingReadSeeker wraps a Read+Seek+ReadAt source and counts reads, seeks,
// ReadAts, and bytes transferred. Stats accumulate across calls; call Reset
// to clear them.
//
// Implements turbodata.ReadSource so it can be passed to the cost-aware path.
type TrackingReadSeeker struct {
	rs interface {
		io.ReadSeeker
		io.ReaderAt
	}
	ReadBytes   int64
	ReadCalls   int64
	SeekCalls   int64
	ReadAtBytes int64
	ReadAtCalls int64
}

func NewTrackingReadSeeker(rs interface {
	io.ReadSeeker
	io.ReaderAt
}) *TrackingReadSeeker {
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

func (t *TrackingReadSeeker) ReadAt(p []byte, off int64) (int, error) {
	n, err := t.rs.ReadAt(p, off)
	t.ReadAtBytes += int64(n)
	t.ReadAtCalls++
	return n, err
}

func (t *TrackingReadSeeker) Reset() {
	t.ReadBytes = 0
	t.ReadCalls = 0
	t.SeekCalls = 0
	t.ReadAtBytes = 0
	t.ReadAtCalls = 0
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
