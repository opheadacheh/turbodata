package iter

import "io"

// ReadSource is the combined I/O capability the Reader expects from its
// underlying storage. It is satisfied natively by *os.File and *bytes.Reader.
//
// Concurrency contract: ReadAt must be safe for concurrent calls (per
// io.ReaderAt's documentation). The cost-aware reader path invokes ReadAt
// from multiple goroutines simultaneously. Read and Seek may be stateful;
// the cost-aware path does not call them. The default (non-cost-aware)
// path calls Read and Seek serially.
type ReadSource interface {
	io.ReadSeeker
	io.ReaderAt
}
