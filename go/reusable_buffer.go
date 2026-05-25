package turbodata

import "turbodata/internal/buffer"

// ReusableBuffer is a growable byte buffer designed to be reused across calls
// to MessageIterator.NextInto, avoiding per-message allocations.
type ReusableBuffer = buffer.ReusableBuffer

// NewReusableBuffer returns a freshly-allocated buffer with zero length.
var NewReusableBuffer = buffer.NewReusableBuffer
