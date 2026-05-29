package buffer

type ReusableBuffer struct {
	Data []byte
}

func NewReusableBuffer() *ReusableBuffer {
	return &ReusableBuffer{Data: make([]byte, 0)}
}

func (b *ReusableBuffer) Prepare(n int) {
	if cap(b.Data) < n {
		b.Data = make([]byte, n)
		return
	}

	b.Data = b.Data[:n]
}
