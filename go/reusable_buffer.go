package turbodata

type ReusableBuffer struct {
	Data []byte
}

func NewReusableBuffer() *ReusableBuffer {
	return &ReusableBuffer{Data: make([]byte, 0)}
}

func (b *ReusableBuffer) Prepare(len int) {
	if cap(b.Data) < len {
		b.Data = make([]byte, len)
		return
	}

	b.Data = b.Data[:len]
}
