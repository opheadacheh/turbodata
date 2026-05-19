package turbodata

import "testing"

func TestNewReusableBuffer(t *testing.T) {
	b := NewReusableBuffer()
	if b == nil {
		t.Fatal("expected non-nil buffer")
	}

	if len(b.Data) != 0 {
		t.Fatalf("expected len(Data) == 0, got %d", len(b.Data))
	}
}

func TestReusableBufferPrepare(t *testing.T) {
	t.Run("within existing capacity", func(t *testing.T) {
		b := &ReusableBuffer{
			Data: make([]byte, 3, 8),
		}
		originalCap := cap(b.Data)

		b.Prepare(6)

		if len(b.Data) != 6 {
			t.Fatalf("expected len(Data) == 6, got %d", len(b.Data))
		}

		if cap(b.Data) != originalCap {
			t.Fatalf("expected cap(Data) to remain %d, got %d", originalCap, cap(b.Data))
		}
	})

	t.Run("grows when requested length exceeds capacity", func(t *testing.T) {
		b := &ReusableBuffer{
			Data: make([]byte, 2, 4),
		}

		b.Prepare(10)

		if len(b.Data) != 10 {
			t.Fatalf("expected len(Data) == 10, got %d", len(b.Data))
		}

		if cap(b.Data) < 10 {
			t.Fatalf("expected cap(Data) >= 10, got %d", cap(b.Data))
		}
	})

	t.Run("prepare zero length", func(t *testing.T) {
		b := &ReusableBuffer{
			Data: make([]byte, 2, 4),
		}

		b.Prepare(0)

		if len(b.Data) != 0 {
			t.Fatalf("expected len(Data) == 0, got %d", len(b.Data))
		}
	})
}
