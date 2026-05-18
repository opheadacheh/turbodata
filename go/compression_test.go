package turbodata

import (
	"testing"
)

func TestCompressDecompress(t *testing.T) {
	data := []byte("hello")
	encoded, err := compress(data)
	if err != nil {
		t.Fatalf("failed to encode: %v", err)
	}

	decoded, err := decompress(encoded)
	if err != nil {
		t.Fatalf("failed to decode: %v", err)
	}

	if string(decoded) != string(data) {
		t.Fatalf("decoded data mismatch: expected %s, got %s", string(data), string(decoded))
	}
}

func TestCompressDecompressInto(t *testing.T) {
	data := []byte("hello")
	in := &ReusableBuffer{}
	compressInto(data, in)

	out := &ReusableBuffer{}
	err := decompressInto(in.Data, out)
	if err != nil {
		t.Fatalf("failed to decode: %v", err)
	}
	if string(out.Data) != string(data) {
		t.Fatalf("decoded data mismatch: expected %s, got %s", string(data), string(out.Data))
	}
}
