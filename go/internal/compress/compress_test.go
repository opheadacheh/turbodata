package compress

import (
	"bytes"
	"testing"

	"turbodata/internal/buffer"
)

func TestCompressDecompressRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name    string
		payload []byte
	}{
		{
			name:    "empty",
			payload: []byte{},
		},
		{
			name:    "small_text",
			payload: []byte("hello compression"),
		},
		{
			name:    "binary_with_zeros",
			payload: []byte{0x00, 0x01, 0x00, 0xff, 0x10, 0x00, 0x7f},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			compressed, err := Compress(tc.payload)
			if err != nil {
				t.Errorf("Compress: %v", err)
				return
			}

			decompressed, err := Decompress(compressed)
			if err != nil {
				t.Errorf("Decompress: %v", err)
				return
			}

			if !bytes.Equal(tc.payload, decompressed) {
				t.Errorf("decompressed payload mismatch: want %v, got %v", tc.payload, decompressed)
			}
		})
	}
}

func TestCompressInto(t *testing.T) {
	payload := []byte("payload for CompressInto")
	buf := &buffer.ReusableBuffer{Data: []byte("junk data that should be reset")}

	CompressInto(payload, buf)

	decompressed, err := Decompress(buf.Data)
	if err != nil {
		t.Errorf("Decompress after CompressInto: %v", err)
		return
	}

	if !bytes.Equal(payload, decompressed) {
		t.Errorf("roundtrip mismatch: want %v, got %v", payload, decompressed)
	}
}

func TestDecompressInto(t *testing.T) {
	payload := []byte("payload for DecompressInto")
	compressed, err := Compress(payload)
	if err != nil {
		t.Errorf("Compress: %v", err)
		return
	}

	buf := &buffer.ReusableBuffer{Data: []byte("junk data that should be reset")}
	err = DecompressInto(compressed, buf)
	if err != nil {
		t.Errorf("DecompressInto: %v", err)
		return
	}

	if !bytes.Equal(payload, buf.Data) {
		t.Errorf("DecompressInto output mismatch: want %v, got %v", payload, buf.Data)
	}
}

func TestDecompressInvalidData(t *testing.T) {
	invalid := []byte("this is not zstd data")

	_, err := Decompress(invalid)
	if err == nil {
		t.Errorf("Decompress should fail for invalid input, got nil error")
		return
	}
}

func TestDecompressIntoInvalidData(t *testing.T) {
	invalid := []byte("this is not zstd data")
	buf := &buffer.ReusableBuffer{Data: []byte("junk")}

	err := DecompressInto(invalid, buf)
	if err == nil {
		t.Errorf("DecompressInto should fail for invalid input, got nil error")
		return
	}
}
