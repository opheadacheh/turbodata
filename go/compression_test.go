package turbodata

import (
	"bytes"
	"testing"
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
			compressed, err := compress(tc.payload)
			if err != nil {
				t.Errorf("compress: %v", err)
				return
			}

			decompressed, err := decompress(compressed)
			if err != nil {
				t.Errorf("decompress: %v", err)
				return
			}

			if !bytes.Equal(tc.payload, decompressed) {
				t.Errorf("decompressed payload mismatch: want %v, got %v", tc.payload, decompressed)
			}
		})
	}
}

func TestCompressInto(t *testing.T) {
	payload := []byte("payload for compressInto")
	buffer := &ReusableBuffer{Data: []byte("junk data that should be reset")}

	compressInto(payload, buffer)

	decompressed, err := decompress(buffer.Data)
	if err != nil {
		t.Errorf("decompress after compressInto: %v", err)
		return
	}

	if !bytes.Equal(payload, decompressed) {
		t.Errorf("roundtrip mismatch: want %v, got %v", payload, decompressed)
	}
}

func TestDecompressInto(t *testing.T) {
	payload := []byte("payload for decompressInto")
	compressed, err := compress(payload)
	if err != nil {
		t.Errorf("compress: %v", err)
		return
	}

	buffer := &ReusableBuffer{Data: []byte("junk data that should be reset")}
	err = decompressInto(compressed, buffer)
	if err != nil {
		t.Errorf("decompressInto: %v", err)
		return
	}

	if !bytes.Equal(payload, buffer.Data) {
		t.Errorf("decompressInto output mismatch: want %v, got %v", payload, buffer.Data)
	}
}

func TestDecompressInvalidData(t *testing.T) {
	invalid := []byte("this is not zstd data")

	_, err := decompress(invalid)
	if err == nil {
		t.Errorf("decompress should fail for invalid input, got nil error")
		return
	}
}

func TestDecompressIntoInvalidData(t *testing.T) {
	invalid := []byte("this is not zstd data")
	buffer := &ReusableBuffer{Data: []byte("junk")}

	err := decompressInto(invalid, buffer)
	if err == nil {
		t.Errorf("decompressInto should fail for invalid input, got nil error")
		return
	}
}
