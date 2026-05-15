package turbodata

import (
	"github.com/klauspost/compress/zstd"
)

var (
	decoder, _ = zstd.NewReader(nil)
	encoder, _ = zstd.NewWriter(nil)
)

func compress(data []byte) ([]byte, error) {
	return encoder.EncodeAll(data, nil), nil
}

func compressInto(data []byte, into *ReusableBuffer) {
	into.Data = into.Data[:0]
	into.Data = encoder.EncodeAll(data, into.Data)
}

func decompress(data []byte) ([]byte, error) {
	decompressed, err := decoder.DecodeAll(data, nil)
	if err != nil {
		return nil, err
	}
	return decompressed, nil
}

func decompressInto(data []byte, into *ReusableBuffer) error {
	into.Data = into.Data[:0]
	var err error
	into.Data, err = decoder.DecodeAll(data, into.Data)
	return err
}
