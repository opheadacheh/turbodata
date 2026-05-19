package turbodata

import (
	"github.com/klauspost/compress/zstd"
)

// encoder and decoder are package-level singletons. EncodeAll/DecodeAll
// create an independent context per call and are safe for concurrent use.
var (
	encoder *zstd.Encoder
	decoder *zstd.Decoder
)

func init() {
	var err error
	encoder, err = zstd.NewWriter(nil)
	if err != nil {
		panic("turbodata: failed to initialize zstd encoder: " + err.Error())
	}
	decoder, err = zstd.NewReader(nil)
	if err != nil {
		panic("turbodata: failed to initialize zstd decoder: " + err.Error())
	}
}

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
