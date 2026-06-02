// Copyright 2026 Wanjia He
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compress

import (
	"github.com/klauspost/compress/zstd"

	"github.com/opheadacheh/turbodata/go/internal/buffer"
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
		panic("github.com/opheadacheh/turbodata/go/compress: failed to initialize zstd encoder: " + err.Error())
	}
	decoder, err = zstd.NewReader(nil)
	if err != nil {
		panic("github.com/opheadacheh/turbodata/go/compress: failed to initialize zstd decoder: " + err.Error())
	}
}

func Compress(data []byte) ([]byte, error) {
	return encoder.EncodeAll(data, nil), nil
}

func CompressInto(data []byte, into *buffer.ReusableBuffer) {
	into.Data = into.Data[:0]
	into.Data = encoder.EncodeAll(data, into.Data)
}

func Decompress(data []byte) ([]byte, error) {
	decompressed, err := decoder.DecodeAll(data, nil)
	if err != nil {
		return nil, err
	}
	return decompressed, nil
}

func DecompressInto(data []byte, into *buffer.ReusableBuffer) error {
	into.Data = into.Data[:0]
	var err error
	into.Data, err = decoder.DecodeAll(data, into.Data)
	return err
}
