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

package turbodata

import "github.com/opheadacheh/turbodata/go/format"

type WriteOption func(metadata map[string]any) error

func WithChunkConfig(chunkConfig *ChunkConfig) WriteOption {
	return func(metadata map[string]any) error {
		metadata[format.MetaKeyChunkConfig] = chunkConfig
		return nil
	}
}

func WithCompression() WriteOption {
	return func(metadata map[string]any) error {
		metadata[format.MetaKeyCompressed] = true
		return nil
	}
}

// WithVideoTopic marks the group as carrying compressed video frames. The
// commitment this option makes:
//
//   - On write, chunk boundaries are gated on key frames so a GOP is never
//     split across two chunks. Callers MUST use Writer.WriteVideoMessage
//     (which takes an isKeyFrame flag) instead of WriteMessage.
//   - On read, the Sample API returns the anchor key frame together with
//     the target frame (Frames + ResetDecoder, see SampleResult); the
//     ReadMessages WithVideoDecodable option snaps StartTimestamp back to
//     the nearest key frame so the decoder can start cold.
//
// Constraints enforced at OpenTopics:
//
//   - The group MUST contain exactly one topic.
//   - The group MUST NOT also use WithCompression: the codec already
//     compresses the bytes, and chunk-level compression would force a
//     whole-chunk decompression on read.
//
// First message on a video topic MUST be a key frame.
//
// The codec, frame-type structure, and bitstream framing are caller
// concerns. If the caller wants to record codec info for downstream
// consumers, they pass it in the per-topic metadata map themselves.
func WithVideoTopic() WriteOption {
	return func(metadata map[string]any) error {
		metadata[format.MetaKeyVideo] = true
		return nil
	}
}
