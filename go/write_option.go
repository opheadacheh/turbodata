package turbodata

import "fmt"

type WriteOption func(metadata map[string]any) error

func WithChunkConfig(chunkConfig *ChunkConfig) WriteOption {
	return func(metadata map[string]any) error {
		metadata["chunk_config"] = chunkConfig
		return nil
	}
}

func WithCompression() WriteOption {
	return func(metadata map[string]any) error {
		metadata["is_compressed"] = true
		return nil
	}
}

// validVideoCodecs is the set of codec strings accepted by WithVideoTopic.
// These match Foxglove's CompressedVideo.format values for direct ecosystem interop.
var validVideoCodecs = map[string]struct{}{
	"h264": {},
	"h265": {},
	"vp9":  {},
	"av1":  {},
}

// WithVideoTopic marks the group as carrying compressed video frames of the
// given codec. Constraints:
//
//   - The group MUST contain exactly one topic (enforced in OpenTopics).
//   - The group MUST NOT also use WithCompression (the codec already
//     compresses; layering zstd buys ~nothing and forces whole-chunk
//     decompression on read).
//   - Callers MUST use Writer.WriteVideoMessage instead of WriteMessage.
//   - First message on the topic MUST be a key frame.
//   - v1 forbids B-frames; the topic metadata records has_b_frames=false.
//
// Bitstream contract (documented, not validated):
//
//   - One coded frame per message.
//   - h264 / h265: Annex-B NAL units. Parameter sets (SPS/PPS for h264;
//     VPS/SPS/PPS for h265) in-band on every key frame.
//   - av1: Low Overhead Bitstream Format (LOBF, AV1 spec §5.2). Sequence
//     Header OBU in-band on every key frame.
//   - vp9: bitstream units per frame; Uncompressed Frame Header on key frames.
func WithVideoTopic(codec string) WriteOption {
	return func(metadata map[string]any) error {
		if _, ok := validVideoCodecs[codec]; !ok {
			return fmt.Errorf("codec %q: %w", codec, ErrUnknownVideoCodec)
		}
		if v, ok := metadata["is_compressed"].(bool); ok && v {
			return ErrVideoTopicCannotBeCompressed
		}
		metadata["is_video"] = true
		metadata["codec"] = codec
		metadata["has_b_frames"] = false
		return nil
	}
}
