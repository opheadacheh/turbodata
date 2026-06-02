// Domain types for turbodata. Mirrors go/types.go.
//
// Numeric type convention (matches @mcap/core):
//   - Go int64 -> bigint
//   - Go uint8 / uint16 / uint32 -> number

export interface Footer {
  /** uncompressed: int64. Length of the (zstd-compressed) summary block. */
  summaryLen: bigint;
  /** Always equals "7URB0" for valid files. */
  magic: Uint8Array;
}

export interface Summary {
  topicsInfos: TopicsInfo[];
}

export interface TopicsInfo {
  topicMetadatas: TopicMetadata[];
  indexChunkInfoList: IndexChunkInfo[];
  /**
   * int64. Total length of all index chunks for this topic group, in bytes
   * (compressed). Used by the reader to compute the length of the *last*
   * index chunk in the group.
   */
  totalLen: bigint;
}

export interface TopicMetadata {
  /** uint16 */
  id: number;
  name: string;
  /**
   * Decoded msgpack map. Int64 values are decoded as bigint (via
   * `useBigInt64: true`); smaller integers stay as number.
   */
  metadata: Map<string, unknown>;
}

export interface IndexChunkInfo {
  /** int64 */
  startTimestamp: bigint;
  /** int64 */
  endTimestamp: bigint;
  /** int64. Absolute file offset where the (compressed) index chunk starts. */
  offset: bigint;
}

export interface IndexChunk {
  topicIndexes: TopicIndex[];
  /** int64. Absolute file offset of this index chunk's data. */
  chunkOffset: bigint;
  /** int64. On-disk (possibly compressed) data length in bytes. */
  chunkLen: bigint;
  /** int64. Decompressed data length, used to compute the last message's length. */
  uncompressedLen: bigint;
}

export interface TopicIndex {
  /** uint16 */
  id: number;
  messageIndexes: MessageIndex[];
  /**
   * uint32 entries: ascending positions into `messageIndexes` marking key
   * frames. Consumed by the video-decodable read/sample paths to anchor GOPs.
   */
  keyFrameIndexes: Uint32Array;
}

export interface MessageIndex {
  /** int64 */
  timestamp: bigint;
  /** int64. Offset relative to the start of the (uncompressed) data chunk. */
  offsetInChunk: bigint;
}

/** The 5 magic bytes that close every turbodata file: "7URB0". */
export const MAGIC: Uint8Array = new Uint8Array([0x37, 0x55, 0x52, 0x42, 0x30]);

/** Footer is fixed at 13 bytes: 8 (summaryLen) + 5 (magic). */
export const FOOTER_LEN = 13;

// Internal per-topic metadata keys used to carry write-format control flags
// alongside caller-supplied metadata. The __td_ prefix namespaces them away
// from user keys. The reader reads these back to drive decompression and
// video-aware sampling.
export const META_KEY_COMPRESSED = "__td_is_compressed";
export const META_KEY_VIDEO = "__td_is_video";
export const META_KEY_CHUNK_CONFIG = "__td_chunk_config";
