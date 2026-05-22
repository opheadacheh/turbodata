// Pluggable zstd decompression. Default backend is fzstd (pure JS, sync).

import { decompress as fzstdDecompress } from "fzstd";

/**
 * Decompress a zstd-compressed buffer. Must accept the exact bytes produced
 * by github.com/klauspost/compress/zstd's EncodeAll (i.e. the standard zstd
 * frame format).
 */
export type Decompressor = (compressed: Uint8Array) => Uint8Array;

export const defaultDecompressor: Decompressor = (compressed) => {
  return fzstdDecompress(compressed);
};
