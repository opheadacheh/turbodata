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
