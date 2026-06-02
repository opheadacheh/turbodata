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

// Public API surface.

export {
  Reader,
  SampleValidationError,
  TopicRemapError,
  DEFAULT_SAMPLE_STRATEGY,
  linSpaceTimestamps,
} from "./reader.js";
export type {
  Frame,
  Message,
  ReaderOptions,
  SampleQuery,
  SampleResult,
  SampleOptions,
} from "./reader.js";

export { MultiReader, VideoSourcesOverlapError } from "./multi_reader.js";

export type { ReadOptions, Order } from "./read_options.js";
export { MAX_INT64 } from "./read_options.js";

export type { ReadStrategy } from "./read_strategy.js";
export {
  StrategyForLatency,
  StrategyForMoney,
  StrategyForBlended,
} from "./read_strategy.js";

export type { ReadSource } from "./read_source.js";
export { BlobReadSource } from "./sources/blob.js";
export { HttpRangeReadSource } from "./sources/http.js";
export type { HttpRangeReadSourceOptions } from "./sources/http.js";

export type { Decompressor } from "./compression.js";
export { defaultDecompressor } from "./compression.js";

export type {
  Footer,
  Summary,
  TopicsInfo,
  TopicMetadata,
  IndexChunkInfo,
  IndexChunk,
  TopicIndex,
  MessageIndex,
} from "./types.js";
export {
  META_KEY_COMPRESSED,
  META_KEY_VIDEO,
  META_KEY_CHUNK_CONFIG,
} from "./types.js";
