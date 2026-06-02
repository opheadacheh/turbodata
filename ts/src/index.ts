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
