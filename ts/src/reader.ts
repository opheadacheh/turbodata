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

// Reader: top-level entry point. Mirrors go/reader.go and go/message_iterator.go.

import type { Decompressor } from "./compression.js";
import { defaultDecompressor } from "./compression.js";
import { BinaryReader, readFooter, readIndexChunk, readSummary } from "./io.js";
import { PreloadedTopicsGroupIterator } from "./iterators/preloaded_topics_group_iterator.js";
import { MessageHeap } from "./iterators/message_heap.js";
import { TopicsGroupIterator } from "./iterators/topics_group_iterator.js";
import { LoadedBytes } from "./loaded_bytes.js";
import { fetchAll } from "./read_fetcher.js";
import { plan, type Range } from "./read_planner.js";
import type { ReadOptions } from "./read_options.js";
import { MAX_INT64 } from "./read_options.js";
import type { ReadSource } from "./read_source.js";
import type { ReadStrategy } from "./read_strategy.js";
import { sampleMessages, type SampleSpec } from "./sample.js";
import { sortAndFilter, type MessageRef } from "./sort_and_filter.js";
import {
  FOOTER_LEN,
  MAGIC,
  META_KEY_COMPRESSED,
  META_KEY_VIDEO,
  type IndexChunk,
  type IndexChunkInfo,
  type Summary,
  type TopicsInfo,
} from "./types.js";

export interface Message {
  /** int64 nanosecond (or arbitrary unit) timestamp from the writer. */
  timestamp: bigint;
  topicName: string;
  /**
   * Message bytes. By default this aliases an internal reusable buffer and
   * is only valid until the next iteration step. Pass `{ copy: true }` to
   * receive a fresh Uint8Array per message.
   */
  data: Uint8Array;
}

export interface ReaderOptions {
  /** Override the default fzstd decompressor (e.g. for a WASM zstd build). */
  decompress?: Decompressor;
  /**
   * Present in-file topic names under different exposed names. Keyed by in-file
   * name, valued by the exposed name emitted from readMessages, accepted by
   * ReadOptions.topicNames and SampleQuery.topic, and reported by summary().
   * Names absent from the map pass through unchanged; omitting it means
   * identity (no behavior change).
   *
   * Validated lazily against the file's summary on first use: it throws a
   * TopicRemapError when two topics collapse onto the same exposed name
   * (whether two in-file names map to the same target, or a renamed name
   * collides with an untouched in-file name).
   */
  topicRemap?: Record<string, string>;
}

/**
 * Thrown when a configured topic remap collapses two in-file topics onto the
 * same exposed name. Detected lazily against the summary on first use.
 */
export class TopicRemapError extends Error {
  readonly exposed: string;
  readonly first: string;
  readonly second: string;
  constructor(exposed: string, first: string, second: string) {
    super(
      `turbodata: topic remap produces duplicate exposed name "${exposed}" (from in-file topics "${first}" and "${second}")`,
    );
    this.name = "TopicRemapError";
    this.exposed = exposed;
    this.first = first;
    this.second = second;
  }
}

/**
 * One sampling request: the floor message of `topic` at each timestamp.
 * Within `timestamps`, values must be strictly increasing, and the same
 * `topic` must not appear in more than one query in a single sample() call.
 */
export interface SampleQuery {
  topic: string;
  /** int64 timestamps; strictly increasing. */
  timestamps: bigint[];
}

/**
 * One coded video frame surfaced as part of a SampleResult's GOP prefix or
 * incremental tail. `data` is an owned copy and is safe to retain.
 */
export interface Frame {
  timestamp: bigint;
  isKeyFrame: boolean;
  data: Uint8Array;
}

/**
 * One returned floor message. out[i][j] corresponds to queries[i].timestamps[j].
 * `found` is false when there is no message with timestamp <= timestamps[j]
 * for the topic.
 *
 * Without videoDecodable, every topic (including video topics) returns its
 * floor message in `data` (an owned copy), `isVideo` is false, and `frames` /
 * `resetDecoder` are unset.
 *
 * With videoDecodable, results for video topics instead carry a decoder-ready
 * GOP sequence and set `isVideo` true. For those results, within a single row
 * (one topic, strictly increasing query timestamps):
 *
 *   - `data` is empty; the frame bytes live in `frames` and the target frame
 *     is the last element. `timestamp` names that target frame.
 *   - For the first found result in each GOP encountered in the row:
 *     `resetDecoder` is true, `frames` = [keyframe ... target] in storage
 *     (decode) order. The decoder must reset before feeding.
 *   - For subsequent found results within the same GOP: `resetDecoder` is
 *     false, `frames` = [previous_target+1 ... target] (only the new frames).
 *     `frames` is empty when two queries resolve to the same target frame;
 *     `timestamp` still names that target.
 */
export interface SampleResult {
  found: boolean;
  timestamp: bigint;
  data: Uint8Array;
  isVideo: boolean;
  frames: Frame[];
  resetDecoder: boolean;
}

/** Options for Reader.sample(). */
export interface SampleOptions {
  /**
   * Cost model for the concurrent reads. Defaults to DEFAULT_SAMPLE_STRATEGY,
   * which is sized for a cloud-object / in-region profile.
   */
  strategy?: ReadStrategy;
  /**
   * int64. Bytes to read speculatively from the file tail when loading the
   * summary. Same semantics as ReadOptions.tailPrefetch.
   */
  tailPrefetch?: bigint;
  /**
   * When true, results for video topics carry a decoder-ready GOP sequence
   * (frames + resetDecoder) and set isVideo. See SampleResult. Non-video
   * topics are unaffected. Default: false.
   */
  videoDecodable?: boolean;
}

/**
 * Default strategy for Reader.sample when none is supplied. Mirrors the Go and
 * Python DEFAULT_SAMPLE_STRATEGY (1 MiB coalesce, 4 MiB split, 16-way fanout).
 */
export const DEFAULT_SAMPLE_STRATEGY: ReadStrategy = {
  coalesceGap: 1n << 20n,
  splitThreshold: 4n << 20n,
  maxConcurrency: 16,
};

/**
 * Aggregated precondition failures from Reader.sample, raised before any data
 * I/O. `violations` lists every problem found across all queries.
 */
export class SampleValidationError extends Error {
  readonly violations: string[];
  constructor(violations: string[]) {
    super(
      `sample: ${violations.length} validation error(s): ${violations.join("; ")}`,
    );
    this.name = "SampleValidationError";
    this.violations = violations;
  }
}

/**
 * N timestamps starting at `start`, each `stride` apart. Useful for "N samples
 * at a given frequency from T" patterns. Mirrors Go LinSpaceTimestamps.
 * Returns [] when count <= 0; throws when stride <= 0 (would break the
 * strictly-increasing contract on SampleQuery.timestamps).
 */
export function linSpaceTimestamps(
  start: bigint,
  stride: bigint,
  count: number,
): bigint[] {
  if (count <= 0) {
    return [];
  }
  if (stride <= 0n) {
    throw new Error("linSpaceTimestamps requires stride > 0");
  }
  const out = new Array<bigint>(count);
  for (let i = 0; i < count; i++) {
    out[i] = start + BigInt(i) * stride;
  }
  return out;
}

/**
 * Common shape of a per-group iterator: returns the next message or
 * undefined on EOF. Both the default-path and cost-aware iterators are
 * adapted to this shape inside the Reader.
 */
interface GroupIt {
  next(): Promise<
    { timestamp: bigint; topicId: number; data: Uint8Array } | undefined
  >;
}

/**
 * Per-exposed-topic span used by MultiReader. `isVideo` flags a video topic
 * (always a single-topic group); [minTs, maxTs] is the inclusive timestamp
 * span of the topic's messages. Mirrors go topicBound / py _TopicBound.
 *
 * @internal
 */
export interface TopicBound {
  isVideo: boolean;
  minTs: bigint;
  maxTs: bigint;
}

export class Reader {
  private readonly rs: ReadSource;
  private readonly decompress: Decompressor;

  // Lazy-loaded summary metadata.
  private cachedSize: bigint | undefined;
  private cachedSummary: Summary | undefined;
  // Cached per-exposed-topic bounds, computed lazily by topicBounds().
  private boundsCache: Map<string, TopicBound> | undefined;

  // topicRemap maps in-file -> exposed names; undefined means identity.
  private readonly topicRemap: Map<string, string> | undefined;
  // Cached prepareRename outputs (computed lazily once the summary is known).
  // renameInverse maps exposed -> in-file; renameErr captures a collision
  // detected against the actual summary.
  private renamePrepared = false;
  private renameInverse: Map<string, string> | undefined;
  private renameErr: TopicRemapError | undefined;

  constructor(rs: ReadSource, opts: ReaderOptions = {}) {
    this.rs = rs;
    this.decompress = opts.decompress ?? defaultDecompressor;
    const remap = opts.topicRemap;
    this.topicRemap =
      remap !== undefined && Object.keys(remap).length > 0
        ? new Map(Object.entries(remap))
        : undefined;
  }

  /**
   * Build the exposed -> in-file inverse map once the summary is known and
   * validate that no two topics collapse onto the same exposed name. Results
   * (including any error) are cached. Returns undefined when no remap is set.
   */
  private prepareRename(summary: Summary): Map<string, string> | undefined {
    if (this.renamePrepared) {
      if (this.renameErr !== undefined) {
        throw this.renameErr;
      }
      return this.renameInverse;
    }
    this.renamePrepared = true;
    if (this.topicRemap === undefined) {
      return undefined;
    }
    const inverse = new Map<string, string>();
    for (const ti of summary.topicsInfos) {
      for (const tm of ti.topicMetadatas) {
        const exposed = this.topicRemap.get(tm.name) ?? tm.name;
        const prev = inverse.get(exposed);
        if (prev !== undefined) {
          this.renameErr = new TopicRemapError(exposed, prev, tm.name);
          throw this.renameErr;
        }
        inverse.set(exposed, tm.name);
      }
    }
    this.renameInverse = inverse;
    return inverse;
  }

  /**
   * Return a copy of `summary` with TopicMetadata.name rewritten to exposed
   * names. Index-chunk lists and metadata maps are shared (read-only here).
   * Returns the original summary when no remap is configured.
   */
  private exposedSummary(summary: Summary): Summary {
    if (this.topicRemap === undefined) {
      return summary;
    }
    this.prepareRename(summary);
    return {
      topicsInfos: summary.topicsInfos.map((ti) => ({
        topicMetadatas: ti.topicMetadatas.map((tm) => ({
          id: tm.id,
          name: this.topicRemap!.get(tm.name) ?? tm.name,
          metadata: tm.metadata,
        })),
        indexChunkInfoList: ti.indexChunkInfoList,
        totalLen: ti.totalLen,
      })),
    };
  }

  /**
   * Returns the parsed summary, loading and caching it on first call. When a
   * topic remap is configured, TopicMetadata.name reports exposed names.
   */
  async summary(): Promise<Summary> {
    return this.exposedSummary(await this.summaryWithHint(0n));
  }

  private async summaryWithHint(prefetch: bigint): Promise<Summary> {
    if (this.cachedSummary !== undefined) {
      return this.cachedSummary;
    }
    if (this.cachedSize === undefined) {
      this.cachedSize = await this.rs.size();
    }
    const size = this.cachedSize;
    if (size < BigInt(FOOTER_LEN)) {
      throw new Error("file is too small to contain a footer");
    }

    let pf = prefetch;
    if (pf <= 0n) {
      pf = BigInt(FOOTER_LEN);
    }
    if (pf > size) {
      pf = size;
    }
    const tail = await this.rs.read(size - pf, pf);

    const footerStart = tail.byteLength - FOOTER_LEN;
    if (footerStart < 0) {
      throw new Error("tail read shorter than footer");
    }
    const footer = readFooter(
      new BinaryReader(tail.subarray(footerStart)),
    );
    if (!magicEquals(footer.magic, MAGIC)) {
      throw new Error("invalid magic number");
    }

    const summaryLen = footer.summaryLen;
    let compressedSummary: Uint8Array;
    const tailLen = BigInt(tail.byteLength);
    if (tailLen >= summaryLen + BigInt(FOOTER_LEN)) {
      const start = Number(tailLen - BigInt(FOOTER_LEN) - summaryLen);
      compressedSummary = tail.subarray(start, start + Number(summaryLen));
    } else {
      compressedSummary = await this.rs.read(
        size - summaryLen - BigInt(FOOTER_LEN),
        summaryLen,
      );
    }

    const decompressed = this.decompress(compressedSummary);
    const summary = readSummary(new BinaryReader(decompressed));
    this.cachedSummary = summary;
    return summary;
  }

  /**
   * Read messages. Returns an async iterable; iterating consumes lazily.
   *
   * Concurrency note: because the iterator carries internal mutable state
   * (cursors and a reusable buffer), only one iteration at a time over the
   * same returned async iterable is supported.
   */
  readMessages(opts: ReadOptions = {}): AsyncIterableIterator<Message> {
    const reader = this;
    const tailPrefetch = opts.tailPrefetch ?? 0n;
    const startTimestamp = opts.startTimestamp ?? 0n;
    const endTimestamp = opts.endTimestamp ?? MAX_INT64;
    const reverse = opts.order === "reverse-time";
    const wantedNames = opts.topicNames;
    const strategy = opts.strategy;
    const copy = opts.copy === true;
    const videoDecodable = opts.videoDecodable === true;

    let initialized = false;
    let exhausted = false;

    let groupIts: GroupIt[] = [];
    const heap = new MessageHeap<{
      groupIdx: number;
      topicId: number;
      data: Uint8Array;
    }>(reverse);
    const idToName = new Map<number, string>();

    const init = async (): Promise<void> => {
      const summary = await reader.summaryWithHint(tailPrefetch);

      // When a remap is configured, caller-supplied topicNames arrive in
      // exposed-name space. Internal matching stays in in-file space, so
      // translate exposed -> in-file (unknown names pass through and match
      // nothing). idToName is built in exposed space for output.
      const remap = reader.topicRemap;
      const inverse =
        remap !== undefined ? reader.prepareRename(summary) : undefined;

      // Build topic-name allowlist. Defaults to "all topics".
      let wantedSet: Set<string>;
      if (wantedNames !== undefined) {
        wantedSet =
          inverse !== undefined
            ? new Set(wantedNames.map((n) => inverse.get(n) ?? n))
            : new Set(wantedNames);
      } else {
        wantedSet = collectAllTopicNames(summary);
      }

      // Translate names -> ids; remember reverse mapping (exposed names).
      const topicIds = new Set<number>();
      for (const ti of summary.topicsInfos) {
        for (const tm of ti.topicMetadatas) {
          if (!wantedSet.has(tm.name)) {
            continue;
          }
          topicIds.add(tm.id);
          idToName.set(tm.id, remap?.get(tm.name) ?? tm.name);
        }
      }

      if (strategy !== undefined) {
        groupIts = await reader.prepareCostAware({
          summary,
          topicIds,
          wantedSet,
          startTimestamp,
          endTimestamp,
          reverse,
          strategy,
          videoDecodable,
        });
      } else {
        groupIts = reader.prepareDefault({
          summary,
          topicIds,
          wantedSet,
          startTimestamp,
          endTimestamp,
          reverse,
          videoDecodable,
        });
      }

      // Prime the heap with one message from each group.
      for (let i = 0; i < groupIts.length; i++) {
        const m = await groupIts[i]!.next();
        if (m === undefined) {
          continue;
        }
        heap.push(m.timestamp, {
          groupIdx: i,
          topicId: m.topicId,
          data: m.data,
        });
      }
    };

    const iter: AsyncIterableIterator<Message> = {
      [Symbol.asyncIterator]() {
        return iter;
      },
      async next(): Promise<IteratorResult<Message>> {
        if (exhausted) {
          return { value: undefined, done: true };
        }
        if (!initialized) {
          await init();
          initialized = true;
        }
        const top = heap.pop();
        if (top === undefined) {
          exhausted = true;
          return { value: undefined, done: true };
        }
        const out: Message = {
          timestamp: top.priority,
          topicName: idToName.get(top.value.topicId) ?? "",
          data: copy ? new Uint8Array(top.value.data) : top.value.data,
        };
        // Pull the next message from the same group and re-heap.
        const nm = await groupIts[top.value.groupIdx]!.next();
        if (nm !== undefined) {
          heap.push(nm.timestamp, {
            groupIdx: top.value.groupIdx,
            topicId: nm.topicId,
            data: nm.data,
          });
        }
        return { value: out, done: false };
      },
      async return(): Promise<IteratorResult<Message>> {
        exhausted = true;
        return { value: undefined, done: true };
      },
    };

    return iter;
  }

  /**
   * Floor-message lookup at concrete timestamps per topic. For each
   * (queries[i].topic, queries[i].timestamps[j]) pair, out[i][j] is the
   * message with the greatest timestamp <= timestamps[j], or found=false when
   * none exists.
   *
   * Preconditions, validated before any data I/O (all violations aggregated
   * into a single SampleValidationError):
   *   - each queries[i].timestamps must be strictly increasing
   *   - each queries[i].topic must be unique across all i
   *   - each queries[i].topic must exist in the file's summary
   *
   * All data I/O runs concurrently under the hood, governed by the strategy.
   */
  async sample(
    queries: SampleQuery[],
    opts: SampleOptions = {},
  ): Promise<SampleResult[][]> {
    const strategy = opts.strategy ?? DEFAULT_SAMPLE_STRATEGY;
    const videoDecodable = opts.videoDecodable === true;
    const summary = await this.summaryWithHint(opts.tailPrefetch ?? 0n);

    // When a remap is configured, queries arrive in exposed-name space.
    // Translate to in-file names before validation and the engine call;
    // SampleResult is positional, so no translation back is needed.
    if (this.topicRemap !== undefined) {
      const inverse = this.prepareRename(summary);
      queries = queries.map((q) => ({
        topic: inverse?.get(q.topic) ?? q.topic,
        timestamps: q.timestamps,
      }));
    }

    validateSampleQueries(queries, summary);

    const specs: SampleSpec[] = queries.map((q) => ({
      topic: q.topic,
      timestamps: q.timestamps,
    }));
    // SampleHit and SampleResult are structurally identical; the engine's
    // hits double as results.
    return sampleMessages(
      this.rs,
      summary,
      specs,
      strategy,
      this.decompress,
      videoDecodable,
    );
  }

  /**
   * Per-exposed-topic bounds, computed once from the cached summary. Honors any
   * configured remap so keys are exposed names. Used by MultiReader to merge
   * streams and enforce video time-disjointness. Mirrors go (*Reader).topicBounds
   * and py Reader._topic_bounds.
   *
   * @internal
   */
  async topicBounds(): Promise<Map<string, TopicBound>> {
    if (this.boundsCache !== undefined) {
      return this.boundsCache;
    }
    const summary = await this.summaryWithHint(0n);
    this.prepareRename(summary);
    const out = new Map<string, TopicBound>();
    for (const ti of summary.topicsInfos) {
      if (ti.indexChunkInfoList.length === 0) {
        continue;
      }
      let minTs = ti.indexChunkInfoList[0]!.startTimestamp;
      let maxTs = ti.indexChunkInfoList[0]!.endTimestamp;
      for (const info of ti.indexChunkInfoList) {
        if (info.startTimestamp < minTs) {
          minTs = info.startTimestamp;
        }
        if (info.endTimestamp > maxTs) {
          maxTs = info.endTimestamp;
        }
      }
      const isVideo = isVideoTopicsInfo(ti);
      for (const tm of ti.topicMetadatas) {
        const exposed = this.topicRemap?.get(tm.name) ?? tm.name;
        out.set(exposed, { isVideo, minTs, maxTs });
      }
    }
    this.boundsCache = out;
    return out;
  }

  // ---- Default path setup ---------------------------------------------

  private prepareDefault(args: {
    summary: Summary;
    topicIds: Set<number>;
    wantedSet: Set<string>;
    startTimestamp: bigint;
    endTimestamp: bigint;
    reverse: boolean;
    videoDecodable: boolean;
  }): GroupIt[] {
    const out: GroupIt[] = [];
    for (const ti of args.summary.topicsInfos) {
      // Skip groups with no relevant topic.
      if (!groupHasAnyOf(ti, args.wantedSet)) {
        continue;
      }
      const it = new TopicsGroupIterator({
        rs: this.rs,
        decompress: this.decompress,
        topicIds: args.topicIds,
        startTimestamp: args.startTimestamp,
        endTimestamp: args.endTimestamp,
        reverse: args.reverse,
        topicsInfo: ti,
        videoDecodable: args.videoDecodable && isVideoTopicsInfo(ti),
      });
      if (!it.hasAny()) {
        continue;
      }
      out.push({ next: () => it.next() });
    }
    return out;
  }

  // ---- Cost-aware path setup ------------------------------------------

  private async prepareCostAware(args: {
    summary: Summary;
    topicIds: Set<number>;
    wantedSet: Set<string>;
    startTimestamp: bigint;
    endTimestamp: bigint;
    reverse: boolean;
    strategy: import("./read_strategy.js").ReadStrategy;
    videoDecodable: boolean;
  }): Promise<GroupIt[]> {
    interface ScopedGroup {
      topicsInfo: TopicsInfo;
      isCompressed: boolean;
      groupVideo: boolean;
      filteredInfos: IndexChunkInfo[];
      filteredInfoLs: bigint[];
    }
    const scoped: ScopedGroup[] = [];
    for (const ti of args.summary.topicsInfos) {
      if (!groupHasAnyOf(ti, args.wantedSet)) {
        continue;
      }
      const isCompressed =
        ti.topicMetadatas[0]?.metadata.get(META_KEY_COMPRESSED) === true;
      const groupVideo = args.videoDecodable && isVideoTopicsInfo(ti);

      const filteredInfos: IndexChunkInfo[] = [];
      const filteredLens: bigint[] = [];
      for (let i = 0; i < ti.indexChunkInfoList.length; i++) {
        const info = ti.indexChunkInfoList[i]!;
        if (info.endTimestamp < args.startTimestamp) {
          continue;
        }
        if (info.startTimestamp > args.endTimestamp) {
          break;
        }
        filteredInfos.push(info);
        let ln: bigint;
        if (i < ti.indexChunkInfoList.length - 1) {
          ln = ti.indexChunkInfoList[i + 1]!.offset - info.offset;
        } else {
          ln = ti.totalLen - info.offset + ti.indexChunkInfoList[0]!.offset;
        }
        filteredLens.push(ln);
      }
      if (filteredInfos.length === 0) {
        continue;
      }
      scoped.push({
        topicsInfo: ti,
        isCompressed,
        groupVideo,
        filteredInfos,
        filteredInfoLs: filteredLens,
      });
    }
    if (scoped.length === 0) {
      return [];
    }

    // ---- Phase A: plan + fetch index chunks across all groups.
    const rangesA: Range[] = [];
    for (const g of scoped) {
      for (let ci = 0; ci < g.filteredInfos.length; ci++) {
        rangesA.push({
          offset: g.filteredInfos[ci]!.offset,
          length: g.filteredInfoLs[ci]!,
        });
      }
    }
    const plannedA = plan(rangesA, args.strategy);
    const bufsA = await fetchAll(this.rs, plannedA.ops, args.strategy.maxConcurrency);
    const loadedIndex = new LoadedBytes(rangesA, plannedA.locations, bufsA);

    // ---- Decode each index chunk + run sortAndFilter.
    interface DecodedChunk {
      indexChunk: IndexChunk;
      messages: MessageRef[];
      messageLens: bigint[];
    }
    const perGroupChunks: DecodedChunk[][] = scoped.map(() => []);
    for (let gi = 0; gi < scoped.length; gi++) {
      const g = scoped[gi]!;
      for (let ci = 0; ci < g.filteredInfos.length; ci++) {
        const raw = loadedIndex.get(g.filteredInfos[ci]!.offset);
        if (raw === undefined) {
          throw new Error(
            `cost-aware setup: missing index chunk at offset ${g.filteredInfos[ci]!.offset}`,
          );
        }
        const decompressed = this.decompress(raw);
        const ic = readIndexChunk(new BinaryReader(decompressed));
        const f = sortAndFilter(
          ic.topicIndexes,
          ic.uncompressedLen,
          args.topicIds,
          args.startTimestamp,
          args.endTimestamp,
          g.groupVideo,
        );
        perGroupChunks[gi]!.push({
          indexChunk: ic,
          messages: f.msgs,
          messageLens: f.lens,
        });
      }
    }

    // ---- Phase B: plan + fetch data ranges. Chunk-level for compressed,
    // per-message for uncompressed.
    const rangesB: Range[] = [];
    for (let gi = 0; gi < scoped.length; gi++) {
      const g = scoped[gi]!;
      if (g.isCompressed) {
        for (const dc of perGroupChunks[gi]!) {
          if (dc.messages.length === 0) {
            continue;
          }
          rangesB.push({
            offset: dc.indexChunk.chunkOffset,
            length: dc.indexChunk.chunkLen,
          });
        }
        continue;
      }
      for (const dc of perGroupChunks[gi]!) {
        for (let k = 0; k < dc.messages.length; k++) {
          const m = dc.messages[k]!;
          rangesB.push({
            offset: dc.indexChunk.chunkOffset + m.offsetInChunk,
            length: dc.messageLens[k]!,
          });
        }
      }
    }
    const plannedB = plan(rangesB, args.strategy);
    const bufsB = await fetchAll(this.rs, plannedB.ops, args.strategy.maxConcurrency);
    const loadedData = new LoadedBytes(rangesB, plannedB.locations, bufsB);

    // ---- Build per-group iterators.
    const iters: PreloadedTopicsGroupIterator[] = [];
    for (let gi = 0; gi < scoped.length; gi++) {
      const g = scoped[gi]!;
      const indexChunks: IndexChunk[] = [];
      const chunkMsgs: MessageRef[][] = [];
      const chunkLens: bigint[][] = [];
      for (const dc of perGroupChunks[gi]!) {
        if (dc.messages.length === 0) {
          continue;
        }
        indexChunks.push(dc.indexChunk);
        chunkMsgs.push(dc.messages);
        chunkLens.push(dc.messageLens);
      }
      if (indexChunks.length === 0) {
        continue;
      }
      iters.push(
        new PreloadedTopicsGroupIterator({
          isCompressed: g.isCompressed,
          reverse: args.reverse,
          indexChunks,
          chunkMessages: chunkMsgs,
          chunkMsgLens: chunkLens,
          loadedData,
          decompress: this.decompress,
        }),
      );
    }

    // Wrap in the shared GroupIt shape so the merge loop in readMessages can
    // treat default and cost-aware iterators identically.
    return iters.map((it) => ({
      next: async () => it.next(),
    }));
  }
}

function magicEquals(a: Uint8Array, b: Uint8Array): boolean {
  if (a.byteLength !== b.byteLength) {
    return false;
  }
  for (let i = 0; i < a.byteLength; i++) {
    if (a[i] !== b[i]) {
      return false;
    }
  }
  return true;
}

/**
 * Returns nothing when all preconditions hold; otherwise throws a
 * SampleValidationError listing every violation (unknown/duplicate topic,
 * non-strictly-increasing timestamps). Mirrors go validateSampleQueries.
 */
function validateSampleQueries(
  queries: SampleQuery[],
  summary: Summary,
): void {
  const known = new Set<string>();
  for (const ti of summary.topicsInfos) {
    for (const tm of ti.topicMetadatas) {
      known.add(tm.name);
    }
  }
  const seen = new Map<string, number>();
  const violations: string[] = [];
  for (let i = 0; i < queries.length; i++) {
    const q = queries[i]!;
    if (!known.has(q.topic)) {
      violations.push(`queries[${i}]: unknown topic "${q.topic}"`);
    }
    const prev = seen.get(q.topic);
    if (prev !== undefined) {
      violations.push(
        `queries[${i}]: duplicate topic "${q.topic}" already used by queries[${prev}]`,
      );
    } else {
      seen.set(q.topic, i);
    }
    for (let j = 1; j < q.timestamps.length; j++) {
      if (q.timestamps[j]! <= q.timestamps[j - 1]!) {
        violations.push(
          `queries[${i}].timestamps not strictly increasing at position ${j} (${q.timestamps[j]} <= ${q.timestamps[j - 1]})`,
        );
      }
    }
  }
  if (violations.length > 0) {
    throw new SampleValidationError(violations);
  }
}

function collectAllTopicNames(summary: Summary): Set<string> {
  const out = new Set<string>();
  for (const ti of summary.topicsInfos) {
    for (const tm of ti.topicMetadatas) {
      out.add(tm.name);
    }
  }
  return out;
}

function groupHasAnyOf(ti: TopicsInfo, names: Set<string>): boolean {
  for (const tm of ti.topicMetadatas) {
    if (names.has(tm.name)) {
      return true;
    }
  }
  return false;
}

/**
 * Whether the (single) topic in this group was opened with video=true. Video
 * groups always have exactly one topic, so the first metadata suffices.
 * Mirrors py _is_video_topics_info.
 */
function isVideoTopicsInfo(ti: TopicsInfo): boolean {
  return ti.topicMetadatas[0]?.metadata.get(META_KEY_VIDEO) === true;
}
