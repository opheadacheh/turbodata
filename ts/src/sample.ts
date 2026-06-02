// Sample engine: floor-message lookup at concrete timestamps.
//
// Ports go/internal/iter/sample.go (via py/turbodata/_sample.py, the no-video
// reference). Two concurrent I/O waves:
//
//   Phase A: fetch the candidate index chunks per (topic, timestamp), decode
//            them, advance each query's forward cursor, and re-queue fallbacks
//            against chunk c-1 when the candidate chunk has no message of the
//            topic with ts <= T. Loop until nothing is pending. Bounded by the
//            chunk count.
//
//   Phase B: fetch data ranges (chunk-level for compressed groups, per-message
//            for uncompressed groups) in one concurrent wave, decompress, and
//            copy the floor bytes into the result slots.
//
// When videoDecodable is set, a video topic instead yields a decoder-ready GOP
// sequence (frames + resetDecoder) per the SampleResult contract; without it a
// video topic is sampled like any other topic (its floor frame in `data`).

import type { Decompressor } from "./compression.js";
import { BinaryReader, readIndexChunk } from "./io.js";
import { LoadedBytes } from "./loaded_bytes.js";
import { fetchAll } from "./read_fetcher.js";
import { plan, type Range } from "./read_planner.js";
import type { ReadSource } from "./read_source.js";
import type { ReadStrategy } from "./read_strategy.js";
import {
  META_KEY_COMPRESSED,
  META_KEY_VIDEO,
  type IndexChunk,
  type Summary,
  type TopicIndex,
} from "./types.js";

/** One engine-level query: floor message of `topic` at each `timestamps[j]`. */
export interface SampleSpec {
  topic: string;
  /** Assumed strictly increasing (the Reader validates this). */
  timestamps: bigint[];
}

/**
 * One coded video frame surfaced in a SampleHit's GOP prefix or incremental
 * tail. Structurally identical to the public turbodata Frame.
 */
export interface SampleFrame {
  timestamp: bigint;
  isKeyFrame: boolean;
  data: Uint8Array;
}

/**
 * One engine-level result. `found` is false when no message with ts <= the
 * requested timestamp exists for the topic. `data` is an owned copy.
 *
 * For video-decodable video topics, `data` stays empty and the frame bytes
 * live in `frames` (target = last element); `isVideo` is true and
 * `resetDecoder` flags GOP boundaries. See the public SampleResult contract.
 */
export interface SampleHit {
  found: boolean;
  timestamp: bigint;
  data: Uint8Array;
  isVideo: boolean;
  frames: SampleFrame[];
  resetDecoder: boolean;
}

interface PendingItem {
  /** Index into the spec's `timestamps`. */
  tIdx: number;
  /** Candidate chunk index within the query's group. */
  chunkIdx: number;
}

interface ResolvedItem {
  tIdx: number;
  groupIdx: number;
  chunkIdx: number;
  msgOffInChunk: bigint;
  msgLen: bigint;
  timestamp: bigint;
  // Video-only: positions into the topic's messageIndexes in the cached
  // IndexChunk. keyframeMsgIdx is the anchor key frame for the GOP that
  // contains the target; targetMsgIdx is the target's position.
  keyframeMsgIdx: number;
  targetMsgIdx: number;
}

interface QueryState {
  spec: SampleSpec;
  groupIdx: number;
  topicId: number;
  isCompressed: boolean;
  isVideo: boolean;
  results: SampleHit[];
  pending: PendingItem[];
  resolved: ResolvedItem[];
}

/**
 * A decoded index chunk plus per-topic message lengths (computed once when the
 * chunk is first decoded). Messages from different topics within a chunk are
 * interleaved by offset, so per-topic lengths can't be derived from a single
 * topic's MessageIndexes alone.
 */
interface ChunkCache {
  ic: IndexChunk;
  perTopicLens: Map<number, bigint[]>;
}

const EMPTY = new Uint8Array(0);

/** Compose the (group, chunk) cache key. */
function chunkKey(group: number, chunk: number): string {
  return `${group},${chunk}`;
}

function cmpBigint(a: bigint, b: bigint): number {
  return a < b ? -1 : a > b ? 1 : 0;
}

/**
 * Resolve floor messages for the given specs. Returns hits shaped like the
 * input: out[i][j] corresponds to specs[i].timestamps[j].
 */
export async function sampleMessages(
  rs: ReadSource,
  summary: Summary,
  specs: SampleSpec[],
  strategy: ReadStrategy,
  decompress: Decompressor,
  videoDecodable = false,
): Promise<SampleHit[][]> {
  if (specs.length === 0) {
    return [];
  }

  const states: QueryState[] = specs.map((spec) => {
    const qs: QueryState = {
      spec,
      groupIdx: 0,
      topicId: 0,
      isCompressed: false,
      isVideo: false,
      results: spec.timestamps.map(() => ({
        found: false,
        timestamp: 0n,
        data: EMPTY,
        isVideo: false,
        frames: [],
        resetDecoder: false,
      })),
      pending: [],
      resolved: [],
    };
    resolveTopic(qs, summary, videoDecodable);
    return qs;
  });

  // Assign initial candidates: per query, walk the strictly-increasing
  // timestamps alongside the group's IndexChunkInfoList with one forward
  // cursor. The candidate is the chunk with the largest StartTimestamp <= T.
  for (const qs of states) {
    const chunks = summary.topicsInfos[qs.groupIdx]!.indexChunkInfoList;
    if (chunks.length === 0) {
      continue;
    }
    let c = -1;
    for (let t = 0; t < qs.spec.timestamps.length; t++) {
      const T = qs.spec.timestamps[t]!;
      while (c + 1 < chunks.length && chunks[c + 1]!.startTimestamp <= T) {
        c++;
      }
      if (c < 0) {
        continue; // T precedes every chunk; results[t].found stays false.
      }
      qs.pending.push({ tIdx: t, chunkIdx: c });
    }
  }

  const cache = new Map<string, ChunkCache>();

  // ---- Phase A ----
  while (states.some((qs) => qs.pending.length > 0)) {
    const needs = gatherChunkNeeds(summary, states, cache);
    if (needs.length > 0) {
      await fetchAndDecodeIndexChunks(rs, strategy, decompress, needs, cache);
    }

    let anyProgress = false;
    for (const qs of states) {
      if (qs.pending.length === 0) {
        continue;
      }
      const still: PendingItem[] = [];
      let i = 0;
      while (i < qs.pending.length) {
        // Group the contiguous run of pending items sharing one candidate chunk.
        let j = i;
        while (
          j < qs.pending.length &&
          qs.pending[j]!.chunkIdx === qs.pending[i]!.chunkIdx
        ) {
          j++;
        }
        const c = qs.pending[i]!.chunkIdx;
        const cc = cache.get(chunkKey(qs.groupIdx, c));
        if (cc === undefined) {
          for (let k = i; k < j; k++) {
            still.push(qs.pending[k]!);
          }
          i = j;
          continue;
        }
        anyProgress = true;
        const fallback = resolveItemsInChunk(qs, c, cc, qs.pending.slice(i, j));
        for (const f of fallback) {
          still.push(f);
        }
        i = j;
      }
      qs.pending = still;
    }

    if (!anyProgress && states.some((qs) => qs.pending.length > 0)) {
      // gatherChunkNeeds should always surface a fetchable chunk for any
      // pending item; reaching here would indicate an internal bug.
      throw new Error(
        "sample: phase A made no progress with pending items remaining",
      );
    }
  }

  // ---- Phase B ----
  await runPhaseB(rs, strategy, decompress, states, cache);

  // Mark every result of a video query (including not-found ones) so callers
  // can branch on isVideo. Mirrors the py/go engine's collect step.
  for (const qs of states) {
    if (qs.isVideo) {
      for (const h of qs.results) {
        h.isVideo = true;
      }
    }
  }

  return states.map((qs) => qs.results);
}

/**
 * Locate the topic's group index, topic id, compression flag, and (when video
 * decoding is requested) video flag in the summary. Assumes the caller
 * validated that the topic exists.
 */
function resolveTopic(
  qs: QueryState,
  summary: Summary,
  videoDecodable: boolean,
): void {
  for (let gi = 0; gi < summary.topicsInfos.length; gi++) {
    const ti = summary.topicsInfos[gi]!;
    for (const tm of ti.topicMetadatas) {
      if (tm.name === qs.spec.topic) {
        qs.groupIdx = gi;
        qs.topicId = tm.id;
        qs.isCompressed =
          ti.topicMetadatas[0]!.metadata.get(META_KEY_COMPRESSED) === true;
        // Video decoding is opt-in. Without it, a video topic is sampled like
        // any other topic (its floor frame in `data`).
        qs.isVideo =
          videoDecodable &&
          ti.topicMetadatas[0]!.metadata.get(META_KEY_VIDEO) === true;
        return;
      }
    }
  }
}

interface ChunkNeed {
  key: string;
  offset: bigint;
  length: bigint;
}

/** Returns the not-yet-cached candidate chunks, sorted by file offset. */
function gatherChunkNeeds(
  summary: Summary,
  states: QueryState[],
  cache: Map<string, ChunkCache>,
): ChunkNeed[] {
  const seen = new Set<string>();
  const out: ChunkNeed[] = [];
  for (const qs of states) {
    for (const p of qs.pending) {
      const key = chunkKey(qs.groupIdx, p.chunkIdx);
      if (cache.has(key) || seen.has(key)) {
        continue;
      }
      seen.add(key);
      const ti = summary.topicsInfos[qs.groupIdx]!;
      const list = ti.indexChunkInfoList;
      const info = list[p.chunkIdx]!;
      // The on-disk length of index chunk c: the gap to the next chunk's
      // offset, or (for the last chunk) the group's total index length closes
      // it. Mirrors go IndexChunkLen / py _gather_chunk_needs.
      let length: bigint;
      if (p.chunkIdx < list.length - 1) {
        length = list[p.chunkIdx + 1]!.offset - info.offset;
      } else {
        length = ti.totalLen - info.offset + list[0]!.offset;
      }
      out.push({ key, offset: info.offset, length });
    }
  }
  out.sort((a, b) => cmpBigint(a.offset, b.offset));
  return out;
}

async function fetchAndDecodeIndexChunks(
  rs: ReadSource,
  strategy: ReadStrategy,
  decompress: Decompressor,
  needs: ChunkNeed[],
  cache: Map<string, ChunkCache>,
): Promise<void> {
  const ranges: Range[] = needs.map((n) => ({
    offset: n.offset,
    length: n.length,
  }));
  const { ops, locations } = plan(ranges, strategy);
  const bufs = await fetchAll(rs, ops, strategy.maxConcurrency);
  const loaded = new LoadedBytes(ranges, locations, bufs);
  for (const n of needs) {
    const raw = loaded.get(n.offset);
    if (raw === undefined) {
      throw new Error(`sample: missing index chunk at offset ${n.offset}`);
    }
    const ic = readIndexChunk(new BinaryReader(decompress(raw)));
    cache.set(n.key, buildChunkCache(ic));
  }
}

/**
 * Derive per-message lengths for every topic in a decoded index chunk. Flatten
 * all topics' MessageIndexes, sort by offset (== timestamp order within a
 * chunk), take successive offset deltas (UncompressedLen closing the last),
 * then project the lengths back onto each topic's MessageIndexes by index.
 */
function buildChunkCache(ic: IndexChunk): ChunkCache {
  const items: { topicId: number; offset: bigint; idx: number }[] = [];
  for (const ti of ic.topicIndexes) {
    for (let i = 0; i < ti.messageIndexes.length; i++) {
      items.push({
        topicId: ti.id,
        offset: ti.messageIndexes[i]!.offsetInChunk,
        idx: i,
      });
    }
  }
  items.sort((a, b) => cmpBigint(a.offset, b.offset));

  const perTopicLens = new Map<number, bigint[]>();
  for (const ti of ic.topicIndexes) {
    perTopicLens.set(ti.id, new Array<bigint>(ti.messageIndexes.length).fill(0n));
  }
  for (let i = 0; i < items.length - 1; i++) {
    const it = items[i]!;
    perTopicLens.get(it.topicId)![it.idx] = items[i + 1]!.offset - it.offset;
  }
  if (items.length > 0) {
    const last = items[items.length - 1]!;
    perTopicLens.get(last.topicId)![last.idx] = ic.uncompressedLen - last.offset;
  }
  return { ic, perTopicLens };
}

function findTopicIndex(
  ic: IndexChunk,
  topicId: number,
): TopicIndex | undefined {
  for (const ti of ic.topicIndexes) {
    if (ti.id === topicId) {
      return ti;
    }
  }
  return undefined;
}

/**
 * Resolve a contiguous run of pending items that all share candidate chunk c.
 * Items are in strictly-increasing T order, so a single forward cursor over
 * the topic's MessageIndexes assigns each its floor. Items the cursor can't
 * satisfy (topic absent, or its first message in this chunk is > T) become
 * fallbacks against c-1; at c == 0 they are marked not-found.
 */
function resolveItemsInChunk(
  qs: QueryState,
  c: number,
  cache: ChunkCache,
  items: PendingItem[],
): PendingItem[] {
  const fallback: PendingItem[] = [];
  const ti = findTopicIndex(cache.ic, qs.topicId);
  if (ti === undefined || ti.messageIndexes.length === 0) {
    for (const p of items) {
      if (c === 0) {
        qs.results[p.tIdx]!.found = false;
      } else {
        fallback.push({ tIdx: p.tIdx, chunkIdx: c - 1 });
      }
    }
    return fallback;
  }

  const mis = ti.messageIndexes;
  const lens = cache.perTopicLens.get(qs.topicId)!;
  // For video, kfPos tracks the position within keyFrameIndexes of the most
  // recent key frame index <= k. Items in this run are in increasing T (hence
  // increasing k) order, so the cursor only moves forward.
  let kfPos = 0;
  let k = 0;
  for (const p of items) {
    const T = qs.spec.timestamps[p.tIdx]!;
    while (k + 1 < mis.length && mis[k + 1]!.timestamp <= T) {
      k++;
    }
    if (mis[k]!.timestamp > T) {
      if (c === 0) {
        qs.results[p.tIdx]!.found = false;
      } else {
        fallback.push({ tIdx: p.tIdx, chunkIdx: c - 1 });
      }
      continue;
    }
    const item: ResolvedItem = {
      tIdx: p.tIdx,
      groupIdx: qs.groupIdx,
      chunkIdx: c,
      msgOffInChunk: mis[k]!.offsetInChunk,
      msgLen: lens[k]!,
      timestamp: mis[k]!.timestamp,
      keyframeMsgIdx: 0,
      targetMsgIdx: 0,
    };
    if (qs.isVideo) {
      const kfs = ti.keyFrameIndexes;
      if (kfs.length === 0 || kfs[0]! > k) {
        // No anchor key frame for this target in this chunk: violates the
        // writer's GOP-integrity invariant. Treat as not found rather than
        // emit an undecodable single frame.
        qs.results[p.tIdx]!.found = false;
        continue;
      }
      while (kfPos + 1 < kfs.length && kfs[kfPos + 1]! <= k) {
        kfPos++;
      }
      item.keyframeMsgIdx = kfs[kfPos]!;
      item.targetMsgIdx = k;
    }
    qs.resolved.push(item);
  }
  return fallback;
}

/**
 * Issue every resolved item's data read in one concurrent wave and copy the
 * floor bytes into result slots. Compressed groups register one range per
 * unique chunk (decompressed once, then sliced per message). Uncompressed
 * groups register one range per unique message offset; plan() coalesces
 * neighbors per the strategy.
 */
async function runPhaseB(
  rs: ReadSource,
  strategy: ReadStrategy,
  decompress: Decompressor,
  states: QueryState[],
  cache: Map<string, ChunkCache>,
): Promise<void> {
  const seenChunk = new Set<string>();
  // Multiple query timestamps in one spec can floor to the same message;
  // dedup by absolute offset so we don't register (and read) it twice. The
  // offset is globally unique per message, and a given offset always has the
  // same length, so LoadedBytes keying by offset stays consistent.
  const seenMsg = new Set<bigint>();
  // Video: dedupe per (group, chunk, keyframe). All queries that share a GOP
  // register a single range from the keyframe to the furthest target across
  // those queries; shorter-target queries slice less of the same loaded bytes.
  // Maps the (group, chunk, keyframe) key -> [startInChunk, endInChunkExcl].
  const videoExtents = new Map<
    string,
    { groupIdx: number; chunkIdx: number; start: bigint; endExcl: bigint }
  >();

  const ranges: Range[] = [];
  for (const qs of states) {
    for (const r of qs.resolved) {
      const cc = cache.get(chunkKey(r.groupIdx, r.chunkIdx))!;
      const ic = cc.ic;
      if (qs.isCompressed) {
        const k = chunkKey(r.groupIdx, r.chunkIdx);
        if (seenChunk.has(k)) {
          continue;
        }
        seenChunk.add(k);
        ranges.push({ offset: ic.chunkOffset, length: ic.chunkLen });
      } else if (qs.isVideo) {
        // Video groups are single-topic by writer invariant.
        const ti = ic.topicIndexes[0]!;
        const mis = ti.messageIndexes;
        const lens = cc.perTopicLens.get(qs.topicId)!;
        const start = mis[r.keyframeMsgIdx]!.offsetInChunk;
        const endExcl = mis[r.targetMsgIdx]!.offsetInChunk + lens[r.targetMsgIdx]!;
        const vk = `${r.groupIdx},${r.chunkIdx},${r.keyframeMsgIdx}`;
        const ve = videoExtents.get(vk);
        if (ve !== undefined) {
          if (endExcl > ve.endExcl) {
            ve.endExcl = endExcl;
          }
        } else {
          videoExtents.set(vk, {
            groupIdx: r.groupIdx,
            chunkIdx: r.chunkIdx,
            start,
            endExcl,
          });
        }
      } else {
        const off = ic.chunkOffset + r.msgOffInChunk;
        if (seenMsg.has(off)) {
          continue;
        }
        seenMsg.add(off);
        ranges.push({ offset: off, length: r.msgLen });
      }
    }
  }
  for (const ve of videoExtents.values()) {
    const ic = cache.get(chunkKey(ve.groupIdx, ve.chunkIdx))!.ic;
    ranges.push({
      offset: ic.chunkOffset + ve.start,
      length: ve.endExcl - ve.start,
    });
  }
  if (ranges.length === 0) {
    return;
  }
  ranges.sort((a, b) => cmpBigint(a.offset, b.offset));

  const { ops, locations } = plan(ranges, strategy);
  const bufs = await fetchAll(rs, ops, strategy.maxConcurrency);
  const loaded = new LoadedBytes(ranges, locations, bufs);

  // Compressed: group resolved items by chunk so each chunk decompresses once.
  const byChunk = new Map<string, { qs: QueryState; r: ResolvedItem }[]>();
  for (const qs of states) {
    if (!qs.isCompressed) {
      continue;
    }
    for (const r of qs.resolved) {
      const k = chunkKey(r.groupIdx, r.chunkIdx);
      let arr = byChunk.get(k);
      if (arr === undefined) {
        arr = [];
        byChunk.set(k, arr);
      }
      arr.push({ qs, r });
    }
  }
  for (const [k, group] of byChunk) {
    const ic = cache.get(k)!.ic;
    const compressed = loaded.get(ic.chunkOffset)!;
    const decompressed = decompress(compressed);
    for (const { qs, r } of group) {
      const start = Number(r.msgOffInChunk);
      const end = start + Number(r.msgLen);
      // .slice copies, so the result owns its bytes.
      qs.results[r.tIdx] = {
        found: true,
        timestamp: r.timestamp,
        data: decompressed.slice(start, end),
        isVideo: false,
        frames: [],
        resetDecoder: false,
      };
    }
  }

  // Uncompressed non-video: per-message copy.
  for (const qs of states) {
    if (qs.isCompressed || qs.isVideo) {
      continue;
    }
    for (const r of qs.resolved) {
      const ic = cache.get(chunkKey(r.groupIdx, r.chunkIdx))!.ic;
      const raw = loaded.get(ic.chunkOffset + r.msgOffInChunk)!;
      qs.results[r.tIdx] = {
        found: true,
        timestamp: r.timestamp,
        data: new Uint8Array(raw), // copy: detach from the shared op buffer.
        isVideo: false,
        frames: [],
        resetDecoder: false,
      };
    }
  }

  // Video: incremental frames + resetDecoder.
  for (const qs of states) {
    if (!qs.isVideo) {
      continue;
    }
    materializeVideo(qs, cache, loaded);
  }
}

/**
 * Walk one query's resolved items in target-timestamp order and emit
 * incremental frames + resetDecoder per the public SampleResult contract.
 * Mirrors py _materialize_video.
 */
function materializeVideo(
  qs: QueryState,
  cache: Map<string, ChunkCache>,
  loaded: LoadedBytes,
): void {
  if (qs.resolved.length === 0) {
    return;
  }
  // Resolved items can arrive out of tIdx order if Phase A required fallbacks;
  // sort so the row's processing order matches the caller's.
  const resolved = [...qs.resolved].sort((a, b) => a.tIdx - b.tIdx);

  // Decoder-state continuity is per-row, scoped to a single GOP (= same chunk
  // + same keyframeMsgIdx).
  let havePrev = false;
  let prevGroupIdx = 0;
  let prevChunkIdx = 0;
  let prevKeyframeIdx = 0;
  let prevTargetMsgIdx = 0;

  for (const r of resolved) {
    const cc = cache.get(chunkKey(r.groupIdx, r.chunkIdx))!;
    const ti = cc.ic.topicIndexes[0]!;
    const mis = ti.messageIndexes;
    const lens = cc.perTopicLens.get(qs.topicId)!;

    const sameGop =
      havePrev &&
      prevGroupIdx === r.groupIdx &&
      prevChunkIdx === r.chunkIdx &&
      prevKeyframeIdx === r.keyframeMsgIdx;

    let startIdx: number;
    let reset: boolean;
    if (!sameGop) {
      startIdx = r.keyframeMsgIdx;
      reset = true;
    } else {
      startIdx = prevTargetMsgIdx + 1;
      reset = false;
    }

    // The Phase B range starts at the GOP's keyframe in this chunk and is long
    // enough to cover the furthest target across queries in this GOP.
    const raw = loaded.get(
      cc.ic.chunkOffset + mis[r.keyframeMsgIdx]!.offsetInChunk,
    )!;
    const baseInChunk = mis[r.keyframeMsgIdx]!.offsetInChunk;

    const frames: SampleFrame[] = [];
    if (startIdx <= r.targetMsgIdx) {
      for (let i = startIdx; i <= r.targetMsgIdx; i++) {
        const frameStart = Number(mis[i]!.offsetInChunk - baseInChunk);
        const frameLen = Number(lens[i]!);
        frames.push({
          timestamp: mis[i]!.timestamp,
          // keyframeMsgIdx is the greatest key frame index <= targetMsgIdx, so
          // it is the only key frame in range.
          isKeyFrame: i === r.keyframeMsgIdx,
          data: raw.slice(frameStart, frameStart + frameLen),
        });
      }
      // data stays empty for video; target is the last frame.
      qs.results[r.tIdx]!.timestamp = frames[frames.length - 1]!.timestamp;
    } else {
      // Same target as the previous in-row result: nothing new to feed.
      qs.results[r.tIdx]!.timestamp = mis[r.targetMsgIdx]!.timestamp;
    }
    qs.results[r.tIdx]!.found = true;
    qs.results[r.tIdx]!.data = EMPTY;
    qs.results[r.tIdx]!.frames = frames;
    qs.results[r.tIdx]!.resetDecoder = reset;

    havePrev = true;
    prevGroupIdx = r.groupIdx;
    prevChunkIdx = r.chunkIdx;
    prevKeyframeIdx = r.keyframeMsgIdx;
    prevTargetMsgIdx = r.targetMsgIdx;
  }
}
