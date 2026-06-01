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
// Video topics are not supported by the TS SDK; a video topic is sampled like
// any other topic (its floor frame is returned in `data`).

import type { Decompressor } from "./compression.js";
import { BinaryReader, readIndexChunk } from "./io.js";
import { LoadedBytes } from "./loaded_bytes.js";
import { fetchAll } from "./read_fetcher.js";
import { plan, type Range } from "./read_planner.js";
import type { ReadSource } from "./read_source.js";
import type { ReadStrategy } from "./read_strategy.js";
import type { IndexChunk, Summary, TopicIndex } from "./types.js";

/** One engine-level query: floor message of `topic` at each `timestamps[j]`. */
export interface SampleSpec {
  topic: string;
  /** Assumed strictly increasing (the Reader validates this). */
  timestamps: bigint[];
}

/**
 * One engine-level result. `found` is false when no message with ts <= the
 * requested timestamp exists for the topic. `data` is an owned copy.
 */
export interface SampleHit {
  found: boolean;
  timestamp: bigint;
  data: Uint8Array;
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
}

interface QueryState {
  spec: SampleSpec;
  groupIdx: number;
  topicId: number;
  isCompressed: boolean;
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
      results: spec.timestamps.map(() => ({
        found: false,
        timestamp: 0n,
        data: EMPTY,
      })),
      pending: [],
      resolved: [],
    };
    resolveTopic(qs, summary);
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

  return states.map((qs) => qs.results);
}

/**
 * Locate the topic's group index, topic id, and compression flag in the
 * summary. Assumes the caller validated that the topic exists.
 */
function resolveTopic(qs: QueryState, summary: Summary): void {
  for (let gi = 0; gi < summary.topicsInfos.length; gi++) {
    const ti = summary.topicsInfos[gi]!;
    for (const tm of ti.topicMetadatas) {
      if (tm.name === qs.spec.topic) {
        qs.groupIdx = gi;
        qs.topicId = tm.id;
        qs.isCompressed =
          ti.topicMetadatas[0]!.metadata.get("is_compressed") === true;
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
    qs.resolved.push({
      tIdx: p.tIdx,
      groupIdx: qs.groupIdx,
      chunkIdx: c,
      msgOffInChunk: mis[k]!.offsetInChunk,
      msgLen: lens[k]!,
      timestamp: mis[k]!.timestamp,
    });
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

  const ranges: Range[] = [];
  for (const qs of states) {
    for (const r of qs.resolved) {
      const ic = cache.get(chunkKey(r.groupIdx, r.chunkIdx))!.ic;
      if (qs.isCompressed) {
        const k = chunkKey(r.groupIdx, r.chunkIdx);
        if (seenChunk.has(k)) {
          continue;
        }
        seenChunk.add(k);
        ranges.push({ offset: ic.chunkOffset, length: ic.chunkLen });
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
      };
    }
  }

  // Uncompressed: per-message copy.
  for (const qs of states) {
    if (qs.isCompressed) {
      continue;
    }
    for (const r of qs.resolved) {
      const ic = cache.get(chunkKey(r.groupIdx, r.chunkIdx))!.ic;
      const raw = loaded.get(ic.chunkOffset + r.msgOffInChunk)!;
      qs.results[r.tIdx] = {
        found: true,
        timestamp: r.timestamp,
        data: new Uint8Array(raw), // copy: detach from the shared op buffer.
      };
    }
  }
}
