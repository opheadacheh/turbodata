// sortAndFilter: pure version that turns a decoded IndexChunk into a flat,
// timestamp-ordered list of kept (topicId, timestamp, offsetInChunk) plus
// per-message lengths. Mirrors go/sort_and_filter.go.
//
// The "sort" is by per-message OffsetInChunk; the writer interleaves messages
// in timestamp order, so offset order == timestamp order within a chunk. We
// preserve that invariant rather than re-sorting by timestamp explicitly.

import type { IndexChunk, MessageIndex, TopicIndex } from "./types.js";

export interface MessageRef {
  timestamp: bigint;
  topicId: number;
  offsetInChunk: bigint;
}

interface ItemWithTopic {
  topicId: number;
  mi: MessageIndex;
}

export function sortAndFilter(
  topicIndexes: TopicIndex[],
  totalLen: bigint,
  topicIds: Set<number>,
  startTimestamp: bigint,
  endTimestamp: bigint,
): { msgs: MessageRef[]; lens: bigint[] } {
  let total = 0;
  for (const ti of topicIndexes) {
    total += ti.messageIndexes.length;
  }
  if (total === 0) {
    return { msgs: [], lens: [] };
  }

  const sorted: ItemWithTopic[] = new Array(total);
  let writeIdx = 0;

  if (topicIndexes.length === 1) {
    // Fast path: single topic, messages are already in offset order.
    const ti = topicIndexes[0]!;
    for (const mi of ti.messageIndexes) {
      sorted[writeIdx++] = { topicId: ti.id, mi };
    }
  } else {
    // K-way merge by offsetInChunk across topics.
    const cursors: number[] = topicIndexes.map(() => 0);
    while (true) {
      let pickedTopic = -1;
      let pickedOffset = 0n;
      for (let t = 0; t < topicIndexes.length; t++) {
        const c = cursors[t]!;
        const mis = topicIndexes[t]!.messageIndexes;
        if (c >= mis.length) {
          continue;
        }
        const off = mis[c]!.offsetInChunk;
        if (pickedTopic === -1 || off < pickedOffset) {
          pickedTopic = t;
          pickedOffset = off;
        }
      }
      if (pickedTopic === -1) {
        break;
      }
      const ti = topicIndexes[pickedTopic]!;
      const cur = cursors[pickedTopic]!;
      sorted[writeIdx++] = { topicId: ti.id, mi: ti.messageIndexes[cur]! };
      cursors[pickedTopic] = cur + 1;
    }
  }

  // Per-entry lengths from offset deltas.
  const allLens = new Array<bigint>(sorted.length);
  for (let i = 0; i < sorted.length - 1; i++) {
    allLens[i] = sorted[i + 1]!.mi.offsetInChunk - sorted[i]!.mi.offsetInChunk;
  }
  allLens[allLens.length - 1] =
    totalLen - sorted[sorted.length - 1]!.mi.offsetInChunk;

  // Filter by topic + timestamp range.
  const msgs: MessageRef[] = [];
  const lens: bigint[] = [];
  for (let i = 0; i < sorted.length; i++) {
    const item = sorted[i]!;
    if (!topicIds.has(item.topicId)) {
      continue;
    }
    const ts = item.mi.timestamp;
    if (ts < startTimestamp || ts > endTimestamp) {
      continue;
    }
    msgs.push({
      timestamp: ts,
      topicId: item.topicId,
      offsetInChunk: item.mi.offsetInChunk,
    });
    lens.push(allLens[i]!);
  }
  return { msgs, lens };
}

/** Convenience: sort+filter directly from a decoded IndexChunk. */
export function sortAndFilterChunk(
  chunk: IndexChunk,
  topicIds: Set<number>,
  startTimestamp: bigint,
  endTimestamp: bigint,
): { msgs: MessageRef[]; lens: bigint[] } {
  return sortAndFilter(
    chunk.topicIndexes,
    chunk.uncompressedLen,
    topicIds,
    startTimestamp,
    endTimestamp,
  );
}
