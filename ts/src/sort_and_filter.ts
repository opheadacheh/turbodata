// sortAndFilter: pure version that turns a decoded IndexChunk into a flat,
// timestamp-ordered list of kept (topicId, timestamp, offsetInChunk) plus
// per-message lengths. Mirrors go/sort_and_filter.go.
//
// The "sort" is by per-message OffsetInChunk; the writer interleaves messages
// in timestamp order, so offset order == timestamp order within a chunk. We
// preserve that invariant rather than re-sorting by timestamp explicitly.

import { MessageHeap } from "./iterators/message_heap.js";
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
    // K-way merge by offsetInChunk across topics, via a min-heap.
    // Mirrors go/sort_and_filter.go's MessageIndexHeap-based merge: O(N log K)
    // regardless of how many topics share a group.
    const heap = new MessageHeap<{ topicSlot: number; cursor: number }>(false);
    for (let t = 0; t < topicIndexes.length; t++) {
      const mis = topicIndexes[t]!.messageIndexes;
      if (mis.length === 0) {
        continue;
      }
      heap.push(mis[0]!.offsetInChunk, { topicSlot: t, cursor: 0 });
    }
    while (heap.size > 0) {
      const top = heap.pop()!;
      const { topicSlot, cursor } = top.value;
      const ti = topicIndexes[topicSlot]!;
      sorted[writeIdx++] = { topicId: ti.id, mi: ti.messageIndexes[cursor]! };
      const next = cursor + 1;
      if (next < ti.messageIndexes.length) {
        heap.push(ti.messageIndexes[next]!.offsetInChunk, {
          topicSlot,
          cursor: next,
        });
      }
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
