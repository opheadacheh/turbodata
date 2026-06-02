// MultiReader: present several single-file Readers as one time-ordered stream.
// Mirrors go/multi_reader.go and py/turbodata/multi_reader.py.
//
// readMessages merges every reader's stream in timestamp order; sample resolves
// floor messages across the union of readers. Both operate entirely in each
// Reader's exposed-name space. Topic-name collisions across files are the
// caller's responsibility: configure per-Reader topicRemap so that names meant
// to union share an exposed name and names meant to stay distinct do not.

import { MessageHeap } from "./iterators/message_heap.js";
import type { ReadOptions } from "./read_options.js";
import {
  Reader,
  SampleValidationError,
  type Message,
  type SampleOptions,
  type SampleQuery,
  type SampleResult,
  type TopicBound,
} from "./reader.js";

/**
 * Thrown by MultiReader.sample/readMessages under videoDecodable when a queried
 * video topic is provided by more than one reader whose time ranges overlap.
 * Decodable multi-file video requires each video topic's source files to be
 * time-disjoint; merging overlapping decodable sources would interleave frames
 * from different GOP chains into an undecodable stream. Mirrors go
 * ErrVideoSourcesOverlap / py VideoSourcesOverlapError.
 */
export class VideoSourcesOverlapError extends Error {
  readonly topic: string;
  readonly first: readonly [bigint, bigint];
  readonly second: readonly [bigint, bigint];
  constructor(
    topic: string,
    first: readonly [bigint, bigint],
    second: readonly [bigint, bigint],
  ) {
    super(
      `turbodata: video topic "${topic}" sources [${first[0]},${first[1]}] and [${second[0]},${second[1]}] overlap in time`,
    );
    this.name = "VideoSourcesOverlapError";
    this.topic = topic;
    this.first = first;
    this.second = second;
  }
}

export class MultiReader {
  private readonly readers: Reader[];

  /**
   * Group readers into one merged view. At least one reader is required.
   * Per-file behavior (remap, etc.) lives on each underlying Reader.
   */
  constructor(readers: Reader[]) {
    if (readers.length === 0) {
      throw new Error("turbodata: MultiReader requires at least one reader");
    }
    this.readers = readers;
  }

  /**
   * Read every reader's messages as one merged, time-ordered stream. Options
   * pass through to each underlying Reader.readMessages unchanged, so order,
   * time bounds, topic filtering (by exposed name), and strategy apply per file
   * before the merge.
   *
   * When videoDecodable is set, any in-scope video topic supplied by more than
   * one reader must have time-disjoint source ranges; overlapping sources throw
   * VideoSourcesOverlapError (merging them would interleave frames from
   * different GOP chains into an undecodable stream). As with the single
   * Reader, all I/O (and this check) is deferred to the first iteration step.
   */
  readMessages(opts: ReadOptions = {}): AsyncIterableIterator<Message> {
    const readers = this.readers;
    const reverse = opts.order === "reverse-time";
    const videoDecodable = opts.videoDecodable === true;
    const wantedNames = opts.topicNames;

    let initialized = false;
    let exhausted = false;
    const subs: AsyncIterableIterator<Message>[] = [];
    const heap = new MessageHeap<{ subIdx: number; msg: Message }>(reverse);

    const init = async (): Promise<void> => {
      for (const r of readers) {
        subs.push(r.readMessages(opts));
      }

      if (videoDecodable) {
        const bounds = await Promise.all(readers.map((r) => r.topicBounds()));
        enforceVideoDisjoint(readScopeTopics(wantedNames, bounds), bounds);
      }

      // Prime the heap with one message from each reader's stream.
      for (let i = 0; i < subs.length; i++) {
        const res = await subs[i]!.next();
        if (res.done === true) {
          continue;
        }
        heap.push(res.value.timestamp, { subIdx: i, msg: res.value });
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
        const out = top.value.msg;
        const nm = await subs[top.value.subIdx]!.next();
        if (nm.done !== true) {
          heap.push(nm.value.timestamp, {
            subIdx: top.value.subIdx,
            msg: nm.value,
          });
        }
        return { value: out, done: false };
      },
      async return(): Promise<IteratorResult<Message>> {
        exhausted = true;
        for (const s of subs) {
          if (s.return !== undefined) {
            await s.return();
          }
        }
        return { value: undefined, done: true };
      },
    };

    return iter;
  }

  /**
   * Resolve floor messages across every reader. For each
   * (queries[i].topic, queries[i].timestamps[j]) pair, out[i][j] is the floor
   * result with the latest found timestamp among all readers that hold the
   * topic (by exposed name) - the true floor over the union. Ties resolve
   * arbitrarily.
   *
   * Preconditions, validated before any data I/O (aggregated into a single
   * SampleValidationError):
   *   - each queries[i].timestamps must be strictly increasing
   *   - each queries[i].topic must be unique across all i
   *   - each queries[i].topic must exist in at least one reader
   *
   * When videoDecodable is set, a video topic supplied by more than one reader
   * must have time-disjoint source ranges; overlapping sources throw
   * VideoSourcesOverlapError.
   */
  async sample(
    queries: SampleQuery[],
    opts: SampleOptions = {},
  ): Promise<SampleResult[][]> {
    const videoDecodable = opts.videoDecodable === true;
    const bounds = await Promise.all(this.readers.map((r) => r.topicBounds()));

    validateMultiSampleQueries(queries, bounds);

    if (videoDecodable) {
      enforceVideoDisjoint(
        queries.map((q) => q.topic),
        bounds,
      );
    }

    const out: SampleResult[][] = queries.map((q) =>
      q.timestamps.map(() => ({
        found: false,
        timestamp: 0n,
        data: new Uint8Array(0),
        isVideo: false,
        frames: [],
        resetDecoder: false,
      })),
    );

    // Dispatch to each reader only the queries whose topic it holds (avoids
    // the engine's unknown-topic error), then merge by latest found floor.
    for (let ri = 0; ri < this.readers.length; ri++) {
      const subset: SampleQuery[] = [];
      const origIdx: number[] = [];
      for (let qi = 0; qi < queries.length; qi++) {
        if (bounds[ri]!.has(queries[qi]!.topic)) {
          subset.push(queries[qi]!);
          origIdx.push(qi);
        }
      }
      if (subset.length === 0) {
        continue;
      }
      const res = await this.readers[ri]!.sample(subset, opts);
      for (let sub = 0; sub < origIdx.length; sub++) {
        const qi = origIdx[sub]!;
        const row = res[sub]!;
        for (let j = 0; j < row.length; j++) {
          const cand = row[j]!;
          if (!cand.found) {
            continue;
          }
          const cur = out[qi]![j]!;
          if (!cur.found || cand.timestamp > cur.timestamp) {
            out[qi]![j] = cand;
          }
        }
      }
    }
    return out;
  }
}

/**
 * The exposed topic names a readMessages call covers: the requested
 * topicNames, or every exposed topic across all readers when unset.
 */
function readScopeTopics(
  topicNames: string[] | undefined,
  bounds: Map<string, TopicBound>[],
): string[] {
  if (topicNames !== undefined && topicNames.length > 0) {
    return topicNames;
  }
  const seen = new Set<string>();
  const out: string[] = [];
  for (const b of bounds) {
    for (const name of b.keys()) {
      if (!seen.has(name)) {
        seen.add(name);
        out.push(name);
      }
    }
  }
  return out;
}

/**
 * Mirror the single-reader checks but treat a topic as known if any reader
 * holds it. All violations are aggregated into one SampleValidationError.
 * Mirrors go validateMultiSampleQueries.
 */
function validateMultiSampleQueries(
  queries: SampleQuery[],
  bounds: Map<string, TopicBound>[],
): void {
  const seen = new Map<string, number>();
  const violations: string[] = [];
  for (let i = 0; i < queries.length; i++) {
    const q = queries[i]!;
    const known = bounds.some((b) => b.has(q.topic));
    if (!known) {
      violations.push(
        `queries[${i}]: unknown topic "${q.topic}" (not in any reader)`,
      );
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

/**
 * Throw VideoSourcesOverlapError when any of the given exposed video topics is
 * provided by readers with overlapping inclusive time ranges. Topics that are
 * not video, or are held by fewer than two readers, are skipped. Mirrors go
 * enforceVideoDisjoint.
 */
function enforceVideoDisjoint(
  topics: string[],
  bounds: Map<string, TopicBound>[],
): void {
  for (const topic of topics) {
    const ranges: [bigint, bigint][] = [];
    let isVideo = false;
    for (const b of bounds) {
      const bound = b.get(topic);
      if (bound === undefined) {
        continue;
      }
      if (bound.isVideo) {
        isVideo = true;
      }
      ranges.push([bound.minTs, bound.maxTs]);
    }
    if (!isVideo || ranges.length < 2) {
      continue;
    }
    for (let a = 0; a < ranges.length; a++) {
      for (let c = a + 1; c < ranges.length; c++) {
        if (ranges[a]![0] <= ranges[c]![1] && ranges[c]![0] <= ranges[a]![1]) {
          throw new VideoSourcesOverlapError(topic, ranges[a]!, ranges[c]!);
        }
      }
    }
  }
}
