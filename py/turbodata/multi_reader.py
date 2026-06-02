# Copyright 2026 Wanjia He
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""MultiReader: present several single-file Readers as one time-ordered stream.

Mirrors go/multi_reader.go. read_messages merges every reader's stream in
timestamp order; sample resolves floor messages across the union of readers.
Both operate entirely in each Reader's exposed-name space.

Topic-name collisions across files are the caller's responsibility: configure
per-Reader topic_remap so that names meant to union share an exposed name and
names meant to stay distinct do not.
"""
from __future__ import annotations

import heapq
from typing import Dict, Iterator, List, Optional, Tuple

from ._iter import Order
from .errors import SampleValidationError, VideoSourcesOverlapError
from .reader import Message, Reader, SampleQuery, SampleResult, _TopicBound
from .strategy import ReadStrategy

_MAX_I64 = (1 << 63) - 1


class MultiReader:
    """Groups several Readers into one merged view. At least one reader is
    required. Per-file behavior (remap, etc.) lives on each underlying Reader.
    """

    def __init__(self, *readers: Reader) -> None:
        if not readers:
            raise ValueError("turbodata: MultiReader requires at least one reader")
        self._readers: List[Reader] = list(readers)

    # ---- read_messages --------------------------------------------------
    def read_messages(
        self,
        *,
        topic_names: Optional[List[str]] = None,
        start_timestamp: int = 0,
        end_timestamp: int = _MAX_I64,
        order: Order = Order.TIME,
        strategy: Optional[ReadStrategy] = None,
        tail_prefetch: int = 0,
        copy: bool = False,
        video_decodable: bool = False,
    ) -> Iterator[Message]:
        """Iterate every reader's messages as one merged, time-ordered stream.

        Options pass through to each underlying Reader.read_messages unchanged,
        so order, time bounds, topic filtering (by exposed name), and strategy
        apply per file before the merge.

        When video_decodable is True, any in-scope video topic supplied by more
        than one reader must have time-disjoint source ranges; overlapping
        sources raise VideoSourcesOverlapError (merging them would interleave
        frames from different GOP chains into an undecodable stream).
        """
        subs = [
            r.read_messages(
                topic_names=topic_names,
                start_timestamp=start_timestamp,
                end_timestamp=end_timestamp,
                order=order,
                strategy=strategy,
                tail_prefetch=tail_prefetch,
                copy=copy,
                video_decodable=video_decodable,
            )
            for r in self._readers
        ]

        if video_decodable:
            bounds = [r._topic_bounds() for r in self._readers]
            _enforce_video_disjoint(_read_scope_topics(topic_names, bounds), bounds)

        return self._merge(subs, order)

    def _merge(self, subs: List[Iterator[Message]], order: Order) -> Iterator[Message]:
        # Heap entries: (key, counter, sub_idx, message). counter disambiguates
        # equal keys so we never compare Message objects.
        reverse = order == Order.REVERSE_TIME
        heap: List[Tuple[int, int, int, Message]] = []
        counter = 0

        for i, sub in enumerate(subs):
            msg = next(sub, None)
            if msg is None:
                continue
            key = -msg.timestamp if reverse else msg.timestamp
            heap.append((key, counter, i, msg))
            counter += 1
        heapq.heapify(heap)

        while heap:
            _, _, si, msg = heapq.heappop(heap)
            yield msg
            nxt = next(subs[si], None)
            if nxt is not None:
                nkey = -nxt.timestamp if reverse else nxt.timestamp
                heapq.heappush(heap, (nkey, counter, si, nxt))
                counter += 1

    # ---- sample ---------------------------------------------------------
    def sample(
        self,
        queries: List[SampleQuery],
        *,
        strategy: Optional[ReadStrategy] = None,
        tail_prefetch: int = 0,
        video_decodable: bool = False,
    ) -> List[List[SampleResult]]:
        """Resolve floor messages across every reader.

        For each (queries[i].topic, queries[i].timestamps[j]) pair, out[i][j] is
        the floor result with the latest found timestamp among all readers that
        hold the topic (by exposed name) - the true floor over the union. Ties
        resolve arbitrarily.

        Preconditions, validated before any data I/O:
          - each queries[i].timestamps must be strictly increasing
          - each queries[i].topic must be unique across all i
          - each queries[i].topic must exist in at least one reader
        Violations are aggregated into a single SampleValidationError.

        When video_decodable is True, a video topic supplied by more than one
        reader must have time-disjoint source ranges; overlapping sources raise
        VideoSourcesOverlapError.
        """
        bounds = [r._topic_bounds() for r in self._readers]

        _validate_multi_sample_queries(queries, bounds)

        if video_decodable:
            _enforce_video_disjoint([q.topic for q in queries], bounds)

        out: List[List[SampleResult]] = [
            [SampleResult(found=False, timestamp=0, data=b"") for _ in q.timestamps]
            for q in queries
        ]

        # Dispatch to each reader only the queries whose topic it holds (avoids
        # the engine's unknown-topic error), then merge by latest found floor.
        for ri, r in enumerate(self._readers):
            subset: List[SampleQuery] = []
            orig_idx: List[int] = []
            for qi, q in enumerate(queries):
                if q.topic in bounds[ri]:
                    subset.append(q)
                    orig_idx.append(qi)
            if not subset:
                continue
            res = r.sample(
                subset,
                strategy=strategy,
                tail_prefetch=tail_prefetch,
                video_decodable=video_decodable,
            )
            for sub, qi in enumerate(orig_idx):
                row = res[sub]
                for j, cand in enumerate(row):
                    if not cand.found:
                        continue
                    cur = out[qi][j]
                    if not cur.found or cand.timestamp > cur.timestamp:
                        out[qi][j] = cand
        return out


def _read_scope_topics(
    topic_names: Optional[List[str]], bounds: List[Dict[str, _TopicBound]]
) -> List[str]:
    """The exposed topic names a read_messages call covers: the requested
    topic_names, or every exposed topic across all readers when unset."""
    if topic_names:
        return list(topic_names)
    seen: set = set()
    out: List[str] = []
    for b in bounds:
        for name in b:
            if name not in seen:
                seen.add(name)
                out.append(name)
    return out


def _validate_multi_sample_queries(
    queries: List[SampleQuery], bounds: List[Dict[str, _TopicBound]]
) -> None:
    """Mirror the single-reader checks but treat a topic as known if any reader
    holds it. All violations are aggregated. Mirrors go validateMultiSampleQueries."""
    seen: Dict[str, int] = {}
    violations: List[str] = []
    for i, q in enumerate(queries):
        known = any(q.topic in b for b in bounds)
        if not known:
            violations.append(f"queries[{i}]: unknown topic {q.topic!r} (not in any reader)")
        if q.topic in seen:
            violations.append(
                f"queries[{i}]: duplicate topic {q.topic!r} already used by queries[{seen[q.topic]}]"
            )
        else:
            seen[q.topic] = i
        for j in range(1, len(q.timestamps)):
            if q.timestamps[j] <= q.timestamps[j - 1]:
                violations.append(
                    f"queries[{i}].timestamps not strictly increasing at position {j} "
                    f"({q.timestamps[j]} <= {q.timestamps[j - 1]})"
                )
    if violations:
        raise SampleValidationError(violations)


def _enforce_video_disjoint(
    topics: List[str], bounds: List[Dict[str, _TopicBound]]
) -> None:
    """Raise VideoSourcesOverlapError when any of the given exposed video
    topics is provided by readers with overlapping inclusive time ranges.
    Topics that are not video, or are held by fewer than two readers, are
    skipped. Mirrors go enforceVideoDisjoint."""
    for topic in topics:
        ranges: List[Tuple[int, int]] = []
        is_video = False
        for b in bounds:
            bound = b.get(topic)
            if bound is None:
                continue
            if bound.is_video:
                is_video = True
            ranges.append((bound.min_ts, bound.max_ts))
        if not is_video or len(ranges) < 2:
            continue
        for a in range(len(ranges)):
            for c in range(a + 1, len(ranges)):
                if ranges[a][0] <= ranges[c][1] and ranges[c][0] <= ranges[a][1]:
                    raise VideoSourcesOverlapError(topic, ranges[a], ranges[c])
