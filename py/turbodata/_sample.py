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

"""Sample engine: floor-message lookup at concrete timestamps.

Ports go/internal/iter/sample.go. Two concurrent I/O waves:

  Phase A: fetch index chunks for candidate chunks per (topic, timestamp),
           decode them, advance per-query cursors, re-queue fallbacks against
           chunk c-1 when the candidate doesn't actually have a message of the
           topic with ts <= T. Loop until no items pending.

  Phase B: fetch data ranges (chunk-level for compressed groups, per-message
           for uncompressed groups) in one concurrent wave, decompress, copy
           into result slots.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from . import _codec, _compress
from ._iorange import Fetcher, LoadedBytes, Range, plan
from .strategy import ReadStrategy


@dataclass
class SampleSpec:
    topic: str
    timestamps: List[int]


@dataclass
class _SampleFrame:
    """Engine-level coded video frame surfaced in a SampleHit's GOP prefix or
    incremental tail. Maps to the public turbodata.Frame."""

    timestamp: int
    is_key_frame: bool
    data: bytes


@dataclass
class SampleHit:
    found: bool = False
    timestamp: int = 0
    data: bytes = b""
    is_video: bool = False
    frames: List[_SampleFrame] = field(default_factory=list)
    reset_decoder: bool = False


@dataclass
class _PendingItem:
    t_idx: int
    chunk_idx: int


@dataclass
class _ResolvedItem:
    t_idx: int
    group_idx: int
    chunk_idx: int
    msg_off_in_chunk: int
    msg_len: int
    timestamp: int
    # Video-only: positions into the topic's message_indexes in the cached
    # IndexChunk. keyframe_msg_idx is the anchor key frame for the GOP that
    # contains the target; target_msg_idx is the target's position.
    keyframe_msg_idx: int = 0
    target_msg_idx: int = 0


@dataclass
class _QueryState:
    spec: SampleSpec
    group_idx: int = 0
    topic_id: int = 0
    is_compressed: bool = False
    is_video: bool = False
    results: List[SampleHit] = field(default_factory=list)
    pending: List[_PendingItem] = field(default_factory=list)
    resolved: List[_ResolvedItem] = field(default_factory=list)


@dataclass
class _ChunkCache:
    ic: _codec.IndexChunk
    per_topic_lens: Dict[int, List[int]]


def _build_chunk_cache(ic: _codec.IndexChunk) -> _ChunkCache:
    """Flatten all topics' MessageIndexes to derive per-message lengths from
    offset deltas; project back into per-topic length slices."""
    items: List[Tuple[int, int, int]] = []  # (topic_id, offset, idx_within_topic)
    for ti in ic.topic_indexes:
        for i, mi in enumerate(ti.message_indexes):
            items.append((ti.id, mi.offset_in_chunk, i))
    items.sort(key=lambda x: x[1])

    per_topic_lens: Dict[int, List[int]] = {
        ti.id: [0] * len(ti.message_indexes) for ti in ic.topic_indexes
    }
    for i in range(len(items) - 1):
        tid, _, idx = items[i]
        per_topic_lens[tid][idx] = items[i + 1][1] - items[i][1]
    if items:
        tid, off, idx = items[-1]
        per_topic_lens[tid][idx] = ic.uncompressed_len - off
    return _ChunkCache(ic=ic, per_topic_lens=per_topic_lens)


def _find_topic_index(ic: _codec.IndexChunk, topic_id: int) -> Optional[_codec.TopicIndex]:
    for ti in ic.topic_indexes:
        if ti.id == topic_id:
            return ti
    return None


def sample(
    source,
    summary: _codec.Summary,
    specs: List[SampleSpec],
    strategy: ReadStrategy,
    video_decodable: bool = False,
) -> List[List[SampleHit]]:
    """Resolve floor messages for the given specs. Returns shape-matching results.

    When video_decodable is True, video topics return a decoder-ready GOP
    sequence (frames + reset_decoder) instead of a single floor frame. Without
    it, video topics are sampled like any other topic.
    """
    if not specs:
        return []

    fetcher = Fetcher(source, strategy.max_concurrency)

    # Locate each topic's group + id, init query state.
    query_states: List[_QueryState] = []
    for spec in specs:
        qs = _QueryState(
            spec=spec,
            results=[SampleHit() for _ in spec.timestamps],
        )
        for gi, ti in enumerate(summary.topics_infos):
            found = False
            for tm in ti.topic_metadatas:
                if tm.name == spec.topic:
                    qs.group_idx = gi
                    qs.topic_id = tm.id
                    qs.is_compressed = bool(
                        ti.topic_metadatas[0].metadata.get(_codec.META_KEY_COMPRESSED, False)
                    )
                    # Video decoding is opt-in. Without it, a video topic is
                    # sampled like any other topic (its floor frame in data).
                    qs.is_video = video_decodable and bool(
                        ti.topic_metadatas[0].metadata.get(_codec.META_KEY_VIDEO, False)
                    )
                    found = True
                    break
            if found:
                break
        query_states.append(qs)

    # Assign initial candidates: per query, walk timestamps alongside index
    # chunks with a single forward cursor.
    for qs in query_states:
        chunks = summary.topics_infos[qs.group_idx].index_chunk_info_list
        if not chunks:
            continue
        c = -1
        for t_idx, T in enumerate(qs.spec.timestamps):
            while c + 1 < len(chunks) and chunks[c + 1].start_timestamp <= T:
                c += 1
            if c < 0:
                continue  # T precedes all chunks; results[t_idx].found stays False
            qs.pending.append(_PendingItem(t_idx=t_idx, chunk_idx=c))

    chunks_cache: Dict[Tuple[int, int], _ChunkCache] = {}

    # Phase A: fetch + decode candidate chunks, walk pending items, re-queue
    # fallbacks against chunk c-1. Bounded by chunk count.
    while any(qs.pending for qs in query_states):
        needs = _gather_chunk_needs(summary, query_states, chunks_cache)
        if needs:
            _fetch_and_decode_index_chunks(fetcher, strategy, needs, chunks_cache)

        any_progress = False
        for qs in query_states:
            if not qs.pending:
                continue
            still: List[_PendingItem] = []
            i = 0
            while i < len(qs.pending):
                j = i
                while (
                    j < len(qs.pending)
                    and qs.pending[j].chunk_idx == qs.pending[i].chunk_idx
                ):
                    j += 1
                c = qs.pending[i].chunk_idx
                cache = chunks_cache.get((qs.group_idx, c))
                if cache is None:
                    still.extend(qs.pending[i:j])
                    i = j
                    continue
                any_progress = True
                still.extend(_resolve_items_in_chunk(qs, c, cache, qs.pending[i:j]))
                i = j
            qs.pending = still

        if not any_progress and any(qs.pending for qs in query_states):
            raise RuntimeError(
                "sample: phase A made no progress with pending items remaining"
            )

    # Phase B: data ranges for all resolved items.
    _run_phase_b(fetcher, strategy, query_states, chunks_cache)

    # Mark every result of a video query (including not-found ones) so callers
    # can branch on is_video. Mirrors the Go engine's collect step.
    for qs in query_states:
        if qs.is_video:
            for h in qs.results:
                h.is_video = True

    return [qs.results for qs in query_states]


def _gather_chunk_needs(
    summary: _codec.Summary,
    states: List[_QueryState],
    cache: Dict[Tuple[int, int], _ChunkCache],
) -> List[Tuple[Tuple[int, int], int, int]]:
    """Returns [(key, offset, length), ...] for chunks not yet cached."""
    seen = set()
    out: List[Tuple[Tuple[int, int], int, int]] = []
    for qs in states:
        for p in qs.pending:
            key = (qs.group_idx, p.chunk_idx)
            if key in cache or key in seen:
                continue
            seen.add(key)
            ti = summary.topics_infos[key[0]]
            info = ti.index_chunk_info_list[key[1]]
            if key[1] < len(ti.index_chunk_info_list) - 1:
                ln = ti.index_chunk_info_list[key[1] + 1].offset - info.offset
            else:
                ln = ti.total_len - info.offset + ti.index_chunk_info_list[0].offset
            out.append((key, info.offset, ln))
    out.sort(key=lambda e: e[1])
    return out


def _fetch_and_decode_index_chunks(
    fetcher: Fetcher,
    strategy: ReadStrategy,
    needs: List[Tuple[Tuple[int, int], int, int]],
    cache: Dict[Tuple[int, int], _ChunkCache],
) -> None:
    ranges = [Range(offset=offs, length=ln) for _, offs, ln in needs]
    ops, locs = plan(ranges, strategy.coalesce_gap, strategy.split_threshold)
    bufs = fetcher.execute(ops)
    loaded = LoadedBytes(ranges, locs, bufs)
    for key, offs, _ln in needs:
        raw = loaded.get(offs)
        decompressed = _compress.decompress(raw)
        ic = _codec.read_index_chunk_from_bytes(decompressed)
        cache[key] = _build_chunk_cache(ic)


def _resolve_items_in_chunk(
    qs: _QueryState,
    c: int,
    cache: _ChunkCache,
    items: List[_PendingItem],
) -> List[_PendingItem]:
    fallback: List[_PendingItem] = []
    ti = _find_topic_index(cache.ic, qs.topic_id)
    if ti is None or not ti.message_indexes:
        for p in items:
            if c == 0:
                qs.results[p.t_idx].found = False
            else:
                fallback.append(_PendingItem(t_idx=p.t_idx, chunk_idx=c - 1))
        return fallback

    mis = ti.message_indexes
    lens = cache.per_topic_lens[qs.topic_id]
    # For video, kf_pos tracks the position within key_frame_indexes of the
    # most recent key frame index <= k. Items in this run are in increasing T
    # (hence increasing k) order, so the cursor only moves forward.
    kf_pos = 0
    k = 0
    for p in items:
        T = qs.spec.timestamps[p.t_idx]
        while k + 1 < len(mis) and mis[k + 1].timestamp <= T:
            k += 1
        if mis[k].timestamp > T:
            if c == 0:
                qs.results[p.t_idx].found = False
            else:
                fallback.append(_PendingItem(t_idx=p.t_idx, chunk_idx=c - 1))
            continue
        item = _ResolvedItem(
            t_idx=p.t_idx,
            group_idx=qs.group_idx,
            chunk_idx=c,
            msg_off_in_chunk=mis[k].offset_in_chunk,
            msg_len=lens[k],
            timestamp=mis[k].timestamp,
        )
        if qs.is_video:
            kfs = ti.key_frame_indexes
            if not kfs or kfs[0] > k:
                # No anchor key frame for this target in this chunk: violates
                # the writer's GOP-integrity invariant. Treat as not found
                # rather than emit an undecodable single frame.
                qs.results[p.t_idx].found = False
                continue
            while kf_pos + 1 < len(kfs) and kfs[kf_pos + 1] <= k:
                kf_pos += 1
            item.keyframe_msg_idx = kfs[kf_pos]
            item.target_msg_idx = k
        qs.resolved.append(item)
    return fallback


def _run_phase_b(
    fetcher: Fetcher,
    strategy: ReadStrategy,
    states: List[_QueryState],
    cache: Dict[Tuple[int, int], _ChunkCache],
) -> None:
    seen_chunk = set()
    entries: List[Tuple[Tuple[int, int], int, int]] = []  # (key, offset, length)
    # Video: dedupe per (group, chunk, keyframe). All queries that share a GOP
    # register a single range from the keyframe to the furthest target across
    # those queries; shorter-target queries slice less of the same loaded bytes.
    # Maps (group, chunk, keyframe_msg_idx) -> [start_in_chunk, end_in_chunk_excl].
    video_extents: Dict[Tuple[int, int, int], List[int]] = {}
    for qs in states:
        for r in qs.resolved:
            cc = cache[(r.group_idx, r.chunk_idx)]
            ic = cc.ic
            if qs.is_compressed:
                key = (r.group_idx, r.chunk_idx)
                if key in seen_chunk:
                    continue
                seen_chunk.add(key)
                entries.append((key, ic.chunk_offset, ic.chunk_len))
            elif qs.is_video:
                # Video groups are single-topic by writer invariant.
                ti = ic.topic_indexes[0]
                mis = ti.message_indexes
                lens = cc.per_topic_lens[qs.topic_id]
                start = mis[r.keyframe_msg_idx].offset_in_chunk
                end_excl = mis[r.target_msg_idx].offset_in_chunk + lens[r.target_msg_idx]
                vk = (r.group_idx, r.chunk_idx, r.keyframe_msg_idx)
                ve = video_extents.get(vk)
                if ve is not None:
                    if end_excl > ve[1]:
                        ve[1] = end_excl
                else:
                    video_extents[vk] = [start, end_excl]
            else:
                entries.append(
                    (
                        (r.group_idx, r.chunk_idx),
                        ic.chunk_offset + r.msg_off_in_chunk,
                        r.msg_len,
                    )
                )
    for (gi, ci, _kf), ve in video_extents.items():
        ic = cache[(gi, ci)].ic
        entries.append(((gi, ci), ic.chunk_offset + ve[0], ve[1] - ve[0]))
    if not entries:
        return
    entries.sort(key=lambda e: e[1])

    ranges = [Range(offset=offs, length=ln) for _, offs, ln in entries]
    ops, locs = plan(ranges, strategy.coalesce_gap, strategy.split_threshold)
    bufs = fetcher.execute(ops)
    loaded = LoadedBytes(ranges, locs, bufs)

    # Compressed: group resolved items by chunk so each chunk decompresses once.
    by_chunk: Dict[Tuple[int, int], List[Tuple[_QueryState, _ResolvedItem]]] = {}
    for qs in states:
        if not qs.is_compressed:
            continue
        for r in qs.resolved:
            by_chunk.setdefault((r.group_idx, r.chunk_idx), []).append((qs, r))
    for key, items in by_chunk.items():
        ic = cache[key].ic
        compressed = loaded.get(ic.chunk_offset)
        decompressed = _compress.decompress(compressed)
        for qs, r in items:
            qs.results[r.t_idx].found = True
            qs.results[r.t_idx].timestamp = r.timestamp
            qs.results[r.t_idx].data = bytes(
                decompressed[r.msg_off_in_chunk : r.msg_off_in_chunk + r.msg_len]
            )

    # Uncompressed non-video: per-message data.
    for qs in states:
        if qs.is_compressed or qs.is_video:
            continue
        for r in qs.resolved:
            ic = cache[(r.group_idx, r.chunk_idx)].ic
            raw = loaded.get(ic.chunk_offset + r.msg_off_in_chunk)
            qs.results[r.t_idx].found = True
            qs.results[r.t_idx].timestamp = r.timestamp
            qs.results[r.t_idx].data = bytes(raw)

    # Video: incremental frames + reset_decoder.
    for qs in states:
        if not qs.is_video:
            continue
        _materialize_video(qs, cache, loaded)


def _materialize_video(
    qs: _QueryState,
    cache: Dict[Tuple[int, int], _ChunkCache],
    loaded: LoadedBytes,
) -> None:
    """Walk one query's resolved items in target-timestamp order and emit
    incremental frames + reset_decoder per the public SampleResult contract."""
    if not qs.resolved:
        return
    # Resolved items can arrive out of t_idx order if Phase A required
    # fallbacks; sort so the row's processing order matches the caller's.
    resolved = sorted(qs.resolved, key=lambda r: r.t_idx)

    # Decoder-state continuity is per-row, scoped to a single GOP (= same
    # chunk + same keyframe_msg_idx).
    have_prev = False
    prev_chunk: Tuple[int, int] = (0, 0)
    prev_keyframe_idx = 0
    prev_target_msg_idx = 0

    for r in resolved:
        cc = cache[(r.group_idx, r.chunk_idx)]
        ti = cc.ic.topic_indexes[0]
        mis = ti.message_indexes
        lens = cc.per_topic_lens[qs.topic_id]
        chunk_key = (r.group_idx, r.chunk_idx)

        same_gop = (
            have_prev
            and prev_chunk == chunk_key
            and prev_keyframe_idx == r.keyframe_msg_idx
        )
        if not same_gop:
            start_idx = r.keyframe_msg_idx
            reset = True
        else:
            start_idx = prev_target_msg_idx + 1
            reset = False

        # The Phase B range starts at the GOP's keyframe in this chunk and is
        # long enough to cover the furthest target across queries in this GOP.
        raw = loaded.get(cc.ic.chunk_offset + mis[r.keyframe_msg_idx].offset_in_chunk)
        base_in_chunk = mis[r.keyframe_msg_idx].offset_in_chunk

        frames: List[_SampleFrame] = []
        if start_idx <= r.target_msg_idx:
            for i in range(start_idx, r.target_msg_idx + 1):
                frame_start = mis[i].offset_in_chunk - base_in_chunk
                frame_len = lens[i]
                frames.append(
                    _SampleFrame(
                        timestamp=mis[i].timestamp,
                        # keyframe_msg_idx is the greatest key frame index <=
                        # target_msg_idx, so it is the only key frame in range.
                        is_key_frame=(i == r.keyframe_msg_idx),
                        data=bytes(raw[frame_start : frame_start + frame_len]),
                    )
                )
            # data stays empty for video; target is the last frame.
            qs.results[r.t_idx].timestamp = frames[-1].timestamp
        else:
            # Same target as the previous in-row result: nothing new to feed.
            qs.results[r.t_idx].timestamp = mis[r.target_msg_idx].timestamp
        qs.results[r.t_idx].found = True
        qs.results[r.t_idx].frames = frames
        qs.results[r.t_idx].reset_decoder = reset

        have_prev = True
        prev_chunk = chunk_key
        prev_keyframe_idx = r.keyframe_msg_idx
        prev_target_msg_idx = r.target_msg_idx
