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
class SampleHit:
    found: bool = False
    timestamp: int = 0
    data: bytes = b""


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


@dataclass
class _QueryState:
    spec: SampleSpec
    group_idx: int = 0
    topic_id: int = 0
    is_compressed: bool = False
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
) -> List[List[SampleHit]]:
    """Resolve floor messages for the given specs. Returns shape-matching results."""
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
                        ti.topic_metadatas[0].metadata.get("is_compressed", False)
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
        qs.resolved.append(
            _ResolvedItem(
                t_idx=p.t_idx,
                group_idx=qs.group_idx,
                chunk_idx=c,
                msg_off_in_chunk=mis[k].offset_in_chunk,
                msg_len=lens[k],
                timestamp=mis[k].timestamp,
            )
        )
    return fallback


def _run_phase_b(
    fetcher: Fetcher,
    strategy: ReadStrategy,
    states: List[_QueryState],
    cache: Dict[Tuple[int, int], _ChunkCache],
) -> None:
    seen_chunk = set()
    entries: List[Tuple[Tuple[int, int], int, int]] = []  # (key, offset, length)
    for qs in states:
        for r in qs.resolved:
            ic = cache[(r.group_idx, r.chunk_idx)].ic
            if qs.is_compressed:
                key = (r.group_idx, r.chunk_idx)
                if key in seen_chunk:
                    continue
                seen_chunk.add(key)
                entries.append((key, ic.chunk_offset, ic.chunk_len))
            else:
                entries.append(
                    (
                        (r.group_idx, r.chunk_idx),
                        ic.chunk_offset + r.msg_off_in_chunk,
                        r.msg_len,
                    )
                )
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

    # Uncompressed: per-message data.
    for qs in states:
        if qs.is_compressed:
            continue
        for r in qs.resolved:
            ic = cache[(r.group_idx, r.chunk_idx)].ic
            raw = loaded.get(ic.chunk_offset + r.msg_off_in_chunk)
            qs.results[r.t_idx].found = True
            qs.results[r.t_idx].timestamp = r.timestamp
            qs.results[r.t_idx].data = bytes(raw)
