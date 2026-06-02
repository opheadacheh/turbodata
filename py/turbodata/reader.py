"""Reader: top-level read API for turbodata files.

Mirrors go/reader.go and go/sampler.go. Two read paths:

  default       - serial seek+read, one chunk at a time. Minimal memory.
  cost-aware    - cost-aware pre-fetch + concurrent reads governed by a
                  ReadStrategy. Best for cloud object storage and large reads.

Both produce the same merged time-ordered (or reverse-time-ordered) stream.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Iterator, List, Optional, Set, Tuple

from . import _codec, _compress
from ._iorange import Fetcher, LoadedBytes, Range, plan
from ._iter import (
    Order,
    PreloadedTopicsGroupIterator,
    TopicsGroupIterator,
    _MsgIdxWithTopicId,
    _filter_index_chunks_in_range,
    sort_and_filter,
)
from ._sample import SampleSpec, sample as _sample
from .errors import (
    FileTooSmallError,
    InvalidMagicError,
    SampleValidationError,
    TopicRemapCollisionError,
)
from .source import ReadSource
from .strategy import ReadStrategy


_MAX_I64 = (1 << 63) - 1


class Message:
    """One message yielded by read_messages.

    `data` aliases an internal buffer and is valid only until the next
    iteration step. Use `copy=True` on read_messages, or call `bytes(msg.data)`
    yourself, to retain bytes past the next pull.
    """

    __slots__ = ("timestamp", "topic_name", "data")

    def __init__(self, timestamp: int, topic_name: str, data: bytes) -> None:
        self.timestamp = timestamp
        self.topic_name = topic_name
        self.data = data

    def __repr__(self) -> str:
        head = self.data[:24]
        return f"Message(ts={self.timestamp}, topic={self.topic_name!r}, data={head!r}...)"


@dataclass
class SampleQuery:
    """One sampling request: floor message at each timestamp for a topic."""

    topic: str
    timestamps: List[int]


@dataclass
class Frame:
    """One coded video frame surfaced as part of a SampleResult's GOP prefix or
    incremental tail. `data` is an owned copy and is safe to retain."""

    timestamp: int
    is_key_frame: bool
    data: bytes


@dataclass
class SampleResult:
    """One returned message. ``out[i][j]`` corresponds to queries[i].timestamps[j].

    Without video_decodable, every topic (including video topics) returns its
    floor message in ``data`` (an owned copy), ``is_video`` is False, and
    ``frames`` / ``reset_decoder`` are unset.

    With video_decodable, results for video topics instead carry a
    decoder-ready GOP sequence and set ``is_video`` True. For those results,
    within a single row (one topic, strictly increasing query timestamps):

      - ``data`` is empty; the frame bytes live in ``frames`` and the target
        frame is the last element. ``timestamp`` names that target frame.
      - For the first found result in each GOP encountered in the row:
        ``reset_decoder`` is True, ``frames`` = [keyframe ... target] in
        storage (decode) order. The decoder must reset before feeding.
      - For subsequent found results within the same GOP: ``reset_decoder`` is
        False, ``frames`` = [previous_target+1 ... target] (only the new
        frames). ``frames`` is empty when two queries resolve to the same
        target frame; ``timestamp`` still names that target.
    """

    found: bool
    timestamp: int
    data: bytes
    is_video: bool = False
    frames: List[Frame] = field(default_factory=list)
    reset_decoder: bool = False


DEFAULT_SAMPLE_STRATEGY = ReadStrategy(
    coalesce_gap=1 << 20,
    split_threshold=4 << 20,
    max_concurrency=16,
)


@dataclass
class _TopicBound:
    """Per-exposed-topic span used by MultiReader. ``is_video`` flags a video
    topic (always a single-topic group); ``[min_ts, max_ts]`` is the inclusive
    timestamp span of the topic's messages. Mirrors go topicBound."""

    is_video: bool
    min_ts: int
    max_ts: int


class Reader:
    def __init__(
        self,
        source: ReadSource,
        *,
        topic_remap: Optional[Dict[str, str]] = None,
    ) -> None:
        """Open a reader over `source`.

        topic_remap presents in-file topic names under different exposed names.
        The dict is keyed by in-file name and valued by the exposed name
        emitted from read_messages, accepted by topic_names and
        SampleQuery.topic, and reported by summary(). Names absent from the map
        pass through unchanged; None/empty means identity (no behavior change).

        The remap is validated lazily against the file's summary on first use:
        it is an error (TopicRemapCollisionError) for two topics to collapse
        onto the same exposed name, whether two in-file names map to the same
        target or a renamed name collides with an untouched in-file name.
        """
        self._source = source
        self._summary: Optional[_codec.Summary] = None
        self._footer: Optional[_codec.Footer] = None

        # topic_remap maps in-file -> exposed names. nil/empty means identity.
        self._topic_remap: Dict[str, str] = dict(topic_remap) if topic_remap else {}
        # Cached prepare_rename outputs (computed lazily once the summary is
        # known). _rename_inverse maps exposed -> in-file; _rename_err captures
        # a remap collision detected against the actual summary.
        self._rename_prepared = False
        self._rename_inverse: Optional[Dict[str, str]] = None
        self._rename_err: Optional[TopicRemapCollisionError] = None

        # Cached per-exposed-topic bounds, computed lazily by _topic_bounds.
        self._bounds_cache: Optional[Dict[str, _TopicBound]] = None

    # ---- remap helpers --------------------------------------------------
    def _prepare_rename(self, summary: _codec.Summary) -> Optional[Dict[str, str]]:
        """Build the exposed -> in-file inverse map once the summary is known
        and validate that no two topics collapse onto the same exposed name.
        Results (including any error) are cached. Returns None when no remap is
        configured."""
        if self._rename_prepared:
            if self._rename_err is not None:
                raise self._rename_err
            return self._rename_inverse
        self._rename_prepared = True
        if not self._topic_remap:
            return None
        inverse: Dict[str, str] = {}
        for ti in summary.topics_infos:
            for tm in ti.topic_metadatas:
                exposed = self._topic_remap.get(tm.name, tm.name)
                if exposed in inverse:
                    self._rename_err = TopicRemapCollisionError(
                        exposed, inverse[exposed], tm.name
                    )
                    raise self._rename_err
                inverse[exposed] = tm.name
        self._rename_inverse = inverse
        return inverse

    def _exposed_summary(self, summary: _codec.Summary) -> _codec.Summary:
        """Return a copy of summary with TopicMetadata.name rewritten to exposed
        names. Index-chunk lists and metadata maps are shared (read-only here).
        When no remap is configured the original summary is returned."""
        if not self._topic_remap:
            return summary
        self._prepare_rename(summary)
        out_infos: List[_codec.TopicsInfo] = []
        for ti in summary.topics_infos:
            new_tms = [
                _codec.TopicMetadata(
                    id=tm.id,
                    name=self._topic_remap.get(tm.name, tm.name),
                    metadata=tm.metadata,
                )
                for tm in ti.topic_metadatas
            ]
            out_infos.append(
                _codec.TopicsInfo(
                    topic_metadatas=new_tms,
                    index_chunk_info_list=ti.index_chunk_info_list,
                    total_len=ti.total_len,
                )
            )
        return _codec.Summary(topics_infos=out_infos)

    # ---- summary --------------------------------------------------------
    def summary(self) -> _codec.Summary:
        """Returns the parsed summary, loaded lazily on first call. When a topic
        remap is configured, TopicMetadata.name reports exposed names."""
        return self._exposed_summary(self._summary_with_hint(0))

    def _summary_with_hint(self, prefetch: int) -> _codec.Summary:
        if self._summary is not None:
            return self._summary

        size = self._source.size()
        if size < _codec.FOOTER_LEN:
            raise FileTooSmallError("file too small to contain a footer")

        if prefetch <= 0:
            prefetch = _codec.FOOTER_LEN
        if prefetch > size:
            prefetch = size

        tail = self._source.read_at(size - prefetch, prefetch)
        footer = _codec.read_footer_from_bytes(tail[-_codec.FOOTER_LEN:])
        if footer.magic != _codec.MAGIC:
            raise InvalidMagicError("invalid magic number")
        self._footer = footer

        if len(tail) >= footer.summary_len + _codec.FOOTER_LEN:
            start = len(tail) - _codec.FOOTER_LEN - footer.summary_len
            compressed = tail[start : start + footer.summary_len]
        else:
            compressed = self._source.read_at(
                size - footer.summary_len - _codec.FOOTER_LEN, footer.summary_len
            )
        decompressed = _compress.decompress(compressed)
        self._summary = _codec.read_summary_from_bytes(decompressed)
        return self._summary

    # ---- topic bounds (for MultiReader) --------------------------------
    def _topic_bounds(self) -> Dict[str, _TopicBound]:
        """Per-exposed-topic bounds, computed once from the cached summary.
        Honors any configured remap so keys are exposed names. Mirrors go
        (*Reader).topicBounds."""
        if self._bounds_cache is not None:
            return self._bounds_cache
        summary = self._summary_with_hint(0)
        self._prepare_rename(summary)
        out: Dict[str, _TopicBound] = {}
        for ti in summary.topics_infos:
            if not ti.index_chunk_info_list:
                continue
            min_ts = ti.index_chunk_info_list[0].start_timestamp
            max_ts = ti.index_chunk_info_list[0].end_timestamp
            for info in ti.index_chunk_info_list:
                if info.start_timestamp < min_ts:
                    min_ts = info.start_timestamp
                if info.end_timestamp > max_ts:
                    max_ts = info.end_timestamp
            is_video = _is_video_topics_info(ti)
            for tm in ti.topic_metadatas:
                exposed = self._topic_remap.get(tm.name, tm.name)
                out[exposed] = _TopicBound(is_video=is_video, min_ts=min_ts, max_ts=max_ts)
        self._bounds_cache = out
        return out

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
        """Iterate messages from the file.

        topic_names    : filter to this set of topic names; None = all topics
        start_timestamp: inclusive lower bound (default 0)
        end_timestamp  : inclusive upper bound (default max int64)
        order          : Order.TIME (default) or Order.REVERSE_TIME
        strategy       : if provided, switch to the cost-aware concurrent path
        tail_prefetch  : trailing bytes to read speculatively for the summary
        copy           : if True, yield fresh bytes per message (safe to keep)
        video_decodable: if True, for any video topic in scope the effective
                         per-group start_timestamp is snapped back to the latest
                         key frame whose timestamp is <= start_timestamp, so the
                         sequence can be fed to a decoder cold. Non-video topics
                         are unaffected.

        Returns an iterator. Iteration runs lazily; pulling None ends it.
        """
        summary = self._summary_with_hint(tail_prefetch)

        # When a remap is configured, caller-supplied topic_names arrive in
        # exposed-name space. Internal matching stays in in-file space, so
        # translate exposed -> in-file (unknown names pass through and simply
        # match nothing). id_to_name is built in exposed space for output.
        inverse = self._prepare_rename(summary) if self._topic_remap else None

        if topic_names is None:
            wanted: Set[str] = {
                tm.name for ti in summary.topics_infos for tm in ti.topic_metadatas
            }
        elif inverse is not None:
            wanted = {inverse.get(n, n) for n in topic_names}
        else:
            wanted = set(topic_names)

        topic_ids: Set[int] = set()
        id_to_name: Dict[int, str] = {}
        for ti in summary.topics_infos:
            for tm in ti.topic_metadatas:
                if tm.name in wanted:
                    topic_ids.add(tm.id)
                    id_to_name[tm.id] = self._topic_remap.get(tm.name, tm.name)

        if strategy is not None:
            group_its = self._prepare_cost_aware(
                summary, topic_ids, wanted, start_timestamp, end_timestamp, order,
                strategy, video_decodable,
            )
        else:
            group_its = self._prepare_default(
                summary, topic_ids, wanted, start_timestamp, end_timestamp, order,
                video_decodable,
            )

        return self._merge_groups(group_its, id_to_name, order, copy)

    # ---- merge ---------------------------------------------------------
    def _merge_groups(
        self,
        group_its,
        id_to_name: Dict[int, str],
        order: Order,
        copy: bool,
    ) -> Iterator[Message]:
        import heapq

        # Heap entries: (key, counter, group_idx, topic_id, data). counter
        # disambiguates equal keys so we never compare data/topic_id.
        reverse = order == Order.REVERSE_TIME
        heap: List[Tuple[int, int, int, int, bytes]] = []
        counter = 0

        for i, git in enumerate(group_its):
            entry = git.next()
            if entry is None:
                continue
            ts, tid, data = entry
            key = -ts if reverse else ts
            heap.append((key, counter, i, tid, data))
            counter += 1
        heapq.heapify(heap)

        while heap:
            key, _, gi, tid, data = heapq.heappop(heap)
            ts = -key if reverse else key
            payload = bytes(data) if copy else data
            yield Message(timestamp=ts, topic_name=id_to_name.get(tid, ""), data=payload)
            nxt = group_its[gi].next()
            if nxt is not None:
                nts, ntid, ndata = nxt
                nkey = -nts if reverse else nts
                heapq.heappush(heap, (nkey, counter, gi, ntid, ndata))
                counter += 1

    # ---- default-path setup -------------------------------------------
    def _prepare_default(
        self,
        summary: _codec.Summary,
        topic_ids: Set[int],
        wanted: Set[str],
        start_ts: int,
        end_ts: int,
        order: Order,
        video_decodable: bool = False,
    ) -> List[TopicsGroupIterator]:
        out: List[TopicsGroupIterator] = []
        for ti in summary.topics_infos:
            if not any(tm.name in wanted for tm in ti.topic_metadatas):
                continue
            git = TopicsGroupIterator(
                source=self._source,
                topics_info=ti,
                topic_ids=topic_ids,
                start_timestamp=start_ts,
                end_timestamp=end_ts,
                order=order,
                video_decodable=video_decodable and _is_video_topics_info(ti),
            )
            if git.has_any():
                out.append(git)
        return out

    # ---- cost-aware-path setup ----------------------------------------
    def _prepare_cost_aware(
        self,
        summary: _codec.Summary,
        topic_ids: Set[int],
        wanted: Set[str],
        start_ts: int,
        end_ts: int,
        order: Order,
        strategy: ReadStrategy,
        video_decodable: bool = False,
    ) -> List[PreloadedTopicsGroupIterator]:
        fetcher = Fetcher(self._source, strategy.max_concurrency)

        scoped: List[Tuple[_codec.TopicsInfo, bool, List[_codec.IndexChunkInfo], List[int], bool]] = []
        for ti in summary.topics_infos:
            if not any(tm.name in wanted for tm in ti.topic_metadatas):
                continue
            is_compressed = bool(
                ti.topic_metadatas[0].metadata.get(_codec.META_KEY_COMPRESSED, False)
            )
            group_video = video_decodable and _is_video_topics_info(ti)
            infos, lens = _filter_index_chunks_in_range(ti, start_ts, end_ts)
            if not infos:
                continue
            scoped.append((ti, is_compressed, infos, lens, group_video))

        if not scoped:
            return []

        # Phase A: fetch all index chunks across all groups.
        ranges_a: List[Range] = []
        for _ti, _ic, infos, lens, _gv in scoped:
            for info, ln in zip(infos, lens):
                ranges_a.append(Range(offset=info.offset, length=ln))
        ops_a, locs_a = plan(ranges_a, strategy.coalesce_gap, strategy.split_threshold)
        bufs_a = fetcher.execute(ops_a)
        loaded_index = LoadedBytes(ranges_a, locs_a, bufs_a)

        # Decode index chunks + sort/filter to learn kept messages.
        per_group_chunks: List[List[Tuple[_codec.IndexChunk, List[_MsgIdxWithTopicId], List[int]]]] = [
            [] for _ in scoped
        ]
        for gi, (_ti, _ic, infos, _lens, group_video) in enumerate(scoped):
            for info in infos:
                raw = loaded_index.get(info.offset)
                decompressed = _compress.decompress(raw)
                ic = _codec.read_index_chunk_from_bytes(decompressed)
                msgs, m_lens = sort_and_filter(
                    ic.topic_indexes,
                    ic.uncompressed_len,
                    topic_ids,
                    start_ts,
                    end_ts,
                    group_video,
                )
                per_group_chunks[gi].append((ic, msgs, m_lens))

        # Phase B: fetch data ranges. Chunk-level for compressed groups,
        # per-message for uncompressed groups.
        ranges_b: List[Range] = []
        for gi, (_ti, is_compressed, _infos, _lens, _gv) in enumerate(scoped):
            if is_compressed:
                for ic, msgs, _m_lens in per_group_chunks[gi]:
                    if not msgs:
                        continue
                    ranges_b.append(Range(offset=ic.chunk_offset, length=ic.chunk_len))
                continue
            for ic, msgs, m_lens in per_group_chunks[gi]:
                for msg, ln in zip(msgs, m_lens):
                    ranges_b.append(
                        Range(
                            offset=ic.chunk_offset + msg.message_index.offset_in_chunk,
                            length=ln,
                        )
                    )
        ops_b, locs_b = plan(ranges_b, strategy.coalesce_gap, strategy.split_threshold)
        bufs_b = fetcher.execute(ops_b)
        loaded_data = LoadedBytes(ranges_b, locs_b, bufs_b)

        # Build per-group preloaded iterators.
        out: List[PreloadedTopicsGroupIterator] = []
        for gi, (_ti, is_compressed, _infos, _lens, _gv) in enumerate(scoped):
            index_chunks: List[_codec.IndexChunk] = []
            chunk_msgs: List[List[_MsgIdxWithTopicId]] = []
            chunk_lens: List[List[int]] = []
            for ic, msgs, m_lens in per_group_chunks[gi]:
                if not msgs:
                    continue
                index_chunks.append(ic)
                chunk_msgs.append(msgs)
                chunk_lens.append(m_lens)
            if not index_chunks:
                continue
            out.append(
                PreloadedTopicsGroupIterator(
                    is_compressed=is_compressed,
                    order=order,
                    index_chunks=index_chunks,
                    chunk_messages=chunk_msgs,
                    chunk_msg_lens=chunk_lens,
                    loaded_data=loaded_data,
                )
            )
        return out

    # ---- sample ---------------------------------------------------------
    def sample(
        self,
        queries: List[SampleQuery],
        *,
        strategy: Optional[ReadStrategy] = None,
        tail_prefetch: int = 0,
        video_decodable: bool = False,
    ) -> List[List[SampleResult]]:
        """Floor-message lookup at concrete timestamps per topic.

        Preconditions, validated before any data I/O:
          - each queries[i].timestamps must be strictly increasing
          - each queries[i].topic must be unique across all i
          - each queries[i].topic must exist in the file's summary
        Violations are aggregated into a single SampleValidationError.

        When video_decodable is True, results for video topics carry a
        decoder-ready GOP sequence (frames + reset_decoder) and set is_video.
        See SampleResult. Non-video topics are unaffected.

        Returns out[i][j] for queries[i].timestamps[j].
        """
        if strategy is None:
            strategy = DEFAULT_SAMPLE_STRATEGY

        summary = self._summary_with_hint(tail_prefetch)

        # When a remap is configured, queries arrive in exposed-name space.
        # Translate to in-file names before validation and the engine call;
        # SampleResult is positional, so no translation back is needed.
        if self._topic_remap:
            inverse = self._prepare_rename(summary)
            queries = [
                SampleQuery(
                    topic=inverse.get(q.topic, q.topic) if inverse else q.topic,
                    timestamps=q.timestamps,
                )
                for q in queries
            ]

        _validate_sample_queries(queries, summary)

        specs = [SampleSpec(topic=q.topic, timestamps=list(q.timestamps)) for q in queries]
        hits = _sample(self._source, summary, specs, strategy, video_decodable)

        out: List[List[SampleResult]] = []
        for row in hits:
            out.append(
                [
                    SampleResult(
                        found=h.found,
                        timestamp=h.timestamp,
                        data=h.data,
                        is_video=h.is_video,
                        frames=[
                            Frame(
                                timestamp=f.timestamp,
                                is_key_frame=f.is_key_frame,
                                data=f.data,
                            )
                            for f in h.frames
                        ],
                        reset_decoder=h.reset_decoder,
                    )
                    for h in row
                ]
            )
        return out


def _is_video_topics_info(ti: _codec.TopicsInfo) -> bool:
    """Whether the (single) topic in this group was opened with video=True.
    Video groups always have exactly one topic, so the first suffices."""
    if not ti.topic_metadatas:
        return False
    return bool(ti.topic_metadatas[0].metadata.get(_codec.META_KEY_VIDEO, False))


def _validate_sample_queries(queries: List[SampleQuery], summary: _codec.Summary) -> None:
    known: Set[str] = {
        tm.name for ti in summary.topics_infos for tm in ti.topic_metadatas
    }
    seen: Dict[str, int] = {}
    violations: List[str] = []
    for i, q in enumerate(queries):
        if q.topic not in known:
            violations.append(f"queries[{i}]: unknown topic {q.topic!r}")
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


def lin_space_timestamps(start: int, stride: int, count: int) -> List[int]:
    """N timestamps from start, stride apart. Mirrors LinSpaceTimestamps."""
    if count <= 0:
        return []
    if stride <= 0:
        raise ValueError("lin_space_timestamps requires stride > 0")
    return [start + i * stride for i in range(count)]
