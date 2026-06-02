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

"""Iterator primitives shared by the default-path and cost-aware reader paths.

  sort_and_filter: per-chunk pass that sorts MessageIndexes from all
    topics by offset, computes per-message lengths from offset deltas, and
    filters by topic id + timestamp range.

  TopicsGroupIterator: default-path per-group iterator. Walks one chunk at a
    time using serial seek+read.

  PreloadedTopicsGroupIterator: cost-aware-path per-group iterator. All bytes
    are pre-fetched into LoadedBytes; this just walks forward (or backward).
"""
from __future__ import annotations

from enum import IntEnum
from typing import List, Optional, Set, Tuple

from . import _codec
from . import _compress
from ._iorange import LoadedBytes


class Order(IntEnum):
    TIME = 0
    REVERSE_TIME = 1


class _MsgIdxWithTopicId:
    __slots__ = ("topic_id", "message_index")

    def __init__(self, topic_id: int, message_index: _codec.MessageIndex) -> None:
        self.topic_id = topic_id
        self.message_index = message_index


def sort_and_filter(
    topic_indexes: List[_codec.TopicIndex],
    total_len: int,
    topic_ids: Set[int],
    start_timestamp: int,
    end_timestamp: int,
    video_decodable: bool = False,
) -> Tuple[List[_MsgIdxWithTopicId], List[int]]:
    """Compute the kept-message list for a single decoded IndexChunk.

    1) Sort MessageIndexes from all topics by offset_in_chunk.
    2) Compute per-message lengths from offset deltas (total_len closes the
       last one).
    3) Drop messages whose topic_id isn't in topic_ids or whose timestamp is
       outside [start_timestamp, end_timestamp].

    When video_decodable is True, the effective lower bound is snapped back from
    start_timestamp to the timestamp of the latest key frame whose timestamp is
    <= start_timestamp within this chunk, so the caller receives a sequence a
    decoder can consume cold. See key_frame_start.
    """
    total_messages = sum(len(ti.message_indexes) for ti in topic_indexes)
    if total_messages == 0:
        return [], []

    sorted_items: List[_MsgIdxWithTopicId]
    if len(topic_indexes) == 1:
        ti = topic_indexes[0]
        sorted_items = [_MsgIdxWithTopicId(ti.id, mi) for mi in ti.message_indexes]
    else:
        # Flatten then sort by offset. Python's Timsort is good enough; the
        # Go version uses a k-way heap merge for streaming behaviour, but the
        # output is identical and we don't need the streaming property.
        sorted_items = []
        for ti in topic_indexes:
            for mi in ti.message_indexes:
                sorted_items.append(_MsgIdxWithTopicId(ti.id, mi))
        sorted_items.sort(key=lambda it: it.message_index.offset_in_chunk)

    sorted_lens: List[int] = [0] * len(sorted_items)
    for i in range(len(sorted_items) - 1):
        sorted_lens[i] = (
            sorted_items[i + 1].message_index.offset_in_chunk
            - sorted_items[i].message_index.offset_in_chunk
        )
    sorted_lens[-1] = total_len - sorted_items[-1].message_index.offset_in_chunk

    # For video-decodable groups, snap the lower bound back to the anchoring
    # key frame so the kept sequence is decodable cold.
    effective_start = start_timestamp
    if video_decodable:
        effective_start = key_frame_start(topic_indexes, start_timestamp)

    out_msgs: List[_MsgIdxWithTopicId] = []
    out_lens: List[int] = []
    for i, item in enumerate(sorted_items):
        if item.topic_id not in topic_ids:
            continue
        ts = item.message_index.timestamp
        if ts < effective_start or ts > end_timestamp:
            continue
        out_msgs.append(item)
        out_lens.append(sorted_lens[i])
    return out_msgs, out_lens


def key_frame_start(
    topic_indexes: List[_codec.TopicIndex], start_timestamp: int
) -> int:
    """Timestamp of the latest key frame whose timestamp is <= start_timestamp,
    or start_timestamp unchanged if no such key frame exists in this chunk.

    Video-decodable groups always hold exactly one topic. key_frame_indexes are
    ascending positions into that topic's timestamp-ordered message_indexes.
    """
    if not topic_indexes:
        return start_timestamp
    ti = topic_indexes[0]
    anchor = start_timestamp
    for kf_idx in ti.key_frame_indexes:
        ts = ti.message_indexes[kf_idx].timestamp
        if ts > start_timestamp:
            break
        anchor = ts
    return anchor


def _filter_index_chunks_in_range(
    topics_info: _codec.TopicsInfo, start_ts: int, end_ts: int
) -> Tuple[List[_codec.IndexChunkInfo], List[int]]:
    """Return (filtered_infos, lengths) for chunks overlapping [start_ts, end_ts].

    Length of an index chunk = distance to next chunk's offset, or for the
    last chunk, total_len - offset + first chunk's offset (since the index
    chunks live in the trailing index region, the writer wraps).
    """
    infos: List[_codec.IndexChunkInfo] = []
    lengths: List[int] = []
    src = topics_info.index_chunk_info_list
    for i, info in enumerate(src):
        if info.end_timestamp < start_ts:
            continue
        if info.start_timestamp > end_ts:
            break
        infos.append(info)
        if i < len(src) - 1:
            lengths.append(src[i + 1].offset - info.offset)
        else:
            lengths.append(topics_info.total_len - info.offset + src[0].offset)
    return infos, lengths


class TopicsGroupIterator:
    """Default-path iterator. One chunk at a time via serial reads.

    Reads a single index chunk into an in-memory buffer, decodes it, then
    reads the matching data chunk and walks its messages. Aliases the
    decompression buffer to keep allocations bounded; callers must copy bytes
    if they need them past the next next() call.
    """

    def __init__(
        self,
        source,
        topics_info: _codec.TopicsInfo,
        topic_ids: Set[int],
        start_timestamp: int,
        end_timestamp: int,
        order: Order,
        video_decodable: bool = False,
    ) -> None:
        self._source = source
        self._topic_ids = topic_ids
        self._start = start_timestamp
        self._end = end_timestamp
        self._order = order
        self._video_decodable = video_decodable
        self._is_compressed = bool(
            topics_info.topic_metadatas[0].metadata.get(_codec.META_KEY_COMPRESSED, False)
        ) if topics_info.topic_metadatas else False

        self._infos, self._info_lens = _filter_index_chunks_in_range(
            topics_info, start_timestamp, end_timestamp
        )

        self._increment = 1
        self._current_index_chunk = 0
        if order == Order.REVERSE_TIME:
            self._increment = -1
            self._current_index_chunk = len(self._infos) - 1

        self._filtered_msgs: List[_MsgIdxWithTopicId] = []
        self._filtered_lens: List[int] = []
        self._current_msg = 0
        self._data_buf: bytes = b""

    def has_any(self) -> bool:
        return len(self._infos) > 0

    def next(self) -> Optional[Tuple[int, int, bytes]]:
        """Returns (timestamp, topic_id, data) or None on EOF."""
        while (
            self._current_msg >= len(self._filtered_msgs)
            or self._current_msg < 0
        ):
            if (
                self._current_index_chunk < 0
                or self._current_index_chunk >= len(self._infos)
            ):
                return None
            info = self._infos[self._current_index_chunk]
            length = self._info_lens[self._current_index_chunk]
            index_chunk = self._load_index_chunk(info, length)
            self._current_index_chunk += self._increment

            self._filtered_msgs, self._filtered_lens = sort_and_filter(
                index_chunk.topic_indexes,
                index_chunk.uncompressed_len,
                self._topic_ids,
                self._start,
                self._end,
                self._video_decodable,
            )
            self._current_msg = 0
            if self._order == Order.REVERSE_TIME:
                self._current_msg = len(self._filtered_msgs) - 1

            if self._filtered_msgs:
                self._load_data_chunk(
                    index_chunk.chunk_offset, index_chunk.chunk_len
                )

        item = self._filtered_msgs[self._current_msg]
        end = item.message_index.offset_in_chunk + self._filtered_lens[self._current_msg]
        data = self._data_buf[item.message_index.offset_in_chunk : end]
        self._current_msg += self._increment
        return (item.message_index.timestamp, item.topic_id, data)

    def _load_index_chunk(
        self, info: _codec.IndexChunkInfo, length: int
    ) -> _codec.IndexChunk:
        raw = self._source.read_at(info.offset, length)
        decompressed = _compress.decompress(raw)
        return _codec.read_index_chunk_from_bytes(decompressed)

    def _load_data_chunk(self, offset: int, length: int) -> None:
        raw = self._source.read_at(offset, length)
        if self._is_compressed:
            self._data_buf = _compress.decompress(raw)
        else:
            self._data_buf = raw


class PreloadedTopicsGroupIterator:
    """Cost-aware-path iterator. All bytes pre-fetched into LoadedBytes."""

    def __init__(
        self,
        is_compressed: bool,
        order: Order,
        index_chunks: List[_codec.IndexChunk],
        chunk_messages: List[List[_MsgIdxWithTopicId]],
        chunk_msg_lens: List[List[int]],
        loaded_data: LoadedBytes,
    ) -> None:
        self._is_compressed = is_compressed
        self._order = order
        self._index_chunks = index_chunks
        self._chunk_messages = chunk_messages
        self._chunk_msg_lens = chunk_msg_lens
        self._loaded = loaded_data

        self._decompressed: bytes = b""
        self._loaded_chunk_idx: int = -1

        if order == Order.REVERSE_TIME:
            self._chunk_idx = len(index_chunks) - 1
            self._msg_idx = (
                len(chunk_messages[self._chunk_idx]) - 1
                if self._chunk_idx >= 0
                else 0
            )
        else:
            self._chunk_idx = 0
            self._msg_idx = 0

    def next(self) -> Optional[Tuple[int, int, bytes]]:
        while True:
            if self._chunk_idx < 0 or self._chunk_idx >= len(self._index_chunks):
                return None
            msgs = self._chunk_messages[self._chunk_idx]
            if self._msg_idx < 0 or self._msg_idx >= len(msgs):
                self._advance_chunk()
                continue

            if self._is_compressed and self._loaded_chunk_idx != self._chunk_idx:
                compressed = self._loaded.get(
                    self._index_chunks[self._chunk_idx].chunk_offset
                )
                if compressed is None:
                    raise IOError(
                        f"missing pre-fetched chunk at offset "
                        f"{self._index_chunks[self._chunk_idx].chunk_offset}"
                    )
                self._decompressed = _compress.decompress(compressed)
                self._loaded_chunk_idx = self._chunk_idx

            msg = msgs[self._msg_idx]
            length = self._chunk_msg_lens[self._chunk_idx][self._msg_idx]
            chunk_off = self._index_chunks[self._chunk_idx].chunk_offset
            if self._is_compressed:
                start = msg.message_index.offset_in_chunk
                data = self._decompressed[start : start + length]
            else:
                data = self._loaded.get(chunk_off + msg.message_index.offset_in_chunk)
                if data is None:
                    raise IOError(
                        f"missing pre-fetched message at offset "
                        f"{chunk_off + msg.message_index.offset_in_chunk}"
                    )

            self._advance_message()
            return (msg.message_index.timestamp, msg.topic_id, data)

    def _advance_message(self) -> None:
        if self._order == Order.REVERSE_TIME:
            self._msg_idx -= 1
        else:
            self._msg_idx += 1

    def _advance_chunk(self) -> None:
        if self._order == Order.REVERSE_TIME:
            self._chunk_idx -= 1
            if self._chunk_idx >= 0:
                self._msg_idx = len(self._chunk_messages[self._chunk_idx]) - 1
        else:
            self._chunk_idx += 1
            if self._chunk_idx < len(self._index_chunks):
                self._msg_idx = 0
