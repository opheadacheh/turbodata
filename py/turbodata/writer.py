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

"""Writer: build a turbodata file by opening topic groups, writing messages,
closing each group, then closing the file. Mirrors go/writer.go.

Per-group sequence:
  open_topics([names...], [metadatas...], chunk_config=..., compression=...)
  for each message:
      write_message(topic_name, payload, timestamp)
  close_topic()
Repeat for additional groups, then close().

Keyword arguments on open_topics replace the Go SDK's WithChunkConfig and
WithCompression options. The __td_chunk_config and __td_is_compressed
entries are also stored in the per-topic metadata map (matching the Go
writer), so the reader can detect compression without out-of-band state.

Video topics (open_topics(..., video=True)) carry compressed video frames.
Callers must use write_video_message (which takes an is_key_frame flag)
instead of write_message; chunk boundaries are gated on key frames so a GOP
is never split across two chunks. Mirrors go/write_option.go WithVideoTopic
and go/writer.go WriteVideoMessage.
"""
from __future__ import annotations

from io import BytesIO
import warnings
from typing import Any, BinaryIO, Dict, List, Optional

from . import _codec, _compress
from .chunk import ChunkConfig, ChunkThresholdMode
from .errors import (
    FirstVideoMessageMustBeKeyFrameError,
    NamesMetadatasMismatchError,
    NoTopicsToOpenError,
    TimestampDecreasesError,
    TopicAlreadyClosedError,
    TopicAlreadyOpenError,
    TopicNotClosedError,
    TopicNameAlreadyOpenedError,
    TopicNotOpenedError,
    TopicNotRegisteredError,
    VideoGroupMustBeSingleTopicError,
    VideoTopicCannotBeCompressedError,
    WriteMessageOnVideoTopicError,
    WriteVideoMessageOnNonVideoTopicError,
)


_DEFAULT_CHUNK_SIZE = 1024 * 1024


class _ChunkStatus:
    __slots__ = ("start_timestamp", "size", "count")

    def __init__(self) -> None:
        self.start_timestamp: int = -1
        self.size: int = 0
        self.count: int = 0


class Writer:
    """Build a .td file.

    Use as a context manager for automatic close() on exit:

        with open(path, "wb") as f, Writer(f) as w:
            w.open_topics(...)
            ...
            w.close_topic()
    """

    def __init__(self, w: BinaryIO) -> None:
        self._w = w
        self._buf = BytesIO()

        self._footer = _codec.Footer(summary_len=0, magic=_codec.MAGIC)
        self._summary = _codec.Summary(topics_infos=[])

        # Chunk-level state.
        self._chunk_status: Optional[_ChunkStatus] = None
        self._id_to_message_indexes: Dict[int, List[_codec.MessageIndex]] = {}
        # Per-topic key frame positions (indexes into _id_to_message_indexes[id])
        # for the in-progress chunk. Reset per topic at chunk flush. Non-video
        # topics keep empty lists.
        self._id_to_key_frame_indexes: Dict[int, List[int]] = {}
        # Running total of messages written per topic over the Writer's
        # lifetime. Persisted as TopicMetadata.message_count at close.
        self._id_to_message_count: Dict[int, int] = {}

        # Topic-group-level state.
        self._last_timestamp: int = 0
        self._is_topic_open: bool = False
        self._names_to_ids: Dict[str, int] = {}
        self._topic_ids: List[int] = []
        self._chunk_config: Optional[ChunkConfig] = None
        self._is_compressed: bool = False
        self._is_video: bool = False
        self._index_chunks: List[_codec.IndexChunk] = []

        # File-level state.
        self._offset: int = 0
        self._current_topic_id: int = 0
        self._index_chunks_list: List[List[_codec.IndexChunk]] = []

        # Every topic name opened over the Writer's lifetime, so the same name
        # cannot be opened twice.
        self._opened_names: set[str] = set()

    # ---- public ---------------------------------------------------------
    def open_topics(
        self,
        names: List[str],
        metadatas: List[Dict[str, Any]],
        *,
        chunk_config: Optional[ChunkConfig] = None,
        compression: bool = False,
        video: bool = False,
    ) -> None:
        if self._is_topic_open:
            raise TopicAlreadyOpenError("topic already open")
        if len(names) != len(metadatas):
            raise NamesMetadatasMismatchError(
                f"names ({len(names)}) and metadatas ({len(metadatas)}) length mismatch"
            )
        if not names:
            raise NoTopicsToOpenError("no topics to open")

        # Each topic name must be unique for the lifetime of the Writer. Reject
        # names already opened by an earlier group as well as duplicates within
        # this call. Validate up front so a rejected call leaves the Writer
        # unchanged and reusable.
        seen: set[str] = set()
        for name in names:
            if name in self._opened_names or name in seen:
                raise TopicNameAlreadyOpenedError(
                    f"topic name already opened: {name!r}"
                )
            seen.add(name)

        # Video groups are constrained: exactly one topic, never co-compressed
        # (the codec already compresses the bytes, and chunk-level compression
        # would force a whole-chunk decompression on read).
        if video:
            if len(names) != 1:
                raise VideoGroupMustBeSingleTopicError(
                    "a video topic must be opened alone in its group"
                )
            if compression:
                raise VideoTopicCannotBeCompressedError(
                    "video topics cannot also be compressed"
                )

        for name, metadata in zip(names, metadatas):
            missing = []
            for key in (
                _codec.META_KEY_SCHEMA_NAME,
                _codec.META_KEY_SCHEMA_ENCODING,
                _codec.META_KEY_SCHEMA_DATA,
            ):
                value = metadata.get(key)
                if value is None or (
                    isinstance(value, (str, bytes, bytearray)) and len(value) == 0
                ):
                    missing.append(key)
            if missing:
                warnings.warn(
                    f"turbodata: topic {name!r}: missing or empty schema metadata "
                    f"{', '.join(missing)}; downstream decoding may be affected",
                    UserWarning,
                    stacklevel=2,
                )

        self._is_topic_open = True
        self._last_timestamp = 0

        # Inject options into the first topic's metadata so the reader can
        # detect compression. Matches Go writer behaviour.
        cc = chunk_config or ChunkConfig(
            mode=ChunkThresholdMode.SIZE, size=_DEFAULT_CHUNK_SIZE
        )

        injected = [dict(m) for m in metadatas]
        injected[0][_codec.META_KEY_CHUNK_CONFIG] = {
            "mode": int(cc.mode),
            "size": int(cc.size),
            "duration": int(cc.duration),
            "count": int(cc.count),
        }
        if compression:
            injected[0][_codec.META_KEY_COMPRESSED] = True
        if video:
            injected[0][_codec.META_KEY_VIDEO] = True

        topic_metadatas: List[_codec.TopicMetadata] = []
        self._names_to_ids = {}
        self._topic_ids = []
        for i, name in enumerate(names):
            self._current_topic_id += 1
            tid = self._current_topic_id
            topic_metadatas.append(
                _codec.TopicMetadata(id=tid, name=name, metadata=injected[i])
            )
            self._names_to_ids[name] = tid
            self._topic_ids.append(tid)
            self._id_to_message_indexes[tid] = []
            self._id_to_key_frame_indexes[tid] = []
            self._id_to_message_count[tid] = 0
            self._opened_names.add(name)

        self._summary.topics_infos.append(
            _codec.TopicsInfo(
                topic_metadatas=topic_metadatas,
                index_chunk_info_list=[],
                total_len=0,
            )
        )

        self._chunk_config = cc
        self._is_compressed = compression
        self._is_video = video
        self._chunk_status = _ChunkStatus()

    def write_message(self, topic_name: str, message: bytes, timestamp: int) -> None:
        if not self._is_topic_open:
            raise TopicNotOpenedError("topic not opened; call open_topics first")
        if self._is_video:
            raise WriteMessageOnVideoTopicError("use write_video_message for video topics")
        if timestamp < self._last_timestamp:
            raise TimestampDecreasesError(
                f"timestamp cannot decrease: {timestamp} < {self._last_timestamp}"
            )
        tid = self._names_to_ids.get(topic_name)
        if tid is None:
            raise TopicNotRegisteredError(
                f"topic {topic_name!r} not registered with open_topics"
            )

        self._last_timestamp = timestamp
        self._id_to_message_indexes[tid].append(
            _codec.MessageIndex(timestamp=timestamp, offset_in_chunk=self._buf.tell())
        )
        self._id_to_message_count[tid] += 1
        self._buf.write(message)

        assert self._chunk_status is not None
        assert self._chunk_config is not None
        cc = self._chunk_config
        if cc.mode == ChunkThresholdMode.SIZE:
            self._chunk_status.size += len(message)
            if self._chunk_status.size >= cc.size:
                self._write_chunk()
        elif cc.mode == ChunkThresholdMode.DURATION:
            if self._chunk_status.start_timestamp == -1:
                self._chunk_status.start_timestamp = timestamp
            if timestamp - self._chunk_status.start_timestamp >= cc.duration:
                self._write_chunk()
        elif cc.mode == ChunkThresholdMode.COUNT:
            self._chunk_status.count += 1
            if self._chunk_status.count >= cc.count:
                self._write_chunk()

    def write_video_message(
        self, topic_name: str, message: bytes, timestamp: int, is_key_frame: bool
    ) -> None:
        """Append one coded video frame to the (single) video topic.

        is_key_frame must reflect whether the message's bytes contain a key
        frame (IDR for h264, IRAP for h265, etc.). Turbodata does not parse the
        bitstream; the caller is responsible for setting is_key_frame correctly.

        Chunking: ChunkConfig thresholds are a lower bound. The current chunk is
        only flushed when a key frame arrives AND the threshold has been
        reached, so a GOP is never split across two chunks. The first message
        on a video topic must be a key frame.
        """
        if not self._is_topic_open:
            raise TopicNotOpenedError("topic not opened; call open_topics first")
        if not self._is_video:
            raise WriteVideoMessageOnNonVideoTopicError(
                "write_video_message requires a topic opened with video=True"
            )
        if timestamp < self._last_timestamp:
            raise TimestampDecreasesError(
                f"timestamp cannot decrease: {timestamp} < {self._last_timestamp}"
            )
        tid = self._names_to_ids.get(topic_name)
        if tid is None:
            raise TopicNotRegisteredError(
                f"topic {topic_name!r} not registered with open_topics"
            )

        # First message of any chunk must be a key frame. Because we only ever
        # flush mid-stream at key frames (the GOP-integrity rule), buf is empty
        # here only on the very first message of the topic.
        if not is_key_frame and self._buf.tell() == 0:
            raise FirstVideoMessageMustBeKeyFrameError(
                "first message of a video topic must be a key frame"
            )

        # Keyframe-gated chunk boundary: only flush at a key frame, only when
        # the threshold is met by the messages currently in the chunk (which
        # end at self._last_timestamp). This guarantees GOP integrity.
        if is_key_frame and self._buf.tell() > 0 and self._threshold_reached(self._last_timestamp):
            self._write_chunk()

        self._last_timestamp = timestamp
        self._id_to_message_indexes[tid].append(
            _codec.MessageIndex(timestamp=timestamp, offset_in_chunk=self._buf.tell())
        )
        self._id_to_message_count[tid] += 1
        if is_key_frame:
            self._id_to_key_frame_indexes[tid].append(
                len(self._id_to_message_indexes[tid]) - 1
            )
        self._buf.write(message)

        # Maintain chunk-status counters for symmetry with the non-video path;
        # the actual flush is gated on key frames by _threshold_reached above.
        assert self._chunk_status is not None
        assert self._chunk_config is not None
        cc = self._chunk_config
        if cc.mode == ChunkThresholdMode.SIZE:
            self._chunk_status.size += len(message)
        elif cc.mode == ChunkThresholdMode.DURATION:
            if self._chunk_status.start_timestamp == -1:
                self._chunk_status.start_timestamp = timestamp
        elif cc.mode == ChunkThresholdMode.COUNT:
            self._chunk_status.count += 1

    def _threshold_reached(self, last_chunk_timestamp: int) -> bool:
        """Whether the in-progress chunk has reached the configured threshold.

        last_chunk_timestamp is the timestamp of the latest message currently
        buffered in the chunk; used only by DURATION mode. Factored out so
        write_video_message can defer the flush to a key frame boundary while
        reusing the same threshold semantics as write_message.
        """
        assert self._chunk_status is not None
        assert self._chunk_config is not None
        cc = self._chunk_config
        if cc.mode == ChunkThresholdMode.SIZE:
            return self._chunk_status.size >= cc.size
        if cc.mode == ChunkThresholdMode.DURATION:
            if self._chunk_status.start_timestamp == -1:
                return False
            return last_chunk_timestamp - self._chunk_status.start_timestamp >= cc.duration
        if cc.mode == ChunkThresholdMode.COUNT:
            return self._chunk_status.count >= cc.count
        return False

    def close_topic(self) -> None:
        if not self._is_topic_open:
            raise TopicAlreadyClosedError("topic is already closed")
        self._is_topic_open = False

        if self._buf.tell() > 0:
            self._write_chunk()

        self._index_chunks_list.append(self._index_chunks)
        self._index_chunks = []

    def close(self) -> None:
        if self._is_topic_open:
            raise TopicNotClosedError("topic not closed; call close_topic first")
        self._write_index_chunks()
        # Stamp the per-topic message counts onto the summary now that no
        # further writes can happen.
        for ti in self._summary.topics_infos:
            for tm in ti.topic_metadatas:
                tm.message_count = self._id_to_message_count[tm.id]
        self._write_summary()
        self._write_footer()
        # No flush call: the caller owns the underlying file/stream.

    def __enter__(self) -> "Writer":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is None:
            self.close()

    # ---- internals ------------------------------------------------------
    def _write_chunk(self) -> None:
        uncompressed_len = self._buf.tell()
        raw = self._buf.getvalue()
        if self._is_compressed:
            payload = _compress.compress(raw)
        else:
            payload = raw

        topic_indexes: List[_codec.TopicIndex] = []
        for tid in self._topic_ids:
            mis = self._id_to_message_indexes[tid]
            if not mis:
                continue
            # Take ownership of the per-topic key-frame slice for this chunk and
            # reset it for the next one. Non-video topics keep an empty list.
            kfs = self._id_to_key_frame_indexes.get(tid, [])
            topic_indexes.append(
                _codec.TopicIndex(id=tid, message_indexes=mis, key_frame_indexes=kfs)
            )
            self._id_to_message_indexes[tid] = []
            self._id_to_key_frame_indexes[tid] = []

        self._index_chunks.append(
            _codec.IndexChunk(
                topic_indexes=topic_indexes,
                chunk_offset=self._offset,
                chunk_len=len(payload),
                uncompressed_len=uncompressed_len,
            )
        )

        n = self._w.write(payload)
        if n is not None and n != len(payload):
            raise IOError(f"short write: wrote {n}, expected {len(payload)}")
        self._offset += len(payload)

        assert self._chunk_status is not None
        self._chunk_status.start_timestamp = -1
        self._chunk_status.size = 0
        self._chunk_status.count = 0
        self._buf = BytesIO()

    def _write_index_chunks(self) -> None:
        for i, index_chunks in enumerate(self._index_chunks_list):
            total_len = 0
            for ic in index_chunks:
                staging = BytesIO()
                _codec.write_index_chunk(staging, ic)
                compressed = _compress.compress(staging.getvalue())

                start_ts = (1 << 63) - 1
                end_ts = -(1 << 63)
                for ti in ic.topic_indexes:
                    if ti.message_indexes[0].timestamp < start_ts:
                        start_ts = ti.message_indexes[0].timestamp
                    if ti.message_indexes[-1].timestamp > end_ts:
                        end_ts = ti.message_indexes[-1].timestamp

                self._summary.topics_infos[i].index_chunk_info_list.append(
                    _codec.IndexChunkInfo(
                        start_timestamp=start_ts,
                        end_timestamp=end_ts,
                        offset=self._offset,
                    )
                )

                self._w.write(compressed)
                self._offset += len(compressed)
                total_len += len(compressed)
            self._summary.topics_infos[i].total_len = total_len

    def _write_summary(self) -> None:
        staging = BytesIO()
        _codec.write_summary(staging, self._summary)
        compressed = _compress.compress(staging.getvalue())
        self._w.write(compressed)
        self._offset += len(compressed)
        self._footer.summary_len = len(compressed)

    def _write_footer(self) -> None:
        _codec.write_footer(self._w, self._footer)
