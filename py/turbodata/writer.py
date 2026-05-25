"""Writer: build a turbodata file by opening topic groups, writing messages,
closing each group, then closing the file. Mirrors go/writer.go.

Per-group sequence:
  open_topics([names...], [metadatas...], chunk_config=..., compression=...)
  for each message:
      write_message(topic_name, payload, timestamp)
  close_topic()
Repeat for additional groups, then close().

Keyword arguments on open_topics replace the Go SDK's WithChunkConfig and
WithCompression options. The chunk_config and is_compressed entries are
also stored in the per-topic metadata map (matching the Go writer), so the
reader can detect compression without out-of-band state.
"""
from __future__ import annotations

from io import BytesIO
from typing import Any, BinaryIO, Dict, List, Optional

from . import _codec, _compress
from .chunk import ChunkConfig, ChunkThresholdMode
from .errors import (
    NamesMetadatasMismatchError,
    NoTopicsToOpenError,
    TimestampDecreasesError,
    TopicAlreadyClosedError,
    TopicAlreadyOpenError,
    TopicNotClosedError,
    TopicNotOpenedError,
    TopicNotRegisteredError,
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

        # Topic-group-level state.
        self._last_timestamp: int = 0
        self._is_topic_open: bool = False
        self._names_to_ids: Dict[str, int] = {}
        self._topic_ids: List[int] = []
        self._chunk_config: Optional[ChunkConfig] = None
        self._is_compressed: bool = False
        self._index_chunks: List[_codec.IndexChunk] = []

        # File-level state.
        self._offset: int = 0
        self._current_topic_id: int = 0
        self._index_chunks_list: List[List[_codec.IndexChunk]] = []

    # ---- public ---------------------------------------------------------
    def open_topics(
        self,
        names: List[str],
        metadatas: List[Dict[str, Any]],
        *,
        chunk_config: Optional[ChunkConfig] = None,
        compression: bool = False,
    ) -> None:
        if self._is_topic_open:
            raise TopicAlreadyOpenError("topic already open")
        if len(names) != len(metadatas):
            raise NamesMetadatasMismatchError(
                f"names ({len(names)}) and metadatas ({len(metadatas)}) length mismatch"
            )
        if not names:
            raise NoTopicsToOpenError("no topics to open")

        self._is_topic_open = True
        self._last_timestamp = 0

        # Inject options into the first topic's metadata so the reader can
        # detect compression. Matches Go writer behaviour.
        cc = chunk_config or ChunkConfig(
            mode=ChunkThresholdMode.SIZE, size=_DEFAULT_CHUNK_SIZE
        )

        injected = [dict(m) for m in metadatas]
        injected[0]["chunk_config"] = {
            "mode": int(cc.mode),
            "size": int(cc.size),
            "duration": int(cc.duration),
            "count": int(cc.count),
        }
        if compression:
            injected[0]["is_compressed"] = True

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

        self._summary.topics_infos.append(
            _codec.TopicsInfo(
                topic_metadatas=topic_metadatas,
                index_chunk_info_list=[],
                total_len=0,
            )
        )

        self._chunk_config = cc
        self._is_compressed = compression
        self._chunk_status = _ChunkStatus()

    def write_message(self, topic_name: str, message: bytes, timestamp: int) -> None:
        if not self._is_topic_open:
            raise TopicNotOpenedError("topic not opened; call open_topics first")
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
            topic_indexes.append(
                _codec.TopicIndex(id=tid, message_indexes=mis, key_frame_indexes=[])
            )
            self._id_to_message_indexes[tid] = []

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
