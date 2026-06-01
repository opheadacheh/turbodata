"""Binary codec for the turbodata on-disk format.

Mirrors go/format/codec.go and go/format/types.go. All multi-byte integers are
big-endian. Strings and msgpack-encoded maps are length-prefixed by a 32-bit
big-endian unsigned length.
"""
from __future__ import annotations

import struct
from dataclasses import dataclass, field
from io import BytesIO
from typing import Any, BinaryIO, Dict, List

import msgpack


MAGIC: bytes = b"7URB0"
FOOTER_LEN: int = 13  # int64 SummaryLen + 5-byte Magic

# Internal per-topic metadata keys used to carry write-format control flags
# alongside caller-supplied metadata. The __td_ prefix namespaces them away
# from user keys. META_KEY_COMPRESSED and META_KEY_VIDEO are read back by the
# reader to drive decompression and video-aware sampling.
META_KEY_COMPRESSED: str = "__td_is_compressed"
META_KEY_VIDEO: str = "__td_is_video"
META_KEY_CHUNK_CONFIG: str = "__td_chunk_config"

_MAX_STRING_LEN: int = 64 * 1024 * 1024
_MAX_MAP_LEN: int = 64 * 1024 * 1024


# ---------------------------------------------------------------------------
# On-disk structures (1:1 with Go format.* types).
# ---------------------------------------------------------------------------
@dataclass
class Footer:
    summary_len: int = 0
    magic: bytes = MAGIC


@dataclass
class TopicMetadata:
    id: int
    name: str
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class IndexChunkInfo:
    start_timestamp: int
    end_timestamp: int
    offset: int


@dataclass
class TopicsInfo:
    topic_metadatas: List[TopicMetadata] = field(default_factory=list)
    index_chunk_info_list: List[IndexChunkInfo] = field(default_factory=list)
    total_len: int = 0


@dataclass
class Summary:
    topics_infos: List[TopicsInfo] = field(default_factory=list)


@dataclass
class MessageIndex:
    timestamp: int
    offset_in_chunk: int


@dataclass
class TopicIndex:
    id: int
    message_indexes: List[MessageIndex] = field(default_factory=list)
    key_frame_indexes: List[int] = field(default_factory=list)


@dataclass
class IndexChunk:
    topic_indexes: List[TopicIndex] = field(default_factory=list)
    chunk_offset: int = 0
    chunk_len: int = 0
    uncompressed_len: int = 0


# ---------------------------------------------------------------------------
# Primitive readers/writers.
# ---------------------------------------------------------------------------
def _read_exact(r: BinaryIO, n: int) -> bytes:
    buf = r.read(n)
    if len(buf) != n:
        raise EOFError(f"short read: wanted {n} got {len(buf)}")
    return buf


def _read_u16(r: BinaryIO) -> int:
    return struct.unpack(">H", _read_exact(r, 2))[0]


def _read_u32(r: BinaryIO) -> int:
    return struct.unpack(">I", _read_exact(r, 4))[0]


def _read_i64(r: BinaryIO) -> int:
    return struct.unpack(">q", _read_exact(r, 8))[0]


def _write_u16(w: BinaryIO, v: int) -> None:
    w.write(struct.pack(">H", v))


def _write_u32(w: BinaryIO, v: int) -> None:
    w.write(struct.pack(">I", v))


def _write_i64(w: BinaryIO, v: int) -> None:
    w.write(struct.pack(">q", v))


def _read_string(r: BinaryIO) -> str:
    strlen = _read_u32(r)
    if strlen > _MAX_STRING_LEN:
        raise ValueError(f"string length {strlen} exceeds maximum {_MAX_STRING_LEN}")
    return _read_exact(r, strlen).decode("utf-8")


def _write_string(w: BinaryIO, s: str) -> None:
    b = s.encode("utf-8")
    _write_u32(w, len(b))
    w.write(b)


def _read_map(r: BinaryIO) -> Dict[str, Any]:
    maplen = _read_u32(r)
    if maplen > _MAX_MAP_LEN:
        raise ValueError(f"map length {maplen} exceeds maximum {_MAX_MAP_LEN}")
    raw = _read_exact(r, maplen)
    m = msgpack.unpackb(raw, raw=False, strict_map_key=False)
    if not isinstance(m, dict):
        raise ValueError("topic metadata must decode to a map")
    return m


def _write_map(w: BinaryIO, m: Dict[str, Any]) -> None:
    raw = msgpack.packb(m, use_bin_type=True)
    _write_u32(w, len(raw))
    w.write(raw)


# ---------------------------------------------------------------------------
# Footer.
# ---------------------------------------------------------------------------
def read_footer(r: BinaryIO) -> Footer:
    summary_len = _read_i64(r)
    magic = _read_exact(r, 5)
    return Footer(summary_len=summary_len, magic=magic)


def write_footer(w: BinaryIO, footer: Footer) -> None:
    _write_i64(w, footer.summary_len)
    if len(footer.magic) != 5:
        raise ValueError("footer magic must be 5 bytes")
    w.write(footer.magic)


# ---------------------------------------------------------------------------
# TopicMetadata.
# ---------------------------------------------------------------------------
def read_topic_metadata(r: BinaryIO) -> TopicMetadata:
    tid = _read_u16(r)
    name = _read_string(r)
    metadata = _read_map(r)
    return TopicMetadata(id=tid, name=name, metadata=metadata)


def write_topic_metadata(w: BinaryIO, tm: TopicMetadata) -> None:
    _write_u16(w, tm.id)
    _write_string(w, tm.name)
    _write_map(w, tm.metadata)


# ---------------------------------------------------------------------------
# IndexChunkInfo.
# ---------------------------------------------------------------------------
def read_index_chunk_info(r: BinaryIO) -> IndexChunkInfo:
    start_ts = _read_i64(r)
    end_ts = _read_i64(r)
    offset = _read_i64(r)
    return IndexChunkInfo(start_timestamp=start_ts, end_timestamp=end_ts, offset=offset)


def write_index_chunk_info(w: BinaryIO, info: IndexChunkInfo) -> None:
    _write_i64(w, info.start_timestamp)
    _write_i64(w, info.end_timestamp)
    _write_i64(w, info.offset)


# ---------------------------------------------------------------------------
# TopicsInfo.
# ---------------------------------------------------------------------------
def read_topics_info(r: BinaryIO) -> TopicsInfo:
    topic_metadata_len = _read_u32(r)
    topic_metadatas = [read_topic_metadata(r) for _ in range(topic_metadata_len)]

    index_chunk_info_len = _read_u32(r)
    index_chunk_info_list = [read_index_chunk_info(r) for _ in range(index_chunk_info_len)]

    total_len = _read_i64(r)
    return TopicsInfo(
        topic_metadatas=topic_metadatas,
        index_chunk_info_list=index_chunk_info_list,
        total_len=total_len,
    )


def write_topics_info(w: BinaryIO, ti: TopicsInfo) -> None:
    _write_u32(w, len(ti.topic_metadatas))
    for tm in ti.topic_metadatas:
        write_topic_metadata(w, tm)
    _write_u32(w, len(ti.index_chunk_info_list))
    for info in ti.index_chunk_info_list:
        write_index_chunk_info(w, info)
    _write_i64(w, ti.total_len)


# ---------------------------------------------------------------------------
# Summary.
# ---------------------------------------------------------------------------
def read_summary(r: BinaryIO) -> Summary:
    topics_info_len = _read_u32(r)
    topics_infos = [read_topics_info(r) for _ in range(topics_info_len)]
    return Summary(topics_infos=topics_infos)


def write_summary(w: BinaryIO, summary: Summary) -> None:
    _write_u32(w, len(summary.topics_infos))
    for ti in summary.topics_infos:
        write_topics_info(w, ti)


# ---------------------------------------------------------------------------
# MessageIndex.
# ---------------------------------------------------------------------------
def read_message_index(r: BinaryIO) -> MessageIndex:
    ts = _read_i64(r)
    off = _read_i64(r)
    return MessageIndex(timestamp=ts, offset_in_chunk=off)


def write_message_index(w: BinaryIO, mi: MessageIndex) -> None:
    _write_i64(w, mi.timestamp)
    _write_i64(w, mi.offset_in_chunk)


# ---------------------------------------------------------------------------
# TopicIndex.
# ---------------------------------------------------------------------------
def read_topic_index(r: BinaryIO) -> TopicIndex:
    tid = _read_u16(r)
    mi_len = _read_u32(r)
    message_indexes = [read_message_index(r) for _ in range(mi_len)]
    kf_len = _read_u32(r)
    key_frame_indexes = [_read_u32(r) for _ in range(kf_len)]
    return TopicIndex(id=tid, message_indexes=message_indexes, key_frame_indexes=key_frame_indexes)


def write_topic_index(w: BinaryIO, ti: TopicIndex) -> None:
    _write_u16(w, ti.id)
    _write_u32(w, len(ti.message_indexes))
    for mi in ti.message_indexes:
        write_message_index(w, mi)
    _write_u32(w, len(ti.key_frame_indexes))
    for kf in ti.key_frame_indexes:
        _write_u32(w, kf)


# ---------------------------------------------------------------------------
# IndexChunk.
# ---------------------------------------------------------------------------
def read_index_chunk(r: BinaryIO) -> IndexChunk:
    ti_len = _read_u32(r)
    topic_indexes = [read_topic_index(r) for _ in range(ti_len)]
    chunk_offset = _read_i64(r)
    chunk_len = _read_i64(r)
    uncompressed_len = _read_i64(r)
    return IndexChunk(
        topic_indexes=topic_indexes,
        chunk_offset=chunk_offset,
        chunk_len=chunk_len,
        uncompressed_len=uncompressed_len,
    )


def write_index_chunk(w: BinaryIO, ic: IndexChunk) -> None:
    _write_u32(w, len(ic.topic_indexes))
    for ti in ic.topic_indexes:
        write_topic_index(w, ti)
    _write_i64(w, ic.chunk_offset)
    _write_i64(w, ic.chunk_len)
    _write_i64(w, ic.uncompressed_len)


# ---------------------------------------------------------------------------
# BytesIO convenience for decoding from a buffer.
# ---------------------------------------------------------------------------
def read_index_chunk_from_bytes(data: bytes) -> IndexChunk:
    return read_index_chunk(BytesIO(data))


def read_summary_from_bytes(data: bytes) -> Summary:
    return read_summary(BytesIO(data))


def read_footer_from_bytes(data: bytes) -> Footer:
    return read_footer(BytesIO(data))
