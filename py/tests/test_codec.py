"""Unit tests for turbodata._codec. Mirrors go/format/codec_test.go.

Tests the binary on-disk codec in isolation: read/write roundtrips for every
struct type and the underlying primitives (length-prefixed strings, msgpack
maps), plus length-limit and truncation error paths.
"""
from __future__ import annotations

import io
import struct

import pytest

from turbodata._codec import (
    FOOTER_LEN,
    MAGIC,
    Footer,
    IndexChunk,
    IndexChunkInfo,
    MessageIndex,
    Summary,
    TopicIndex,
    TopicMetadata,
    TopicsInfo,
    _MAX_MAP_LEN,
    _MAX_STRING_LEN,
    _read_map,
    _read_string,
    _write_map,
    _write_string,
    read_footer,
    read_index_chunk,
    read_index_chunk_info,
    read_message_index,
    read_summary,
    read_topic_index,
    read_topic_metadata,
    read_topics_info,
    write_footer,
    write_index_chunk,
    write_index_chunk_info,
    write_message_index,
    write_summary,
    write_topic_index,
    write_topic_metadata,
    write_topics_info,
)


def _u32_be(n: int) -> bytes:
    return struct.pack(">I", n)


# ---------------------------------------------------------------------------
# Strings
# ---------------------------------------------------------------------------
class TestString:
    @pytest.mark.parametrize(
        "s",
        ["", "hello world", "こんにちは世界", "x" * 1024],
        ids=["empty", "ascii", "unicode", "kbyte"],
    )
    def test_roundtrip(self, s):
        buf = io.BytesIO()
        _write_string(buf, s)
        buf.seek(0)
        assert _read_string(buf) == s

    def test_oversized_raises(self):
        oversized = io.BytesIO(_u32_be(_MAX_STRING_LEN + 1))
        with pytest.raises(ValueError, match="exceeds maximum"):
            _read_string(oversized)

    def test_truncated_length_raises(self):
        with pytest.raises(EOFError):
            _read_string(io.BytesIO(b""))

    def test_truncated_body_raises(self):
        buf = io.BytesIO(_u32_be(5) + b"\x01\x02")  # promises 5 bytes, gives 2
        with pytest.raises(EOFError):
            _read_string(buf)


# ---------------------------------------------------------------------------
# Maps (msgpack-encoded)
# ---------------------------------------------------------------------------
class TestMap:
    @pytest.mark.parametrize(
        "m",
        [
            {},
            {"key": "value"},
            {"num": 42},
            {"hello": "world", "foo": 123},
            {"hz": 100, "encoding": "jpeg", "is_compressed": True},
            {"nested": {"a": 1, "b": [1, 2, 3]}},
        ],
        ids=["empty", "string_value", "int_value", "mixed", "writer_metadata", "nested"],
    )
    def test_roundtrip(self, m):
        buf = io.BytesIO()
        _write_map(buf, m)
        buf.seek(0)
        assert _read_map(buf) == m

    def test_oversized_raises(self):
        oversized = io.BytesIO(_u32_be(_MAX_MAP_LEN + 1))
        with pytest.raises(ValueError, match="exceeds maximum"):
            _read_map(oversized)

    def test_truncated_body_raises(self):
        buf = io.BytesIO(_u32_be(5) + b"\x01\x02")
        with pytest.raises(EOFError):
            _read_map(buf)


# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------
class TestFooter:
    def test_roundtrip(self):
        f = Footer(summary_len=12345, magic=MAGIC)
        buf = io.BytesIO()
        write_footer(buf, f)
        assert buf.tell() == FOOTER_LEN
        buf.seek(0)
        got = read_footer(buf)
        assert got.summary_len == f.summary_len
        assert got.magic == f.magic

    def test_zero_summary_len_roundtrip(self):
        f = Footer(summary_len=0, magic=MAGIC)
        buf = io.BytesIO()
        write_footer(buf, f)
        buf.seek(0)
        got = read_footer(buf)
        assert got.summary_len == 0
        assert got.magic == MAGIC

    def test_invalid_magic_length_raises(self):
        f = Footer(summary_len=0, magic=b"BAD")
        with pytest.raises(ValueError, match="5 bytes"):
            write_footer(io.BytesIO(), f)

    def test_truncated_raises(self):
        with pytest.raises(EOFError):
            read_footer(io.BytesIO(b""))


# ---------------------------------------------------------------------------
# TopicMetadata
# ---------------------------------------------------------------------------
class TestTopicMetadata:
    @pytest.mark.parametrize(
        "tm",
        [
            TopicMetadata(id=1, name="/imu", metadata={}),
            TopicMetadata(id=65535, name="/cam/front", metadata={"hz": 30}),
            TopicMetadata(
                id=42,
                name="/topic_with_unicode_名前",
                metadata={"a": "b", "c": 1, "d": True},
            ),
        ],
        ids=["empty_metadata", "max_id", "unicode_name"],
    )
    def test_roundtrip(self, tm):
        buf = io.BytesIO()
        write_topic_metadata(buf, tm)
        buf.seek(0)
        got = read_topic_metadata(buf)
        assert got.id == tm.id
        assert got.name == tm.name
        assert got.metadata == tm.metadata


# ---------------------------------------------------------------------------
# IndexChunkInfo
# ---------------------------------------------------------------------------
class TestIndexChunkInfo:
    def test_roundtrip(self):
        info = IndexChunkInfo(start_timestamp=100, end_timestamp=200, offset=4096)
        buf = io.BytesIO()
        write_index_chunk_info(buf, info)
        # 3 int64 = 24 bytes
        assert buf.tell() == 24
        buf.seek(0)
        got = read_index_chunk_info(buf)
        assert got == info

    def test_negative_timestamps_roundtrip(self):
        info = IndexChunkInfo(start_timestamp=-1, end_timestamp=-1, offset=0)
        buf = io.BytesIO()
        write_index_chunk_info(buf, info)
        buf.seek(0)
        assert read_index_chunk_info(buf) == info


# ---------------------------------------------------------------------------
# TopicsInfo
# ---------------------------------------------------------------------------
class TestTopicsInfo:
    def test_roundtrip_with_chunks(self):
        ti = TopicsInfo(
            topic_metadatas=[
                TopicMetadata(id=1, name="/a", metadata={"hz": 10}),
                TopicMetadata(id=2, name="/b", metadata={}),
            ],
            index_chunk_info_list=[
                IndexChunkInfo(start_timestamp=10, end_timestamp=20, offset=100),
                IndexChunkInfo(start_timestamp=30, end_timestamp=40, offset=200),
            ],
            total_len=1024,
        )
        buf = io.BytesIO()
        write_topics_info(buf, ti)
        buf.seek(0)
        got = read_topics_info(buf)
        assert got == ti

    def test_roundtrip_empty(self):
        ti = TopicsInfo(topic_metadatas=[], index_chunk_info_list=[], total_len=0)
        buf = io.BytesIO()
        write_topics_info(buf, ti)
        buf.seek(0)
        assert read_topics_info(buf) == ti


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
class TestSummary:
    def test_roundtrip_populated(self):
        summary = Summary(
            topics_infos=[
                TopicsInfo(
                    topic_metadatas=[TopicMetadata(id=1, name="t", metadata={"k": "v"})],
                    index_chunk_info_list=[
                        IndexChunkInfo(start_timestamp=1, end_timestamp=2, offset=3)
                    ],
                    total_len=64,
                ),
                TopicsInfo(
                    topic_metadatas=[TopicMetadata(id=2, name="u", metadata={})],
                    index_chunk_info_list=[],
                    total_len=0,
                ),
            ]
        )
        buf = io.BytesIO()
        write_summary(buf, summary)
        buf.seek(0)
        assert read_summary(buf) == summary

    def test_roundtrip_empty(self):
        s = Summary(topics_infos=[])
        buf = io.BytesIO()
        write_summary(buf, s)
        buf.seek(0)
        assert read_summary(buf) == s


# ---------------------------------------------------------------------------
# MessageIndex / TopicIndex / IndexChunk
# ---------------------------------------------------------------------------
class TestMessageIndex:
    def test_roundtrip(self):
        mi = MessageIndex(timestamp=12345, offset_in_chunk=678)
        buf = io.BytesIO()
        write_message_index(buf, mi)
        # 2 int64 = 16 bytes
        assert buf.tell() == 16
        buf.seek(0)
        assert read_message_index(buf) == mi


class TestTopicIndex:
    def test_roundtrip_with_keyframes(self):
        ti = TopicIndex(
            id=5,
            message_indexes=[
                MessageIndex(timestamp=10, offset_in_chunk=0),
                MessageIndex(timestamp=20, offset_in_chunk=100),
            ],
            key_frame_indexes=[0, 1, 5],
        )
        buf = io.BytesIO()
        write_topic_index(buf, ti)
        buf.seek(0)
        got = read_topic_index(buf)
        assert got == ti

    def test_roundtrip_empty(self):
        ti = TopicIndex(id=1, message_indexes=[], key_frame_indexes=[])
        buf = io.BytesIO()
        write_topic_index(buf, ti)
        buf.seek(0)
        assert read_topic_index(buf) == ti


class TestIndexChunk:
    def test_roundtrip(self):
        ic = IndexChunk(
            topic_indexes=[
                TopicIndex(
                    id=1,
                    message_indexes=[
                        MessageIndex(timestamp=10, offset_in_chunk=0),
                        MessageIndex(timestamp=20, offset_in_chunk=50),
                    ],
                    key_frame_indexes=[],
                ),
                TopicIndex(
                    id=2,
                    message_indexes=[MessageIndex(timestamp=15, offset_in_chunk=25)],
                    key_frame_indexes=[],
                ),
            ],
            chunk_offset=4096,
            chunk_len=200,
            uncompressed_len=400,
        )
        buf = io.BytesIO()
        write_index_chunk(buf, ic)
        buf.seek(0)
        assert read_index_chunk(buf) == ic
