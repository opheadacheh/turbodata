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

"""Unit tests for turbodata.reader. Mirrors go/reader_test.go.

Covers Reader construction, summary loading (including the tail-prefetch hint
that decides whether the summary load costs 1 or 2 ReadAt calls), error paths
for malformed files, and summary result caching.
"""
from __future__ import annotations

import io
import struct
from pathlib import Path

import pytest

from turbodata import (
    BytesReadSource,
    FileTooSmallError,
    IndexEntry,
    InvalidMagicError,
    Order,
    ReadStrategy,
    Reader,
)
from turbodata._codec import (
    FOOTER_LEN,
    MAGIC,
    Footer,
    Summary,
    TopicMetadata,
    TopicsInfo,
    write_footer,
    write_summary,
)
from turbodata._compress import compress

from .conftest import TS_FIXTURES, TrackingSource, build_file


def _make_reader_fixture(
    body: bytes, compressed_summary: bytes, footer: Footer = None
) -> bytes:
    if footer is None:
        footer = Footer(summary_len=len(compressed_summary), magic=MAGIC)
    buf = io.BytesIO()
    buf.write(body)
    buf.write(compressed_summary)
    write_footer(buf, footer)
    return buf.getvalue()


def _compress_summary(summary: Summary) -> bytes:
    sb = io.BytesIO()
    write_summary(sb, summary)
    return compress(sb.getvalue())


# ---------------------------------------------------------------------------
# Construction / magic / truncation
# ---------------------------------------------------------------------------
class TestNewReader:
    def test_valid_footer_and_magic(self):
        summary = Summary(topics_infos=[])
        compressed = _compress_summary(summary)
        data = _make_reader_fixture(b"", compressed)
        reader = Reader(BytesReadSource(data))
        s = reader.summary()
        assert s.topics_infos == []

    def test_invalid_magic_raises(self):
        summary = Summary(topics_infos=[])
        compressed = _compress_summary(summary)
        bad_footer = Footer(summary_len=len(compressed), magic=b"BAD!!")
        data = _make_reader_fixture(b"", compressed, bad_footer)
        reader = Reader(BytesReadSource(data))
        with pytest.raises(InvalidMagicError):
            reader.summary()

    def test_file_too_small_raises(self):
        reader = Reader(BytesReadSource(b"\x01\x02"))
        with pytest.raises(FileTooSmallError):
            reader.summary()


# ---------------------------------------------------------------------------
# Summary content + caching
# ---------------------------------------------------------------------------
class TestReaderSummary:
    @pytest.mark.parametrize(
        "summary",
        [
            Summary(
                topics_infos=[
                    TopicsInfo(
                        topic_metadatas=[
                            TopicMetadata(
                                id=1, name="topic/1", metadata={"encoding": "json"}
                            )
                        ],
                        index_chunk_info_list=[],
                        total_len=512,
                    )
                ]
            ),
            Summary(topics_infos=[]),
        ],
        ids=["populated", "empty"],
    )
    def test_roundtrip(self, summary):
        compressed = _compress_summary(summary)
        data = _make_reader_fixture(b"body-bytes", compressed)
        reader = Reader(BytesReadSource(data))
        got = reader.summary()
        assert got == summary

    def test_cached_result(self):
        summary = Summary(topics_infos=[])
        compressed = _compress_summary(summary)
        data = _make_reader_fixture(b"body", compressed)
        reader = Reader(BytesReadSource(data))
        first = reader.summary()

        # Replace the source with one that would explode if accessed; cached
        # summary must still be returned.
        class _ExplodingSource:
            def size(self):
                raise AssertionError("should not be called after caching")

            def read_at(self, off, n):
                raise AssertionError("should not be called after caching")

        reader._source = _ExplodingSource()
        second = reader.summary()
        assert first is second  # exact object identity

    def test_invalid_compressed_summary_raises(self):
        data = _make_reader_fixture(b"", b"not-zstd-data")
        reader = Reader(BytesReadSource(data))
        with pytest.raises(Exception):
            reader.summary()

    def test_invalid_summary_payload_raises(self):
        # Decompresses to 2 bytes -> too short for a Summary (needs at least
        # a 4-byte topics_info_len).
        compressed = compress(b"\x01\x02")
        data = _make_reader_fixture(b"", compressed)
        reader = Reader(BytesReadSource(data))
        with pytest.raises(EOFError):
            reader.summary()


# ---------------------------------------------------------------------------
# Tail prefetch hint
# ---------------------------------------------------------------------------
class TestSummaryWithHint:
    def _setup(self):
        summary = Summary(
            topics_infos=[
                TopicsInfo(
                    topic_metadatas=[TopicMetadata(id=1, name="t", metadata={})],
                    index_chunk_info_list=[],
                    total_len=8,
                )
            ]
        )
        compressed = _compress_summary(summary)
        body = b"\x00" * 4096
        data = _make_reader_fixture(body, compressed)
        return data, compressed

    def test_hint_zero_issues_two_read_ats(self):
        # No hint: one ReadAt for the footer, one for the compressed summary.
        data, _compressed = self._setup()
        tracking = TrackingSource(BytesReadSource(data))
        reader = Reader(tracking)
        reader.summary()
        assert tracking.read_at_calls == 2

    @pytest.mark.parametrize("method", ["read_messages", "read_index"])
    def test_hint_covers_footer_and_summary_issues_one_read_at(self, method):
        # Sufficient hint: one ReadAt covers footer + compressed summary.
        data, compressed = self._setup()
        tracking = TrackingSource(BytesReadSource(data))
        reader = Reader(tracking)
        hint = len(compressed) + FOOTER_LEN + 8  # covers footer + summary + slack
        list(getattr(reader, method)(tail_prefetch=hint))
        assert tracking.read_at_calls == 1

    def test_hint_too_small_falls_back_to_two_read_ats(self):
        # Hint covers footer but not summary: one ReadAt for the tail, one for
        # the summary.
        data, _compressed = self._setup()
        tracking = TrackingSource(BytesReadSource(data))
        reader = Reader(tracking)
        reader.read_messages(tail_prefetch=FOOTER_LEN)
        # Implementations may issue 2 (footer+summary) or 1 (if footer-only hint
        # already happens to cover summary). For our writer this fits in 2.
        assert tracking.read_at_calls == 2

    def test_hint_larger_than_file_clamps_to_file_size(self):
        """A hint larger than the file must not raise; it gets clamped."""
        data, _compressed = self._setup()
        tracking = TrackingSource(BytesReadSource(data))
        reader = Reader(tracking)
        reader.read_messages(tail_prefetch=len(data) * 100)
        assert tracking.read_at_calls == 1

@pytest.mark.parametrize("strategy", [None, ReadStrategy(1 << 20, 64, 3)])
@pytest.mark.parametrize("order", [Order.TIME, Order.REVERSE_TIME])
@pytest.mark.parametrize("video_decodable", [False, True])
@pytest.mark.parametrize("file, topic, start, end", [
    ("multi-group-mixed.td", "/groupB/y", 4000000, 4004700),
    ("video-multi-gop-chunks.td", "/cam", 20, 60),
])
def test_read_index_without_payload(file, topic, start, end, order, strategy, video_decodable):
    data = (Path(TS_FIXTURES) / file).read_bytes()
    reference = Reader(BytesReadSource(data), topic_remap={topic: "renamed"})
    index_start = reference.summary().topics_infos[0].index_chunk_info_list[0].offset

    class IndexRegionSource(BytesReadSource):
        def read_at(self, offset, n):
            assert offset >= index_start, f"payload read at {offset}"
            return super().read_at(offset, n)

    for names in (None, ["renamed"], ["missing"]):
        reader = Reader(IndexRegionSource(data), topic_remap={topic: "renamed"})
        query = dict(topic_names=names, start_timestamp=start, end_timestamp=end,
                     order=order, video_decodable=video_decodable)
        expected = [IndexEntry(m.timestamp, m.topic_name, m.is_key_frame)
                    for m in reference.read_messages(**query)]
        assert list(reader.read_index(**query, strategy=strategy, copy=True)) == expected


def test_read_index_preserves_duplicate_timestamps_at_zero():
    def write(w):
        w.open_topics(["imu"], [{}])
        for ts in (0, 0, 1):
            w.write_message("imu", b"data", timestamp=ts)
        w.close_topic()

    reader = Reader(BytesReadSource(build_file(write)))
    assert list(reader.read_index(end_timestamp=0)) == [IndexEntry(0, "imu", False)] * 2


def test_read_index_strategy_prefetches_candidate_chunks():
    data = (Path(TS_FIXTURES) / "video-multi-gop-chunks.td").read_bytes()
    source = TrackingSource(BytesReadSource(data))
    reader = Reader(source)
    group = reader.summary().topics_infos[0]
    assert len(group.index_chunk_info_list) > 1

    before = source.read_at_bytes
    assert next(reader.read_index()).timestamp == 10
    assert source.read_at_bytes - before == (
        group.index_chunk_info_list[1].offset - group.index_chunk_info_list[0].offset
    )

    before = source.read_at_bytes
    calls = source.read_at_calls
    strategy = ReadStrategy(coalesce_gap=1, max_concurrency=2)
    entries = list(reader.read_index(strategy=strategy))
    assert len(entries) == 6
    assert source.read_at_calls - calls == 1
    assert source.read_at_bytes - before == group.total_len
