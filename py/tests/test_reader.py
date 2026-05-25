"""Unit tests for turbodata.reader. Mirrors go/reader_test.go.

Covers Reader construction, summary loading (including the tail-prefetch hint
that decides whether the summary load costs 1 or 2 ReadAt calls), error paths
for malformed files, and summary result caching.
"""
from __future__ import annotations

import io
import struct

import pytest

from turbodata import (
    BytesReadSource,
    FileTooSmallError,
    InvalidMagicError,
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

from .conftest import TrackingSource


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

    def test_hint_covers_footer_and_summary_issues_one_read_at(self):
        # Sufficient hint: one ReadAt covers footer + compressed summary.
        data, compressed = self._setup()
        tracking = TrackingSource(BytesReadSource(data))
        reader = Reader(tracking)
        hint = len(compressed) + FOOTER_LEN + 8  # covers footer + summary + slack
        reader.read_messages(tail_prefetch=hint)
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
