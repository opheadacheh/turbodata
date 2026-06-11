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

"""Unit tests for turbodata.writer. Mirrors go/writer_test.go.

Covers:
  - State-machine errors (open/close ordering, duplicate opens, etc.)
  - Per-message validation (timestamp monotonicity, unknown topic)
  - Chunk threshold modes (size, count, duration)
  - Compression option
  - Multi-group files
  - Resulting file structure (footer magic, summary roundtrip, chunk offsets)
"""
from __future__ import annotations

import io
import struct

import pytest

from turbodata import (
    BytesReadSource,
    ChunkConfig,
    ChunkThresholdMode,
    NamesMetadatasMismatchError,
    NoTopicsToOpenError,
    Reader,
    TimestampDecreasesError,
    TopicAlreadyClosedError,
    TopicAlreadyOpenError,
    TopicNameAlreadyOpenedError,
    TopicNotClosedError,
    TopicNotOpenedError,
    TopicNotRegisteredError,
    Writer,
)
from turbodata._codec import FOOTER_LEN, MAGIC


# ---------------------------------------------------------------------------
# Constructor
# ---------------------------------------------------------------------------
class TestWriterConstruction:
    def test_returns_writer(self):
        assert Writer(io.BytesIO()) is not None

    def test_context_manager_calls_close(self):
        buf = io.BytesIO()
        with Writer(buf) as w:
            w.open_topics(["t"], [{}])
            w.write_message("t", b"x", 1)
            w.close_topic()
        # After __exit__, footer + magic must be present.
        assert buf.getvalue()[-5:] == MAGIC


# ---------------------------------------------------------------------------
# open_topics: state machine + validation
# ---------------------------------------------------------------------------
class TestOpenTopics:
    def test_error_when_already_open(self):
        w = Writer(io.BytesIO())
        w.open_topics(["a"], [{}])
        with pytest.raises(TopicAlreadyOpenError):
            w.open_topics(["b"], [{}])

    def test_error_when_name_reopened_after_close(self):
        w = Writer(io.BytesIO())
        w.open_topics(["a"], [{}])
        w.close_topic()
        with pytest.raises(TopicNameAlreadyOpenedError):
            w.open_topics(["a"], [{}])

    def test_error_when_name_duplicated_within_call(self):
        w = Writer(io.BytesIO())
        with pytest.raises(TopicNameAlreadyOpenedError):
            w.open_topics(["a", "a"], [{}, {}])

    def test_reusable_after_duplicate_rejected(self):
        w = Writer(io.BytesIO())
        with pytest.raises(TopicNameAlreadyOpenedError):
            w.open_topics(["a", "a"], [{}, {}])
        # A rejected call must leave the Writer unchanged and reusable.
        w.open_topics(["a", "b"], [{}, {}])

    def test_error_names_metadatas_length_mismatch(self):
        w = Writer(io.BytesIO())
        with pytest.raises(NamesMetadatasMismatchError):
            w.open_topics(["a", "b"], [{}])

    def test_error_no_topics(self):
        w = Writer(io.BytesIO())
        with pytest.raises(NoTopicsToOpenError):
            w.open_topics([], [])

    def test_assigns_sequential_ids_within_a_group(self):
        buf = io.BytesIO()
        w = Writer(buf)
        w.open_topics(["a", "b", "c"], [{}, {}, {}])
        w.write_message("a", b"1", 1)
        w.write_message("b", b"2", 2)
        w.write_message("c", b"3", 3)
        w.close_topic()
        w.close()
        summary = Reader(BytesReadSource(buf.getvalue())).summary()
        ids = [tm.id for tm in summary.topics_infos[0].topic_metadatas]
        assert ids == [1, 2, 3]

    def test_ids_persist_across_groups(self):
        buf = io.BytesIO()
        w = Writer(buf)
        w.open_topics(["a"], [{}])
        w.write_message("a", b"1", 1)
        w.close_topic()
        w.open_topics(["b", "c"], [{}, {}])
        w.write_message("b", b"2", 2)
        w.write_message("c", b"3", 3)
        w.close_topic()
        w.close()
        summary = Reader(BytesReadSource(buf.getvalue())).summary()
        # group 0 -> id 1, group 1 -> ids 2, 3
        assert [tm.id for tm in summary.topics_infos[0].topic_metadatas] == [1]
        assert [tm.id for tm in summary.topics_infos[1].topic_metadatas] == [2, 3]


# ---------------------------------------------------------------------------
# write_message: state + validation
# ---------------------------------------------------------------------------
class TestWriteMessage:
    def test_error_when_topic_not_opened(self):
        w = Writer(io.BytesIO())
        with pytest.raises(TopicNotOpenedError):
            w.write_message("a", b"x", 1)

    def test_error_when_topic_not_registered(self):
        w = Writer(io.BytesIO())
        w.open_topics(["a"], [{}])
        with pytest.raises(TopicNotRegisteredError):
            w.write_message("nope", b"x", 1)

    def test_error_when_timestamp_decreases(self):
        w = Writer(io.BytesIO())
        w.open_topics(["a"], [{}])
        w.write_message("a", b"x", 10)
        with pytest.raises(TimestampDecreasesError):
            w.write_message("a", b"y", 5)

    def test_equal_timestamp_allowed(self):
        """Non-decreasing means >=, not strictly >."""
        w = Writer(io.BytesIO())
        w.open_topics(["a"], [{}])
        w.write_message("a", b"x", 10)
        w.write_message("a", b"y", 10)  # equal is fine
        w.close_topic()
        w.close()


# ---------------------------------------------------------------------------
# close_topic / close: state machine
# ---------------------------------------------------------------------------
class TestClose:
    def test_close_topic_error_when_not_open(self):
        w = Writer(io.BytesIO())
        with pytest.raises(TopicAlreadyClosedError):
            w.close_topic()

    def test_close_error_when_topic_still_open(self):
        w = Writer(io.BytesIO())
        w.open_topics(["a"], [{}])
        with pytest.raises(TopicNotClosedError):
            w.close()

    def test_close_writes_footer_with_magic(self):
        buf = io.BytesIO()
        w = Writer(buf)
        w.open_topics(["a"], [{}])
        w.write_message("a", b"x", 1)
        w.close_topic()
        w.close()
        out = buf.getvalue()
        assert len(out) >= FOOTER_LEN
        assert out[-5:] == MAGIC

    def test_close_records_summary_len_in_footer(self):
        buf = io.BytesIO()
        w = Writer(buf)
        w.open_topics(["a"], [{}])
        w.write_message("a", b"x", 1)
        w.close_topic()
        w.close()
        out = buf.getvalue()
        summary_len = struct.unpack(">q", out[-FOOTER_LEN : -5])[0]
        assert summary_len > 0
        # Summary lives at out[-(FOOTER_LEN + summary_len) : -FOOTER_LEN]
        # so the offset must be inside the file.
        assert summary_len + FOOTER_LEN <= len(out)


# ---------------------------------------------------------------------------
# Chunk threshold modes
# ---------------------------------------------------------------------------
class TestChunkModes:
    def test_size_mode_rolls_chunk_at_threshold(self):
        cfg = ChunkConfig(mode=ChunkThresholdMode.SIZE, size=10)
        buf = io.BytesIO()
        w = Writer(buf)
        w.open_topics(["a"], [{}], chunk_config=cfg)
        # 4 messages of 6 bytes each: thresholds at msg 2 (12 >= 10) and msg 4 (24>=10 after reset).
        for i in range(4):
            w.write_message("a", b"123456", (i + 1) * 10)
        w.close_topic()
        w.close()
        summary = Reader(BytesReadSource(buf.getvalue())).summary()
        # Expect multiple chunks.
        assert len(summary.topics_infos[0].index_chunk_info_list) >= 2

    def test_count_mode_rolls_chunk_at_threshold(self):
        cfg = ChunkConfig(mode=ChunkThresholdMode.COUNT, count=2)
        buf = io.BytesIO()
        w = Writer(buf)
        w.open_topics(["a"], [{}], chunk_config=cfg)
        for i in range(5):
            w.write_message("a", b"x", (i + 1) * 10)
        w.close_topic()
        w.close()
        summary = Reader(BytesReadSource(buf.getvalue())).summary()
        # 5 msgs, count=2 -> chunks of 2+2+1 = 3 chunks.
        assert len(summary.topics_infos[0].index_chunk_info_list) == 3

    def test_duration_mode_rolls_chunk_at_threshold(self):
        cfg = ChunkConfig(mode=ChunkThresholdMode.DURATION, duration=100)
        buf = io.BytesIO()
        w = Writer(buf)
        w.open_topics(["a"], [{}], chunk_config=cfg)
        # ts=0, 50, 100, 200, 300 with duration=100:
        #   ts=0 starts chunk at 0; ts=100 reaches 100>=100 -> flush {0,50,100}.
        #   ts=200 starts new chunk at 200; ts=300 reaches 100>=100 -> flush {200,300}.
        # Result: 2 chunks. Verifies the flush-and-reset semantics, not just count.
        for ts in (0, 50, 100, 200, 300):
            w.write_message("a", b"x", ts)
        w.close_topic()
        w.close()
        infos = (
            Reader(BytesReadSource(buf.getvalue()))
            .summary()
            .topics_infos[0]
            .index_chunk_info_list
        )
        assert len(infos) == 2
        assert (infos[0].start_timestamp, infos[0].end_timestamp) == (0, 100)
        assert (infos[1].start_timestamp, infos[1].end_timestamp) == (200, 300)


# ---------------------------------------------------------------------------
# Compression option
# ---------------------------------------------------------------------------
class TestCompression:
    def test_uncompressed_default(self):
        buf = io.BytesIO()
        w = Writer(buf)
        w.open_topics(["a"], [{}])
        w.write_message("a", b"x" * 100, 1)
        w.close_topic()
        w.close()
        summary = Reader(BytesReadSource(buf.getvalue())).summary()
        # __td_is_compressed is not in metadata if compression=False.
        assert "__td_is_compressed" not in summary.topics_infos[0].topic_metadatas[0].metadata

    def test_compression_sets_metadata_flag(self):
        buf = io.BytesIO()
        w = Writer(buf)
        w.open_topics(["a"], [{}], compression=True)
        w.write_message("a", b"x" * 100, 1)
        w.close_topic()
        w.close()
        summary = Reader(BytesReadSource(buf.getvalue())).summary()
        assert summary.topics_infos[0].topic_metadatas[0].metadata.get("__td_is_compressed") is True

    def test_chunk_config_stored_in_metadata(self):
        cfg = ChunkConfig(mode=ChunkThresholdMode.COUNT, count=3)
        buf = io.BytesIO()
        w = Writer(buf)
        w.open_topics(["a"], [{}], chunk_config=cfg)
        w.write_message("a", b"x", 1)
        w.close_topic()
        w.close()
        summary = Reader(BytesReadSource(buf.getvalue())).summary()
        meta = summary.topics_infos[0].topic_metadatas[0].metadata
        assert meta["__td_chunk_config"]["mode"] == int(ChunkThresholdMode.COUNT)
        assert meta["__td_chunk_config"]["count"] == 3


# ---------------------------------------------------------------------------
# Multi-group structure
# ---------------------------------------------------------------------------
class TestMultiGroup:
    def test_summary_has_one_topics_info_per_open_call(self):
        buf = io.BytesIO()
        w = Writer(buf)
        for name in ["a", "b", "c"]:
            w.open_topics([name], [{}])
            w.write_message(name, b"x", 1)
            w.close_topic()
        w.close()
        summary = Reader(BytesReadSource(buf.getvalue())).summary()
        assert len(summary.topics_infos) == 3
        assert [ti.topic_metadatas[0].name for ti in summary.topics_infos] == ["a", "b", "c"]

    def test_index_chunk_offsets_are_increasing(self):
        buf = io.BytesIO()
        w = Writer(buf)
        for name in ["a", "b"]:
            cfg = ChunkConfig(mode=ChunkThresholdMode.COUNT, count=1)
            w.open_topics([name], [{}], chunk_config=cfg)
            for i in range(3):
                w.write_message(name, b"x", i + 1)
            w.close_topic()
        w.close()
        summary = Reader(BytesReadSource(buf.getvalue())).summary()
        all_offsets = []
        for ti in summary.topics_infos:
            for info in ti.index_chunk_info_list:
                all_offsets.append(info.offset)
        assert all_offsets == sorted(all_offsets)
