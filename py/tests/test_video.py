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

"""Video support tests. Mirrors go/video_test.go.

Covers the write-time option/constraints, write_video_message API gating and
GOP-integrity chunking, the sample GOP-prefix semantics (and the floor
fallback without the decodable opt-in), and read_messages key-frame snap-back.
"""
from __future__ import annotations

import io
from dataclasses import dataclass
from typing import List

import pytest

from turbodata import (
    BytesReadSource,
    ChunkConfig,
    ChunkThresholdMode,
    FirstVideoMessageMustBeKeyFrameError,
    Frame,
    Reader,
    SampleQuery,
    VideoGroupMustBeSingleTopicError,
    VideoTopicCannotBeCompressedError,
    Writer,
    WriteMessageOnVideoTopicError,
    WriteVideoMessageOnNonVideoTopicError,
)
from turbodata import _codec, _compress

from .conftest import build_file, collect, writer_round_trip


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
@dataclass
class _F:
    """A synthetic (timestamp, is_key_frame, payload) video frame."""

    ts: int
    is_key_frame: bool
    data: bytes


def _write_video(w: Writer, topic: str, frames: List[_F]) -> None:
    for f in frames:
        w.write_video_message(topic, f.data, f.ts, f.is_key_frame)


def _load_index_chunk(reader: Reader, ti: _codec.TopicsInfo, ci: int) -> _codec.IndexChunk:
    """Fetch + decompress + parse one index chunk via the reader's source.
    Mirrors mustLoadIndexChunk: needed to inspect key_frame_indexes on the wire."""
    infos = ti.index_chunk_info_list
    info = infos[ci]
    if ci < len(infos) - 1:
        ln = infos[ci + 1].offset - info.offset
    else:
        ln = ti.total_len - info.offset + infos[0].offset
    raw = reader._source.read_at(info.offset, ln)
    decompressed = _compress.decompress(raw)
    return _codec.read_index_chunk_from_bytes(decompressed)


def _assert_frames_match(got: List[Frame], want: List[_F]) -> None:
    got_payloads = [f.data for f in got]
    want_payloads = [f.data for f in want]
    assert len(got) == len(want), f"frames: got {got_payloads} want {want_payloads}"
    for i, (g, w) in enumerate(zip(got, want)):
        assert g.data == w.data, f"frames[{i}].data: got {g.data!r} want {w.data!r}"
        assert g.timestamp == w.ts, f"frames[{i}].timestamp"
        assert g.is_key_frame == w.is_key_frame, f"frames[{i}].is_key_frame"


def _payloads(msgs) -> List[bytes]:
    return [m.data for m in msgs]


# ---------------------------------------------------------------------------
# WithVideoTopic option validation
# ---------------------------------------------------------------------------
class TestVideoTopicOption:
    def test_sets_is_video_metadata(self):
        def setup(w: Writer) -> None:
            w.open_topics(["cam"], [{}], video=True)
            assert w._is_video is True
            _write_video(w, "cam", [_F(1, True, b"K")])
            w.close_topic()

        r = writer_round_trip(setup)
        summary = r.summary()
        meta = summary.topics_infos[0].topic_metadatas[0].metadata
        assert meta.get("__td_is_video") is True

    def test_compression_and_video_rejected(self):
        w = Writer(io.BytesIO())
        with pytest.raises(VideoTopicCannotBeCompressedError):
            w.open_topics(["cam"], [{}], compression=True, video=True)

    def test_video_group_must_be_single_topic(self):
        w = Writer(io.BytesIO())
        with pytest.raises(VideoGroupMustBeSingleTopicError):
            w.open_topics(["cam", "stereo"], [{}, {}], video=True)


# ---------------------------------------------------------------------------
# API gating + first-message rule
# ---------------------------------------------------------------------------
class TestVideoApiGating:
    def test_write_message_rejected_on_video_topic(self):
        w = Writer(io.BytesIO())
        w.open_topics(["cam"], [{}], video=True)
        with pytest.raises(WriteMessageOnVideoTopicError):
            w.write_message("cam", b"x", 1)

    def test_write_video_message_rejected_on_non_video_topic(self):
        w = Writer(io.BytesIO())
        w.open_topics(["imu"], [{}])
        with pytest.raises(WriteVideoMessageOnNonVideoTopicError):
            w.write_video_message("imu", b"x", 1, True)

    def test_first_video_message_must_be_key_frame(self):
        w = Writer(io.BytesIO())
        w.open_topics(["cam"], [{}], video=True)
        with pytest.raises(FirstVideoMessageMustBeKeyFrameError):
            w.write_video_message("cam", b"p0", 1, False)


# ---------------------------------------------------------------------------
# GOP integrity at chunk boundaries
# ---------------------------------------------------------------------------
class TestVideoGOPIntegrity:
    def test_gop_integrity_chunk_boundary(self):
        # 1-byte payloads + size threshold 1: the writer is eager to flush, so
        # threshold is met before every key frame except the very first.
        cfg = ChunkConfig(mode=ChunkThresholdMode.SIZE, size=1)
        frames = [
            _F(10, True, b"K0"),
            _F(20, False, b"P1"),
            _F(30, False, b"P2"),
            _F(40, True, b"K3"),
            _F(50, False, b"P4"),
            _F(60, True, b"K5"),
        ]

        def setup(w: Writer) -> None:
            w.open_topics(["cam"], [{}], chunk_config=cfg, video=True)
            _write_video(w, "cam", frames)
            w.close_topic()

        r = writer_round_trip(setup)
        summary = r.summary()
        assert len(summary.topics_infos) == 1

        # Three GOPs: [K0 P1 P2], [K3 P4], [K5] -> three chunks.
        infos = summary.topics_infos[0].index_chunk_info_list
        assert len(infos) == 3, f"expected 3 chunks, got {len(infos)}"

        for ci in range(len(infos)):
            ic = _load_index_chunk(r, summary.topics_infos[0], ci)
            assert len(ic.topic_indexes) == 1
            ti = ic.topic_indexes[0]
            assert len(ti.key_frame_indexes) >= 1, f"chunk {ci}: no key frames"
            assert ti.key_frame_indexes[0] == 0, (
                f"chunk {ci}: key_frame_indexes[0]={ti.key_frame_indexes[0]}, "
                "want 0 (chunk must begin with a key frame)"
            )

        msgs = collect(r)
        assert len(msgs) == len(frames)
        for i, m in enumerate(msgs):
            assert m.data == frames[i].data
            assert m.timestamp == frames[i].ts

    def test_duration_threshold_uses_last_timestamp(self):
        # Duration=100. GOP A spans 0..50 (< 100). Next keyframe at ts=110.
        # The flush decision must measure the chunk's current contents (ending
        # at lastTimestamp=50), so KB joins the same chunk: one chunk total.
        cfg = ChunkConfig(mode=ChunkThresholdMode.DURATION, duration=100)
        frames = [
            _F(0, True, b"KA"),
            _F(50, False, b"A1"),
            _F(110, True, b"KB"),
        ]

        def setup(w: Writer) -> None:
            w.open_topics(["cam"], [{}], chunk_config=cfg, video=True)
            _write_video(w, "cam", frames)
            w.close_topic()

        r = writer_round_trip(setup)
        summary = r.summary()
        assert len(summary.topics_infos[0].index_chunk_info_list) == 1

    def test_no_flush_mid_gop(self):
        cfg = ChunkConfig(mode=ChunkThresholdMode.SIZE, size=1)
        frames = [
            _F(1, True, b"K"),
            _F(2, False, b"P"),
            _F(3, False, b"P"),
            _F(4, False, b"P"),
            _F(5, False, b"P"),
        ]

        def setup(w: Writer) -> None:
            w.open_topics(["cam"], [{}], chunk_config=cfg, video=True)
            _write_video(w, "cam", frames)
            w.close_topic()

        r = writer_round_trip(setup)
        summary = r.summary()
        assert len(summary.topics_infos[0].index_chunk_info_list) == 1


# ---------------------------------------------------------------------------
# Sample with video_decodable
# ---------------------------------------------------------------------------
class TestSampleVideoDecodable:
    def test_single_query_returns_gop_prefix(self):
        frames = [
            _F(10, True, b"K10"),
            _F(20, False, b"P20"),
            _F(30, False, b"P30"),
            _F(40, False, b"P40"),
        ]

        def setup(w: Writer) -> None:
            w.open_topics(["cam"], [{}], video=True)
            _write_video(w, "cam", frames)
            w.close_topic()

        r = writer_round_trip(setup)
        out = r.sample([SampleQuery(topic="cam", timestamps=[30])], video_decodable=True)
        res = out[0][0]
        assert res.found
        assert res.is_video
        assert res.reset_decoder
        _assert_frames_match(res.frames, [frames[0], frames[1], frames[2]])
        assert res.data == b""  # bytes live in frames for video
        assert res.timestamp == 30
        assert res.frames[-1].data == b"P30"

    def test_query_on_key_frame(self):
        frames = [_F(10, True, b"K10"), _F(20, False, b"P20")]

        def setup(w: Writer) -> None:
            w.open_topics(["cam"], [{}], video=True)
            _write_video(w, "cam", frames)
            w.close_topic()

        r = writer_round_trip(setup)
        out = r.sample([SampleQuery(topic="cam", timestamps=[10])], video_decodable=True)
        res = out[0][0]
        assert res.found and res.reset_decoder
        assert len(res.frames) == 1
        assert res.frames[0].is_key_frame

    def test_incremental_same_gop(self):
        frames = [
            _F(10, True, b"K0"),
            _F(20, False, b"P1"),
            _F(30, False, b"P2"),
            _F(40, False, b"P3"),
            _F(50, False, b"P4"),
            _F(60, False, b"P5"),
        ]

        def setup(w: Writer) -> None:
            w.open_topics(["cam"], [{}], video=True)
            _write_video(w, "cam", frames)
            w.close_topic()

        r = writer_round_trip(setup)
        # 25 -> P1, 45 -> P3, 60 -> P5. All same GOP.
        out = r.sample(
            [SampleQuery(topic="cam", timestamps=[25, 45, 60])], video_decodable=True
        )
        row = out[0]
        assert len(row) == 3

        assert row[0].reset_decoder
        _assert_frames_match(row[0].frames, [frames[0], frames[1]])

        assert not row[1].reset_decoder
        _assert_frames_match(row[1].frames, [frames[2], frames[3]])

        assert not row[2].reset_decoder
        _assert_frames_match(row[2].frames, [frames[4], frames[5]])

        # Dedup: total bytes equal the full prefix up to the last target.
        total_got = sum(len(f.data) for res in row for f in res.frames)
        total_want = sum(len(f.data) for f in frames)
        assert total_got == total_want

    def test_spanning_two_gops(self):
        cfg = ChunkConfig(mode=ChunkThresholdMode.SIZE, size=1)
        frames = [
            _F(10, True, b"KA"),
            _F(20, False, b"A1"),
            _F(30, False, b"A2"),
            _F(40, True, b"KB"),
            _F(50, False, b"B1"),
            _F(60, False, b"B2"),
        ]

        def setup(w: Writer) -> None:
            w.open_topics(["cam"], [{}], chunk_config=cfg, video=True)
            _write_video(w, "cam", frames)
            w.close_topic()

        r = writer_round_trip(setup)
        # 25 -> A1 (GOP A), 55 -> B1 (GOP B).
        out = r.sample(
            [SampleQuery(topic="cam", timestamps=[25, 55])], video_decodable=True
        )
        row = out[0]
        assert row[0].reset_decoder
        _assert_frames_match(row[0].frames, [frames[0], frames[1]])
        # Different GOP -> reset again, full prefix from GOP B's key frame.
        assert row[1].reset_decoder
        _assert_frames_match(row[1].frames, [frames[3], frames[4]])

    def test_two_queries_same_target(self):
        frames = [_F(10, True, b"K"), _F(20, False, b"P1"), _F(30, False, b"P2")]

        def setup(w: Writer) -> None:
            w.open_topics(["cam"], [{}], video=True)
            _write_video(w, "cam", frames)
            w.close_topic()

        r = writer_round_trip(setup)
        # 21 and 22 both floor to P1 (ts=20).
        out = r.sample(
            [SampleQuery(topic="cam", timestamps=[21, 22])], video_decodable=True
        )
        row = out[0]
        assert row[0].reset_decoder and len(row[0].frames) == 2
        assert not row[1].reset_decoder
        assert row[1].is_video
        assert len(row[1].frames) == 0  # same target, nothing new to feed
        assert row[1].data == b""
        assert row[1].timestamp == 20
        assert row[0].frames[-1].data == b"P1"  # row[1]'s target bytes

    def test_before_first_key_frame(self):
        frames = [_F(10, True, b"K"), _F(20, False, b"P")]

        def setup(w: Writer) -> None:
            w.open_topics(["cam"], [{}], video=True)
            _write_video(w, "cam", frames)
            w.close_topic()

        r = writer_round_trip(setup)
        out = r.sample([SampleQuery(topic="cam", timestamps=[5])], video_decodable=True)
        assert not out[0][0].found


# ---------------------------------------------------------------------------
# Sample without the decodable option: plain floor semantics
# ---------------------------------------------------------------------------
class TestSampleVideoFloorWithoutOption:
    def test_floor_semantics_apply(self):
        frames = [
            _F(10, True, b"K"),
            _F(20, False, b"P1"),
            _F(30, False, b"P2"),
        ]

        def setup(w: Writer) -> None:
            w.open_topics(["cam"], [{}], video=True)
            _write_video(w, "cam", frames)
            w.close_topic()

        r = writer_round_trip(setup)
        # No video_decodable: video topic behaves like any other topic.
        out = r.sample([SampleQuery(topic="cam", timestamps=[10, 30])])
        row = out[0]
        want_data = [b"K", b"P2"]
        want_ts = [10, 30]
        for i, res in enumerate(row):
            assert res.found
            assert not res.is_video
            assert res.frames == [] and not res.reset_decoder
            assert res.data == want_data[i]
            assert res.timestamp == want_ts[i]


# ---------------------------------------------------------------------------
# read_messages snap-back
# ---------------------------------------------------------------------------
class TestReadMessagesVideoDecodable:
    def test_snap_back(self):
        frames = [
            _F(10, True, b"K10"),
            _F(20, False, b"P20"),
            _F(30, False, b"P30"),
            _F(40, False, b"P40"),
        ]

        def setup(w: Writer) -> None:
            w.open_topics(["cam"], [{}], video=True)
            _write_video(w, "cam", frames)
            w.close_topic()

        r = writer_round_trip(setup)

        # Without snap-back, start=25 skips K10 and P20.
        msgs = collect(r, start_timestamp=25)
        assert _payloads(msgs) == [b"P30", b"P40"]

        # With snap-back, start=25 snaps to ts=10 (the key frame).
        r2 = writer_round_trip(setup)
        msgs = collect(r2, start_timestamp=25, video_decodable=True)
        assert _payloads(msgs) == [b"K10", b"P20", b"P30", b"P40"]

    def test_snap_back_multiple_gops_across_chunks(self):
        cfg = ChunkConfig(mode=ChunkThresholdMode.SIZE, size=1)
        frames = [
            _F(10, True, b"KA"),
            _F(20, False, b"A1"),
            _F(30, True, b"KB"),
            _F(40, False, b"B1"),
            _F(50, False, b"B2"),
        ]

        def setup(w: Writer) -> None:
            w.open_topics(["cam"], [{}], chunk_config=cfg, video=True)
            _write_video(w, "cam", frames)
            w.close_topic()

        r = writer_round_trip(setup)
        # start=45 mid-GOP B; snap should land on KB (ts=30), not KA.
        msgs = collect(r, start_timestamp=45, video_decodable=True)
        assert _payloads(msgs) == [b"KB", b"B1", b"B2"]

    def test_snap_back_multiple_gops_same_chunk(self):
        frames = [
            _F(10, True, b"KA"),
            _F(20, False, b"A1"),
            _F(30, True, b"KB"),
            _F(40, False, b"B1"),
            _F(50, True, b"KC"),
            _F(60, False, b"C1"),
        ]

        def setup(w: Writer) -> None:
            w.open_topics(["cam"], [{}], video=True)
            _write_video(w, "cam", frames)
            w.close_topic()

        r = writer_round_trip(setup)
        # Guard: all frames in a single chunk.
        summary = r.summary()
        assert len(summary.topics_infos[0].index_chunk_info_list) == 1

        # start=45 mid-GOP B; snap must land on KB (ts=30), not the chunk's
        # first key frame KA (ts=10).
        msgs = collect(r, start_timestamp=45, video_decodable=True)
        assert _payloads(msgs) == [b"KB", b"B1", b"KC", b"C1"]

    def test_no_snap_back_by_default(self):
        frames = [
            _F(10, True, b"K"),
            _F(20, False, b"P1"),
            _F(30, False, b"P2"),
        ]

        def setup(w: Writer) -> None:
            w.open_topics(["cam"], [{}], video=True)
            _write_video(w, "cam", frames)
            w.close_topic()

        r = writer_round_trip(setup)
        msgs = collect(r, start_timestamp=25)
        assert _payloads(msgs) == [b"P2"]


# ---------------------------------------------------------------------------
# Cost-aware path snap-back parity
# ---------------------------------------------------------------------------
class TestReadMessagesVideoDecodableCostAware:
    def test_snap_back_cost_aware(self):
        from turbodata import ReadStrategy

        frames = [
            _F(10, True, b"K10"),
            _F(20, False, b"P20"),
            _F(30, False, b"P30"),
            _F(40, False, b"P40"),
        ]

        def setup(w: Writer) -> None:
            w.open_topics(["cam"], [{}], video=True)
            _write_video(w, "cam", frames)
            w.close_topic()

        r = writer_round_trip(setup)
        msgs = collect(
            r,
            start_timestamp=25,
            video_decodable=True,
            strategy=ReadStrategy(
                coalesce_gap=1 << 20, split_threshold=4 << 20, max_concurrency=4
            ),
        )
        assert _payloads(msgs) == [b"K10", b"P20", b"P30", b"P40"]
