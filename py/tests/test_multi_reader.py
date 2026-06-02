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

"""Unit tests for turbodata.MultiReader. Mirrors go/multi_reader_test.go.

Covers time-split union, augmentation, union-vs-split remap, multi-sample
latest-floor semantics, and the video decodable time-disjoint / overlap rules
for both read_messages and sample.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import List

import pytest

from turbodata import (
    BytesReadSource,
    MultiReader,
    Order,
    Reader,
    SampleQuery,
    SampleResult,
    VideoSourcesOverlapError,
    Writer,
)

from .conftest import build_file


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
@dataclass
class _F:
    ts: int
    is_key_frame: bool
    data: bytes


def _build_one_topic(topic: str, *tss: int) -> bytes:
    """A single-topic file with one message per timestamp. Payload is
    "<topic>@<ts>" so callers can assert provenance. Mirrors buildOneTopic."""

    def setup(w: Writer) -> None:
        w.open_topics([topic], [{}])
        for ts in tss:
            w.write_message(topic, f"{topic}@{ts}".encode(), ts)
        w.close_topic()

    return build_file(setup)


def _build_video_file(topic: str, frames: List[_F]) -> bytes:
    def setup(w: Writer) -> None:
        w.open_topics([topic], [{}], video=True)
        for f in frames:
            w.write_video_message(topic, f.data, f.ts, f.is_key_frame)
        w.close_topic()

    return build_file(setup)


def _build_video_plus_topic(
    video_topic: str, vframes: List[_F], plain_topic: str, plain_ts: List[int]
) -> bytes:
    def setup(w: Writer) -> None:
        w.open_topics([video_topic], [{}], video=True)
        for f in vframes:
            w.write_video_message(video_topic, f.data, f.ts, f.is_key_frame)
        w.close_topic()
        w.open_topics([plain_topic], [{}])
        for ts in plain_ts:
            w.write_message(plain_topic, f"{plain_topic}@{ts}".encode(), ts)
        w.close_topic()

    return build_file(setup)


def _reader(raw: bytes, **kwargs) -> Reader:
    return Reader(BytesReadSource(raw), **kwargs)


def _collect(mr: MultiReader, **opts):
    return [(m.timestamp, m.topic_name, m.data) for m in mr.read_messages(copy=True, **opts)]


def _assert_results_equal(got: List[SampleResult], want: List[SampleResult]) -> None:
    assert len(got) == len(want), f"count: got {len(got)} want {len(want)}"
    for i, (g, w) in enumerate(zip(got, want)):
        assert g.found == w.found, f"[{i}] found"
        if not w.found:
            continue
        assert g.timestamp == w.timestamp, f"[{i}] timestamp"
        assert g.data == w.data, f"[{i}] data"


def _assert_frames_match(got, want: List[_F]) -> None:
    assert len(got) == len(want), f"frames: got {len(got)} want {len(want)}"
    for i, (g, w) in enumerate(zip(got, want)):
        assert g.data == w.data, f"frames[{i}]: got {g.data!r} want {w.data!r}"


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------
def test_new_multi_reader_requires_a_reader():
    with pytest.raises(ValueError):
        MultiReader()


# ---------------------------------------------------------------------------
# read_messages: merging
# ---------------------------------------------------------------------------
def test_multi_read_time_split():
    """Same topic over disjoint time ranges merges into one ordered stream,
    forward and reverse."""
    a = _build_one_topic("cam", 10, 30, 50)
    b = _build_one_topic("cam", 20, 40, 60)
    mr = MultiReader(_reader(a), _reader(b))

    fwd = _collect(mr)
    assert [ts for ts, _, _ in fwd] == [10, 20, 30, 40, 50, 60]
    assert all(name == "cam" for _, name, _ in fwd)

    rev = _collect(mr, order=Order.REVERSE_TIME)
    assert [ts for ts, _, _ in rev] == [60, 50, 40, 30, 20, 10]


def test_multi_read_augmentation():
    """Disjoint topics over overlapping time merge into the time-ordered union."""
    a = _build_one_topic("lidar", 10, 20, 30)
    b = _build_one_topic("radar", 15, 25, 35)
    mr = MultiReader(_reader(a), _reader(b))

    got = [(ts, name) for ts, name, _ in _collect(mr)]
    assert got == [
        (10, "lidar"),
        (15, "radar"),
        (20, "lidar"),
        (25, "radar"),
        (30, "lidar"),
        (35, "radar"),
    ]


def test_multi_read_union_vs_split():
    """A shared in-file name unions across files; a per-Reader remap keeps the
    colliding name distinct."""
    a = _build_one_topic("cam", 10, 30)
    b = _build_one_topic("cam", 20, 40)

    union = MultiReader(_reader(a), _reader(b))
    um = _collect(union)
    assert len(um) == 4
    assert all(name == "cam" for _, name, _ in um)

    split = MultiReader(_reader(a), _reader(b, topic_remap={"cam": "cam_b"}))
    sm = _collect(split)
    names = {}
    for _, name, _ in sm:
        names[name] = names.get(name, 0) + 1
    assert names == {"cam": 2, "cam_b": 2}


# ---------------------------------------------------------------------------
# sample
# ---------------------------------------------------------------------------
def test_multi_sample_latest_floor():
    """The same non-video topic split across two files resolves each timestamp
    to the latest floor across the union."""
    a = _build_one_topic("cam", 10, 20)
    b = _build_one_topic("cam", 30, 40)
    mr = MultiReader(_reader(a), _reader(b))

    out = mr.sample([SampleQuery(topic="cam", timestamps=[5, 15, 35, 50])])
    _assert_results_equal(
        out[0],
        [
            SampleResult(False, 0, b""),
            SampleResult(True, 10, b"cam@10"),
            SampleResult(True, 30, b"cam@30"),
            SampleResult(True, 40, b"cam@40"),
        ],
    )


def test_multi_sample_unknown_topic_rejected():
    a = _build_one_topic("cam", 10, 20)
    mr = MultiReader(_reader(a))
    with pytest.raises(Exception):
        mr.sample([SampleQuery(topic="nope", timestamps=[5])])


# ---------------------------------------------------------------------------
# video: sample
# ---------------------------------------------------------------------------
def test_multi_sample_video_time_disjoint():
    """A video topic spread across two time-disjoint files samples correctly,
    each picked cell carrying its reader's GOP prefix (reset at the boundary)."""
    a_frames = [
        _F(10, True, b"K10"),
        _F(20, False, b"P20"),
        _F(30, False, b"P30"),
    ]
    b_frames = [
        _F(100, True, b"K100"),
        _F(110, False, b"P110"),
        _F(120, False, b"P120"),
    ]
    mr = MultiReader(
        _reader(_build_video_file("cam", a_frames)),
        _reader(_build_video_file("cam", b_frames)),
    )

    out = mr.sample(
        [SampleQuery(topic="cam", timestamps=[25, 115])], video_decodable=True
    )
    row = out[0]
    assert len(row) == 2

    # T=25 -> file A, floor P20, full GOP prefix [K10, P20].
    assert row[0].found and row[0].is_video and row[0].reset_decoder
    assert row[0].timestamp == 20
    _assert_frames_match(row[0].frames, [a_frames[0], a_frames[1]])

    # T=115 -> file B wins, first found cell of B's row so reset with [K100, P110].
    assert row[1].found and row[1].is_video and row[1].reset_decoder
    assert row[1].timestamp == 110
    _assert_frames_match(row[1].frames, [b_frames[0], b_frames[1]])


def test_multi_sample_video_overlap_error():
    """Overlapping video sources are rejected under video_decodable but allowed
    (plain floor) without it."""
    a_frames = [_F(10, True, b"K10"), _F(30, False, b"P30"), _F(50, False, b"P50")]
    b_frames = [_F(40, True, b"K40"), _F(60, False, b"P60"), _F(80, False, b"P80")]
    mr = MultiReader(
        _reader(_build_video_file("cam", a_frames)),
        _reader(_build_video_file("cam", b_frames)),
    )

    with pytest.raises(VideoSourcesOverlapError):
        mr.sample([SampleQuery(topic="cam", timestamps=[45])], video_decodable=True)

    # Without the decodable option, overlap is fine: plain floor semantics.
    out = mr.sample([SampleQuery(topic="cam", timestamps=[45])])
    got = out[0][0]
    assert got.found and got.timestamp == 40 and got.data == b"K40"


# ---------------------------------------------------------------------------
# video: read_messages
# ---------------------------------------------------------------------------
def test_multi_read_video_time_disjoint():
    """A video topic split across two time-disjoint files merges fine under
    video_decodable."""
    a_frames = [_F(10, True, b"K10"), _F(20, False, b"P20"), _F(30, False, b"P30")]
    b_frames = [_F(100, True, b"K100"), _F(110, False, b"P110"), _F(120, False, b"P120")]
    mr = MultiReader(
        _reader(_build_video_file("cam", a_frames)),
        _reader(_build_video_file("cam", b_frames)),
    )

    got = _collect(mr, video_decodable=True)
    assert [ts for ts, _, _ in got] == [10, 20, 30, 100, 110, 120]
    assert all(name == "cam" for _, name, _ in got)


def test_multi_read_video_overlap_error():
    """Under video_decodable, overlapping video sources are rejected; without
    the option the merge is allowed."""
    a_frames = [_F(10, True, b"K10"), _F(30, False, b"P30"), _F(50, False, b"P50")]
    b_frames = [_F(40, True, b"K40"), _F(60, False, b"P60"), _F(80, False, b"P80")]
    mr = MultiReader(
        _reader(_build_video_file("cam", a_frames)),
        _reader(_build_video_file("cam", b_frames)),
    )

    with pytest.raises(VideoSourcesOverlapError):
        # Generator body runs eagerly up to the disjoint check.
        mr.read_messages(video_decodable=True)

    # Without the option, the merge is allowed (no decode contract).
    list(mr.read_messages())


def test_multi_read_video_overlap_out_of_scope():
    """An overlapping video topic filtered out via topic_names must not trigger
    the disjoint check; bringing it back into scope re-triggers it."""
    a_frames = [_F(10, True, b"K10"), _F(50, False, b"P50")]
    b_frames = [_F(40, True, b"K40"), _F(80, False, b"P80")]
    mr = MultiReader(
        _reader(_build_video_plus_topic("cam", a_frames, "imu", [15, 25])),
        _reader(_build_video_plus_topic("cam", b_frames, "imu", [45, 55])),
    )

    got = _collect(mr, video_decodable=True, topic_names=["imu"])
    assert [ts for ts, _, _ in got] == [15, 25, 45, 55]
    assert all(name == "imu" for _, name, _ in got)

    with pytest.raises(VideoSourcesOverlapError):
        mr.read_messages(video_decodable=True, topic_names=["cam"])
