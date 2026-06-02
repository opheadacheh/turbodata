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

"""Unit tests for Reader.sample. Mirrors go/sample_test.go.

Covers floor semantics, before/after-bounds behaviour, multi-topic, mixed
compressed/uncompressed groups, the fallback-to-previous-chunk path, batching
guarantees, validation aggregation, and concurrency under a high-fanout
strategy.
"""
from __future__ import annotations

from typing import List

import pytest

from turbodata import (
    BytesReadSource,
    ChunkConfig,
    ChunkThresholdMode,
    Reader,
    ReadStrategy,
    SampleQuery,
    SampleResult,
    SampleValidationError,
    Writer,
    lin_space_timestamps,
)

from .conftest import SlowReadSource, TrackingSource, build_file


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _build_sample_reader(setup) -> tuple:
    raw = build_file(setup)
    tracking = TrackingSource(BytesReadSource(raw))
    return Reader(tracking), tracking


def _assert_results_equal(got: List[SampleResult], want: List[SampleResult]) -> None:
    assert len(got) == len(want), f"count: got {len(got)} want {len(want)}"
    for i, (g, w) in enumerate(zip(got, want)):
        assert g.found == w.found, f"[{i}] found"
        assert g.timestamp == w.timestamp, f"[{i}] timestamp"
        assert g.data == w.data, f"[{i}] data"


# ---------------------------------------------------------------------------
# Floor semantics
# ---------------------------------------------------------------------------
class TestSampleFloorSemantics:
    def test_floor_on_single_topic(self):
        def setup(w: Writer) -> None:
            w.open_topics(["cam"], [{}])
            for ts, data in [(10, b"f10"), (20, b"f20"), (30, b"f30"), (40, b"f40"), (50, b"f50")]:
                w.write_message("cam", data, ts)
            w.close_topic()

        r, _ = _build_sample_reader(setup)
        out = r.sample([SampleQuery(topic="cam", timestamps=[10, 15, 20, 50])])
        _assert_results_equal(
            out[0],
            [
                SampleResult(True, 10, b"f10"),   # exact hit
                SampleResult(True, 10, b"f10"),   # between, floor
                SampleResult(True, 20, b"f20"),   # exact hit
                SampleResult(True, 50, b"f50"),   # last
            ],
        )

    def test_after_last_message_returns_last(self):
        def setup(w: Writer) -> None:
            w.open_topics(["x"], [{}])
            for ts, data in [(10, b"a"), (20, b"b"), (30, b"c")]:
                w.write_message("x", data, ts)
            w.close_topic()

        r, _ = _build_sample_reader(setup)
        out = r.sample([SampleQuery(topic="x", timestamps=[100])])
        assert out[0][0].found
        assert out[0][0].timestamp == 30
        assert out[0][0].data == b"c"

    def test_before_first_message_returns_not_found(self):
        def setup(w: Writer) -> None:
            w.open_topics(["x"], [{}])
            w.write_message("x", b"a", 10)
            w.write_message("x", b"b", 20)
            w.close_topic()

        r, _ = _build_sample_reader(setup)
        out = r.sample([SampleQuery(topic="x", timestamps=[5])])
        assert out[0][0].found is False


# ---------------------------------------------------------------------------
# Batching: same-chunk timestamps don't fetch the chunk multiple times.
# ---------------------------------------------------------------------------
class TestSampleBatching:
    def test_same_chunk_deduplicates_reads(self):
        def setup(w: Writer) -> None:
            w.open_topics(["cam"], [{}], compression=True)
            for ts, data in [(10, b"f10"), (20, b"f20"), (30, b"f30"), (40, b"f40"), (50, b"f50")]:
                w.write_message("cam", data, ts)
            w.close_topic()

        r, tracking = _build_sample_reader(setup)
        r.summary()  # prime the summary load
        before = tracking.read_at_calls

        out = r.sample([SampleQuery(topic="cam", timestamps=[15, 25, 35, 45])])
        _assert_results_equal(
            out[0],
            [
                SampleResult(True, 10, b"f10"),
                SampleResult(True, 20, b"f20"),
                SampleResult(True, 30, b"f30"),
                SampleResult(True, 40, b"f40"),
            ],
        )
        # One Phase A op (the single index chunk), one Phase B op (the single
        # data chunk). Allow some slack for coalescing variations.
        diff = tracking.read_at_calls - before
        assert diff <= 3, f"expected <= 3 ReadAt calls for 4 same-chunk samples, got {diff}"


# ---------------------------------------------------------------------------
# Multi-topic / mixed compression
# ---------------------------------------------------------------------------
class TestSampleMultiTopic:
    def test_mixed_compressed_and_uncompressed(self):
        def setup(w: Writer) -> None:
            w.open_topics(["img"], [{}], compression=True)
            for ts, d in [(10, b"img10"), (20, b"img20"), (30, b"img30")]:
                w.write_message("img", d, ts)
            w.close_topic()
            w.open_topics(["imu"], [{}])
            for ts, d in [(15, b"imu15"), (25, b"imu25"), (35, b"imu35")]:
                w.write_message("imu", d, ts)
            w.close_topic()

        r, _ = _build_sample_reader(setup)
        out = r.sample(
            [
                SampleQuery(topic="img", timestamps=[12, 22, 32]),
                SampleQuery(topic="imu", timestamps=[20, 30]),
            ]
        )
        _assert_results_equal(
            out[0],
            [
                SampleResult(True, 10, b"img10"),
                SampleResult(True, 20, b"img20"),
                SampleResult(True, 30, b"img30"),
            ],
        )
        _assert_results_equal(
            out[1],
            [
                SampleResult(True, 15, b"imu15"),
                SampleResult(True, 25, b"imu25"),
            ],
        )


# ---------------------------------------------------------------------------
# Fallback to previous chunk
# ---------------------------------------------------------------------------
class TestSampleFallback:
    def test_falls_back_when_candidate_chunk_lacks_topic_message(self):
        """Two topics interleaved at Count=1 so chunks contain only one
        topic each. Querying T=15 for topic 'a' lands in chunk index 1
        (b@10), which has no 'a'. Engine must fall back to chunk 0 (a@5)."""

        def setup(w: Writer) -> None:
            cfg = ChunkConfig(mode=ChunkThresholdMode.COUNT, count=1)
            w.open_topics(["a", "b"], [{}, {}], chunk_config=cfg)
            w.write_message("a", b"a5", 5)
            w.write_message("b", b"b10", 10)
            w.write_message("a", b"a30", 30)
            w.write_message("b", b"b40", 40)
            w.close_topic()

        r, _ = _build_sample_reader(setup)
        out = r.sample([SampleQuery(topic="a", timestamps=[15])])
        got = out[0][0]
        assert got.found and got.timestamp == 5 and got.data == b"a5"

    def test_fallback_past_chunk_zero_returns_not_found(self):
        def setup(w: Writer) -> None:
            cfg = ChunkConfig(mode=ChunkThresholdMode.COUNT, count=1)
            w.open_topics(["a", "b"], [{}, {}], chunk_config=cfg)
            w.write_message("a", b"a5", 5)
            w.write_message("b", b"b10", 10)
            w.close_topic()

        r, _ = _build_sample_reader(setup)
        out = r.sample([SampleQuery(topic="a", timestamps=[3])])
        assert out[0][0].found is False


# ---------------------------------------------------------------------------
# Validation aggregation
# ---------------------------------------------------------------------------
class TestSampleValidation:
    def test_aggregates_all_violations(self):
        def setup(w: Writer) -> None:
            w.open_topics(["a", "b"], [{}, {}])
            w.write_message("a", b"a1", 10)
            w.write_message("b", b"b1", 20)
            w.close_topic()

        r, tracking = _build_sample_reader(setup)
        r.summary()
        before = tracking.read_at_calls

        queries = [
            SampleQuery(topic="ghost", timestamps=[10]),                  # unknown
            SampleQuery(topic="a", timestamps=[30, 20]),                  # not strictly increasing
            SampleQuery(topic="a", timestamps=[40]),                      # duplicate
            SampleQuery(topic="b", timestamps=[10, 10, 20]),              # dup ts
        ]
        with pytest.raises(SampleValidationError) as exc_info:
            r.sample(queries)

        msg = str(exc_info.value)
        for needle in [
            "queries[0]",
            "unknown topic",
            "'ghost'",
            "queries[1]",
            "strictly increasing",
            "queries[2]",
            "duplicate topic",
            "queries[3]",
        ]:
            assert needle in msg, f"missing {needle!r} in error: {msg}"

        assert len(exc_info.value.violations) == 4

        # Validation must not issue further data reads after summary load.
        assert tracking.read_at_calls == before


class TestSampleEmptyTimestamps:
    def test_empty_timestamps_returns_empty_row(self):
        def setup(w: Writer) -> None:
            w.open_topics(["a"], [{}])
            w.write_message("a", b"x", 10)
            w.close_topic()

        r, tracking = _build_sample_reader(setup)
        r.summary()
        before = tracking.read_at_calls

        out = r.sample([SampleQuery(topic="a", timestamps=[])])
        assert len(out) == 1
        assert out[0] == []
        assert tracking.read_at_calls == before


# ---------------------------------------------------------------------------
# lin_space_timestamps
# ---------------------------------------------------------------------------
class TestLinSpaceTimestamps:
    def test_basic(self):
        assert lin_space_timestamps(100, 10, 5) == [100, 110, 120, 130, 140]

    def test_strictly_increasing_contract(self):
        out = lin_space_timestamps(1000, 1, 1000)
        for i in range(1, len(out)):
            assert out[i] > out[i - 1]

    @pytest.mark.parametrize("count", [0, -5])
    def test_zero_or_negative_count_returns_empty(self, count):
        assert lin_space_timestamps(0, 1, count) == []

    @pytest.mark.parametrize("stride", [0, -1])
    def test_non_positive_stride_raises(self, stride):
        with pytest.raises(ValueError):
            lin_space_timestamps(0, stride, 3)


# ---------------------------------------------------------------------------
# Concurrency
# ---------------------------------------------------------------------------
class TestSampleConcurrency:
    def test_sample_issues_concurrent_read_ats(self):
        """Many small chunks; one timestamp per chunk; CoalesceGap=0 to defeat
        coalescing; MaxConcurrency=8. The fetcher must observably overlap."""

        def setup(w: Writer) -> None:
            cfg = ChunkConfig(mode=ChunkThresholdMode.COUNT, count=1)
            w.open_topics(["t"], [{}], chunk_config=cfg)
            for i in range(32):
                w.write_message("t", bytes([i]), (i + 1) * 10)
            w.close_topic()

        raw = build_file(setup)
        # In-memory reads complete in nanoseconds; slow them so concurrency
        # is observable.
        slow = SlowReadSource(BytesReadSource(raw), delay_seconds=0.005)
        tracking = TrackingSource(slow)
        reader = Reader(tracking)

        timestamps = [(i + 1) * 10 for i in range(32)]
        out = reader.sample(
            [SampleQuery(topic="t", timestamps=timestamps)],
            strategy=ReadStrategy(coalesce_gap=0, split_threshold=(1 << 63) - 1, max_concurrency=8),
        )
        assert len(out[0]) == 32
        for i, res in enumerate(out[0]):
            assert res.found
            assert res.timestamp == (i + 1) * 10
            assert res.data == bytes([i])
        assert tracking.read_at_max_flight >= 2, (
            f"expected concurrent ReadAt; max in-flight = {tracking.read_at_max_flight}"
        )
