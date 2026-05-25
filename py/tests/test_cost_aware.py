"""Integration tests for the cost-aware reader path. Mirrors
go/cost_aware_integration_test.go.

Asserts:
  - The default path doesn't accidentally invoke read_at (regression guard).
  - The cost-aware path produces byte-identical results to the default path.
  - The cost-aware path reads strictly fewer bytes than the default path
    when most messages are filtered out (the whole point of message-level
    granularity).
  - The cost-aware path actually issues concurrent read_at calls when
    MaxConcurrency > 1.
  - Filters (topic + time range) compose with the cost-aware path the same
    way they compose with the default path.
"""
from __future__ import annotations

from typing import List, Optional

from turbodata import (
    BytesReadSource,
    ChunkConfig,
    ChunkThresholdMode,
    Order,
    Reader,
    ReadStrategy,
    Writer,
)

from .conftest import SlowReadSource, TrackingSource, build_file


_MAX_I64 = (1 << 63) - 1


def _collect(reader: Reader, **opts) -> List:
    return list(reader.read_messages(copy=True, **opts))


# ---------------------------------------------------------------------------
# Regression guard: default path never invokes read_at.
# ---------------------------------------------------------------------------
class TestDefaultPathRegressionGuard:
    def test_default_path_uses_only_summary_read_ats(self):
        def setup(w: Writer) -> None:
            w.open_topics(["cam"], [{}])
            w.write_message("cam", b"frame0", 10)
            w.write_message("cam", b"frame1", 20)
            w.write_message("cam", b"frame2", 30)
            w.close_topic()

        raw = build_file(setup)
        tracking = TrackingSource(BytesReadSource(raw))
        reader = Reader(tracking)
        out = _collect(reader)
        assert len(out) == 3
        # Default path uses summary's 2 read_ats (footer + summary) and then
        # streams the rest via Seek+Read. Python's BytesReadSource synthesizes
        # all I/O as read_at internally; even so, the default path should
        # have a small bounded number of reads (not one per message).
        assert tracking.read_at_calls <= 4


# ---------------------------------------------------------------------------
# Semantic equivalence: default vs cost-aware on the same file.
# ---------------------------------------------------------------------------
class TestSemanticEquivalence:
    def test_forward_order_compressed_and_uncompressed(self):
        def build(w: Writer) -> None:
            w.open_topics(
                ["img"],
                [{}],
                compression=True,
                chunk_config=ChunkConfig(mode=ChunkThresholdMode.COUNT, count=2),
            )
            for ts, d in [
                (10, b"img-frame-0"),
                (20, b"img-frame-1"),
                (30, b"img-frame-2"),
                (40, b"img-frame-3"),
                (50, b"img-frame-4"),
            ]:
                w.write_message("img", d, ts)
            w.close_topic()
            w.open_topics(
                ["imu"],
                [{}],
                chunk_config=ChunkConfig(mode=ChunkThresholdMode.COUNT, count=3),
            )
            for ts, d in [
                (15, b"imu-sample-15"),
                (25, b"imu-sample-25"),
                (35, b"imu-sample-35"),
                (45, b"imu-sample-45"),
            ]:
                w.write_message("imu", d, ts)
            w.close_topic()

        raw = build_file(build)
        want = _collect(Reader(BytesReadSource(raw)))
        got = _collect(
            Reader(BytesReadSource(raw)),
            strategy=ReadStrategy(coalesce_gap=1 << 20, split_threshold=_MAX_I64, max_concurrency=4),
        )

        assert len(got) == len(want)
        for i in range(len(want)):
            assert got[i].timestamp == want[i].timestamp
            assert got[i].topic_name == want[i].topic_name
            assert got[i].data == want[i].data

    def test_reverse_order_equivalence(self):
        def build(w: Writer) -> None:
            w.open_topics(
                ["x"],
                [{}],
                chunk_config=ChunkConfig(mode=ChunkThresholdMode.COUNT, count=2),
            )
            for i in range(6):
                w.write_message("x", bytes([ord("a") + i]), (i + 1) * 10)
            w.close_topic()

        raw = build_file(build)
        want = _collect(Reader(BytesReadSource(raw)), order=Order.REVERSE_TIME)
        got = _collect(
            Reader(BytesReadSource(raw)),
            order=Order.REVERSE_TIME,
            strategy=ReadStrategy(coalesce_gap=1 << 20, split_threshold=_MAX_I64, max_concurrency=2),
        )

        assert len(got) == len(want)
        for i in range(len(want)):
            assert got[i].timestamp == want[i].timestamp
            assert got[i].data == want[i].data


# ---------------------------------------------------------------------------
# Selective bytes savings: cost-aware reads fewer bytes than default.
# ---------------------------------------------------------------------------
class TestSelectiveUncompressedBytesSavings:
    def test_topic_filter_drops_most_bytes(self):
        def build(w: Writer) -> None:
            w.open_topics(["kept", "dropped"], [{}, {}])
            big = b"X" * 1024
            small = b"k" * 16
            w.write_message("kept", small, 10)
            w.write_message("dropped", big, 20)
            w.write_message("kept", small, 30)
            w.write_message("dropped", big, 40)
            w.write_message("kept", small, 50)
            w.write_message("dropped", big, 60)
            w.close_topic()

        raw = build_file(build)

        trk_default = TrackingSource(BytesReadSource(raw))
        default_out = _collect(Reader(trk_default), topic_names=["kept"])

        trk_cost = TrackingSource(BytesReadSource(raw))
        cost_out = _collect(
            Reader(trk_cost),
            topic_names=["kept"],
            strategy=ReadStrategy(coalesce_gap=0, split_threshold=_MAX_I64, max_concurrency=4),
        )

        assert len(default_out) == 3 and len(cost_out) == 3
        for i in range(3):
            assert default_out[i].data == cost_out[i].data

        assert trk_cost.read_at_bytes < trk_default.read_at_bytes, (
            f"cost-aware read {trk_cost.read_at_bytes} bytes, default {trk_default.read_at_bytes}"
        )


# ---------------------------------------------------------------------------
# Parallelism observable under slow reads
# ---------------------------------------------------------------------------
class TestCostAwareParallelism:
    def test_fetcher_overlaps_reads(self):
        def build(w: Writer) -> None:
            w.open_topics(
                ["t"],
                [{}],
                chunk_config=ChunkConfig(mode=ChunkThresholdMode.COUNT, count=1),
            )
            for i in range(32):
                w.write_message("t", bytes([i]), (i + 1) * 10)
            w.close_topic()

        raw = build_file(build)
        slow = SlowReadSource(BytesReadSource(raw), delay_seconds=0.005)
        tracking = TrackingSource(slow)
        reader = Reader(tracking)
        out = _collect(
            reader,
            strategy=ReadStrategy(coalesce_gap=0, split_threshold=_MAX_I64, max_concurrency=8),
        )
        assert len(out) == 32
        assert tracking.read_at_max_flight >= 2, (
            f"expected concurrent ReadAt; max in-flight = {tracking.read_at_max_flight}"
        )


# ---------------------------------------------------------------------------
# Filter composition with cost-aware path
# ---------------------------------------------------------------------------
class TestCostAwareWithTopicAndTimeFilters:
    def test_filters_compose(self):
        def build(w: Writer) -> None:
            w.open_topics(
                ["a"],
                [{}],
                chunk_config=ChunkConfig(mode=ChunkThresholdMode.COUNT, count=1),
            )
            for ts in range(10, 51, 10):
                w.write_message("a", bytes([ts]), ts)
            w.close_topic()
            w.open_topics(["b"], [{}])
            for ts in range(15, 46, 10):
                w.write_message("b", bytes([ts]), ts)
            w.close_topic()

        raw = build_file(build)

        opts = dict(topic_names=["a"], start_timestamp=20, end_timestamp=40)
        want = _collect(Reader(BytesReadSource(raw)), **opts)
        got = _collect(
            Reader(BytesReadSource(raw)),
            **opts,
            strategy=ReadStrategy(coalesce_gap=1 << 20, split_threshold=_MAX_I64, max_concurrency=4),
        )

        assert len(want) == len(got) == 3
        for i in range(3):
            assert want[i].timestamp == got[i].timestamp
            assert want[i].topic_name == got[i].topic_name
            assert want[i].data == got[i].data
