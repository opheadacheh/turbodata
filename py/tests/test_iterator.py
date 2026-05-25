"""Unit tests for the message-iteration path. Mirrors go/internal/iter/iterator_test.go
and group_test.go (default path) — both files cover the message iterator
end-to-end since Python doesn't expose a separate group iterator class.
"""
from __future__ import annotations

from turbodata import (
    ChunkConfig,
    ChunkThresholdMode,
    Order,
    Writer,
)

from .conftest import collect, writer_round_trip


# ---------------------------------------------------------------------------
# Iterator basics
# ---------------------------------------------------------------------------
class TestNextEmpty:
    def test_empty_file_iterator_is_empty(self):
        r = writer_round_trip(lambda w: None)
        assert collect(r) == []


class TestNextSingleTopic:
    def test_single_topic_in_order(self):
        def setup(w: Writer) -> None:
            w.open_topics(["cam"], [{}])
            w.write_message("cam", b"frame0", 10)
            w.write_message("cam", b"frame1", 20)
            w.write_message("cam", b"frame2", 30)
            w.close_topic()

        msgs = collect(writer_round_trip(setup))
        assert [(m.timestamp, m.topic_name, m.data) for m in msgs] == [
            (10, "cam", b"frame0"),
            (20, "cam", b"frame1"),
            (30, "cam", b"frame2"),
        ]


# ---------------------------------------------------------------------------
# Multi-topic merge within a group
# ---------------------------------------------------------------------------
class TestNextMultiTopicMerge:
    def test_interleaved_by_timestamp(self):
        def setup(w: Writer) -> None:
            w.open_topics(["a", "b"], [{}, {}])
            w.write_message("a", b"a1", 10)
            w.write_message("b", b"b1", 20)
            w.write_message("a", b"a2", 30)
            w.write_message("b", b"b2", 40)
            w.close_topic()

        msgs = collect(writer_round_trip(setup))
        assert [(m.timestamp, m.topic_name, m.data) for m in msgs] == [
            (10, "a", b"a1"),
            (20, "b", b"b1"),
            (30, "a", b"a2"),
            (40, "b", b"b2"),
        ]


# ---------------------------------------------------------------------------
# Reverse order
# ---------------------------------------------------------------------------
class TestNextReverseOrder:
    def test_descending_timestamps(self):
        def setup(w: Writer) -> None:
            w.open_topics(["a", "b"], [{}, {}])
            w.write_message("a", b"a1", 10)
            w.write_message("b", b"b1", 20)
            w.write_message("a", b"a2", 30)
            w.write_message("b", b"b2", 40)
            w.close_topic()

        msgs = collect(writer_round_trip(setup), order=Order.REVERSE_TIME)
        assert [m.timestamp for m in msgs] == [40, 30, 20, 10]


# ---------------------------------------------------------------------------
# Topic-name filter: separate groups
# ---------------------------------------------------------------------------
class TestNextTopicNameFilter:
    def test_separate_groups(self):
        def setup(w: Writer) -> None:
            w.open_topics(["topic_a"], [{}])
            w.write_message("topic_a", b"a1", 10)
            w.write_message("topic_a", b"a2", 20)
            w.close_topic()
            w.open_topics(["topic_b"], [{}])
            w.write_message("topic_b", b"b1", 30)
            w.write_message("topic_b", b"b2", 40)
            w.close_topic()

        msgs = collect(writer_round_trip(setup), topic_names=["topic_a"])
        assert len(msgs) == 2
        assert all(m.topic_name == "topic_a" for m in msgs)


# ---------------------------------------------------------------------------
# Topic-name filter: same group (exercises intra-chunk filter)
# ---------------------------------------------------------------------------
class TestIntraGroupTopicFilter:
    """Same-group filter exercises sort_and_filter_merge's topic_ids check,
    not just the group-level skip in newTopicsGroupIterator."""

    def test_same_group_one_topic_kept(self):
        def setup(w: Writer) -> None:
            w.open_topics(["a", "b"], [{}, {}])
            w.write_message("a", b"a1", 10)
            w.write_message("b", b"b1", 20)
            w.write_message("a", b"a2", 30)
            w.write_message("b", b"b2", 40)
            w.close_topic()

        msgs = collect(writer_round_trip(setup), topic_names=["a"])
        assert [(m.timestamp, m.topic_name, m.data) for m in msgs] == [
            (10, "a", b"a1"),
            (30, "a", b"a2"),
        ]


# ---------------------------------------------------------------------------
# Time range filter
# ---------------------------------------------------------------------------
class TestNextTimestampRange:
    def test_one_message_per_chunk_chunk_filter(self):
        """One message per chunk so the chunk-level filter does the work."""
        cfg = ChunkConfig(mode=ChunkThresholdMode.COUNT, count=1)

        def setup(w: Writer) -> None:
            w.open_topics(["t"], [{}], chunk_config=cfg)
            for ts, data in [(10, b"m10"), (20, b"m20"), (30, b"m30"), (40, b"m40"), (50, b"m50")]:
                w.write_message("t", data, ts)
            w.close_topic()

        msgs = collect(
            writer_round_trip(setup), start_timestamp=20, end_timestamp=40
        )
        assert [m.timestamp for m in msgs] == [20, 30, 40]


class TestPerMessageTimestampFilter:
    """All messages in one chunk so boundary messages are dropped by the
    per-message filter inside sort_and_filter_merge."""

    def test_filters_boundary_messages_within_one_chunk(self):
        def setup(w: Writer) -> None:
            w.open_topics(["t"], [{}])
            for ts, data in [(5, b"m5"), (10, b"m10"), (20, b"m20"), (25, b"m25")]:
                w.write_message("t", data, ts)
            w.close_topic()

        msgs = collect(
            writer_round_trip(setup), start_timestamp=10, end_timestamp=20
        )
        assert [m.timestamp for m in msgs] == [10, 20]


# ---------------------------------------------------------------------------
# Chunk skipping when all messages are filtered out
# ---------------------------------------------------------------------------
class TestEmptyChunkAfterFilter:
    def test_iterator_advances_past_fully_filtered_chunk(self):
        cfg = ChunkConfig(mode=ChunkThresholdMode.COUNT, count=2)

        def setup(w: Writer) -> None:
            w.open_topics(["t"], [{}], chunk_config=cfg)
            for ts in (1, 2, 100, 200):
                w.write_message("t", str(ts).encode(), ts)
            w.close_topic()

        msgs = collect(writer_round_trip(setup), start_timestamp=100)
        assert [m.timestamp for m in msgs] == [100, 200]


# ---------------------------------------------------------------------------
# Multi-chunk
# ---------------------------------------------------------------------------
class TestNextMultipleChunks:
    def test_one_message_per_chunk_all_recovered(self):
        cfg = ChunkConfig(mode=ChunkThresholdMode.COUNT, count=1)

        def setup(w: Writer) -> None:
            w.open_topics(["t"], [{}], chunk_config=cfg)
            for i in range(5):
                w.write_message("t", bytes([i]), i * 10)
            w.close_topic()

        msgs = collect(writer_round_trip(setup))
        assert len(msgs) == 5
        for i, m in enumerate(msgs):
            assert m.timestamp == i * 10
            assert m.data[0] == i


# ---------------------------------------------------------------------------
# Multi-group merge
# ---------------------------------------------------------------------------
class TestNextMultiGroupMerge:
    def test_interleaved_across_groups(self):
        def setup(w: Writer) -> None:
            w.open_topics(["x"], [{}])
            w.write_message("x", b"x10", 10)
            w.write_message("x", b"x30", 30)
            w.write_message("x", b"x50", 50)
            w.close_topic()
            w.open_topics(["y"], [{}])
            w.write_message("y", b"y20", 20)
            w.write_message("y", b"y40", 40)
            w.close_topic()

        msgs = collect(writer_round_trip(setup))
        assert [(m.timestamp, m.topic_name) for m in msgs] == [
            (10, "x"), (20, "y"), (30, "x"), (40, "y"), (50, "x"),
        ]


# ---------------------------------------------------------------------------
# Compressed data path
# ---------------------------------------------------------------------------
class TestCompressedData:
    def test_compressed_group_roundtrip(self):
        def setup(w: Writer) -> None:
            w.open_topics(["t"], [{}], compression=True)
            w.write_message("t", b"hello", 10)
            w.write_message("t", b"world", 20)
            w.write_message("t", b"compressed", 30)
            w.close_topic()

        msgs = collect(writer_round_trip(setup))
        assert [(m.timestamp, m.data) for m in msgs] == [
            (10, b"hello"),
            (20, b"world"),
            (30, b"compressed"),
        ]
