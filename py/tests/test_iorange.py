"""Unit tests for turbodata._iorange (plan / Fetcher / LoadedBytes).

Mirrors:
  go/internal/iorange/planner_test.go
  go/internal/iorange/fetcher_test.go
  go/internal/iorange/loaded_test.go
"""
from __future__ import annotations

import threading
import time

import pytest

from turbodata import BytesReadSource
from turbodata._iorange import (
    Fetcher,
    LoadedBytes,
    Range,
    RangeLocation,
    ReadOp,
    plan,
)


_MAX = (1 << 63) - 1


# ---------------------------------------------------------------------------
# plan(): coalesce + split
# ---------------------------------------------------------------------------
class TestPlanEmpty:
    def test_no_ranges_no_ops(self):
        ops, locs = plan([], 16, _MAX)
        assert ops == []
        assert locs == []


class TestPlanSingleRange:
    def test_single_range_one_op(self):
        ops, locs = plan([Range(offset=100, length=50)], 0, _MAX)
        assert ops == [ReadOp(offset=100, length=50)]
        assert locs == [RangeLocation(op_index=0, in_op_off=0, length=50)]


class TestPlanCoalesce:
    @pytest.mark.parametrize(
        "ranges, gap, want_ops, want_locs",
        [
            # gap == threshold: do NOT merge
            (
                [Range(0, 10), Range(20, 10)],
                10,
                [ReadOp(0, 10), ReadOp(20, 10)],
                [RangeLocation(0, 0, 10), RangeLocation(1, 0, 10)],
            ),
            # gap < threshold: merge
            (
                [Range(0, 10), Range(20, 10)],
                11,
                [ReadOp(0, 30)],
                [RangeLocation(0, 0, 10), RangeLocation(0, 20, 10)],
            ),
            # gap == 0 (adjacent): merge with any positive threshold
            (
                [Range(0, 10), Range(10, 5)],
                1,
                [ReadOp(0, 15)],
                [RangeLocation(0, 0, 10), RangeLocation(0, 10, 5)],
            ),
            # gap=0 with coalesce_gap=0: do NOT merge (gap is not < 0)
            (
                [Range(0, 10), Range(10, 5)],
                0,
                [ReadOp(0, 10), ReadOp(10, 5)],
                [RangeLocation(0, 0, 10), RangeLocation(1, 0, 5)],
            ),
        ],
        ids=["no_coalesce_at_threshold", "coalesce_below_threshold", "coalesce_adjacent", "coalesce_disabled"],
    )
    def test_coalesce_table(self, ranges, gap, want_ops, want_locs):
        ops, locs = plan(ranges, gap, _MAX)
        assert ops == want_ops
        assert locs == want_locs


class TestPlanSplit:
    @pytest.mark.parametrize(
        "ranges, gap, split, want_ops",
        [
            # below threshold -> one op
            (
                [Range(0, 40), Range(40, 40)],
                1,
                100,
                [ReadOp(0, 80)],
            ),
            # greedy split at member boundary: pack 0+40+40=80 ok; adding next pushes to 120>100, flush
            (
                [Range(0, 40), Range(40, 40), Range(80, 40)],
                1,
                100,
                [ReadOp(0, 80), ReadOp(80, 40)],
            ),
            # single oversize range stays as one op (atomic unit)
            (
                [Range(0, 250)],
                1,
                100,
                [ReadOp(0, 250)],
            ),
            # oversize first range stays alone; small trailing members merge
            (
                [Range(0, 250), Range(250, 10), Range(260, 10)],
                1,
                100,
                [ReadOp(0, 250), ReadOp(250, 20)],
            ),
            # exact threshold boundary keeps one op
            (
                [Range(0, 50), Range(50, 50)],
                1,
                100,
                [ReadOp(0, 100)],
            ),
        ],
        ids=[
            "below_threshold",
            "greedy_split",
            "single_oversize",
            "oversize_then_trailing",
            "exact_threshold_boundary",
        ],
    )
    def test_split_table(self, ranges, gap, split, want_ops):
        ops, locs = plan(ranges, gap, split)
        assert ops == want_ops
        # Sanity: every original range round-trips back to its absolute offset+length.
        for i, r in enumerate(ranges):
            op = ops[locs[i].op_index]
            assert op.offset + locs[i].in_op_off == r.offset, f"range[{i}]"
            assert locs[i].length == r.length, f"range[{i}]"

    def test_split_disabled(self):
        ranges = [Range(0, 100), Range(100, 100), Range(200, 100)]
        ops, _ = plan(ranges, 1, 0)
        assert ops == [ReadOp(0, 300)]


# ---------------------------------------------------------------------------
# Fetcher
# ---------------------------------------------------------------------------
class TestFetcherEmpty:
    def test_empty_ops_returns_empty_bufs(self):
        f = Fetcher(BytesReadSource(b"hello"), 4)
        assert f.execute([]) == []


class TestFetcherCorrectness:
    def test_reads_correct_bytes_at_offsets(self):
        data = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        f = Fetcher(BytesReadSource(data), 4)
        bufs = f.execute(
            [ReadOp(0, 5), ReadOp(10, 3), ReadOp(33, 3)]
        )
        assert bufs == [b"01234", b"ABC", b"XYZ"]


class _ConcurrencyObserver:
    """Observes max simultaneous in-flight read_at calls."""

    def __init__(self, inner, hold_seconds: float) -> None:
        self._inner = inner
        self._hold = hold_seconds
        self._lock = threading.Lock()
        self.in_flight = 0
        self.max_seen = 0

    def size(self) -> int:
        return self._inner.size()

    def read_at(self, offset: int, n: int) -> bytes:
        with self._lock:
            self.in_flight += 1
            if self.in_flight > self.max_seen:
                self.max_seen = self.in_flight
        try:
            time.sleep(self._hold)
            return self._inner.read_at(offset, n)
        finally:
            with self._lock:
                self.in_flight -= 1


class TestFetcherConcurrency:
    def test_respects_max_concurrency(self):
        obs = _ConcurrencyObserver(BytesReadSource(b"x" * 1024), hold_seconds=0.005)
        f = Fetcher(obs, max_concurrency=3)
        ops = [ReadOp(offset=i * 4, length=4) for i in range(16)]
        f.execute(ops)
        assert obs.max_seen <= 3, f"observed {obs.max_seen} in-flight, want <= 3"
        # And > 1 to confirm parallelism actually happened.
        assert obs.max_seen >= 2, "expected concurrent reads to overlap"

    def test_serial_when_concurrency_clamped(self):
        obs = _ConcurrencyObserver(BytesReadSource(b"hello world"), hold_seconds=0.002)
        f = Fetcher(obs, max_concurrency=0)  # clamps to 1
        f.execute([ReadOp(0, 5), ReadOp(6, 5)])
        assert obs.max_seen == 1, "max_concurrency=0 must clamp to serial (1)"


class _FailingReadAt:
    """Succeeds on the first call, raises on subsequent ones. Concurrency-safe."""

    def __init__(self, size: int, exc: BaseException) -> None:
        self._size = size
        self._exc = exc
        self._lock = threading.Lock()
        self._calls = 0

    def size(self) -> int:
        return self._size

    def read_at(self, offset: int, n: int) -> bytes:
        with self._lock:
            self._calls += 1
            calls_now = self._calls
        if calls_now >= 2:
            raise self._exc
        return b"\x00" * n


class TestFetcherErrors:
    def test_propagates_error_from_underlying_source(self):
        f = Fetcher(_FailingReadAt(size=1024, exc=IOError("boom")), max_concurrency=1)
        with pytest.raises(IOError, match="boom"):
            f.execute([ReadOp(0, 4), ReadOp(4, 4)])

    def test_short_read_raises(self):
        class _Short:
            def size(self):
                return 100

            def read_at(self, off, n):
                return b"\x00" * (n - 1)  # always 1 byte short

        f = Fetcher(_Short(), max_concurrency=1)
        with pytest.raises(IOError, match="short read"):
            f.execute([ReadOp(0, 10)])


# ---------------------------------------------------------------------------
# LoadedBytes
# ---------------------------------------------------------------------------
class TestLoadedBytes:
    def test_get_returns_correct_bytes_at_registered_offsets(self):
        # Three input ranges. After planning, two live in op 0 (offset 0, len 30)
        # and one in op 1 (offset 100, len 20).
        ranges = [Range(0, 10), Range(100, 20), Range(20, 10)]
        locs = [
            RangeLocation(op_index=0, in_op_off=0, length=10),
            RangeLocation(op_index=1, in_op_off=0, length=20),
            RangeLocation(op_index=0, in_op_off=20, length=10),
        ]
        bufs = [
            b"0123456789AAAAAAAAAA9876543210",
            b"ZZZZZZZZZZYYYYYYYYYY",
        ]
        lb = LoadedBytes(ranges, locs, bufs)
        assert bytes(lb.get(0)) == b"0123456789"
        assert bytes(lb.get(20)) == b"9876543210"
        assert bytes(lb.get(100)) == b"ZZZZZZZZZZYYYYYYYYYY"

    def test_get_unregistered_offset_returns_none(self):
        lb = LoadedBytes([Range(0, 5)], [RangeLocation(0, 0, 5)], [b"hello"])
        assert lb.get(999) is None
