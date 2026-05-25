"""I/O range planning, concurrent fetching, and loaded-bytes lookup.

Ports go/internal/iorange/{planner,fetcher,loaded}.go.

  Range:       caller-requested atomic byte range
  ReadOp:      one read_at call the fetcher will execute, covering one or more
               coalesced Ranges (possibly with split for large ops)
  plan():      groups Ranges into ReadOps per a strategy's coalesce_gap and
               split_threshold
  Fetcher:     executes ReadOps concurrently against a ReadSource via a
               ThreadPoolExecutor
  LoadedBytes: indexed lookup back to the original Range offsets after fetch
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, FIRST_EXCEPTION, wait
from dataclasses import dataclass
from typing import List, Tuple


@dataclass(frozen=True)
class Range:
    offset: int
    length: int


@dataclass(frozen=True)
class ReadOp:
    offset: int
    length: int


@dataclass(frozen=True)
class RangeLocation:
    op_index: int
    in_op_off: int
    length: int


def plan(
    ranges: List[Range], coalesce_gap: int, split_threshold: int
) -> Tuple[List[ReadOp], List[RangeLocation]]:
    """Group ranges into ReadOps.

    Precondition: ranges must be non-decreasing in offset.

    Algorithm:
      1) Coalesce: merge adjacent ranges with gap < coalesce_gap.
      2) Split: if a merged op exceeds split_threshold, slice it at internal
         range boundaries via greedy packing. Atomic units are never split,
         so a single range > split_threshold becomes one oversize op.
    """
    locations: List[RangeLocation] = [None] * len(ranges)  # type: ignore[list-item]
    if not ranges:
        return [], locations

    # Coalesce pass.
    groups: List[Tuple[int, int, List[int]]] = []  # (offset, end_exclusive, members)
    for idx, r in enumerate(ranges):
        if not groups:
            groups.append((r.offset, r.offset + r.length, [idx]))
            continue
        cur_off, cur_end, cur_members = groups[-1]
        gap = r.offset - cur_end
        if gap >= 0 and gap < coalesce_gap:
            new_end = cur_end if r.offset + r.length <= cur_end else r.offset + r.length
            cur_members.append(idx)
            groups[-1] = (cur_off, new_end, cur_members)
            continue
        groups.append((r.offset, r.offset + r.length, [idx]))

    ops: List[ReadOp] = []
    split_disabled = split_threshold <= 0
    for g_off, g_end, members in groups:
        if split_disabled or (g_end - g_off) <= split_threshold:
            op_index = len(ops)
            ops.append(ReadOp(offset=g_off, length=g_end - g_off))
            for mi in members:
                locations[mi] = RangeLocation(
                    op_index=op_index,
                    in_op_off=ranges[mi].offset - g_off,
                    length=ranges[mi].length,
                )
            continue

        # Greedy split at range boundaries.
        first = members[0]
        op_start_idx = 0
        op_start_off = ranges[first].offset
        op_end_off = ranges[first].offset + ranges[first].length

        def _flush(last_member_idx: int) -> None:
            op_index = len(ops)
            ops.append(ReadOp(offset=op_start_off, length=op_end_off - op_start_off))
            for k in range(op_start_idx, last_member_idx + 1):
                mi = members[k]
                locations[mi] = RangeLocation(
                    op_index=op_index,
                    in_op_off=ranges[mi].offset - op_start_off,
                    length=ranges[mi].length,
                )

        for k in range(1, len(members)):
            mi = members[k]
            next_end = ranges[mi].offset + ranges[mi].length
            if next_end - op_start_off > split_threshold:
                _flush(k - 1)
                op_start_idx = k
                op_start_off = ranges[mi].offset
                op_end_off = next_end
                continue
            op_end_off = next_end
        _flush(len(members) - 1)

    return ops, locations


class Fetcher:
    """Executes ReadOps against a ReadSource. Threads share read_at.

    The ReadSource's read_at must be safe for concurrent calls. The standard
    FileReadSource uses os.pread (thread-safe on POSIX) or a lock fallback.
    """

    def __init__(self, source, max_concurrency: int) -> None:
        if max_concurrency <= 0:
            max_concurrency = 1
        self._source = source
        self._workers = max_concurrency

    def execute(self, ops: List[ReadOp]) -> List[bytes]:
        if not ops:
            return []

        def _read_one(i: int, op: ReadOp) -> bytes:
            data = self._source.read_at(op.offset, op.length)
            if len(data) != op.length:
                raise IOError(
                    f"short read: op {i} offset={op.offset} wanted={op.length} got={len(data)}"
                )
            return data

        if self._workers == 1 or len(ops) == 1:
            return [_read_one(i, op) for i, op in enumerate(ops)]

        bufs: List[bytes] = [b""] * len(ops)

        def _do(i: int, op: ReadOp) -> None:
            bufs[i] = _read_one(i, op)

        with ThreadPoolExecutor(max_workers=self._workers) as pool:
            futures = [pool.submit(_do, i, op) for i, op in enumerate(ops)]
            done, not_done = wait(futures, return_when=FIRST_EXCEPTION)
            for f in done:
                exc = f.exception()
                if exc is not None:
                    for nd in not_done:
                        nd.cancel()
                    raise exc
            for f in not_done:
                exc = f.exception()
                if exc is not None:
                    raise exc
        return bufs


class LoadedBytes:
    """Indexed lookup back to original Range offsets after the fetcher returns.

    Get(offset) returns the bytes for the Range originally registered at that
    offset, or None if no range with that offset was registered. The returned
    bytes alias the underlying op buffer; callers must not mutate.
    """

    def __init__(
        self,
        ranges: List[Range],
        locations: List[RangeLocation],
        bufs: List[bytes],
    ) -> None:
        self._bufs = bufs
        self._index = {}
        for r, loc in zip(ranges, locations):
            self._index[r.offset] = (loc.op_index, loc.in_op_off, loc.length)

    def get(self, offset: int):
        ent = self._index.get(offset)
        if ent is None:
            return None
        buf_idx, in_off, length = ent
        return self._bufs[buf_idx][in_off : in_off + length]
