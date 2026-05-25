"""ReadStrategy and helper constructors. Mirrors go/readstrategy/strategy.go.

The cost-aware reader path coalesces nearby byte ranges into single reads and
optionally splits large coalesced reads into parallel sub-reads. The three
knobs collapse any cost model into operational settings:

  coalesce_gap     - merge two adjacent ranges if gap_bytes < coalesce_gap
  split_threshold  - slice a merged op larger than this at range boundaries
  max_concurrency  - cap on in-flight read_at calls

Helper constructors translate common cost models (latency, money, blended).
"""
from __future__ import annotations

from dataclasses import dataclass

_MAX_I64 = (1 << 63) - 1


@dataclass
class ReadStrategy:
    coalesce_gap: int = 0
    split_threshold: int = 0
    max_concurrency: int = 1


def strategy_for_latency(
    rtt_seconds: float, per_stream_bw: int, concurrency: int
) -> ReadStrategy:
    """Minimize wall-clock by sizing reads to the bandwidth-latency product.

    rtt_seconds: per-request round-trip cost (seconds).
    per_stream_bw: steady-state bandwidth per concurrent stream (bytes/sec).
    concurrency: max in-flight read_at calls.
    """
    bdp = int(rtt_seconds * float(per_stream_bw))
    if bdp <= 0:
        bdp = 1
    return ReadStrategy(coalesce_gap=bdp, split_threshold=bdp, max_concurrency=concurrency)


def strategy_for_money(
    req_price: float, byte_price: float, concurrency: int
) -> ReadStrategy:
    """Minimize total spend; aggressively coalesce, never split.

    req_price: per-request cost (currency unit).
    byte_price: per-byte cost (same currency unit).
    concurrency: max in-flight read_at calls.
    """
    if byte_price > 0:
        gap = int(req_price / byte_price)
    else:
        gap = _MAX_I64
    return ReadStrategy(coalesce_gap=gap, split_threshold=_MAX_I64, max_concurrency=concurrency)


def strategy_for_blended(
    req_price: float,
    byte_price: float,
    max_read_seconds: float,
    per_stream_bw: int,
    concurrency: int,
) -> ReadStrategy:
    """Money-driven coalescing capped by max_read_seconds * per_stream_bw splits."""
    if byte_price > 0:
        gap = int(req_price / byte_price)
    else:
        gap = _MAX_I64
    split = int(max_read_seconds * float(per_stream_bw))
    if split <= 0:
        split = _MAX_I64
    return ReadStrategy(coalesce_gap=gap, split_threshold=split, max_concurrency=concurrency)
