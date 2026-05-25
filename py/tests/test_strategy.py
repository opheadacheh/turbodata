"""Unit tests for turbodata.strategy. Mirrors go/readstrategy/strategy_test.go."""
from __future__ import annotations

import pytest

from turbodata.strategy import (
    ReadStrategy,
    strategy_for_blended,
    strategy_for_latency,
    strategy_for_money,
)


_MAX_I64 = (1 << 63) - 1


class TestStrategyForLatency:
    def test_typical_values(self):
        # 50 ms RTT, 100 MiB/s per stream -> BDP = 0.05 * 100*1024*1024 ≈ 5 MiB.
        s = strategy_for_latency(0.050, 100 * 1024 * 1024, 8)
        want_bdp = int(0.050 * float(100 * 1024 * 1024))
        assert s.coalesce_gap == want_bdp
        assert s.split_threshold == want_bdp
        assert s.max_concurrency == 8

    def test_zero_product_clamps_to_one(self):
        # Degenerate case: rtt=0 -> product=0 -> clamp to 1 so we still have a
        # sane bound.
        s = strategy_for_latency(0, 100, 1)
        assert s.coalesce_gap == 1
        assert s.split_threshold == 1


class TestStrategyForMoney:
    def test_typical_values(self):
        # $0.0004 per request, $0.00000009 per byte -> gap ≈ 4444 bytes.
        req_price, byte_price = 0.0004, 0.00000009
        s = strategy_for_money(req_price, byte_price, 4)
        assert s.coalesce_gap == int(req_price / byte_price)
        assert s.split_threshold == _MAX_I64
        assert s.max_concurrency == 4

    def test_free_egress_always_coalesces(self):
        # byte_price = 0 means egress is free, so we should never split a
        # request to save bytes.
        s = strategy_for_money(0.0004, 0, 4)
        assert s.coalesce_gap == _MAX_I64


class TestStrategyForBlended:
    def test_combines_money_gap_with_time_split(self):
        rtt_seconds = 0.100
        bw = 50 * 1024 * 1024
        req_price, byte_price = 0.0004, 0.00000009
        s = strategy_for_blended(req_price, byte_price, rtt_seconds, bw, 16)
        assert s.coalesce_gap == int(req_price / byte_price)
        assert s.split_threshold == int(rtt_seconds * float(bw))
        assert s.max_concurrency == 16

    def test_zero_split_time_disables_splitting(self):
        s = strategy_for_blended(0.0004, 0.00000009, 0, 0, 1)
        assert s.split_threshold == _MAX_I64

    def test_free_egress(self):
        s = strategy_for_blended(0.0004, 0, 0.1, 1024, 1)
        assert s.coalesce_gap == _MAX_I64


class TestReadStrategyDefaults:
    def test_default_construction(self):
        s = ReadStrategy()
        assert s.coalesce_gap == 0
        assert s.split_threshold == 0
        assert s.max_concurrency == 1

    def test_explicit_construction(self):
        s = ReadStrategy(coalesce_gap=100, split_threshold=200, max_concurrency=4)
        assert s.coalesce_gap == 100
        assert s.split_threshold == 200
        assert s.max_concurrency == 4
