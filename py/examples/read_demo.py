"""Example: read a turbodata file. Mirrors go/examples/read/main.go.

Run after write_demo.py:
    python examples/write_demo.py
    python examples/read_demo.py
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from turbodata import (
    FileReadSource,
    Order,
    Reader,
    ReadStrategy,
    strategy_for_blended,
    strategy_for_latency,
    strategy_for_money,
)


IN_PATH = os.path.join(os.path.dirname(__file__), "demo.td")


def section(title: str) -> None:
    print(f"\n--- {title} ---")


def snippet(b: bytes, max_len: int = 24) -> str:
    if len(b) <= max_len:
        return repr(b)
    return repr(b[:max_len]) + "..."


def iterate(it, limit: int = 0) -> None:
    total = 0
    for msg in it:
        total += 1
        if limit == 0 or total <= limit:
            print(
                f"  ts={msg.timestamp:>4} topic={msg.topic_name:<12} "
                f"data={snippet(msg.data)}"
            )
    if limit > 0 and total > limit:
        print(f"  ... ({total - limit} more messages)")
    print(f"  total: {total} messages")


def demo_summary(reader: Reader) -> None:
    section("summary only (no message I/O)")
    summary = reader.summary()
    for ti in summary.topics_infos:
        names = [tm.name for tm in ti.topic_metadatas]
        print(
            f"  group: topics={names} chunks={len(ti.index_chunk_info_list)} "
            f"totalLen={ti.total_len}"
        )


def demo_read_all(reader: Reader) -> None:
    section("read all messages (defaults)")
    iterate(reader.read_messages(), limit=5)


def demo_topic_filter(reader: Reader) -> None:
    section("only /imu and /gps")
    iterate(reader.read_messages(topic_names=["/imu", "/gps"]))


def demo_time_range(reader: Reader) -> None:
    section("time range [300, 700]")
    iterate(reader.read_messages(start_timestamp=300, end_timestamp=700))


def demo_reverse_order(reader: Reader) -> None:
    section("reverse time order (last 5)")
    iterate(reader.read_messages(order=Order.REVERSE_TIME), limit=5)


def demo_tail_prefetch(reader: Reader) -> None:
    section("tail prefetch hint of 64 KiB")
    iterate(reader.read_messages(tail_prefetch=64 * 1024), limit=3)


def demo_strategy_default(reader: Reader) -> None:
    section("cost-aware path, hand-tuned strategy")
    strat = ReadStrategy(
        coalesce_gap=1 << 20,
        split_threshold=4 << 20,
        max_concurrency=8,
    )
    iterate(reader.read_messages(strategy=strat))


def demo_strategy_latency(reader: Reader) -> None:
    section("strategy: minimize wall-clock (object storage)")
    strat = strategy_for_latency(
        rtt_seconds=0.020,
        per_stream_bw=125 * 1024 * 1024,
        concurrency=16,
    )
    iterate(reader.read_messages(strategy=strat))


def demo_strategy_money(reader: Reader) -> None:
    section("strategy: minimize spend (paid-request storage)")
    strat = strategy_for_money(
        req_price=0.0000004,
        byte_price=0.00000000009,
        concurrency=8,
    )
    iterate(reader.read_messages(strategy=strat))


def demo_strategy_blended(reader: Reader) -> None:
    section("strategy: blended cost + latency cap")
    strat = strategy_for_blended(
        req_price=0.0000004,
        byte_price=0.00000000009,
        max_read_seconds=0.100,
        per_stream_bw=125 * 1024 * 1024,
        concurrency=8,
    )
    iterate(reader.read_messages(strategy=strat))


def main() -> None:
    with FileReadSource(IN_PATH) as src:
        reader = Reader(src)
        demo_summary(reader)
        demo_read_all(reader)
        demo_topic_filter(reader)
        demo_time_range(reader)
        demo_reverse_order(reader)
        demo_tail_prefetch(reader)
        demo_strategy_default(reader)
        demo_strategy_latency(reader)
        demo_strategy_money(reader)
        demo_strategy_blended(reader)


if __name__ == "__main__":
    main()
