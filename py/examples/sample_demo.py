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

"""Example: sample messages at specific timestamps. Mirrors go/examples/sample.

Run after write_demo.py:
    python examples/write_demo.py
    python examples/sample_demo.py
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from turbodata import (
    FileReadSource,
    ReadStrategy,
    Reader,
    SampleQuery,
    lin_space_timestamps,
)


IN_PATH = os.path.join(os.path.dirname(__file__), "demo.td")


def section(title: str) -> None:
    print(f"\n--- {title} ---")


def snippet(b: bytes, max_len: int = 24) -> str:
    if len(b) <= max_len:
        return repr(b)
    return repr(b[:max_len]) + "..."


def print_results(topic, queried_at, results):
    for q, r in zip(queried_at, results):
        if not r.found:
            print(f"  {topic}@T={q:<4}  (no message at or before T)")
            continue
        print(f"  {topic}@T={q:<4}  -> ts={r.timestamp:<4} data={snippet(r.data)}")


def demo_single_topic(reader: Reader) -> None:
    section("single topic, three timestamps")
    out = reader.sample([SampleQuery(topic="/imu", timestamps=[200, 500, 1000])])
    print_results("/imu", [200, 500, 1000], out[0])


def demo_floor(reader: Reader) -> None:
    section("floor semantics: T=250 between /imu@200 and /imu@300")
    out = reader.sample([SampleQuery(topic="/imu", timestamps=[250])])
    print_results("/imu", [250], out[0])


def demo_lin_space(reader: Reader) -> None:
    section("lin_space_timestamps: 6 samples at stride 200 from T=100")
    ts = lin_space_timestamps(100, 200, 6)
    out = reader.sample([SampleQuery(topic="/imu", timestamps=ts)])
    print_results("/imu", ts, out[0])


def demo_multi_topic(reader: Reader) -> None:
    section("multi-topic in one Sample call")
    out = reader.sample(
        [
            SampleQuery(topic="/imu", timestamps=[350, 750]),
            SampleQuery(topic="/cam/front", timestamps=[350, 750]),
            SampleQuery(topic="/gps", timestamps=[350, 750]),
        ]
    )
    print_results("/imu", [350, 750], out[0])
    print_results("/cam/front", [350, 750], out[1])
    print_results("/gps", [350, 750], out[2])


def demo_edge_cases(reader: Reader) -> None:
    section("edge cases: before-first and after-last")
    out = reader.sample([SampleQuery(topic="/imu", timestamps=[50, 9999])])
    print_results("/imu", [50, 9999], out[0])


def demo_strategy(reader: Reader) -> None:
    section("custom sample strategy")
    out = reader.sample(
        [SampleQuery(topic="/imu", timestamps=[200, 500, 800])],
        strategy=ReadStrategy(
            coalesce_gap=256 * 1024,
            split_threshold=2 * 1024 * 1024,
            max_concurrency=4,
        ),
    )
    print_results("/imu", [200, 500, 800], out[0])


def main() -> None:
    with FileReadSource(IN_PATH) as src:
        reader = Reader(src)
        demo_single_topic(reader)
        demo_floor(reader)
        demo_lin_space(reader)
        demo_multi_topic(reader)
        demo_edge_cases(reader)
        demo_strategy(reader)


if __name__ == "__main__":
    main()
