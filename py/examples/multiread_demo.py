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

"""Example: read several turbodata files as one time-ordered stream.

Mirrors go/examples/multiread/main.go. MultiReader merges per-file streams.
Topic-name collisions across files are resolved per Reader with topic_remap:
names meant to union share an exposed name; names meant to stay distinct are
remapped apart.

Builds two small files in memory and demonstrates:

    union read    - same topic in both files, time-split, merged as one
    split read    - one file's colliding topic remapped to a distinct name
    multi sample  - floor across files picks the latest floor (the union floor)

Run:
    python examples/multiread_demo.py
"""
from __future__ import annotations

import io
import os
import sys
from typing import Dict, List

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from turbodata import BytesReadSource, MultiReader, Reader, SampleQuery, Writer


def section(title: str) -> None:
    print(f"\n--- {title} ---")


def build_file(topics: Dict[str, List[int]]) -> bytes:
    """One topic per entry, one message per timestamp. Payloads are "<topic>@<ts>"."""
    buf = io.BytesIO()
    w = Writer(buf)
    for topic, tss in topics.items():
        w.open_topics([topic], [{}])
        for ts in tss:
            w.write_message(topic, f"{topic}@{ts}".encode(), ts)
        w.close_topic()
    w.close()
    return buf.getvalue()


def iterate(reader: MultiReader, **opts) -> None:
    for m in reader.read_messages(**opts):
        print(f"  ts={m.timestamp:4d} topic={m.topic_name:<8} data={m.data!r}")


def main() -> None:
    file_a = build_file({"/cam": [100, 300, 500], "/imu": [150, 350]})
    file_b = build_file({"/cam": [200, 400, 600], "/lidar": [250, 450]})

    # union: no remap. "/cam" appears in both files over disjoint time, so it
    # unions into a single time-ordered stream; "/imu" and "/lidar" augment it.
    section("union read (shared /cam, augmented by /imu + /lidar)")
    union = MultiReader(
        Reader(BytesReadSource(file_a)),
        Reader(BytesReadSource(file_b)),
    )
    iterate(union)

    # split: remap file B's "/cam" to "/cam_b" so the two cameras stay distinct.
    section("split read (file B /cam remapped to /cam_b)")
    split = MultiReader(
        Reader(BytesReadSource(file_a)),
        Reader(BytesReadSource(file_b), topic_remap={"/cam": "/cam_b"}),
    )
    iterate(split)

    # multi sample: floor of "/cam" across both files; each timestamp resolves
    # to the latest floor across the union of A and B.
    section("multi sample of /cam (latest floor across files)")
    sampler = MultiReader(
        Reader(BytesReadSource(file_a)),
        Reader(BytesReadSource(file_b)),
    )
    ts = [150, 350, 550, 700]
    out = sampler.sample([SampleQuery(topic="/cam", timestamps=ts)])
    for j, res in enumerate(out[0]):
        if not res.found:
            print(f"  T={ts[j]:4d} -> (no floor)")
            continue
        print(f"  T={ts[j]:4d} -> ts={res.timestamp:4d} data={res.data!r}")


if __name__ == "__main__":
    main()
