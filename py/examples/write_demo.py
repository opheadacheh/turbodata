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

"""Example: write a turbodata file. Mirrors go/examples/write/main.go.

Produces examples/demo.td with four topic groups:

  Group A  /imu                 uncompressed, size-based default chunking
  Group B  /cam/front           compressed,   count=3 chunks
  Group C  /odom, /gps          compressed,   count=4 chunks (two-topic group)
  Group D  /cam/h264            video,        two GOPs (key-frame gated chunking)

Run from the py/ directory:
    python examples/write_demo.py
Then run read_demo.py / sample_demo.py against the produced file.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from turbodata import ChunkConfig, ChunkThresholdMode, Writer


OUT_PATH = os.path.join(os.path.dirname(__file__), "demo.td")


def write_imu_group(w: Writer) -> None:
    """One uncompressed topic with default size-based chunking. Ten ~10-byte
    messages fit in one chunk."""
    w.open_topics(["/imu"], [{"hz": 100}])
    for i in range(1, 11):
        ts = i * 100
        w.write_message("/imu", f"imu-{i:03d}".encode(), ts)
    w.close_topic()


def write_cam_group(w: Writer) -> None:
    """One compressed topic, count-mode chunks of 3 (so 4 chunks total)."""
    cfg = ChunkConfig(mode=ChunkThresholdMode.COUNT, count=3)
    w.open_topics(
        ["/cam/front"],
        [{"hz": 10, "encoding": "jpeg"}],
        chunk_config=cfg,
        compression=True,
    )
    for i in range(10):
        ts = 150 + i * 100
        msg = f"cam-{i:03d}-".encode() + b"\x00" * 64
        w.write_message("/cam/front", msg, ts)
    w.close_topic()


def write_odom_gps_group(w: Writer) -> None:
    """Two topics in one compressed group, count-mode chunks of 4."""
    cfg = ChunkConfig(mode=ChunkThresholdMode.COUNT, count=4)
    w.open_topics(
        ["/odom", "/gps"],
        [
            {"hz": 20, "frame": "base_link"},
            {"hz": 1, "frame": "wgs84"},
        ],
        chunk_config=cfg,
        compression=True,
    )
    entries = [
        ("/odom", 120), ("/odom", 220), ("/gps", 300), ("/odom", 320),
        ("/odom", 420), ("/odom", 520), ("/gps", 600), ("/odom", 620),
        ("/odom", 720), ("/gps", 900),
    ]
    for i, (topic, ts) in enumerate(entries):
        msg = f"{topic[1:]}-{i:03d}".encode()
        w.write_message(topic, msg, ts)
    w.close_topic()


def write_video_group(w: Writer) -> None:
    """A single video topic. A video group must hold exactly one topic and must
    not be compressed; frames are written with write_video_message (NOT
    write_message), which carries the is_key_frame flag. The first frame MUST be
    a key frame. The writer gates chunk boundaries on key frames, so a GOP is
    never split across chunks. Two GOPs (A: ts 100..400, B: ts 500..800) let the
    read/sample demos show key-frame snap-back and GOP-prefix sampling."""
    w.open_topics(["/cam/h264"], [{"hz": 10, "codec": "h264"}], video=True)
    frames = [
        (100, True), (200, False), (300, False), (400, False),  # GOP A
        (500, True), (600, False), (700, False), (800, False),  # GOP B
    ]
    for ts, is_key in frames:
        kind = "K" if is_key else "P"
        # Pad the payload so it looks like a coded frame rather than a label.
        msg = f"{kind}{ts}-".encode() + b"\x00" * 32
        w.write_video_message("/cam/h264", msg, ts, is_key)
    w.close_topic()


def main() -> None:
    with open(OUT_PATH, "wb") as f, Writer(f) as w:
        write_imu_group(w)
        write_cam_group(w)
        write_odom_gps_group(w)
        write_video_group(w)

    size = os.path.getsize(OUT_PATH)
    print(f"wrote {OUT_PATH} ({size} bytes)")


if __name__ == "__main__":
    main()
