"""Example: write a turbodata file. Mirrors go/examples/write/main.go.

Produces examples/demo.td with three topic groups:

  Group A  /imu                 uncompressed, size-based default chunking
  Group B  /cam/front           compressed,   count=3 chunks
  Group C  /odom, /gps          compressed,   count=4 chunks (two-topic group)

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


def main() -> None:
    with open(OUT_PATH, "wb") as f, Writer(f) as w:
        write_imu_group(w)
        write_cam_group(w)
        write_odom_gps_group(w)

    size = os.path.getsize(OUT_PATH)
    print(f"wrote {OUT_PATH} ({size} bytes)")


if __name__ == "__main__":
    main()
