"""Chunk thresholding configuration. Mirrors go/chunk.go.

The writer rolls a new chunk whenever the configured threshold is reached:
  SIZE      - chunk's accumulated message bytes >= size
  DURATION  - timestamp - chunk_start_timestamp >= duration
  COUNT     - number of messages in chunk >= count
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum


class ChunkThresholdMode(IntEnum):
    SIZE = 0
    DURATION = 1
    COUNT = 2


@dataclass
class ChunkConfig:
    mode: ChunkThresholdMode = ChunkThresholdMode.SIZE
    size: int = 0
    duration: int = 0
    count: int = 0
