"""zstd compress/decompress wrapper. Mirrors go/internal/compress/compress.go.

EncodeAll/DecodeAll-equivalent calls. zstandard ZstdCompressor/ZstdDecompressor
instances are safe to share across threads, so we keep one of each at module
level.

The Go and TS encoders sometimes omit the original content size from the zstd
frame header, so the simple `decompressor.decompress(data)` overload (which
requires content-size to preallocate) fails on cross-language reads. We use
`stream_reader` instead, which streams arbitrary-length output.
"""
from __future__ import annotations

import io

import zstandard

_compressor = zstandard.ZstdCompressor()
_decompressor = zstandard.ZstdDecompressor()


def compress(data: bytes) -> bytes:
    return _compressor.compress(data)


def decompress(data: bytes) -> bytes:
    with _decompressor.stream_reader(io.BytesIO(data)) as r:
        return r.read()
