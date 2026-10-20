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

"""zstd compress/decompress wrapper. Mirrors go/internal/compress/compress.go.

EncodeAll/DecodeAll-equivalent calls. zstandard ZstdCompressor/ZstdDecompressor
instances cannot be used concurrently, so each thread lazily creates and reuses
its own instances.

The Go and TS encoders sometimes omit the original content size from the zstd
frame header, so the simple `decompressor.decompress(data)` overload (which
requires content-size to preallocate) fails on cross-language reads. We use
`stream_reader` instead, which streams arbitrary-length output.
"""
from __future__ import annotations

import io
import threading

import zstandard

_contexts = threading.local()


def compress(data: bytes) -> bytes:
    compressor = getattr(_contexts, "compressor", None)
    if compressor is None:
        compressor = _contexts.compressor = zstandard.ZstdCompressor()
    return compressor.compress(data)


def decompress(data: bytes) -> bytes:
    decompressor = getattr(_contexts, "decompressor", None)
    if decompressor is None:
        decompressor = _contexts.decompressor = zstandard.ZstdDecompressor()
    with decompressor.stream_reader(io.BytesIO(data)) as r:
        return r.read()
