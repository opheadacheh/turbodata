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

"""Unit tests for turbodata._compress. Mirrors go/internal/compress/compress_test.go."""
from __future__ import annotations

import pytest
import zstandard

from turbodata._compress import compress, decompress


class TestRoundtrip:
    @pytest.mark.parametrize(
        "payload",
        [
            b"",
            b"hello compression",
            bytes([0x00, 0x01, 0x00, 0xFF, 0x10, 0x00, 0x7F]),
            b"x" * 4096,
            b"\x00" * 65536,
        ],
        ids=["empty", "small_text", "binary_with_zeros", "kbyte_repeat", "all_zeros_64k"],
    )
    def test_compress_decompress(self, payload):
        assert decompress(compress(payload)) == payload


class TestInvalidInput:
    def test_decompress_invalid_data_raises(self):
        with pytest.raises(zstandard.ZstdError):
            decompress(b"this is not zstd data")

    def test_decompress_empty_returns_empty(self):
        # Our wrapper uses stream_reader which treats an empty frame stream
        # as zero output rather than raising. This is acceptable because the
        # codec layer never calls decompress with empty input in practice;
        # the index chunk size always covers at least one zstd frame.
        assert decompress(b"") == b""


class TestCrossLanguageFrames:
    """Go's zstandard encoder sometimes omits the original content size from
    the frame header, which breaks the simple `decompressor.decompress(data)`
    overload. Our wrapper uses stream_reader to handle either case. Verify
    both shapes round-trip."""

    def test_handles_no_content_size_frame(self):
        cctx = zstandard.ZstdCompressor(write_content_size=False)
        payload = b"frame with no content size header" * 10
        frame = cctx.compress(payload)
        assert decompress(frame) == payload

    def test_handles_with_content_size_frame(self):
        cctx = zstandard.ZstdCompressor(write_content_size=True)
        payload = b"frame with content size header" * 10
        frame = cctx.compress(payload)
        assert decompress(frame) == payload
