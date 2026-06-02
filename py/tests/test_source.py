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

"""Unit tests for turbodata.source (FileReadSource, BytesReadSource)."""
from __future__ import annotations

import os
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from turbodata.source import BytesReadSource, FileReadSource


# ---------------------------------------------------------------------------
# BytesReadSource
# ---------------------------------------------------------------------------
class TestBytesReadSource:
    def test_size(self):
        assert BytesReadSource(b"hello").size() == 5

    def test_accepts_bytes_bytearray_memoryview(self):
        for src in (
            BytesReadSource(b"hello"),
            BytesReadSource(bytearray(b"hello")),
            BytesReadSource(memoryview(b"hello")),
        ):
            assert src.size() == 5
            assert src.read_at(0, 5) == b"hello"

    def test_rejects_non_bytes(self):
        with pytest.raises(TypeError):
            BytesReadSource("not bytes")

    def test_read_at_returns_exact_slice(self):
        src = BytesReadSource(b"0123456789")
        assert src.read_at(0, 5) == b"01234"
        assert src.read_at(5, 5) == b"56789"
        assert src.read_at(2, 6) == b"234567"
        assert src.read_at(0, 0) == b""

    @pytest.mark.parametrize(
        "off, n",
        [
            (-1, 5),
            (0, -1),
            (5, 10),
            (10, 1),
            (11, 0),
        ],
    )
    def test_out_of_range_raises(self, off, n):
        src = BytesReadSource(b"0123456789")
        with pytest.raises(ValueError):
            src.read_at(off, n)


# ---------------------------------------------------------------------------
# FileReadSource
# ---------------------------------------------------------------------------
@pytest.fixture
def tmp_file_with_bytes():
    data = bytes(range(256)) * 16  # 4 KiB of deterministic bytes
    with tempfile.NamedTemporaryFile(delete=False) as f:
        f.write(data)
        path = f.name
    yield path, data
    os.unlink(path)


class TestFileReadSource:
    def test_size(self, tmp_file_with_bytes):
        path, data = tmp_file_with_bytes
        with FileReadSource(path) as src:
            assert src.size() == len(data)

    def test_read_at_returns_correct_bytes(self, tmp_file_with_bytes):
        path, data = tmp_file_with_bytes
        with FileReadSource(path) as src:
            assert src.read_at(0, 10) == data[0:10]
            assert src.read_at(100, 50) == data[100:150]
            assert src.read_at(len(data) - 5, 5) == data[-5:]

    def test_read_at_zero_length(self, tmp_file_with_bytes):
        path, _ = tmp_file_with_bytes
        with FileReadSource(path) as src:
            assert src.read_at(0, 0) == b""

    def test_concurrent_read_at_returns_correct_bytes(self, tmp_file_with_bytes):
        """Concurrent reads from many threads must each return the exact bytes
        for their offset. This guards against a missing lock or seek/read race."""
        path, data = tmp_file_with_bytes
        with FileReadSource(path) as src:
            requests = [(i * 16, 32) for i in range(len(data) // 16 - 1)]

            def _do(req):
                off, n = req
                return off, src.read_at(off, n)

            with ThreadPoolExecutor(max_workers=8) as pool:
                results = list(pool.map(_do, requests))

            for off, got in results:
                assert got == data[off : off + 32], f"mismatch at offset {off}"

    def test_accepts_file_object_with_fileno(self, tmp_file_with_bytes):
        path, data = tmp_file_with_bytes
        with open(path, "rb") as f:
            src = FileReadSource(f)
            assert src.size() == len(data)
            assert src.read_at(0, 10) == data[:10]
            # Don't close src — it doesn't own the fd.

    def test_close_is_idempotent(self, tmp_file_with_bytes):
        path, _ = tmp_file_with_bytes
        src = FileReadSource(path)
        src.close()
        src.close()  # second close is a no-op

    def test_rejects_unsupported_type(self):
        with pytest.raises(TypeError):
            FileReadSource(object())
