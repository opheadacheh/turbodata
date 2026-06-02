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

"""Read sources for the turbodata Reader.

A ReadSource exposes:
  size()                -> int (file size in bytes)
  read_at(offset, n)    -> bytes (read n bytes starting at offset)

read_at MUST be safe to call from multiple threads concurrently. The
cost-aware reader path issues many concurrent reads from a ThreadPoolExecutor.

Two built-in sources are provided: FileReadSource (local file, opened r+b /
rb) and BytesReadSource (in-memory bytes). Users can implement their own
source for HTTP range reads, GCS, S3, etc.
"""
from __future__ import annotations

import os
import threading
from typing import Protocol, runtime_checkable


@runtime_checkable
class ReadSource(Protocol):
    def size(self) -> int: ...
    def read_at(self, offset: int, n: int) -> bytes: ...


class BytesReadSource:
    """In-memory bytes source. Useful for tests and small files."""

    def __init__(self, data: bytes) -> None:
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise TypeError("BytesReadSource expects bytes-like")
        self._data: bytes = bytes(data)

    def size(self) -> int:
        return len(self._data)

    def read_at(self, offset: int, n: int) -> bytes:
        if offset < 0 or n < 0 or offset + n > len(self._data):
            raise ValueError(
                f"BytesReadSource: out-of-range read offset={offset} n={n} size={len(self._data)}"
            )
        return self._data[offset : offset + n]


class FileReadSource:
    """Local-file source. Wraps os.pread which is atomic across threads on Linux/macOS.

    Falls back to a lock + seek/read on platforms that don't expose os.pread
    (e.g. Windows). Construct with either a path or an already-open binary
    file descriptor.
    """

    def __init__(self, path_or_fd) -> None:
        if isinstance(path_or_fd, (str, bytes, os.PathLike)):
            self._fd = os.open(path_or_fd, os.O_RDONLY)
            self._owns_fd = True
        elif isinstance(path_or_fd, int):
            self._fd = path_or_fd
            self._owns_fd = False
        elif hasattr(path_or_fd, "fileno"):
            self._fd = path_or_fd.fileno()
            self._owns_fd = False
        else:
            raise TypeError(
                "FileReadSource expects a path, file descriptor, or file object with fileno()"
            )
        st = os.fstat(self._fd)
        self._size = st.st_size

        self._has_pread = hasattr(os, "pread")
        self._lock = None if self._has_pread else threading.Lock()

    def size(self) -> int:
        return self._size

    def read_at(self, offset: int, n: int) -> bytes:
        if n == 0:
            return b""
        if self._has_pread:
            return self._pread_full(offset, n)
        assert self._lock is not None
        with self._lock:
            os.lseek(self._fd, offset, os.SEEK_SET)
            return self._read_full(n)

    def _pread_full(self, offset: int, n: int) -> bytes:
        out = bytearray(n)
        view = memoryview(out)
        got = 0
        while got < n:
            chunk = os.pread(self._fd, n - got, offset + got)
            if not chunk:
                raise EOFError(f"short pread: wanted {n} got {got}")
            view[got : got + len(chunk)] = chunk
            got += len(chunk)
        return bytes(out)

    def _read_full(self, n: int) -> bytes:
        out = bytearray(n)
        view = memoryview(out)
        got = 0
        while got < n:
            chunk = os.read(self._fd, n - got)
            if not chunk:
                raise EOFError(f"short read: wanted {n} got {got}")
            view[got : got + len(chunk)] = chunk
            got += len(chunk)
        return bytes(out)

    def close(self) -> None:
        if self._owns_fd and self._fd >= 0:
            os.close(self._fd)
            self._fd = -1

    def __enter__(self) -> "FileReadSource":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass
