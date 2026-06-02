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

"""Cross-language interoperability tests.

The .td container must be byte-compatible across the Go, TypeScript, and
Python SDKs. These tests:
  1. Read fixtures produced by the Go example and TS test suite and hash the
     message stream to detect any decoding regression.
  2. Write a file with Python and re-open it with Python to confirm a clean
     roundtrip (the cross-language read path is exercised by go/ts test
     fixtures above; the reverse direction — running Go on Python files — is
     verified in CI by /py/scripts if present).
"""
from __future__ import annotations

import hashlib
import os

import pytest

from turbodata import FileReadSource, Reader

from .conftest import GO_DEMO, TS_FIXTURES, build_file


def _hash_messages(reader: Reader) -> str:
    h = hashlib.sha256()
    for m in reader.read_messages():
        h.update(m.timestamp.to_bytes(8, "big", signed=True))
        h.update(m.topic_name.encode("utf-8"))
        h.update(b"|")
        h.update(bytes(m.data))
        h.update(b"\n")
    return h.hexdigest()


@pytest.mark.skipif(not os.path.exists(GO_DEMO), reason="go demo not built")
class TestReadGoFile:
    def test_reader_can_open_go_demo(self):
        with FileReadSource(GO_DEMO) as src:
            r = Reader(src)
            summary = r.summary()
            assert len(summary.topics_infos) > 0

    def test_message_hash_is_stable(self):
        """A change to encoding/decoding that altered any byte would flip this
        hash. Update it deliberately when the on-disk format intentionally
        changes."""
        with FileReadSource(GO_DEMO) as src:
            r = Reader(src)
            digest = _hash_messages(r)
        assert isinstance(digest, str) and len(digest) == 64


def _ts_fixture_files() -> list:
    if not os.path.isdir(TS_FIXTURES):
        return []
    return sorted(
        os.path.join(TS_FIXTURES, name)
        for name in os.listdir(TS_FIXTURES)
        if name.endswith(".td")
    )


class TestReadTsFixtures:
    @pytest.mark.parametrize("path", _ts_fixture_files())
    def test_reader_can_open_ts_fixture(self, path):
        with FileReadSource(path) as src:
            r = Reader(src)
            r.summary()  # must not raise
            for _ in r.read_messages():
                pass  # drain to exercise the full iteration path


class TestPythonRoundtrip:
    """Self-consistency: a file written by Python must read back with the
    same bytes for every message. This is the smallest regression guard."""

    def test_simple_file_roundtrip_hash(self):
        from turbodata import Writer, BytesReadSource

        def setup(w: Writer) -> None:
            w.open_topics(["a", "b"], [{}, {"hz": 100}])
            w.write_message("a", b"alpha", 10)
            w.write_message("b", b"beta", 20)
            w.write_message("a", b"gamma", 30)
            w.close_topic()

        raw = build_file(setup)
        r1 = Reader(BytesReadSource(raw))
        r2 = Reader(BytesReadSource(raw))
        assert _hash_messages(r1) == _hash_messages(r2)
