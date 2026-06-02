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

"""Unit tests for Reader topic_remap. Mirrors go/remap_test.go.

Covers exposed output names, topic_names filtering in exposed space, summary
reporting exposed names, collision detection, and sample() in exposed space.
"""
from __future__ import annotations

import pytest

from turbodata import (
    BytesReadSource,
    Reader,
    SampleQuery,
    SampleResult,
    TopicRemapCollisionError,
    Writer,
)

from .conftest import build_file


def _build_remap_reader(remap) -> Reader:
    """Two-topic file ("a", "b"); Reader over it with the given remap applied."""

    def setup(w: Writer) -> None:
        w.open_topics(["a", "b"], [{}, {}])
        w.write_message("a", b"a1", 10)
        w.write_message("b", b"b1", 20)
        w.write_message("a", b"a2", 30)
        w.write_message("b", b"b2", 40)
        w.close_topic()

    return Reader(BytesReadSource(build_file(setup)), topic_remap=remap)


def _collect(reader: Reader, **opts):
    return [
        (m.timestamp, m.topic_name, m.data)
        for m in reader.read_messages(copy=True, **opts)
    ]


def test_remap_output_names():
    r = _build_remap_reader({"a": "a_v2"})
    got = _collect(r)
    assert got == [
        (10, "a_v2", b"a1"),
        (20, "b", b"b1"),
        (30, "a_v2", b"a2"),
        (40, "b", b"b2"),
    ]


def test_remap_topic_names_filter():
    # Filtering must accept the exposed name and return only that topic's
    # messages, labeled with the exposed name.
    r = _build_remap_reader({"a": "a_v2"})
    got = _collect(r, topic_names=["a_v2"])
    assert got == [
        (10, "a_v2", b"a1"),
        (30, "a_v2", b"a2"),
    ]


def test_remap_summary_exposed_names():
    r = _build_remap_reader({"a": "a_v2"})
    names = {tm.name for ti in r.summary().topics_infos for tm in ti.topic_metadatas}
    assert "a_v2" in names
    assert "b" in names
    assert "a" not in names


def test_remap_collision_error():
    # Renaming "a" onto the untouched in-file name "b" collapses two topics
    # onto one exposed name and must error on first use.
    r = _build_remap_reader({"a": "b"})

    with pytest.raises(TopicRemapCollisionError) as exc:
        r.summary()
    assert "duplicate exposed name" in str(exc.value)

    with pytest.raises(TopicRemapCollisionError):
        list(r.read_messages())


def test_remap_sample():
    r = _build_remap_reader({"a": "a_v2"})
    out = r.sample([SampleQuery(topic="a_v2", timestamps=[10, 25, 30])])
    want = [
        SampleResult(True, 10, b"a1"),
        SampleResult(True, 10, b"a1"),
        SampleResult(True, 30, b"a2"),
    ]
    got = out[0]
    assert len(got) == len(want)
    for g, w in zip(got, want):
        assert (g.found, g.timestamp, g.data) == (w.found, w.timestamp, w.data)


def test_no_remap_is_identity():
    r = _build_remap_reader(None)
    got = _collect(r)
    assert got == [
        (10, "a", b"a1"),
        (20, "b", b"b1"),
        (30, "a", b"a2"),
        (40, "b", b"b2"),
    ]
