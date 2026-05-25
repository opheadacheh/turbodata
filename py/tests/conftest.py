"""Shared test helpers.

Mirrors go/test_helpers_test.go and go/internal/iter/helpers_test.go. Helpers
follow the same names where possible so cross-language comparisons stay easy.
"""
from __future__ import annotations

import io
import os
import threading
from dataclasses import dataclass
from typing import Callable, List, Optional

from turbodata import (
    BytesReadSource,
    Message,
    Reader,
    Writer,
)


REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
TS_FIXTURES = os.path.join(REPO_ROOT, "ts", "test", "fixtures")
GO_DEMO = os.path.join(REPO_ROOT, "go", "examples", "demo.td")


def build_file(setup: Callable[[Writer], None]) -> bytes:
    """Run setup against a Writer and return the resulting bytes."""
    buf = io.BytesIO()
    w = Writer(buf)
    setup(w)
    w.close()
    return buf.getvalue()


def writer_round_trip(setup: Callable[[Writer], None]) -> Reader:
    """Run setup against a Writer, then return a Reader over the result."""
    return Reader(BytesReadSource(build_file(setup)))


def collect(reader: Reader, **opts) -> List[Message]:
    """Drain a Reader into a list of Messages (with bytes copied to be safe)."""
    out: List[Message] = []
    for m in reader.read_messages(copy=True, **opts):
        out.append(m)
    return out


# ---------------------------------------------------------------------------
# Tracking source: counts read_at calls, observes max in-flight reads.
# Mirrors go/cost_aware_integration_test.go's trackingSource.
# ---------------------------------------------------------------------------
class TrackingSource:
    """Wraps a ReadSource and tracks call counts + max simultaneous reads."""

    def __init__(self, inner) -> None:
        self._inner = inner
        self._lock = threading.Lock()
        self.read_at_calls = 0
        self.read_at_bytes = 0
        self.read_at_in_flight = 0
        self.read_at_max_flight = 0

    def size(self) -> int:
        return self._inner.size()

    def read_at(self, offset: int, n: int) -> bytes:
        with self._lock:
            self.read_at_in_flight += 1
            if self.read_at_in_flight > self.read_at_max_flight:
                self.read_at_max_flight = self.read_at_in_flight
        try:
            data = self._inner.read_at(offset, n)
            with self._lock:
                self.read_at_calls += 1
                self.read_at_bytes += len(data)
            return data
        finally:
            with self._lock:
                self.read_at_in_flight -= 1


# ---------------------------------------------------------------------------
# Slow source: per-call sleep so concurrency is observable in tests.
# Mirrors go/cost_aware_integration_test.go's slowReadAtSource.
# ---------------------------------------------------------------------------
class SlowReadSource:
    """Sleeps `delay_seconds` per read_at so workers visibly overlap."""

    def __init__(self, inner, delay_seconds: float) -> None:
        self._inner = inner
        self._delay = delay_seconds

    def size(self) -> int:
        return self._inner.size()

    def read_at(self, offset: int, n: int) -> bytes:
        import time

        time.sleep(self._delay)
        return self._inner.read_at(offset, n)


# ---------------------------------------------------------------------------
# Failing source: succeeds for the first `succeed_first` calls then raises.
# ---------------------------------------------------------------------------
class FailingReadSource:
    def __init__(self, inner, exc: BaseException, succeed_first: int = 1) -> None:
        self._inner = inner
        self._exc = exc
        self._remaining = succeed_first
        self._lock = threading.Lock()

    def size(self) -> int:
        return self._inner.size()

    def read_at(self, offset: int, n: int) -> bytes:
        with self._lock:
            allow = self._remaining > 0
            self._remaining -= 1
        if not allow:
            raise self._exc
        return self._inner.read_at(offset, n)
