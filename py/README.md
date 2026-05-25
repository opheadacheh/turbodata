# turbodata (Python)

Python SDK for the turbodata `.td` container format. Read/write port of the
Go reference implementation in `../go`. The on-disk format is identical, so
files written by any SDK can be read by any other.

Built so model developers can directly manipulate `.td` files (read training
data, write preprocessed shards, sample at specific timestamps for multi-topic
alignment).

## Install

```bash
cd py
python -m venv .venv
source .venv/bin/activate
pip install -e .
# or, for tests:
pip install -e ".[test]"
```

Dependencies: `msgpack`, `zstandard`.

## Quick start

### Read

```python
from turbodata import FileReadSource, Reader

with FileReadSource("data.td") as src:
    reader = Reader(src)
    for msg in reader.read_messages():
        # msg.timestamp: int (nanoseconds or arbitrary unit, set by the writer)
        # msg.topic_name: str
        # msg.data: bytes (aliases an internal buffer; copy if you need to keep it)
        print(msg.timestamp, msg.topic_name, len(msg.data))
```

`msg.data` aliases an internal reusable buffer and is only valid until the
next iteration step. Pass `copy=True` to get fresh bytes each iteration:

```python
for msg in reader.read_messages(copy=True):
    ...  # msg.data is independent of subsequent pulls
```

### Write

```python
from turbodata import ChunkConfig, ChunkThresholdMode, Writer

with open("out.td", "wb") as f, Writer(f) as w:
    w.open_topics(
        ["/imu", "/cam"],
        [{"hz": 100}, {"hz": 10}],
        chunk_config=ChunkConfig(mode=ChunkThresholdMode.SIZE, size=1024 * 1024),
        compression=True,
    )
    w.write_message("/imu", b"...", timestamp=1_000)
    w.write_message("/cam", b"...", timestamp=2_000)
    w.close_topic()
```

The writer enforces non-decreasing timestamps within an open topic group.

### Sample (floor lookup at concrete timestamps)

```python
from turbodata import FileReadSource, Reader, SampleQuery, lin_space_timestamps

with FileReadSource("data.td") as src:
    reader = Reader(src)
    out = reader.sample([
        SampleQuery(topic="/imu", timestamps=lin_space_timestamps(0, 100_000_000, 10)),
        SampleQuery(topic="/cam", timestamps=[1_000_000_000, 2_000_000_000]),
    ])
    for q, row in zip(["/imu", "/cam"], out):
        for r in row:
            if r.found:
                print(q, r.timestamp, len(r.data))
```

The same topic must not appear in more than one `SampleQuery`. Timestamps
within one query must be strictly increasing.

## Filters and ordering

```python
from turbodata import Order

reader.read_messages(
    topic_names=["/imu", "/cam"],        # default: all topics
    start_timestamp=1_000_000_000,       # inclusive lower bound
    end_timestamp=2_000_000_000,         # inclusive upper bound
    order=Order.REVERSE_TIME,            # default: Order.TIME
)
```

## Cost-aware concurrent reads

For cloud object storage (S3 / GCS) or large local reads, switch to the
cost-aware path by passing a `ReadStrategy`. The reader pre-plans every
needed byte range, coalesces nearby ranges into single reads, optionally
splits large reads for parallelism, and issues them concurrently via a
thread pool.

```python
from turbodata import Reader, ReadStrategy, strategy_for_latency

reader.read_messages(
    strategy=strategy_for_latency(
        rtt_seconds=0.020,
        per_stream_bw=125 * 1024 * 1024,
        concurrency=16,
    ),
)
```

Helper constructors mirror the Go SDK: `strategy_for_latency`,
`strategy_for_money`, `strategy_for_blended`. Pass a hand-tuned
`ReadStrategy(coalesce_gap=..., split_threshold=..., max_concurrency=...)`
when you know the right values for your storage backend.

## ReadSource protocol

A `ReadSource` exposes two methods:

```python
class ReadSource(Protocol):
    def size(self) -> int: ...
    def read_at(self, offset: int, n: int) -> bytes: ...
```

`read_at` MUST be safe to call from multiple threads concurrently when used
with the cost-aware path. Built-in sources:

- `FileReadSource(path_or_fd)` — local files, uses `os.pread` on POSIX
- `BytesReadSource(bytes)` — in-memory bytes for tests/small files

Roll your own for HTTP range reads, S3, GCS, etc.

## Mapping to the Go SDK

| Go                                  | Python                              |
| ----------------------------------- | ----------------------------------- |
| `NewReader(rs)`                     | `Reader(source)`                    |
| `Summary()`                         | `reader.summary()`                  |
| `ReadMessages(opts...)`             | `reader.read_messages(**kwargs)`    |
| `Sample(queries, opts...)`          | `reader.sample(queries, **kwargs)`  |
| `WithTopicNames`                    | `topic_names=[...]`                 |
| `WithStartTimestamp`                | `start_timestamp=...`               |
| `WithEndTimestamp`                  | `end_timestamp=...`                 |
| `WithOrder(ReverseTimeOrder)`       | `order=Order.REVERSE_TIME`          |
| `WithReadStrategy(s)`               | `strategy=...`                      |
| `WithTailPrefetch(n)`               | `tail_prefetch=n`                   |
| `NewWriter(f)`                      | `Writer(f)`                         |
| `OpenTopics(names, metas, opts...)` | `writer.open_topics(...)`           |
| `WithChunkConfig(cc)`               | `chunk_config=ChunkConfig(...)`     |
| `WithCompression()`                 | `compression=True`                  |
| `WriteMessage(topic, data, ts)`     | `writer.write_message(topic, data, ts)` |
| `CloseTopic()`                      | `writer.close_topic()`              |
| `Close()`                           | `writer.close()` (or `with`)        |

## Run the examples

```bash
python examples/write_demo.py        # produces examples/demo.td
python examples/read_demo.py
python examples/sample_demo.py
```

## Run the tests

```bash
pip install -e ".[test]"
pytest tests/
```

Cross-language tests run against the Go SDK's example fixtures
(`../ts/test/fixtures/*.td` and `../go/examples/demo.td`) when those files
are present.
