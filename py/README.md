# turbodata (Python)

Python SDK for the turbodata `.td` container format. Read/write port of the
Go reference implementation in `../go`. The on-disk format is identical, so
files written by any SDK can be read by any other.

Built so model developers can directly manipulate `.td` files (read training
data, write preprocessed shards, sample at specific timestamps for multi-topic
alignment).

```bash
pip install turbodata          # or, for local dev: pip install -e ".[test]"
```

Dependencies: `msgpack`, `zstandard`. See the
[top-level README](../README.md) for scope and the format overview. This guide
is API usage by feature: **write → read → sample → multi-file**. Runnable
versions of every snippet live in [`examples/`](./examples).

## Write

Open one or more topics as a group, append messages with non-decreasing
timestamps, then close the topic group. The `Writer` context manager finalizes
the file on exit.

```python
from turbodata import Writer

with open("out.td", "wb") as f, Writer(f) as w:
    w.open_topics(
        ["/imu", "/cam"],
        [{"hz": 100}, {"hz": 10}],
        compression=True,
    )
    w.write_message("/imu", b"...", timestamp=1_000)
    w.write_message("/cam", b"...", timestamp=2_000)
    w.close_topic()
```

The writer enforces non-decreasing timestamps within an open topic group.

### Chunk config

Chunks are the unit of indexing and I/O. By default the writer flushes a chunk
at a size threshold; `chunk_config` overrides the policy (by size, message
count, or time duration).

```python
from turbodata import ChunkConfig, ChunkThresholdMode

w.open_topics(
    ["/imu"],
    [{"hz": 100}],
    chunk_config=ChunkConfig(mode=ChunkThresholdMode.SIZE, size=1024 * 1024),
)
```

### Video

Mark a group as video with `video=True` and append frames with
`write_video_message` (which carries an `is_key_frame` flag). Chunk boundaries
are gated on key frames, so a GOP is never split across two chunks.

```python
w.open_topics(["/cam/h264"], [{"codec": "h264"}], video=True)
w.write_video_message("/cam/h264", key_frame_bytes, 1_000, True)  # first frame must be a key frame
w.write_video_message("/cam/h264", delta_bytes, 2_000, False)
w.close_topic()
```

A video group MUST contain exactly one topic and MUST NOT also be compressed
(the codec already compresses the bytes). The first frame MUST be a key frame.

## Read

```python
from turbodata import FileReadSource, Reader

with FileReadSource("data.td") as src:
    reader = Reader(src)
    for msg in reader.read_messages():
        # msg.timestamp: int (unit set by the writer)
        # msg.topic_name: str
        # msg.data: bytes (aliases an internal buffer; copy if you need to keep it)
        print(msg.timestamp, msg.topic_name, len(msg.data))
```

`msg.data` aliases an internal reusable buffer and is only valid until the
next iteration step. Pass `copy=True` for fresh bytes each iteration:

```python
for msg in reader.read_messages(copy=True):
    ...  # msg.data is independent of subsequent pulls
```

### Order, time, and topic filters

Topics not listed are skipped; chunks fully outside the `[start, end]` window
are never fetched.

```python
from turbodata import Order

reader.read_messages(
    order=Order.REVERSE_TIME,           # default: Order.TIME
    topic_names=["/imu", "/cam"],        # default: all topics
    start_timestamp=1_000_000_000,       # inclusive lower bound
    end_timestamp=2_000_000_000,         # inclusive upper bound
)
```

### Cost-aware concurrent reads

For cloud object storage (S3 / GCS) or large local reads, pass a
`ReadStrategy`. The reader pre-plans every needed byte range, coalesces nearby
ranges, optionally splits large reads, and issues them concurrently via a
thread pool. Without a strategy it uses the memory-minimal lazy path.

```python
from turbodata import strategy_for_latency

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

### Video

A non-key frame is only decodable after its GOP's key frame, so a plain time
filter that starts mid-GOP yields bytes a decoder can't cold-start on.
`video_decodable=True` snaps the effective start back to the latest key frame
at or before it, so the emitted sequence starts decodable. Non-video topics
are unaffected.

```python
reader.read_messages(
    topic_names=["/cam/h264"],
    start_timestamp=650,        # mid-GOP
    video_decodable=True,       # snaps back to the key frame at/<=650
)
```

## Sample (floor lookup at concrete timestamps)

`sample` returns the floor message per `(topic, timestamp)` pair: the
most-recent message at or before each requested timestamp. Use it for "the
state at time T" rather than "every message in `[t0, t1]`".

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

`out[i][j]` corresponds to `queries[i].timestamps[j]`. The same topic must not
appear in more than one `SampleQuery`; timestamps within one query must be
strictly increasing.

### Strategy

Pass `strategy=...` to override the default sample strategy for your backend:

```python
from turbodata import ReadStrategy

reader.sample(
    queries,
    strategy=ReadStrategy(coalesce_gap=256 * 1024, split_threshold=2 * 1024 * 1024, max_concurrency=4),
)
```

### Video

With `video_decodable=True`, results for a video topic carry a decoder-ready
GOP sequence in `frames` (and `data` is empty) instead of the single floor
frame. Within a row (one topic, strictly increasing timestamps), the first
result in each GOP sets `reset_decoder=True` with the full prefix
`[keyframe ... target]`; later results in the same GOP set
`reset_decoder=False` with only the new frames since the previous query.

```python
out = reader.sample(
    [SampleQuery(topic="/cam/h264", timestamps=[350, 650])],
    video_decodable=True,
)
for r in out[0]:
    if r.found:
        # r.is_video is True; feed r.frames to a decoder,
        # resetting it first when r.reset_decoder is True.
        print(r.timestamp, r.reset_decoder, len(r.frames))
```

## Multi-file reading (MultiReader)

`MultiReader` presents several single-file `Reader`s as one time-ordered
stream. Read/sample options pass through to each underlying reader. Topic-name
collisions across files are resolved with per-`Reader` `topic_remap`: names
meant to union share an exposed name; names meant to stay distinct are remapped
apart.

```python
from turbodata import BytesReadSource, MultiReader, Reader, SampleQuery

mr = MultiReader(
    Reader(BytesReadSource(file_a)),
    # Keep file B's "/cam" distinct instead of unioning it with A's.
    Reader(BytesReadSource(file_b), topic_remap={"/cam": "/cam_b"}),
)

for msg in mr.read_messages():
    print(msg.timestamp, msg.topic_name)

# sample() returns, per cell, the latest floor across the union of files.
out = mr.sample([SampleQuery(topic="/cam", timestamps=[150, 350])])
```

### Topic remap

`topic_remap` is keyed by in-file name and valued by the exposed name reported
by `summary()`, emitted from `read_messages`, and accepted by `topic_names`
and `SampleQuery.topic`. Names absent from the map pass through unchanged. It
works on a single `Reader` too:

```python
reader = Reader(src, topic_remap={"/cam": "/cam_v2"})
for msg in reader.read_messages(topic_names=["/cam_v2"]):
    print(msg.topic_name)  # "/cam_v2"
```

The remap is validated lazily against the file's summary on first use: it
raises `TopicRemapCollisionError` if two topics collapse onto the same exposed
name.

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
| `WithVideoDecodable()`              | `video_decodable=True`              |
| `WithSampleVideoDecodable()`        | `video_decodable=True` (on sample)  |
| `WithTopicRemap(m)`                 | `Reader(src, topic_remap={...})`    |
| `NewWriter(f)`                      | `Writer(f)`                         |
| `OpenTopics(names, metas, opts...)` | `writer.open_topics(...)`           |
| `WithChunkConfig(cc)`               | `chunk_config=ChunkConfig(...)`     |
| `WithCompression()`                 | `compression=True`                  |
| `WithVideoTopic()`                  | `video=True`                        |
| `WriteMessage(topic, data, ts)`     | `writer.write_message(topic, data, ts)` |
| `WriteVideoMessage(topic, data, ts, kf)` | `writer.write_video_message(topic, data, ts, kf)` |
| `CloseTopic()`                      | `writer.close_topic()`              |
| `Close()`                           | `writer.close()` (or `with`)        |

## Run the examples

Run `write_demo.py` first; it produces `examples/demo.td`, which the read and
sample demos consume.

```bash
python examples/write_demo.py     # produces examples/demo.td (incl. a video group)
python examples/read_demo.py      # filters, order, strategy, video snap-back
python examples/sample_demo.py    # floor lookup, strategy, video GOP-prefix
python examples/multiread_demo.py # union, split-via-remap, cross-file sample
```

## Run the tests

```bash
pip install -e ".[test]"
pytest tests/
```

Cross-language tests run against the Go SDK's example fixtures
(`../ts/test/fixtures/*.td` and `../go/examples/demo.td`) when those files
are present.
