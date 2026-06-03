# turbodata (Go)

Go SDK for the turbodata `.td` container format, and the **reference
implementation** of the format. The Python (`../../py`) and TypeScript
(`../../ts`) SDKs are ports; the on-disk format is identical, so files written
by any SDK can be read by any other.

Read **and** write, with a default lazy reader path for local disk and an
optional cost-aware concurrent-I/O path for object storage (S3/GCS).

```bash
go get github.com/opheadacheh/turbodata/go/turbodata
```

```go
import "github.com/opheadacheh/turbodata/go/turbodata"
```

See the [top-level README](../../README.md) for scope and the format overview.
This guide is API usage by feature: **write → read → sample → multi-file**.
Runnable versions of every snippet live in [`../examples`](../examples).

## Write

Open one or more topics as a group, append messages with non-decreasing
timestamps, then close the topic group. `Close` finalizes the file.

```go
f, _ := os.Create("out.td")
defer f.Close()

w := turbodata.NewWriter(f)
if err := w.OpenTopics(
	[]string{"/imu", "/cam"},
	[]map[string]any{{"hz": int64(100)}, {"hz": int64(10)}},
	turbodata.WithCompression(),
); err != nil {
	log.Fatal(err)
}
_ = w.WriteMessage("/imu", []byte("..."), 1_000)
_ = w.WriteMessage("/cam", []byte("..."), 2_000)
_ = w.CloseTopic()

if err := w.Close(); err != nil {
	log.Fatal(err)
}
```

The writer enforces non-decreasing timestamps within an open topic group.

### Chunk config

Chunks are the unit of indexing and I/O. By default the writer flushes a chunk
when it reaches a size threshold; `WithChunkConfig` overrides the policy (by
size, message count, or time duration).

```go
w.OpenTopics(
	[]string{"/imu"},
	[]map[string]any{{"hz": int64(100)}},
	turbodata.WithChunkConfig(&turbodata.ChunkConfig{
		Mode: turbodata.ChunkThresholdModeSize,
		Size: 1 << 20, // 1 MiB
	}),
)
```

### Video

Mark a group as video with `WithVideoTopic` and append frames with
`WriteVideoMessage` (which carries an `isKeyFrame` flag). Chunk boundaries are
gated on key frames, so a GOP is never split across two chunks — readers can
always start a chunk from a key frame.

```go
w.OpenTopics(
	[]string{"/cam/h264"},
	[]map[string]any{{"codec": "h264"}},
	turbodata.WithVideoTopic(),
)
_ = w.WriteVideoMessage("/cam/h264", keyFrameBytes, 1_000, true)  // first frame must be a key frame
_ = w.WriteVideoMessage("/cam/h264", deltaBytes, 2_000, false)
_ = w.CloseTopic()
```

A video group MUST contain exactly one topic and MUST NOT also be compressed
(the codec already compresses the bytes). The first frame MUST be a key frame.

## Read

`NewReader` accepts a `ReadSource` (satisfied by `*os.File`, `*bytes.Reader`,
or your own). `ReadMessages` returns an iterator; `NextInto` fills a reusable
buffer to avoid per-message allocation.

```go
f, _ := os.Open("data.td")
defer f.Close()

r := turbodata.NewReader(f)
it, err := r.ReadMessages()
if err != nil {
	log.Fatal(err)
}

buf := turbodata.NewReusableBuffer()
for {
	ts, topic, err := it.NextInto(buf)
	if errors.Is(err, io.EOF) {
		break
	}
	if err != nil {
		log.Fatal(err)
	}
	// ts: int64, topic: string, buf.Data: []byte
	// buf.Data aliases an internal buffer; copy it if you need to retain it.
	_, _ = ts, topic
}
```

### Order, time, and topic filters

`ReadMessages` takes options. Topics not listed are skipped; chunks fully
outside the `[start, end]` window are never fetched.

```go
it, err := r.ReadMessages(
	turbodata.WithOrder(turbodata.ReverseTimeOrder),    // default: TimeOrder
	turbodata.WithTopicNames([]string{"/imu", "/cam"}), // default: all topics
	turbodata.WithStartTimestamp(1_000),                // inclusive lower bound
	turbodata.WithEndTimestamp(2_000),                  // inclusive upper bound
)
```

### Cost-aware concurrent reads

By default the reader walks one chunk at a time with serial `Seek`+`Read` —
minimal memory, good for local disk. `WithReadStrategy` switches onto the
cost-aware path, which pre-plans every needed byte range, coalesces neighbors,
optionally splits large reads, and fetches concurrently via `ReadAt`.

```go
it, err := r.ReadMessages(
	turbodata.WithReadStrategy(turbodata.StrategyForLatency(
		20*time.Millisecond, // RTT
		125*1024*1024,       // per-stream bandwidth (B/s)
		16,                  // concurrency
	)),
)
```

Helper constructors for common cost models:

- `StrategyForLatency(rtt, perStreamBW, concurrency)` — minimize wall-clock.
- `StrategyForMoney(reqPrice, bytePrice, concurrency)` — minimize spend on
  paid-request storage (e.g. S3 GET pricing).
- `StrategyForBlended(reqPrice, bytePrice, maxReadTime, perStreamBW, concurrency)`
  — minimize spend with a per-read latency cap.

Or build a `ReadStrategy{CoalesceGap, SplitThreshold, MaxConcurrency}` directly
when you know the right values for your backend.

### Video

A non-key frame is only decodable after its GOP's key frame, so a plain time
filter that starts mid-GOP yields bytes a decoder can't cold-start on.
`WithVideoDecodable` snaps the effective `StartTimestamp` back to the latest
key frame at or before it, so the emitted sequence starts decodable.
Non-video topics are unaffected.

```go
it, err := r.ReadMessages(
	turbodata.WithTopicNames([]string{"/cam/h264"}),
	turbodata.WithStartTimestamp(650), // mid-GOP
	turbodata.WithVideoDecodable(),    // snaps back to the key frame at/<=650
)
```

## Sample (floor lookup at concrete timestamps)

`Sample` returns the floor message per `(topic, timestamp)` pair: the
most-recent message at or before each requested timestamp. Use it for "the
state at time T" rather than "every message in `[t0, t1]`".

```go
out, err := r.Sample([]turbodata.SampleQuery{
	{Topic: "/imu", Timestamps: turbodata.LinSpaceTimestamps(0, 100, 10)},
	{Topic: "/cam", Timestamps: []int64{1_000, 2_000}},
})
if err != nil {
	log.Fatal(err)
}
for i, row := range out {
	for j, res := range row {
		if res.Found {
			fmt.Println(i, j, res.Timestamp, len(res.Data))
		}
	}
}
```

`out[i][j]` corresponds to `queries[i].Timestamps[j]`. A topic must not appear
in more than one `SampleQuery`, and timestamps within one query must be
strictly increasing.

### Strategy

Data I/O is concurrent under the hood, governed by `DefaultSampleStrategy`.
Override it for your backend with `WithSampleReadStrategy`:

```go
out, err := r.Sample(queries, turbodata.WithSampleReadStrategy(turbodata.ReadStrategy{
	CoalesceGap:    256 * 1024,
	SplitThreshold: 2 * 1024 * 1024,
	MaxConcurrency: 4,
}))
```

### Video

With `WithSampleVideoDecodable`, results for a video topic carry a
decoder-ready GOP sequence in `Frames` (and `Data` is nil) instead of the
single floor frame. Within a row (one topic, strictly increasing timestamps),
the first result in each GOP sets `ResetDecoder=true` with the full prefix
`[keyframe ... target]`; later results in the same GOP set `ResetDecoder=false`
with only the new frames since the previous query.

```go
out, err := r.Sample(
	[]turbodata.SampleQuery{{Topic: "/cam/h264", Timestamps: []int64{350, 650}}},
	turbodata.WithSampleVideoDecodable(),
)
for _, res := range out[0] {
	if res.Found {
		// res.IsVideo == true; feed res.Frames to a decoder,
		// calling Reset() first when res.ResetDecoder is true.
		fmt.Println(res.Timestamp, res.ResetDecoder, len(res.Frames))
	}
}
```

## Reading several files (MultiReader)

`NewMultiReader` presents several single-file readers as one time-ordered
stream. Read/sample options pass through to each underlying reader. Topic-name
collisions across files are resolved with per-reader topic remaps: names meant
to union share an exposed name; names meant to stay distinct are remapped
apart.

```go
mr, err := turbodata.NewMultiReader(
	turbodata.NewReader(srcA),
	// Keep file B's "/cam" distinct instead of unioning it with A's.
	turbodata.NewReader(srcB, turbodata.WithTopicRemap(map[string]string{"/cam": "/cam_b"})),
)
if err != nil {
	log.Fatal(err)
}

it, err := mr.ReadMessages() // same options as Reader.ReadMessages
// ... iterate with NextInto ...

// Sample returns, per cell, the latest floor across the union of files.
out, err := mr.Sample([]turbodata.SampleQuery{{Topic: "/cam", Timestamps: []int64{150, 350}}})
```

### Topic remap

`WithTopicRemap` is keyed by in-file name and valued by the exposed name.
Names absent from the map pass through unchanged. It works on a single
`Reader` too:

```go
r := turbodata.NewReader(src, turbodata.WithTopicRemap(map[string]string{
	"/cam": "/cam_v2",
}))
```

## ReadSource

`NewReader` accepts a `ReadSource`, satisfied natively by `*os.File` and
`*bytes.Reader`. `ReadAt` must be safe for concurrent use (the cost-aware path
calls it from multiple goroutines). Implement your own for HTTP range reads,
S3, GCS, etc.

## Examples

Runnable examples live in [`../examples`](../examples). Run `write` first; it
produces `examples/demo.td`, which `read` and `sample` consume.

```bash
cd go
go run ./examples/write      # produces examples/demo.td (incl. a video group)
go run ./examples/read       # filters, order, strategy, video snap-back
go run ./examples/sample     # floor lookup, strategy, video GOP-prefix
go run ./examples/multiread  # union, split-via-remap, cross-file sample
```

(The examples are a separate Go module; the `go/go.work` workspace wires them
to this package for local development.)

## Tests

```bash
cd go
go test ./turbodata/...
```
