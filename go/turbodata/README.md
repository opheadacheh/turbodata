# turbodata (Go)

Go SDK for the turbodata `.td` container format, and the **reference
implementation** of the format. The Python (`../../py`) and TypeScript
(`../../ts`) SDKs are ports; the on-disk format is identical, so files written
by any SDK can be read by any other.

Read **and** write, with a default lazy reader path for local disk and an
optional cost-aware concurrent-I/O path for object storage (S3/GCS).

## Install

```bash
go get github.com/opheadacheh/turbodata/go/turbodata
```

```go
import "github.com/opheadacheh/turbodata/go/turbodata"
```

Module path: `github.com/opheadacheh/turbodata/go/turbodata` (package
`turbodata`).

## Quick start

### Read

```go
f, err := os.Open("data.td")
if err != nil {
	log.Fatal(err)
}
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
	// ts: int64 timestamp, topic: string, buf.Data: []byte
	// buf.Data aliases an internal buffer; copy it if you need to retain it.
	_ = ts
	_ = topic
}
```

### Write

```go
f, err := os.Create("out.td")
if err != nil {
	log.Fatal(err)
}
defer f.Close()

w := turbodata.NewWriter(f)
if err := w.OpenTopics(
	[]string{"/imu", "/cam"},
	[]map[string]any{{"hz": int64(100)}, {"hz": int64(10)}},
	turbodata.WithCompression(),
	turbodata.WithChunkConfig(&turbodata.ChunkConfig{
		Mode: turbodata.ChunkThresholdModeSize,
		Size: 1 << 20, // 1 MiB
	}),
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

### Sample (floor lookup at concrete timestamps)

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

## Filters and ordering

`ReadMessages` takes options:

```go
it, err := r.ReadMessages(
	turbodata.WithTopicNames([]string{"/imu", "/cam"}), // default: all topics
	turbodata.WithStartTimestamp(1_000),                // inclusive lower bound
	turbodata.WithEndTimestamp(2_000),                  // inclusive upper bound
	turbodata.WithOrder(turbodata.ReverseTimeOrder),    // default: TimeOrder
)
```

## Cost-aware concurrent reads

By default the reader walks one chunk at a time with serial `Seek`+`Read` —
minimal memory, good for local disk. Pass `WithReadStrategy` to switch onto the
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

## Topic remap

Present in-file topic names under different exposed names. The map is keyed by
in-file name and valued by the exposed name. Names absent from the map pass
through unchanged.

```go
r := turbodata.NewReader(src, turbodata.WithTopicRemap(map[string]string{
	"/cam": "/cam_v2",
}))
```

## Reading several files (MultiReader)

`NewMultiReader` presents several single-file readers as one time-ordered
stream. Read options pass through to each underlying reader. Topic-name
collisions across files are resolved with per-reader topic remaps.

## ReadSource

`NewReader` accepts a `ReadSource`, satisfied natively by `*os.File` and
`*bytes.Reader`. `ReadAt` must be safe for concurrent use (the cost-aware path
calls it from multiple goroutines). Implement your own for HTTP range reads,
S3, GCS, etc.

## Examples

Runnable examples live in [`../examples`](../examples):

```bash
cd go
go run ./examples/write      # produces examples/demo.td
go run ./examples/read
go run ./examples/sample
go run ./examples/multiread
```

(The examples are a separate Go module; the `go/go.work` workspace wires them
to this package for local development.)

## Tests

```bash
cd go
go test ./turbodata/...
```
