# turbodata

[![CI](https://github.com/opheadacheh/turbodata/actions/workflows/ci.yml/badge.svg)](https://github.com/opheadacheh/turbodata/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/turbodata)](https://pypi.org/project/turbodata/)
[![npm](https://img.shields.io/npm/v/turbodata)](https://www.npmjs.com/package/turbodata)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](./LICENSE)

**turbodata** is a container format for storing multi-topic, timestamped
messages — sensor logs, robotics/AV recordings, ML training shards — optimized
for **random, read-efficient access** across many topics and large files.

A single `.td` file holds any number of *topics* (named, metadata-tagged
streams) whose messages carry a monotonically non-decreasing timestamp. The
format is chunked and indexed so a reader can answer questions like *"give me
`/camera` and `/imu` between t₁ and t₂"* or *"the latest frame at or before
each of these 1,000 timestamps"* without scanning the whole file — and can do
so efficiently over local disk **or** remote object storage (S3/GCS) via
concurrent, cost-aware range reads.

The on-disk format is identical across every SDK, so **a file written by any
language can be read by any other.**

## SDKs

| Language | Package | Capabilities | Install |
| --- | --- | --- | --- |
| **Go** (reference) | `github.com/opheadacheh/turbodata/go/turbodata` | read + write | `go get github.com/opheadacheh/turbodata/go/turbodata` |
| **Python** | [`turbodata`](https://pypi.org/project/turbodata/) | read + write | `pip install turbodata` |
| **TypeScript** | [`turbodata`](https://www.npmjs.com/package/turbodata) | read-only (browser) | `npm install turbodata` |

The Go implementation is the reference; the Python and TypeScript SDKs are
ports that produce/consume byte-identical files.

## Quick start

The example below writes a file in Go, then reads it back in Python and in the
browser with TypeScript — demonstrating cross-language compatibility.

### Write (Go)

```go
package main

import (
	"log"
	"os"

	"github.com/opheadacheh/turbodata/go/turbodata"
)

func main() {
	f, err := os.Create("data.td")
	if err != nil {
		log.Fatal(err)
	}
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
}
```

### Read (Python)

```python
from turbodata import FileReadSource, Reader

with FileReadSource("data.td") as src:
    reader = Reader(src)
    for msg in reader.read_messages():
        print(msg.timestamp, msg.topic_name, len(msg.data))
```

### Read (TypeScript, browser)

```ts
import { Reader, BlobReadSource } from "turbodata";

const file: File = /* from <input type="file"> */;
const reader = new Reader(new BlobReadSource(file));

for await (const msg of reader.readMessages()) {
  console.log(msg.timestamp, msg.topicName, msg.data.length);
}
```

## Features

- **Filtered iteration** — read a subset of topics and/or a `[start, end]`
  timestamp window; chunks fully outside the window are never fetched.
- **Ordering** — forward (default) or reverse time order, merged across topics.
- **Floor sampling** — for a list of query timestamps per topic, return the
  latest message at or before each (multi-topic temporal alignment).
- **Cost-aware concurrent reads** — an optional strategy plans every needed
  byte range, coalesces neighbors, optionally splits large reads, and issues
  them concurrently. Tunable for latency (object storage) or request cost (S3),
  via `StrategyForLatency` / `StrategyForMoney` / `StrategyForBlended`.
- **Video friendly** - GOP is guaranteed not to be split into different chunks,
  reading a specific video frame is ensured to start with a keyframe.
- **Topic remap** — present in-file topic names under different exposed names.
- **Multi-file reading** — merge several files into one time-ordered stream.
- **Pluggable sources** — local files, in-memory bytes, HTTP range, S3/GCS, …
  via a small `ReadSource` interface.

Capabilities are mirrored across SDKs (the TypeScript SDK is read-only). See
each SDK's README for the language-specific API.

## Documentation

- **[Format specification](./FORMAT.md)** — the precise on-disk layout (what makes files cross-SDK compatible).
- **[Design notes](./DESIGN.md)** — why the format is shaped the way it is, target scenarios, and trade-offs.

SDK guides:

- **Go** — [`go/turbodata/README.md`](./go/turbodata/README.md) · runnable examples in [`go/examples/`](./go/examples)
- **Python** — [`py/README.md`](./py/README.md) · runnable examples in [`py/examples/`](./py/examples)
- **TypeScript** — [`ts/README.md`](./ts/README.md) · runnable examples in [`ts/examples/`](./ts/examples)

Each SDK guide walks the same feature order — write, read, sample, multi-file —
with runnable examples covering filters, ordering, cost-aware strategies, and
video.

Rendered HTML versions of the spec and design notes live in [`docs/`](./docs)
(`docs/format.html`, `docs/design.html`); regenerate them with `python docs/build.py`
(see the script header for dependencies).

## Repository layout

```
go/
  turbodata/    Go SDK — the reference implementation (read + write)
  examples/     runnable Go examples (write, read, sample, multiread, mcap→td)
  benchmark/    performance benchmarks
  cmd/          maintenance tooling
py/             Python SDK (read + write) + examples + tests
ts/             TypeScript SDK (browser, read-only) + examples + tests
```

The Go workspace is split so that consumers of the library
(`go/turbodata`) only pull in format-related dependencies; the example,
benchmark, and tooling modules (which depend on heavier packages such as MCAP)
live in separate modules unioned by `go/go.work` for local development.

## Development

```bash
# Go (workspace covers all four modules)
cd go && go test ./turbodata/... && (cd examples && go build ./...)

# Python
cd py && pip install -e ".[test]" && pytest tests/

# TypeScript
cd ts && npm install && npm test
```

## License

Apache License 2.0 — see [LICENSE](./LICENSE) and [NOTICE](./NOTICE).
