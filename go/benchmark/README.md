# Turbodata Benchmark Suite

Compares turbodata (`.td`) vs MCAP across read scenarios and chunk configurations.
Metrics collected: wall time, memory allocations, and IO counters (bytes read, read calls, seek calls).

## Prerequisites

- Go 1.26+
- An input `.mcap` file

## Quick Start

```bash
cd go
go test ./benchmark/ -bench=. -benchmem -count=5 \
    -benchtime=10s \
    -args -mcap=/path/to/input.mcap
```

`TestMain` converts the MCAP to a set of `.td` files (one per chunk config) on first run and
prints their paths and sizes. The generated `.td` files are written alongside the input `.mcap`.

## Flags

| Flag | Description |
|------|-------------|
| `-mcap=<path>` | **Required.** Path to the input MCAP file. |

Pass custom flags after `-args` so the Go test runner does not interpret them:

```bash
go test ./benchmark/ -bench=. -args -mcap=/path/to/input.mcap
```

## Benchmarks

| Name | What it measures |
|------|-----------------|
| `BenchmarkReadAllMessages` | Full sequential scan of every message |
| `BenchmarkReadSingleTopic/img` | Read all messages from one image topic (`foxglove.RawImage`) |
| `BenchmarkReadSingleTopic/nonimg` | Read all messages from one non-image topic |
| `BenchmarkReadTimeRange` | Read up to 5 mixed topics within the middle-third time window |
| `BenchmarkWrite` | Write all messages (pre-loaded in memory) to `io.Discard` |

Each benchmark runs sub-benchmarks for:
- `mcap` — the MCAP indexed reader (read benchmarks only)
- `td/<config>` — each turbodata chunk config variant

## Chunk Config Variants

| Label | Image chunk size | Non-image chunk size |
|-------|-----------------|---------------------|
| `img128k_nonimg512k` | 128 KB | 512 KB |
| `img1m_nonimg1m` | 1 MB | 1 MB |
| `img4m_nonimg4m` | 4 MB | 4 MB |
| `img16m_nonimg16m` | 16 MB | 16 MB |

All variants use size-based chunking (`ChunkThresholdModeSize`) with compression enabled.

## Custom Metrics

Beyond the standard `ns/op` and `allocs/op` from `-benchmem`, each benchmark reports:

### Read benchmarks (mcap and td)

| Metric | Description |
|--------|-------------|
| `bytes/op` | Total bytes read through the IO layer per benchmark iteration |
| `reads/op` | Number of `Read()` calls per iteration |
| `seeks/op` | Number of `Seek()` calls per iteration |

Lower `bytes/op` and `seeks/op` mean the format's index is more effective at skipping
irrelevant data. TD with larger chunks makes fewer seeks but reads more bytes per seek.

> **Note:** For MCAP, `reads/op` may be lower than expected because the MCAP lexer wraps the
> reader in a buffered layer for sequential metadata reads. Random-access chunk data reads are
> unaffected and go directly through the tracking wrapper.

### Write benchmarks (td only)

| Metric | Description |
|--------|-------------|
| `write_bytes/op` | Total bytes flushed to the underlying writer per iteration |
| `write_calls/op` | Number of `Write()` calls to the underlying writer per iteration |

The turbodata writer uses an internal 128 KB `bufio.Writer`, so `write_calls/op` reflects
128 KB block flushes rather than per-message writes. Larger chunks reduce `write_calls/op`
because data is buffered longer before flushing.

## OS Page Cache

By default all benchmark iterations run with a warm OS page cache (the file is in memory after
the first read). This measures memory bandwidth rather than disk latency.

To get cold-cache numbers (requires root):

```bash
sync && echo 3 | sudo tee /proc/sys/vm/drop_caches
go test ./benchmark/ -bench=BenchmarkReadAllMessages -count=1 -benchtime=1x \
    -args -mcap=/path/to/input.mcap
```

## Comparing Results with benchstat

Save results to separate files and use `benchstat` to compare:

```bash
go test ./benchmark/ -bench=. -benchmem -count=10 -benchtime=5s \
    -args -mcap=/path/to/input.mcap \
    | tee results.txt

# install benchstat once
go install golang.org/x/perf/cmd/benchstat@latest

# compare td configs against mcap
grep 'ReadAllMessages' results.txt | benchstat /dev/stdin
```

To compare two separate runs (e.g. before/after a code change):

```bash
go test ./benchmark/ -bench=. -benchmem -count=10 -args -mcap=... > before.txt
# make changes
go test ./benchmark/ -bench=. -benchmem -count=10 -args -mcap=... > after.txt
benchstat before.txt after.txt
```

## Selecting Specific Benchmarks

```bash
# Read benchmarks only
go test ./benchmark/ -bench=BenchmarkRead -args -mcap=...

# Single-topic image read only
go test ./benchmark/ -bench=BenchmarkReadSingleTopic/img -args -mcap=...

# Write benchmark only
go test ./benchmark/ -bench=BenchmarkWrite -args -mcap=...

# One specific TD config
go test ./benchmark/ -bench='td/img1m_nonimg1m' -args -mcap=...
```

## Strategy Benchmark

`BenchmarkStrategy` measures the IO advantage of `WithReadStrategy()` against the
default TD reader and an MCAP reference, under synthetic network latency. It
isolates the read-side effect of strategy choice — chunk coalescing and parallel
`ReadAt` — from compression and chunk-config noise.

### Fixture

The benchmark uses a separate `.td` file (`<input>.strategy_img4m_uncomp_nonimg4m.td`)
built once on first run:

- Image topics: **uncompressed**, 4 MB chunks.  Uncompressed chunks let the
  cost-aware reader fetch individual messages by byte range; this is the whole
  point of the benchmark.
- Non-image topics: compressed, 4 MB chunks.

Because real-world image data is typically pre-compressed (JPEG / H.264 / ...),
compressing TD chunks again adds little. Use the `mcap-mock-compress` tool below
to convert a raw-image MCAP into a fixture that mimics this shape.

### Sub-benchmark axes

`{scenario}/{variant}/{rtt}`:

| Axis | Values | Notes |
|------|--------|-------|
| scenario | `all`, `img`, `range` | full scan / one image topic / 5 topics in middle-third window |
| variant | `mcap`, `td_default`, `td_latency_serial`, `td_latency_parallel`, `td_money` | see below |
| rtt | `0ms`, `1ms`, `10ms` | injected on every Read / ReadAt |

Latency model: each `Read` or `ReadAt` sleeps `rtt + n / 100MB/s`. `Seek` is free
(real object stores have no Seek; cost is charged at the next Read).

| Variant | Strategy |
|---------|----------|
| `mcap` | MCAP indexed reader, latency-wrapped (reference baseline) |
| `td_default` | No `WithReadStrategy()` — raw `Read` + `Seek` path |
| `td_latency_serial` | `StrategyForLatency(rtt, 100MB/s, 1)` — coalesces and splits at the bandwidth-latency product, single in-flight read |
| `td_latency_parallel` | `StrategyForLatency(rtt, 100MB/s, 8)` — same as serial, up to 8 in-flight reads |
| `td_money` | `StrategyForMoney(0.0004, 0, 1)` — free egress, infinite coalesce, no split |

### Running

```bash
go test ./benchmark/ -bench=BenchmarkStrategy -benchmem -count=5 \
    -benchtime=2s -timeout=60m \
    -args -mcap=/path/to/mock_compressed.mcap
```

Higher RTT × higher b.N can take a long time; pin `-benchtime` and `-count`
explicitly. Wall-clock dominates over CPU at non-zero RTT, so `-cpu` flags
have little effect.

### Generating a mock-compressed fixture

If your input MCAP stores images in raw RGB (`foxglove.RawImage`), the chunk
compressor will hide the per-message read advantage. Convert it first:

```bash
go run ./benchmark/cmd/mcap-mock-compress \
    -in  raw_images.mcap \
    -out mock_compressed.mcap \
    -size 60KB
```

This rewrites every `foxglove.RawImage` payload as 60 KB of pseudo-random bytes
(incompressible, mimicking JPEG / H.264 codec output). All other messages,
schemas, channels, attachments, and metadata are passed through unchanged.

> **Note:** The output is not a valid `foxglove.RawImage` stream — payload size
> no longer matches `width * height * channels`. It is a benchmark fixture, not
> a Foxglove Studio replay file.

### Selecting strategy sub-benchmarks

```bash
# Strategy benchmark only
go test ./benchmark/ -bench=BenchmarkStrategy -args -mcap=...

# One scenario at one RTT
go test ./benchmark/ -bench='Strategy/img/.*/10ms' -args -mcap=...

# One specific variant at one RTT
go test ./benchmark/ -bench='Strategy/img/td_latency_parallel/10ms' -args -mcap=...
```

