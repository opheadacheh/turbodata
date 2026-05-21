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
