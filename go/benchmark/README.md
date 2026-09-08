# Turbodata Benchmark Suite

Compares turbodata (`.td`) against MCAP, and surfaces the chunk-config and
read-strategy tradeoffs that turbodata is designed for.

## What this suite answers

| Objective | Bench | Status |
|---|---|---|
| 1. Is turbodata's **write** path competitive with MCAP's? | `BenchmarkWrite` | ✅ Phase A |
| 2. Is turbodata's **read** path competitive with MCAP's? | `BenchmarkRead` | ✅ Phase A |
| 3. How does **chunk config** affect read performance? | `BenchmarkChunkConfig` | ✅ Phase C |
| 4. When does **`WithReadStrategy`** pay off, per use case? | `BenchmarkUseCase` | ✅ Phase D |

Each benchmark answers exactly one question. Sanity checks (1, 2) compare
formats on a canonical fixture; showcases (3, 4) compare turbodata against
itself with different knobs.

## Results

These numbers come from the reference dataset described in
[Prerequisites](#prerequisites). Reproduce them with the commands in
[Generating a markdown report](#generating-a-markdown-report).

**Environment.** Intel i5-12400F (12 threads), go 1.25.0, Ubuntu 24.04 (WSL2),
warm OS page cache. Source MCAP: `go/test.mcap` (717 MiB, 6 image topics,
15 non-image topics, 28.5 s span), with `-mock-image-size=60KB` (default)
applied so image payloads are realistically incompressible. Working
fixtures sit at ~143 MiB.

**Reading the tables.** `bytes/op` and `reads/op` come from a `TrackingReadSeeker`
wrapping the file; `peak_heap_delta_B` is peak `runtime.MemStats.HeapInuse`
sampled by a 1 ms background poller, minus the baseline at sampler start.
Timing and IO metrics come from a no-`-heap` run (`-count=5 -benchtime=2s`);
heap metrics come from a separate `-heap` run (`-count=3 -benchtime=1s`).

### Obj 1 — Write (`BenchmarkWrite`)

| sub | ns/op | write_bytes/op | write_calls/op | B/op | peak_heap_delta_B |
|---|---:|---:|---:|---:|---:|
| mcap | 105.61 ms | 143.88 MiB | 6,648 | 23.29 MiB | 23.30 MiB |
| td   | 105.94 ms | 143.38 MiB |   144 |  9.26 MiB |  9.21 MiB |

Both writers complete one full pass in ~106 ms — within noise of each
other. TD batches IO into ~46× fewer `Write` calls (`bufio` + larger TD
chunks). Allocation churn is 2.5× lower for TD; live working set is also
2.5× lower.

<details><summary>Full table (10 columns)</summary>

| sub | ns/op | write_bytes/op | write_calls/op | B/op | allocs/op | total_alloc_B/op | peak_heap_B | peak_heap_delta_B | retained_heap_B |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| mcap | 105.61 ms | 143.88 MiB | 6.65k | 23.29 MiB | 5.98k | 23.29 MiB | 395.00 MiB | 23.30 MiB | 371.72 MiB |
| td   | 105.94 ms | 143.38 MiB | 144   |  9.26 MiB | 59.08k |  9.26 MiB | 380.85 MiB |  9.21 MiB | 371.68 MiB |

`peak_heap_B` and `retained_heap_B` are dominated by the in-memory
preloaded message stream (~371 MiB) that both writers consume; the
`peak_heap_delta_B` column subtracts that baseline and is the right
comparison.

</details>

### Obj 2 — Read (`BenchmarkRead`)

| sub | ns/op | bytes/op | reads/op | seeks/op | peak_heap_delta_B |
|---|---:|---:|---:|---:|---:|
| all/mcap | 37.36 ms | 143.68 MiB | 172 | 148 | 11.21 MiB |
| all/td   | 43.00 ms | 143.38 MiB | 288 | 287 |  7.48 MiB |

Full sequential read of the same content. MCAP is ~15% faster on warm
cache; it issues fewer Read+Seek calls (172 vs 288). TD pulls slightly
fewer total bytes from the disk and uses ~33% less peak heap. Neither
format leaks state across iterations (`retained_heap_B ≈ 3.7 MiB` for
both, see full table).

<details><summary>Full table (10 columns)</summary>

| sub | ns/op | bytes/op | reads/op | seeks/op | B/op | allocs/op | total_alloc_B/op | peak_heap_B | peak_heap_delta_B | retained_heap_B |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| all/mcap | 37.36 ms | 143.68 MiB | 172 | 148 | 13.48 MiB |  3.56k | 13.48 MiB | 14.83 MiB | 11.21 MiB | 3.65 MiB |
| all/td   | 43.00 ms | 143.38 MiB | 288 | 287 | 14.32 MiB | 53.63k | 14.37 MiB | 11.09 MiB |  7.48 MiB | 3.70 MiB |

</details>

### Obj 3 — Chunk config (`BenchmarkChunkConfig`)

Compares `canonical` (1 MiB chunks, all topics in one group, all
compressed) vs `improved` (per-image-topic groups, images uncompressed
in 1 MiB chunks; non-image topics co-located in one group with 64 KiB
chunks).

| scenario | canonical ns/op | improved ns/op | speedup | canonical bytes/op | improved bytes/op | reduction |
|---|---:|---:|---:|---:|---:|---:|
| all              | 43.09 ms | 23.62 ms |  1.8× | 143.38 MiB | 142.33 MiB |   1.0× |
| single_image     | 38.67 ms |  2.63 ms | 14.7× | 143.38 MiB |  23.68 MiB |   6.1× |
| single_nonimage  | 38.21 ms |  4.95 ms |  7.7× | 143.38 MiB | 278.14 KiB | 528.0× |
| selected_range   |  1.64 ms |  0.72 ms |  2.3× |   5.03 MiB |   2.13 MiB |   2.4× |

The improved config wins everywhere, but for different reasons in each
scenario:

- **`all`** — both fixtures hold the same data; the 1.8× speedup comes
  from skipping image-chunk decompression (improved has uncompressed
  image chunks).
- **`single_image`** — under canonical, every chunk holds messages from
  every topic, so filtering by topic still pays for full-file IO. Under
  improved each image topic owns its chunks → reader touches only 48 of
  them.
- **`single_nonimage`** (`/tf`) — the headline result. Reading just `/tf`
  drops from 143 MiB to 278 KiB (528× less). The 64 KiB non-image chunks
  are small enough that co-locating 15 topics doesn't pull much extra
  data when filtering for one.
- **`selected_range`** (1 image + `/tf` + 1 calibration, middle 1 s) —
  improved is 2.4× more selective on bytes; non-image co-location makes
  the small-topic part of the range a single chunk read.

These are not "the right" config — there is no golden config; users tune
for their workload. The point is to show how selectively tightening
grouping and chunk size cuts per-scenario IO by 1–2 orders of magnitude.

<details><summary>Full table (10 columns)</summary>

| sub | ns/op | bytes/op | reads/op | seeks/op | B/op | allocs/op | total_alloc_B/op | peak_heap_B | peak_heap_delta_B | retained_heap_B |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| all/canonical              | 43.09 ms | 143.38 MiB | 288 | 287 | 14.32 MiB | 53.63k | 14.38 MiB | 11.36 MiB |  7.89 MiB | 3.64 MiB |
| all/improved               | 23.62 ms | 142.33 MiB | 330 | 329 |  7.71 MiB | 42.14k |  7.90 MiB | 15.71 MiB | 14.24 MiB | 8.43 MiB |
| single_image/canonical     | 38.67 ms | 143.38 MiB | 288 | 287 | 14.32 MiB | 53.63k | 14.37 MiB | 11.08 MiB |  7.49 MiB | 3.68 MiB |
| single_image/improved      |  2.63 ms |  23.68 MiB |  48 |  47 |  1.21 MiB |  2.12k |  1.22 MiB |  3.86 MiB |  2.32 MiB | 2.65 MiB |
| single_nonimage/canonical  | 38.21 ms | 143.38 MiB | 288 | 287 | 14.25 MiB | 53.63k | 14.31 MiB | 11.37 MiB |  7.78 MiB | 3.68 MiB |
| single_nonimage/improved   |  4.95 ms | 278.14 KiB |  54 |  53 |  1.18 MiB | 35.49k |  1.17 MiB |  3.11 MiB |  1.44 MiB | 1.89 MiB |
| selected_range/canonical   |  1.64 ms |   5.03 MiB |  12 |  11 |  3.23 MiB |  3.02k |  3.24 MiB |  6.84 MiB |  3.27 MiB | 3.65 MiB |
| selected_range/improved    |  0.72 ms |   2.13 MiB |  10 |   9 |  1.39 MiB |  4.40k |  1.39 MiB |  4.27 MiB |  2.62 MiB | 2.91 MiB |

</details>

### Obj 4 — Use cases (`BenchmarkUseCase`)

Compares the default reader (no strategy) against `WithReadStrategy(...)`
across three workflows × multiple storage profiles. Each profile is
modeled by `LatencyReadSource` (RTT + `len(p)/perStreamBW` per
Read/ReadAt). Wall-clock under any non-trivial profile is *modeled*, not
measured CPU; trust `bytes/op`, `reads/op`, and `uUSD/op` for cross-row
comparisons.

| Profile | RTT | Per-stream BW | Bytes price | Request price |
|---|---|---|---|---|
| `local_nvme` | 20 µs | 3 GiB/s | — | — |
| `local_hdd` | 5 ms | 150 MiB/s | — | — |
| `cloud_obj_internal` | 20 ms | 80 MiB/s | $0 (free egress) | $1 × 10⁻⁶/req |
| `cloud_obj_public` | 20 ms | 80 MiB/s | $0.09/GiB | $1 × 10⁻⁶/req |

Each (use case, profile) pair benchmarks one carefully-chosen strategy
against `td_default`. We don't run mismatched strategies — picking a wrong
strategy demonstrates picking wrong, not strategy efficacy.

#### Labeling — single-image annotation, public S3

One image topic over the full timeline, served from `cloud_obj_public`.
Bytes are expensive; `StrategyForMoney` with `CoalesceGap = reqPrice/bytePrice`
(~12 KiB) tightly bounds wasted reads while amortizing per-request RTT.

| sub | ns/op | bytes/op | reads/op | uUSD/op | peak_heap_delta_B |
|---|---:|---:|---:|---:|---:|
| img_only/td_default       | 1.31 s   | 23.68 MiB | 48  | 2.13 mUSD |  1.25 MiB |
| img_only/td_strategy      | 394.83 ms | 23.68 MiB |  4 | 2.08 mUSD | 23.91 MiB |
| img_plus_small/td_default | 2.42 s   | 23.95 MiB | 100 | 2.21 mUSD |  2.66 MiB |
| img_plus_small/td_strategy | 405.73 ms | 23.95 MiB |  5 | 2.11 mUSD | 25.90 MiB |

Strategy is 3–6× faster on wall-clock and pays out 10–20× fewer requests.
Money savings are small (~3%) because both variants pull the same payload
and bytes dominate cost on public egress. The headline trade-off is
**memory for time**: strategy buffers the coalesced ranges (~24 MiB) in
flight; default streams a few hundred KiB at a time. For a labeling
worker that's an obvious win.

#### Training — selected topics in a 1 s window

Three topics (1 image + `/tf` + 1 calibration) within a 1 s window in
the middle of the file. Run on all four profiles to show that the right
strategy depends on storage class.

| sub | ns/op | bytes/op | reads/op | uUSD/op | peak_heap_delta_B |
|---|---:|---:|---:|---:|---:|
| local_nvme/td_default          |  10.63 ms | 2.13 MiB    | 10 | 0           | 2.60 MiB |
| local_nvme/td_strategy         |   4.21 ms | 889.74 KiB  | 18 | 0           | 1.44 MiB |
| local_hdd/td_default           |  74.70 ms | 2.13 MiB    | 10 | 0           | 2.57 MiB |
| local_hdd/td_strategy          |  44.06 ms | 891.07 KiB  |  6 | 0           | 1.29 MiB |
| cloud_obj_internal/td_default  | 237.92 ms | 2.13 MiB    | 10 | 10.00 µUSD  | 2.56 MiB |
| cloud_obj_internal/td_strategy |  95.90 ms | 891.07 KiB  |  5 |  5.00 µUSD  | 1.30 MiB |
| cloud_obj_public/td_default    | 237.92 ms | 2.13 MiB    | 10 | 197.50 µUSD | 2.55 MiB |
| cloud_obj_public/td_strategy   |  95.14 ms | 864.81 KiB  |  6 |  80.23 µUSD | 1.25 MiB |

Three observations:

1. Strategy is **always faster and always pulls fewer bytes** — selective
   reads inherently leave gaps, and the strategy's coalescing within
   bandwidth-latency-product collapses adjacent gaps without dragging in
   the rest of the file.
2. On `local_nvme` the strategy issues *more* reads (18 vs 10) — RTT is
   so cheap (20 µs) that splitting reads across the 8-way concurrency
   pool wins on wall-clock even though the request count goes up.
3. On `cloud_obj_public` the strategy halves wall-clock **and** money
   (197 µUSD → 80 µUSD/op): less data, fewer requests, both costs drop.

#### Viewing — interactive sample preview, 100 ms window

Same 3 topics, 100 ms window. Latency dominates. Profiles: `local_nvme`
(typical dev workstation) and `cloud_obj_internal` (training cluster
preview pane).

| sub | ns/op | bytes/op | reads/op | uUSD/op | peak_heap_delta_B |
|---|---:|---:|---:|---:|---:|
| local_nvme/td_default          |   6.26 ms |   1.07 MiB | 6 | 0          | 2.50 MiB |
| local_nvme/td_strategy         |   3.79 ms | 100.68 KiB | 5 | 0          | 496 KiB  |
| cloud_obj_internal/td_default  | 140.06 ms |   1.07 MiB | 6 | 6.00 µUSD  | 2.48 MiB |
| cloud_obj_internal/td_strategy |  85.15 ms | 100.68 KiB | 5 | 5.00 µUSD  | 456 KiB  |

For 100 ms window the strategy fetches **10× less data** (1.07 MiB →
100 KiB) — the BDP-driven CoalesceGap is small enough to cleanly skip the
unused parts of each chunk. On `cloud_obj_internal` interactive previews
go from 140 ms to 85 ms; on local NVMe from 6 ms to 4 ms. Real interactive
latency on a 1 GbE / cross-region link would be substantially worse for
the default reader and the gap would widen.

<details><summary>Full BenchmarkUseCase table (12 columns)</summary>

| sub | ns/op | bytes/op | reads/op | seeks/op | uUSD/op | B/op | allocs/op | total_alloc_B/op | peak_heap_B | peak_heap_delta_B | retained_heap_B |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| labeling/img_only/cloud_obj_public/td_default       | 1.31 s    | 23.68 MiB |  48 |  47 | 2.13 mUSD |  1.24 MiB |  2.16k |  1.25 MiB |  2.26 MiB |  1.25 MiB | 2.12 MiB |
| labeling/img_only/cloud_obj_public/td_strategy      | 394.83 ms | 23.68 MiB |   4 |   1 | 2.08 mUSD | 23.94 MiB |  2.29k | 23.95 MiB | 24.96 MiB | 23.91 MiB | 1.05 MiB |
| labeling/img_plus_small/cloud_obj_public/td_default | 2.42 s    | 23.95 MiB | 100 |  99 | 2.21 mUSD |  2.65 MiB | 36.69k |  2.58 MiB |  3.98 MiB |  2.66 MiB | 2.71 MiB |
| labeling/img_plus_small/cloud_obj_public/td_strategy| 405.73 ms | 23.95 MiB |   5 |   1 | 2.11 mUSD | 26.13 MiB | 37.06k | 26.13 MiB | 27.55 MiB | 25.90 MiB | 1.68 MiB |
| training/range/local_nvme/td_default           |  10.63 ms |   2.13 MiB |  10 |   9 | 0          | 1.39 MiB | 4.41k | 1.39 MiB | 4.09 MiB | 2.60 MiB | 2.77 MiB |
| training/range/local_nvme/td_strategy          |   4.21 ms | 889.74 KiB |  18 |   1 | 0          | 1.28 MiB | 3.93k | 1.29 MiB | 3.17 MiB | 1.44 MiB | 1.78 MiB |
| training/range/local_hdd/td_default            |  74.70 ms |   2.13 MiB |  10 |   9 | 0          | 1.39 MiB | 4.41k | 1.39 MiB | 4.24 MiB | 2.57 MiB | 2.93 MiB |
| training/range/local_hdd/td_strategy           |  44.06 ms | 891.07 KiB |   6 |   1 | 0          | 1.22 MiB | 3.88k | 1.23 MiB | 3.05 MiB | 1.29 MiB | 1.78 MiB |
| training/range/cloud_obj_internal/td_default   | 237.92 ms |   2.13 MiB |  10 |   9 | 10.00 µUSD | 1.39 MiB | 4.42k | 1.39 MiB | 4.22 MiB | 2.56 MiB | 2.90 MiB |
| training/range/cloud_obj_internal/td_strategy  |  95.90 ms | 891.07 KiB |   5 |   1 |  5.00 µUSD | 1.22 MiB | 3.88k | 1.23 MiB | 3.05 MiB | 1.30 MiB | 1.77 MiB |
| training/range/cloud_obj_public/td_default     | 237.92 ms |   2.13 MiB |  10 |   9 | 197.50 µUSD| 1.39 MiB | 4.42k | 1.39 MiB | 4.21 MiB | 2.55 MiB | 2.90 MiB |
| training/range/cloud_obj_public/td_strategy    |  95.14 ms | 864.81 KiB |   6 |   1 |  80.23 µUSD| 1.20 MiB | 3.88k | 1.20 MiB | 2.98 MiB | 1.25 MiB | 1.75 MiB |
| viewing/range_short/local_nvme/td_default          |   6.26 ms |   1.07 MiB |  6 |   5 | 0         | 1.36 MiB | 3.06k | 1.36 MiB | 4.21 MiB | 2.50 MiB | 2.94 MiB |
| viewing/range_short/local_nvme/td_strategy         |   3.79 ms | 100.68 KiB |  5 |   1 | 0         | 395.54 KiB | 2.48k | 406.57 KiB | 2.21 MiB |   496 KiB | 1.75 MiB |
| viewing/range_short/cloud_obj_internal/td_default  | 140.06 ms |   1.07 MiB |  6 |   5 | 6.00 µUSD | 1.36 MiB | 3.07k | 1.36 MiB | 4.17 MiB | 2.48 MiB | 2.93 MiB |
| viewing/range_short/cloud_obj_internal/td_strategy |  85.15 ms | 100.68 KiB |  5 |   1 | 5.00 µUSD | 395.56 KiB | 2.48k | 406.55 KiB | 2.16 MiB |   456 KiB | 1.73 MiB |

</details>

**Caveat on choosing strategies.** There is no golden formula. The
training/cloud_obj_internal row above uses `StrategyForLatency`, not
`StrategyForMoney`, despite "free in-region bytes" suggesting the latter.
The reason: `StrategyForMoney(byte=0)` sets `CoalesceGap = MaxInt64`
(merge anything), which for selective range reads collapses disjoint
small ranges into one whole-file fetch — a 27× over-read in our test.
Picking the right strategy requires understanding the workload's access
pattern, not just the storage's price sheet.

## Prerequisites

- Go 1.26+
- An input `.mcap` file with multiple image topics (`foxglove.RawImage`) and
  several non-image topics. The repo's `go/test.mcap` (717 MB, 6 image
  topics, 15 non-image topics) is the reference dataset.

## Quick start

```bash
cd go
go test ./benchmark/ -bench=. -benchmem -count=5 -benchtime=2s \
    -args -mcap=$PWD/test.mcap
```

On first run, `TestMain` builds four fixtures next to the input MCAP and
reuses them on subsequent runs:

- `<input>.mock.mcap` — input MCAP with every `foxglove.RawImage` payload
  replaced by 60 KiB of pseudo-random bytes. Real-world image data is
  almost always pre-compressed (JPEG / H.264) before being logged, so a raw
  MCAP gives zstd unrealistic compression ratios. The mock makes
  zstd-on-images effectively a no-op, which is what production looks like.
  Disable with `-mock-image-size=` (empty).
- `<input>.mock.mcap.canonical_1m_compressed.td` — turbodata, 1 MiB chunks,
  zstd, all topics in one group. Baseline for Obj 1, 2, 3.
- `<input>.mock.mcap.canonical_1m_compressed.mcap` — MCAP re-encoded with
  the same 1 MiB / zstd / CRC settings, so reads compare formats on
  equivalent encodings.
- `<input>.mock.mcap.improved.td` — turbodata, image topics in their own
  groups (uncompressed, 1 MiB chunks), all non-image topics co-located in
  one group (compressed, 64 KiB chunks). Used by Obj 3 and Obj 4.

When `-mock-image-size=` is empty the original `-mcap` is used as the
source.

## Flags

| Flag | Purpose |
|---|---|
| `-mcap=<path>` | Required. Path to input MCAP. |
| `-mock-image-size=<size>` | Replace `foxglove.RawImage` payloads with pseudo-random bytes of this size. Default `60KB`; set to `""` to disable. |
| `-heap` | Enable heap sampling (perturbs timings — run separately). |

Pass these after `-args` so the Go test runner doesn't intercept them:

```bash
go test ./benchmark/ -bench=. -args -mcap=... -heap
```

## Metrics

Standard metrics (always reported with `-benchmem`):

| Metric | Meaning |
|---|---|
| `ns/op` | Wall-clock per iteration |
| `B/op` | Total bytes allocated per op (testing's `b.ReportAllocs`) |
| `allocs/op` | Allocation count per op |

Read-bench custom metrics:

| Metric | Meaning |
|---|---|
| `bytes/op` | Bytes pulled through the IO layer (Read + ReadAt) |
| `reads/op` | Number of Read+ReadAt calls |
| `seeks/op` | Number of Seek calls |

Write-bench custom metrics:

| Metric | Meaning |
|---|---|
| `write_bytes/op` | Bytes flushed to the underlying writer |
| `write_calls/op` | Number of Write calls |

Heap metrics (only with `-heap`):

| Metric | Meaning |
|---|---|
| `peak_heap_B` | Max `runtime.MemStats.HeapInuse` observed by a 1 ms background poller. |
| `peak_heap_delta_B` | `peak_heap_B` minus the baseline at sampler start. Cancels out fixtures held in memory by the harness (e.g. preloaded messages in write benches), giving the bench's true working set. |
| `retained_heap_B` | `HeapInuse` after a forced GC at the last iteration boundary. Detects state that survives across iterations (leaks, accumulated indexes, etc.). |
| `total_alloc_B/op` | Cumulative allocation bytes / b.N |

The four metrics answer different questions and are all informative:

- `B/op` — allocation **churn** within one iteration (testing's built-in)
- `peak_heap_delta_B` — peak live **working set** during iterations
- `retained_heap_B` — heap **surviving across** iterations (leak detector)

Heavy churn with a small working set means the GC is keeping up. Large
working set means the bench holds many objects live concurrently. Large
retained means state outlives an iteration.

## Storage profiles (for objective 4)

Each profile pairs a per-call latency model with a cost model, so a single
local `.td` fixture can stand in for any storage class.

| Profile | RTT | Per-stream BW | Bytes price | Request price |
|---|---|---|---|---|
| `local_nvme` | 20µs | 3 GB/s | — | — |
| `local_hdd` | 5ms | 150 MB/s | — | — |
| `cloud_obj_internal` | 20ms | 80 MB/s | $0 (free egress) | $1e-6/req |
| `cloud_obj_public` | 20ms | 80 MB/s | $0.09/GiB | $1e-6/req |

Latency model: each `Read`/`ReadAt` sleeps `rtt + n / perStreamBW`. `Seek`
is free — real object stores have no Seek; cost is charged at the next Read.

## Generating a markdown report

Timing and heap measurement perturb each other: the `-heap` flag adds a
background poller and forces a GC at every iteration boundary, which slows
the bench by 10–20%. So the suite produces a report from **two runs** —
one for trustworthy timings, one for trustworthy heap numbers — merged by
`cmd/report`.

```bash
# Run A: timing + IO + alloc churn (no -heap).
go test ./benchmark/ -bench=. -benchmem -count=3 -benchtime=2s \
    -args -mcap=$PWD/test.mcap > bench_time.txt

# Run B: heap (with -heap; timings here are noisy and ignored).
go test ./benchmark/ -bench=. -benchmem -count=1 -benchtime=2s \
    -args -mcap=$PWD/test.mcap -heap > bench_heap.txt

# Merge into one Markdown report.
go run ./benchmark/cmd/report \
    -time bench_time.txt -heap bench_heap.txt > REPORT.md
```

`cmd/report` takes timing metrics (`ns/op`, `bytes/op`, `reads/op`,
`seeks/op`, `write_*`, `B/op`, `allocs/op`) from `-time` and heap metrics
(`peak_heap_B`, `peak_heap_delta_B`, `retained_heap_B`,
`total_alloc_B/op`) from `-heap`. Either flag may be omitted; if both are
omitted the tool reads stdin as a single run.

`benchstat` works on either raw file for statistical comparison across runs:

```bash
go install golang.org/x/perf/cmd/benchstat@latest

go test ./benchmark/ -bench=. -benchmem -count=10 -args -mcap=... > before.txt
# make changes
go test ./benchmark/ -bench=. -benchmem -count=10 -args -mcap=... > after.txt
benchstat before.txt after.txt
```

## Caveats

- **Run timing and heap separately.** `-heap` slows the bench (background
  poll + forced GCs); `ns/op` from a `-heap` run is unreliable. The report
  tool merges a `-time` run and a `-heap` run; do the same when reading
  raw output.
- **Page cache.** By default the OS page cache stays warm across iterations
  (this measures memory bandwidth, not disk). For cold-cache numbers:

  ```bash
  sync && echo 3 | sudo tee /proc/sys/vm/drop_caches
  go test ./benchmark/ -bench=BenchmarkRead -count=1 -benchtime=1x \
      -args -mcap=$PWD/test.mcap
  ```

- **Wall-clock under storage profiles is modeled, not measured.** When the
  use-case benchmarks (Phase D) inject latency via `time.Sleep`, the
  reported `ns/op` is dominated by sleep — trust `bytes/op` + `reads/op`
  for comparing strategies.

## Selecting specific benchmarks

```bash
# Just the write sanity check
go test ./benchmark/ -bench=BenchmarkWrite -args -mcap=...

# Just the read sanity check
go test ./benchmark/ -bench=BenchmarkRead -args -mcap=...

# Chunk-config showcase (Obj 3)
go test ./benchmark/ -bench=BenchmarkChunkConfig -args -mcap=...

# Only TD (skip MCAP baseline)
go test ./benchmark/ -bench='Read/all/td' -args -mcap=...
```

## Obj 3 — what BenchmarkChunkConfig answers

Compares a baseline (`canonical`) and a hand-tuned config (`improved`)
across four scenarios. The improved fixture makes four explicit changes
relative to canonical:

| Knob | canonical | improved |
|---|---|---|
| Image topic grouping | all topics in 1 group | one topic per group |
| Image compression | zstd | none |
| Non-image topic grouping | all topics in 1 group | all 15 in 1 group (same shape, but separate from images) |
| Non-image chunk size | 1 MiB | 64 KiB |

The point isn't that improved is "the right" config — there is no golden
config; users tune for their workload. It demonstrates how selectively
tightening grouping and chunk size can cut bytes/op by 1–2 orders of
magnitude on selective reads.

Sub-benchmarks: `BenchmarkChunkConfig/{scenario}/{config}`.

| Scenario | What it reads |
|---|---|
| `all` | every message |
| `single_image` | one image topic, full timeline |
| `single_nonimage` | the most-busy non-image topic (`/tf` if present) |
| `selected_range` | one image + the busy non-image + one more, middle 1s |

## Auxiliary tools

- `cmd/mcap-mock-compress` — standalone CLI for the same transform
  TestMain auto-applies. Useful for one-off conversions:

  ```bash
  go run ./benchmark/cmd/mcap-mock-compress \
      -in raw.mcap -out mock.mcap -size 60KB
  ```

- `cmd/mcap-analyze` — walks an MCAP and prints per-topic stats (count,
  size distribution, rate, share of total) plus chunk-config hints:

  ```bash
  go run ./benchmark/cmd/mcap-analyze -mcap path/to/file.mcap
  ```

- `cmd/report` — turns `go test -bench` output (timing run + heap run)
  into one merged Markdown report.

## File layout

```
benchmark/
  bench_test.go              TestMain, flags, package-level state
  fixtures.go                MCAP info loading + canonical/improved .td and .mcap builds
  scenarios.go               Format-agnostic Scenario + runMcap/runTd
  tracking_io.go             TrackingReadSeeker + TrackingWriter (IO counters)
  latency_source.go          StorageProfile + LatencyReadSource
  metrics.go                 reportReadIO + reportWriteIO + reportMoney + HeapSampler
  write_bench_test.go        Obj 1: BenchmarkWrite
  read_bench_test.go         Obj 2: BenchmarkRead
  chunkconfig_bench_test.go  Obj 3: BenchmarkChunkConfig
  usecase_bench_test.go      Obj 4: BenchmarkUseCase
  mockcompress/              library used by TestMain to auto-build .mock.mcap
  cmd/
    mcap-mock-compress/      thin CLI wrapper around mockcompress
    mcap-analyze/            per-topic stats for chunk-config decisions
    report/                  bench output → Markdown
```
