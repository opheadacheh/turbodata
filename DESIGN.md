# turbodata design notes

This document explains *why* turbodata exists and the reasoning behind its
design. For the byte-level contract, see [FORMAT.md](./FORMAT.md).

## The problem

A lot of valuable data is **multi-topic, timestamped streams**: a robot or
autonomous vehicle log with `/camera`, `/lidar`, `/imu`, `/gps`, `/odom`; an ML
training shard interleaving several sensor modalities; any time-series capture
with heterogeneous channels. The access patterns that matter for these are
rarely "read the whole file start to finish." They are:

- *"Give me `/camera` and `/imu` between t₁ and t₂."*
- *"For these 1,000 timestamps, give me the latest `/camera` frame at or before
  each."* (temporal alignment across topics for training/eval)
- *"Play this back in a browser, seeking around, fetching over HTTP."*

And increasingly the bytes don't live on local disk — they live in **object
storage** (S3/GCS), where every request has latency and (often) a price, so
*how* you read matters as much as *what* you read.

turbodata is a container format built specifically for **read-efficient,
random access** over exactly this kind of data, on both local disk and remote
object storage, from multiple languages (including the browser).

### Goals

- Fast topic- and time-bounded random reads without scanning the whole file.
- First-class multi-topic temporal alignment (floor sampling).
- Efficient reads over high-latency / paid object storage.
- Identical on-disk format across languages, so a file written anywhere reads
  anywhere — including read-only in the browser.
- Single-pass streaming writes.

### Non-goals

- In-place mutation or random writes. Files are written once, front to back.
- Interpreting payloads. Message bytes are opaque; schema/encoding is the
  caller's concern (carried in topic metadata if desired).
- A general database. There are no secondary indexes beyond time-per-topic.

## Key design decisions

### Index separated from data, summary at the end

The payload bytes (data chunks) come first; the index chunks, then the summary,
then a fixed footer come last. A reader starts at the footer, jumps to the
summary, and from there knows every topic, every index chunk, and its time
range — having read only a few KB. It then fetches only the index and data
chunks that overlap the query.

Putting the summary **at the end** is what makes single-pass writing possible:
the writer streams data chunks as messages arrive and only needs to know the
full directory once everything has been written. The cost — a reader can't
begin until it has seen the tail — is irrelevant for random access (you seek
anyway) and cheap to mitigate for streaming (read the tail first).

### Topic groups that share chunks

Topics opened together form a **group** and interleave their messages into
shared chunks. This is a deliberate knob: co-locating topics that are usually
read together (e.g. a stereo pair, or a sensor and its calibration) keeps their
bytes physically close, so one coalesced read serves them both. Topics that are
read independently can be opened in separate groups to keep their bytes — and
their chunk boundaries — apart.

### Chunking with configurable thresholds

Data is split into chunks by size, duration, or message count. Chunk size is
the central random-access trade-off:

- **Smaller chunks** → finer granularity, less wasted I/O per query, larger
  index, slightly worse compression ratios.
- **Larger chunks** → better compression, smaller index, coarser reads.

Making this per-group and configurable lets a writer tune for its data (e.g.
tiny IMU messages vs. large camera frames) instead of accepting one global
compromise.

### Per-chunk compression

Compression (Zstandard) is applied **per chunk**, not per file. This is what
keeps random access cheap under compression: to read one chunk you decompress
one frame, not the whole file. zstd was chosen for its strong ratio at high
decompression speed and its broad, well-maintained library support across Go,
Python, and JavaScript/WASM — a hard requirement for a cross-language format.

### Portable, flexible encoding

Fixed-width fields are **big-endian** integers with explicit widths, so the
layout is unambiguous across languages and architectures. Per-topic metadata is
an open `map<string, any>` encoded with **MessagePack**, so callers can attach
arbitrary structured metadata (schema name, encoding, calibration, frame ids)
without any format change, while the hot path (offsets, timestamps, lengths)
stays in tight fixed-width records.

### Floor sampling as a first-class operation

"For each of these query timestamps, give me the latest message at or before
it" is the core primitive for aligning multiple topics onto a common time base
(e.g. labeling camera frames with the most recent IMU reading). Doing this well
requires the index — binary-searching per-topic timestamps and fetching only
the chunks that contain the floor messages — so it belongs *in* the format's
reader, not bolted on by every caller. The reader plans all the needed reads up
front and issues them together.

### Cost-aware concurrent reads

This is the feature that most distinguishes turbodata. The default reader path
is a simple lazy, serial walk — minimal memory, ideal for local disk. But for
object storage that path is pathological: thousands of tiny serial GETs, each
paying a full round-trip.

So the reader has a second path driven by a `ReadStrategy`. It pre-plans every
byte range it will need, then:

- **coalesces** adjacent ranges separated by less than `CoalesceGap` into one
  request (trading a few wasted bytes for one fewer round-trip),
- **splits** any single range larger than `SplitThreshold` into parallel
  sub-reads (trading extra requests for lower wall-clock), and
- issues everything concurrently up to `MaxConcurrency`.

The right values depend on your storage's cost model, so the SDK ships explicit
constructors for the common ones:

- `StrategyForLatency(rtt, perStreamBW, concurrency)` — minimize wall-clock.
  Coalesce/split around the bandwidth-delay product so each parallel stream
  stays saturated for ~one RTT.
- `StrategyForMoney(reqPrice, bytePrice, concurrency)` — minimize spend where
  each request costs money (e.g. S3 GET). Coalesce aggressively (every avoided
  request is money saved up to `reqPrice/bytePrice` bytes) and never split
  (splits are extra paid requests).
- `StrategyForBlended(reqPrice, bytePrice, maxReadTime, perStreamBW, concurrency)`
  — minimize spend but cap any single read's latency.

Exposing the cost model rather than a single "fast" mode lets the same file be
read optimally from local disk, in-region S3, or cross-region storage just by
changing the strategy.

### Video: GOP-aware chunking

Coded video can't be decoded from an arbitrary frame — you need the key frame
that starts its Group of Pictures (GOP) and every frame since. turbodata makes
video topics randomly decodable by (1) treating chunk thresholds as a lower
bound and only ever cutting a chunk at a key frame, guaranteeing a GOP never
straddles two chunks, and (2) storing per-chunk key-frame positions in the
index. A reader can then fetch a single chunk, find the keyframe at or before
the target, and feed the decoder forward. turbodata never parses the bitstream;
the writer flags key frames, keeping the format codec-agnostic.

### Zero-copy iteration

The reader exposes a "next into a reusable buffer" iteration style: message
bytes alias an internal buffer that is reused across iterations, avoiding an
allocation per message on the hot path. Callers that need to retain a message
copy it explicitly (and the SDKs offer a `copy` option). This matters when
iterating millions of small messages.

### Multi-file reading

A `MultiReader` merges several files into one time-ordered stream, applying
filters per file before the merge and resolving topic-name collisions via
per-file topic remaps. This supports the common pattern of a recording split
across many files, or augmenting a base recording with a sidecar file, without
physically rewriting anything.

## Relationship to MCAP

turbodata is informed by [MCAP](https://mcap.dev/) (the Foxglove container for
robotics logs) and the toolchain interoperates with it — the Go examples and
benchmarks convert MCAP to `.td` and compare against it. turbodata is narrower
and more opinionated: it focuses on read-efficient random access and multi-topic
temporal alignment over local *and* object storage, with an explicit cost-aware
read planner and a browser-first read-only port. Where MCAP is a general,
schema-rich logging container, turbodata optimizes the specific
random-access/sampling workloads above.

## Multi-language strategy

- **Go** is the reference implementation and the only full read+write SDK. The
  format is whatever the Go implementation reads and writes.
- **Python** is a read+write port aimed at data/ML workflows (read training
  data, write preprocessed shards, sample for alignment).
- **TypeScript** is a read-only, browser-targeted port so web apps (e.g.
  Foxglove/Lichtblick-style viewers) can load `.td` files locally (`File`/`Blob`)
  or remotely (HTTP range), using the same cost-aware path.

Cross-language correctness is enforced by tests that read fixtures produced by
other SDKs and compare results, so the "write anywhere, read anywhere"
guarantee is checked, not just claimed.

## Trade-offs and limitations

- **Write-once.** No in-place edits; mutation means rewriting (or layering with
  `MultiReader`).
- **Monotonic per group.** Messages within a group must be non-decreasing in
  time; out-of-order producers must sort or split into groups.
- **No on-disk version field yet.** The format is pre-1.0; the magic is the
  only identifier (see [FORMAT.md](./FORMAT.md#versioning)).
- **Opaque payloads.** No built-in schema system; schema/encoding lives in
  topic metadata by convention.
