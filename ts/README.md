# turbodata (TypeScript)

Browser TypeScript reader for the turbodata format. **Read-only** port of the
Go reference implementation. Built primarily so web apps (e.g. Lichtblick) can
load `.td`/turbodata files locally (`Blob`/`File`) or remotely (HTTP Range
requests). The on-disk format is identical across SDKs, so files written by Go
or Python read back here unchanged.

```bash
npm install turbodata
```

See the [top-level README](../README.md) for scope and the format overview.
This guide is API usage by feature: **read → sample → multi-file** (writing is
not supported in the browser SDK). Runnable versions of every snippet live in
[`examples/`](./examples).

## Numeric types

| Go field type | TS type |
| --- | --- |
| `int64` (timestamps, offsets, lengths) | `bigint` |
| `uint8`, `uint16`, `uint32` | `number` |

This matches the convention used by `@mcap/core`. Mixing `bigint` and `number`
in arithmetic raises `TypeError` in JavaScript, so callers must pass `bigint`
values for `startTimestamp`, `endTimestamp`, etc.

## Read

```ts
import { Reader, BlobReadSource } from "turbodata";

const file: File = /* from <input type="file"> */;
const reader = new Reader(new BlobReadSource(file));

for await (const msg of reader.readMessages()) {
  // msg.timestamp: bigint
  // msg.topicName: string
  // msg.data: Uint8Array (aliases an internal buffer; copy to persist)
}
```

### Buffer aliasing

By default, `msg.data` aliases an internal reusable buffer and is only valid
until the next iteration step. For Lichtblick-style code that posts messages
to other workers, pass `{ copy: true }` to get a fresh `Uint8Array` per
message:

```ts
for await (const msg of reader.readMessages({ copy: true })) { ... }
```

### Order, time, and topic filters

Topics not listed are skipped; chunks fully outside the `[start, end]` window
are never fetched. Timestamps are `bigint`.

```ts
for await (const msg of reader.readMessages({
  order: "reverse-time",                 // default: "time"
  topicNames: ["/imu", "/cam"],          // default: all topics
  startTimestamp: 1_000n,                // inclusive lower bound
  endTimestamp: 2_000n,                  // inclusive upper bound
})) { ... }
```

### Cost-aware reading (HTTP Range)

Pass a `strategy` to switch onto the cost-aware concurrent path — it pre-plans
every needed byte range, coalesces neighbors, optionally splits large reads,
and fetches concurrently. Without a strategy, the reader uses the default lazy
path. Helper constructors: `StrategyForLatency`, `StrategyForMoney`,
`StrategyForBlended`.

```ts
import { Reader, HttpRangeReadSource, StrategyForLatency } from "turbodata";

const reader = new Reader(new HttpRangeReadSource(url));
for await (const msg of reader.readMessages({
  strategy: StrategyForLatency({
    rttSeconds: 0.05,
    perStreamBytesPerSec: 8_000_000,
    concurrency: 8,
  }),
})) { ... }
```

### Video

A non-key frame is only decodable after its GOP's key frame, so a plain time
filter that starts mid-GOP yields bytes a decoder can't cold-start on. Pass
`{ videoDecodable: true }` to snap the effective `startTimestamp` back to the
latest key frame at or before it. Non-video topics are unaffected.

```ts
for await (const msg of reader.readMessages({
  topicNames: ["/cam/h264"],
  startTimestamp: 650n,    // mid-GOP
  videoDecodable: true,    // snaps back to the key frame at/<=650
})) { ... }
```

## Sample (floor lookup at concrete timestamps)

`sample` returns the floor message per `(topic, timestamp)` pair: the most
recent message at or before each requested timestamp. `out[i][j]` corresponds
to `queries[i].timestamps[j]`; `found` is false when nothing precedes the
query. Timestamps within a query must be strictly increasing.

```ts
const out = await reader.sample([
  { topic: "/imu", timestamps: [150n, 350n] },
  { topic: "/cam", timestamps: [1_000n, 2_000n] },
]);
for (const row of out) {
  for (const res of row) {
    if (res.found) console.log(res.timestamp, res.data.length);
  }
}
```

Pass `{ strategy }` to override the default sample cost model for your backend.

### Video

With `{ videoDecodable: true }`, results for a video topic carry a
decoder-ready GOP sequence in `frames` (and `data` is empty) instead of the
single floor frame, and set `isVideo`. Within a row, the first result in each
GOP sets `resetDecoder: true` with the full prefix `[keyframe ... target]`;
later results in the same GOP carry only the new frames since the previous
query.

```ts
const out = await reader.sample(
  [{ topic: "/cam", timestamps: [25n, 55n] }],
  { videoDecodable: true },
);
for (const res of out[0]) {
  if (res.found) {
    // feed res.frames to a decoder, resetting it first when res.resetDecoder.
    console.log(res.timestamp, res.resetDecoder, res.frames.length);
  }
}
```

## Reading several files (MultiReader)

`MultiReader` presents several single-file `Reader`s as one time-ordered
stream. Options pass through to each underlying reader, so topic filtering,
time bounds, order, and strategy apply per file before the merge. Topic-name
collisions across files are resolved with per-`Reader` `topicRemap`: names
meant to union share an exposed name; names meant to stay distinct are
remapped apart.

```ts
import { MultiReader, Reader } from "turbodata";

const mr = new MultiReader([
  new Reader(sourceA),
  // Keep file B's "/cam" distinct instead of unioning it with A's.
  new Reader(sourceB, { topicRemap: { "/cam": "/cam_b" } }),
]);

for await (const msg of mr.readMessages()) {
  console.log(msg.timestamp, msg.topicName);
}

// sample() returns, per cell, the latest floor across the union of files.
const out = await mr.sample([{ topic: "/cam", timestamps: [150n, 350n] }]);
```

Under `{ videoDecodable: true }`, any in-scope video topic supplied by more
than one reader must have time-disjoint source ranges; overlapping sources
throw `VideoSourcesOverlapError` (merging them would interleave frames from
different GOP chains into an undecodable stream).

### Topic remap

`topicRemap` is keyed by in-file name and valued by the exposed name reported
by `summary()`, emitted from `readMessages`, and accepted by `topicNames` and
`SampleQuery.topic`. Names absent from the map pass through unchanged. It works
on a single `Reader` too:

```ts
const reader = new Reader(source, { topicRemap: { "/cam": "/cam_v2" } });
for await (const msg of reader.readMessages({ topicNames: ["/cam_v2"] })) {
  console.log(msg.topicName); // "/cam_v2"
}
```

The remap is validated lazily against the file's summary on first use: it
throws `TopicRemapError` if two topics collapse onto the same exposed name.

## Examples

Runnable examples live in [`examples/`](./examples). They run under Node via
[`tsx`](https://github.com/privatenumber/tsx) by wrapping a committed test
fixture's bytes in a `BlobReadSource`, so no writer step is needed:

```bash
cd ts
npm install
npm run example:read       # defaults, order, topic + time filters, strategy, video snap-back
npm run example:sample     # floor lookup, multi-topic, strategy, video GOP-prefix
npm run example:multiread  # union, split-via-remap, cross-file sample
```

### Schema metadata

The package exports `META_KEY_SCHEMA_NAME`, `META_KEY_SCHEMA_ENCODING`, and
`META_KEY_SCHEMA_DATA` for accessing each topic's metadata map. The first two
values are strings; schema data decodes as `Uint8Array`. Older files may omit
these keys. Reading does not warn or automatically decode message payloads.
