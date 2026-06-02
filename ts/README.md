# turbodata (TypeScript)

Browser TypeScript reader for the turbodata format. Read-only port of the Go
reference implementation. Built primarily so web apps (e.g. Lichtblick) can
load `.td`/turbodata files locally (`Blob`/`File`) or remotely (HTTP Range
requests).

## Scope

- Read-only.
- Browser-targeted (no Node-specific APIs in core).
- Supports both the default lazy reader path and the cost-aware
  `WithReadStrategy` path, matching the Go API.

## Numeric types

| Go field type | TS type |
| --- | --- |
| `int64` (timestamps, offsets, lengths) | `bigint` |
| `uint8`, `uint16`, `uint32` | `number` |

This matches the convention used by `@mcap/core`. Mixing `bigint` and `number`
in arithmetic raises `TypeError` in JavaScript, so callers must pass `bigint`
values for `startTimestamp`, `endTimestamp`, etc.

## Quick start

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

## Buffer aliasing

By default, `msg.data` aliases an internal reusable buffer and is only valid
until the next iteration step. For Lichtblick-style code that posts messages
to other workers, pass `{ copy: true }`:

```ts
for await (const msg of reader.readMessages({ copy: true })) { ... }
```

## Topic remap

Present in-file topic names under different exposed names. The map is keyed by
in-file name and valued by the exposed name reported by `summary()`, emitted
from `readMessages`, and accepted by `topicNames` and `SampleQuery.topic`.
Names absent from the map pass through unchanged.

```ts
const reader = new Reader(source, { topicRemap: { "/cam": "/cam_v2" } });
for await (const msg of reader.readMessages({ topicNames: ["/cam_v2"] })) {
  console.log(msg.topicName); // "/cam_v2"
}
```

The remap is validated lazily against the file's summary on first use: it
throws `TopicRemapError` if two topics collapse onto the same exposed name.

## Reading several files (MultiReader)

`MultiReader` presents several single-file `Reader`s as one time-ordered
stream. Options pass through to each underlying reader, so topic filtering,
time bounds, order, and strategy apply per file before the merge. Topic-name
collisions across files are resolved with per-`Reader` `topicRemap`: names meant
to union share an exposed name; names meant to stay distinct are remapped apart.

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

Under `{ videoDecodable: true }`, any in-scope video topic supplied by more than
one reader must have time-disjoint source ranges; overlapping sources throw
`VideoSourcesOverlapError` (merging them would interleave frames from different
GOP chains into an undecodable stream).

## Cost-aware reading (HTTP Range)

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
