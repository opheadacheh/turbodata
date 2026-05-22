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
