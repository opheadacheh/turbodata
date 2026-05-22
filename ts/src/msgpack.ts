// msgpack decoder for TopicMetadata.Metadata. Uses @msgpack/msgpack with
// useBigInt64 so int64 values round-trip exactly.

import { decode } from "@msgpack/msgpack";

/**
 * Decode a msgpack-encoded map into a Map<string, unknown>.
 *
 * - `useBigInt64: true` so any int64 values round-trip exactly as bigint.
 *   Smaller integers (int32 and below) decode as number.
 * - The Go writer always msgpack-encodes a `map[string]any`. We coerce the
 *   decoded plain object into a `Map` for an unambiguous string-keyed view.
 */
export function decodeMetadata(buf: Uint8Array): Map<string, unknown> {
  const decoded = decode(buf, { useBigInt64: true });
  const out = new Map<string, unknown>();
  if (decoded === null || typeof decoded !== "object") {
    return out;
  }
  if (decoded instanceof Map) {
    for (const [k, v] of decoded as Map<unknown, unknown>) {
      if (typeof k === "string") {
        out.set(k, v);
      }
    }
    return out;
  }
  for (const k of Object.keys(decoded as Record<string, unknown>)) {
    out.set(k, (decoded as Record<string, unknown>)[k]);
  }
  return out;
}
