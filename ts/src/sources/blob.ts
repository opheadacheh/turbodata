// BlobReadSource: ReadSource backed by a Blob (or File, which extends Blob).
//
// Blob.slice() is synchronous, so read concurrency is implicit at the browser
// level — the only async work is decoding to Uint8Array via arrayBuffer().

import type { ReadSource } from "../read_source.js";

export class BlobReadSource implements ReadSource {
  private readonly blob: Blob;

  constructor(blob: Blob) {
    this.blob = blob;
  }

  async size(): Promise<bigint> {
    return BigInt(this.blob.size);
  }

  async read(offset: bigint, length: bigint): Promise<Uint8Array> {
    // Blob.slice takes number offsets. We accept that browsers can't host a
    // single Blob > 2^53 bytes anyway; clamp via Number() and validate.
    const start = Number(offset);
    const end = Number(offset + length);
    if (!Number.isSafeInteger(start) || !Number.isSafeInteger(end)) {
      throw new Error(
        `BlobReadSource.read: offset/length exceed Number.MAX_SAFE_INTEGER (offset=${offset}, length=${length})`,
      );
    }
    const slice = this.blob.slice(start, end);
    const buf = await slice.arrayBuffer();
    if (buf.byteLength !== Number(length)) {
      throw new Error(
        `BlobReadSource.read: short read at offset ${offset}, expected ${length}, got ${buf.byteLength}`,
      );
    }
    return new Uint8Array(buf);
  }
}
