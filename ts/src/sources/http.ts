// Copyright 2026 Wanjia He
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// HttpRangeReadSource: ReadSource backed by HTTP Range requests.
//
// The browser's fetch caps concurrent connections per origin (~6 on HTTP/1.1,
// effectively unlimited mux on HTTP/2). The Reader's cost-aware path bounds
// concurrency to `maxConcurrency`; HTTP-level limits are an additional cap.
//
// `size()` is resolved lazily via a HEAD (or GET Range: bytes=0-0). We prefer
// HEAD because most static-file servers respond cheaply, but fall back to a
// 1-byte GET if the server forbids HEAD.

import type { ReadSource } from "../read_source.js";

export interface HttpRangeReadSourceOptions {
  /** Custom fetch implementation (e.g. for tests or auth headers). */
  fetch?: typeof fetch;
  /** Extra request init applied to every request (e.g. credentials, headers). */
  init?: RequestInit;
}

export class HttpRangeReadSource implements ReadSource {
  private readonly url: string;
  private readonly fetchImpl: typeof fetch;
  private readonly init: RequestInit;
  private cachedSize: bigint | undefined;

  constructor(url: string, opts: HttpRangeReadSourceOptions = {}) {
    this.url = url;
    this.fetchImpl = opts.fetch ?? fetch.bind(globalThis);
    this.init = opts.init ?? {};
  }

  async size(): Promise<bigint> {
    if (this.cachedSize !== undefined) {
      return this.cachedSize;
    }
    // Try HEAD first.
    let resp: Response;
    try {
      resp = await this.fetchImpl(this.url, { ...this.init, method: "HEAD" });
    } catch (err) {
      throw new Error(
        `HttpRangeReadSource: HEAD failed for ${this.url}: ${(err as Error).message}`,
      );
    }
    if (resp.ok) {
      const cl = resp.headers.get("content-length");
      if (cl !== null) {
        const v = BigInt(cl);
        this.cachedSize = v;
        return v;
      }
    }
    // Fallback: a 1-byte ranged GET, parse Content-Range total.
    const probe = await this.fetchImpl(this.url, {
      ...this.init,
      method: "GET",
      headers: { ...(this.init.headers ?? {}), Range: "bytes=0-0" },
    });
    if (!probe.ok && probe.status !== 206) {
      throw new Error(
        `HttpRangeReadSource: probe GET failed: ${probe.status} ${probe.statusText}`,
      );
    }
    const cr = probe.headers.get("content-range");
    if (cr === null) {
      throw new Error(
        `HttpRangeReadSource: server returned no Content-Length or Content-Range`,
      );
    }
    // Format: "bytes 0-0/12345"
    const m = /\/(\d+)$/.exec(cr);
    if (m === null) {
      throw new Error(`HttpRangeReadSource: malformed Content-Range: ${cr}`);
    }
    const v = BigInt(m[1]!);
    this.cachedSize = v;
    // Drain the probe body so the connection can be reused.
    await probe.arrayBuffer();
    return v;
  }

  async read(offset: bigint, length: bigint): Promise<Uint8Array> {
    if (length <= 0n) {
      return new Uint8Array(0);
    }
    const end = offset + length - 1n;
    const headers = new Headers(this.init.headers);
    headers.set("Range", `bytes=${offset.toString()}-${end.toString()}`);
    const resp = await this.fetchImpl(this.url, {
      ...this.init,
      method: "GET",
      headers,
    });
    if (!resp.ok && resp.status !== 206) {
      throw new Error(
        `HttpRangeReadSource: GET range ${offset}-${end} failed: ${resp.status} ${resp.statusText}`,
      );
    }
    const buf = new Uint8Array(await resp.arrayBuffer());
    if (BigInt(buf.byteLength) !== length) {
      throw new Error(
        `HttpRangeReadSource: short read at ${offset}, expected ${length}, got ${buf.byteLength}`,
      );
    }
    return buf;
  }
}
