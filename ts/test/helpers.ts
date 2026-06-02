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

// Test helpers shared across tests.

import { createHash } from "node:crypto";
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";

import {
  Reader,
  type Message,
  type ReadOptions,
  type ReadSource,
} from "../src/index.js";

const FIXTURES_DIR = new URL("./fixtures/", import.meta.url).pathname;

export interface Golden {
  topics: { id: number; name: string; metadataJson: string }[];
  messageCount: number;
  streamSha256: string;
  firstLine: string;
  lastLine: string;
}

export function listFixtures(): string[] {
  return readdirSync(FIXTURES_DIR)
    .filter((f) => f.endsWith(".td"))
    .sort();
}

export function fixturePath(name: string): string {
  return join(FIXTURES_DIR, name);
}

export function readFixture(name: string): Uint8Array {
  return new Uint8Array(readFileSync(fixturePath(name)));
}

export function readGolden(name: string): Golden {
  const text = readFileSync(fixturePath(name) + ".golden.txt", "utf8");
  const lines = text.split("\n").filter((l) => l.length > 0);
  const topics: Golden["topics"] = [];
  let messageCount = 0;
  let streamSha256 = "";
  let firstLine = "";
  let lastLine = "";
  for (const line of lines) {
    if (line.startsWith("topics:")) {
      // Count is informational; topics array is filled by `topic ` lines.
      continue;
    }
    if (line.startsWith("topic ")) {
      // "topic <id> <name> <json>"
      const m = /^topic (\d+) (\S+) (.*)$/.exec(line);
      if (m === null) {
        throw new Error(`malformed topic line: ${line}`);
      }
      topics.push({
        id: Number(m[1]),
        name: m[2]!,
        metadataJson: m[3]!,
      });
      continue;
    }
    if (line.startsWith("messages:")) {
      messageCount = Number(line.slice("messages:".length));
      continue;
    }
    if (line.startsWith("stream-sha256:")) {
      streamSha256 = line.slice("stream-sha256:".length);
      continue;
    }
    if (line.startsWith("first ")) {
      firstLine = line.slice("first ".length);
      continue;
    }
    if (line.startsWith("last ")) {
      lastLine = line.slice("last ".length);
      continue;
    }
  }
  return { topics, messageCount, streamSha256, firstLine, lastLine };
}

/**
 * Compute the same stream hash that go/cmd/gen-fixtures emits:
 *   for each yielded message:
 *     write 8 bytes big-endian timestamp
 *     write utf-8 topicName bytes
 *     write 0x00 separator
 *     write data bytes
 *     write 0x00 separator
 *
 * Also captures count, first message line, last message line.
 */
export interface IterationStats {
  hash: string;
  count: number;
  firstLine: string;
  lastLine: string;
}

export async function iterateAndHash(
  reader: Reader,
  opts?: ReadOptions,
): Promise<IterationStats> {
  const hasher = createHash("sha256");
  const sep = new Uint8Array([0]);
  const tsBuf = new Uint8Array(8);
  const tsView = new DataView(tsBuf.buffer);
  const enc = new TextEncoder();
  let count = 0;
  let firstLine = "";
  let lastLine = "";
  for await (const msg of reader.readMessages(opts)) {
    tsView.setBigInt64(0, msg.timestamp, false);
    hasher.update(tsBuf);
    hasher.update(enc.encode(msg.topicName));
    hasher.update(sep);
    hasher.update(msg.data);
    hasher.update(sep);

    if (count === 0 || count + 1 === Number.MAX_SAFE_INTEGER) {
      // capture first
    }
    const line = `${msg.timestamp} ${msg.topicName} ${sha256Hex(msg.data)}`;
    if (count === 0) {
      firstLine = line;
    }
    lastLine = line;
    count++;
  }
  return { hash: hasher.digest("hex"), count, firstLine, lastLine };
}

function sha256Hex(data: Uint8Array): string {
  return createHash("sha256").update(data).digest("hex");
}

/** Node-side ReadSource for tests: in-memory bytes. */
export class BytesReadSource implements ReadSource {
  private readonly bytes: Uint8Array;
  public reads = 0;

  constructor(bytes: Uint8Array) {
    this.bytes = bytes;
  }

  async size(): Promise<bigint> {
    return BigInt(this.bytes.byteLength);
  }

  async read(offset: bigint, length: bigint): Promise<Uint8Array> {
    this.reads++;
    const start = Number(offset);
    const end = start + Number(length);
    if (end > this.bytes.byteLength) {
      throw new Error(
        `BytesReadSource: short read at ${offset}, len ${length}, size ${this.bytes.byteLength}`,
      );
    }
    // Defensive copy: same semantics as a real network/file read.
    return new Uint8Array(this.bytes.subarray(start, end));
  }
}

export type { Message };
