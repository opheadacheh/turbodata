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

// Binary readers for turbodata records. Mirrors go/io.go.
//
// All multi-byte fields are big-endian (Go's encoding/binary.BigEndian default).

import { decodeMetadata } from "./msgpack.js";
import {
  FOOTER_LEN,
  type Footer,
  type IndexChunk,
  type IndexChunkInfo,
  type MessageIndex,
  type Summary,
  type TopicIndex,
  type TopicMetadata,
  type TopicsInfo,
} from "./types.js";

/** Mirrors go/io.go's maxStringLen / maxMapLen safety caps (64 MiB each). */
const MAX_STRING_LEN = 64 * 1024 * 1024;
const MAX_MAP_LEN = 64 * 1024 * 1024;

const UTF8 = new TextDecoder("utf-8", { fatal: false });

/**
 * Forward-only cursor over a Uint8Array. The Go reader uses io.Reader+
 * binary.Read; here we expose the equivalent calls. All reads are big-endian.
 *
 * We deliberately do NOT support seeking; everything in the format is
 * sequentially decodable from a known start point.
 */
export class BinaryReader {
  private readonly view: DataView;
  private readonly bytes: Uint8Array;
  private off: number;

  constructor(bytes: Uint8Array) {
    this.bytes = bytes;
    this.view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    this.off = 0;
  }

  get offset(): number {
    return this.off;
  }

  get remaining(): number {
    return this.bytes.byteLength - this.off;
  }

  private require(n: number): void {
    if (this.off + n > this.bytes.byteLength) {
      throw new Error(
        `unexpected EOF: need ${n} bytes at offset ${this.off}, have ${this.bytes.byteLength - this.off}`,
      );
    }
  }

  readUint8(): number {
    this.require(1);
    const v = this.view.getUint8(this.off);
    this.off += 1;
    return v;
  }

  readUint16(): number {
    this.require(2);
    const v = this.view.getUint16(this.off, false);
    this.off += 2;
    return v;
  }

  readUint32(): number {
    this.require(4);
    const v = this.view.getUint32(this.off, false);
    this.off += 4;
    return v;
  }

  readInt64(): bigint {
    this.require(8);
    const v = this.view.getBigInt64(this.off, false);
    this.off += 8;
    return v;
  }

  readBytes(n: number): Uint8Array {
    this.require(n);
    const out = this.bytes.subarray(this.off, this.off + n);
    this.off += n;
    return out;
  }
}

function readString(r: BinaryReader): string {
  const strLen = r.readUint32();
  if (strLen > MAX_STRING_LEN) {
    throw new Error(
      `string length ${strLen} exceeds maximum allowed ${MAX_STRING_LEN}`,
    );
  }
  const bytes = r.readBytes(strLen);
  return UTF8.decode(bytes);
}

function readMap(r: BinaryReader): Map<string, unknown> {
  const mapLen = r.readUint32();
  if (mapLen > MAX_MAP_LEN) {
    throw new Error(
      `map length ${mapLen} exceeds maximum allowed ${MAX_MAP_LEN}`,
    );
  }
  const buf = r.readBytes(mapLen);
  return decodeMetadata(buf);
}

/**
 * Parse a fixed-size (13-byte) footer.
 *
 * Layout: 8-byte int64 SummaryLen, then 5-byte Magic.
 */
export function readFooter(r: BinaryReader): Footer {
  const summaryLen = r.readInt64();
  const magic = r.readBytes(5);
  // Defensive copy: r.readBytes returns a subarray view into the source buffer.
  return { summaryLen, magic: new Uint8Array(magic) };
}

export function readTopicMetadata(r: BinaryReader): TopicMetadata {
  const id = r.readUint16();
  const name = readString(r);
  const metadata = readMap(r);
  const messageCount = r.readUint32();
  return { id, name, metadata, messageCount };
}

export function readIndexChunkInfo(r: BinaryReader): IndexChunkInfo {
  const startTimestamp = r.readInt64();
  const endTimestamp = r.readInt64();
  const offset = r.readInt64();
  return { startTimestamp, endTimestamp, offset };
}

export function readTopicsInfo(r: BinaryReader): TopicsInfo {
  const topicMetadataLen = r.readUint32();
  const topicMetadatas: TopicMetadata[] = new Array(topicMetadataLen);
  for (let i = 0; i < topicMetadataLen; i++) {
    topicMetadatas[i] = readTopicMetadata(r);
  }
  const indexChunkInfoLen = r.readUint32();
  const indexChunkInfoList: IndexChunkInfo[] = new Array(indexChunkInfoLen);
  for (let i = 0; i < indexChunkInfoLen; i++) {
    indexChunkInfoList[i] = readIndexChunkInfo(r);
  }
  const totalLen = r.readInt64();
  return { topicMetadatas, indexChunkInfoList, totalLen };
}

export function readSummary(r: BinaryReader): Summary {
  const topicsInfoLen = r.readUint32();
  const topicsInfos: TopicsInfo[] = new Array(topicsInfoLen);
  for (let i = 0; i < topicsInfoLen; i++) {
    topicsInfos[i] = readTopicsInfo(r);
  }
  return { topicsInfos };
}

export function readMessageIndex(r: BinaryReader): MessageIndex {
  const timestamp = r.readInt64();
  const offsetInChunk = r.readInt64();
  return { timestamp, offsetInChunk };
}

export function readTopicIndex(r: BinaryReader): TopicIndex {
  const id = r.readUint16();
  const messageIndexLen = r.readUint32();
  const messageIndexes: MessageIndex[] = new Array(messageIndexLen);
  for (let i = 0; i < messageIndexLen; i++) {
    messageIndexes[i] = readMessageIndex(r);
  }
  const keyFrameIndexLen = r.readUint32();
  const keyFrameIndexes = new Uint32Array(keyFrameIndexLen);
  for (let i = 0; i < keyFrameIndexLen; i++) {
    keyFrameIndexes[i] = r.readUint32();
  }
  return { id, messageIndexes, keyFrameIndexes };
}

export function readIndexChunk(r: BinaryReader): IndexChunk {
  const topicIndexesLen = r.readUint32();
  const topicIndexes: TopicIndex[] = new Array(topicIndexesLen);
  for (let i = 0; i < topicIndexesLen; i++) {
    topicIndexes[i] = readTopicIndex(r);
  }
  const chunkOffset = r.readInt64();
  const chunkLen = r.readInt64();
  const uncompressedLen = r.readInt64();
  return { topicIndexes, chunkOffset, chunkLen, uncompressedLen };
}

export { FOOTER_LEN };
