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

// Default-path per-group iterator. Lazily reads one index chunk and one data
// chunk at a time; mirrors go/topics_group_iterator.go but uses readAt
// (offset/length) instead of Read+Seek state.

import type { Decompressor } from "../compression.js";
import { BinaryReader, readIndexChunk } from "../io.js";
import type { ReadSource } from "../read_source.js";
import { sortAndFilter, type MessageRef } from "../sort_and_filter.js";
import { META_KEY_COMPRESSED, type IndexChunkInfo, type TopicsInfo } from "../types.js";

export class TopicsGroupIterator {
  private readonly rs: ReadSource;
  private readonly decompress: Decompressor;
  private readonly topicIds: Set<number>;
  private readonly startTimestamp: bigint;
  private readonly endTimestamp: bigint;
  private readonly reverse: boolean;
  private readonly isCompressed: boolean;
  private readonly videoDecodable: boolean;

  // Per-chunk metadata.
  private readonly indexChunkInfoList: IndexChunkInfo[];
  private readonly indexChunkInfoLens: bigint[];

  // Iteration cursors.
  private chunkIdx: number;
  private msgIdx: number;
  private currentMessages: MessageRef[] = [];
  private currentLens: bigint[] = [];

  // Most-recently-loaded data chunk (uncompressed bytes).
  private dataBuffer: Uint8Array | undefined;

  constructor(args: {
    rs: ReadSource;
    decompress: Decompressor;
    topicIds: Set<number>;
    startTimestamp: bigint;
    endTimestamp: bigint;
    reverse: boolean;
    topicsInfo: TopicsInfo;
    /** When true, snap the per-chunk lower bound back to the GOP key frame. */
    videoDecodable?: boolean;
  }) {
    this.rs = args.rs;
    this.decompress = args.decompress;
    this.topicIds = args.topicIds;
    this.startTimestamp = args.startTimestamp;
    this.endTimestamp = args.endTimestamp;
    this.reverse = args.reverse;
    this.videoDecodable = args.videoDecodable === true;

    const firstMeta = args.topicsInfo.topicMetadatas[0];
    const flag = firstMeta?.metadata.get(META_KEY_COMPRESSED);
    this.isCompressed = flag === true;

    // Time-range filter at the chunk level + compute compressed lengths.
    const filteredInfos: IndexChunkInfo[] = [];
    const filteredLens: bigint[] = [];
    const all = args.topicsInfo.indexChunkInfoList;
    for (let i = 0; i < all.length; i++) {
      const info = all[i]!;
      if (info.endTimestamp < args.startTimestamp) {
        continue;
      }
      if (info.startTimestamp > args.endTimestamp) {
        break;
      }
      filteredInfos.push(info);
      let ln: bigint;
      if (i < all.length - 1) {
        ln = all[i + 1]!.offset - info.offset;
      } else {
        // Last index chunk: its length is (group total) - (its offset) + (first chunk's offset).
        ln = args.topicsInfo.totalLen - info.offset + all[0]!.offset;
      }
      filteredLens.push(ln);
    }
    this.indexChunkInfoList = filteredInfos;
    this.indexChunkInfoLens = filteredLens;

    if (args.reverse) {
      this.chunkIdx = filteredInfos.length - 1;
    } else {
      this.chunkIdx = 0;
    }
    this.msgIdx = 0;
  }

  /** True if we have any in-scope index chunks. */
  hasAny(): boolean {
    return this.indexChunkInfoList.length > 0;
  }

  /**
   * Next emits one (timestamp, topicId, data) record or returns undefined on
   * EOF. The returned Uint8Array aliases an internal buffer; the caller must
   * copy if persistence is needed.
   */
  async next(): Promise<{
    timestamp: bigint;
    topicId: number;
    data: Uint8Array;
  } | undefined> {
    while (
      this.msgIdx >= this.currentMessages.length ||
      this.msgIdx < 0
    ) {
      if (
        this.chunkIdx >= this.indexChunkInfoList.length ||
        this.chunkIdx < 0
      ) {
        return undefined;
      }
      await this.loadCurrentChunk();
      this.chunkIdx += this.reverse ? -1 : 1;
      if (this.reverse) {
        this.msgIdx = this.currentMessages.length - 1;
      } else {
        this.msgIdx = 0;
      }
    }

    const ref = this.currentMessages[this.msgIdx]!;
    const len = this.currentLens[this.msgIdx]!;
    this.msgIdx += this.reverse ? -1 : 1;

    const start = Number(ref.offsetInChunk);
    const end = start + Number(len);
    const data = this.dataBuffer!.subarray(start, end);
    return { timestamp: ref.timestamp, topicId: ref.topicId, data };
  }

  private async loadCurrentChunk(): Promise<void> {
    const info = this.indexChunkInfoList[this.chunkIdx]!;
    const ln = this.indexChunkInfoLens[this.chunkIdx]!;

    // Read + decompress the index chunk.
    const compressedIndex = await this.rs.read(info.offset, ln);
    const indexBytes = this.decompress(compressedIndex);
    const indexChunk = readIndexChunk(new BinaryReader(indexBytes));

    const filtered = sortAndFilter(
      indexChunk.topicIndexes,
      indexChunk.uncompressedLen,
      this.topicIds,
      this.startTimestamp,
      this.endTimestamp,
      this.videoDecodable,
    );
    this.currentMessages = filtered.msgs;
    this.currentLens = filtered.lens;

    if (filtered.msgs.length === 0) {
      this.dataBuffer = undefined;
      return;
    }

    // Read (and possibly decompress) the data chunk.
    const dataBytes = await this.rs.read(
      indexChunk.chunkOffset,
      indexChunk.chunkLen,
    );
    this.dataBuffer = this.isCompressed ? this.decompress(dataBytes) : dataBytes;
  }
}
