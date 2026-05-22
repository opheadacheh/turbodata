// Cost-aware per-group iterator: all bytes pre-fetched into LoadedBytes;
// per-chunk kept-message lists pre-computed at setup time. Mirrors
// go/cost_aware_group_iterator.go.

import type { Decompressor } from "../compression.js";
import type { LoadedBytes } from "../loaded_bytes.js";
import type { MessageRef } from "../sort_and_filter.js";
import type { IndexChunk } from "../types.js";

export class CostAwareGroupIterator {
  private readonly isCompressed: boolean;
  private readonly reverse: boolean;
  private readonly indexChunks: IndexChunk[];
  private readonly chunkMessages: MessageRef[][];
  private readonly chunkMsgLens: bigint[][];
  private readonly loadedData: LoadedBytes;
  private readonly decompress: Decompressor;

  // Decompressed buffer for the *current* compressed data chunk; undefined
  // when isCompressed=false or no chunk has been materialized yet.
  private currentDecompressed: Uint8Array | undefined;
  private loadedChunkIdx = -1;

  private chunkIdx: number;
  private msgIdx: number;

  constructor(args: {
    isCompressed: boolean;
    reverse: boolean;
    indexChunks: IndexChunk[];
    chunkMessages: MessageRef[][];
    chunkMsgLens: bigint[][];
    loadedData: LoadedBytes;
    decompress: Decompressor;
  }) {
    this.isCompressed = args.isCompressed;
    this.reverse = args.reverse;
    this.indexChunks = args.indexChunks;
    this.chunkMessages = args.chunkMessages;
    this.chunkMsgLens = args.chunkMsgLens;
    this.loadedData = args.loadedData;
    this.decompress = args.decompress;

    if (args.reverse) {
      this.chunkIdx = args.indexChunks.length - 1;
      this.msgIdx =
        this.chunkIdx >= 0
          ? args.chunkMessages[this.chunkIdx]!.length - 1
          : 0;
    } else {
      this.chunkIdx = 0;
      this.msgIdx = 0;
    }
  }

  /**
   * Next is synchronous (no I/O remaining). Returns undefined on EOF.
   * `data` aliases either loadedData or the per-chunk decompressed buffer.
   */
  next():
    | { timestamp: bigint; topicId: number; data: Uint8Array }
    | undefined {
    while (true) {
      if (this.chunkIdx < 0 || this.chunkIdx >= this.indexChunks.length) {
        return undefined;
      }
      const msgs = this.chunkMessages[this.chunkIdx]!;
      if (this.msgIdx < 0 || this.msgIdx >= msgs.length) {
        this.advanceChunk();
        continue;
      }

      // Materialize this chunk's decompressed bytes on first touch.
      if (this.isCompressed && this.loadedChunkIdx !== this.chunkIdx) {
        const compressed = this.loadedData.get(
          this.indexChunks[this.chunkIdx]!.chunkOffset,
        );
        if (compressed === undefined) {
          throw new Error(
            `cost-aware iterator: missing compressed chunk at offset ${this.indexChunks[this.chunkIdx]!.chunkOffset}`,
          );
        }
        this.currentDecompressed = this.decompress(compressed);
        this.loadedChunkIdx = this.chunkIdx;
      }

      const msg = msgs[this.msgIdx]!;
      const length = this.chunkMsgLens[this.chunkIdx]![this.msgIdx]!;
      const chunkOffset = this.indexChunks[this.chunkIdx]!.chunkOffset;

      let data: Uint8Array;
      if (this.isCompressed) {
        const start = Number(msg.offsetInChunk);
        const end = start + Number(length);
        data = this.currentDecompressed!.subarray(start, end);
      } else {
        const got = this.loadedData.get(chunkOffset + msg.offsetInChunk);
        if (got === undefined) {
          throw new Error(
            `cost-aware iterator: missing message bytes at offset ${chunkOffset + msg.offsetInChunk}`,
          );
        }
        data = got;
      }

      this.advanceMessage();
      return { timestamp: msg.timestamp, topicId: msg.topicId, data };
    }
  }

  private advanceMessage(): void {
    this.msgIdx += this.reverse ? -1 : 1;
  }

  private advanceChunk(): void {
    if (this.reverse) {
      this.chunkIdx -= 1;
      if (this.chunkIdx >= 0) {
        this.msgIdx = this.chunkMessages[this.chunkIdx]!.length - 1;
      }
    } else {
      this.chunkIdx += 1;
      if (this.chunkIdx < this.indexChunks.length) {
        this.msgIdx = 0;
      }
    }
  }
}
