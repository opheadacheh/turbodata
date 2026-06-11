# turbodata format specification

This document specifies the on-disk layout of a turbodata (`.td`) file. The Go
SDK (`go/turbodata`) is the reference implementation; the Python and TypeScript
SDKs produce and consume byte-identical files. Anything described here is the
contract — an independent reader/writer that follows it will interoperate.

- **Status:** v0 (pre-1.0). See [Versioning](#versioning).
- **Endianness:** all fixed-width integers are **big-endian**.
- **Compression:** [Zstandard](https://datatracker.ietf.org/doc/html/rfc8878)
  (standard zstd frames).

## Terminology

| Term | Meaning |
| --- | --- |
| **Message** | One opaque payload (`bytes`) plus an `int64` timestamp. turbodata does not interpret payload bytes. |
| **Topic** | A named, metadata-tagged stream of messages, assigned a numeric `id`. |
| **Topic group** | A set of topics opened together that share data chunks. Messages from all topics in a group are interleaved by timestamp into the same chunks. |
| **Data chunk** | A contiguous, optionally zstd-compressed blob holding the concatenated payloads of the messages in one chunk of one group. |
| **Index chunk** | The per-data-chunk index: which topics/messages it contains, their timestamps, and their byte offsets. Always zstd-compressed. |
| **Summary** | The file-level directory: per group, the topic metadata and the list of index-chunk locations + time ranges. zstd-compressed. |
| **Footer** | A fixed 13-byte trailer: the summary length and the magic identifier. |

Timestamps are signed 64-bit integers in a unit chosen by the writer
(commonly nanoseconds). The only format-level requirement is that, within a
single topic group, messages are written in **non-decreasing timestamp order**.

## File layout

A turbodata file has **no leading header**; it is identified solely by the
trailing magic in the footer. The overall layout is:

```
+-------------------------------------------+  offset 0
|  Data chunks, group 0                     |
|  Data chunks, group 1                     |   message payloads
|  ...                                      |   (raw, or one zstd frame per chunk)
|  Data chunks, group N-1                   |
+-------------------------------------------+
|  Index chunks, group 0                    |
|  Index chunks, group 1                    |   one zstd frame per index chunk
|  ...                                      |
|  Index chunks, group N-1                  |
+-------------------------------------------+
|  Summary                                  |   one zstd frame
+-------------------------------------------+
|  Footer (13 bytes, uncompressed)          |
+-------------------------------------------+  EOF
```

All data chunks are written first (in group-open order), then all index chunks,
then the summary, then the footer. This lets a writer stream message bytes to
the output in a single forward pass and emit the index/summary only at the end,
while a reader can start from the footer and seek directly to what it needs.

## Primitive encodings

Three primitives are reused throughout:

| Primitive | Encoding |
| --- | --- |
| **intN** | `int8/16/32/64` / `uint8/16/32/64`, big-endian, fixed width. |
| **string** | `uint32` byte length (big-endian) followed by that many UTF-8 bytes. |
| **map** | `uint32` byte length (big-endian) followed by a [MessagePack](https://msgpack.org/)-encoded `map<string, any>`. |

Strings and maps are each capped at 64 MiB by the reference reader.

## Data chunk

A data chunk is the concatenation of the raw payload bytes of every message
assigned to that chunk, across all topics in the group, in write order
(non-decreasing timestamp). If the group is compressed, the entire concatenated
buffer is emitted as a single zstd frame; otherwise it is emitted raw.

Message boundaries are **not** stored inside the data chunk. A message's bytes
span `[offset, next_offset)` in the *uncompressed* chunk buffer, where:

- `offset` is the message's `OffsetInChunk` (from its index entry), and
- `next_offset` is the smallest `OffsetInChunk` greater than `offset` among all
  message-index entries of all topics in that chunk, or the chunk's
  `UncompressedLen` if there is none.

Because offsets are assigned from a single shared buffer per group, the offsets
of all messages in a chunk partition `[0, UncompressedLen)`.

## Index chunk

Each data chunk has exactly one index chunk. The index chunk is serialized as
below and then **zstd-compressed** as a whole before being written to the file.

```
IndexChunk:
  uint32                  topic_index_count
  TopicIndex[topic_index_count]
  int64                   chunk_offset       # file offset of the data chunk
  int64                   chunk_len          # on-disk (compressed) length of the data chunk
  int64                   uncompressed_len    # uncompressed length of the data chunk

TopicIndex:
  uint16                  topic_id
  uint32                  message_index_count
  MessageIndex[message_index_count]
  uint32                  key_frame_count
  uint32[key_frame_count]  # positions into MessageIndex[] that are key frames (video only)

MessageIndex:               # 16 bytes
  int64                   timestamp
  int64                   offset_in_chunk     # start of this message in the uncompressed chunk
```

A `TopicIndex` is present only for topics that actually have messages in that
chunk. `key_frame_count` is `0` for non-video topics.

## Summary

The summary is the file directory: one `TopicsInfo` per topic group, in
group-open order. It is serialized as below and then **zstd-compressed** as a
whole.

```
Summary:
  uint32                  topics_info_count
  TopicsInfo[topics_info_count]

TopicsInfo:
  uint32                  topic_metadata_count
  TopicMetadata[topic_metadata_count]
  uint32                  index_chunk_info_count
  IndexChunkInfo[index_chunk_info_count]
  int64                   total_len           # total on-disk bytes of this group's index chunks

TopicMetadata:
  uint16                  id
  string                  name
  map                     metadata            # caller-supplied, plus control flags (see below)
  uint32                  message_count       # number of messages written into this topic

IndexChunkInfo:             # 24 bytes
  int64                   start_timestamp     # min timestamp in the index chunk
  int64                   end_timestamp       # max timestamp in the index chunk
  int64                   offset              # file offset of the (compressed) index chunk
```

### Locating index chunks

A group's index chunks are stored contiguously, starting at
`index_chunk_info[0].offset` and spanning `total_len` bytes. The on-disk length
of index chunk `i` is therefore:

```
len(i) = index_chunk_info[i+1].offset - index_chunk_info[i].offset           # i < last
len(last) = total_len - index_chunk_info[last].offset + index_chunk_info[0].offset
```

### Reserved metadata keys

A topic's `metadata` map carries arbitrary caller key/values, plus reserved
control keys (namespaced with a `__td_` prefix) that the reader depends on:

| Key | Type | Meaning |
| --- | --- | --- |
| `__td_is_compressed` | bool | The group's data chunks are zstd-compressed. |
| `__td_is_video` | bool | This is a video topic (GOP-aware; see [Video topics](#video-topics)). |

(A third write-time-only key, `__td_chunk_config`, controls chunking but is
stripped before serialization and never appears on disk.)

## Footer

The footer is a fixed **13 bytes**, written uncompressed:

```
Footer:
  int64                   summary_len         # on-disk (compressed) length of the summary
  uint8[5]                magic = {'7','U','R','B','0'}   # 0x37 55 52 42 30
```

## Reading algorithm

1. Read the last `13` bytes (the footer). Verify `magic == "7URB0"`.
2. The summary occupies the `summary_len` bytes immediately preceding the
   footer, i.e. `[file_size - 13 - summary_len, file_size - 13)`. Read and
   zstd-decompress it, then parse. (A reader may speculatively read a larger
   tail window in one request to fetch footer + summary together.)
3. The summary gives, per group: topic metadata and the list of index-chunk
   offsets with their time ranges. Use the time ranges to skip index chunks
   outside a query window.
4. For each needed index chunk, read its bytes (offset from the summary, length
   computed as in [Locating index chunks](#locating-index-chunks)),
   zstd-decompress, and parse. It yields the per-message timestamps/offsets and
   the location (`chunk_offset`, `chunk_len`, `uncompressed_len`) of the data
   chunk.
5. Read the data chunk, decompress if the group is compressed, and slice out
   messages using consecutive `offset_in_chunk` values.

Because the index is separate from and far smaller than the payload data, a
reader can answer time/topic-bounded queries by fetching only the index chunks
and the data chunks that actually overlap the query.

## Writing algorithm

A writer streams output in one forward pass:

1. **Open a group** of one or more topics (assigning each an incrementing
   `id`), recording their metadata and a chunk-threshold configuration.
2. **Write messages** in non-decreasing timestamp order. Each payload is
   appended to the in-progress chunk buffer and a `MessageIndex` (timestamp +
   offset) is recorded. When the chunk threshold is reached, the chunk is
   flushed: optionally compressed, written to the file, and its `IndexChunk`
   recorded in memory.
3. **Close the group**, flushing any partial chunk.
4. Repeat for further groups.
5. On **close**: write all groups' index chunks (each compressed), then the
   compressed summary, then the footer.

### Chunk thresholds

A chunk is flushed when the in-progress chunk reaches a configurable threshold:

| Mode | Flush when |
| --- | --- |
| Size | accumulated payload bytes ≥ `size` |
| Duration | `last_timestamp - first_timestamp_in_chunk` ≥ `duration` |
| Count | message count ≥ `count` |

Smaller chunks give finer random-access granularity (less wasted I/O per query)
at the cost of a larger index and slightly worse compression; larger chunks
compress better but read coarser. The default is size-based at 1 MiB.

## Video topics

A topic opened as a video topic (`__td_is_video = true`) stores coded video
frames and has extra constraints so that any frame is randomly decodable:

- A video group contains **exactly one topic** and is **never co-compressed**
  (the bitstream is already compressed).
- The **first message of the topic must be a key frame**, otherwise the leading
  GOP would be undecodable.
- Chunk thresholds act as a **lower bound**: a chunk is only flushed when a key
  frame arrives *and* the threshold has been met. This guarantees a Group of
  Pictures (GOP) is never split across chunks, so decoding any frame requires
  at most one data chunk.
- `KeyFrameIndexes` in each `TopicIndex` lists the positions (within that
  chunk's `MessageIndexes`) that are key frames, so a reader can find the
  keyframe at or before a target frame and feed the decoder forward from there.

turbodata does not parse the bitstream; the writer is responsible for correctly
flagging key frames (IDR for H.264, IRAP for H.265, `KEY_FRAME` for AV1,
keyframe for VP9).

## Versioning

The format is currently **v0 / pre-1.0**. There is no explicit version field on
disk; the 5-byte magic `7URB0` is the only identifier. Backward-incompatible
changes before 1.0 may not be detectable by older readers beyond a parse
failure. Introducing an explicit version number is tracked for a future
revision; until then, treat the magic as identifying "the turbodata format as
described in this document."
