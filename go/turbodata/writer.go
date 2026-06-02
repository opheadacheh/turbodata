package turbodata

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"math"

	"turbodata/format"
	"turbodata/internal/buffer"
	"turbodata/internal/compress"
)

type WriterConfig struct {
	namesToIds   map[string]uint16
	topicIds     []uint16
	chunkConfig  *ChunkConfig
	isCompressed bool

	// isVideo is true when this group was opened with WithVideoTopic.
	// Gates one-topic-per-group, keyframe-aware chunking, and the
	// WriteMessage / WriteVideoMessage routing checks.
	isVideo bool
}

type Writer struct {
	// IO resources.
	w           io.Writer
	bw          *bufio.Writer
	buf         *bytes.Buffer
	compressBuf *buffer.ReusableBuffer

	// Chunk-level state.
	chunkStatus        *ChunkStatus
	idToMessageIndexes map[uint16][]format.MessageIndex
	// idToKeyFrameIndexes accumulates per-topic key frame positions
	// (indexes into idToMessageIndexes[id]) for the in-progress chunk.
	// Reset per topic at chunk flush. Non-video topics never populate this.
	idToKeyFrameIndexes map[uint16][]uint32

	// Topic-level state.
	lastTimestamp int64
	isTopicOpen   bool
	writerConfig  *WriterConfig
	indexChunks   []*format.IndexChunk

	// File-level state.
	offset          int64
	currentTopicId  uint16
	footer          *format.Footer
	summary         *format.Summary
	indexChunksList [][]*format.IndexChunk
}

func NewWriter(w io.Writer) *Writer {
	bw := bufio.NewWriterSize(w, 128*1024)
	return &Writer{
		w:  bw,
		bw: bw,
		footer: &format.Footer{
			Magic: format.Magic,
		},
		summary: &format.Summary{
			TopicsInfos: []*format.TopicsInfo{},
		},
		buf: bytes.NewBuffer(nil),
		compressBuf: &buffer.ReusableBuffer{
			Data: make([]byte, 0),
		},
		idToMessageIndexes:  make(map[uint16][]format.MessageIndex),
		idToKeyFrameIndexes: make(map[uint16][]uint32),
	}
}

func (w *Writer) OpenTopics(names []string, metadatas []map[string]any, opts ...WriteOption) error {
	if w.isTopicOpen {
		return ErrTopicAlreadyOpen
	}
	if len(names) != len(metadatas) {
		return ErrNamesMetadatasMismatch
	}
	if len(names) == 0 {
		return ErrNoTopicsToOpen
	}

	w.isTopicOpen = true
	w.lastTimestamp = 0

	for i := range metadatas {
		for _, opt := range opts {
			if err := opt(metadatas[i]); err != nil {
				return err
			}
		}
	}

	// Detect whether any topic in this group was opened with WithVideoTopic.
	// Video groups are constrained: exactly one topic, never co-compressed.
	anyVideo := false
	for i := range metadatas {
		if v, ok := metadatas[i][format.MetaKeyVideo].(bool); ok && v {
			anyVideo = true
			break
		}
	}
	if anyVideo {
		if len(names) != 1 {
			return ErrVideoGroupMustBeSingleTopic
		}
		if v, ok := metadatas[0][format.MetaKeyCompressed].(bool); ok && v {
			return ErrVideoTopicCannotBeCompressed
		}
	}

	topicMedatas := make([]*format.TopicMetadata, len(names))
	namesToIds := make(map[string]uint16)
	topicIds := make([]uint16, len(names))
	for i := range names {
		w.currentTopicId++
		topicMedatas[i] = &format.TopicMetadata{
			Id:       w.currentTopicId,
			Name:     names[i],
			Metadata: metadatas[i],
		}

		namesToIds[names[i]] = w.currentTopicId
		topicIds[i] = w.currentTopicId
		w.idToMessageIndexes[w.currentTopicId] = []format.MessageIndex{}
		w.idToKeyFrameIndexes[w.currentTopicId] = []uint32{}
	}

	w.summary.TopicsInfos = append(w.summary.TopicsInfos, &format.TopicsInfo{
		TopicMetadatas:     topicMedatas,
		IndexChunkInfoList: []*format.IndexChunkInfo{},
		TotalLen:           0,
	})

	chunkConfig, ok := metadatas[0][format.MetaKeyChunkConfig].(*ChunkConfig)
	if !ok {
		chunkConfig = &ChunkConfig{
			Mode: ChunkThresholdModeSize,
			Size: 1024 * 1024,
		}
	}

	// chunk_config is write-time-only state; strip it from every topic's
	// metadata so it never reaches the persisted summary. is_compressed and
	// is_video are intentionally retained: the reader depends on them.
	for i := range metadatas {
		delete(metadatas[i], format.MetaKeyChunkConfig)
	}

	isCompressed, ok := metadatas[0][format.MetaKeyCompressed].(bool)
	if !ok {
		isCompressed = false
	}

	w.writerConfig = &WriterConfig{
		namesToIds:   namesToIds,
		topicIds:     topicIds,
		chunkConfig:  chunkConfig,
		isCompressed: isCompressed,
		isVideo:      anyVideo,
	}

	w.chunkStatus = &ChunkStatus{
		startTimestamp: -1,
		size:           0,
		count:          0,
	}
	return nil
}

func (w *Writer) WriteMessage(topicName string, message []byte, timestamp int64) error {
	if !w.isTopicOpen {
		return ErrTopicNotOpened
	}
	if w.writerConfig.isVideo {
		return ErrWriteMessageOnVideoTopic
	}

	if timestamp < w.lastTimestamp {
		return fmt.Errorf("timestamp cannot decrease, current: %d < last: %d: %w", timestamp, w.lastTimestamp, ErrTimestampDecreases)
	}

	id, ok := w.writerConfig.namesToIds[topicName]
	if !ok {
		return fmt.Errorf("topic name: %s: %w", topicName, ErrTopicNotRegistered)
	}

	w.lastTimestamp = timestamp
	w.idToMessageIndexes[id] = append(w.idToMessageIndexes[id], format.MessageIndex{
		Timestamp:     timestamp,
		OffsetInChunk: int64(w.buf.Len()),
	})

	w.buf.Write(message)

	switch w.writerConfig.chunkConfig.Mode {
	case ChunkThresholdModeSize:
		w.chunkStatus.size += int64(len(message))
		if w.chunkStatus.size >= w.writerConfig.chunkConfig.Size {
			return w.writeChunk()
		}
	case ChunkThresholdModeDuration:
		if w.chunkStatus.startTimestamp == -1 {
			w.chunkStatus.startTimestamp = timestamp
		}
		if timestamp-w.chunkStatus.startTimestamp >= w.writerConfig.chunkConfig.Duration {
			return w.writeChunk()
		}
	case ChunkThresholdModeCount:
		w.chunkStatus.count++
		if w.chunkStatus.count >= w.writerConfig.chunkConfig.Count {
			return w.writeChunk()
		}
	}

	return nil
}

// WriteVideoMessage appends one coded video frame to the (single) video topic
// in the currently-open group. isKeyFrame must reflect whether the message's
// bytes contain a key frame (IDR for h264, IRAP for h265, KEY_FRAME for av1,
// keyframe for vp9). Turbodata does not parse the bitstream; the caller is
// responsible for setting isKeyFrame correctly.
//
// Chunking: ChunkConfig thresholds are treated as a lower bound. The current
// chunk is only flushed when a key frame arrives AND the threshold has been
// reached. This guarantees a GOP is never split across two chunks, so a
// random-access read for any frame in a GOP requires at most one data chunk.
//
// First-message rule: the first message on a video topic MUST be a key frame
// (returns ErrFirstVideoMessageMustBeKeyFrame otherwise). Otherwise the
// leading GOP would be undecodable.
func (w *Writer) WriteVideoMessage(topicName string, message []byte, timestamp int64, isKeyFrame bool) error {
	if !w.isTopicOpen {
		return ErrTopicNotOpened
	}
	if !w.writerConfig.isVideo {
		return ErrWriteVideoMessageOnNonVideoTopic
	}

	if timestamp < w.lastTimestamp {
		return fmt.Errorf("timestamp cannot decrease, current: %d < last: %d: %w", timestamp, w.lastTimestamp, ErrTimestampDecreases)
	}

	id, ok := w.writerConfig.namesToIds[topicName]
	if !ok {
		return fmt.Errorf("topic name: %s: %w", topicName, ErrTopicNotRegistered)
	}

	// First message of any chunk must be a key frame. Because we only ever
	// flush mid-stream at key frames (the GOP-integrity rule), the only time
	// buf.Len() == 0 here is on the very first message of the topic — which
	// is exactly the case this contract wants to catch.
	if !isKeyFrame && w.buf.Len() == 0 {
		return ErrFirstVideoMessageMustBeKeyFrame
	}

	// Keyframe-gated chunk boundary: only flush at a key frame, only when the
	// threshold is met by the messages CURRENTLY in the chunk (which end at
	// w.lastTimestamp; the incoming keyframe isn't in the chunk yet and will
	// either join the current chunk or start the next one depending on the
	// flush decision). This is what guarantees GOP integrity.
	if isKeyFrame && w.buf.Len() > 0 && w.thresholdReached(w.lastTimestamp) {
		if err := w.writeChunk(); err != nil {
			return err
		}
	}

	w.lastTimestamp = timestamp
	offsetInChunk := int64(w.buf.Len())
	w.idToMessageIndexes[id] = append(w.idToMessageIndexes[id], format.MessageIndex{
		Timestamp:     timestamp,
		OffsetInChunk: offsetInChunk,
	})
	if isKeyFrame {
		// uint32: per-chunk message-index position of this key frame.
		// Wire format reserves uint32; chunks holding > 4 G messages of one
		// topic are not a real concern.
		w.idToKeyFrameIndexes[id] = append(w.idToKeyFrameIndexes[id],
			uint32(len(w.idToMessageIndexes[id])-1))
	}
	w.buf.Write(message)

	// Maintain chunk-status counters for symmetry with the non-video path so
	// duration/count thresholds are observable; we don't act on them inside a
	// GOP though - thresholdReached gates the flush above.
	switch w.writerConfig.chunkConfig.Mode {
	case ChunkThresholdModeSize:
		w.chunkStatus.size += int64(len(message))
	case ChunkThresholdModeDuration:
		if w.chunkStatus.startTimestamp == -1 {
			w.chunkStatus.startTimestamp = timestamp
		}
	case ChunkThresholdModeCount:
		w.chunkStatus.count++
	}
	return nil
}

// thresholdReached returns whether the in-progress chunk has reached the
// configured threshold. lastChunkTimestamp is the timestamp of the latest
// message currently buffered in the chunk (i.e. w.lastTimestamp at the
// caller's point of view), used only by ChunkThresholdModeDuration.
//
// Factored out so WriteVideoMessage can defer the actual flush to a key
// frame boundary while still using the same threshold semantics as
// WriteMessage's inline checks.
func (w *Writer) thresholdReached(lastChunkTimestamp int64) bool {
	cfg := w.writerConfig.chunkConfig
	switch cfg.Mode {
	case ChunkThresholdModeSize:
		return w.chunkStatus.size >= cfg.Size
	case ChunkThresholdModeDuration:
		if w.chunkStatus.startTimestamp == -1 {
			return false
		}
		return lastChunkTimestamp-w.chunkStatus.startTimestamp >= cfg.Duration
	case ChunkThresholdModeCount:
		return w.chunkStatus.count >= cfg.Count
	}
	return false
}

func (w *Writer) CloseTopic() error {
	if !w.isTopicOpen {
		return ErrTopicAlreadyClosed
	}
	w.isTopicOpen = false

	if w.buf.Len() > 0 {
		if err := w.writeChunk(); err != nil {
			return err
		}
	}

	w.indexChunksList = append(w.indexChunksList, w.indexChunks)
	w.indexChunks = []*format.IndexChunk{}
	return nil
}

func (w *Writer) writeChunk() error {
	var chunkBytes []byte
	uncompressedLen := int64(w.buf.Len())
	if w.writerConfig.isCompressed {
		compress.CompressInto(w.buf.Bytes(), w.compressBuf)
		chunkBytes = w.compressBuf.Data
	} else {
		chunkBytes = w.buf.Bytes()
	}

	topicIndexes := make([]*format.TopicIndex, 0, len(w.writerConfig.topicIds))
	for _, id := range w.writerConfig.topicIds {
		messageIndexes := w.idToMessageIndexes[id]
		if len(messageIndexes) == 0 {
			continue
		}
		// Take ownership of the per-topic key-frame slice for this chunk and
		// reset it for the next one. Non-video topics keep an empty slice,
		// matching the prior behaviour.
		keyFrameIndexes := w.idToKeyFrameIndexes[id]
		if keyFrameIndexes == nil {
			keyFrameIndexes = []uint32{}
		}
		topicIndexes = append(topicIndexes, &format.TopicIndex{
			Id:              id,
			MessageIndexes:  messageIndexes,
			KeyFrameIndexes: keyFrameIndexes,
		})
		w.idToMessageIndexes[id] = []format.MessageIndex{}
		w.idToKeyFrameIndexes[id] = []uint32{}
	}

	w.indexChunks = append(w.indexChunks, &format.IndexChunk{
		TopicIndexes:    topicIndexes,
		ChunkOffset:     w.offset,
		ChunkLen:        int64(len(chunkBytes)),
		UncompressedLen: uncompressedLen,
	})

	n, err := w.w.Write(chunkBytes)
	if err != nil {
		return err
	}
	if n != len(chunkBytes) {
		return fmt.Errorf("wrote %d bytes, expected %d", n, len(chunkBytes))
	}
	w.offset += int64(n)

	w.chunkStatus.startTimestamp = -1
	w.chunkStatus.size = 0
	w.chunkStatus.count = 0
	w.buf.Reset()

	return nil
}

func (w *Writer) writeIndexChunks() error {
	for i, indexChunks := range w.indexChunksList {
		totalLen := int64(0)
		for _, indexChunk := range indexChunks {
			if err := format.WriteIndexChunk(w.buf, indexChunk); err != nil {
				return err
			}

			compress.CompressInto(w.buf.Bytes(), w.compressBuf)
			compressed := w.compressBuf.Data
			w.buf.Reset()

			startTimestamp := int64(math.MaxInt64)
			endTimestamp := int64(math.MinInt64)
			for _, topicIndex := range indexChunk.TopicIndexes {
				if topicIndex.MessageIndexes[0].Timestamp < startTimestamp {
					startTimestamp = topicIndex.MessageIndexes[0].Timestamp
				}
				if topicIndex.MessageIndexes[len(topicIndex.MessageIndexes)-1].Timestamp > endTimestamp {
					endTimestamp = topicIndex.MessageIndexes[len(topicIndex.MessageIndexes)-1].Timestamp
				}
			}

			w.summary.TopicsInfos[i].IndexChunkInfoList = append(w.summary.TopicsInfos[i].IndexChunkInfoList, &format.IndexChunkInfo{
				StartTimestamp: startTimestamp,
				EndTimestamp:   endTimestamp,
				Offset:         w.offset,
			})

			n, err := w.w.Write(compressed)
			if err != nil {
				return err
			}
			if n != len(compressed) {
				return fmt.Errorf("wrote %d bytes, expected %d", n, len(compressed))
			}
			w.offset += int64(n)
			totalLen += int64(n)
		}
		w.summary.TopicsInfos[i].TotalLen = totalLen
	}
	return nil
}

func (w *Writer) writeSummary() error {
	if err := format.WriteSummary(w.buf, w.summary); err != nil {
		return err
	}
	compress.CompressInto(w.buf.Bytes(), w.compressBuf)
	compressed := w.compressBuf.Data
	w.buf.Reset()

	n, err := w.w.Write(compressed)
	if err != nil {
		return err
	}
	if n != len(compressed) {
		return fmt.Errorf("wrote %d bytes, expected %d", n, len(compressed))
	}
	w.offset += int64(n)
	w.footer.SummaryLen = int64(n)
	return nil
}

func (w *Writer) writeFooter() error {
	if err := format.WriteFooter(w.w, w.footer); err != nil {
		return err
	}
	return nil
}

func (w *Writer) Close() error {
	if w.isTopicOpen {
		return ErrTopicNotClosed
	}

	if err := w.writeIndexChunks(); err != nil {
		return err
	}
	if err := w.writeSummary(); err != nil {
		return err
	}
	if err := w.writeFooter(); err != nil {
		return err
	}
	return w.bw.Flush()
}
