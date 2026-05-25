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
}

type Writer struct {
	// IO resources.
	w           io.Writer
	bw          *bufio.Writer
	buf         *bytes.Buffer
	compressBuf *buffer.ReusableBuffer

	// Chunk-level state.
	chunkStatus        *ChunkStatus
	idToMessageIndexes map[uint16][]*format.MessageIndex

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
		idToMessageIndexes: make(map[uint16][]*format.MessageIndex),
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
		w.idToMessageIndexes[w.currentTopicId] = []*format.MessageIndex{}
	}

	w.summary.TopicsInfos = append(w.summary.TopicsInfos, &format.TopicsInfo{
		TopicMetadatas:     topicMedatas,
		IndexChunkInfoList: []*format.IndexChunkInfo{},
		TotalLen:           0,
	})

	chunkConfig, ok := metadatas[0]["chunk_config"].(*ChunkConfig)
	if !ok {
		chunkConfig = &ChunkConfig{
			Mode: ChunkThresholdModeSize,
			Size: 1024 * 1024,
		}
	}

	isCompressed, ok := metadatas[0]["is_compressed"].(bool)
	if !ok {
		isCompressed = false
	}

	w.writerConfig = &WriterConfig{
		namesToIds:   namesToIds,
		topicIds:     topicIds,
		chunkConfig:  chunkConfig,
		isCompressed: isCompressed,
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

	if timestamp < w.lastTimestamp {
		return fmt.Errorf("timestamp cannot decrease, current: %d < last: %d: %w", timestamp, w.lastTimestamp, ErrTimestampDecreases)
	}

	id, ok := w.writerConfig.namesToIds[topicName]
	if !ok {
		return fmt.Errorf("topic name: %s: %w", topicName, ErrTopicNotRegistered)
	}

	w.lastTimestamp = timestamp
	w.idToMessageIndexes[id] = append(w.idToMessageIndexes[id], &format.MessageIndex{
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
	var bytes []byte
	uncompressedLen := int64(w.buf.Len())
	if w.writerConfig.isCompressed {
		compress.CompressInto(w.buf.Bytes(), w.compressBuf)
		bytes = w.compressBuf.Data
	} else {
		bytes = w.buf.Bytes()
	}

	topicIndexes := make([]*format.TopicIndex, 0, len(w.writerConfig.topicIds))
	for _, id := range w.writerConfig.topicIds {
		messageIndexes := w.idToMessageIndexes[id]
		if len(messageIndexes) == 0 {
			continue
		}
		topicIndexes = append(topicIndexes, &format.TopicIndex{
			Id:              id,
			MessageIndexes:  messageIndexes,
			KeyFrameIndexes: []uint32{},
		})
		w.idToMessageIndexes[id] = []*format.MessageIndex{}
	}

	w.indexChunks = append(w.indexChunks, &format.IndexChunk{
		TopicIndexes:    topicIndexes,
		ChunkOffset:     w.offset,
		ChunkLen:        int64(len(bytes)),
		UncompressedLen: uncompressedLen,
	})

	n, err := w.w.Write(bytes)
	if err != nil {
		return err
	}
	if n != len(bytes) {
		return fmt.Errorf("wrote %d bytes, expected %d", n, len(bytes))
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
