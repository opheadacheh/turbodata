package turbodata

import (
	"bytes"
	"fmt"
	"io"
	"math"
)

type WriterConfig struct {
	namesToIds   map[string]uint16
	chunkConfig  *ChunkConfig
	isCompressed bool
}

type Writer struct {
	// IO resources.
	w           io.Writer
	buf         *bytes.Buffer
	compressBuf *ReusableBuffer

	// Chunk-level state.
	chunkStatus        *ChunkStatus
	idToMessageIndexes map[uint16][]*MessageIndex

	// Topic-level state.
	lastTimestamp uint64
	isTopicOpen   bool
	writerConfig  *WriterConfig
	indexChunks   []*IndexChunk

	// File-level state.
	offset          uint64
	currentTopicId  uint16
	footer          *Footer
	summary         *Summary
	indexChunksList [][]*IndexChunk
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{
		w: w,
		footer: &Footer{
			Magic: [5]byte{'7', 'U', 'R', 'B', '0'},
		},
		summary: &Summary{
			TopicsInfos: []*TopicsInfo{},
		},
		buf: bytes.NewBuffer(nil),
		compressBuf: &ReusableBuffer{
			Data: make([]byte, 0),
		},
	}
}

func (w *Writer) OpenTopics(names []string, metadatas []map[string]any, opts ...WriteOption) error {
	if w.isTopicOpen {
		return fmt.Errorf("topic already opened, close it first")
	}
	if len(names) != len(metadatas) {
		return fmt.Errorf("names and metadatas must have the same length")
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

	topicMedatas := make([]*TopicMetadata, len(names))
	namesToIds := make(map[string]uint16)
	for i := range names {
		w.currentTopicId++
		topicMedatas[i] = &TopicMetadata{
			Id:       w.currentTopicId,
			Name:     names[i],
			Metadata: metadatas[i],
		}

		namesToIds[names[i]] = w.currentTopicId
	}

	w.summary.TopicsInfos = append(w.summary.TopicsInfos, &TopicsInfo{
		TopicMetadatas:     topicMedatas,
		IndexChunkInfoList: []*IndexChunkInfo{},
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
		chunkConfig:  chunkConfig,
		isCompressed: isCompressed,
	}

	w.chunkStatus = &ChunkStatus{
		startTimestamp: 0,
		size:           0,
		count:          0,
	}
	return nil
}

func (w *Writer) WriteMessage(topicName string, message []byte, timestamp uint64) error {
	if timestamp < w.lastTimestamp {
		return fmt.Errorf("timestamp cannot decrease, current: %d < last: %d", timestamp, w.lastTimestamp)
	}

	id, ok := w.writerConfig.namesToIds[topicName]
	if !ok {
		return fmt.Errorf("topic name: %s not registered with OpenTopics", topicName)
	}

	w.lastTimestamp = timestamp
	w.idToMessageIndexes[id] = append(w.idToMessageIndexes[id], &MessageIndex{
		Timestamp:     timestamp,
		OffsetInChunk: uint64(w.buf.Len()),
	})

	w.buf.Write(message)

	switch w.writerConfig.chunkConfig.Mode {
	case ChunkThresholdModeSize:
		w.chunkStatus.size += uint64(len(message))
	case ChunkThresholdModeDuration:
		if w.chunkStatus.startTimestamp == 0 {
			w.chunkStatus.startTimestamp = timestamp
		}
	case ChunkThresholdModeCount:
		w.chunkStatus.count++
	}

	switch w.writerConfig.chunkConfig.Mode {
	case ChunkThresholdModeSize:
		if w.chunkStatus.size >= w.writerConfig.chunkConfig.Size {
			return w.writeChunk()
		}
	case ChunkThresholdModeDuration:
		if timestamp-w.chunkStatus.startTimestamp >= w.writerConfig.chunkConfig.Duration {
			return w.writeChunk()
		}
	case ChunkThresholdModeCount:
		if w.chunkStatus.count >= w.writerConfig.chunkConfig.Count {
			return w.writeChunk()
		}
	}

	return nil
}

func (w *Writer) CloseTopic() error {
	if !w.isTopicOpen {
		return fmt.Errorf("topic is already closed")
	}
	w.isTopicOpen = false

	if w.buf.Len() > 0 {
		w.writeChunk()
	}

	w.indexChunksList = append(w.indexChunksList, w.indexChunks)
	w.indexChunks = []*IndexChunk{}
	return nil
}

func (w *Writer) writeChunk() error {
	var bytes []byte
	uncompressedLen := uint64(w.buf.Len())
	if w.writerConfig.isCompressed {
		compressInto(w.buf.Bytes(), w.compressBuf)
		bytes = w.compressBuf.Data
	} else {
		bytes = w.buf.Bytes()
	}

	topicIndexes := []*TopicIndex{}
	for id, messageIndexes := range w.idToMessageIndexes {
		topicIndexes = append(topicIndexes, &TopicIndex{
			Id:              id,
			MessageIndexes:  messageIndexes,
			KeyFrameIndexes: []uint32{},
		})
	}

	w.indexChunks = append(w.indexChunks, &IndexChunk{
		TopicIndexes:    topicIndexes,
		ChunkOffset:     w.offset,
		ChunkLen:        uint64(len(bytes)),
		UncompressedLen: uncompressedLen,
	})

	w.w.Write(bytes)
	w.offset += uint64(len(bytes))

	w.chunkStatus.startTimestamp = 0
	w.chunkStatus.size = 0
	w.chunkStatus.count = 0
	w.buf.Reset()

	return nil
}

func (w *Writer) writeIndexChunks() error {
	for i, indexChunks := range w.indexChunksList {
		totalLen := uint64(0)
		for _, indexChunk := range indexChunks {
			if err := WriteIndexChunk(w.buf, indexChunk); err != nil {
				return err
			}

			compressInto(w.buf.Bytes(), w.compressBuf)
			compressed := w.compressBuf.Data
			w.buf.Reset()

			startTimestamp := uint64(math.MaxUint64)
			endTimestamp := uint64(0)
			for _, topicIndex := range indexChunk.TopicIndexes {
				if topicIndex.MessageIndexes[0].Timestamp < startTimestamp {
					startTimestamp = topicIndex.MessageIndexes[0].Timestamp
				}
				if topicIndex.MessageIndexes[len(topicIndex.MessageIndexes)-1].Timestamp > endTimestamp {
					endTimestamp = topicIndex.MessageIndexes[len(topicIndex.MessageIndexes)-1].Timestamp
				}
			}

			w.summary.TopicsInfos[i].IndexChunkInfoList = append(w.summary.TopicsInfos[i].IndexChunkInfoList, &IndexChunkInfo{
				StartTimestamp: startTimestamp,
				EndTimestamp:   endTimestamp,
				Offset:         w.offset,
			})

			w.w.Write(compressed)
			w.offset += uint64(len(compressed))
			totalLen += uint64(len(compressed))
		}
		w.summary.TopicsInfos[i].TotalLen = totalLen
	}
	return nil
}

func (w *Writer) writeSummary() error {
	if err := WriteSummary(w.buf, w.summary); err != nil {
		return err
	}
	compressInto(w.buf.Bytes(), w.compressBuf)
	compressed := w.compressBuf.Data
	w.buf.Reset()

	w.w.Write(compressed)
	w.offset += uint64(len(compressed))
	w.footer.SummaryLen = uint64(len(compressed))
	return nil
}

func (w *Writer) writeFooter() error {
	if err := WriteFooter(w.w, w.footer); err != nil {
		return err
	}
	return nil
}

func (w *Writer) Close() error {
	if w.isTopicOpen {
		return fmt.Errorf("topic is not closed, call CloseTopic first")
	}

	w.writeIndexChunks()
	w.writeSummary()
	w.writeFooter()

	return nil
}
