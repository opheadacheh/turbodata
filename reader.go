package turbodata

import (
	"bytes"
	"container/heap"
	"fmt"
	"io"
)

type Reader struct {
	rs io.ReadSeeker

	summary *Summary
	footer  *Footer
}

const footerLen = 13

func NewReader(rs io.ReadSeeker) (*Reader, error) {
	rs.Seek(-footerLen, io.SeekEnd)

	footer, err := ReadFooter(rs)
	if err != nil {
		return nil, err
	}

	if footer.Magic != [5]byte{'7', 'U', 'R', 'B', '0'} {
		return nil, fmt.Errorf("invalid magic number")
	}

	return &Reader{
		rs:     rs,
		footer: footer,
	}, nil
}

func (r *Reader) Summary() (*Summary, error) {
	if r.summary != nil {
		return r.summary, nil
	}

	r.rs.Seek(int64(-r.footer.SummaryLen-footerLen), io.SeekEnd)

	compressed := make([]byte, r.footer.SummaryLen)
	if _, err := r.rs.Read(compressed); err != nil {
		return nil, err
	}
	decompressed, err := decompress(compressed)
	if err != nil {
		return nil, err
	}

	summary, err := ReadSummary(bytes.NewReader(decompressed))
	if err != nil {
		return nil, err
	}
	return summary, nil
}

func (r *Reader) ReadMessages(opts ...ReadOption) (*MessageIterator, error) {
	it := newMessageIterator(r.rs)
	for _, opt := range opts {
		if err := opt(it); err != nil {
			return nil, err
		}
	}

	switch it.order {
	case TimeOrder:
		it.heap = &TimeOrderHeap{}
		heap.Init(it.heap)
	case ReverseTimeOrder:
		it.reverseHeap = &ReverseTimeOrderHeap{}
		heap.Init(it.reverseHeap)
	}

	summary, err := r.Summary()
	if err != nil {
		return nil, err
	}

	topicNames := make(map[string]bool)
	for _, topicName := range it.topicNames {
		topicNames[topicName] = true
	}
	for _, topicsInfo := range summary.TopicsInfos {
		if len(it.topicNames) > 0 && !topicNames[topicsInfo.TopicMetadatas[0].Name] {
			continue
		}

		indexChunkInfoLens := make([]uint64, len(topicsInfo.IndexChunkInfoList))
		for i := range topicsInfo.IndexChunkInfoList {
			if i == 0 {
				continue
			}

			indexChunkInfoLens[i-1] = topicsInfo.IndexChunkInfoList[i].Offset - topicsInfo.IndexChunkInfoList[i-1].Offset
		}
		indexChunkInfoLens[len(indexChunkInfoLens)-1] = topicsInfo.TotalLen - topicsInfo.IndexChunkInfoList[len(topicsInfo.IndexChunkInfoList)-1].Offset + topicsInfo.IndexChunkInfoList[0].Offset

		indexChunkInfoList := []*indexChunkInfoWithLen{}
		for i, indexChunkInfo := range topicsInfo.IndexChunkInfoList {
			if indexChunkInfo.EndTimestamp < it.startTimestamp || indexChunkInfo.StartTimestamp > it.endTimestamp {
				continue
			}

			indexChunkInfoList = append(indexChunkInfoList, &indexChunkInfoWithLen{
				indexChunkInfo: indexChunkInfo,
				len:            indexChunkInfoLens[i],
			})
		}

		isCompressed, ok := topicsInfo.TopicMetadatas[0].Metadata["is_compressed"].(bool)
		if !ok {
			isCompressed = false
		}

		it.topicState[topicsInfo.TopicMetadatas[0].Id] = &TopicState{
			indexChunkInfoList: indexChunkInfoList,
			isCompressed:       isCompressed,
			buf:                &ReusableBuffer{Data: make([]byte, 0)},
			name:               topicsInfo.TopicMetadatas[0].Name,
		}
	}

	return it, nil
}
