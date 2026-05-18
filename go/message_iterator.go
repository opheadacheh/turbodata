package turbodata

import (
	"container/heap"
	"fmt"
	"io"
	"math"
)

type MessageIterator struct {
	rs io.ReadSeeker

	heap   heap.Interface
	loaded bool

	topicNames     []string
	startTimestamp int64
	endTimestamp   int64
	order          Order
	summary        *Summary

	topicsGroupIterators []*TopicsGroupIterator
	topicIdToNames       map[uint16]string
}

func prepareReusableBuf(buf *ReusableBuffer, len int64) {
	if cap(buf.Data) < int(len) {
		buf.Data = make([]byte, len)
		return
	}

	buf.Data = buf.Data[:len]
}

func getMessageIndexLens(messageIndexes []*MessageIndex, totalLen int64) []int64 {
	messageIndexLens := make([]int64, len(messageIndexes))
	for i, messageIndex := range messageIndexes {
		if i == 0 {
			continue
		}
		messageIndexLens[i-1] = messageIndex.OffsetInChunk - messageIndexes[i-1].OffsetInChunk
	}

	messageIndexLens[len(messageIndexLens)-1] = totalLen - messageIndexes[len(messageIndexes)-1].OffsetInChunk

	return messageIndexLens
}

func newMessageIterator(rs io.ReadSeeker, summary *Summary) *MessageIterator {
	return &MessageIterator{
		rs:             rs,
		summary:        summary,
		order:          TimeOrder,
		startTimestamp: 0,
		endTimestamp:   math.MaxInt64,
		topicNames:     []string{},
		topicIdToNames: make(map[uint16]string),
	}
}

func (it *MessageIterator) prepare() error {
	switch it.order {
	case TimeOrder:
		it.heap = &MessageHeap{}
	case ReverseTimeOrder:
		it.heap = &ReverseMessageHeap{}
	}
	heap.Init(it.heap)

	topicNamesMap := make(map[string]struct{})
	if len(it.topicNames) > 0 {
		for _, topicName := range it.topicNames {
			topicNamesMap[topicName] = struct{}{}
		}
	} else {
		for _, topicsInfo := range it.summary.TopicsInfos {
			for _, topicMetadata := range topicsInfo.TopicMetadatas {
				topicNamesMap[topicMetadata.Name] = struct{}{}
			}
		}
	}

	topicIds := make(map[uint16]struct{})
	for _, topicsInfo := range it.summary.TopicsInfos {
		for _, topicMetadata := range topicsInfo.TopicMetadatas {
			if _, ok := topicNamesMap[topicMetadata.Name]; !ok {
				continue
			}

			topicIds[topicMetadata.Id] = struct{}{}
			it.topicIdToNames[topicMetadata.Id] = topicMetadata.Name
		}
	}

	for _, topicsInfo := range it.summary.TopicsInfos {
		for _, topicMetadata := range topicsInfo.TopicMetadatas {
			if _, ok := topicNamesMap[topicMetadata.Name]; !ok {
				continue
			}

			topicsGroupIt := newTopicsGroupIterator(it, topicIds, topicsInfo)
			if topicsGroupIt != nil {
				it.topicsGroupIterators = append(it.topicsGroupIterators, topicsGroupIt)
			}

			break
		}
	}

	return nil
}

func (it *MessageIterator) NextInto(buf *ReusableBuffer) (string, error) {
	if !it.loaded {
		if err := it.initLoad(); err != nil {
			return "", err
		}
	}
	it.loaded = true

	if it.heap.Len() == 0 {
		return "", io.EOF
	}

	fmt.Println("heap len", it.heap.Len())

	item, _ := heap.Pop(it.heap).(*message)
	buf.Prepare(len(item.data))
	copy(buf.Data, item.data)

	topicsGroupIt := it.topicsGroupIterators[item.groupIndex]
	topicId, data, err := topicsGroupIt.Next()
	if err == nil {
		heap.Push(it.heap, &message{topicId: topicId, data: data, groupIndex: item.groupIndex})
	}
	if err != io.EOF && err != nil {
		return "", err
	}

	return it.topicIdToNames[item.topicId], nil
}

func (it *MessageIterator) initLoad() error {
	for i, topicsGroupIt := range it.topicsGroupIterators {
		topicId, data, err := topicsGroupIt.Next()
		if err == io.EOF {
			continue
		}
		fmt.Println("initLoad err", err)
		if err != nil {
			return err
		}

		heap.Push(it.heap, &message{topicId: topicId, data: data, groupIndex: i})
	}

	return nil
}
