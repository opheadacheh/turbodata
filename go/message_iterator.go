package turbodata

import (
	"container/heap"
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

func (it *MessageIterator) prepare() {
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
}

func (it *MessageIterator) NextInto(buf *ReusableBuffer) (int64, string, error) {
	if !it.loaded {
		if err := it.initLoad(); err != nil {
			return 0, "", err
		}
	}
	it.loaded = true

	if it.heap.Len() == 0 {
		return 0, "", io.EOF
	}

	item, _ := heap.Pop(it.heap).(*message)
	buf.Prepare(len(item.data))
	copy(buf.Data, item.data)

	retTimestamp := item.timestamp
	retTopicId := item.topicId

	topicsGroupIt := it.topicsGroupIterators[item.groupIndex]
	timestamp, topicId, data, err := topicsGroupIt.Next()
	if err == nil {
		item.timestamp = timestamp
		item.topicId = topicId
		item.data = data
		heap.Push(it.heap, item)
	}
	if err != io.EOF && err != nil {
		return 0, "", err
	}

	return retTimestamp, it.topicIdToNames[retTopicId], nil
}

func (it *MessageIterator) initLoad() error {
	for i, topicsGroupIt := range it.topicsGroupIterators {
		timestamp, topicId, data, err := topicsGroupIt.Next()
		if err == io.EOF {
			continue
		}
		if err != nil {
			return err
		}

		heap.Push(it.heap, &message{timestamp: timestamp, topicId: topicId, data: data, groupIndex: i})
	}

	return nil
}
