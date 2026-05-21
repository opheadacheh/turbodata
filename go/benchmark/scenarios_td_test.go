package benchmark

import (
	"errors"
	"io"

	"turbodata"
)

// tdReadAllMessages reads every message from the TD file accessed via tracker.
// Returns the total message count.
func tdReadAllMessages(tracker *TrackingReadSeeker) (int, error) {
	r, err := turbodata.NewReader(tracker)
	if err != nil {
		return 0, err
	}
	it, err := r.ReadMessages()
	if err != nil {
		return 0, err
	}
	return drainTd(it)
}

// tdReadSingleTopic reads all messages for one topic from the TD file accessed via tracker.
func tdReadSingleTopic(tracker *TrackingReadSeeker, topic string) (int, error) {
	r, err := turbodata.NewReader(tracker)
	if err != nil {
		return 0, err
	}
	it, err := r.ReadMessages(turbodata.WithTopicNames([]string{topic}))
	if err != nil {
		return 0, err
	}
	return drainTd(it)
}

// tdReadTimeRange reads messages for the given topics within [startTs, endTs] from the TD file.
func tdReadTimeRange(tracker *TrackingReadSeeker, topics []string, startTs, endTs int64) (int, error) {
	r, err := turbodata.NewReader(tracker)
	if err != nil {
		return 0, err
	}
	it, err := r.ReadMessages(
		turbodata.WithTopicNames(topics),
		turbodata.WithStartTimestamp(startTs),
		turbodata.WithEndTimestamp(endTs),
	)
	if err != nil {
		return 0, err
	}
	return drainTd(it)
}

func drainTd(it *turbodata.MessageIterator) (int, error) {
	buf := turbodata.NewReusableBuffer()
	count := 0
	for {
		_, _, err := it.NextInto(buf)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}

// tdWriteGroups writes all pre-loaded groups to tw using the given config pair.
// It returns the total number of messages written.
func tdWriteGroups(tw *TrackingWriter, pair ConfigPair) (int, error) {
	w := turbodata.NewWriter(tw)
	count := 0

	for _, grp := range imgGroups {
		if err := w.OpenTopics(
			grp.names,
			cloneMetadatas(grp.metadatas),
			turbodata.WithCompression(),
			turbodata.WithChunkConfig(pair.ImgConfig),
		); err != nil {
			return count, err
		}
		for _, msg := range grp.messages {
			if err := w.WriteMessage(msg.topicName, msg.data, msg.timestamp); err != nil {
				return count, err
			}
			count++
		}
		if err := w.CloseTopic(); err != nil {
			return count, err
		}
	}

	for _, grp := range nonImgGroups {
		if err := w.OpenTopics(
			grp.names,
			cloneMetadatas(grp.metadatas),
			turbodata.WithCompression(),
			turbodata.WithChunkConfig(pair.NonImgConfig),
		); err != nil {
			return count, err
		}
		for _, msg := range grp.messages {
			if err := w.WriteMessage(msg.topicName, msg.data, msg.timestamp); err != nil {
				return count, err
			}
			count++
		}
		if err := w.CloseTopic(); err != nil {
			return count, err
		}
	}

	return count, w.Close()
}

// cloneMetadatas returns a deep copy of a metadata slice so that WriteOption
// mutations (chunk_config, is_compressed) don't persist across benchmark iterations.
func cloneMetadatas(ms []map[string]any) []map[string]any {
	out := make([]map[string]any, len(ms))
	for i, m := range ms {
		clone := make(map[string]any, len(m))
		for k, v := range m {
			clone[k] = v
		}
		out[i] = clone
	}
	return out
}
