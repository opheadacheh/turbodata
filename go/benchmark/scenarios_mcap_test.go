package benchmark

import (
	"errors"
	"io"

	"github.com/foxglove/mcap/go/mcap"
)

// mcapReadAllMessages reads every message from the MCAP file accessed via tracker.
// Returns the total message count.
//
// tracker must implement io.ReadSeeker so the MCAP reader uses its indexed path.
func mcapReadAllMessages(tracker *TrackingReadSeeker) (int, error) {
	r, err := mcap.NewReader(tracker)
	if err != nil {
		return 0, err
	}
	defer r.Close()

	it, err := r.Messages(mcap.InOrder(mcap.LogTimeOrder))
	if err != nil {
		return 0, err
	}
	return drainMcap(it)
}

// mcapReadSingleTopic reads all messages for one topic from the MCAP file accessed via tracker.
func mcapReadSingleTopic(tracker *TrackingReadSeeker, topic string) (int, error) {
	r, err := mcap.NewReader(tracker)
	if err != nil {
		return 0, err
	}
	defer r.Close()

	it, err := r.Messages(mcap.InOrder(mcap.LogTimeOrder), mcap.WithTopics([]string{topic}))
	if err != nil {
		return 0, err
	}
	return drainMcap(it)
}

// mcapReadTimeRange reads messages for the given topics within [startNs, endNs] from the MCAP file.
func mcapReadTimeRange(tracker *TrackingReadSeeker, topics []string, startNs, endNs uint64) (int, error) {
	r, err := mcap.NewReader(tracker)
	if err != nil {
		return 0, err
	}
	defer r.Close()

	it, err := r.Messages(
		mcap.InOrder(mcap.LogTimeOrder),
		mcap.WithTopics(topics),
		mcap.AfterNanos(startNs),
		mcap.BeforeNanos(endNs),
	)
	if err != nil {
		return 0, err
	}
	return drainMcap(it)
}

func drainMcap(it mcap.MessageIterator) (int, error) {
	msg := &mcap.Message{}
	count := 0
	for {
		_, _, _, err := it.NextInto(msg)
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
