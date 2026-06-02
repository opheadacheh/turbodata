// Copyright 2026 Wanjia He
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package benchmark

import (
	"errors"
	"io"

	"github.com/opheadacheh/turbodata/go/turbodata"

	"github.com/foxglove/mcap/go/mcap"
)

// Scenario captures one read pattern in a way that is independent of the
// underlying format. Concrete bench files instantiate it via the helpers
// below.
type Scenario struct {
	Name           string
	Topics         []string // empty == all topics
	StartTimestamp int64    // 0 == open start
	EndTimestamp   int64    // 0 == open end
}

// runMcap drains an MCAP scenario from src, returning the message count.
func runMcap(src io.ReadSeeker, sc Scenario) (int, error) {
	r, err := mcap.NewReader(src)
	if err != nil {
		return 0, err
	}
	defer r.Close()

	opts := []mcap.ReadOpt{mcap.InOrder(mcap.LogTimeOrder)}
	if len(sc.Topics) > 0 {
		opts = append(opts, mcap.WithTopics(sc.Topics))
	}
	if sc.StartTimestamp > 0 {
		opts = append(opts, mcap.AfterNanos(uint64(sc.StartTimestamp)))
	}
	if sc.EndTimestamp > 0 {
		opts = append(opts, mcap.BeforeNanos(uint64(sc.EndTimestamp)))
	}
	it, err := r.Messages(opts...)
	if err != nil {
		return 0, err
	}
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

// runTd drains a turbodata scenario from src with optional extra ReadOptions
// (e.g. WithReadStrategy). Returns the message count.
func runTd(src turbodata.ReadSource, sc Scenario, extra ...turbodata.ReadOption) (int, error) {
	r := turbodata.NewReader(src)
	opts := make([]turbodata.ReadOption, 0, 4+len(extra))
	if len(sc.Topics) > 0 {
		opts = append(opts, turbodata.WithTopicNames(sc.Topics))
	}
	if sc.StartTimestamp > 0 {
		opts = append(opts, turbodata.WithStartTimestamp(sc.StartTimestamp))
	}
	if sc.EndTimestamp > 0 {
		opts = append(opts, turbodata.WithEndTimestamp(sc.EndTimestamp))
	}
	opts = append(opts, extra...)

	it, err := r.ReadMessages(opts...)
	if err != nil {
		return 0, err
	}
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
