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

package iter

import "github.com/opheadacheh/turbodata/go/turbodata/format"

type messageIndexWithTopicId struct {
	topicId      uint16
	messageIndex *format.MessageIndex
}

// Time ordered message index heap.
type MessageIndexHeap []*messageIndexWithTopicId

func (h MessageIndexHeap) Len() int { return len(h) }
func (h MessageIndexHeap) Less(i, j int) bool {
	return h[i].messageIndex.OffsetInChunk < h[j].messageIndex.OffsetInChunk
}
func (h MessageIndexHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *MessageIndexHeap) Push(x any) {
	*h = append(*h, x.(*messageIndexWithTopicId))
}

func (h *MessageIndexHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
