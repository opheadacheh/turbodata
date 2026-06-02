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

type message struct {
	topicId    uint16
	data       []byte
	groupIndex int
	timestamp  int64
}

// MessageHeap is a timestamp-ordered message heap. When reverse is true the
// ordering is flipped (latest timestamp first).
type MessageHeap struct {
	messages []*message
	reverse  bool
}

func (h MessageHeap) Len() int { return len(h.messages) }
func (h MessageHeap) Less(i, j int) bool {
	if h.reverse {
		return h.messages[i].timestamp > h.messages[j].timestamp
	}
	return h.messages[i].timestamp < h.messages[j].timestamp
}
func (h MessageHeap) Swap(i, j int) { h.messages[i], h.messages[j] = h.messages[j], h.messages[i] }

func (h *MessageHeap) Push(x any) {
	h.messages = append(h.messages, x.(*message))
}

func (h *MessageHeap) Pop() any {
	old := h.messages
	n := len(old)
	x := old[n-1]
	h.messages = old[0 : n-1]
	return x
}
