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

import (
	"container/heap"
	"testing"
)

func TestMessageHeap(t *testing.T) {
	h := &MessageHeap{}
	heap.Init(h)
	heap.Push(h, &message{timestamp: 3})
	heap.Push(h, &message{timestamp: 1})
	heap.Push(h, &message{timestamp: 2})

	if h.Len() != 3 {
		t.Errorf("heap length should be 3, but got %d", h.Len())
	}

	len := h.Len()
	for i := 0; i < len; i++ {
		timestamp := heap.Pop(h).(*message).timestamp
		if timestamp != int64(i+1) {
			t.Errorf("heap pop should return %d, but got %d", i+1, timestamp)
		}
	}
}

func TestReverseMessageHeap(t *testing.T) {
	h := &MessageHeap{reverse: true}
	heap.Init(h)
	heap.Push(h, &message{timestamp: 2})
	heap.Push(h, &message{timestamp: 1})
	heap.Push(h, &message{timestamp: 3})

	if h.Len() != 3 {
		t.Errorf("heap length should be 3, but got %d", h.Len())
	}

	len := h.Len()
	for i := 0; i < len; i++ {
		timestamp := heap.Pop(h).(*message).timestamp
		if timestamp != int64(len-i) {
			t.Errorf("heap pop should return %d, but got %d", len-i, timestamp)
		}
	}
}
