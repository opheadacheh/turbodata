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
	"errors"
	"io"
	"testing"

	"github.com/opheadacheh/turbodata/go/format"
	"github.com/opheadacheh/turbodata/go/internal/compress"
	"github.com/opheadacheh/turbodata/go/internal/iorange"
)

func collectGroup(t *testing.T, it *preloadedTopicsGroupIterator) []testMsg {
	t.Helper()
	var out []testMsg
	for {
		ts, tid, data, err := it.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		cp := make([]byte, len(data))
		copy(cp, data)
		out = append(out, testMsg{ts: ts, name: string(rune('A' + tid)), data: cp})
	}
	return out
}

// mkMsg builds the pointer-typed message-with-topic struct used by the
// preloaded iterator. Test ergonomics only.
func mkMsg(ts int64, tid uint16, off int64) *messageIndexWithTopicId {
	return &messageIndexWithTopicId{
		topicId:      tid,
		messageIndex: &format.MessageIndex{Timestamp: ts, OffsetInChunk: off},
	}
}

// TestPreloadedTopicsGroupIteratorUncompressedForward builds a LoadedBytes that
// holds messages directly (mimicking what the cost-aware setup does for
// uncompressed groups: register per-message offsets), then walks them.
func TestPreloadedTopicsGroupIteratorUncompressedForward(t *testing.T) {
	// One chunk at offset 1000, with three messages at offsets 0, 100, 250
	// within the chunk and respective lengths 10, 50, 30.
	chunkOff := int64(1000)
	msgs := []*messageIndexWithTopicId{
		mkMsg(10, 0, 0),
		mkMsg(20, 0, 100),
		mkMsg(30, 0, 250),
	}
	lens := []int64{10, 50, 30}

	bufs := [][]byte{
		[]byte("AAAAAAAAAA"), // 10 bytes for msg[0]
		[]byte("BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"), // 50 bytes for msg[1]
		[]byte("CCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"),                     // 30 bytes for msg[2]
	}
	ranges := []iorange.Range{
		{Offset: chunkOff + 0, Length: 10},
		{Offset: chunkOff + 100, Length: 50},
		{Offset: chunkOff + 250, Length: 30},
	}
	locs := []iorange.RangeLocation{
		{OpIndex: 0, InOpOff: 0, Length: 10},
		{OpIndex: 1, InOpOff: 0, Length: 50},
		{OpIndex: 2, InOpOff: 0, Length: 30},
	}
	lb := iorange.NewLoadedBytes(ranges, locs, bufs)
	ic := &format.IndexChunk{ChunkOffset: chunkOff, ChunkLen: 280, UncompressedLen: 280}

	it := newPreloadedTopicsGroupIterator(false, TimeOrder, []*format.IndexChunk{ic}, [][]*messageIndexWithTopicId{msgs}, [][]int64{lens}, lb)
	out := collectGroup(t, it)
	if len(out) != 3 {
		t.Fatalf("len=%d", len(out))
	}
	wantTs := []int64{10, 20, 30}
	wantData := []string{"AAAAAAAAAA", "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB", "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"}
	for i, m := range out {
		if m.ts != wantTs[i] {
			t.Errorf("[%d] ts=%d want %d", i, m.ts, wantTs[i])
		}
		if string(m.data) != wantData[i] {
			t.Errorf("[%d] data=%q want %q", i, m.data, wantData[i])
		}
	}
}

// TestPreloadedTopicsGroupIteratorUncompressedReverse exercises ReverseTimeOrder
// across two chunks to verify reverse traversal logic.
func TestPreloadedTopicsGroupIteratorUncompressedReverse(t *testing.T) {
	chunkOff1 := int64(0)
	chunkOff2 := int64(1000)
	msgs1 := []*messageIndexWithTopicId{
		mkMsg(10, 0, 0),
		mkMsg(20, 0, 10),
	}
	msgs2 := []*messageIndexWithTopicId{
		mkMsg(30, 0, 0),
		mkMsg(40, 0, 5),
	}
	bufs := [][]byte{
		[]byte("0000000000"), // chunk1 msg[0] (10 bytes)
		[]byte("1111111111"), // chunk1 msg[1] (10 bytes)
		[]byte("22222"),      // chunk2 msg[0] (5 bytes)
		[]byte("33333"),      // chunk2 msg[1] (5 bytes)
	}
	ranges := []iorange.Range{
		{Offset: chunkOff1 + 0, Length: 10},
		{Offset: chunkOff1 + 10, Length: 10},
		{Offset: chunkOff2 + 0, Length: 5},
		{Offset: chunkOff2 + 5, Length: 5},
	}
	locs := []iorange.RangeLocation{
		{OpIndex: 0, InOpOff: 0, Length: 10},
		{OpIndex: 1, InOpOff: 0, Length: 10},
		{OpIndex: 2, InOpOff: 0, Length: 5},
		{OpIndex: 3, InOpOff: 0, Length: 5},
	}
	lb := iorange.NewLoadedBytes(ranges, locs, bufs)
	ic1 := &format.IndexChunk{ChunkOffset: chunkOff1, ChunkLen: 20, UncompressedLen: 20}
	ic2 := &format.IndexChunk{ChunkOffset: chunkOff2, ChunkLen: 10, UncompressedLen: 10}

	it := newPreloadedTopicsGroupIterator(
		false,
		ReverseTimeOrder,
		[]*format.IndexChunk{ic1, ic2},
		[][]*messageIndexWithTopicId{msgs1, msgs2},
		[][]int64{{10, 10}, {5, 5}},
		lb,
	)
	out := collectGroup(t, it)
	wantTs := []int64{40, 30, 20, 10}
	if len(out) != 4 {
		t.Fatalf("len=%d", len(out))
	}
	for i, m := range out {
		if m.ts != wantTs[i] {
			t.Errorf("[%d] ts=%d want %d", i, m.ts, wantTs[i])
		}
	}
}

// TestPreloadedTopicsGroupIteratorCompressed registers one compressed chunk in
// LoadedBytes and verifies the iterator decompresses it once and slices
// individual messages out of the decompressed buffer.
func TestPreloadedTopicsGroupIteratorCompressed(t *testing.T) {
	// Build a synthetic "decompressed" payload of 30 bytes; messages of length
	// 10 each at offsets 0, 10, 20.
	uncompressed := []byte("AAAAAAAAAABBBBBBBBBBCCCCCCCCCC")
	compressed, err := compress.Compress(uncompressed)
	if err != nil {
		t.Fatalf("Compress: %v", err)
	}

	chunkOff := int64(500)
	msgs := []*messageIndexWithTopicId{
		mkMsg(1, 0, 0),
		mkMsg(2, 0, 10),
		mkMsg(3, 0, 20),
	}
	lens := []int64{10, 10, 10}
	bufs := [][]byte{compressed}
	ranges := []iorange.Range{
		{Offset: chunkOff, Length: int64(len(compressed))},
	}
	locs := []iorange.RangeLocation{
		{OpIndex: 0, InOpOff: 0, Length: len(compressed)},
	}
	lb := iorange.NewLoadedBytes(ranges, locs, bufs)
	ic := &format.IndexChunk{ChunkOffset: chunkOff, ChunkLen: int64(len(compressed)), UncompressedLen: int64(len(uncompressed))}

	it := newPreloadedTopicsGroupIterator(true, TimeOrder, []*format.IndexChunk{ic}, [][]*messageIndexWithTopicId{msgs}, [][]int64{lens}, lb)
	out := collectGroup(t, it)
	if len(out) != 3 {
		t.Fatalf("len=%d", len(out))
	}
	wantData := []string{"AAAAAAAAAA", "BBBBBBBBBB", "CCCCCCCCCC"}
	for i, m := range out {
		if string(m.data) != wantData[i] {
			t.Errorf("[%d] data=%q want %q", i, m.data, wantData[i])
		}
	}
}

// TestPreloadedTopicsGroupIteratorEmpty verifies an empty group iterator returns EOF
// without panicking.
func TestPreloadedTopicsGroupIteratorEmpty(t *testing.T) {
	lb := iorange.NewLoadedBytes(nil, nil, nil)
	it := newPreloadedTopicsGroupIterator(false, TimeOrder, nil, nil, nil, lb)
	_, _, _, err := it.Next()
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF, got %v", err)
	}
}

// testMsg is a local helper struct mirroring the shape of test results used in
// the root package's collect helper. Defined here so the in-package tests can
// build it directly without depending on root.
type testMsg struct {
	ts   int64
	name string
	data []byte
}
