package turbodata

import (
	"errors"
	"io"
	"testing"
)

func collectGroup(t *testing.T, it *costAwareGroupIterator) []testMsg {
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

// TestCostAwareGroupIteratorUncompressedForward builds a LoadedBytes that
// holds messages directly (mimicking what the cost-aware setup does for
// uncompressed groups: register per-message offsets), then walks them.
func TestCostAwareGroupIteratorUncompressedForward(t *testing.T) {
	// One chunk at offset 1000, with three messages at offsets 0, 100, 250
	// within the chunk and respective lengths 10, 50, 30.
	chunkOff := int64(1000)
	msgs := []messageRef{
		{Timestamp: 10, TopicId: 0, OffsetInChunk: 0},
		{Timestamp: 20, TopicId: 0, OffsetInChunk: 100},
		{Timestamp: 30, TopicId: 0, OffsetInChunk: 250},
	}
	lens := []int64{10, 50, 30}

	bufs := [][]byte{
		[]byte("AAAAAAAAAA"), // 10 bytes for msg[0]
		[]byte("BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"), // 50 bytes for msg[1]
		[]byte("CCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"),                     // 30 bytes for msg[2]
	}
	lb := &LoadedBytes{
		buffers: bufs,
		index: map[int64]bytesLocation{
			chunkOff + 0:   {bufferIdx: 0, inBufOff: 0, length: 10},
			chunkOff + 100: {bufferIdx: 1, inBufOff: 0, length: 50},
			chunkOff + 250: {bufferIdx: 2, inBufOff: 0, length: 30},
		},
	}
	ic := &IndexChunk{ChunkOffset: chunkOff, ChunkLen: 280, UncompressedLen: 280}

	it := newCostAwareGroupIterator(false, TimeOrder, []*IndexChunk{ic}, [][]messageRef{msgs}, [][]int64{lens}, lb)
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

// TestCostAwareGroupIteratorUncompressedReverse exercises ReverseTimeOrder
// across two chunks to verify reverse traversal logic.
func TestCostAwareGroupIteratorUncompressedReverse(t *testing.T) {
	chunkOff1 := int64(0)
	chunkOff2 := int64(1000)
	msgs1 := []messageRef{
		{Timestamp: 10, TopicId: 0, OffsetInChunk: 0},
		{Timestamp: 20, TopicId: 0, OffsetInChunk: 10},
	}
	msgs2 := []messageRef{
		{Timestamp: 30, TopicId: 0, OffsetInChunk: 0},
		{Timestamp: 40, TopicId: 0, OffsetInChunk: 5},
	}
	bufs := [][]byte{
		[]byte("0000000000"), // chunk1 msg[0] (10 bytes)
		[]byte("1111111111"), // chunk1 msg[1] (10 bytes)
		[]byte("22222"),      // chunk2 msg[0] (5 bytes)
		[]byte("33333"),      // chunk2 msg[1] (5 bytes)
	}
	lb := &LoadedBytes{
		buffers: bufs,
		index: map[int64]bytesLocation{
			chunkOff1 + 0:  {bufferIdx: 0, inBufOff: 0, length: 10},
			chunkOff1 + 10: {bufferIdx: 1, inBufOff: 0, length: 10},
			chunkOff2 + 0:  {bufferIdx: 2, inBufOff: 0, length: 5},
			chunkOff2 + 5:  {bufferIdx: 3, inBufOff: 0, length: 5},
		},
	}
	ic1 := &IndexChunk{ChunkOffset: chunkOff1, ChunkLen: 20, UncompressedLen: 20}
	ic2 := &IndexChunk{ChunkOffset: chunkOff2, ChunkLen: 10, UncompressedLen: 10}

	it := newCostAwareGroupIterator(
		false,
		ReverseTimeOrder,
		[]*IndexChunk{ic1, ic2},
		[][]messageRef{msgs1, msgs2},
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

// TestCostAwareGroupIteratorCompressed registers one compressed chunk in
// LoadedBytes and verifies the iterator decompresses it once and slices
// individual messages out of the decompressed buffer.
func TestCostAwareGroupIteratorCompressed(t *testing.T) {
	// Build a synthetic "decompressed" payload of 30 bytes; messages of length
	// 10 each at offsets 0, 10, 20.
	uncompressed := []byte("AAAAAAAAAABBBBBBBBBBCCCCCCCCCC")
	compressed, err := compress(uncompressed)
	if err != nil {
		t.Fatalf("compress: %v", err)
	}

	chunkOff := int64(500)
	msgs := []messageRef{
		{Timestamp: 1, TopicId: 0, OffsetInChunk: 0},
		{Timestamp: 2, TopicId: 0, OffsetInChunk: 10},
		{Timestamp: 3, TopicId: 0, OffsetInChunk: 20},
	}
	lens := []int64{10, 10, 10}
	bufs := [][]byte{compressed}
	lb := &LoadedBytes{
		buffers: bufs,
		index: map[int64]bytesLocation{
			chunkOff: {bufferIdx: 0, inBufOff: 0, length: len(compressed)},
		},
	}
	ic := &IndexChunk{ChunkOffset: chunkOff, ChunkLen: int64(len(compressed)), UncompressedLen: int64(len(uncompressed))}

	it := newCostAwareGroupIterator(true, TimeOrder, []*IndexChunk{ic}, [][]messageRef{msgs}, [][]int64{lens}, lb)
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

// TestCostAwareGroupIteratorEmpty verifies an empty group iterator returns EOF
// without panicking.
func TestCostAwareGroupIteratorEmpty(t *testing.T) {
	it := newCostAwareGroupIterator(false, TimeOrder, nil, nil, nil, &LoadedBytes{index: map[int64]bytesLocation{}})
	_, _, _, err := it.Next()
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF, got %v", err)
	}
}
