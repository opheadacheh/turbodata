package turbodata

import (
	"errors"
	"io"
	"testing"
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
		messageIndex: &MessageIndex{Timestamp: ts, OffsetInChunk: off},
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
	lb := &LoadedBytes{
		buffers: bufs,
		index: map[int64]bytesLocation{
			chunkOff + 0:   {bufferIdx: 0, inBufOff: 0, length: 10},
			chunkOff + 100: {bufferIdx: 1, inBufOff: 0, length: 50},
			chunkOff + 250: {bufferIdx: 2, inBufOff: 0, length: 30},
		},
	}
	ic := &IndexChunk{ChunkOffset: chunkOff, ChunkLen: 280, UncompressedLen: 280}

	it := newPreloadedTopicsGroupIterator(false, TimeOrder, []*IndexChunk{ic}, [][]*messageIndexWithTopicId{msgs}, [][]int64{lens}, lb)
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

	it := newPreloadedTopicsGroupIterator(
		false,
		ReverseTimeOrder,
		[]*IndexChunk{ic1, ic2},
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
	compressed, err := compress(uncompressed)
	if err != nil {
		t.Fatalf("compress: %v", err)
	}

	chunkOff := int64(500)
	msgs := []*messageIndexWithTopicId{
		mkMsg(1, 0, 0),
		mkMsg(2, 0, 10),
		mkMsg(3, 0, 20),
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

	it := newPreloadedTopicsGroupIterator(true, TimeOrder, []*IndexChunk{ic}, [][]*messageIndexWithTopicId{msgs}, [][]int64{lens}, lb)
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
	it := newPreloadedTopicsGroupIterator(false, TimeOrder, nil, nil, nil, &LoadedBytes{index: map[int64]bytesLocation{}})
	_, _, _, err := it.Next()
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF, got %v", err)
	}
}
