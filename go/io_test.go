package turbodata

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

// oversizedReader returns a reader whose only content is a uint32 big-endian
// length prefix with no following body, used to trigger length-limit errors.
func oversizedReader(length uint32) io.Reader {
	buf := &bytes.Buffer{}
	binary.Write(buf, binary.BigEndian, length)
	return buf
}

// TestString covers readString / writeString.
func TestString(t *testing.T) {
	t.Run("roundtrip", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			s    string
		}{
			{"empty", ""},
			{"ascii", "hello world"},
			{"unicode", "こんにちは世界"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				buf := &bytes.Buffer{}
				if err := writeString(buf, tc.s); err != nil {
					t.Errorf("writeString(%q): %v", tc.s, err)
					return
				}
				got, err := readString(buf)
				if err != nil {
					t.Errorf("readString: %v", err)
					return
				}
				if diff := cmp.Diff(tc.s, got); diff != "" {
					t.Errorf("mismatch (-want +got):\n%s", diff)
				}
			})
		}
	})

	t.Run("oversized", func(t *testing.T) {
		_, err := readString(oversizedReader(maxStringLen + 1))
		if err == nil {
			t.Errorf("expected error for oversized string length, got nil")
		} else if !strings.Contains(err.Error(), "exceeds maximum") {
			t.Errorf("error %q should mention \"exceeds maximum\"", err)
		}
	})

	t.Run("truncated_len", func(t *testing.T) {
		_, err := readString(bytes.NewReader([]byte{}))
		if err == nil {
			t.Errorf("expected error reading length prefix from empty reader, got nil")
		} else if !errors.Is(err, io.EOF) {
			t.Errorf("expected io.EOF, got %v", err)
		}
	})

	t.Run("truncated_body", func(t *testing.T) {
		buf := &bytes.Buffer{}
		binary.Write(buf, binary.BigEndian, uint32(5))
		buf.Write([]byte{0x01, 0x02}) // only 2 bytes; length says 5
		_, err := readString(buf)
		if err == nil {
			t.Errorf("expected error for truncated body, got nil")
		} else if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Errorf("expected io.ErrUnexpectedEOF, got %v", err)
		}
	})
}

// TestMap covers readMap / writeMap.
func TestMap(t *testing.T) {
	t.Run("roundtrip", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			m    map[string]any
		}{
			{"empty", map[string]any{}},
			{"string_value", map[string]any{"key": "value"}},
			{"int_value", map[string]any{"num": int8(42)}},
			{"mixed", map[string]any{"hello": "world", "foo": int8(123)}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				buf := &bytes.Buffer{}
				if err := writeMap(buf, tc.m); err != nil {
					t.Errorf("writeMap: %v", err)
					return
				}
				got, err := readMap(buf)
				if err != nil {
					t.Errorf("readMap: %v", err)
					return
				}
				if diff := cmp.Diff(tc.m, got); diff != "" {
					t.Errorf("mismatch (-want +got):\n%s", diff)
				}
			})
		}
	})

	t.Run("oversized", func(t *testing.T) {
		_, err := readMap(oversizedReader(maxMapLen + 1))
		if err == nil {
			t.Errorf("expected error for oversized map length, got nil")
		} else if !strings.Contains(err.Error(), "exceeds maximum") {
			t.Errorf("error %q should mention \"exceeds maximum\"", err)
		}
	})

	t.Run("truncated_len", func(t *testing.T) {
		_, err := readMap(bytes.NewReader([]byte{}))
		if err == nil {
			t.Errorf("expected error reading length prefix from empty reader, got nil")
		} else if !errors.Is(err, io.EOF) {
			t.Errorf("expected io.EOF, got %v", err)
		}
	})

	t.Run("truncated_body", func(t *testing.T) {
		buf := &bytes.Buffer{}
		binary.Write(buf, binary.BigEndian, uint32(10))
		buf.Write([]byte{0x01, 0x02}) // only 2 bytes; length says 10
		_, err := readMap(buf)
		if err == nil {
			t.Errorf("expected error for truncated body, got nil")
		} else if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Errorf("expected io.ErrUnexpectedEOF, got %v", err)
		}
	})

	t.Run("invalid_msgpack", func(t *testing.T) {
		// 0xc1 is the "never used" format byte in msgpack — any conforming
		// implementation must reject it.
		invalid := []byte{0xc1}
		buf := &bytes.Buffer{}
		binary.Write(buf, binary.BigEndian, uint32(len(invalid)))
		buf.Write(invalid)
		_, err := readMap(buf)
		if err == nil {
			t.Errorf("expected unmarshal error for invalid msgpack bytes, got nil")
		} else if !strings.Contains(err.Error(), "msgpack") {
			t.Errorf("error %q should mention \"msgpack\"", err)
		}
	})
}

// TestFooter covers ReadFooter / WriteFooter.
func TestFooter(t *testing.T) {
	footer := &Footer{
		SummaryLen: 100,
		Magic:      [5]byte{'7', 'U', 'R', 'B', '0'},
	}

	t.Run("roundtrip", func(t *testing.T) {
		buf := &bytes.Buffer{}
		if err := WriteFooter(buf, footer); err != nil {
			t.Errorf("WriteFooter: %v", err)
			return
		}
		got, err := ReadFooter(buf)
		if err != nil {
			t.Errorf("ReadFooter: %v", err)
			return
		}
		if diff := cmp.Diff(footer, got); diff != "" {
			t.Errorf("mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("binary_size", func(t *testing.T) {
		// int64 (8 bytes) + [5]byte (5 bytes) = 13 bytes
		buf := &bytes.Buffer{}
		if err := WriteFooter(buf, footer); err != nil {
			t.Errorf("WriteFooter: %v", err)
			return
		}
		if buf.Len() != 13 {
			t.Errorf("binary size: got %d, want 13", buf.Len())
		}
	})

	t.Run("truncated", func(t *testing.T) {
		_, err := ReadFooter(bytes.NewReader([]byte{}))
		if err == nil {
			t.Errorf("expected error for empty reader, got nil")
		} else if !errors.Is(err, io.EOF) {
			t.Errorf("expected io.EOF, got %v", err)
		}
	})
}

// TestTopicMetadata covers ReadTopicMetadata / WriteTopicMetadata.
func TestTopicMetadata(t *testing.T) {
	t.Run("roundtrip", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			tm   *TopicMetadata
		}{
			{"populated", &TopicMetadata{Id: 42, Name: "camera/front", Metadata: map[string]any{"encoding": "jpeg"}}},
			{"empty_name_and_metadata", &TopicMetadata{Id: 1, Name: "", Metadata: map[string]any{}}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				buf := &bytes.Buffer{}
				if err := WriteTopicMetadata(buf, tc.tm); err != nil {
					t.Errorf("WriteTopicMetadata: %v", err)
					return
				}
				got, err := ReadTopicMetadata(buf)
				if err != nil {
					t.Errorf("ReadTopicMetadata: %v", err)
					return
				}
				if diff := cmp.Diff(tc.tm, got); diff != "" {
					t.Errorf("mismatch (-want +got):\n%s", diff)
				}
			})
		}
	})

	t.Run("truncated", func(t *testing.T) {
		_, err := ReadTopicMetadata(bytes.NewReader([]byte{}))
		if err == nil {
			t.Errorf("expected error for empty reader, got nil")
		} else if !errors.Is(err, io.EOF) {
			t.Errorf("expected io.EOF, got %v", err)
		}
	})
}

// TestIndexChunkInfo covers ReadIndexChunkInfo / WriteIndexChunkInfo.
func TestIndexChunkInfo(t *testing.T) {
	info := &IndexChunkInfo{StartTimestamp: 100, EndTimestamp: 200, Offset: 300}

	t.Run("roundtrip", func(t *testing.T) {
		buf := &bytes.Buffer{}
		if err := WriteIndexChunkInfo(buf, info); err != nil {
			t.Errorf("WriteIndexChunkInfo: %v", err)
			return
		}
		got, err := ReadIndexChunkInfo(buf)
		if err != nil {
			t.Errorf("ReadIndexChunkInfo: %v", err)
			return
		}
		if diff := cmp.Diff(info, got); diff != "" {
			t.Errorf("mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("binary_size", func(t *testing.T) {
		// 3 × int64 = 24 bytes
		buf := &bytes.Buffer{}
		if err := WriteIndexChunkInfo(buf, info); err != nil {
			t.Errorf("WriteIndexChunkInfo: %v", err)
			return
		}
		if buf.Len() != 24 {
			t.Errorf("binary size: got %d, want 24", buf.Len())
		}
	})

	t.Run("truncated", func(t *testing.T) {
		_, err := ReadIndexChunkInfo(bytes.NewReader([]byte{}))
		if err == nil {
			t.Errorf("expected error for empty reader, got nil")
		} else if !errors.Is(err, io.EOF) {
			t.Errorf("expected io.EOF, got %v", err)
		}
	})
}

// TestTopicsInfo covers ReadTopicsInfo / WriteTopicsInfo.
func TestTopicsInfo(t *testing.T) {
	populated := &TopicsInfo{
		TopicMetadatas: []*TopicMetadata{
			{Id: 1, Name: "topic1", Metadata: map[string]any{"a": "b"}},
			{Id: 2, Name: "topic2", Metadata: map[string]any{"c": "d"}},
		},
		IndexChunkInfoList: []*IndexChunkInfo{
			{StartTimestamp: 1, EndTimestamp: 2, Offset: 3},
			{StartTimestamp: 4, EndTimestamp: 5, Offset: 6},
		},
		TotalLen: 1000,
	}

	t.Run("roundtrip", func(t *testing.T) {
		buf := &bytes.Buffer{}
		if err := WriteTopicsInfo(buf, populated); err != nil {
			t.Errorf("WriteTopicsInfo: %v", err)
			return
		}
		got, err := ReadTopicsInfo(buf)
		if err != nil {
			t.Errorf("ReadTopicsInfo: %v", err)
			return
		}
		if diff := cmp.Diff(populated, got); diff != "" {
			t.Errorf("mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("empty", func(t *testing.T) {
		empty := &TopicsInfo{
			TopicMetadatas:     []*TopicMetadata{},
			IndexChunkInfoList: []*IndexChunkInfo{},
			TotalLen:           0,
		}
		buf := &bytes.Buffer{}
		if err := WriteTopicsInfo(buf, empty); err != nil {
			t.Errorf("WriteTopicsInfo: %v", err)
			return
		}
		got, err := ReadTopicsInfo(buf)
		if err != nil {
			t.Errorf("ReadTopicsInfo: %v", err)
			return
		}
		if diff := cmp.Diff(empty, got); diff != "" {
			t.Errorf("mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("truncated", func(t *testing.T) {
		_, err := ReadTopicsInfo(bytes.NewReader([]byte{}))
		if err == nil {
			t.Errorf("expected error for empty reader, got nil")
		} else if !errors.Is(err, io.EOF) {
			t.Errorf("expected io.EOF, got %v", err)
		}
	})
}

// TestSummary covers ReadSummary / WriteSummary.
func TestSummary(t *testing.T) {
	populated := &Summary{
		TopicsInfos: []*TopicsInfo{
			{
				TopicMetadatas:     []*TopicMetadata{{Id: 1, Name: "t1", Metadata: map[string]any{}}},
				IndexChunkInfoList: []*IndexChunkInfo{{StartTimestamp: 0, EndTimestamp: 10, Offset: 5}},
				TotalLen:           100,
			},
			{
				TopicMetadatas:     []*TopicMetadata{{Id: 2, Name: "t2", Metadata: map[string]any{}}},
				IndexChunkInfoList: []*IndexChunkInfo{{StartTimestamp: 10, EndTimestamp: 20, Offset: 15}},
				TotalLen:           200,
			},
		},
	}

	t.Run("roundtrip", func(t *testing.T) {
		buf := &bytes.Buffer{}
		if err := WriteSummary(buf, populated); err != nil {
			t.Errorf("WriteSummary: %v", err)
			return
		}
		got, err := ReadSummary(buf)
		if err != nil {
			t.Errorf("ReadSummary: %v", err)
			return
		}
		if diff := cmp.Diff(populated, got); diff != "" {
			t.Errorf("mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("empty", func(t *testing.T) {
		empty := &Summary{TopicsInfos: []*TopicsInfo{}}
		buf := &bytes.Buffer{}
		if err := WriteSummary(buf, empty); err != nil {
			t.Errorf("WriteSummary: %v", err)
			return
		}
		got, err := ReadSummary(buf)
		if err != nil {
			t.Errorf("ReadSummary: %v", err)
			return
		}
		if diff := cmp.Diff(empty, got); diff != "" {
			t.Errorf("mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("truncated", func(t *testing.T) {
		_, err := ReadSummary(bytes.NewReader([]byte{}))
		if err == nil {
			t.Errorf("expected error for empty reader, got nil")
		} else if !errors.Is(err, io.EOF) {
			t.Errorf("expected io.EOF, got %v", err)
		}
	})
}

// TestMessageIndex covers ReadMessageIndex / WriteMessageIndex.
func TestMessageIndex(t *testing.T) {
	mi := &MessageIndex{Timestamp: 123456789, OffsetInChunk: 42}

	t.Run("roundtrip", func(t *testing.T) {
		buf := &bytes.Buffer{}
		if err := WriteMessageIndex(buf, mi); err != nil {
			t.Errorf("WriteMessageIndex: %v", err)
			return
		}
		got, err := ReadMessageIndex(buf)
		if err != nil {
			t.Errorf("ReadMessageIndex: %v", err)
			return
		}
		if diff := cmp.Diff(mi, got); diff != "" {
			t.Errorf("mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("binary_size", func(t *testing.T) {
		// 2 × int64 = 16 bytes
		buf := &bytes.Buffer{}
		if err := WriteMessageIndex(buf, mi); err != nil {
			t.Errorf("WriteMessageIndex: %v", err)
			return
		}
		if buf.Len() != 16 {
			t.Errorf("binary size: got %d, want 16", buf.Len())
		}
	})

	t.Run("truncated", func(t *testing.T) {
		_, err := ReadMessageIndex(bytes.NewReader([]byte{}))
		if err == nil {
			t.Errorf("expected error for empty reader, got nil")
		} else if !errors.Is(err, io.EOF) {
			t.Errorf("expected io.EOF, got %v", err)
		}
	})
}

// TestTopicIndex covers ReadTopicIndex / WriteTopicIndex.
func TestTopicIndex(t *testing.T) {
	populated := &TopicIndex{
		Id: 7,
		MessageIndexes: []*MessageIndex{
			{Timestamp: 1, OffsetInChunk: 10},
			{Timestamp: 2, OffsetInChunk: 20},
		},
		KeyFrameIndexes: []uint32{0, 1, 5},
	}

	t.Run("roundtrip", func(t *testing.T) {
		buf := &bytes.Buffer{}
		if err := WriteTopicIndex(buf, populated); err != nil {
			t.Errorf("WriteTopicIndex: %v", err)
			return
		}
		got, err := ReadTopicIndex(buf)
		if err != nil {
			t.Errorf("ReadTopicIndex: %v", err)
			return
		}
		if diff := cmp.Diff(populated, got); diff != "" {
			t.Errorf("mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("empty", func(t *testing.T) {
		empty := &TopicIndex{
			Id:              0,
			MessageIndexes:  []*MessageIndex{},
			KeyFrameIndexes: []uint32{},
		}
		buf := &bytes.Buffer{}
		if err := WriteTopicIndex(buf, empty); err != nil {
			t.Errorf("WriteTopicIndex: %v", err)
			return
		}
		got, err := ReadTopicIndex(buf)
		if err != nil {
			t.Errorf("ReadTopicIndex: %v", err)
			return
		}
		if diff := cmp.Diff(empty, got); diff != "" {
			t.Errorf("mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("truncated", func(t *testing.T) {
		_, err := ReadTopicIndex(bytes.NewReader([]byte{}))
		if err == nil {
			t.Errorf("expected error for empty reader, got nil")
		} else if !errors.Is(err, io.EOF) {
			t.Errorf("expected io.EOF, got %v", err)
		}
	})
}

// TestIndexChunk covers ReadIndexChunk / WriteIndexChunk.
func TestIndexChunk(t *testing.T) {
	populated := &IndexChunk{
		TopicIndexes: []*TopicIndex{
			{
				Id:              1,
				MessageIndexes:  []*MessageIndex{{Timestamp: 100, OffsetInChunk: 0}},
				KeyFrameIndexes: []uint32{0},
			},
			{
				Id:              2,
				MessageIndexes:  []*MessageIndex{{Timestamp: 200, OffsetInChunk: 16}},
				KeyFrameIndexes: []uint32{0},
			},
		},
		ChunkOffset:     512,
		ChunkLen:        1024,
		UncompressedLen: 2048,
	}

	t.Run("roundtrip", func(t *testing.T) {
		buf := &bytes.Buffer{}
		if err := WriteIndexChunk(buf, populated); err != nil {
			t.Errorf("WriteIndexChunk: %v", err)
			return
		}
		got, err := ReadIndexChunk(buf)
		if err != nil {
			t.Errorf("ReadIndexChunk: %v", err)
			return
		}
		if diff := cmp.Diff(populated, got); diff != "" {
			t.Errorf("mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("empty", func(t *testing.T) {
		empty := &IndexChunk{
			TopicIndexes:    []*TopicIndex{},
			ChunkOffset:     0,
			ChunkLen:        0,
			UncompressedLen: 0,
		}
		buf := &bytes.Buffer{}
		if err := WriteIndexChunk(buf, empty); err != nil {
			t.Errorf("WriteIndexChunk: %v", err)
			return
		}
		got, err := ReadIndexChunk(buf)
		if err != nil {
			t.Errorf("ReadIndexChunk: %v", err)
			return
		}
		if diff := cmp.Diff(empty, got); diff != "" {
			t.Errorf("mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("truncated", func(t *testing.T) {
		_, err := ReadIndexChunk(bytes.NewReader([]byte{}))
		if err == nil {
			t.Errorf("expected error for empty reader, got nil")
		} else if !errors.Is(err, io.EOF) {
			t.Errorf("expected io.EOF, got %v", err)
		}
	})
}
