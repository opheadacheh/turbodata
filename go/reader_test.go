package turbodata

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func writeReaderFixture(t *testing.T, setup func(*Writer) error, write func(*Writer) error) []byte {
	t.Helper()
	buf := bytes.NewBuffer(nil)
	writer := NewWriter(buf)
	if err := setup(writer); err != nil {
		t.Fatalf("setup: %v", err)
	}
	if write != nil {
		if err := write(writer); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	if err := writer.CloseTopic(); err != nil {
		t.Fatalf("close topic: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	return buf.Bytes()
}

func newReaderFromBytes(t *testing.T, data []byte) *Reader {
	t.Helper()
	reader, err := NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	return reader
}

func patchFooter(data []byte, summaryLen *int64, magic *[5]byte) []byte {
	out := append([]byte(nil), data...)
	if summaryLen != nil {
		binary.BigEndian.PutUint64(out[len(out)-footerLen:], uint64(*summaryLen))
	}
	if magic != nil {
		copy(out[len(out)-5:], magic[:])
	}
	return out
}

func minimalReaderSetup(compressed bool) func(*Writer) error {
	return func(w *Writer) error {
		opts := []WriteOption{}
		if compressed {
			opts = append(opts, WithCompression())
		}
		return w.OpenTopics([]string{"test"}, []map[string]any{{"foo": "bar"}}, opts...)
	}
}

func minimalReaderWrite(w *Writer) error {
	return w.WriteMessage("test", []byte("test"), 1)
}

var minimalSummaryWanted = &Summary{
	TopicsInfos: []*TopicsInfo{
		{
			TopicMetadatas: []*TopicMetadata{
				{Id: 1, Name: "test", Metadata: map[string]any{"foo": "bar"}},
			},
			IndexChunkInfoList: []*IndexChunkInfo{
				{StartTimestamp: 1, EndTimestamp: 1, Offset: 4},
			},
			TotalLen: 40,
		},
	},
}

var minimalCompressedSummaryWanted = &Summary{
	TopicsInfos: []*TopicsInfo{
		{
			TopicMetadatas: []*TopicMetadata{
				{Id: 1, Name: "test", Metadata: map[string]any{"foo": "bar", "is_compressed": true}},
			},
			IndexChunkInfoList: []*IndexChunkInfo{
				{StartTimestamp: 1, EndTimestamp: 1, Offset: 17},
			},
			TotalLen: 40,
		},
	},
}

func TestNewReader(t *testing.T) {
	validData := writeReaderFixture(t, minimalReaderSetup(false), minimalReaderWrite)

	t.Run("valid", func(t *testing.T) {
		reader, err := NewReader(bytes.NewReader(validData))
		if err != nil {
			t.Fatalf("NewReader: %v", err)
		}
		if reader == nil {
			t.Fatal("expected non-nil reader")
		}
	})

	t.Run("too_short", func(t *testing.T) {
		_, err := NewReader(bytes.NewReader([]byte{1, 2, 3}))
		if err == nil {
			t.Fatal("expected error for short file")
		}
		if !strings.Contains(err.Error(), "negative position") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("bad_magic", func(t *testing.T) {
		badMagic := [5]byte{'X', 'X', 'X', 'X', 'X'}
		data := patchFooter(validData, nil, &badMagic)
		_, err := NewReader(bytes.NewReader(data))
		if err == nil {
			t.Fatal("expected error for bad magic")
		}
		if !strings.Contains(err.Error(), "invalid magic number") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("truncated_footer", func(t *testing.T) {
		data := validData[:len(validData)-5]
		_, err := NewReader(bytes.NewReader(data))
		if err == nil {
			t.Fatal("expected error for truncated footer")
		}
		if !strings.Contains(err.Error(), "invalid magic number") {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestReaderSummary(t *testing.T) {
	t.Run("minimal", func(t *testing.T) {
		data := writeReaderFixture(t, minimalReaderSetup(false), minimalReaderWrite)
		reader := newReaderFromBytes(t, data)

		summary, err := reader.Summary()
		if err != nil {
			t.Fatalf("Summary: %v", err)
		}
		if diff := cmp.Diff(summary, minimalSummaryWanted); diff != "" {
			t.Fatalf("summary mismatch: %s", diff)
		}
	})

	t.Run("compressed", func(t *testing.T) {
		data := writeReaderFixture(t, minimalReaderSetup(true), minimalReaderWrite)
		reader := newReaderFromBytes(t, data)

		summary, err := reader.Summary()
		if err != nil {
			t.Fatalf("Summary: %v", err)
		}
		if diff := cmp.Diff(summary, minimalCompressedSummaryWanted); diff != "" {
			t.Fatalf("summary mismatch: %s", diff)
		}
	})

	t.Run("idempotent", func(t *testing.T) {
		data := writeReaderFixture(t, minimalReaderSetup(false), minimalReaderWrite)
		reader := newReaderFromBytes(t, data)

		s1, err := reader.Summary()
		if err != nil {
			t.Fatalf("first Summary: %v", err)
		}
		s2, err := reader.Summary()
		if err != nil {
			t.Fatalf("second Summary: %v", err)
		}
		if s1 != s2 {
			t.Fatal("expected cached summary pointer")
		}
	})
}
