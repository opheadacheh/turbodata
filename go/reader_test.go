package turbodata

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func makeReaderFixture(t *testing.T, body []byte, compressedSummary []byte, footer *Footer) io.ReadSeeker {
	t.Helper()

	if footer == nil {
		footer = &Footer{
			SummaryLen: int64(len(compressedSummary)),
			Magic:      [5]byte{'7', 'U', 'R', 'B', '0'},
		}
	}

	buf := &bytes.Buffer{}
	if _, err := buf.Write(body); err != nil {
		t.Fatalf("write body: %v", err)
	}
	if _, err := buf.Write(compressedSummary); err != nil {
		t.Fatalf("write compressed summary: %v", err)
	}
	if err := WriteFooter(buf, footer); err != nil {
		t.Fatalf("WriteFooter: %v", err)
	}

	return bytes.NewReader(buf.Bytes())
}

func makeCompressedSummary(t *testing.T, summary *Summary) []byte {
	t.Helper()

	summaryBuf := &bytes.Buffer{}
	if err := WriteSummary(summaryBuf, summary); err != nil {
		t.Fatalf("WriteSummary: %v", err)
	}

	compressed, err := compress(summaryBuf.Bytes())
	if err != nil {
		t.Fatalf("compress summary: %v", err)
	}

	return compressed
}

func TestNewReader(t *testing.T) {
	validSummary := &Summary{TopicsInfos: []*TopicsInfo{}}
	validCompressed := makeCompressedSummary(t, validSummary)

	t.Run("valid_footer_and_magic", func(t *testing.T) {
		reader, err := NewReader(makeReaderFixture(t, nil, validCompressed, nil))
		if err != nil {
			t.Errorf("NewReader: %v", err)
			return
		}
		if reader == nil {
			t.Errorf("NewReader returned nil reader without error")
		}
	})

	t.Run("invalid_magic", func(t *testing.T) {
		rs := makeReaderFixture(t, nil, validCompressed, &Footer{
			SummaryLen: int64(len(validCompressed)),
			Magic:      [5]byte{'B', 'A', 'D', '!', '!'},
		})
		_, err := NewReader(rs)
		if err == nil {
			t.Errorf("expected invalid magic error, got nil")
		} else if !strings.Contains(err.Error(), "invalid magic number") {
			t.Errorf("error %q should mention invalid magic number", err)
		}
	})

	t.Run("truncated_input", func(t *testing.T) {
		_, err := NewReader(bytes.NewReader([]byte{0x01, 0x02}))
		if err == nil {
			t.Errorf("expected error for input shorter than footer, got nil")
		}
	})
}

func TestReaderSummary(t *testing.T) {
	populated := &Summary{
		TopicsInfos: []*TopicsInfo{
			{
				TopicMetadatas: []*TopicMetadata{
					{Id: 1, Name: "topic/1", Metadata: map[string]any{"encoding": "json"}},
				},
				IndexChunkInfoList: []*IndexChunkInfo{
					{StartTimestamp: 10, EndTimestamp: 20, Offset: 128},
				},
				TotalLen: 512,
			},
		},
	}
	empty := &Summary{TopicsInfos: []*TopicsInfo{}}

	t.Run("roundtrip_populated_and_empty", func(t *testing.T) {
		for _, tc := range []struct {
			name    string
			summary *Summary
		}{
			{name: "populated", summary: populated},
			{name: "empty", summary: empty},
		} {
			t.Run(tc.name, func(t *testing.T) {
				compressed := makeCompressedSummary(t, tc.summary)
				reader, err := NewReader(makeReaderFixture(t, []byte("body"), compressed, nil))
				if err != nil {
					t.Errorf("NewReader: %v", err)
					return
				}

				got, err := reader.Summary()
				if err != nil {
					t.Errorf("Summary: %v", err)
					return
				}
				if diff := cmp.Diff(tc.summary, got); diff != "" {
					t.Errorf("summary mismatch (-want +got):\n%s", diff)
				}
			})
		}
	})

	t.Run("cached_result", func(t *testing.T) {
		compressed := makeCompressedSummary(t, populated)
		reader, err := NewReader(makeReaderFixture(t, []byte("body"), compressed, nil))
		if err != nil {
			t.Errorf("NewReader: %v", err)
			return
		}

		first, err := reader.Summary()
		if err != nil {
			t.Errorf("first Summary(): %v", err)
			return
		}

		reader.rs = bytes.NewReader([]byte{})
		reader.footer.SummaryLen = 1 << 60

		second, err := reader.Summary()
		if err != nil {
			t.Errorf("second Summary(): %v", err)
			return
		}

		if first != second {
			t.Errorf("expected cached summary pointer to be reused")
		}
	})

	t.Run("truncated_compressed_summary", func(t *testing.T) {
		compressed := makeCompressedSummary(t, populated)
		rs := makeReaderFixture(t, nil, compressed, &Footer{
			SummaryLen: int64(len(compressed) + 1),
			Magic:      [5]byte{'7', 'U', 'R', 'B', '0'},
		})
		reader, err := NewReader(rs)
		if err != nil {
			t.Errorf("NewReader: %v", err)
			return
		}

		_, err = reader.Summary()
		if err == nil {
			t.Errorf("expected error for truncated compressed summary, got nil")
		}
	})

	t.Run("invalid_compressed_bytes", func(t *testing.T) {
		reader, err := NewReader(makeReaderFixture(t, nil, []byte("not-zstd-data"), nil))
		if err != nil {
			t.Errorf("NewReader: %v", err)
			return
		}

		_, err = reader.Summary()
		if err == nil {
			t.Errorf("expected decompress error for invalid compressed bytes, got nil")
		}
	})

	t.Run("invalid_summary_payload", func(t *testing.T) {
		compressed, err := compress([]byte{0x01, 0x02})
		if err != nil {
			t.Fatalf("compress invalid summary payload: %v", err)
		}

		reader, err := NewReader(makeReaderFixture(t, nil, compressed, nil))
		if err != nil {
			t.Errorf("NewReader: %v", err)
			return
		}

		_, err = reader.Summary()
		if err == nil {
			t.Errorf("expected summary parse error, got nil")
		} else if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Errorf("expected EOF parse error, got %v", err)
		}
	})
}
