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

package turbodata

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/opheadacheh/turbodata/go/turbodata/format"
	"github.com/opheadacheh/turbodata/go/turbodata/internal/compress"
)

func makeReaderFixture(t *testing.T, body []byte, compressedSummary []byte, footer *format.Footer) ReadSource {
	t.Helper()

	if footer == nil {
		footer = &format.Footer{
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
	if err := format.WriteFooter(buf, footer); err != nil {
		t.Fatalf("WriteFooter: %v", err)
	}

	return bytes.NewReader(buf.Bytes())
}

func makeCompressedSummary(t *testing.T, summary *format.Summary) []byte {
	t.Helper()

	summaryBuf := &bytes.Buffer{}
	if err := format.WriteSummary(summaryBuf, summary); err != nil {
		t.Fatalf("WriteSummary: %v", err)
	}

	compressed, err := compress.Compress(summaryBuf.Bytes())
	if err != nil {
		t.Fatalf("compress summary: %v", err)
	}

	return compressed
}

func TestNewReader(t *testing.T) {
	validSummary := &format.Summary{TopicsInfos: []*format.TopicsInfo{}}
	validCompressed := makeCompressedSummary(t, validSummary)

	t.Run("valid_footer_and_magic", func(t *testing.T) {
		reader := NewReader(makeReaderFixture(t, nil, validCompressed, nil))
		if reader == nil {
			t.Errorf("NewReader returned nil reader")
		}
	})

	t.Run("invalid_magic", func(t *testing.T) {
		rs := makeReaderFixture(t, nil, validCompressed, &format.Footer{
			SummaryLen: int64(len(validCompressed)),
			Magic:      [5]byte{'B', 'A', 'D', '!', '!'},
		})
		reader := NewReader(rs)
		_, err := reader.Summary()
		if err == nil {
			t.Errorf("expected invalid magic error, got nil")
		} else if !strings.Contains(err.Error(), "invalid magic number") {
			t.Errorf("error %q should mention invalid magic number", err)
		}
	})

	t.Run("truncated_input", func(t *testing.T) {
		reader := NewReader(bytes.NewReader([]byte{0x01, 0x02}))
		_, err := reader.Summary()
		if err == nil {
			t.Errorf("expected error for input shorter than footer, got nil")
		}
	})
}

func TestReaderSummary(t *testing.T) {
	populated := &format.Summary{
		TopicsInfos: []*format.TopicsInfo{
			{
				TopicMetadatas: []*format.TopicMetadata{
					{Id: 1, Name: "topic/1", Metadata: map[string]any{"encoding": "json"}},
				},
				IndexChunkInfoList: []*format.IndexChunkInfo{
					{StartTimestamp: 10, EndTimestamp: 20, Offset: 128},
				},
				TotalLen: 512,
			},
		},
	}
	empty := &format.Summary{TopicsInfos: []*format.TopicsInfo{}}

	t.Run("roundtrip_populated_and_empty", func(t *testing.T) {
		for _, tc := range []struct {
			name    string
			summary *format.Summary
		}{
			{name: "populated", summary: populated},
			{name: "empty", summary: empty},
		} {
			t.Run(tc.name, func(t *testing.T) {
				compressed := makeCompressedSummary(t, tc.summary)
				reader := NewReader(makeReaderFixture(t, []byte("body"), compressed, nil))

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
		reader := NewReader(makeReaderFixture(t, []byte("body"), compressed, nil))

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
		rs := makeReaderFixture(t, nil, compressed, &format.Footer{
			SummaryLen: int64(len(compressed) + 1),
			Magic:      [5]byte{'7', 'U', 'R', 'B', '0'},
		})
		reader := NewReader(rs)

		_, err := reader.Summary()
		if err == nil {
			t.Errorf("expected error for truncated compressed summary, got nil")
		}
	})

	t.Run("invalid_compressed_bytes", func(t *testing.T) {
		reader := NewReader(makeReaderFixture(t, nil, []byte("not-zstd-data"), nil))

		_, err := reader.Summary()
		if err == nil {
			t.Errorf("expected decompress error for invalid compressed bytes, got nil")
		}
	})

	t.Run("invalid_summary_payload", func(t *testing.T) {
		compressed, err := compress.Compress([]byte{0x01, 0x02})
		if err != nil {
			t.Fatalf("compress invalid summary payload: %v", err)
		}

		reader := NewReader(makeReaderFixture(t, nil, compressed, nil))

		_, err = reader.Summary()
		if err == nil {
			t.Errorf("expected summary parse error, got nil")
		} else if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Errorf("expected EOF parse error, got %v", err)
		}
	})
}

// countingReadSource wraps a ReadSource, counting Read/Seek/ReadAt calls so
// tests can assert how many I/O requests summaryWithHint issues.
type countingReadSource struct {
	rs          ReadSource
	ReadCalls   int
	SeekCalls   int
	ReadAtCalls int
}

func (c *countingReadSource) Read(p []byte) (int, error) { c.ReadCalls++; return c.rs.Read(p) }
func (c *countingReadSource) Seek(off int64, w int) (int64, error) {
	c.SeekCalls++
	return c.rs.Seek(off, w)
}
func (c *countingReadSource) ReadAt(p []byte, off int64) (int, error) {
	c.ReadAtCalls++
	return c.rs.ReadAt(p, off)
}

func TestSummaryWithHint(t *testing.T) {
	summary := &format.Summary{
		TopicsInfos: []*format.TopicsInfo{
			{
				TopicMetadatas: []*format.TopicMetadata{
					{Id: 1, Name: "t", Metadata: map[string]any{}},
				},
				TotalLen: 8,
			},
		},
	}
	compressed := makeCompressedSummary(t, summary)
	body := make([]byte, 4096) // arbitrary body so file is larger than tail.

	t.Run("hint_zero_falls_back_to_seek_read", func(t *testing.T) {
		// No hint: footer and summary each cost one Seek+Read (2 total).
		c := &countingReadSource{rs: makeReaderFixture(t, body, compressed, nil)}
		reader := NewReader(c)

		if _, err := reader.summaryWithHint(0); err != nil {
			t.Fatalf("summaryWithHint(0): %v", err)
		}
		if c.ReadAtCalls != 2 {
			t.Errorf("hint=0 should issue two ReadAt calls, got ReadAtCalls=%d", c.ReadAtCalls)
		}
		if c.SeekCalls != 1 || c.ReadCalls != 0 {
			t.Errorf("hint=0 should not issue 1 Seek and 0 Read, got SeekCalls=%d ReadCalls=%d", c.SeekCalls, c.ReadCalls)
		}
	})

	t.Run("hint_covers_footer_and_summary_one_read_at", func(t *testing.T) {
		// Sufficient hint: one ReadAt covers footer + compressed summary.
		c := &countingReadSource{rs: makeReaderFixture(t, body, compressed, nil)}
		reader := NewReader(c)

		hint := int64(len(compressed) + format.FooterLen + 8) // covers footer + summary + slack
		if _, err := reader.ReadIndex(WithTailPrefetch(hint)); err != nil {
			t.Fatalf("ReadIndex: %v", err)
		}
		if c.ReadAtCalls != 1 {
			t.Errorf("expected exactly 1 ReadAt for prefetch hit, got %d", c.ReadAtCalls)
		}
		if c.SeekCalls != 1 || c.ReadCalls != 0 {
			t.Errorf("hint=0 should not issue 1 Seek and 0 Read, got SeekCalls=%d ReadCalls=%d", c.SeekCalls, c.ReadCalls)
		}
	})

	t.Run("hint_too_small_falls_back_to_seek_read", func(t *testing.T) {
		// Hint covers footer but not summary: one ReadAt for the tail + one
		// Seek+Read for the summary.
		c := &countingReadSource{rs: makeReaderFixture(t, body, compressed, nil)}
		reader := NewReader(c)

		if _, err := reader.summaryWithHint(format.FooterLen); err != nil {
			t.Fatalf("summaryWithHint: %v", err)
		}
		if c.ReadAtCalls != 2 {
			t.Errorf("hint=footerLen should issue two ReadAt calls, got ReadAtCalls=%d", c.ReadAtCalls)
		}
		if c.SeekCalls != 1 || c.ReadCalls != 0 {
			t.Errorf("hint=0 should not issue 1 Seek and 0 Read, got SeekCalls=%d ReadCalls=%d", c.SeekCalls, c.ReadCalls)
		}
	})
}

// indexRegionSource rejects payload reads through both the serial and the
// concurrent I/O entry points, including speculative or coalesced ranges.
type indexRegionSource struct {
	*bytes.Reader
	indexStart int64
}

func (s *indexRegionSource) Read(p []byte) (int, error) {
	offset, err := s.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	if offset < s.indexStart {
		return 0, fmt.Errorf("payload read at %d", offset)
	}
	return s.Reader.Read(p)
}

func (s *indexRegionSource) ReadAt(p []byte, offset int64) (int, error) {
	if offset < s.indexStart {
		return 0, fmt.Errorf("payload read at %d", offset)
	}
	return s.Reader.ReadAt(p, offset)
}

func TestReadIndex(t *testing.T) {
	for _, tc := range []struct {
		file           string
		topic          string
		start, end     int64
		videoDecodable bool
	}{
		{"multi-group-mixed.td", "/groupB/y", 4000000, 4004700, true},
		{"video-multi-gop-chunks.td", "/cam", 20, 60, false},
		{"video-multi-gop-chunks.td", "/cam", 20, 60, true},
	} {
		t.Run(tc.file, func(t *testing.T) {
			file, err := os.ReadFile("../../ts/test/fixtures/" + tc.file)
			if err != nil {
				t.Fatal(err)
			}
			remap := WithTopicRemap(map[string]string{tc.topic: "renamed"})
			reference := NewReader(bytes.NewReader(file), remap)
			summary, err := reference.Summary()
			if err != nil {
				t.Fatal(err)
			}
			indexStart := summary.TopicsInfos[0].IndexChunkInfoList[0].Offset
			for _, order := range []Order{TimeOrder, ReverseTimeOrder} {
				for _, topics := range [][]string{nil, {"renamed"}, {"missing"}} {
					opts := []ReadOption{WithTopicNames(topics), WithStartTimestamp(tc.start), WithEndTimestamp(tc.end), WithOrder(order)}
					if tc.videoDecodable {
						opts = append(opts, WithVideoDecodable())
					}
					msgs, err := reference.ReadMessages(opts...)
					if err != nil {
						t.Fatal(err)
					}
					var want []IndexEntry
					buf := NewReusableBuffer()
					for {
						ts, name, mark, err := msgs.NextIntoWithKeyFrameMark(buf)
						if err == io.EOF {
							break
						}
						if err != nil {
							t.Fatal(err)
						}
						want = append(want, IndexEntry{Timestamp: ts, TopicName: name, IsKeyFrame: mark})
					}
					for _, strategy := range []*ReadStrategy{nil, {CoalesceGap: 1 << 20, SplitThreshold: 64, MaxConcurrency: 3}} {
						r := NewReader(&indexRegionSource{bytes.NewReader(file), indexStart}, remap)
						indexOpts := opts
						if strategy != nil {
							indexOpts = append(indexOpts, WithReadStrategy(*strategy))
						}
						it, err := r.ReadIndex(indexOpts...)
						if err != nil {
							t.Fatal(err)
						}
						var got []IndexEntry
						for {
							entry, err := it.Next()
							if err == io.EOF {
								break
							}
							if err != nil {
								t.Fatal(err)
							}
							got = append(got, entry)
						}
						if diff := cmp.Diff(want, got); diff != "" {
							t.Fatalf("index entries (-want +got):\n%s", diff)
						}
					}
				}
			}
		})
	}
}

func TestReadIndexStopsAtChunkBoundary(t *testing.T) {
	file, err := os.ReadFile("../../ts/test/fixtures/video-multi-gop-chunks.td")
	if err != nil {
		t.Fatal(err)
	}
	source := &countingReadSource{rs: bytes.NewReader(file)}
	it, err := NewReader(source).ReadIndex()
	if err != nil {
		t.Fatal(err)
	}
	// The first index chunk has two frames. Returning both should still
	// require just one serial chunk read, even at the chunk boundary.
	for _, ts := range []int64{10, 20} {
		entry, err := it.Next()
		if err != nil || entry.Timestamp != ts {
			t.Fatalf("Next() = %v, %v; want timestamp %d", entry, err, ts)
		}
	}
	if source.ReadCalls != 1 {
		t.Fatalf("read %d chunks, want 1", source.ReadCalls)
	}
}
