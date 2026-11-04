package turbodata

import (
	"bytes"
	"io"
	"os"
	"testing"
)

func TestReadKeyFrameMarks(t *testing.T) {
	// Shared fixtures also exercise TS/Python. A range starting at a delta
	// frame must not mark that first result as a key frame.
	for _, cost := range []bool{false, true} {
		for _, reverse := range []bool{false, true} {
			for _, decodable := range []bool{false, true} {
				var readers []*Reader
				for _, name := range []string{"video-multi-gop-one-chunk.td", "single-topic-uncompressed.td"} {
					data, err := os.ReadFile("../../ts/test/fixtures/" + name)
					if err != nil {
						t.Fatal(err)
					}
					readers = append(readers, NewReader(bytes.NewReader(data)))
				}
				mr, err := NewMultiReader(readers...)
				if err != nil {
					t.Fatal(err)
				}
				opts := []ReadOption{WithStartTimestamp(35)}
				if cost {
					opts = append(opts, WithReadStrategy(ReadStrategy{CoalesceGap: 1 << 20, SplitThreshold: 4 << 20, MaxConcurrency: 2}))
				}
				if reverse {
					opts = append(opts, WithOrder(ReverseTimeOrder))
				}
				if decodable {
					opts = append(opts, WithVideoDecodable())
				}
				it, err := mr.ReadMessages(opts...)
				if err != nil {
					t.Fatal(err)
				}
				want := []string{"B1", "KC", "C1"}
				if decodable {
					want = append([]string{"KB"}, want...)
				}
				if reverse {
					for i, j := 0, len(want)-1; i < j; i, j = i+1, j-1 {
						want[i], want[j] = want[j], want[i]
					}
				}
				buf := NewReusableBuffer()
				n := 0
				for {
					_, topic, mark, err := it.NextIntoWithKeyFrameMark(buf)
					if err == io.EOF {
						break
					}
					if err != nil {
						t.Fatal(err)
					}
					if topic != "/cam" {
						if mark {
							t.Fatal("non-video marked as key frame")
						}
						continue
					}
					label := string(buf.Data)
					if n >= len(want) || label != want[n] || mark != (label == "KB" || label == "KC") {
						t.Fatalf("unexpected frame %s mark=%v", label, mark)
					}
					n++
				}
				if n != len(want) {
					t.Fatalf("got %d frames, want %d", n, len(want))
				}
			}
		}
	}
}
