// Command gen-fixtures emits a small set of turbodata files plus a
// per-fixture .golden.txt that the TypeScript reader can compare against.
//
// Each .golden.txt has the form:
//
//	# format: hex line per topic, hex line per message, then trailing summary
//	topics:<count>
//	topic <id> <name> <metadata-json>
//	... (one per topic)
//	messages:<count>
//	stream-sha256:<hex>
//	first <ts> <topicName> <sha256(data)>
//	last  <ts> <topicName> <sha256(data)>
//
// The `stream-sha256` is a sha256 over a deterministic encoding of the full
// default-path iteration:
//
//	for each yielded message:
//	    write 8 bytes big-endian timestamp
//	    write utf-8 topicName bytes
//	    write 0x00 separator
//	    write data bytes
//	    write 0x00 separator
//
// The TS reader computes the same hash and asserts equality.
//
// Usage:
//
//	cd go && go run ./cmd/gen-fixtures -out ../ts/test/fixtures
package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	turbodata "turbodata"
)

type fixtureSpec struct {
	name string
	build func(w *turbodata.Writer) error
}

func main() {
	out := flag.String("out", "../ts/test/fixtures", "output directory for fixtures")
	flag.Parse()

	if err := os.MkdirAll(*out, 0o755); err != nil {
		fail(err)
	}

	specs := []fixtureSpec{
		{name: "single-topic-uncompressed", build: buildSingleTopicUncompressed},
		{name: "single-topic-compressed", build: buildSingleTopicCompressed},
		{name: "multi-topic-compressed", build: buildMultiTopicCompressed},
		{name: "multi-group-mixed", build: buildMultiGroupMixed},
	}

	for _, s := range specs {
		path := filepath.Join(*out, s.name+".td")
		if err := writeFixture(path, s.build); err != nil {
			fail(fmt.Errorf("%s: %w", s.name, err))
		}
		if err := writeGolden(path); err != nil {
			fail(fmt.Errorf("%s golden: %w", s.name, err))
		}
		fmt.Printf("wrote %s\n", path)
	}
}

func writeFixture(path string, build func(w *turbodata.Writer) error) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := turbodata.NewWriter(f)
	if err := build(w); err != nil {
		return err
	}
	return w.Close()
}

// --------- fixture builders ---------

// 1 topic, uncompressed, ~30 small messages, default size-based chunking
// (so all messages live in a single chunk).
func buildSingleTopicUncompressed(w *turbodata.Writer) error {
	if err := w.OpenTopics(
		[]string{"/sensor"},
		[]map[string]any{{"description": "uncompressed sensor"}},
	); err != nil {
		return err
	}
	for i := 0; i < 30; i++ {
		ts := int64(1_000_000 + i*1000)
		msg := []byte(fmt.Sprintf("uncompressed-message-%03d", i))
		if err := w.WriteMessage("/sensor", msg, ts); err != nil {
			return err
		}
	}
	return w.CloseTopic()
}

// 1 topic, compressed, ~30 messages, default size-based chunking.
func buildSingleTopicCompressed(w *turbodata.Writer) error {
	if err := w.OpenTopics(
		[]string{"/sensor"},
		[]map[string]any{{"description": "compressed sensor"}},
		turbodata.WithCompression(),
	); err != nil {
		return err
	}
	for i := 0; i < 30; i++ {
		ts := int64(2_000_000 + i*1000)
		// Pad to make compression non-trivial.
		msg := append([]byte(fmt.Sprintf("compressed-message-%03d-", i)),
			make([]byte, 64)...)
		if err := w.WriteMessage("/sensor", msg, ts); err != nil {
			return err
		}
	}
	return w.CloseTopic()
}

// 1 group with 3 topics, compressed, count-based chunking forcing multiple
// chunks. Verifies cross-topic interleaved sortAndFilter and the multi-chunk
// path.
func buildMultiTopicCompressed(w *turbodata.Writer) error {
	chunkCfg := &turbodata.ChunkConfig{
		Mode:  turbodata.ChunkThresholdModeCount,
		Count: 12, // ~3 chunks for 36 total messages
	}
	if err := w.OpenTopics(
		[]string{"/cam", "/imu", "/lidar"},
		[]map[string]any{
			{"channel": "cam", "rate": int64(30)},
			{"channel": "imu", "rate": int64(200)},
			{"channel": "lidar"},
		},
		turbodata.WithCompression(),
		turbodata.WithChunkConfig(chunkCfg),
	); err != nil {
		return err
	}
	// Interleave across topics; timestamps strictly non-decreasing.
	topics := []string{"/cam", "/imu", "/lidar"}
	for i := 0; i < 36; i++ {
		ts := int64(3_000_000 + i*100)
		topic := topics[i%len(topics)]
		msg := []byte(fmt.Sprintf("topic=%s seq=%03d payload=%s",
			topic, i, mkPayload(i)))
		if err := w.WriteMessage(topic, msg, ts); err != nil {
			return err
		}
	}
	return w.CloseTopic()
}

// 2 separate topic groups (separate OpenTopics/CloseTopic cycles): one
// uncompressed group, one compressed group. Verifies cross-group merge in
// the message heap.
func buildMultiGroupMixed(w *turbodata.Writer) error {
	// Group A: uncompressed, single topic, count chunks of 6.
	if err := w.OpenTopics(
		[]string{"/groupA/data"},
		[]map[string]any{{"group": "A"}},
		turbodata.WithChunkConfig(&turbodata.ChunkConfig{
			Mode:  turbodata.ChunkThresholdModeCount,
			Count: 6,
		}),
	); err != nil {
		return err
	}
	for i := 0; i < 18; i++ {
		ts := int64(4_000_000 + i*200)
		if err := w.WriteMessage("/groupA/data",
			[]byte(fmt.Sprintf("A-%03d-%s", i, mkPayload(i))), ts); err != nil {
			return err
		}
	}
	if err := w.CloseTopic(); err != nil {
		return err
	}

	// Group B: compressed, two topics, count chunks of 8. Timestamps
	// overlap with Group A so the cross-group merge is exercised.
	if err := w.OpenTopics(
		[]string{"/groupB/x", "/groupB/y"},
		[]map[string]any{
			{"group": "B", "channel": "x"},
			{"group": "B", "channel": "y"},
		},
		turbodata.WithCompression(),
		turbodata.WithChunkConfig(&turbodata.ChunkConfig{
			Mode:  turbodata.ChunkThresholdModeCount,
			Count: 8,
		}),
	); err != nil {
		return err
	}
	bTopics := []string{"/groupB/x", "/groupB/y"}
	for i := 0; i < 24; i++ {
		ts := int64(4_000_100 + i*200)
		topic := bTopics[i%2]
		if err := w.WriteMessage(topic,
			[]byte(fmt.Sprintf("B-%s-%03d-%s", topic, i, mkPayload(i))), ts); err != nil {
			return err
		}
	}
	return w.CloseTopic()
}

func mkPayload(seed int) string {
	b := make([]byte, 48)
	for i := range b {
		b[i] = byte('a' + ((seed + i) % 26))
	}
	return string(b)
}

// --------- golden file ---------

func writeGolden(fixturePath string) error {
	data, err := os.ReadFile(fixturePath)
	if err != nil {
		return err
	}

	rs := newByteReadSource(data)
	r, err := turbodata.NewReader(rs)
	if err != nil {
		return err
	}
	summary, err := r.Summary()
	if err != nil {
		return err
	}

	type topicEntry struct {
		Id       uint16         `json:"id"`
		Name     string         `json:"name"`
		Metadata map[string]any `json:"metadata"`
	}
	topics := make([]topicEntry, 0)
	for _, ti := range summary.TopicsInfos {
		for _, tm := range ti.TopicMetadatas {
			// Drop *ChunkConfig pointer values that aren't JSON-serializable
			// and aren't part of the on-disk msgpack metadata anyway —
			// `chunk_config` is consumed by the writer and never persisted
			// to disk. We re-derive the on-disk metadata by round-tripping
			// through msgpack-friendly types only.
			topics = append(topics, topicEntry{
				Id:       tm.Id,
				Name:     tm.Name,
				Metadata: jsonSafeMetadata(tm.Metadata),
			})
		}
	}

	it, err := r.ReadMessages()
	if err != nil {
		return err
	}

	hasher := sha256.New()
	buf := turbodata.NewReusableBuffer()
	count := 0
	var firstLine, lastLine string
	for {
		ts, name, err := it.NextInto(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		var tsBytes [8]byte
		binary.BigEndian.PutUint64(tsBytes[:], uint64(ts))
		hasher.Write(tsBytes[:])
		hasher.Write([]byte(name))
		hasher.Write([]byte{0})
		hasher.Write(buf.Data)
		hasher.Write([]byte{0})

		dataHash := sha256.Sum256(buf.Data)
		line := fmt.Sprintf("%d %s %s", ts, name, hex.EncodeToString(dataHash[:]))
		if count == 0 {
			firstLine = line
		}
		lastLine = line
		count++
	}

	// Stable topic order in golden output by id.
	sort.Slice(topics, func(i, j int) bool { return topics[i].Id < topics[j].Id })

	goldenPath := fixturePath + ".golden.txt"
	gf, err := os.Create(goldenPath)
	if err != nil {
		return err
	}
	defer gf.Close()
	fmt.Fprintf(gf, "topics:%d\n", len(topics))
	for _, t := range topics {
		md, err := json.Marshal(t.Metadata)
		if err != nil {
			return err
		}
		fmt.Fprintf(gf, "topic %d %s %s\n", t.Id, t.Name, string(md))
	}
	fmt.Fprintf(gf, "messages:%d\n", count)
	fmt.Fprintf(gf, "stream-sha256:%s\n", hex.EncodeToString(hasher.Sum(nil)))
	fmt.Fprintf(gf, "first %s\n", firstLine)
	fmt.Fprintf(gf, "last %s\n", lastLine)
	return nil
}

// jsonSafeMetadata strips values that don't survive a JSON round-trip
// (e.g. *ChunkConfig pointers added at write time). The on-disk msgpack
// metadata only contains the user-supplied keys plus `is_compressed`, and
// we want golden files to be diffable.
func jsonSafeMetadata(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		switch vv := v.(type) {
		case bool, string, int64, uint64, float64, int, int32, uint32:
			out[k] = vv
		default:
			// Skip unrepresentable values (e.g. *ChunkConfig).
			_ = vv
		}
	}
	return out
}

// --------- byteReadSource: in-memory ReadSource for golden generation ---------

type byteReadSource struct {
	data []byte
	pos  int64
}

func newByteReadSource(data []byte) *byteReadSource {
	return &byteReadSource{data: data}
}

func (b *byteReadSource) Read(p []byte) (int, error) {
	if b.pos >= int64(len(b.data)) {
		return 0, io.EOF
	}
	n := copy(p, b.data[b.pos:])
	b.pos += int64(n)
	return n, nil
}

func (b *byteReadSource) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(b.data)) {
		return 0, io.EOF
	}
	n := copy(p, b.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (b *byteReadSource) Seek(offset int64, whence int) (int64, error) {
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = b.pos + offset
	case io.SeekEnd:
		abs = int64(len(b.data)) + offset
	default:
		return 0, fmt.Errorf("invalid whence %d", whence)
	}
	if abs < 0 {
		return 0, fmt.Errorf("negative position")
	}
	b.pos = abs
	return abs, nil
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
