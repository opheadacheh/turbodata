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

package benchmark

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/opheadacheh/turbodata/go/turbodata"
	"github.com/opheadacheh/turbodata/go/turbodata/format"

	"github.com/foxglove/mcap/go/mcap"
)

// MCAPInfo is a thin wrapper around what we discover from the source MCAP
// once at TestMain time. All scenario derivation reads from this, never from
// the file directly.
type MCAPInfo struct {
	Path string

	// Topics, partitioned.
	ImageTopics    []string // schema name == "foxglove.RawImage"
	NonImageTopics []string

	// Time bounds (ns).
	StartTimestamp int64
	EndTimestamp   int64

	// Per-channel info, indexed by topic name. Schema is the channel's
	// schema struct (needed to write the .td fixture).
	Channels map[string]ChannelInfo
}

type ChannelInfo struct {
	Schema *mcap.Schema
}

// loadMCAPInfo opens the MCAP, reads its info, and partitions topics.
func loadMCAPInfo(path string) (*MCAPInfo, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open mcap: %w", err)
	}
	defer f.Close()

	r, err := mcap.NewReader(f)
	if err != nil {
		return nil, fmt.Errorf("new mcap reader: %w", err)
	}
	defer r.Close()

	info, err := r.Info()
	if err != nil {
		return nil, fmt.Errorf("mcap info: %w", err)
	}

	mi := &MCAPInfo{
		Path:     path,
		Channels: make(map[string]ChannelInfo, len(info.Channels)),
	}
	for _, ch := range info.Channels {
		schema := info.Schemas[ch.SchemaID]
		mi.Channels[ch.Topic] = ChannelInfo{Schema: schema}
		if schema != nil && schema.Name == "foxglove.RawImage" {
			mi.ImageTopics = append(mi.ImageTopics, ch.Topic)
		} else {
			mi.NonImageTopics = append(mi.NonImageTopics, ch.Topic)
		}
	}
	if info.Statistics != nil {
		mi.StartTimestamp = int64(info.Statistics.MessageStartTime)
		mi.EndTimestamp = int64(info.Statistics.MessageEndTime)
	}
	return mi, nil
}

// CanonicalChunkSize is the fixed chunk size used for objectives 1 & 2.
// 1MB matches MCAP's and turbodata's default chunk size.
const CanonicalChunkSize = 1 << 20

// canonicalConvertMcapToTd produces the .td fixture used by objectives 1 & 2:
//   - All topics in a single topic group
//   - 1MB size-based chunks
//   - All compressed
//
// This is the simplest mapping from MCAP and exists primarily to verify the
// turbodata write/read paths haven't regressed.
func canonicalConvertMcapToTd(mi *MCAPInfo, dstPath string) error {
	in, err := os.Open(mi.Path)
	if err != nil {
		return err
	}
	defer in.Close()

	r, err := mcap.NewReader(in)
	if err != nil {
		return err
	}
	defer r.Close()

	out, err := os.Create(dstPath)
	if err != nil {
		return err
	}
	defer out.Close()

	w := turbodata.NewWriter(out)

	allTopics := make([]string, 0, len(mi.Channels))
	allTopics = append(allTopics, mi.ImageTopics...)
	allTopics = append(allTopics, mi.NonImageTopics...)

	metadatas := make([]map[string]any, len(allTopics))
	for i, t := range allTopics {
		s := mi.Channels[t].Schema
		metadatas[i] = schemaMeta(s)
	}

	cfg := &turbodata.ChunkConfig{Mode: turbodata.ChunkThresholdModeSize, Size: CanonicalChunkSize}
	if err := w.OpenTopics(allTopics, metadatas,
		turbodata.WithCompression(),
		turbodata.WithChunkConfig(cfg),
	); err != nil {
		return err
	}

	it, err := r.Messages(mcap.InOrder(mcap.LogTimeOrder))
	if err != nil {
		return err
	}
	msg := &mcap.Message{}
	for {
		_, ch, _, err := it.NextInto(msg)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if err := w.WriteMessage(ch.Topic, msg.Data, int64(msg.LogTime)); err != nil {
			return err
		}
	}
	if err := w.CloseTopic(); err != nil {
		return err
	}
	return w.Close()
}

func schemaMeta(s *mcap.Schema) map[string]any {
	if s == nil {
		return map[string]any{}
	}
	return map[string]any{
		format.MetaKeySchemaEncoding: s.Encoding,
		format.MetaKeySchemaData:     s.Data,
		format.MetaKeySchemaName:     s.Name,
	}
}

// ensureCanonicalTd builds (if missing) and returns the path of the
// canonical .td fixture next to the source MCAP. Reusing across runs is the
// normal case; rebuilding only happens when the file is absent.
func ensureCanonicalTd(mi *MCAPInfo) (string, error) {
	dst := mi.Path + ".canonical_1m_compressed.td"
	if fi, err := os.Stat(dst); err == nil {
		fmt.Printf("td/canonical: reusing %s (%d bytes)\n", dst, fi.Size())
		return dst, nil
	}
	fmt.Printf("td/canonical: building %s\n", dst)
	if err := canonicalConvertMcapToTd(mi, dst); err != nil {
		return "", fmt.Errorf("build canonical td: %w", err)
	}
	fi, _ := os.Stat(dst)
	fmt.Printf("td/canonical: built %s (%d bytes)\n", dst, fi.Size())
	return dst, nil
}

// canonicalConvertMcapToMcap rewrites the source MCAP into a fresh MCAP with
// the same chunk size and compression as the canonical TD fixture. This is
// the apples-to-apples baseline for BenchmarkRead: when comparing TD vs
// MCAP, both should be encoded by *the same pipeline* in *this run*, so
// neither benefits from accidental layout differences in the source file
// (older writer versions, different chunk sizes, etc.).
//
// We stream messages through mcap-reader → mcap-writer rather than copying
// raw chunks, because the source's chunking is exactly what we want to
// normalize away.
func canonicalConvertMcapToMcap(mi *MCAPInfo, dstPath string) error {
	in, err := os.Open(mi.Path)
	if err != nil {
		return err
	}
	defer in.Close()

	r, err := mcap.NewReader(in)
	if err != nil {
		return err
	}
	defer r.Close()

	info, err := r.Info()
	if err != nil {
		return err
	}

	out, err := os.Create(dstPath)
	if err != nil {
		return err
	}
	defer out.Close()

	w, err := mcap.NewWriter(out, &mcap.WriterOptions{
		Chunked:     true,
		ChunkSize:   CanonicalChunkSize,
		Compression: mcap.CompressionZSTD,
		IncludeCRC:  true,
	})
	if err != nil {
		return err
	}
	if err := w.WriteHeader(&mcap.Header{
		Profile: info.Header.Profile,
		Library: "turbodata-bench-canonical",
	}); err != nil {
		return err
	}
	for _, s := range info.Schemas {
		if err := w.WriteSchema(s); err != nil {
			return err
		}
	}
	for _, c := range info.Channels {
		if err := w.WriteChannel(c); err != nil {
			return err
		}
	}

	it, err := r.Messages(mcap.InOrder(mcap.LogTimeOrder))
	if err != nil {
		return err
	}
	msg := &mcap.Message{}
	for {
		_, _, _, err := it.NextInto(msg)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if err := w.WriteMessage(msg); err != nil {
			return err
		}
	}
	return w.Close()
}

// ensureCanonicalMcap builds (if missing) and returns the path of the
// canonical re-encoded MCAP fixture next to the source.
func ensureCanonicalMcap(mi *MCAPInfo) (string, error) {
	dst := mi.Path + ".canonical_1m_compressed.mcap"
	if fi, err := os.Stat(dst); err == nil {
		fmt.Printf("mcap/canonical: reusing %s (%d bytes)\n", dst, fi.Size())
		return dst, nil
	}
	fmt.Printf("mcap/canonical: building %s\n", dst)
	if err := canonicalConvertMcapToMcap(mi, dst); err != nil {
		return "", fmt.Errorf("build canonical mcap: %w", err)
	}
	fi, _ := os.Stat(dst)
	fmt.Printf("mcap/canonical: built %s (%d bytes)\n", dst, fi.Size())
	return dst, nil
}

// Improved-fixture chunk sizes. Image side stays at 1 MiB so the only
// changes vs canonical are grouping and compression; non-image side drops
// to 64 KiB because the entire non-image group totals ~1.6 MiB — a 64 KiB
// chunk produces ~25 chunks, fine-grained enough that filtering reads
// for one topic still touches a small number of chunks.
const (
	ImprovedImageChunkSize    = 1 << 20  // 1 MiB
	ImprovedNonImageChunkSize = 64 << 10 // 64 KiB
)

// improvedConvertMcapToTd produces the .td fixture used by objectives 3 & 4.
//
// Layout:
//   - Each image topic is its own group. Image groups are uncompressed
//     (real-world image data is already compressed before logging) and use
//     1 MiB chunks. Uncompressed chunks let the cost-aware reader fetch
//     individual messages by byte range (Obj 4).
//   - All non-image topics are written into a single group with 64 KiB
//     chunks, compressed. Co-locating small topics makes "selected topics
//     in time range" reads pull adjacent messages in the same chunk.
//
// We stream the source MCAP once per image topic (cheap; image topics are
// small individually) and once for the combined non-image group.
func improvedConvertMcapToTd(mi *MCAPInfo, dstPath string) error {
	in, err := os.Open(mi.Path)
	if err != nil {
		return err
	}
	defer in.Close()

	r, err := mcap.NewReader(in)
	if err != nil {
		return err
	}
	defer r.Close()

	out, err := os.Create(dstPath)
	if err != nil {
		return err
	}
	defer out.Close()

	w := turbodata.NewWriter(out)

	imgCfg := &turbodata.ChunkConfig{Mode: turbodata.ChunkThresholdModeSize, Size: ImprovedImageChunkSize}
	nonImgCfg := &turbodata.ChunkConfig{Mode: turbodata.ChunkThresholdModeSize, Size: ImprovedNonImageChunkSize}

	// Per-image-topic groups, uncompressed.
	for _, topic := range mi.ImageTopics {
		meta := schemaMeta(mi.Channels[topic].Schema)
		if err := w.OpenTopics(
			[]string{topic},
			[]map[string]any{meta},
			turbodata.WithChunkConfig(imgCfg),
		); err != nil {
			return err
		}
		if err := streamTopicsInto(r, w, []string{topic}); err != nil {
			return err
		}
		if err := w.CloseTopic(); err != nil {
			return err
		}
	}

	// All non-image topics in one group, compressed, small chunks.
	if len(mi.NonImageTopics) > 0 {
		topics := mi.NonImageTopics
		metadatas := make([]map[string]any, len(topics))
		for i, t := range topics {
			metadatas[i] = schemaMeta(mi.Channels[t].Schema)
		}
		if err := w.OpenTopics(
			topics,
			metadatas,
			turbodata.WithCompression(),
			turbodata.WithChunkConfig(nonImgCfg),
		); err != nil {
			return err
		}
		if err := streamTopicsInto(r, w, topics); err != nil {
			return err
		}
		if err := w.CloseTopic(); err != nil {
			return err
		}
	}
	return w.Close()
}

// streamTopicsInto reads all messages for the given topics from r in
// log-time order and writes them into w. Used by improved-fixture builders
// that want one MCAP→TD pass per group.
func streamTopicsInto(r *mcap.Reader, w *turbodata.Writer, topics []string) error {
	it, err := r.Messages(mcap.InOrder(mcap.LogTimeOrder), mcap.WithTopics(topics))
	if err != nil {
		return err
	}
	msg := &mcap.Message{}
	for {
		_, ch, _, err := it.NextInto(msg)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := w.WriteMessage(ch.Topic, msg.Data, int64(msg.LogTime)); err != nil {
			return err
		}
	}
}

// ensureImprovedTd builds (if missing) and returns the path of the
// improved .td fixture used by objectives 3 and 4.
func ensureImprovedTd(mi *MCAPInfo) (string, error) {
	dst := mi.Path + ".improved.td"
	if fi, err := os.Stat(dst); err == nil {
		fmt.Printf("td/improved: reusing %s (%d bytes)\n", dst, fi.Size())
		return dst, nil
	}
	fmt.Printf("td/improved: building %s\n", dst)
	if err := improvedConvertMcapToTd(mi, dst); err != nil {
		return "", fmt.Errorf("build improved td: %w", err)
	}
	fi, _ := os.Stat(dst)
	fmt.Printf("td/improved: built %s (%d bytes)\n", dst, fi.Size())
	return dst, nil
}
