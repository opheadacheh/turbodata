// Example: convert an MCAP file to a turbodata file with customized topic
// grouping and chunk configs.
//
// Run:
//
//	cd go
//	go run ./examples/mcap_to_td <input.mcap> <output.td>
//
// Grouping policy in this example (tune via the constants below):
//
//	1. Image topics (schema "foxglove.RawImage") each become their own
//	   single-topic group. Images are large and best read independently, so
//	   the cost-aware reader can fetch one image without dragging in
//	   neighbors.
//
//	2. Every other topic is grouped with all other topics that share its
//	   schema. Same-schema topics tend to have similar message sizes and
//	   are usually consumed together, so shared chunks save metadata
//	   overhead.
//
// Per-group settings (compression, chunk config) are picked from the
// constants below. Edit them to match your data:
//
//	imageChunk     — applies to each image topic's group
//	nonImageChunk  — applies to every shared-schema group
//	compressImages / compressNonImages — toggle zstd compression per group
//
// You can also override the image schema name, or change the grouping rule
// itself in groupChannels.
package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"turbodata"

	"github.com/foxglove/mcap/go/mcap"
)

// ----- tunables -----

const imageSchemaName = "foxglove.RawImage"

var (
	imageChunk = &turbodata.ChunkConfig{
		Mode: turbodata.ChunkThresholdModeSize,
		Size: 4 * 1024 * 1024,
	}
	nonImageChunk = &turbodata.ChunkConfig{
		Mode: turbodata.ChunkThresholdModeSize,
		Size: 1 * 1024 * 1024,
	}

	compressImages    = true
	compressNonImages = true
)

// --------------------

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintf(os.Stderr, "usage: mcap_to_td <input.mcap> <output.td>\n")
		os.Exit(1)
	}
	mcapPath, tdPath := os.Args[1], os.Args[2]

	mcapFile, err := os.Open(mcapPath)
	if err != nil {
		log.Fatalf("open mcap: %v", err)
	}
	defer mcapFile.Close()

	mcapReader, err := mcap.NewReader(mcapFile)
	if err != nil {
		log.Fatalf("new mcap reader: %v", err)
	}
	defer mcapReader.Close()

	info, err := mcapReader.Info()
	if err != nil {
		log.Fatalf("mcap info: %v", err)
	}

	tdFile, err := os.Create(tdPath)
	if err != nil {
		log.Fatalf("create td: %v", err)
	}
	defer tdFile.Close()

	tdWriter := turbodata.NewWriter(tdFile)

	imageGroups, sharedGroups := groupChannels(info)

	for _, g := range imageGroups {
		if err := writeGroup(tdWriter, mcapReader, g, imageChunk, compressImages); err != nil {
			log.Fatalf("write image group %v: %v", g.topics, err)
		}
	}
	for _, g := range sharedGroups {
		if err := writeGroup(tdWriter, mcapReader, g, nonImageChunk, compressNonImages); err != nil {
			log.Fatalf("write shared-schema group %v: %v", g.topics, err)
		}
	}

	if err := tdWriter.Close(); err != nil {
		log.Fatalf("close td writer: %v", err)
	}
	log.Printf("converted %s -> %s", mcapPath, tdPath)
}

// group is one set of topics that will be written as a single turbodata
// topic group. Topics within a group share storage (chunks, index chunks)
// and any group-level options (compression, chunk config).
type group struct {
	topics []string
	schema *mcap.Schema
}

// groupChannels applies the grouping rule:
//   - Each image topic is its own single-topic group.
//   - Non-image topics are bucketed by schema id; one group per bucket.
//
// Returns (imageGroups, sharedSchemaGroups). To change the rule (e.g.
// per-channel grouping, or splitting a large bucket further), edit this
// function in one place — the rest of the pipeline is unchanged.
func groupChannels(info *mcap.Info) ([]group, []group) {
	schemasByID := make(map[uint16]*mcap.Schema, len(info.Schemas))
	for _, s := range info.Schemas {
		schemasByID[s.ID] = s
	}

	var images []group
	bySchemaID := make(map[uint16]*group)
	var sharedOrder []uint16 // preserve first-seen order for determinism

	for _, ch := range info.Channels {
		schema := schemasByID[ch.SchemaID]
		if schema == nil {
			continue
		}
		if schema.Name == imageSchemaName {
			images = append(images, group{topics: []string{ch.Topic}, schema: schema})
			continue
		}
		g, ok := bySchemaID[ch.SchemaID]
		if !ok {
			bySchemaID[ch.SchemaID] = &group{schema: schema}
			g = bySchemaID[ch.SchemaID]
			sharedOrder = append(sharedOrder, ch.SchemaID)
		}
		g.topics = append(g.topics, ch.Topic)
	}

	shared := make([]group, 0, len(bySchemaID))
	for _, id := range sharedOrder {
		shared = append(shared, *bySchemaID[id])
	}
	return images, shared
}

// writeGroup opens one turbodata topic group, streams every matching MCAP
// message into it in log-time order, and closes it. The same metadata is
// attached to every topic in the group so the reader can recover the schema
// from any topic.
func writeGroup(
	w *turbodata.Writer,
	mr *mcap.Reader,
	g group,
	chunkCfg *turbodata.ChunkConfig,
	compressed bool,
) error {
	metadatas := make([]map[string]any, len(g.topics))
	for i := range metadatas {
		metadatas[i] = map[string]any{
			"schema_name":     g.schema.Name,
			"schema_encoding": g.schema.Encoding,
			"schema_data":     g.schema.Data,
		}
	}

	opts := []turbodata.WriteOption{turbodata.WithChunkConfig(chunkCfg)}
	if compressed {
		opts = append(opts, turbodata.WithCompression())
	}

	if err := w.OpenTopics(g.topics, metadatas, opts...); err != nil {
		return fmt.Errorf("open topics: %w", err)
	}

	it, err := mr.Messages(mcap.InOrder(mcap.LogTimeOrder), mcap.WithTopics(g.topics))
	if err != nil {
		return fmt.Errorf("mcap iterator: %w", err)
	}

	msg := &mcap.Message{}
	for {
		_, channel, _, err := it.NextInto(msg)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("mcap next: %w", err)
		}
		if err := w.WriteMessage(channel.Topic, msg.Data, int64(msg.LogTime)); err != nil {
			return fmt.Errorf("write %s: %w", channel.Topic, err)
		}
	}

	return w.CloseTopic()
}
