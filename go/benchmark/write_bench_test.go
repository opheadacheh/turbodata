package benchmark

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"

	"turbodata"

	"github.com/foxglove/mcap/go/mcap"
)

// preloadedMessages holds every message from the source MCAP in memory, in
// log-time order. Both TD and MCAP write benchmarks consume this exact slice
// so the comparison is apples-to-apples (no read overhead, identical input).
//
// Memory cost: ~equal to the source MCAP's uncompressed payload size. For
// the 717MB test.mcap this is several GB after expansion. If this is a
// problem on a given machine, trim by N messages or skip Obj 1.
type preloadedMessages struct {
	// Channels keyed by topic. Each channel keeps its schema for writing.
	topics  []string
	schemas map[string]*mcap.Schema
	// Per-message: topic name, payload, log-time.
	msgs []preloadedMsg
}

type preloadedMsg struct {
	topic     string
	data      []byte
	timestamp int64
}

var (
	preloadedOnce sync.Once
	preloadedData *preloadedMessages
	preloadedErr  error
)

// getPreloadedMessages loads every message from mcapInfo.Path into memory on
// first call and returns the cached result on subsequent calls.
func getPreloadedMessages() (*preloadedMessages, error) {
	preloadedOnce.Do(func() {
		preloadedData, preloadedErr = doPreload(mcapInfo)
	})
	return preloadedData, preloadedErr
}

func doPreload(mi *MCAPInfo) (*preloadedMessages, error) {
	f, err := os.Open(mi.Path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r, err := mcap.NewReader(f)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	it, err := r.Messages(mcap.InOrder(mcap.LogTimeOrder))
	if err != nil {
		return nil, err
	}

	pm := &preloadedMessages{
		schemas: make(map[string]*mcap.Schema, len(mi.Channels)),
	}
	for t, ci := range mi.Channels {
		pm.schemas[t] = ci.Schema
	}
	pm.topics = append(pm.topics, mi.ImageTopics...)
	pm.topics = append(pm.topics, mi.NonImageTopics...)

	msg := &mcap.Message{}
	for {
		_, ch, _, err := it.NextInto(msg)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		data := make([]byte, len(msg.Data))
		copy(data, msg.Data)
		pm.msgs = append(pm.msgs, preloadedMsg{
			topic:     ch.Topic,
			data:      data,
			timestamp: int64(msg.LogTime),
		})
	}
	fmt.Printf("preloaded: %d messages from %s\n", len(pm.msgs), mi.Path)
	return pm, nil
}

// BenchmarkWrite measures write throughput of TD vs MCAP on the same in-memory
// message stream. Both writers target io.Discard wrapped in TrackingWriter, so
// the comparison isolates writer CPU and allocation cost from disk speed.
//
// Sub-benchmarks: BenchmarkWrite/{td,mcap}.
func BenchmarkWrite(b *testing.B) {
	if mcapInfo == nil {
		b.Skip("no -mcap flag provided")
	}
	pm, err := getPreloadedMessages()
	if err != nil {
		b.Fatalf("preload: %v", err)
	}

	b.Run("td", func(b *testing.B) {
		runWriteBench(b, func(tw *TrackingWriter) error {
			return writeAllToTd(tw, pm)
		})
	})
	b.Run("mcap", func(b *testing.B) {
		runWriteBench(b, func(tw *TrackingWriter) error {
			return writeAllToMcap(tw, pm)
		})
	})
}

// runWriteBench is the shared loop: reset tracker, run body, collect IO+heap.
func runWriteBench(b *testing.B, body func(tw *TrackingWriter) error) {
	b.Helper()
	tw := NewTrackingWriter(io.Discard)

	var hs *HeapSampler
	if HeapEnabled() {
		hs = StartHeapSampler()
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Tracker accumulates across iterations on purpose: reportWriteIO
		// divides by b.N. Resetting each iteration would yield numbers off
		// by a factor of N.
		if err := body(tw); err != nil {
			b.Fatal(err)
		}
		if hs != nil {
			b.StopTimer()
			hs.Sample()
			b.StartTimer()
		}
	}
	b.StopTimer()
	reportWriteIO(b, tw)
	if hs != nil {
		hs.Stop()
		hs.Report(b)
	}
}

func writeAllToTd(tw *TrackingWriter, pm *preloadedMessages) error {
	w := turbodata.NewWriter(tw)
	cfg := &turbodata.ChunkConfig{Mode: turbodata.ChunkThresholdModeSize, Size: CanonicalChunkSize}

	metadatas := make([]map[string]any, len(pm.topics))
	for i, t := range pm.topics {
		metadatas[i] = schemaMeta(pm.schemas[t])
	}
	if err := w.OpenTopics(pm.topics, metadatas,
		turbodata.WithCompression(),
		turbodata.WithChunkConfig(cfg),
	); err != nil {
		return err
	}
	for i := range pm.msgs {
		m := &pm.msgs[i]
		if err := w.WriteMessage(m.topic, m.data, m.timestamp); err != nil {
			return err
		}
	}
	if err := w.CloseTopic(); err != nil {
		return err
	}
	return w.Close()
}

func writeAllToMcap(tw *TrackingWriter, pm *preloadedMessages) error {
	w, err := mcap.NewWriter(tw, &mcap.WriterOptions{
		Chunked:     true,
		ChunkSize:   CanonicalChunkSize,
		Compression: mcap.CompressionZSTD,
		IncludeCRC:  true,
	})
	if err != nil {
		return err
	}
	if err := w.WriteHeader(&mcap.Header{
		Profile: "",
		Library: "turbodata-bench",
	}); err != nil {
		return err
	}
	// Assign IDs: schemas by name, channels by topic.
	schemaIDs := make(map[string]uint16)
	var nextSchemaID uint16 = 1
	for _, t := range pm.topics {
		s := pm.schemas[t]
		if s == nil {
			continue
		}
		key := s.Name + "|" + s.Encoding
		if _, ok := schemaIDs[key]; ok {
			continue
		}
		id := nextSchemaID
		nextSchemaID++
		schemaIDs[key] = id
		if err := w.WriteSchema(&mcap.Schema{
			ID:       id,
			Name:     s.Name,
			Encoding: s.Encoding,
			Data:     s.Data,
		}); err != nil {
			return err
		}
	}
	channelIDs := make(map[string]uint16)
	var nextChannelID uint16 = 0
	for _, t := range pm.topics {
		s := pm.schemas[t]
		var sid uint16
		if s != nil {
			sid = schemaIDs[s.Name+"|"+s.Encoding]
		}
		id := nextChannelID
		nextChannelID++
		channelIDs[t] = id
		if err := w.WriteChannel(&mcap.Channel{
			ID:              id,
			SchemaID:        sid,
			Topic:           t,
			MessageEncoding: "msgpack", // best-effort; MCAP stores it but doesn't act on it
		}); err != nil {
			return err
		}
	}
	for i := range pm.msgs {
		m := &pm.msgs[i]
		if err := w.WriteMessage(&mcap.Message{
			ChannelID: channelIDs[m.topic],
			LogTime:   uint64(m.timestamp),
			Data:      m.data,
		}); err != nil {
			return err
		}
	}
	return w.Close()
}
