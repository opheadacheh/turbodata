package benchmark

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"testing"

	"turbodata"

	"github.com/foxglove/mcap/go/mcap"
)

// ConfigPair holds a pair of chunk configs for the two topic groupings used in conversion.
// ImgConfig applies to image topics (one topic per group).
// NonImgConfig applies to non-image topics (grouped by schema).
type ConfigPair struct {
	Label        string
	ImgConfig    *turbodata.ChunkConfig
	NonImgConfig *turbodata.ChunkConfig
}

// topicGroup is a pre-loaded OpenTopics group for the write benchmark.
type topicGroup struct {
	names     []string
	metadatas []map[string]any
	messages  []benchMsg
}

// benchMsg is a single pre-loaded message.
type benchMsg struct {
	topicName string
	data      []byte
	timestamp int64
}

var mcapFlag = flag.String("mcap", "", "path to input MCAP file (required to run benchmarks)")

// Package-level state populated by TestMain.
var (
	tdFilePaths map[string]string // config pair label → path to converted .td file
	configPairs []ConfigPair

	// Scenario parameters derived from the MCAP file.
	imgTopicName    string   // a representative image topic
	nonImgTopicName string   // a representative non-image topic
	timeRangeTopics []string // topics used in the time-range scenario (up to 5)
	midStartTs      int64    // start of middle-third timestamp window
	midEndTs        int64    // end of middle-third timestamp window

	// Pre-loaded groups for write benchmarks.
	imgGroups    []topicGroup
	nonImgGroups []topicGroup
)

func TestMain(m *testing.M) {
	flag.Parse()

	configPairs = []ConfigPair{
		{
			Label:        "img128k_nonimg512k",
			ImgConfig:    &turbodata.ChunkConfig{Mode: turbodata.ChunkThresholdModeSize, Size: 128 << 10},
			NonImgConfig: &turbodata.ChunkConfig{Mode: turbodata.ChunkThresholdModeSize, Size: 512 << 10},
		},
		{
			Label:        "img1m_nonimg1m",
			ImgConfig:    &turbodata.ChunkConfig{Mode: turbodata.ChunkThresholdModeSize, Size: 1 << 20},
			NonImgConfig: &turbodata.ChunkConfig{Mode: turbodata.ChunkThresholdModeSize, Size: 1 << 20},
		},
		{
			Label:        "img4m_nonimg4m",
			ImgConfig:    &turbodata.ChunkConfig{Mode: turbodata.ChunkThresholdModeSize, Size: 4 << 20},
			NonImgConfig: &turbodata.ChunkConfig{Mode: turbodata.ChunkThresholdModeSize, Size: 4 << 20},
		},
		{
			Label:        "img16m_nonimg16m",
			ImgConfig:    &turbodata.ChunkConfig{Mode: turbodata.ChunkThresholdModeSize, Size: 16 << 20},
			NonImgConfig: &turbodata.ChunkConfig{Mode: turbodata.ChunkThresholdModeSize, Size: 16 << 20},
		},
	}

	if *mcapFlag == "" {
		fmt.Fprintln(os.Stderr, "no -mcap flag provided; benchmarks will be skipped")
		os.Exit(m.Run())
	}

	if err := setup(*mcapFlag); err != nil {
		log.Fatalf("benchmark setup: %v", err)
	}
	os.Exit(m.Run())
}

func setup(mcapPath string) error {
	f, err := os.Open(mcapPath)
	if err != nil {
		return fmt.Errorf("open mcap: %w", err)
	}
	defer f.Close()

	r, err := mcap.NewReader(f)
	if err != nil {
		return fmt.Errorf("new mcap reader: %w", err)
	}
	defer r.Close()

	info, err := r.Info()
	if err != nil {
		return fmt.Errorf("mcap info: %w", err)
	}

	if err := deriveScenarioParams(info); err != nil {
		return err
	}

	tdFilePaths = make(map[string]string)
	allExist := true
	for _, pair := range configPairs {
		path := mcapPath + "." + pair.Label + ".td"
		if _, err := os.Stat(path); os.IsNotExist(err) {
			allExist = false
			break
		}
		tdFilePaths[pair.Label] = path
	}

	if allExist {
		for _, pair := range configPairs {
			fi, _ := os.Stat(tdFilePaths[pair.Label])
			fmt.Printf("td/%s: reusing existing file %s (%d bytes)\n", pair.Label, tdFilePaths[pair.Label], fi.Size())
		}
		return nil
	}

	if err := preloadMessages(mcapPath, info); err != nil {
		return err
	}

	for _, pair := range configPairs {
		path := mcapPath + "." + pair.Label + ".td"
		if err := convertMcapToTd(mcapPath, path, pair); err != nil {
			return fmt.Errorf("convert %s: %w", pair.Label, err)
		}
		fi, _ := os.Stat(path)
		fmt.Printf("td/%s: %s (%d bytes)\n", pair.Label, path, fi.Size())
		tdFilePaths[pair.Label] = path
	}
	return nil
}

// deriveScenarioParams picks representative topics and computes the middle-third time window.
func deriveScenarioParams(info *mcap.Info) error {
	schemaByID := info.Schemas

	var imgTopics, nonImgTopics []string
	for _, ch := range info.Channels {
		schema := schemaByID[ch.SchemaID]
		if schema.Name == "foxglove.RawImage" {
			imgTopics = append(imgTopics, ch.Topic)
		} else {
			nonImgTopics = append(nonImgTopics, ch.Topic)
		}
	}

	if len(imgTopics) > 0 {
		imgTopicName = imgTopics[0]
	} else if len(nonImgTopics) > 0 {
		imgTopicName = nonImgTopics[0]
	}

	if len(nonImgTopics) > 0 {
		nonImgTopicName = nonImgTopics[0]
	} else if len(imgTopics) > 0 {
		nonImgTopicName = imgTopics[0]
	}

	// Time range: middle third of the file's time span.
	if info.Statistics != nil && info.Statistics.MessageEndTime > info.Statistics.MessageStartTime {
		span := info.Statistics.MessageEndTime - info.Statistics.MessageStartTime
		midStartTs = int64(info.Statistics.MessageStartTime + span/3)
		midEndTs = midStartTs + 1000000000 // 1 second
	}

	timeRangeTopics = []string{
		"/cameras/wrist/left/image",
		"/motor_toques/torque1",
		"/motor_toques/torque2",
		"/motor_toques/torque3",
		"/motor_toques/torque4",
		"/motor_toques/torque5",
		"/motor_toques/torque6",
		"/motor_toques/torque7",
	}

	fmt.Printf("imgTopic=%q  nonImgTopic=%q  timeRangeTopics=%v\n", imgTopicName, nonImgTopicName, timeRangeTopics)
	fmt.Printf("midStartTs=%d  midEndTs=%d\n", midStartTs, midEndTs)
	return nil
}

// preloadMessages loads all messages into memory for the write benchmark.
func preloadMessages(mcapPath string, info *mcap.Info) error {
	schemaByID := info.Schemas

	// Image channels: one group per channel.
	for _, ch := range info.Channels {
		schema := schemaByID[ch.SchemaID]
		if schema.Name != "foxglove.RawImage" {
			continue
		}
		meta := map[string]any{
			"schema_encoding": schema.Encoding,
			"schema_data":     schema.Data,
			"schema_name":     schema.Name,
		}
		msgs, err := loadMsgsFromMcap(mcapPath, []string{ch.Topic})
		if err != nil {
			return fmt.Errorf("load img topic %s: %w", ch.Topic, err)
		}
		imgGroups = append(imgGroups, topicGroup{
			names:     []string{ch.Topic},
			metadatas: []map[string]any{meta},
			messages:  msgs,
		})
	}

	// Non-image channels: grouped by schema ID.
	schemaIdToTopics := make(map[uint16][]string)
	for _, ch := range info.Channels {
		if schemaByID[ch.SchemaID].Name == "foxglove.RawImage" {
			continue
		}
		schemaIdToTopics[ch.SchemaID] = append(schemaIdToTopics[ch.SchemaID], ch.Topic)
	}
	for schemaID, topics := range schemaIdToTopics {
		schema := schemaByID[schemaID]
		metadatas := make([]map[string]any, len(topics))
		for i := range topics {
			metadatas[i] = map[string]any{
				"schema_encoding": schema.Encoding,
				"schema_data":     schema.Data,
				"schema_name":     schema.Name,
			}
		}
		msgs, err := loadMsgsFromMcap(mcapPath, topics)
		if err != nil {
			return fmt.Errorf("load non-img topics %v: %w", topics, err)
		}
		nonImgGroups = append(nonImgGroups, topicGroup{
			names:     topics,
			metadatas: metadatas,
			messages:  msgs,
		})
	}
	return nil
}

// loadMsgsFromMcap reads all messages for the given topics into memory.
func loadMsgsFromMcap(mcapPath string, topics []string) ([]benchMsg, error) {
	f, err := os.Open(mcapPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r, err := mcap.NewReader(f)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	it, err := r.Messages(mcap.InOrder(mcap.LogTimeOrder), mcap.WithTopics(topics))
	if err != nil {
		return nil, err
	}

	var msgs []benchMsg
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
		msgs = append(msgs, benchMsg{
			topicName: ch.Topic,
			data:      data,
			timestamp: int64(msg.LogTime),
		})
	}
	return msgs, nil
}

// convertMcapToTd converts an MCAP file to a TD file using the given config pair.
func convertMcapToTd(mcapPath, tdPath string, pair ConfigPair) error {
	f, err := os.Open(mcapPath)
	if err != nil {
		return err
	}
	defer f.Close()

	r, err := mcap.NewReader(f)
	if err != nil {
		return err
	}
	defer r.Close()

	info, err := r.Info()
	if err != nil {
		return err
	}

	out, err := os.Create(tdPath)
	if err != nil {
		return err
	}
	defer out.Close()

	w := turbodata.NewWriter(out)
	if err := writeTdFromMcap(r, info, w, pair); err != nil {
		return err
	}
	return w.Close()
}

// writeTdFromMcap writes all messages from the MCAP reader into the TD writer using the given pair.
func writeTdFromMcap(r *mcap.Reader, info *mcap.Info, w *turbodata.Writer, pair ConfigPair) error {
	schemaByID := info.Schemas

	// Image topics: one group per channel.
	for _, ch := range info.Channels {
		schema := schemaByID[ch.SchemaID]
		if schema.Name != "foxglove.RawImage" {
			continue
		}
		meta := map[string]any{
			"schema_encoding": schema.Encoding,
			"schema_data":     schema.Data,
			"schema_name":     schema.Name,
		}
		if err := w.OpenTopics(
			[]string{ch.Topic},
			[]map[string]any{meta},
			turbodata.WithCompression(),
			turbodata.WithChunkConfig(pair.ImgConfig),
		); err != nil {
			return err
		}

		it, err := r.Messages(mcap.InOrder(mcap.LogTimeOrder), mcap.WithTopics([]string{ch.Topic}))
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
			if err := w.WriteMessage(ch.Topic, msg.Data, int64(msg.LogTime)); err != nil {
				return err
			}
		}
		if err := w.CloseTopic(); err != nil {
			return err
		}
	}

	// Non-image topics: grouped by schema ID.
	schemaIdToTopics := make(map[uint16][]string)
	for _, ch := range info.Channels {
		if schemaByID[ch.SchemaID].Name == "foxglove.RawImage" {
			continue
		}
		schemaIdToTopics[ch.SchemaID] = append(schemaIdToTopics[ch.SchemaID], ch.Topic)
	}
	for schemaID, topics := range schemaIdToTopics {
		schema := schemaByID[schemaID]
		metadatas := make([]map[string]any, len(topics))
		for i := range topics {
			metadatas[i] = map[string]any{
				"schema_encoding": schema.Encoding,
				"schema_data":     schema.Data,
				"schema_name":     schema.Name,
			}
		}
		if err := w.OpenTopics(
			topics,
			metadatas,
			turbodata.WithCompression(),
			turbodata.WithChunkConfig(pair.NonImgConfig),
		); err != nil {
			return err
		}
		it, err := r.Messages(mcap.InOrder(mcap.LogTimeOrder), mcap.WithTopics(topics))
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
	}
	return nil
}
