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

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintf(os.Stderr, "usage: mcap_to_td <input.mcap> <output.td>\n")
		os.Exit(1)
	}

	mcapPath := os.Args[1]
	tdPath := os.Args[2]

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

	tdFile, err := os.Create(tdPath)
	if err != nil {
		log.Fatalf("create td: %v", err)
	}
	defer tdFile.Close()

	tdWriter := turbodata.NewWriter(tdFile)
	defer tdWriter.Close()

	info, err := mcapReader.Info()
	if err != nil {
		log.Fatalf("mcap info: %v", err)
	}

	schemaIdToSchema := make(map[uint16]*mcap.Schema)
	for _, schema := range info.Schemas {
		schemaIdToSchema[schema.ID] = schema
	}

	for _, channel := range info.Channels {
		schema := schemaIdToSchema[channel.SchemaID]

		if schema.Name != "foxglove.RawImage" {
			continue
		}

		metadata := make(map[string]any)
		metadata["schema_encoding"] = schema.Encoding
		metadata["schema_data"] = schema.Data
		metadata["schema_name"] = schema.Name

		if err := tdWriter.OpenTopics([]string{channel.Topic}, []map[string]any{metadata}, turbodata.WithCompression()); err != nil {
			log.Fatalf("open topic: %v", err)
		}

		it, err := mcapReader.Messages(mcap.InOrder(mcap.LogTimeOrder), mcap.WithTopics([]string{channel.Topic}))
		if err != nil {
			log.Fatalf("failed to create mcap messages iterator: %v", err)
		}

		msg := &mcap.Message{}
		for {
			_, _, _, err := it.NextInto(msg)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				log.Fatalf("read message: %v", err)
			}

			if err := tdWriter.WriteMessage(channel.Topic, msg.Data, int64(msg.LogTime)); err != nil {
				log.Fatalf("write message: %v", err)
			}
		}

		if err := tdWriter.CloseTopic(); err != nil {
			log.Fatalf("close topic: %v", err)
		}
	}

	schemaIdToTopic := make(map[uint16][]string)
	for _, channel := range info.Channels {
		if schemaIdToTopic[channel.SchemaID] == nil {
			schemaIdToTopic[channel.SchemaID] = make([]string, 0)
		}
		schemaIdToTopic[channel.SchemaID] = append(schemaIdToTopic[channel.SchemaID], channel.Topic)
	}

	for schemaId, topics := range schemaIdToTopic {
		schema := schemaIdToSchema[schemaId]
		metadata := make(map[string]any)
		metadata["schema_encoding"] = schema.Encoding
		metadata["schema_data"] = schema.Data
		metadata["schema_name"] = schema.Name
		if err := tdWriter.OpenTopics(topics, []map[string]any{metadata}, turbodata.WithCompression()); err != nil {
			log.Fatalf("open topic: %v", err)
		}
	}

	// log.Printf("converted %s -> %s", mcapPath, tdPath)
}
