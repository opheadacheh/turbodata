package main

import (
	"errors"
	"io"
	"log"
	"os"
	"turbodata"
)

const fileName = "test.td"

func writeTurboData() {
	file, err := os.Create(fileName)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	writer := turbodata.NewWriter(file)
	defer writer.Close()

	if err := writer.OpenTopics([]string{"testTopic"}, []map[string]any{{}}, turbodata.WithCompression()); err != nil {
		log.Fatal(err)
	}

	if err := writer.WriteMessage("testTopic", []byte("Hello, World!"), 1); err != nil {
		log.Fatal(err)
	}
	if err := writer.CloseTopic(); err != nil {
		log.Fatal(err)
	}
}

func readTurboData() {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	reader, err := turbodata.NewReader(file)
	if err != nil {
		log.Fatal(err)
	}

	it, err := reader.ReadMessages()
	if err != nil {
		log.Fatal(err)
	}

	rb := turbodata.NewReusableBuffer()
	for {
		ts, name, err := it.NextInto(rb)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("ts: %d, name: %s, data: %s\n", ts, name, string(rb.Data))
	}
}

func main() {
	writeTurboData()
	readTurboData()
}
