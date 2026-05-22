// mcap-mock-compress rewrites a raw-image MCAP file into a fixture that mimics
// what the file would look like if image messages were stored in a compressed
// codec (e.g. JPEG / H.264). For every channel whose schema is
// foxglove.RawImage, each message's payload is replaced with a fixed-size
// buffer of pseudo-random bytes (incompressible, like real codec output).
// All other messages, schemas, channels, attachments and metadata are passed
// through unchanged.
//
// The resulting file is NOT a valid foxglove.RawImage stream (the byte length
// no longer matches width*height*channels). It is intended only as a benchmark
// fixture for read-side experiments.
//
// Usage:
//
//	mcap-mock-compress -in input.mcap -out output.mcap [-size 60KB] [-seed 42]
package main

import (
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"

	"github.com/foxglove/mcap/go/mcap"
)

func main() {
	var (
		inPath  = flag.String("in", "", "path to input MCAP file (required)")
		outPath = flag.String("out", "", "path to output MCAP file (required)")
		sizeStr = flag.String("size", "60KB", "replacement payload size for foxglove.RawImage messages (e.g. 60KB, 200KB, 1MB)")
		seed    = flag.Uint64("seed", 42, "PRNG seed for replacement bytes")
	)
	flag.Parse()

	if *inPath == "" || *outPath == "" {
		fmt.Fprintln(os.Stderr, "error: -in and -out are required")
		flag.Usage()
		os.Exit(2)
	}

	size, err := parseSize(*sizeStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: invalid -size: %v\n", err)
		os.Exit(2)
	}

	if err := run(*inPath, *outPath, size, *seed); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run(inPath, outPath string, size int, seed uint64) error {
	in, err := os.Open(inPath)
	if err != nil {
		return fmt.Errorf("open input: %w", err)
	}
	defer in.Close()

	r, err := mcap.NewReader(in)
	if err != nil {
		return fmt.Errorf("new mcap reader: %w", err)
	}
	defer r.Close()

	info, err := r.Info()
	if err != nil {
		return fmt.Errorf("mcap info: %w", err)
	}

	out, err := os.Create(outPath)
	if err != nil {
		return fmt.Errorf("create output: %w", err)
	}
	defer out.Close()

	w, err := mcap.NewWriter(out, &mcap.WriterOptions{
		Chunked:     true,
		ChunkSize:   1 << 20,
		Compression: mcap.CompressionZSTD,
		IncludeCRC:  true,
	})
	if err != nil {
		return fmt.Errorf("new mcap writer: %w", err)
	}

	if err := w.WriteHeader(&mcap.Header{
		Profile: info.Header.Profile,
		Library: "mcap-mock-compress",
	}); err != nil {
		return fmt.Errorf("write header: %w", err)
	}

	// Re-emit schemas with their original IDs.
	for _, s := range info.Schemas {
		if err := w.WriteSchema(s); err != nil {
			return fmt.Errorf("write schema %d: %w", s.ID, err)
		}
	}

	// Re-emit channels with their original IDs and identify which channels
	// carry foxglove.RawImage messages.
	rawImageChannels := make(map[uint16]bool)
	for _, c := range info.Channels {
		if s, ok := info.Schemas[c.SchemaID]; ok && s.Name == "foxglove.RawImage" {
			rawImageChannels[c.ID] = true
		}
		if err := w.WriteChannel(c); err != nil {
			return fmt.Errorf("write channel %d: %w", c.ID, err)
		}
	}

	// Per-message random payload. We reuse one buffer to avoid allocations
	// but refill it with fresh random bytes for every image message so zstd
	// (and any other compressor) cannot dedupe identical payloads across
	// messages, mimicking real codec output.
	rng := rand.New(rand.NewPCG(seed, seed^0x9E3779B97F4A7C15))
	randomPayload := make([]byte, size)
	fillRandom := func() {
		i := 0
		for ; i+8 <= len(randomPayload); i += 8 {
			binary.LittleEndian.PutUint64(randomPayload[i:], rng.Uint64())
		}
		if i < len(randomPayload) {
			var tail [8]byte
			binary.LittleEndian.PutUint64(tail[:], rng.Uint64())
			copy(randomPayload[i:], tail[:])
		}
	}

	// Stream all messages in log-time order.
	it, err := r.Messages(mcap.InOrder(mcap.LogTimeOrder))
	if err != nil {
		return fmt.Errorf("mcap messages iterator: %w", err)
	}

	var (
		imgRewritten int64
		othersCopied int64
	)
	msg := &mcap.Message{}
	for {
		_, _, _, err := it.NextInto(msg)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("read message: %w", err)
		}

		if rawImageChannels[msg.ChannelID] {
			// Refill the buffer with fresh random bytes so each message has
			// distinct, incompressible content. Swap msg.Data in/out around
			// WriteMessage so iterator-owned memory isn't mutated.
			fillRandom()
			origData := msg.Data
			msg.Data = randomPayload
			err := w.WriteMessage(msg)
			msg.Data = origData
			if err != nil {
				return fmt.Errorf("write image message: %w", err)
			}
			imgRewritten++
		} else {
			if err := w.WriteMessage(msg); err != nil {
				return fmt.Errorf("write message: %w", err)
			}
			othersCopied++
		}
	}

	if err := w.Close(); err != nil {
		return fmt.Errorf("close mcap writer: %w", err)
	}

	inFi, _ := os.Stat(inPath)
	outFi, _ := os.Stat(outPath)
	fmt.Printf("rewrote %d image messages (payload=%d bytes each)\n", imgRewritten, size)
	fmt.Printf("copied  %d non-image messages\n", othersCopied)
	fmt.Printf("input:  %s (%d bytes)\n", inPath, inFi.Size())
	fmt.Printf("output: %s (%d bytes)\n", outPath, outFi.Size())
	return nil
}

// parseSize parses sizes like "60KB", "200KB", "1MB", "1024" (bytes).
// Suffixes are case-insensitive. KB/MB/GB use 1024-based units.
func parseSize(s string) (int, error) {
	s = strings.TrimSpace(strings.ToUpper(s))
	if s == "" {
		return 0, fmt.Errorf("empty")
	}
	mult := 1
	switch {
	case strings.HasSuffix(s, "KB"):
		mult = 1 << 10
		s = strings.TrimSuffix(s, "KB")
	case strings.HasSuffix(s, "MB"):
		mult = 1 << 20
		s = strings.TrimSuffix(s, "MB")
	case strings.HasSuffix(s, "GB"):
		mult = 1 << 30
		s = strings.TrimSuffix(s, "GB")
	case strings.HasSuffix(s, "B"):
		s = strings.TrimSuffix(s, "B")
	}
	n, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil {
		return 0, err
	}
	if n <= 0 {
		return 0, fmt.Errorf("must be positive")
	}
	return n * mult, nil
}
