package turbodata

type WriteOption func(metadata map[string]any) error

func WithChunkConfig(chunkConfig *ChunkConfig) WriteOption {
	return func(metadata map[string]any) error {
		metadata["chunk_config"] = chunkConfig
		return nil
	}
}

func WithCompression() WriteOption {
	return func(metadata map[string]any) error {
		metadata["is_compressed"] = true
		return nil
	}
}
