package turbodata

type ChunkThresholdMode uint8

const (
	// When the data size of the chunk is greater than or equal to the threshold, a new chunk is created.
	ChunkThresholdModeSize ChunkThresholdMode = iota
	// When the time duration of the chunk is greater than or equal to the threshold, a new chunk is created.
	ChunkThresholdModeDuration
	// When the message count of the chunk is greater than or equal to the threshold, a new chunk is created.
	ChunkThresholdModeCount
)

type ChunkConfig struct {
	Mode     ChunkThresholdMode
	Size     uint64
	Duration uint64
	Count    uint32
}

type ChunkStatus struct {
	startTimestamp uint64
	size           uint64
	count          uint32
}
