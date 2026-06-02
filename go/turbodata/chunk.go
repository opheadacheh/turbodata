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
	Size     int64
	Duration int64
	Count    uint32
}

type ChunkStatus struct {
	startTimestamp int64
	size           int64
	count          uint32
}
