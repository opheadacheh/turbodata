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
