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

import "github.com/opheadacheh/turbodata/go/internal/iter"

// ReadSource is the combined I/O capability the Reader expects from its
// underlying storage. It is satisfied natively by *os.File and *bytes.Reader.
//
// Concurrency contract: ReadAt must be safe for concurrent calls (per
// io.ReaderAt's documentation). The cost-aware reader path invokes ReadAt
// from multiple goroutines simultaneously. Read and Seek may be stateful;
// the cost-aware path does not call them. The default (non-cost-aware)
// path calls Read and Seek serially.
type ReadSource = iter.ReadSource
