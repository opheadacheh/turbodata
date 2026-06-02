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

package buffer

import "testing"

func TestNewReusableBuffer(t *testing.T) {
	b := NewReusableBuffer()
	if b == nil {
		t.Fatal("expected non-nil buffer")
	}

	if len(b.Data) != 0 {
		t.Fatalf("expected len(Data) == 0, got %d", len(b.Data))
	}
}

func TestReusableBufferPrepare(t *testing.T) {
	t.Run("within existing capacity", func(t *testing.T) {
		b := &ReusableBuffer{
			Data: make([]byte, 3, 8),
		}
		originalCap := cap(b.Data)

		b.Prepare(6)

		if len(b.Data) != 6 {
			t.Fatalf("expected len(Data) == 6, got %d", len(b.Data))
		}

		if cap(b.Data) != originalCap {
			t.Fatalf("expected cap(Data) to remain %d, got %d", originalCap, cap(b.Data))
		}
	})

	t.Run("grows when requested length exceeds capacity", func(t *testing.T) {
		b := &ReusableBuffer{
			Data: make([]byte, 2, 4),
		}

		b.Prepare(10)

		if len(b.Data) != 10 {
			t.Fatalf("expected len(Data) == 10, got %d", len(b.Data))
		}

		if cap(b.Data) < 10 {
			t.Fatalf("expected cap(Data) >= 10, got %d", cap(b.Data))
		}
	})

	t.Run("prepare zero length", func(t *testing.T) {
		b := &ReusableBuffer{
			Data: make([]byte, 2, 4),
		}

		b.Prepare(0)

		if len(b.Data) != 0 {
			t.Fatalf("expected len(Data) == 0, got %d", len(b.Data))
		}
	})
}
