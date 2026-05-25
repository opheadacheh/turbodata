package iorange

import (
	"testing"
)

func TestLoadedBytesGet(t *testing.T) {
	// Three input ranges. After "planning", two of them live in op 0 (offset 0,
	// length 30) and one in op 1 (offset 100, length 20).
	ranges := []Range{
		{Offset: 0, Length: 10},
		{Offset: 100, Length: 20},
		{Offset: 20, Length: 10},
	}
	locs := []RangeLocation{
		{OpIndex: 0, InOpOff: 0, Length: 10},
		{OpIndex: 1, InOpOff: 0, Length: 20},
		{OpIndex: 0, InOpOff: 20, Length: 10},
	}
	bufs := [][]byte{
		[]byte("0123456789AAAAAAAAAA9876543210"),
		[]byte("ZZZZZZZZZZYYYYYYYYYY"),
	}
	lb := NewLoadedBytes(ranges, locs, bufs)

	got := lb.Get(0)
	if string(got) != "0123456789" {
		t.Errorf("Get(0) = %q, want %q", got, "0123456789")
	}
	got = lb.Get(20)
	if string(got) != "9876543210" {
		t.Errorf("Get(20) = %q, want %q", got, "9876543210")
	}
	got = lb.Get(100)
	if string(got) != "ZZZZZZZZZZYYYYYYYYYY" {
		t.Errorf("Get(100) = %q, want %q", got, "ZZZZZZZZZZYYYYYYYYYY")
	}

	if lb.Get(999) != nil {
		t.Errorf("Get(999) should return nil for unregistered offset, got %q", lb.Get(999))
	}
}
