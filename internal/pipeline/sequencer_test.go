package pipeline

import (
	"testing"

	"github.com/wesleymassine/chainwatch/internal/ethrpc"
)

// chunk builds a contiguous run of blocks [from, to].
func chunk(from, to uint64) []*ethrpc.Block {
	var out []*ethrpc.Block
	for n := from; n <= to; n++ {
		out = append(out, &ethrpc.Block{Number: n})
	}
	return out
}

// released flattens what add returned into the block numbers it let through.
func released(chunks [][]*ethrpc.Block) []uint64 {
	var out []uint64
	for _, c := range chunks {
		for _, b := range c {
			out = append(out, b.Number)
		}
	}
	return out
}

func TestSequencerReleasesInOrderChunksImmediately(t *testing.T) {
	s := newSequencer(100)
	for start := uint64(100); start < 106; start += 2 {
		got := released(s.add(chunk(start, start+1)))
		if want := []uint64{start, start + 1}; !equal(got, want) {
			t.Fatalf("add(%d-%d) released %v, want %v", start, start+1, got, want)
		}
	}
	if s.held() != 0 {
		t.Errorf("held %d chunks after a clean run, want 0", s.held())
	}
}

// The property the whole design depends on: a chunk is not released while any
// block before it is still missing.
func TestSequencerHoldsChunksUntilTheGapCloses(t *testing.T) {
	s := newSequencer(100)

	for _, c := range [][2]uint64{{104, 105}, {102, 103}, {106, 107}} {
		if got := released(s.add(chunk(c[0], c[1]))); len(got) != 0 {
			t.Fatalf("add(%d-%d) released %v while 100-101 was missing, want nothing", c[0], c[1], got)
		}
	}
	if s.held() != 3 {
		t.Errorf("held %d chunks, want 3", s.held())
	}
	if s.awaiting() != 100 {
		t.Errorf("awaiting %d, want 100 — nothing may be considered done yet", s.awaiting())
	}

	// The missing chunk arrives last and everything behind it drains at once,
	// in order.
	got := released(s.add(chunk(100, 101)))
	want := []uint64{100, 101, 102, 103, 104, 105, 106, 107}
	if !equal(got, want) {
		t.Fatalf("released %v, want %v", got, want)
	}
	if s.held() != 0 {
		t.Errorf("held %d chunks after the gap closed, want 0", s.held())
	}
	if s.awaiting() != 108 {
		t.Errorf("awaiting %d, want 108", s.awaiting())
	}
}

func TestSequencerHandlesChunksOfDifferentSizes(t *testing.T) {
	s := newSequencer(10)
	if got := released(s.add(chunk(13, 13))); len(got) != 0 {
		t.Fatalf("released %v out of order", got)
	}
	if got := released(s.add(chunk(10, 12))); !equal(got, []uint64{10, 11, 12, 13}) {
		t.Fatalf("released %v, want 10-13", got)
	}
}

func TestSequencerIgnoresEmptyChunks(t *testing.T) {
	s := newSequencer(100)
	if got := s.add(nil); got != nil {
		t.Errorf("add(nil) = %v, want nil", got)
	}
	if s.awaiting() != 100 {
		t.Errorf("an empty chunk moved awaiting to %d", s.awaiting())
	}
}
