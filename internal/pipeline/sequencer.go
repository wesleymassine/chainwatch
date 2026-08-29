package pipeline

import "github.com/wesleymassine/chainwatch/internal/ethrpc"

// sequencer restores order to chunks fetched concurrently.
//
// Workers fetch contiguous ranges rather than single blocks, because the RPC
// client batches anyway. So what arrives out of order is whole chunks, and
// putting them back in order is a map keyed by each chunk's first block rather
// than a sliding window over every block in flight.
//
// Nothing here does I/O, takes a lock or starts a goroutine. That is the point:
// ordering is the property most likely to break under concurrency, so it lives
// in a type that can be tested without any.
type sequencer struct {
	next    uint64
	pending map[uint64][]*ethrpc.Block
}

func newSequencer(from uint64) *sequencer {
	return &sequencer{next: from, pending: make(map[uint64][]*ethrpc.Block)}
}

// add takes a fetched chunk and returns every chunk that is now contiguous with
// what has already been released, in order.
//
// It returns nothing while a gap remains. That is the whole contract: a chunk is
// released only once every block before it has been, so a consumer of add can
// treat what it receives as the definitive order without knowing that anything
// was ever concurrent.
func (s *sequencer) add(blocks []*ethrpc.Block) [][]*ethrpc.Block {
	if len(blocks) == 0 {
		return nil
	}
	s.pending[blocks[0].Number] = blocks

	var ready [][]*ethrpc.Block
	for {
		chunk, ok := s.pending[s.next]
		if !ok {
			return ready
		}
		delete(s.pending, s.next)
		ready = append(ready, chunk)
		// Taken from the last block rather than from len(chunk), so a chunk that
		// was not what we assumed cannot silently shift everything after it.
		s.next = chunk[len(chunk)-1].Number + 1
	}
}

// awaiting is the block the sequencer is still waiting for. Everything below it
// has been released, which makes it exactly the point a checkpoint may advance
// to — that is what commit 7 will use it for.
func (s *sequencer) awaiting() uint64 { return s.next }

// held is how many chunks are waiting for a gap to close. It bounds memory, so
// the pipeline logs it rather than letting it grow unobserved.
func (s *sequencer) held() int { return len(s.pending) }
