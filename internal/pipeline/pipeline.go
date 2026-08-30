// Package pipeline joins the three steps this service is made of: fetch a block,
// match its transactions against the watched set, publish what matched.
package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wesleymassine/chainwatch/internal/checkpoint"
	"github.com/wesleymassine/chainwatch/internal/ethrpc"
	"github.com/wesleymassine/chainwatch/internal/matcher"
	"github.com/wesleymassine/chainwatch/internal/publisher"
)

// Options are the knobs that differ between chains. Mainnet produces a block
// every 12-13s and Arbitrum up to four a second, so the same numbers cannot
// serve both. Commit 10 turns these into per-chain profiles.
type Options struct {
	Poll      time.Duration // how often to ask for a new head once caught up
	Workers   int           // concurrent fetches in flight
	BatchSize int           // blocks per fetch

	// ConfirmDepth is how far behind the head to stay. It is the cheap defence
	// against reorgs: a block that is two deep on mainnet is very unlikely to be
	// replaced, so staying behind avoids publishing work that the chain then
	// discards. Arbitrum has a centralised sequencer and does not reorg, so it
	// runs at zero and pays no latency for a risk it does not have.
	ConfirmDepth uint64
}

// How far to go back when the chain turns out not to link up. Reorgs on
// mainnet are one or two blocks deep and ConfirmDepth already absorbs those, so
// this only runs for something unusual — and going back further than necessary
// costs duplicates, which at-least-once permits, rather than correctness.
const rewindDepth = 16

// How long the checkpoint save is allowed to outlive a shutdown. See publish.
const checkpointGrace = 5 * time.Second

// How many reorgs in a row before giving up. A chain that will not link up after
// three rewinds is not reorganising, it is broken, and quietly rewinding forever
// would look like a service that is running.
const maxConsecutiveReorgs = 3

// reorgError says the chain did not link up where it was expected to.
type reorgError struct {
	block  uint64
	parent string
	expect string
}

func (e *reorgError) Error() string {
	return fmt.Sprintf("block %d has parent %s, expected %s: the chain reorganised",
		e.block, e.parent, e.expect)
}

type Pipeline struct {
	client     *ethrpc.Client
	matcher    *matcher.Matcher
	publisher  publisher.Publisher
	checkpoint checkpoint.Store
	opts       Options
	log        *slog.Logger

	// Hash of the last published block, so the next one can be checked against
	// it. Only ever touched by the goroutine draining the sequencer, which is
	// why it needs no lock. Empty means the link is unknown and the next block
	// is taken on trust — the first block after a start or a rewind.
	lastHash string

	// Read by whatever is serving /metrics, so these are atomic.
	counters struct {
		blocks, txs, events, reorgs atomic.Uint64
		lastBlock, head             atomic.Uint64
	}
}

// Stats is a snapshot of what the pipeline has done. Lag is the one worth
// watching: it is how far behind the chain the service is, and a lag that grows
// steadily is the signal that it has stopped keeping up.
type Stats struct {
	Blocks    uint64 `json:"blocksProcessed"`
	Txs       uint64 `json:"transactionsScanned"`
	Events    uint64 `json:"eventsPublished"`
	Reorgs    uint64 `json:"reorgsHandled"`
	LastBlock uint64 `json:"lastBlock"`
	Head      uint64 `json:"chainHead"`
	Lag       uint64 `json:"blocksBehind"`
}

func (p *Pipeline) Stats() Stats {
	s := Stats{
		Blocks:    p.counters.blocks.Load(),
		Txs:       p.counters.txs.Load(),
		Events:    p.counters.events.Load(),
		Reorgs:    p.counters.reorgs.Load(),
		LastBlock: p.counters.lastBlock.Load(),
		Head:      p.counters.head.Load(),
	}
	if s.Head > s.LastBlock {
		s.Lag = s.Head - s.LastBlock
	}
	return s
}

func New(c *ethrpc.Client, m *matcher.Matcher, pub publisher.Publisher, cp checkpoint.Store, opts Options, log *slog.Logger) *Pipeline {
	return &Pipeline{client: c, matcher: m, publisher: pub, checkpoint: cp, opts: opts, log: log}
}

// Run resumes from the stored checkpoint, or from fallback if there is none,
// and processes blocks until ctx is cancelled.
func (p *Pipeline) Run(ctx context.Context, fallback uint64) error {
	next, err := p.resume(ctx, fallback)
	if err != nil {
		return err
	}
	reorgs := 0
	for {
		head, err := p.client.BlockNumber(ctx)
		if err != nil {
			return fmt.Errorf("reading head: %w", err)
		}
		p.counters.head.Store(head)
		// Everything within ConfirmDepth of the head is still liable to change.
		target := head
		if target < p.opts.ConfirmDepth {
			target = 0
		} else {
			target -= p.opts.ConfirmDepth
		}

		if next <= target {
			err := p.catchUp(ctx, next, target)
			var reorg *reorgError
			if errors.As(err, &reorg) {
				reorgs++
				p.counters.reorgs.Add(1)
				if reorgs > maxConsecutiveReorgs {
					return fmt.Errorf("chain still not linking up after %d rewinds: %w",
						maxConsecutiveReorgs, err)
				}
				next = rewind(reorg.block, rewindDepth)
				// The link is unknown again: whatever we published before the
				// fork may no longer be on the chain, so the block we resume at
				// is taken on trust and re-establishes it.
				p.lastHash = ""
				p.log.Warn("reorg detected, rewinding",
					"at", reorg.block, "resuming", next, "attempt", reorgs)
				continue
			}
			if err != nil {
				return err
			}
			reorgs = 0
			next = target + 1
		}
		select {
		case <-time.After(p.opts.Poll):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func rewind(from, depth uint64) uint64 {
	if from < depth {
		return 0
	}
	return from - depth
}

// resume decides which block to start from.
//
// A stored checkpoint always wins over configuration. START_BLOCK says where to
// begin when there is nothing to resume; obeying it on a restart would replay
// from the wrong place, or worse, jump to the head and skip everything in
// between.
func (p *Pipeline) resume(ctx context.Context, fallback uint64) (uint64, error) {
	cp, found, err := p.checkpoint.Load(ctx)
	if err != nil {
		return 0, fmt.Errorf("loading checkpoint: %w", err)
	}
	if !found {
		p.log.Info("no checkpoint found, starting fresh", "from", fallback)
		return fallback, nil
	}
	// This is what the stored hash is for. Restoring it means the first block
	// fetched after a restart is checked against the last one published before
	// it, so a reorg that happened while the service was down is caught rather
	// than assumed away.
	p.lastHash = cp.Hash
	p.log.Info("resuming from checkpoint", "block", cp.Block, "hash", cp.Hash, "from", cp.Block+1)
	return cp.Block + 1, nil
}

// catchUp fetches [from, to] with a pool of workers and publishes it in order.
//
// The same path serves both regimes. At the head it is a single chunk of one
// block; after an interruption it is thousands. Having one path means the code
// that runs in the rare, important case is the code that runs all the time.
func (p *Pipeline) catchUp(parent context.Context, from, to uint64) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	jobs := make(chan []uint64)
	// Buffered by the worker count: a worker that has fetched may hand its
	// chunk over and take the next job, but the pool cannot run further than
	// that ahead of the publisher. That bound is what keeps memory flat during
	// a long catch-up rather than letting fetched blocks pile up.
	results := make(chan []*ethrpc.Block, p.opts.Workers)
	fetchErr := make(chan error, 1)

	go p.feed(ctx, jobs, from, to)

	var wg sync.WaitGroup
	for i := 0; i < p.opts.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				blocks, err := p.client.Blocks(ctx, job)
				if err != nil {
					select {
					case fetchErr <- fmt.Errorf("fetching blocks %d-%d: %w", job[0], job[len(job)-1], err):
					default: // another worker already reported one
					}
					cancel()
					return
				}
				select {
				case results <- blocks:
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	go func() {
		wg.Wait()
		close(results)
	}()

	seq := newSequencer(from)
	for blocks := range results {
		for _, chunk := range seq.add(blocks) {
			if err := p.publish(ctx, chunk); err != nil {
				return err
			}
		}
	}

	select {
	case err := <-fetchErr:
		return err
	default:
	}
	if err := parent.Err(); err != nil {
		return err
	}
	// Nothing above should be able to end the loop with blocks unreleased, but
	// this is the invariant the whole service rests on: finishing quietly while
	// holding a gap would look exactly like success.
	if seq.awaiting() != to+1 {
		return fmt.Errorf("stopped at block %d with %d chunks held, expected to reach %d",
			seq.awaiting(), seq.held(), to+1)
	}
	return nil
}

// feed splits [from, to] into contiguous chunks for the workers.
func (p *Pipeline) feed(ctx context.Context, jobs chan<- []uint64, from, to uint64) {
	defer close(jobs)
	for start := from; start <= to; start += uint64(p.opts.BatchSize) {
		end := start + uint64(p.opts.BatchSize) - 1
		if end > to {
			end = to
		}
		chunk := make([]uint64, 0, end-start+1)
		for n := start; n <= end; n++ {
			chunk = append(chunk, n)
		}
		select {
		case jobs <- chunk:
		case <-ctx.Done():
			return
		}
	}
}

// publish sends a whole chunk, then records that it is done.
//
// The order of these three steps is the at-least-once guarantee, and it only
// reads correctly in this direction: match, publish and wait for the broker's
// acknowledgement, and only then move the checkpoint. Saving first would mean an
// interruption in between leaves the checkpoint claiming work that was never
// published, and those transactions would never be looked at again.
//
// The whole chunk goes in one Publish because the ack barrier and the checkpoint
// must have the same granularity. Paying for twenty acknowledgements to protect
// a window that is already twenty blocks wide buys nothing but latency.
func (p *Pipeline) publish(ctx context.Context, blocks []*ethrpc.Block) error {
	if len(blocks) == 0 {
		return nil
	}
	first, last := blocks[0], blocks[len(blocks)-1]

	// Check the whole chunk links up before publishing any of it. A block whose
	// parent is not what we published is a block on a different chain, and
	// emitting it would credit users for transactions that no longer happened.
	prev := p.lastHash
	for _, block := range blocks {
		if prev != "" && block.ParentHash != prev {
			return &reorgError{block: block.Number, parent: block.ParentHash, expect: prev}
		}
		prev = block.Hash
	}

	var events []matcher.Event
	txs := 0
	for _, block := range blocks {
		events = append(events, p.matcher.Block(block)...)
		txs += len(block.Txs)
	}

	if err := p.publisher.Publish(ctx, events); err != nil {
		return fmt.Errorf("publishing blocks %d-%d: %w", first.Number, last.Number, err)
	}
	// The broker has the events now. Recording that must not be abandoned just
	// because a shutdown started a moment ago, so the save gets its own context:
	// giving up here would republish this whole chunk on the next start, which
	// is a duplicate we chose to make rather than one we could not avoid.
	saveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), checkpointGrace)
	defer cancel()
	if err := p.checkpoint.Save(saveCtx, checkpoint.Checkpoint{Block: last.Number, Hash: last.Hash}); err != nil {
		return err
	}

	p.lastHash = prev
	p.counters.blocks.Add(uint64(len(blocks)))
	p.counters.txs.Add(uint64(txs))
	p.counters.events.Add(uint64(len(events)))
	p.counters.lastBlock.Store(last.Number)
	p.log.Info("blocks processed",
		"from", first.Number, "to", last.Number, "txs", txs, "events", len(events))
	return nil
}
