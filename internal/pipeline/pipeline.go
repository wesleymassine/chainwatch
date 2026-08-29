// Package pipeline joins the three steps this service is made of: fetch a block,
// match its transactions against the watched set, publish what matched.
package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

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
}

type Pipeline struct {
	client    *ethrpc.Client
	matcher   *matcher.Matcher
	publisher publisher.Publisher
	opts      Options
	log       *slog.Logger
}

func New(c *ethrpc.Client, m *matcher.Matcher, p publisher.Publisher, opts Options, log *slog.Logger) *Pipeline {
	return &Pipeline{client: c, matcher: m, publisher: p, opts: opts, log: log}
}

// Run processes blocks from `next` onwards until ctx is cancelled.
func (p *Pipeline) Run(ctx context.Context, next uint64) error {
	for {
		head, err := p.client.BlockNumber(ctx)
		if err != nil {
			return fmt.Errorf("reading head: %w", err)
		}
		if next <= head {
			if err := p.catchUp(ctx, next, head); err != nil {
				return err
			}
			next = head + 1
		}
		select {
		case <-time.After(p.opts.Poll):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
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

// publish walks a chunk in order. The order of these lines is the at-least-once
// guarantee: a block is not done until Publish has returned, and Publish does
// not return until the broker has acknowledged.
func (p *Pipeline) publish(ctx context.Context, blocks []*ethrpc.Block) error {
	for _, block := range blocks {
		events := p.matcher.Block(block)
		if err := p.publisher.Publish(ctx, events); err != nil {
			return fmt.Errorf("publishing block %d: %w", block.Number, err)
		}
		p.log.Info("block processed",
			"number", block.Number, "txs", len(block.Txs), "events", len(events))
	}
	return nil
}
