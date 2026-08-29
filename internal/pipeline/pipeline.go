// Package pipeline joins the three steps this service is made of: fetch a block,
// match its transactions against the watched set, publish what matched.
package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/wesleymassine/chainwatch/internal/ethrpc"
	"github.com/wesleymassine/chainwatch/internal/matcher"
	"github.com/wesleymassine/chainwatch/internal/publisher"
)

// Pipeline processes blocks one at a time, in order.
//
// This is deliberately the slow version. Getting the ordering right while there
// is only one block in flight makes the invariant easy to see — a block is
// published before the next one is fetched — and everything that comes later
// (concurrent fetching, a checkpoint, reorg handling) has to preserve it.
type Pipeline struct {
	client    *ethrpc.Client
	matcher   *matcher.Matcher
	publisher publisher.Publisher
	poll      time.Duration
	log       *slog.Logger
}

func New(c *ethrpc.Client, m *matcher.Matcher, p publisher.Publisher, poll time.Duration, log *slog.Logger) *Pipeline {
	return &Pipeline{client: c, matcher: m, publisher: p, poll: poll, log: log}
}

// Run processes blocks from `next` onwards until ctx is cancelled.
//
// It returns on the first error rather than skipping ahead. A block that cannot
// be published must not be stepped over: for now that means stopping, and from
// commit 7 it means stopping without having moved the checkpoint, so a restart
// resumes exactly there.
func (p *Pipeline) Run(ctx context.Context, next uint64) error {
	for {
		head, err := p.client.BlockNumber(ctx)
		if err != nil {
			return fmt.Errorf("reading head: %w", err)
		}
		if next > head {
			p.log.Debug("caught up", "next", next, "head", head)
		}
		for ; next <= head; next++ {
			if err := p.processBlock(ctx, next); err != nil {
				return err
			}
		}
		select {
		case <-time.After(p.poll):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// processBlock is the whole service in five lines, and the order of those lines
// is the at-least-once guarantee: nothing is considered done until Publish has
// returned, because Publish does not return until the broker has acknowledged.
func (p *Pipeline) processBlock(ctx context.Context, number uint64) error {
	blocks, err := p.client.Blocks(ctx, []uint64{number})
	if err != nil {
		return fmt.Errorf("fetching block %d: %w", number, err)
	}
	block := blocks[0]

	events := p.matcher.Block(block)
	if err := p.publisher.Publish(ctx, events); err != nil {
		return fmt.Errorf("publishing block %d: %w", number, err)
	}

	p.log.Info("block processed",
		"number", block.Number, "txs", len(block.Txs), "events", len(events))

	return nil
}
