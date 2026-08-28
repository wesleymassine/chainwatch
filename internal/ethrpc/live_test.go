package ethrpc

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/wesleymassine/chainwatch/internal/addresses"
)

// TestLive talks to a real node. It is skipped unless CHAINWATCH_LIVE_RPC is set,
// so the normal test run stays offline and deterministic.
func TestLive(t *testing.T) {
	url := os.Getenv("CHAINWATCH_LIVE_RPC")
	if url == "" {
		t.Skip("set CHAINWATCH_LIVE_RPC to run against a real node")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	c := New(url, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	head, err := c.BlockNumber(ctx)
	if err != nil {
		t.Fatal(err)
	}

	want := numbers(head-19, head)
	blocks, err := c.Blocks(ctx, want)
	if err != nil {
		t.Fatal(err)
	}
	if len(blocks) != len(want) {
		t.Fatalf("got %d blocks, want %d", len(blocks), len(want))
	}

	txs, senders, creations := 0, 0, 0
	for i, b := range blocks {
		if i > 0 && b.ParentHash != blocks[i-1].Hash {
			t.Errorf("block %d does not link to %d", b.Number, blocks[i-1].Number)
		}
		for _, tx := range b.Txs {
			txs++
			if tx.From != (addresses.Address{}) {
				senders++
			}
			if tx.To == nil {
				creations++
			}
		}
	}
	t.Logf("head %d: %d blocks, %d txs, %d with a sender, %d contract creations",
		head, len(blocks), txs, senders, creations)
	if txs > 0 && senders != txs {
		t.Errorf("%d of %d transactions decoded without a sender", txs-senders, txs)
	}
}
