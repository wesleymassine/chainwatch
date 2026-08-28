// Package ethrpc talks to an Ethereum node over plain JSON-RPC.
//
// It implements the two methods this service needs rather than depending on
// go-ethereum, which would pull 168 modules to provide an HTTP wrapper and a
// 20-byte array. The decoding it does bring is the part we specifically do not
// want: see the comment on Tx.
package ethrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"time"
)

// A batch response can arrive incomplete, so one fetch may need several rounds.
// The bound exists only so a node that never returns a block cannot hang us.
const maxBatchRounds = 5

type Client struct {
	url   string
	http  *http.Client
	agent string
	log   *slog.Logger
}

// New returns a client for one endpoint. A nil logger falls back to the default.
func New(url string, log *slog.Logger) *Client {
	if log == nil {
		log = slog.Default()
	}
	return &Client{
		url:   url,
		agent: "chainwatch/0.1",
		log:   log,
		http: &http.Client{
			Timeout: 60 * time.Second,
			Transport: &http.Transport{
				// The default is 2. With a pool of workers sharing one client,
				// that means connections are torn down and redialled constantly.
				MaxIdleConnsPerHost: 32,
			},
		},
	}
}

// BlockNumber returns the current head.
func (c *Client) BlockNumber(ctx context.Context) (uint64, error) {
	res, err := c.call(ctx, []request{{Method: "eth_blockNumber", Params: []any{}}})
	if err != nil {
		return 0, err
	}
	if len(res) != 1 || res[0].Error != nil {
		return 0, fmt.Errorf("eth_blockNumber: %w", responseErr(res))
	}
	var hex string
	if err := json.Unmarshal(res[0].Result, &hex); err != nil {
		return 0, err
	}
	return parseHexUint64(hex)
}

// Blocks fetches the given block numbers with their full transaction lists,
// sorted ascending.
//
// It never assumes the node returned as many results as were asked for. A public
// mainnet endpoint caps its response at around 24 MB and truncates the batch
// silently: ask for 100 blocks, receive 40, with HTTP 200 and no error anywhere.
// Trusting the arity there would drop blocks without a single log line, which is
// the exact failure the at-least-once requirement exists to prevent. So results
// are correlated by id and whatever is missing is asked for again.
func (c *Client) Blocks(ctx context.Context, numbers []uint64) ([]*Block, error) {
	found := make(map[uint64]*Block, len(numbers))
	pending := numbers

	for attempt := 0; len(pending) > 0; attempt++ {
		if attempt == maxBatchRounds {
			return nil, fmt.Errorf("%d of %d blocks still missing after %d rounds (first: %d)",
				len(pending), len(numbers), maxBatchRounds, pending[0])
		}

		reqs := make([]request, len(pending))
		for i, n := range pending {
			reqs[i] = request{ID: i, Method: "eth_getBlockByNumber",
				Params: []any{fmt.Sprintf("0x%x", n), true}}
		}
		res, err := c.call(ctx, reqs)
		if err != nil {
			return nil, err
		}

		for _, r := range res {
			if r.ID < 0 || r.ID >= len(pending) {
				return nil, fmt.Errorf("response id %d is outside the batch", r.ID)
			}
			n := pending[r.ID]
			if r.Error != nil {
				return nil, fmt.Errorf("block %d: %w", n, r.Error)
			}
			// A null result means the node does not have the block. Retrying
			// would loop until maxAttempts and hide the real problem, which is
			// that we asked for something past the head.
			if len(r.Result) == 0 || bytes.Equal(r.Result, []byte("null")) {
				return nil, fmt.Errorf("block %d not found", n)
			}
			var b Block
			if err := json.Unmarshal(r.Result, &b); err != nil {
				return nil, fmt.Errorf("block %d: %w", n, err)
			}
			found[n] = &b
		}

		var missing []uint64
		for _, n := range pending {
			if _, ok := found[n]; !ok {
				missing = append(missing, n)
			}
		}
		pending = missing
	}

	out := make([]*Block, 0, len(found))
	for _, b := range found {
		out = append(out, b)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Number < out[j].Number })
	return out, nil
}
