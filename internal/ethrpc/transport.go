package ethrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"time"
)

// How many transport failures one HTTP request tolerates before giving up.
// Separate from maxBatchRounds: one counts failures, the other counts incomplete
// batch responses, and they have no reason to move together.
//
// Note what this does not bound: rate limiting. See post.
const maxHTTPFailures = 5

type request struct {
	JSONRPC string `json:"jsonrpc"`
	ID      int    `json:"id"`
	Method  string `json:"method"`
	Params  []any  `json:"params"`
}

type response struct {
	ID     int             `json:"id"`
	Result json.RawMessage `json:"result"`
	Error  *rpcError       `json:"error"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *rpcError) Error() string { return fmt.Sprintf("rpc %d: %s", e.Code, e.Message) }

func responseErr(res []response) error {
	for _, r := range res {
		if r.Error != nil {
			return r.Error
		}
	}
	return fmt.Errorf("empty response")
}

// call sends every request as a JSON-RPC batch, even a single one, so there is
// only ever one shape to decode.
func (c *Client) call(ctx context.Context, reqs []request) ([]response, error) {
	for i := range reqs {
		reqs[i].JSONRPC = "2.0"
	}
	body, err := json.Marshal(reqs)
	if err != nil {
		return nil, err
	}
	raw, err := c.post(ctx, body)
	if err != nil {
		return nil, err
	}
	var res []response
	if err := json.Unmarshal(raw, &res); err != nil {
		return nil, fmt.Errorf("decoding batch response: %w", err)
	}
	return res, nil
}

// post retries what is worth retrying, and treats rate limiting as something
// other than a failure.
//
// The brief says to assume unrestricted usage and not to slow down for rate
// limits. Nothing here does: the steady state is never throttled, and no
// concurrency is given up to stay under a limit.
//
// But a 429 is the endpoint asking us to come back, not a request that failed.
// Giving up on one drops blocks, and that is the one outcome not allowed. So a
// 429 is retried for as long as the caller's context lives, and only real
// failures spend the budget.
//
// Arbitrum's public endpoint makes this concrete. Eight workers fetching batches
// of a hundred draw a storm of 429s within a second. With a finite budget the
// service dies on the very L2 it has to support.
//
// The retries are logged, so the rate limiting stays visible instead of hidden.
func (c *Client) post(ctx context.Context, body []byte) ([]byte, error) {
	var lastErr error
	for attempt, failures := 0, 0; failures < maxHTTPFailures; attempt++ {
		if attempt > 0 {
			select {
			case <-time.After(c.backoff(attempt)):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		// Both public endpoints answer 403 to Go's default User-Agent.
		req.Header.Set("User-Agent", c.agent)

		resp, err := c.http.Do(req)
		if err != nil {
			lastErr = err
			failures++
			continue
		}
		out, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			lastErr = err
			failures++
			continue
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			lastErr = fmt.Errorf("http %d: %s", resp.StatusCode, snippet(out))
			// Deliberately does not spend the failure budget.
			c.log.Warn("rate limited, retrying",
				"attempt", attempt+1, "backoff", c.backoff(attempt+1).String())
			continue
		}
		if resp.StatusCode >= 500 {
			lastErr = fmt.Errorf("http %d: %s", resp.StatusCode, snippet(out))
			failures++
			c.log.Warn("rpc request retried",
				"status", resp.StatusCode, "failure", failures, "of", maxHTTPFailures)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("http %d: %s", resp.StatusCode, snippet(out))
		}
		return out, nil
	}
	return nil, fmt.Errorf("after %d failures: %w", maxHTTPFailures, lastErr)
}

// backoff grows the pause between attempts and jitters it. Without the jitter a
// pool of workers that all hit the same rate limit would retry in lockstep and
// trip it again together.
func (c *Client) backoff(attempt int) time.Duration {
	base := c.backoffBase
	const maxDelay = 2 * time.Second
	// Clamped before shifting: with rate limiting retried indefinitely, the
	// attempt count is unbounded and a bare shift would overflow into nonsense.
	shift := attempt - 1
	if shift > 8 {
		shift = 8
	}
	d := base << shift
	if d > maxDelay {
		d = maxDelay
	}
	return d/2 + rand.N(d/2)
}

func snippet(b []byte) string {
	const limit = 120
	if len(b) > limit {
		return string(b[:limit]) + "…"
	}
	return string(b)
}
