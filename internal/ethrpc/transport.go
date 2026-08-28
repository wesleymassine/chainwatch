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

// How many times one HTTP request is attempted before giving up. Separate from
// maxBatchRounds: one counts transport failures, the other counts incomplete
// batch responses, and they have no reason to move together.
const maxHTTPAttempts = 5

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

// post retries the failures that are worth retrying.
//
// The brief says to assume unrestricted usage and not to slow down for rate
// limits, and this does not: the steady state is never throttled. But public
// endpoints do answer 429 under load, and treating that as fatal would drop
// blocks. Losing data is the one outcome that is not allowed, so a 429 is
// logged and the request is made again.
//
// Logging it rather than swallowing it is the point: the rate limiting is real
// and visible in the output, without ever being worked around.
func (c *Client) post(ctx context.Context, body []byte) ([]byte, error) {
	var lastErr error
	for attempt := 0; attempt < maxHTTPAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-time.After(backoff(attempt)):
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
			continue
		}
		out, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			lastErr = err
			continue
		}
		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
			lastErr = fmt.Errorf("http %d: %s", resp.StatusCode, snippet(out))
			c.log.Warn("rpc request retried",
				"status", resp.StatusCode, "attempt", attempt+1, "of", maxHTTPAttempts,
				"rate_limited", resp.StatusCode == http.StatusTooManyRequests)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("http %d: %s", resp.StatusCode, snippet(out))
		}
		return out, nil
	}
	return nil, fmt.Errorf("after %d attempts: %w", maxHTTPAttempts, lastErr)
}

// backoff grows the pause between attempts and jitters it. Without the jitter a
// pool of workers that all hit the same rate limit would retry in lockstep and
// trip it again together.
func backoff(attempt int) time.Duration {
	const (
		base = 100 * time.Millisecond
		max  = 2 * time.Second
	)
	d := base << (attempt - 1)
	if d > max {
		d = max
	}
	return d/2 + rand.N(d/2)
}

func snippet(b []byte) string {
	const max = 120
	if len(b) > max {
		return string(b[:max]) + "…"
	}
	return string(b)
}
