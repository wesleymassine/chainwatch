package ethrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wesleymassine/chainwatch/internal/addresses"
)

// fakeNode answers eth_getBlockByNumber from a generated set of blocks and can
// be told to reply with fewer results than were asked for, which is what a real
// public endpoint does when the batch response outgrows its size cap.
type fakeNode struct {
	mu        sync.Mutex
	maxResult int      // 0 means answer everything
	batches   []int    // how many requests each batch carried
	agents    []string // User-Agent seen per call
	status    int      // non-zero forces this status until okAfter calls
	okAfter   int
	logs      bytes.Buffer
}

func (f *fakeNode) server(t *testing.T) *Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var reqs []request
		if err := json.NewDecoder(r.Body).Decode(&reqs); err != nil {
			t.Errorf("decoding request: %v", err)
			return
		}

		f.mu.Lock()
		f.batches = append(f.batches, len(reqs))
		f.agents = append(f.agents, r.Header.Get("User-Agent"))
		calls := len(f.batches)
		status, okAfter, maxResult := f.status, f.okAfter, f.maxResult
		f.mu.Unlock()

		if status != 0 && calls <= okAfter {
			w.WriteHeader(status)
			fmt.Fprint(w, `{"error":"slow down"}`)
			return
		}

		out := make([]response, 0, len(reqs))
		for i, req := range reqs {
			if maxResult > 0 && i >= maxResult {
				break // silent truncation, exactly like the real endpoint
			}
			switch req.Method {
			case "eth_blockNumber":
				out = append(out, response{ID: req.ID, Result: json.RawMessage(`"0x18a1b2c"`)})
			case "eth_getBlockByNumber":
				n := req.Params[0].(string)
				out = append(out, response{ID: req.ID, Result: json.RawMessage(blockJSON(n))})
			}
		}
		json.NewEncoder(w).Encode(out)
	}))
	t.Cleanup(srv.Close)
	return New(srv.URL, slog.New(slog.NewTextHandler(&f.logs, nil)))
}

func blockJSON(number string) string {
	return fmt.Sprintf(`{"number":%q,"hash":"0xaa","parentHash":"0xbb","transactions":[
		{"hash":"0xcc","from":"0x28c6c06298d514db089934071355e5743bf21d60",
		 "to":"0x05ff6964d21e5dae3b1010d5ae0465b3c450f381","value":"0xde0b6b3a7640000"}]}`, number)
}

func numbers(from, to uint64) []uint64 {
	var out []uint64
	for n := from; n <= to; n++ {
		out = append(out, n)
	}
	return out
}

func TestBlockNumber(t *testing.T) {
	node := &fakeNode{}
	got, err := node.server(t).BlockNumber(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if want := uint64(0x18a1b2c); got != want {
		t.Errorf("BlockNumber() = %d, want %d", got, want)
	}
}

func TestBlocks(t *testing.T) {
	node := &fakeNode{}
	got, err := node.server(t).Blocks(context.Background(), numbers(100, 109))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 10 {
		t.Fatalf("got %d blocks, want 10", len(got))
	}
	for i, b := range got {
		if want := uint64(100 + i); b.Number != want {
			t.Errorf("blocks[%d].Number = %d, want %d (results must be sorted)", i, b.Number, want)
		}
	}
}

// The one that matters: a node that silently returns fewer results than asked
// must not cost us blocks.
func TestBlocksRecoversFromTruncatedBatch(t *testing.T) {
	node := &fakeNode{maxResult: 4}
	got, err := node.server(t).Blocks(context.Background(), numbers(100, 109))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 10 {
		t.Fatalf("got %d blocks, want all 10 despite truncation", len(got))
	}
	for i, b := range got {
		if want := uint64(100 + i); b.Number != want {
			t.Fatalf("blocks[%d].Number = %d, want %d", i, b.Number, want)
		}
	}
	// 10 asked, 4 answered each round: 10 -> 6 -> 2 -> done.
	if want := []int{10, 6, 2}; !equal(node.batches, want) {
		t.Errorf("batch sizes = %v, want %v (only the missing ones are re-asked)", node.batches, want)
	}
}

func TestBlocksGivesUpWhenNodeNeverCompletes(t *testing.T) {
	node := &fakeNode{maxResult: 1}
	_, err := node.server(t).Blocks(context.Background(), numbers(100, 199))
	if err == nil {
		t.Fatal("want an error rather than silently returning fewer blocks")
	}
	if !strings.Contains(err.Error(), "still missing") {
		t.Errorf("error = %v, want it to name the missing blocks", err)
	}
}

func TestPostRetriesOnRateLimit(t *testing.T) {
	node := &fakeNode{status: http.StatusTooManyRequests, okAfter: 2}
	got, err := node.server(t).Blocks(context.Background(), numbers(100, 101))
	if err != nil {
		t.Fatalf("429 must be retried, not fatal: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d blocks, want 2", len(got))
	}
	// The rate limiting has to be visible. We never slow down to avoid it, but
	// pretending it did not happen would hide a real property of the endpoint.
	logs := node.logs.String()
	if !strings.Contains(logs, "rate_limited=true") || !strings.Contains(logs, "status=429") {
		t.Errorf("429 was not logged; got:\n%s", logs)
	}
}

func TestBackoffIsJittered(t *testing.T) {
	seen := map[time.Duration]bool{}
	for i := 0; i < 50; i++ {
		d := backoff(3)
		if d < 200*time.Millisecond || d >= 400*time.Millisecond {
			t.Fatalf("backoff(3) = %v, want it within [200ms, 400ms)", d)
		}
		seen[d] = true
	}
	if len(seen) < 10 {
		t.Errorf("backoff produced %d distinct values in 50 calls; workers would retry in lockstep", len(seen))
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestSendsExplicitUserAgent(t *testing.T) {
	node := &fakeNode{}
	if _, err := node.server(t).BlockNumber(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Both public endpoints answer 403 to Go's default.
	if got := node.agents[0]; !strings.HasPrefix(got, "chainwatch/") {
		t.Errorf("User-Agent = %q, want it to start with chainwatch/", got)
	}
}

func TestBlocksRejectsMissingBlock(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `[{"id":0,"result":null}]`)
	}))
	defer srv.Close()
	_, err := New(srv.URL, discardLogger()).Blocks(context.Background(), []uint64{42})
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("error = %v, want it to say block 42 was not found", err)
	}
}

func TestBlocksPropagatesRPCError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `[{"id":0,"error":{"code":-32000,"message":"header not found"}}]`)
	}))
	defer srv.Close()
	_, err := New(srv.URL, discardLogger()).Blocks(context.Background(), []uint64{42})
	if err == nil || !strings.Contains(err.Error(), "header not found") {
		t.Fatalf("error = %v, want the node's message", err)
	}
}

// The decoder must survive every transaction type both chains actually produce.
// These fixtures are real blocks, captured 2026-08-28.
func TestDecodeRealBlocks(t *testing.T) {
	tests := []struct {
		file          string
		wantTxs       int
		wantCreations int
	}{
		{file: "testdata/arbitrum_block.json", wantTxs: 4},                  // includes a 0x6a system tx
		{file: "testdata/mainnet_block.json", wantTxs: 6, wantCreations: 1}, // 0x0 0x1 0x2 0x3 0x4
	}
	for _, tt := range tests {
		t.Run(tt.file, func(t *testing.T) {
			data, err := os.ReadFile(tt.file)
			if err != nil {
				t.Fatal(err)
			}
			var b Block
			if err := json.Unmarshal(data, &b); err != nil {
				t.Fatalf("a real block must decode: %v", err)
			}
			if len(b.Txs) != tt.wantTxs {
				t.Fatalf("decoded %d transactions, want %d", len(b.Txs), tt.wantTxs)
			}
			if b.Number == 0 || b.Hash == "" || b.ParentHash == "" {
				t.Errorf("block header did not decode: %+v", b)
			}

			creations := 0
			for _, tx := range b.Txs {
				if tx.From == (addresses.Address{}) {
					t.Errorf("tx %s decoded without a from — the node always sends one", tx.Hash)
				}
				if tx.Value == nil {
					t.Errorf("tx %s decoded without a value", tx.Hash)
				}
				if tx.To == nil {
					creations++
				}
			}
			if creations != tt.wantCreations {
				t.Errorf("got %d contract creations, want %d", creations, tt.wantCreations)
			}
		})
	}
}

func equal(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// A value is hex from a node we do not control, and big.Int.SetString happily
// accepts a sign. A negative amount would reach the ledger looking valid.
func TestDecodeRejectsMalformedValues(t *testing.T) {
	const from = `"from":"0x28c6c06298d514db089934071355e5743bf21d60"`
	tests := []struct {
		name string
		json string
	}{
		{name: "negative", json: `{"hash":"0x1",` + from + `,"to":null,"value":"0x-1"}`},
		{name: "not hex", json: `{"hash":"0x1",` + from + `,"to":null,"value":"0xzz"}`},
		{name: "empty", json: `{"hash":"0x1",` + from + `,"to":null,"value":"0x"}`},
		{name: "missing", json: `{"hash":"0x1",` + from + `,"to":null}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var tx Tx
			if err := json.Unmarshal([]byte(tt.json), &tx); err == nil {
				t.Fatalf("decoded a %s value as %s, want an error", tt.name, tx.Value)
			}
		})
	}
}

// Decoding must assign every field, not just the ones present. A Tx reused
// across decodes that kept a previous recipient would credit the wrong user.
func TestDecodeClearsRecipientOnReuse(t *testing.T) {
	const from = `"from":"0x28c6c06298d514db089934071355e5743bf21d60"`
	var tx Tx
	if err := json.Unmarshal([]byte(`{"hash":"0x1",`+from+`,"to":"0x05ff6964d21e5dae3b1010d5ae0465b3c450f381","value":"0x1"}`), &tx); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(`{"hash":"0x2",`+from+`,"to":null,"value":"0x1"}`), &tx); err != nil {
		t.Fatal(err)
	}
	if tx.To != nil {
		t.Fatalf("To = %s after decoding a contract creation, want nil", tx.To)
	}
}

func TestParseHexUint64AcceptsEitherCasePrefix(t *testing.T) {
	for _, in := range []string{"0x1f", "0X1F", "1f"} {
		got, err := parseHexUint64(in)
		if err != nil {
			t.Errorf("parseHexUint64(%q): %v", in, err)
			continue
		}
		if got != 31 {
			t.Errorf("parseHexUint64(%q) = %d, want 31", in, got)
		}
	}
}
