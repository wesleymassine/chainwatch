package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wesleymassine/chainwatch/internal/addresses"
	"github.com/wesleymassine/chainwatch/internal/ethrpc"
	"github.com/wesleymassine/chainwatch/internal/matcher"
	"github.com/wesleymassine/chainwatch/internal/publisher"
)

const (
	watchedAddr = "0x28c6c06298d514db089934071355e5743bf21d60"
	otherAddr   = "0x1111111111111111111111111111111111111111"
)

// node serves the two JSON-RPC methods the pipeline uses, and records which
// blocks were asked for.
type node struct {
	mu        sync.Mutex
	head      uint64
	asked     []uint64
	watchedIn map[uint64]bool // blocks whose transaction belongs to a watched user
}

func (n *node) start(t *testing.T) *ethrpc.Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var reqs []struct {
			ID     int    `json:"id"`
			Method string `json:"method"`
			Params []any  `json:"params"`
		}
		if err := json.Unmarshal(body, &reqs); err != nil {
			t.Errorf("decoding request: %v", err)
			return
		}
		var out []string
		for _, req := range reqs {
			switch req.Method {
			case "eth_blockNumber":
				n.mu.Lock()
				head := n.head
				n.mu.Unlock()
				out = append(out, fmt.Sprintf(`{"id":%d,"result":"0x%x"}`, req.ID, head))
			case "eth_getBlockByNumber":
				var number uint64
				fmt.Sscanf(req.Params[0].(string), "0x%x", &number)
				n.mu.Lock()
				n.asked = append(n.asked, number)
				from := otherAddr
				if n.watchedIn[number] {
					from = watchedAddr
				}
				n.mu.Unlock()
				out = append(out, fmt.Sprintf(
					`{"id":%d,"result":{"number":"0x%x","hash":"0x%02x","parentHash":"0x%02x","transactions":[`+
						`{"hash":"0xdead","from":%q,"to":%q,"value":"0xde0b6b3a7640000"}]}}`,
					req.ID, number, number, number-1, from, otherAddr))
			}
		}
		fmt.Fprint(w, "["+strings.Join(out, ",")+"]")
	}))
	t.Cleanup(srv.Close)
	return ethrpc.New(srv.URL, discard())
}

func (n *node) blocksAsked() []uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]uint64(nil), n.asked...)
}

func discard() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func newPipeline(t *testing.T, n *node, pub publisher.Publisher) *Pipeline {
	t.Helper()
	set, err := addresses.Load(strings.NewReader("userId,address\n7," + watchedAddr + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	// A poll interval long enough that Run never gets a second round: every test
	// here is about the first pass over a fixed range.
	return New(n.start(t), matcher.New(set), pub, time.Hour, discard())
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestRunPublishesMatchesUpToHead(t *testing.T) {
	n := &node{head: 104, watchedIn: map[uint64]bool{101: true, 103: true}}
	fake := publisher.NewFake()
	p := newPipeline(t, n, fake)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx, 100) }()

	waitFor(t, "both matching blocks to be published", func() bool { return len(fake.Events()) == 2 })
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() = %v, want context.Canceled", err)
	}

	if want := []uint64{100, 101, 102, 103, 104}; !equal(n.blocksAsked(), want) {
		t.Errorf("blocks asked = %v, want %v with no gaps", n.blocksAsked(), want)
	}
	for _, e := range fake.Events() {
		if e.UserID != 7 {
			t.Errorf("event for user %d, want 7", e.UserID)
		}
		if e.Amount != "1000000000000000000" {
			t.Errorf("amount = %s, want 1 ETH in wei", e.Amount)
		}
	}
}

// The invariant the whole design rests on: a block that could not be published
// stops the pipeline instead of being stepped over.
func TestRunStopsAtTheBlockItCannotPublish(t *testing.T) {
	n := &node{head: 104, watchedIn: map[uint64]bool{100: true, 101: true, 102: true, 103: true}}
	fake := publisher.NewFake()
	fake.FailAfter(2, errors.New("broker gone"))

	err := newPipeline(t, n, fake).Run(context.Background(), 100)
	if err == nil || !strings.Contains(err.Error(), "publishing block 102") {
		t.Fatalf("Run() = %v, want it to name the block it stopped on", err)
	}
	// 103 and 104 must never have been fetched: moving on would leave a hole
	// that nothing would ever come back for.
	if want := []uint64{100, 101, 102}; !equal(n.blocksAsked(), want) {
		t.Errorf("blocks asked = %v, want %v", n.blocksAsked(), want)
	}
	if got := len(fake.Events()); got != 2 {
		t.Errorf("published %d events, want the 2 from before the failure", got)
	}
}

func TestRunStopsOnCancellation(t *testing.T) {
	n := &node{head: 100, watchedIn: map[uint64]bool{}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := newPipeline(t, n, publisher.NewFake()).Run(ctx, 100); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() = %v, want context.Canceled", err)
	}
}

func TestRunPublishesNothingWhenNoAddressMatches(t *testing.T) {
	n := &node{head: 102, watchedIn: map[uint64]bool{}}
	fake := publisher.NewFake()
	p := newPipeline(t, n, fake)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx, 100) }()

	waitFor(t, "all blocks to be read", func() bool { return len(n.blocksAsked()) == 3 })
	cancel()
	<-done

	if got := len(fake.Events()); got != 0 {
		t.Errorf("published %d events for blocks that match nobody, want 0", got)
	}
}

func equal(a, b []uint64) bool {
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
