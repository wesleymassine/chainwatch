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
	watchedIn map[uint64]bool          // blocks whose transaction belongs to a watched user
	delay     map[uint64]time.Duration // hold a block back, to force out-of-order arrival
	inFlight  int
	maxFlight int
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
				n.inFlight++
				if n.inFlight > n.maxFlight {
					n.maxFlight = n.inFlight
				}
				from := otherAddr
				if n.watchedIn[number] {
					from = watchedAddr
				}
				hold := n.delay[number]
				n.mu.Unlock()
				if hold > 0 {
					time.Sleep(hold)
				}
				n.mu.Lock()
				n.inFlight--
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

func newPipeline(t *testing.T, n *node, pub publisher.Publisher, opts Options) *Pipeline {
	t.Helper()
	set, err := addresses.Load(strings.NewReader("userId,address\n7," + watchedAddr + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	// A poll interval long enough that Run never gets a second round: every test
	// here is about the first pass over a fixed range.
	return New(n.start(t), matcher.New(set), pub, opts, discard())
}

// sequential is the shape the ordering assertions rely on: one worker, one block
// per fetch, so "blocks asked" is exactly the order they were processed in.
var sequential = Options{Poll: time.Hour, Workers: 1, BatchSize: 1}

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
	p := newPipeline(t, n, fake, sequential)

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

	err := newPipeline(t, n, fake, sequential).Run(context.Background(), 100)
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
	if err := newPipeline(t, n, publisher.NewFake(), sequential).Run(ctx, 100); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() = %v, want context.Canceled", err)
	}
}

func TestRunPublishesNothingWhenNoAddressMatches(t *testing.T) {
	n := &node{head: 102, watchedIn: map[uint64]bool{}}
	fake := publisher.NewFake()
	p := newPipeline(t, n, fake, sequential)

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

// Fetching concurrently is only allowed if the order that comes out the other
// end is still the order of the chain. Block 100 is held back so that 101-107
// are fetched and returned first; every one of them must wait.
func TestConcurrentFetchingStillPublishesInOrder(t *testing.T) {
	watched := map[uint64]bool{}
	for n := uint64(100); n <= 107; n++ {
		watched[n] = true
	}
	n := &node{
		head:      107,
		watchedIn: watched,
		delay:     map[uint64]time.Duration{100: 300 * time.Millisecond},
	}
	fake := publisher.NewFake()
	p := newPipeline(t, n, fake, Options{Poll: time.Hour, Workers: 4, BatchSize: 1})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx, 100) }()

	waitFor(t, "all eight blocks to be published", func() bool { return len(fake.Events()) == 8 })
	cancel()
	<-done

	var order []uint64
	for _, e := range fake.Events() {
		order = append(order, e.BlockNumber)
	}
	if want := []uint64{100, 101, 102, 103, 104, 105, 106, 107}; !equal(order, want) {
		t.Errorf("published in order %v, want %v", order, want)
	}
}

// And the pool has to be a pool: if the fetches were serialised, this would take
// eight times the per-block delay instead of roughly two.
func TestWorkersFetchInParallel(t *testing.T) {
	const hold = 100 * time.Millisecond
	watched := map[uint64]bool{}
	delay := map[uint64]time.Duration{}
	for n := uint64(100); n <= 107; n++ {
		watched[n], delay[n] = true, hold
	}
	n := &node{head: 107, watchedIn: watched, delay: delay}
	fake := publisher.NewFake()
	p := newPipeline(t, n, fake, Options{Poll: time.Hour, Workers: 4, BatchSize: 1})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	start := time.Now()
	go func() { done <- p.Run(ctx, 100) }()
	waitFor(t, "all eight blocks", func() bool { return len(fake.Events()) == 8 })
	elapsed := time.Since(start)
	cancel()
	<-done

	if serial := 8 * hold; elapsed >= serial {
		t.Errorf("took %v for 8 blocks; serial would be %v, so nothing ran in parallel", elapsed, serial)
	}
	n.mu.Lock()
	peak := n.maxFlight
	n.mu.Unlock()
	if peak < 2 {
		t.Errorf("peak concurrent fetches = %d, want at least 2", peak)
	}
	if peak > 4 {
		t.Errorf("peak concurrent fetches = %d, want at most the 4 workers configured", peak)
	}
}
