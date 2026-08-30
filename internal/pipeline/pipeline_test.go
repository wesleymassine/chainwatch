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
	"github.com/wesleymassine/chainwatch/internal/checkpoint"
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

	// version stamps the block hashes. Bumping it swaps the chain underneath the
	// pipeline, which is what a reorg looks like from here: the same numbers
	// come back with different hashes that no longer link to what was published.
	version int
	forkAt  int  // bump the version once this many blocks have been served
	chaos   bool // bump on every block, so the chain never settles
	served  int
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
				n.served++
				if n.chaos || (n.forkAt > 0 && n.served == n.forkAt) {
					n.version++
				}
				version := n.version
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
					`{"id":%d,"result":{"number":"0x%x","hash":"0x%d%02x","parentHash":"0x%d%02x","transactions":[`+
						`{"hash":"0xdead","from":%q,"to":%q,"value":"0xde0b6b3a7640000"}]}}`,
					req.ID, number, version, number, version, number-1, from, otherAddr))
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

// nodeHash mirrors the hashes the fake node serves, so tests can seed a
// checkpoint the chain will actually agree with.
func nodeHash(version int, number uint64) string {
	return fmt.Sprintf("0x%d%02x", version, number)
}

func watchedRange(from, to uint64) map[uint64]bool {
	out := map[uint64]bool{}
	for n := from; n <= to; n++ {
		out[n] = true
	}
	return out
}

func newPipeline(t *testing.T, n *node, pub publisher.Publisher, opts Options) (*Pipeline, *checkpoint.Fake) {
	t.Helper()
	set, err := addresses.Load(strings.NewReader("userId,address\n7," + watchedAddr + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	// A poll interval long enough that Run never gets a second round: every test
	// here is about the first pass over a fixed range.
	cp := checkpoint.NewFake()
	return New(n.start(t), matcher.New(set), pub, cp, opts, discard()), cp
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
	p, _ := newPipeline(t, n, fake, sequential)

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

	p, cp := newPipeline(t, n, fake, sequential)
	err := p.Run(context.Background(), 100)
	if err == nil || !strings.Contains(err.Error(), "publishing blocks 102") {
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
	// The checkpoint must not have moved past what was actually published: a
	// restart has to come back to 102, not step over it.
	saved := cp.Saved()
	if len(saved) != 2 || saved[len(saved)-1].Block != 101 {
		t.Errorf("checkpoints saved = %+v, want the last one to be block 101", saved)
	}
}

func TestRunStopsOnCancellation(t *testing.T) {
	n := &node{head: 100, watchedIn: map[uint64]bool{}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	p, _ := newPipeline(t, n, publisher.NewFake(), sequential)
	if err := p.Run(ctx, 100); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() = %v, want context.Canceled", err)
	}
}

func TestRunPublishesNothingWhenNoAddressMatches(t *testing.T) {
	n := &node{head: 102, watchedIn: map[uint64]bool{}}
	fake := publisher.NewFake()
	p, _ := newPipeline(t, n, fake, sequential)

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
	p, _ := newPipeline(t, n, fake, Options{Poll: time.Hour, Workers: 4, BatchSize: 1})

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
	p, _ := newPipeline(t, n, fake, Options{Poll: time.Hour, Workers: 4, BatchSize: 1})

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

// A restart must come back to where it left off, not to where configuration
// says to start. Getting this backwards means either replaying from scratch or,
// far worse, jumping to the head and never looking at the gap.
func TestResumesFromTheCheckpointRatherThanTheFallback(t *testing.T) {
	watched := map[uint64]bool{}
	for n := uint64(100); n <= 105; n++ {
		watched[n] = true
	}
	n := &node{head: 105, watchedIn: watched}
	fake := publisher.NewFake()
	p, cp := newPipeline(t, n, fake, sequential)
	cp.Seed(checkpoint.Checkpoint{Block: 102, Hash: nodeHash(0, 102)})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx, 100) }() // fallback says 100, checkpoint says 103

	waitFor(t, "the remaining blocks", func() bool { return len(fake.Events()) == 3 })
	cancel()
	<-done

	if want := []uint64{103, 104, 105}; !equal(n.blocksAsked(), want) {
		t.Errorf("blocks asked = %v, want %v — 100-102 were already published", n.blocksAsked(), want)
	}
}

// One acknowledgement per chunk, not per block. The barrier and the checkpoint
// window are the same thing, so paying for one per block buys only latency.
func TestPublishesOneBatchPerChunk(t *testing.T) {
	watched := map[uint64]bool{}
	for n := uint64(100); n <= 119; n++ {
		watched[n] = true
	}
	n := &node{head: 119, watchedIn: watched}
	fake := publisher.NewFake()
	p, cp := newPipeline(t, n, fake, Options{Poll: time.Hour, Workers: 2, BatchSize: 5})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx, 100) }()

	waitFor(t, "all twenty blocks", func() bool { return len(fake.Events()) == 20 })
	cancel()
	<-done

	// 20 blocks in chunks of 5: four calls, not twenty.
	if got := fake.Calls(); got != 4 {
		t.Errorf("Publish called %d times for 20 blocks in chunks of 5, want 4", got)
	}
	saved := cp.Saved()
	if len(saved) != 4 {
		t.Fatalf("saved %d checkpoints, want one per chunk", len(saved))
	}
	// Each checkpoint names the last block of its chunk, and they advance in order.
	for i, want := range []uint64{104, 109, 114, 119} {
		if saved[i].Block != want {
			t.Errorf("checkpoint %d = block %d, want %d", i, saved[i].Block, want)
		}
	}
}

// The checkpoint may only move after the broker has acknowledged. If it moved
// first, an interruption in between would leave it claiming work that was never
// published, and those transactions would never be revisited.
func TestCheckpointDoesNotMoveWhenPublishFails(t *testing.T) {
	watched := map[uint64]bool{}
	for n := uint64(100); n <= 109; n++ {
		watched[n] = true
	}
	n := &node{head: 109, watchedIn: watched}
	fake := publisher.NewFake()
	fake.FailAfter(5, errors.New("broker gone"))
	p, cp := newPipeline(t, n, fake, Options{Poll: time.Hour, Workers: 1, BatchSize: 5})

	if err := p.Run(context.Background(), 100); err == nil {
		t.Fatal("want an error")
	}
	saved := cp.Saved()
	if len(saved) != 1 || saved[0].Block != 104 {
		t.Fatalf("checkpoints = %+v, want only the first chunk (block 104)", saved)
	}
}

// Blocks near the head can still be replaced, so they are left alone until they
// are ConfirmDepth deep.
func TestConfirmDepthStaysBehindTheHead(t *testing.T) {
	n := &node{head: 110, watchedIn: watchedRange(100, 110)}
	fake := publisher.NewFake()
	p, _ := newPipeline(t, n, fake, Options{Poll: time.Hour, Workers: 1, BatchSize: 1, ConfirmDepth: 3})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx, 100) }()

	waitFor(t, "the confirmed blocks", func() bool { return len(fake.Events()) == 8 })
	cancel()
	<-done

	// 110 minus a depth of 3 leaves 107 as the last block that may be published.
	if want := []uint64{100, 101, 102, 103, 104, 105, 106, 107}; !equal(n.blocksAsked(), want) {
		t.Errorf("blocks asked = %v, want %v", n.blocksAsked(), want)
	}
}

// A block whose parent is not what we published belongs to a different chain.
// Publishing it would credit users for transactions that no longer happened, so
// the whole chunk is refused and the pipeline goes back.
func TestReorgIsDetectedAndRewound(t *testing.T) {
	n := &node{head: 140, watchedIn: watchedRange(100, 140), forkAt: 5}
	fake := publisher.NewFake()
	p, cp := newPipeline(t, n, fake, Options{Poll: time.Hour, Workers: 1, BatchSize: 1, ConfirmDepth: 0})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx, 100) }()

	// The fork lands after five blocks, so the pipeline must go back before 105
	// and come out the other side still reaching the head.
	waitFor(t, "the run to reach the head", func() bool {
		saved := cp.Saved()
		return len(saved) > 0 && saved[len(saved)-1].Block == 140
	})
	cancel()
	<-done

	asked := n.blocksAsked()
	var rewound bool
	for i := 1; i < len(asked); i++ {
		if asked[i] < asked[i-1] {
			rewound = true
			if asked[i] > 104 {
				t.Errorf("rewound only to %d, want to before the fork at 105", asked[i])
			}
		}
	}
	if !rewound {
		t.Fatalf("never went back; blocks asked = %v", asked)
	}
	if len(fake.Events()) == 0 {
		t.Error("published nothing at all")
	}
}

// A chain that will not link up is broken, not reorganising. Rewinding forever
// would look like a service that is running.
func TestGivesUpAfterRepeatedReorgs(t *testing.T) {
	n := &node{head: 200, watchedIn: watchedRange(100, 200), chaos: true}
	p, _ := newPipeline(t, n, publisher.NewFake(), Options{Poll: time.Hour, Workers: 1, BatchSize: 4, ConfirmDepth: 0})

	err := p.Run(context.Background(), 100)
	if err == nil {
		t.Fatal("want an error rather than rewinding forever")
	}
	if !strings.Contains(err.Error(), "not linking up") {
		t.Errorf("error = %v, want it to say the chain never settled", err)
	}
}

// The hash stored with the checkpoint exists for this: a reorg that happened
// while the service was down has to be caught on the way back up, not assumed
// away.
func TestReorgWhileDownIsCaughtOnResume(t *testing.T) {
	n := &node{head: 140, watchedIn: watchedRange(100, 140)}
	n.version = 1 // the chain moved on while we were away
	fake := publisher.NewFake()
	p, cp := newPipeline(t, n, fake, Options{Poll: time.Hour, Workers: 1, BatchSize: 1, ConfirmDepth: 0})
	// Recorded against the chain as it was before the fork.
	cp.Seed(checkpoint.Checkpoint{Block: 120, Hash: nodeHash(0, 120)})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx, 100) }()

	waitFor(t, "the rewind", func() bool {
		asked := n.blocksAsked()
		return len(asked) > 1 && asked[len(asked)-1] < asked[0]
	})
	cancel()
	<-done

	asked := n.blocksAsked()
	if asked[0] != 121 {
		t.Fatalf("resumed at %d, want 121", asked[0])
	}
	// 121 does not link to the stored hash, so it goes back rather than
	// publishing a block from a chain that no longer exists.
	if got := asked[1]; got > 105 {
		t.Errorf("after the failed link it asked for %d, want a rewind below 105", got)
	}
}
