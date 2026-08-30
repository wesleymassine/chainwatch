package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wesleymassine/chainwatch/internal/addresses"
	"github.com/wesleymassine/chainwatch/internal/checkpoint"
	"github.com/wesleymassine/chainwatch/internal/ethrpc"
	"github.com/wesleymassine/chainwatch/internal/matcher"
	"github.com/wesleymassine/chainwatch/internal/publisher"
)

// busyNode serves blocks shaped like mainnet: a few hundred transactions, a
// small share of them belonging to watched users.
//
// It runs in the same process on purpose. Against a public endpoint the network
// is the ceiling — 76 blocks/s on mainnet, 8 on Arbitrum — so measuring there
// measures the provider. The brief says to assume an unrestricted node, and this
// is what that assumption looks like.
type busyNode struct {
	head     uint64
	txs      string // pre-rendered transaction list, identical in every block
	requests atomic.Int64
	blocks   atomic.Int64
}

func newBusyNode(head uint64, txPerBlock, watchedEvery int) *busyNode {
	var b strings.Builder
	for i := 0; i < txPerBlock; i++ {
		from, to := otherAddr, otherAddr
		if watchedEvery > 0 && i%watchedEvery == 0 {
			from = watchedAddr
		}
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"hash":"0x%064x","from":%q,"to":%q,"value":"0xde0b6b3a7640000"}`, i, from, to)
	}
	return &busyNode{head: head, txs: b.String()}
}

func (n *busyNode) start(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n.requests.Add(1)
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
		var out strings.Builder
		out.WriteByte('[')
		for i, req := range reqs {
			if i > 0 {
				out.WriteByte(',')
			}
			switch req.Method {
			case "eth_blockNumber":
				fmt.Fprintf(&out, `{"id":%d,"result":"0x%x"}`, req.ID, n.head)
			case "eth_getBlockByNumber":
				var number uint64
				fmt.Sscanf(req.Params[0].(string), "0x%x", &number)
				n.blocks.Add(1)
				fmt.Fprintf(&out,
					`{"id":%d,"result":{"number":"0x%x","hash":"0x%016x","parentHash":"0x%016x","transactions":[%s]}}`,
					req.ID, number, number, number-1, n.txs)
			}
		}
		out.WriteByte(']')
		io.WriteString(w, out.String())
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func ethrpcClient(t *testing.T, url string) *ethrpc.Client {
	t.Helper()
	return ethrpc.New(url, discard())
}

// watchedSet is the real thing at real size when the dataset exists, so the
// matcher is measured against 500k entries rather than a toy map.
func watchedSet(t testing.TB) *addresses.Set {
	t.Helper()
	if f, err := os.Open("../../testdata/addresses.csv"); err == nil {
		defer f.Close()
		set, err := addresses.Load(f)
		if err != nil {
			t.Fatal(err)
		}
		return set
	}
	set, err := addresses.Load(strings.NewReader("userId,address\n7," + watchedAddr + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	return set
}

// Every block between the first and the last must be accounted for. A hole is
// the failure this service exists to prevent, and it is invisible unless
// something counts.
func TestEndToEndLeavesNoGaps(t *testing.T) {
	const from, to = 1_000_000, 1_000_499
	node := newBusyNode(to, 50, 5)
	pub, cp := publisher.NewFake(), checkpoint.NewFake()

	p := New(ethrpcClient(t, node.start(t)), matcher.New(watchedSet(t)), pub, cp,
		Options{Poll: time.Hour, Workers: 8, BatchSize: 20, ConfirmDepth: 0}, discard())

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	runUntil(t, p, ctx, from, func() bool {
		saved := cp.Saved()
		return len(saved) > 0 && saved[len(saved)-1].Block == to
	})

	seen := map[uint64]bool{}
	for _, e := range pub.Events() {
		seen[e.BlockNumber] = true
	}
	for n := uint64(from); n <= to; n++ {
		if !seen[n] {
			t.Fatalf("block %d never produced an event, and every block here has matches", n)
		}
	}
	if got := cp.Saved()[len(cp.Saved())-1].Block; got != to {
		t.Errorf("final checkpoint = %d, want %d", got, to)
	}
}

// The spot instance requirement, end to end: kill the service mid-run, start a
// new one against the same checkpoint, and no block may be missing afterwards.
func TestSurvivesRestartWithoutLosingBlocks(t *testing.T) {
	const from, to = 2_000_000, 2_000_399
	node := newBusyNode(to, 50, 5)
	url := node.start(t)
	pub, cp := publisher.NewFake(), checkpoint.NewFake()
	opts := Options{Poll: time.Hour, Workers: 4, BatchSize: 20, ConfirmDepth: 0}

	// First life: stopped as soon as it has made some progress.
	first := New(ethrpcClient(t, url), matcher.New(watchedSet(t)), pub, cp, opts, discard())
	ctx1, cancel1 := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- first.Run(ctx1, from) }()
	waitFor(t, "some progress before the kill", func() bool { return len(cp.Saved()) >= 3 })
	cancel1()
	<-done

	killedAt := cp.Saved()[len(cp.Saved())-1].Block
	if killedAt >= to {
		t.Fatal("finished before it could be interrupted; nothing was tested")
	}

	// Second life: a fresh pipeline, the same stores, no configured start.
	second := New(ethrpcClient(t, url), matcher.New(watchedSet(t)), pub, cp, opts, discard())
	ctx2, cancel2 := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel2()
	runUntil(t, second, ctx2, 0, func() bool {
		saved := cp.Saved()
		return len(saved) > 0 && saved[len(saved)-1].Block == to
	})

	counts := map[uint64]int{}
	for _, e := range pub.Events() {
		counts[e.BlockNumber]++
	}
	for n := uint64(from); n <= to; n++ {
		if counts[n] == 0 {
			t.Fatalf("block %d was lost across the restart", n)
		}
	}
	t.Logf("interrupted after block %d, resumed and reached %d with no gaps", killedAt, to)
}

// runUntil drives a pipeline until done reports true, then stops it. The
// deadline comes from ctx: these runs move real volume and are slower under
// -race than the unit tests waitFor was written for.
func runUntil(t *testing.T, p *Pipeline, ctx context.Context, from uint64, done func() bool) {
	t.Helper()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	errs := make(chan error, 1)
	go func() { errs <- p.Run(ctx, from) }()
	for !done() {
		if ctx.Err() != nil {
			t.Fatalf("run did not finish before the deadline")
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-errs
}

// TestThroughput reports what the pipeline does when the node is not the
// constraint, which is the condition the brief asks us to assume.
//
// It is the counterpart to the public-endpoint measurements in
// notes/03-rpc-probe.md: there, throughput flattens at 76 blocks/s on mainnet
// and 8 on Arbitrum no matter how many workers are added, because the provider
// saturates. Here the node is in-process, so what the numbers describe is the
// service.
func TestThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("throughput measurement is slow by nature")
	}
	const (
		blocks     = 500
		txPerBlock = 300 // a busy mainnet block
		from       = uint64(3_000_000)
	)
	node := newBusyNode(from+blocks-1, txPerBlock, 50)
	url := node.start(t)
	watched := watchedSet(t)
	t.Logf("watching %d addresses, %d blocks of %d transactions each",
		watched.Len(), blocks, txPerBlock)

	for _, workers := range []int{1, 4, 8, 16} {
		pub, cp := publisher.NewFake(), checkpoint.NewFake()
		p := New(ethrpcClient(t, url), matcher.New(watched), pub, cp,
			Options{Poll: time.Hour, Workers: workers, BatchSize: 20, ConfirmDepth: 0}, discard())

		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		start := time.Now()
		runUntil(t, p, ctx, from, func() bool {
			saved := cp.Saved()
			return len(saved) > 0 && saved[len(saved)-1].Block == from+blocks-1
		})
		elapsed := time.Since(start)
		cancel()

		events := len(pub.Events())
		t.Logf("workers=%-3d %7.0f blocks/s %9.0f txs/s %8.0f events/s (%d events in %v)",
			workers,
			float64(blocks)/elapsed.Seconds(),
			float64(blocks*txPerBlock)/elapsed.Seconds(),
			float64(events)/elapsed.Seconds(),
			events, elapsed.Round(time.Millisecond))
	}
}
