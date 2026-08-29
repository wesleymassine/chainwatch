package matcher

import (
	"encoding/json"
	"math/big"
	"strings"
	"testing"

	"github.com/wesleymassine/chainwatch/internal/addresses"
	"github.com/wesleymassine/chainwatch/internal/ethrpc"
)

const (
	alice    = "0x28c6c06298d514db089934071355e5743bf21d60" // user 1
	bob      = "0x05ff6964d21e5dae3b1010d5ae0465b3c450f381" // user 2
	aliceAlt = "0xdfd5293d8e347dfe59e90efd55b2956a1343963d" // user 1 as well
	stranger = "0x1111111111111111111111111111111111111111"
)

func testMatcher(t *testing.T) *Matcher {
	t.Helper()
	set, err := addresses.Load(strings.NewReader(
		"userId,address\n1," + alice + "\n2," + bob + "\n1," + aliceAlt + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	return New(set)
}

func addr(t *testing.T, s string) addresses.Address {
	t.Helper()
	a, err := addresses.ParseAddress([]byte(s))
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func block(t *testing.T, from string, to *string, wei string) *ethrpc.Block {
	t.Helper()
	value, ok := new(big.Int).SetString(wei, 10)
	if !ok {
		t.Fatalf("bad wei %q", wei)
	}
	tx := ethrpc.Tx{Hash: "0xdead", From: addr(t, from), Value: value}
	if to != nil {
		a := addr(t, *to)
		tx.To = &a
	}
	return &ethrpc.Block{Number: 25854432, Hash: "0xaa", ParentHash: "0xbb",
		Txs: []ethrpc.Tx{tx}}
}

func ptr(s string) *string { return &s }

func TestBlock(t *testing.T) {
	tests := []struct {
		name string
		from string
		to   *string
		want []uint64 // userIds, in order
	}{
		{name: "sender is watched", from: alice, to: ptr(stranger), want: []uint64{1}},
		{name: "recipient is watched", from: stranger, to: ptr(bob), want: []uint64{2}},
		{name: "both sides watched yields one event each", from: alice, to: ptr(bob), want: []uint64{1, 2}},
		{name: "same user on both sides yields one event", from: alice, to: ptr(aliceAlt), want: []uint64{1}},
		{name: "self transfer yields one event", from: alice, to: ptr(alice), want: []uint64{1}},
		{name: "nobody watched", from: stranger, to: ptr(stranger), want: nil},
		{name: "contract creation by a watched sender", from: alice, to: nil, want: []uint64{1}},
		{name: "contract creation by a stranger", from: stranger, to: nil, want: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := testMatcher(t).Block(block(t, tt.from, tt.to, "1000000000000000000"))
			if len(got) != len(tt.want) {
				t.Fatalf("got %d events, want %d: %+v", len(got), len(tt.want), got)
			}
			for i, id := range tt.want {
				if got[i].UserID != id {
					t.Errorf("events[%d].UserID = %d, want %d", i, got[i].UserID, id)
				}
			}
		})
	}
}

// The reason amount is a string. 10 ETH in wei is past 2^53, where a float64
// silently rounds; if this ever regresses, the ledger is wrong and nothing warns.
func TestAmountSurvivesLargeValues(t *testing.T) {
	const tenETH = "10000000000000000000"
	events := testMatcher(t).Block(block(t, alice, ptr(stranger), tenETH))
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
	if events[0].Amount != tenETH {
		t.Fatalf("Amount = %s, want %s", events[0].Amount, tenETH)
	}

	data, err := json.Marshal(events[0])
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"amount":"`+tenETH+`"`) {
		t.Fatalf("amount must be a quoted decimal string, got %s", data)
	}

	var round Event
	if err := json.Unmarshal(data, &round); err != nil {
		t.Fatal(err)
	}
	if round.Amount != tenETH {
		t.Fatalf("after a round trip Amount = %s, want %s", round.Amount, tenETH)
	}
}

func TestEventShapeMatchesTheBrief(t *testing.T) {
	events := testMatcher(t).Block(block(t, alice, ptr(bob), "1"))
	data, err := json.Marshal(events[0])
	if err != nil {
		t.Fatal(err)
	}
	want := `{"userId":1,"from":"` + alice + `","to":"` + bob +
		`","amount":"1","hash":"0xdead","blockNumber":25854432}`
	if string(data) != want {
		t.Errorf("payload =\n  %s\nwant\n  %s", data, want)
	}
}

func TestContractCreationMarshalsToAsNull(t *testing.T) {
	events := testMatcher(t).Block(block(t, alice, nil, "0"))
	data, err := json.Marshal(events[0])
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"to":null`) {
		t.Errorf("payload = %s, want a null recipient", data)
	}
}

func BenchmarkBlock(b *testing.B) {
	set, err := addresses.Load(strings.NewReader("userId,address\n1," + alice + "\n"))
	if err != nil {
		b.Fatal(err)
	}
	m := New(set)

	// A block shaped like mainnet: a few hundred transactions, almost none of
	// which belong to us. The miss path is what runs millions of times.
	watched, stranger := mustAddr(b, alice), mustAddr(b, "0x1111111111111111111111111111111111111111")
	blk := &ethrpc.Block{Number: 1, Txs: make([]ethrpc.Tx, 300)}
	for i := range blk.Txs {
		to := stranger
		blk.Txs[i] = ethrpc.Tx{Hash: "0xdead", From: stranger, To: &to, Value: big.NewInt(1)}
	}
	blk.Txs[7].From = watched

	b.ReportAllocs()
	b.SetBytes(int64(len(blk.Txs)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if got := m.Block(blk); len(got) != 1 {
			b.Fatalf("got %d events, want 1", len(got))
		}
	}
}

func mustAddr(tb testing.TB, s string) addresses.Address {
	tb.Helper()
	a, err := addresses.ParseAddress([]byte(s))
	if err != nil {
		tb.Fatal(err)
	}
	return a
}

// (*big.Int)(nil).String() is "<nil>", not a panic. The decoder never produces a
// nil value, but nothing should be able to put that string in an amount field.
func TestAmountIsNeverTheStringNil(t *testing.T) {
	set, err := addresses.Load(strings.NewReader("1," + alice))
	if err != nil {
		t.Fatal(err)
	}
	blk := &ethrpc.Block{Number: 1, Txs: []ethrpc.Tx{{Hash: "0x1", From: addr(t, alice)}}}
	events := New(set).Block(blk)
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
	if events[0].Amount == "<nil>" {
		t.Fatalf("Amount = %q, which no consumer can parse", events[0].Amount)
	}
}
