// Package matcher turns blocks into the events this service publishes.
package matcher

import (
	"math/big"

	"github.com/wesleymassine/chainwatch/internal/addresses"
	"github.com/wesleymassine/chainwatch/internal/ethrpc"
)

// Event is the payload published to Kafka. The field names and their order come
// straight from the brief.
//
// Amount is a decimal string, not a number, and that is not a style choice. Ten
// ether is 10000000000000000000 wei, well past the 2^53 where a float64-backed
// JSON parser starts rounding. The value would arrive slightly wrong and nothing
// would warn us. In a ledger that is the worst kind of bug.
//
// To is a pointer so that a contract creation marshals as null rather than
// pretending the transaction went to the zero address.
type Event struct {
	UserID      uint64  `json:"userId"`
	From        string  `json:"from"`
	To          *string `json:"to"`
	Amount      string  `json:"amount"`
	Hash        string  `json:"hash"`
	BlockNumber uint64  `json:"blockNumber"`
}

// Matcher holds the watched set. It is read-only and safe for concurrent use.
type Matcher struct {
	watched *addresses.Set
}

func New(watched *addresses.Set) *Matcher {
	return &Matcher{watched: watched}
}

// Block returns one event for every user involved in the block.
//
// A transaction between two watched users produces two events, one per userId, so
// a consumer partitioned by user still sees both sides. A transaction where both
// ends belong to the same user produces one. Emitting the same event twice would
// be a duplicate we created on purpose, and at-least-once is a floor to respect,
// not an excuse.
func (m *Matcher) Block(b *ethrpc.Block) []Event {
	var events []Event
	for i := range b.Txs {
		tx := &b.Txs[i]

		sender, senderWatched := m.watched.Lookup(tx.From)
		var recipient uint64
		var recipientWatched bool
		if tx.To != nil {
			recipient, recipientWatched = m.watched.Lookup(*tx.To)
		}
		if !senderWatched && !recipientWatched {
			continue
		}

		// Built once per matched transaction, then shared by both events.
		from := tx.From.String()
		var to *string
		if tx.To != nil {
			s := tx.To.String()
			to = &s
		}
		event := Event{
			From:        from,
			To:          to,
			Amount:      weiString(tx.Value),
			Hash:        tx.Hash,
			BlockNumber: b.Number,
		}

		if senderWatched {
			event.UserID = sender
			events = append(events, event)
		}
		// The sender check has to come first: without it, an unwatched sender
		// leaves `sender` at its zero value and a user whose id is 0 would be
		// silently dropped.
		if recipientWatched && (!senderWatched || recipient != sender) {
			event.UserID = recipient
			events = append(events, event)
		}
	}
	return events
}

// weiString renders a transaction value.
//
// The decoder never yields a nil value. But (*big.Int)(nil).String() returns the
// text "<nil>" instead of panicking, so a hand-built Tx could put a string no
// consumer can parse into an amount field. Zero is the safe answer: an amount of
// 0 already means something here, a contract call that moved no ether.
func weiString(v *big.Int) string {
	if v == nil {
		return "0"
	}
	return v.String()
}
