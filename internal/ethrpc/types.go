package ethrpc

import (
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"

	"github.com/wesleymassine/chainwatch/internal/addresses"
)

// Block is the part of a block this service cares about.
type Block struct {
	Number     uint64
	Hash       string
	ParentHash string
	Txs        []Tx
}

// Tx is the part of a transaction this service cares about.
//
// Note what is missing: the transaction type. We never look at it, so a chain
// can introduce a new one without breaking us. That matters more than it sounds
// — Arbitrum emits one 0x6a system transaction in every single block, and
// mainnet carries 0x3 blob and 0x4 set-code transactions in ordinary ones. A
// decoder built on a fixed set of transaction types rejects those and the usual
// reaction is to skip the whole block, which loses real user deposits.
//
// Note what is also missing: the signature fields. The node already tells us who
// sent the transaction, so we never recover the sender from the signature. That
// recovery costs 50-100µs per transaction; reading the field costs nothing.
type Tx struct {
	Hash  string
	From  addresses.Address
	To    *addresses.Address // nil when the transaction creates a contract
	Value *big.Int           // wei
}

func (b *Block) UnmarshalJSON(data []byte) error {
	var raw struct {
		Number       string `json:"number"`
		Hash         string `json:"hash"`
		ParentHash   string `json:"parentHash"`
		Transactions []Tx   `json:"transactions"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	n, err := parseHexUint64(raw.Number)
	if err != nil {
		return fmt.Errorf("block number: %w", err)
	}
	b.Number, b.Hash, b.ParentHash, b.Txs = n, raw.Hash, raw.ParentHash, raw.Transactions
	return nil
}

func (t *Tx) UnmarshalJSON(data []byte) error {
	var raw struct {
		Hash  string  `json:"hash"`
		From  string  `json:"from"`
		To    *string `json:"to"`
		Value string  `json:"value"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	from, err := addresses.ParseAddress([]byte(raw.From))
	if err != nil {
		return fmt.Errorf("tx %s: from: %w", raw.Hash, err)
	}

	// to stays a local until the end. Assigning it in place would leave a
	// previous recipient behind when this decodes a contract creation into a
	// reused Tx — which in this service means crediting the wrong user.
	var to *addresses.Address
	if raw.To != nil {
		parsed, err := addresses.ParseAddress([]byte(*raw.To))
		if err != nil {
			return fmt.Errorf("tx %s: to: %w", raw.Hash, err)
		}
		to = &parsed
	}

	// big.Int.SetString accepts a leading sign, so "0x-1" would decode to a
	// negative amount and travel on looking perfectly valid. The value comes
	// from a public node we do not control, which is exactly the input worth
	// distrusting.
	value, ok := new(big.Int).SetString(trimHexPrefix(raw.Value), 16)
	if !ok || value.Sign() < 0 {
		return fmt.Errorf("tx %s: bad value %q", raw.Hash, raw.Value)
	}

	t.Hash, t.From, t.To, t.Value = raw.Hash, from, to, value
	return nil
}

func parseHexUint64(s string) (uint64, error) {
	return strconv.ParseUint(trimHexPrefix(s), 16, 64)
}

func trimHexPrefix(s string) string {
	if len(s) >= 2 && s[0] == '0' && (s[1] == 'x' || s[1] == 'X') {
		return s[2:]
	}
	return s
}
