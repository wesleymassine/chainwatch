// Package checkpoint remembers how far the service has got, so that an
// interruption costs duplicates rather than missed transactions.
package checkpoint

import "context"

// Checkpoint is the last block whose events are known to be in Kafka.
//
// The hash travels with the number because a number alone does not identify a
// block. After a reorg the same number can name different content, and the hash
// is how we notice.
type Checkpoint struct {
	Block uint64 `json:"blockNumber"`
	Hash  string `json:"blockHash"`
}

// Store persists the checkpoint somewhere that survives the process.
//
// Local disk would not: the service is meant to run on spot instances, which
// lose their disk when they are reclaimed. Kafka is already a dependency and is
// already durable, so the checkpoint lives there.
type Store interface {
	// Load returns the stored checkpoint. The bool is false when there is none
	// yet, which is different from an error: a first run starts from scratch,
	// but a failed read must never be mistaken for one.
	Load(ctx context.Context) (Checkpoint, bool, error)

	// Save records progress. It must only ever be called after the events for
	// that block have been acknowledged by the broker.
	Save(ctx context.Context, cp Checkpoint) error

	Close() error
}
