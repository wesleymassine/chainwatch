// Package publisher writes matched events to Kafka.
package publisher

import (
	"context"

	"github.com/wesleymassine/chainwatch/internal/matcher"
)

// Publisher is the seam between the pipeline and Kafka.
//
// Publish must not return until the broker has acknowledged every event. That
// is the whole at-least-once contract: the caller is only allowed to advance its
// checkpoint after this returns nil. Acknowledging first and publishing later
// would lose transactions on an interruption, silently.
type Publisher interface {
	Publish(ctx context.Context, events []matcher.Event) error
	Close() error
}
