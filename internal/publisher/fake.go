package publisher

import (
	"context"
	"errors"
	"sync"

	"github.com/wesleymassine/chainwatch/internal/matcher"
)

// Fake is an in-memory Publisher for tests. It is safe for concurrent use,
// because the pipeline publishes from more than one goroutine.
type Fake struct {
	mu        sync.Mutex
	events    []matcher.Event
	calls     int
	failAfter int
	failErr   error
	closed    bool
}

func NewFake() *Fake { return &Fake{} }

// FailAfter makes Publish start failing once n events have been recorded. It is
// how the crash tests simulate a broker that stops acknowledging: the pipeline
// must then refuse to advance its checkpoint.
func (f *Fake) FailAfter(n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failAfter, f.failErr = n, err
}

func (f *Fake) Publish(ctx context.Context, events []matcher.Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return errors.New("publish after close")
	}
	// Nothing is recorded on failure: an unacknowledged batch must look exactly
	// like a batch that never happened, or the tests would prove the wrong thing.
	if f.failErr != nil && len(f.events) >= f.failAfter {
		return f.failErr
	}
	f.calls++
	f.events = append(f.events, events...)
	return nil
}

// Calls is how many times Publish succeeded. One call per chunk is the property
// that keeps the acknowledgement barrier as wide as the checkpoint window.
func (f *Fake) Calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// Events returns a copy of everything published so far.
func (f *Fake) Events() []matcher.Event {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]matcher.Event(nil), f.events...)
}

func (f *Fake) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}
