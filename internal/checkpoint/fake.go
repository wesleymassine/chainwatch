package checkpoint

import (
	"context"
	"sync"
)

// Fake is an in-memory Store for tests.
type Fake struct {
	mu      sync.Mutex
	saved   []Checkpoint
	initial *Checkpoint
	saveErr error
	failAt  int
}

func NewFake() *Fake { return &Fake{} }

// Seed makes Load report an existing checkpoint, as a restart would find.
func (f *Fake) Seed(cp Checkpoint) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.initial = &cp
}

// FailAfter makes Save start failing once n checkpoints have been recorded, so
// a test can cut the service off between one chunk and the next.
func (f *Fake) FailAfter(n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failAt, f.saveErr = n, err
}

func (f *Fake) Load(ctx context.Context) (Checkpoint, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.saved) > 0 {
		return f.saved[len(f.saved)-1], true, nil
	}
	if f.initial != nil {
		return *f.initial, true, nil
	}
	return Checkpoint{}, false, nil
}

func (f *Fake) Save(ctx context.Context, cp Checkpoint) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.saveErr != nil && len(f.saved) >= f.failAt {
		return f.saveErr
	}
	f.saved = append(f.saved, cp)
	return nil
}

// Saved returns every checkpoint recorded, in order.
func (f *Fake) Saved() []Checkpoint {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]Checkpoint(nil), f.saved...)
}

func (f *Fake) Close() error { return nil }
