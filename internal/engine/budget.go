package engine

import (
	"context"
	"sync"
	"time"
)

// storageClock is one storage's time budget within ONE run: the budget covers
// the time the engine spends inside that storage's own calls, so a slow dump or
// compression never eats the upload budget, and it is created per run, so a
// second Run in one process starts with a full budget.
type storageClock struct {
	mu     sync.Mutex
	budget time.Duration
	spent  time.Duration
}

// call bounds one storage call by the budget that is left; an exhausted budget
// yields an already-expired context, and the call fails like any timeout.
func (c *storageClock) call(parent context.Context) (context.Context, context.CancelFunc) {
	c.mu.Lock()
	left := c.budget - c.spent
	c.mu.Unlock()
	return context.WithDeadline(parent, time.Now().Add(left))
}

// add books the time one call took against the budget.
func (c *storageClock) add(d time.Duration) {
	c.mu.Lock()
	c.spent += d
	c.mu.Unlock()
}

// total is the time this storage's calls have spent in this run — what the
// report carries as that storage's duration.
func (c *storageClock) total() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.spent
}

// storageCall runs one storage operation inside the budget that is left and
// books the time it took.
func storageCall[T any](clock *storageClock, parent context.Context, fn func(ctx context.Context) (T, error)) (T, error) {
	start := time.Now()
	ctx, cancel := clock.call(parent)
	defer cancel()
	v, err := fn(ctx)
	clock.add(time.Since(start))
	return v, err
}
