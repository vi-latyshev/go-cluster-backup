package engine

import (
	"context"
	"sync"
	"time"
)

// storageClock is one storage's time budget within ONE run: the budget covers
// the WALL TIME the engine has at least one call in flight to that storage, so a
// slow dump or compression never eats the upload budget, and it is created per
// run, so a second Run in one process starts with a full budget. Overlapping
// calls — parallel manifest reads, parallel parts — book their union once.
type storageClock struct {
	mu     sync.Mutex
	budget time.Duration
	spent  time.Duration
	// active counts the calls in flight and since is when the current busy
	// interval opened — the instant the union of the calls is measured from.
	active int
	since  time.Time
}

// begin opens one call's interval; the returned end closes it, and it is
// idempotent so a repeated call cannot move the open-call count below zero. The
// time is booked when the LAST call in flight ends: the union of the calls, not
// their sum.
func (c *storageClock) begin() (end func()) {
	c.mu.Lock()
	if c.active == 0 {
		c.since = time.Now()
	}
	c.active++
	c.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			c.mu.Lock()
			c.active--
			if c.active == 0 {
				c.spent += time.Since(c.since)
			}
			c.mu.Unlock()
		})
	}
}

// bound bounds one call by the budget that is left in the interval it belongs
// to: one absolute deadline for every call of a busy interval, measured from the
// interval's start, so a call that begins mid-interval does not get a fresh
// budget. An exhausted budget yields an already-expired context, and the call
// fails like any timeout. It is called on the interval begin opened.
func (c *storageClock) bound(parent context.Context) (context.Context, context.CancelFunc) {
	c.mu.Lock()
	start := c.since
	left := c.budget - c.spent
	c.mu.Unlock()
	return context.WithDeadline(parent, start.Add(left))
}

// total is the time this storage's calls have spent in this run — what the
// report carries as that storage's duration. An interval still open counts as
// time spent so far.
func (c *storageClock) total() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	d := c.spent
	if c.active > 0 {
		d += time.Since(c.since)
	}
	return d
}

// storageCall runs one storage operation inside the budget that is left and
// books the time it took.
func storageCall[T any](clock *storageClock, parent context.Context, fn func(ctx context.Context) (T, error)) (T, error) {
	end := clock.begin()
	defer end()
	ctx, cancel := clock.bound(parent)
	defer cancel()
	return fn(ctx)
}
