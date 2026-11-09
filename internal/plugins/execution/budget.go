package execution

import (
	"context"
	"sync"
	"time"
)

// Clock is one storage's time budget within ONE run: the budget covers the WALL
// TIME the run has at least one call in flight to that storage, so a slow dump
// or compression never eats the upload budget, and it is created per run, so a
// second run in one process starts with a full budget. Overlapping calls —
// parallel manifest reads, parallel parts — book their union once.
type Clock struct {
	mu     sync.Mutex
	budget time.Duration
	spent  time.Duration
	// active counts the calls in flight and since is when the current busy
	// interval opened — the instant the union of the calls is measured from.
	active int
	since  time.Time
}

// NewClock returns the clock of one storage with the configured budget.
func NewClock(budget time.Duration) *Clock { return &Clock{budget: budget} }

// Begin opens one call's interval; the returned end closes it, and it is
// idempotent so a repeated call cannot move the open-call count below zero. The
// time is booked when the LAST call in flight ends: the union of the calls, not
// their sum. A caller that must write what the budget would refuse — this run's
// manifest — opens an interval with Begin and skips Bound.
func (c *Clock) Begin() (end func()) {
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

// Bound bounds one call by the budget that is left in the interval it belongs
// to: one absolute deadline for every call of a busy interval, measured from the
// interval's start, so a call that begins mid-interval does not get a fresh
// budget. An exhausted budget yields an already-expired context, and the call
// fails like any timeout. It is called on the interval Begin opened.
func (c *Clock) Bound(parent context.Context) (context.Context, context.CancelFunc) {
	c.mu.Lock()
	start := c.since
	left := c.budget - c.spent
	c.mu.Unlock()
	return context.WithDeadline(parent, start.Add(left))
}

// Total is the time this storage's calls have spent in this run — what the
// report carries as that storage's duration. An interval still open counts as
// time spent so far.
func (c *Clock) Total() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	d := c.spent
	if c.active > 0 {
		d += time.Since(c.since)
	}
	return d
}

// Call runs one storage operation inside the budget that is left and books the
// time it took.
func Call[T any](clock *Clock, parent context.Context, fn func(ctx context.Context) (T, error)) (T, error) {
	end := clock.Begin()
	defer end()
	ctx, cancel := clock.Bound(parent)
	defer cancel()
	return fn(ctx)
}
