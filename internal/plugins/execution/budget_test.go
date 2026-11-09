package execution

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

// TestStorageClockBooksOverlappingCallsOnce (A1, C1): two calls that overlap
// spend the union of their intervals, not their sum — the time a storage spends
// with a call in flight is what the budget pays for, once.
func TestStorageClockBooksOverlappingCallsOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := NewClock(time.Second)
		end := c.Begin()
		done := make(chan struct{})
		go func() {
			defer close(done)
			time.Sleep(10 * time.Millisecond)
			second := c.Begin()
			time.Sleep(40 * time.Millisecond)
			second()
		}()
		time.Sleep(40 * time.Millisecond)
		end()
		<-done
		if got := c.Total(); got != 50*time.Millisecond {
			t.Fatalf("total = %s, want 50ms: the calls overlap from 10ms to 40ms, and the union is 50ms, not the 80ms sum", got)
		}
	})
}

// TestStorageClockBoundSharesOneDeadline (A1, C1): every call of one busy
// interval runs against the same absolute deadline — the interval's start plus
// what was left of the budget — so a call that starts mid-interval does not get
// a fresh budget.
func TestStorageClockBoundSharesOneDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := NewClock(60 * time.Millisecond)
		end := c.Begin()
		defer end()
		time.Sleep(30 * time.Millisecond)
		ctx, cancel := c.Bound(context.Background())
		defer cancel()
		want := time.Now().Add(30 * time.Millisecond)
		deadline, ok := ctx.Deadline()
		if !ok || !deadline.Equal(want) {
			t.Fatalf("deadline = %s (set=%v), want the interval's one %s", deadline, ok, want)
		}
	})
}

// TestStorageClockEndIsIdempotent (A2, C1): a repeated end books the interval
// once and never pushes the open-call count below zero — the clock keeps
// counting the calls that follow.
func TestStorageClockEndIsIdempotent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := NewClock(time.Second)
		end := c.Begin()
		time.Sleep(10 * time.Millisecond)
		end()
		end()
		if got := c.Total(); got != 10*time.Millisecond {
			t.Fatalf("total after a repeated end = %s, want 10ms booked once", got)
		}
		end = c.Begin()
		time.Sleep(10 * time.Millisecond)
		end()
		if got := c.Total(); got != 20*time.Millisecond {
			t.Fatalf("total after the next call = %s, want 20ms: the repeated end moved the open-call count", got)
		}
	})
}

// TestStorageClockTotalIncludesTheOpenInterval (C1): a total read while a call
// is still in flight already counts its elapsed time — the budget and the report
// never disagree about what the storage spent so far.
func TestStorageClockTotalIncludesTheOpenInterval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := NewClock(time.Second)
		end := c.Begin()
		time.Sleep(10 * time.Millisecond)
		if got := c.Total(); got != 10*time.Millisecond {
			t.Fatalf("total during an open interval = %s, want 10ms", got)
		}
		end()
		if got := c.Total(); got != 10*time.Millisecond {
			t.Fatalf("total after the interval closed = %s, want 10ms", got)
		}
	})
}

// TestNewClockCarriesTheBudgetIntoTheBound: the clock is built from the typed
// budget alone — no config object reaches it — and that budget is what the
// call's deadline is made of.
func TestNewClockCarriesTheBudgetIntoTheBound(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := NewClock(70 * time.Millisecond)
		end := c.Begin()
		defer end()
		time.Sleep(20 * time.Millisecond)
		ctx, cancel := c.Bound(context.Background())
		defer cancel()
		deadline, ok := ctx.Deadline()
		if !ok || !deadline.Equal(time.Now().Add(50*time.Millisecond)) {
			t.Fatalf("deadline = %s (set=%v), want the 70ms budget minus the 20ms already spent", deadline, ok)
		}
	})
}

// TestCallBoundsTheOperationAndBooksItsTime: one storage call runs against the
// budget that is left and its time lands in the total the report carries.
func TestCallBoundsTheOperationAndBooksItsTime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := NewClock(100 * time.Millisecond)
		got, err := Call(c, context.Background(), func(ctx context.Context) (int, error) {
			deadline, ok := ctx.Deadline()
			if !ok || !deadline.Equal(time.Now().Add(100*time.Millisecond)) {
				t.Fatalf("deadline = %s (set=%v), want the full 100ms budget", deadline, ok)
			}
			time.Sleep(30 * time.Millisecond)
			return 7, nil
		})
		if err != nil || got != 7 {
			t.Fatalf("call = %d, %v, want the value", got, err)
		}
		if got := c.Total(); got != 30*time.Millisecond {
			t.Fatalf("total = %s, want the 30ms the call took", got)
		}
	})
}

// TestCallFailsWhenTheBudgetIsSpent: the budget the config names is still a
// budget — a call that starts on a spent clock sees an expired context and
// fails like any timeout.
func TestCallFailsWhenTheBudgetIsSpent(t *testing.T) {
	c := NewClock(0)
	_, err := Call(c, context.Background(), func(ctx context.Context) (struct{}, error) {
		return struct{}{}, ctx.Err()
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want the exhausted budget's deadline", err)
	}
}
