package engine

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/model"
)

// budgetConfig is the run config with a storage budget short enough to witness:
// production longer than the budget must not cost the upload.
func budgetConfig(t *testing.T, w *world, budget string) (*Engine, map[string]any) {
	t.Helper()
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["storages"].(map[string]any)["primary"].(map[string]any)["time_budget"] = budget
	return serviceCheck(t, w, cfg), cfg
}

// TestStorageBudgetCountsOnlyStorageTime: the budget covers the time spent
// inside that storage's own calls — a dump slower than the budget still gets a
// full upload budget, and the report carries what the storage's calls took.
func TestStorageBudgetCountsOnlyStorageTime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newWorld(t)
		e, _ := budgetConfig(t, w, "1s")
		w.sources["vpn"].dumpDelay = 1500 * time.Millisecond

		rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
		if err != nil {
			t.Fatal(err)
		}
		if rep.Outcome != model.OutcomeDelivered {
			t.Fatalf("outcome %s: the dump outlasted the storage budget and cost the upload", rep.Outcome)
		}
		got := storageResult(rep, "primary")
		if got.Outcome != model.StorageVerified {
			t.Fatalf("storage %+v", got)
		}
		if got.Duration.Std() < 0 || got.Duration.Std() > 1500*time.Millisecond {
			t.Fatalf("the recorded storage duration is %s, which is not the storage's own time", got.Duration)
		}
	})
}

// TestStorageBudgetResetsPerRun: the clock lives in the run, not on the Engine,
// so a second Run in one process starts with the full budget.
func TestStorageBudgetResetsPerRun(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newWorld(t)
		e, _ := budgetConfig(t, w, "1s")
		w.sources["vpn"].dumpDelay = 1200 * time.Millisecond

		for i := range 2 {
			rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
			if err != nil {
				t.Fatal(err)
			}
			if rep.Outcome != model.OutcomeDelivered {
				t.Fatalf("run %d: outcome %s — a budget survived from the run before", i+1, rep.Outcome)
			}
		}
	})
}

// TestStorageClockBooksOverlappingCallsOnce (A1, C1): two calls that overlap
// spend the union of their intervals, not their sum — the time a storage spends
// with a call in flight is what the budget pays for, once.
func TestStorageClockBooksOverlappingCallsOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := &storageClock{budget: time.Second}
		end := c.begin()
		done := make(chan struct{})
		go func() {
			defer close(done)
			time.Sleep(10 * time.Millisecond)
			second := c.begin()
			time.Sleep(40 * time.Millisecond)
			second()
		}()
		time.Sleep(40 * time.Millisecond)
		end()
		<-done
		if got := c.total(); got != 50*time.Millisecond {
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
		c := &storageClock{budget: 60 * time.Millisecond}
		end := c.begin()
		defer end()
		time.Sleep(30 * time.Millisecond)
		ctx, cancel := c.bound(context.Background())
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
		c := &storageClock{budget: time.Second}
		end := c.begin()
		time.Sleep(10 * time.Millisecond)
		end()
		end()
		if got := c.total(); got != 10*time.Millisecond {
			t.Fatalf("total after a repeated end = %s, want 10ms booked once", got)
		}
		end = c.begin()
		time.Sleep(10 * time.Millisecond)
		end()
		if got := c.total(); got != 20*time.Millisecond {
			t.Fatalf("total after the next call = %s, want 20ms: the repeated end moved the open-call count", got)
		}
	})
}

// TestStorageClockTotalIncludesTheOpenInterval (C1): a total read while a call
// is still in flight already counts its elapsed time — the budget and the report
// never disagree about what the storage spent so far.
func TestStorageClockTotalIncludesTheOpenInterval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := &storageClock{budget: time.Second}
		end := c.begin()
		time.Sleep(10 * time.Millisecond)
		if got := c.total(); got != 10*time.Millisecond {
			t.Fatalf("total during an open interval = %s, want 10ms", got)
		}
		end()
		if got := c.total(); got != 10*time.Millisecond {
			t.Fatalf("total after the interval closed = %s, want 10ms", got)
		}
	})
}

// TestStorageBudgetExhaustionIsAnError: the budget the config names is still a
// budget — a storage slower than it fails its calls rather than hanging the run.
func TestStorageBudgetExhaustionIsAnError(t *testing.T) {
	w := newWorld(t)
	e, cfg := budgetConfig(t, w, "1ns")
	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome == model.OutcomeDelivered {
		t.Fatalf("an exhausted storage budget still delivered: %+v", rep.Storages)
	}
	if cfg["run"] == nil {
		t.Fatal("fixture drift")
	}
}
