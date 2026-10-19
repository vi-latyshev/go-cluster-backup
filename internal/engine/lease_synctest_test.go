package engine

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/model"
)

// The two lease-wait outcomes run in a synctest bubble: the wait is virtual, so
// a test proves "waited, then proceeded" and "waited, then failed" without
// sitting out a real second of the configured wait.

// TestScheduledProceedsWhenTheHolderFinishesMidWait: a scheduled run that finds
// the claim held waits its bounded time and, when the holder finishes before the
// bound, takes the claim and runs.
func TestScheduledProceedsWhenTheHolderFinishesMidWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newWorld(t)
		cfg := manualConfig(t, map[string]any{"alpha": manualStorage(1 << 40)})
		run := cfg["run"].(map[string]any)
		run["lease_wait"] = "1h"
		run["lease_poll"] = "1s"
		run["lease_hard_bound"] = "1h"
		e := w.newEngine(t, cfg)
		w.catalog.Hold("other-run", model.RunKindManual, "infrastructure", "backup-other", "uid-other", time.Now())
		w.catalog.SetTerminated(false, false)
		// The holder finishes mid-wait: the run must wait, then proceed.
		go func() {
			time.Sleep(3 * time.Second)
			if held, ok := w.catalog.Held(); ok {
				_ = w.catalog.Release(context.Background(), held)
			}
		}()
		rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
		if err != nil {
			t.Fatalf("the run failed while waiting for a holder that finished: %v", err)
		}
		if rep.Outcome == model.OutcomeFailed {
			t.Fatalf("the run failed after the holder finished: %+v", rep.Lease)
		}
		if w.catalog.AcquireCount() < 2 {
			t.Fatalf("the run did not wait and retry: %d acquisitions", w.catalog.AcquireCount())
		}
	})
}

// TestScheduledWaitExpiryIsNotified: a scheduled run whose bounded wait for a
// live holder expires ends through the common finish — failed, the lease reason
// in the report, and the finished notification sent.
func TestScheduledWaitExpiryIsNotified(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newWorld(t)
		cfg := manualConfig(t, map[string]any{"alpha": manualStorage(1 << 40)})
		cfg["notifiers"] = map[string]any{"ops": instance("fake")}
		run := cfg["run"].(map[string]any)
		run["lease_wait"] = "30s"
		run["lease_poll"] = "1s"
		run["lease_hard_bound"] = "1h"
		e := w.newEngine(t, cfg)
		w.catalog.Hold("other-run", model.RunKindManual, "infrastructure", "backup-other", "uid-other", time.Now())
		w.catalog.SetTerminated(false, false)
		rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
		if err != nil {
			t.Fatal(err)
		}
		if rep.Outcome != model.OutcomeFailed || rep.Lease == nil || rep.Lease.Error == "" {
			t.Fatalf("the expired wait did not fail with the lease reason: outcome=%q lease=%+v", rep.Outcome, rep.Lease)
		}
		ev, ok := w.notifiers["ops"].last()
		if !ok || ev.Name != model.EventFinished {
			t.Fatalf("the failed run was not notified: %v", w.notifiers["ops"].eventNames())
		}
		if ev.Run.Outcome != model.OutcomeFailed {
			t.Fatalf("the notification does not carry the failed run: %+v", ev.Run)
		}
	})
}
