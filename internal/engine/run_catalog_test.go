package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/model"
)

// TestCatalogLoadFailureIsNotFatal: a catalog that cannot be read leaves the
// previous-run fields empty and the run proceeds.
func TestCatalogLoadFailureIsNotFatal(t *testing.T) {
	w := newWorld(t)
	cat := &errCatalog{loadErr: errors.New("catalog unreadable")}
	registerCatalogType(t, w, "errcat", cat)
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["catalog"] = instance("errcat")
	e := w.newEngine(t, cfg)

	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeDelivered {
		t.Fatalf("outcome %q", rep.Outcome)
	}
	if rep.Previous.RunID != "" || rep.Previous.Unfinished {
		t.Fatalf("previous %+v, want the empty answer", rep.Previous)
	}
	loads, saves := cat.counts()
	if loads == 0 {
		t.Fatal("the catalog was never read")
	}
	if saves != 2 {
		t.Fatalf("the catalog was saved %d times, want 2 (started and finished)", saves)
	}
}

// TestCatalogSaveFailureIsNotFatal: both saves fail, the run still reports.
func TestCatalogSaveFailureIsNotFatal(t *testing.T) {
	w := newWorld(t)
	cat := &errCatalog{saveErr: errors.New("catalog unwritable")}
	registerCatalogType(t, w, "errcat", cat)
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["catalog"] = instance("errcat")
	e := w.newEngine(t, cfg)

	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatalf("a failing catalog save must not fail the run: %v", err)
	}
	if rep.Outcome != model.OutcomeDelivered {
		t.Fatalf("outcome %q", rep.Outcome)
	}
	_, saves := cat.counts()
	if saves < 2 {
		t.Fatalf("saves = %d, want the started and the finished save attempted", saves)
	}
	if len(cat.stored) != 0 {
		t.Fatalf("a failed save stored something: %v", cat.stored)
	}
}

// TestCatalogTypeWithoutACatalog: the log prefix of a catalog instance is empty
// when the config declares none.
func TestCatalogTypeWithoutACatalog(t *testing.T) {
	w := newWorld(t)
	e := w.newEngine(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}))
	if got := e.catalogType(); got != "" {
		t.Fatalf("catalogType() = %q, want empty without a catalog", got)
	}
}

// --- notify ------------------------------------------------------------------

// TestNotifySkipsANotifierWhoseCheckFailed: a notifier that failed its Check is
// not called, counts as failed, and its type is looked up for the log prefix.
func TestNotifySkipsANotifierWhoseCheckFailed(t *testing.T) {
	w := newWorld(t)
	n := &fakeNotifier{}
	w.notifiers["ops"] = n
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["notifiers"] = map[string]any{"ops": instance("fake")}
	e := w.newEngine(t, cfg)

	checks := failedChecks{instanceKey{"notifier", "ops"}: errors.New("check failed")}
	failed := e.notify(context.Background(), model.Event{Name: model.EventStarted}, checks)
	if !failed {
		t.Fatal("a notifier whose Check failed did not count as failed")
	}
	if len(n.events) != 0 {
		t.Fatalf("a notifier whose Check failed was called: %v", n.events)
	}
	if got := e.notifierType("absent"); got != "" {
		t.Fatalf("notifierType(absent) = %q, want empty", got)
	}
	if got := e.notifierType("ops"); got != "fake" {
		t.Fatalf("notifierType(ops) = %q", got)
	}
}

// --- capState ----------------------------------------------------------------

// TestCapStateReserveAndRelease: no cap admits everything, a retry of an
// already-reserved key is never refused, and a confirmed deletion never drives
// the occupancy below zero.
func TestCapStateReserveAndRelease(t *testing.T) {
	uncapped := newCapState(0, 0, true)
	if !uncapped.reserve("k", 1<<40, true) {
		t.Fatal("a storage without a cap refused a write")
	}

	c := newCapState(100, 0, true)
	if !c.reserve("k", 10, true) {
		t.Fatal("a write within the cap was refused")
	}
	if !c.reserve("k", 1<<40, true) {
		t.Fatal("a retry of an already-reserved object was refused")
	}
	if got := c.used(); got != 10 {
		t.Fatalf("used = %d, want 10", got)
	}

	d := newCapState(100, 50, true)
	d.releaseBytes(1000)
	if got := d.used(); got != 0 {
		t.Fatalf("used = %d, want 0 (clamped)", got)
	}
}

// --- dry run -----------------------------------------------------------------
