package engine

import (
	"context"
	"testing"
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
}

// TestStorageBudgetResetsPerRun: the clock lives in the run, not on the Engine,
// so a second Run in one process starts with the full budget.
func TestStorageBudgetResetsPerRun(t *testing.T) {
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
