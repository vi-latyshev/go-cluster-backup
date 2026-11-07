package backup_test

import (
	"context"
	"errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/workflow/backup"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
)

func TestGuardCurrentRunCountsAsItsOwnCopy(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"alpha": storageInstance("fake", "keep_last")})
	withRetention(cfg, "alpha", keepLast1())
	e := w.newAssembly(t, cfg)
	f := w.storage(t, "alpha")
	putRun(t, f, "alpha", "yesterday", time.Now().UTC().AddDate(0, 0, -1), true)

	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeDelivered {
		t.Fatalf("outcome %s", rep.Outcome)
	}
	ids := runIDsOn(f)
	if ids["yesterday"] {
		t.Fatalf("yesterday's run survived a keep_last(1) run: %v", ids)
	}
	if len(ids) != 1 {
		t.Fatalf("the storage holds %v, want exactly the current run", ids)
	}
}

// TestGuardCurrentRunSupersedesTheSameDayCopy is the regression for the floor's
// per-artifact ledger: two runs on one GFS day. The earlier run holds the newest
// verified copy of an artifact; the run being written uploads a newer verified
// copy of the same name. Retention after the second run's verified upload must
// drop the first — the pin releases because a newer copy now exists.
//
// The pin only releases if the current run's own verified copies reach the
// ledger before the floor decides. A mutant that leaves that update out keeps
// the earlier run pinned as the newest holder and this test fails.
func TestGuardCurrentRunSupersedesTheSameDayCopy(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"alpha": storageInstance("fake", "keep_last")})
	cfg["sources"] = map[string]any{"db": sourceInstance("fake", "databases", "prod", []string{"zstd3"}, "required", "plain")}
	withRetention(cfg, "alpha", gfsDaily1())
	e := w.newAssembly(t, cfg)
	f := w.storage(t, "alpha")

	// The earlier run of the same day, holding the newest verified copy of
	// databases/prod. A minute back keeps it on the current run's calendar day
	// except within a minute of midnight, and even across midnight the gfs day
	// tier still drops it — the pin is what this test measures either way.
	prevAt := time.Now().UTC().Add(-time.Minute)
	prevID := prevAt.Format("20060102T150405Z")
	putCopiedRun(t, f, "alpha", prevID, prevAt, model.OutcomeDelivered, [][2]string{{"databases", "prod"}})

	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeDelivered {
		t.Fatalf("outcome %s, want delivered\n%+v", rep.Outcome, rep.Storages)
	}
	if runIDsOn(f)[prevID] {
		t.Fatalf("the earlier same-day run survived: the current run's verified copy never reached the floor ledger (%v)", runIDsOn(f))
	}
	if len(runIDsOn(f)) != 1 {
		t.Fatalf("the storage holds %v, want exactly the current run", runIDsOn(f))
	}
}

// TestGuardIncompleteCurrentRunIsCounted is O2: a partially delivered run is
// still a run the storage holds, so the storage's .Kept counts it. It is not a
// verified run and the floor may not lean on it — but leaving it out of .Kept
// made the run being written invisible on the storage it had just reached.
//
// The older run holds the artifact this night fails to produce, so the floor
// keeps it and nothing is deleted: .Kept alone tells whether the current run
// was counted.
func TestGuardIncompleteCurrentRunIsCounted(t *testing.T) {
	w := newWorld(t)
	w.Sources["vol"] = &fakeSource{DumpErr: errors.New("the volume refused to read")}
	cfg := baseConfig(t, map[string]any{"alpha": storageInstance("fake", "keep_last")})
	cfg["sources"] = map[string]any{
		"db":  sourceInstance("fake", "databases", "prod", []string{"zstd3"}, "required", "plain"),
		"vol": sourceInstance("fake", "volumes", "uploads", []string{"zstd3"}, "required", "plain"),
	}
	withRetention(cfg, "alpha", keepLastN(1))
	e := w.newAssembly(t, cfg)
	f := w.storage(t, "alpha")
	olderAt := time.Now().UTC().Add(-time.Hour)
	putCopiedRun(t, f, "alpha", olderAt.Format("20060102T150405Z"), olderAt, model.OutcomeDelivered,
		[][2]string{{"volumes", "uploads"}})

	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	var kept int
	for _, s := range rep.Storages {
		if s.Storage == "alpha" {
			kept = s.Kept
		}
	}
	if kept != 2 {
		t.Fatalf("kept %d runs, want 2: the incomplete run being written is a run the storage holds", kept)
	}
}

// TestGuardFloorNamesTheBudgetOnlyWhenItBit: the floor line carries the
// byte-budget step of the protection ledger's cap eviction is floored. Here a tiny cap
// wants both runs gone, but each holds the newest copy of an artifact — the
// floor refuses both, the storage stays over its cap, and the overflow is
// reported rather than a run failing. Its negative is
// TestGuardNewestArtifactCopyOutlivesAStandingFailed, where a tier's own drop is
// floored and no cap is in play.
