package backup_test

import (
	"context"
	"errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/workflow/backup"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage/storagetest"
)

// manualStorage is one storage that accepts both run kinds, with the given cap.
func manualStorage(cap int64) map[string]any {
	return map[string]any{
		"type": "fake",
		"retention": map[string]any{
			"scheduled": map[string]any{"type": "keep_last", "config": map[string]any{
				"count": 10, "min_keep": 1, "count_incomplete": true}},
			"manual": map[string]any{"type": "keep_last", "config": map[string]any{
				"count": 5, "min_keep": 1, "count_incomplete": true}},
		},
		"max_total_bytes": cap,
		"time_budget":     "10s", "config": map[string]any{},
		"call_parallelism": 1,
	}
}

// manualConfig is baseConfig plus a lease-capable catalog and a manual partition.
func manualConfig(t *testing.T, storages map[string]any) map[string]any {
	t.Helper()
	cfg := baseConfig(t, storages)
	cfg["catalog"] = instance("fake")
	return cfg
}

// TestManualRunRecordsItsKindAndSuffixedID: a manual run's identity is on the
// report, in its id — the survey's own derivation — and in the manifest it
// writes, so a manual run killed before it wrote one is still identifiable.
func TestManualRunRecordsItsKindAndSuffixedID(t *testing.T) {
	w := newWorld(t)
	cfg := withRestore(t, manualConfig(t, map[string]any{"alpha": manualStorage(1 << 40)}), "alpha")
	e := w.newAssembly(t, cfg)
	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindManual})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Kind != model.RunKindManual {
		t.Fatalf("report kind = %q, want manual", rep.Kind)
	}
	if !strings.HasSuffix(rep.RunID, "-manual") {
		t.Fatalf("manual run id %q does not carry the suffix", rep.RunID)
	}
	if rep.Outcome != model.OutcomeDelivered {
		t.Fatalf("outcome = %q, want delivered: %+v", rep.Outcome, rep.Storages)
	}
	body, ok := w.storage(t, "alpha").Bytes("runs/" + rep.RunID + "/manifest.json")
	if !ok {
		t.Fatalf("no manifest written for %s", rep.RunID)
	}
	if !strings.Contains(string(body), `"kind":"manual"`) {
		t.Fatalf("the manifest does not record the kind: %s", body)
	}
	// The survey derives the same kind from the key alone.
	views, err := e.StoredRuns(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(views) != 1 || views[0].Kind != model.RunKindManual {
		t.Fatalf("stored runs = %+v, want one manual run", views)
	}
}

// TestRunRefusesAnUnknownKind: an empty or unknown kind is refused before any
// write — the run never starts.
func TestRunRefusesAnUnknownKind(t *testing.T) {
	w := newWorld(t)
	cfg := manualConfig(t, map[string]any{"alpha": manualStorage(1 << 40)})
	e := w.newAssembly(t, cfg)
	if _, err := e.Run(context.Background(), backup.RunRequest{Kind: ""}); err == nil {
		t.Fatal("an empty kind was accepted")
	}
	if _, err := e.Run(context.Background(), backup.RunRequest{Kind: "nightly"}); err == nil {
		t.Fatal("an unknown kind was accepted")
	}
}

// TestManualRefusesWhenTheLeaseIsHeld: a manual run refuses at once on a live
// writer lease — outcome failed, no storage touched — so it never races the run
// that holds the claim.
func TestManualRefusesWhenTheLeaseIsHeld(t *testing.T) {
	w := newWorld(t)
	cfg := manualConfig(t, map[string]any{"alpha": manualStorage(1 << 40)})
	e := w.newAssembly(t, cfg)
	w.Catalog.Hold("other-run", model.RunKindManual, "infrastructure", "backup-other", "uid-other", time.Now().Add(-time.Minute))
	w.Catalog.SetTerminated(false, false)
	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindManual})
	if err != nil {
		t.Fatalf("a manual refusal is a failed run, not an error: %v", err)
	}
	if rep.Outcome != model.OutcomeFailed {
		t.Fatalf("outcome = %q, want failed", rep.Outcome)
	}
	if rep.ExitCode != 7 {
		t.Fatalf("exit code = %d, want the configured failed code 7", rep.ExitCode)
	}
	// The refusal is machine-readable: a template names the holder without
	// inferring it from "manual && failed && no storages".
	if rep.Lease == nil || !rep.Lease.Refused {
		t.Fatalf("the report does not carry the lease refusal: %+v", rep.Lease)
	}
	if rep.Lease.Holder != "other-run" || rep.Lease.HolderKind != model.RunKindManual {
		t.Fatalf("the refusal does not name the holder: %+v", rep.Lease)
	}
	if keys := w.storage(t, "alpha").Keys(); len(keys) != 0 {
		t.Fatalf("the refused manual run touched the storage: %v", keys)
	}
}

// TestScheduledWaitsForTheLeaseThenFailsLoudly: with run.lease_wait at 0 a
// scheduled run does not wait at all. It no longer returns a plain error: the
// failure goes through the common finish — the outcome is failed, the lease
// reason is in the report, and the finished notification is sent, so a night
// that could not start still tells anyone.
func TestScheduledWaitsForTheLeaseThenFailsLoudly(t *testing.T) {
	w := newWorld(t)
	cfg := manualConfig(t, map[string]any{"alpha": manualStorage(1 << 40)})
	cfg["notifiers"] = map[string]any{"ops": instance("fake")}
	e := w.newAssembly(t, cfg)
	w.Catalog.Hold("other-run", model.RunKindManual, "infrastructure", "backup-other", "uid-other", time.Now().Add(-time.Minute))
	w.Catalog.SetTerminated(false, false)
	rep := runScheduledFailed(t, e)
	if !strings.Contains(rep.Lease.Error, "held by run other-run") {
		t.Fatalf("the failure does not name the lease holder: %q", rep.Lease.Error)
	}
	ev, ok := w.Notifiers["ops"].Last()
	if !ok || ev.Name != model.EventFinished {
		t.Fatalf("the failed run was not notified: %v", w.Notifiers["ops"].EventNames())
	}
	if ev.Run.Outcome != model.OutcomeFailed || ev.Run.Lease == nil || ev.Run.Lease.Error == "" {
		t.Fatalf("the notification does not carry the failed run and its lease: %+v", ev.Run)
	}
}

// TestManualCapAdmissionRefusesAndDeletesNothing: a manual run whose object
// would cross the storage's configured cap is refused on that storage, the
// refusal is machine-readable, and nothing already there is deleted.
func TestManualCapAdmissionRefusesAndDeletesNothing(t *testing.T) {
	w := newWorld(t)
	cfg := manualConfig(t, map[string]any{"alpha": manualStorage(1)})
	e := w.newAssembly(t, cfg)
	// A scheduled run already fills the storage beyond its one-byte cap.
	putCopiedRun(t, w.storage(t, "alpha"), "alpha", "old-scheduled", time.Now().Add(-time.Hour),
		model.OutcomeDelivered, [][2]string{{"databases", "prod"}})
	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindManual})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeFailed {
		t.Fatalf("outcome = %q, want failed (no storage verified)", rep.Outcome)
	}
	if len(rep.Storages) != 1 || !rep.Storages[0].CapExceeded {
		t.Fatalf("the cap refusal is not machine-readable: %+v", rep.Storages)
	}
	if got := rep.Storages[0]; got.CapLimitBytes != 1 || got.CapUsedBytes <= 1 {
		t.Fatalf("the refusal does not carry the limit and the bytes used: %+v", got)
	}
	if rep.Storages[0].OccupancyUnknown {
		t.Fatalf("a known over-cap refusal reported an unknown occupancy: %+v", rep.Storages[0])
	}
	if !strings.Contains(rep.Storages[0].Error, "cap_exceeded") {
		t.Fatalf("the reason does not carry cap_exceeded: %q", rep.Storages[0].Error)
	}
	if ids := runIDsOn(w.storage(t, "alpha")); !ids["old-scheduled"] {
		t.Fatalf("the manual run deleted a scheduled run: %v", ids)
	}
}

// TestManualRefusedOnOneStorageIsDegraded (owner decision D3): one storage took
// the whole manual run, the other refused on its cap — the outcome is degraded,
// with the refusal reported on the storage that refused.
func TestManualRefusedOnOneStorageIsDegraded(t *testing.T) {
	w := newWorld(t)
	cfg := manualConfig(t, map[string]any{
		"alpha": manualStorage(1 << 40),
		"beta":  manualStorage(1),
	})
	e := w.newAssembly(t, cfg)
	putCopiedRun(t, w.storage(t, "beta"), "beta", "old-scheduled", time.Now().Add(-time.Hour),
		model.OutcomeDelivered, [][2]string{{"databases", "prod"}})
	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindManual})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeDegraded {
		t.Fatalf("outcome = %q, want degraded: %+v", rep.Outcome, rep.Storages)
	}
	byName := map[string]model.StorageResult{}
	for _, s := range rep.Storages {
		byName[s.Storage] = s
	}
	if byName["alpha"].Outcome != model.StorageVerified {
		t.Fatalf("alpha = %+v, want verified", byName["alpha"])
	}
	if !byName["beta"].CapExceeded {
		t.Fatalf("beta did not report the cap refusal: %+v", byName["beta"])
	}
}

// TestManualRunNeedsAManualPartition: a storage without a manual retention
// policy refuses a manual run before it starts.
func TestManualRunNeedsAManualPartition(t *testing.T) {
	w := newWorld(t)
	cfg := manualConfig(t, map[string]any{"alpha": storageInstance("fake", "keep_last")})
	e := w.newAssembly(t, cfg)
	if _, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindManual}); err == nil {
		t.Fatal("a storage without a manual partition accepted a manual run")
	}
}

// TestManualRunNeedsACatalog: with no catalog configured there is no durable
// claim to take, and a manual run is refused outright.
func TestManualRunNeedsACatalog(t *testing.T) {
	w := newWorld(t)
	cfg := manualConfig(t, map[string]any{"alpha": manualStorage(1 << 40)})
	delete(cfg, "catalog")
	e := w.newAssembly(t, cfg)
	if _, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindManual}); err == nil {
		t.Fatal("a manual run started without a catalog")
	}
}

// TestRetentionPartitionsByKind: a manual run never takes a scheduled GFS slot —
// the protection ledger hands each partition its own homogeneous list. Feeding both kinds to
// one gfs policy would drop one of the two same-day runs; the partition keeps
// both.
func TestRunRefusesACollidingRunID(t *testing.T) {
	w := newWorld(t)
	cfg := manualConfig(t, map[string]any{"alpha": manualStorage(1 << 40)})
	cfg["run"].(map[string]any)["id_layout"] = "fixedrun"
	e := w.newAssembly(t, cfg)
	putCopiedRun(t, w.storage(t, "alpha"), "alpha", "fixedrun", time.Now().Add(-time.Hour),
		model.OutcomeDelivered, [][2]string{{"databases", "prod"}})
	if _, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled}); err == nil {
		t.Fatal("a colliding run id was accepted")
	} else if !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("the refusal does not name the collision: %v", err)
	}
	if puts := w.storage(t, "alpha").CallCount(storagetest.OpPut, ""); puts != 0 {
		t.Fatalf("the colliding run wrote %d objects before refusing", puts)
	}
}

// TestManifestKindMustAgreeWithTheRunID: a manifest whose kind disagrees with
// the id it lives under is a problem run — the policy's drop is refused, because
// the survey cannot trust what the run says about itself.
func TestScheduledNeverFailsOnTheCap(t *testing.T) {
	w := newWorld(t)
	cfg := manualConfig(t, map[string]any{"alpha": manualStorage(1)})
	e := w.newAssembly(t, cfg)
	// The old run holds the artifact the run itself produces, so the new copy
	// releases its pin and the eviction can reclaim it.
	putCopiedRun(t, w.storage(t, "alpha"), "alpha", "old", time.Now().Add(-time.Hour),
		model.OutcomeDelivered, [][2]string{{"vpn", "wireguard"}})
	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome == model.OutcomeFailed {
		t.Fatalf("a scheduled run failed on the cap: %+v", rep.Storages)
	}
	for _, s := range rep.Storages {
		if s.CapExceeded {
			t.Fatalf("a scheduled run reported a cap refusal: %+v", s)
		}
	}
	if ids := runIDsOn(w.storage(t, "alpha")); ids["old"] {
		t.Fatalf("the cap eviction did not free the old run: %v", ids)
	}
	got := storageResult(rep, "alpha")
	if !slices.Contains(got.CapEvicted, "old") {
		t.Fatalf("the cap eviction is not named in the report: %+v", got)
	}
	if got.CapOverflowBytes <= 0 {
		t.Fatalf("the new run itself exceeds the one-byte cap, so the overflow must be reported: %+v", got)
	}
	if got.RetentionError != "" {
		t.Fatalf("an eviction failure is not a retention error: %+v", got)
	}
}

// TestManualSurveyFailureRefusesOnlyItsStorage (F6, S3): a storage whose survey
// failed has an unknown occupancy, so a manual run is refused there — and only
// there: the other storage takes the whole run, and the outcome is degraded.
func TestManualSurveyFailureRefusesOnlyItsStorage(t *testing.T) {
	w := newWorld(t)
	cfg := manualConfig(t, map[string]any{
		"alpha": manualStorage(1 << 40),
		"beta":  manualStorage(1 << 40),
	})
	e := w.newAssembly(t, cfg)
	w.storage(t, "alpha").Fault = func(op storagetest.Op, _ string, _ int) error {
		if op == storagetest.OpList {
			return pluginerrors.Permanent(errors.New("list refused"))
		}
		return nil
	}
	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindManual})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeDegraded {
		t.Fatalf("outcome = %q, want degraded: %+v", rep.Outcome, rep.Storages)
	}
	byName := map[string]model.StorageResult{}
	for _, s := range rep.Storages {
		byName[s.Storage] = s
	}
	if got := byName["alpha"]; !got.CapExceeded || got.Outcome != model.StorageFailed {
		t.Fatalf("alpha = %+v, want the unknown-occupancy refusal", got)
	}
	if !byName["alpha"].OccupancyUnknown {
		t.Fatalf("alpha's unknown occupancy is not in the report: %+v", byName["alpha"])
	}
	if byName["beta"].OccupancyUnknown {
		t.Fatalf("beta's known occupancy is reported unknown: %+v", byName["beta"])
	}
	if got := byName["beta"]; got.Outcome != model.StorageVerified {
		t.Fatalf("beta = %+v, want the whole run", got)
	}
	if keys := w.storage(t, "alpha").Keys(); len(keys) != 0 {
		t.Fatalf("the refused storage was written to: %v", keys)
	}
}
