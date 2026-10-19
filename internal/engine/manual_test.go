package engine

import (
	"context"
	"strings"
	"testing"
	"time"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/catalog"
	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
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
	e := w.newEngine(t, cfg)
	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindManual})
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
	views, err := e.StoredRuns(context.Background(), StoredRunsRequest{})
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
	e := w.newEngine(t, cfg)
	if _, err := e.Run(context.Background(), RunRequest{Kind: ""}); err == nil {
		t.Fatal("an empty kind was accepted")
	}
	if _, err := e.Run(context.Background(), RunRequest{Kind: "nightly"}); err == nil {
		t.Fatal("an unknown kind was accepted")
	}
}

// TestManualRefusesWhenTheLeaseIsHeld: a manual run refuses at once on a live
// writer lease — outcome failed, no storage touched — so it never races the run
// that holds the claim.
func TestManualRefusesWhenTheLeaseIsHeld(t *testing.T) {
	w := newWorld(t)
	cfg := manualConfig(t, map[string]any{"alpha": manualStorage(1 << 40)})
	e := w.newEngine(t, cfg)
	w.catalog.Hold("other-run", model.RunKindManual, "infrastructure", "backup-other", "uid-other", time.Now().Add(-time.Minute))
	w.catalog.SetTerminated(false, false)
	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindManual})
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
	e := w.newEngine(t, cfg)
	w.catalog.Hold("other-run", model.RunKindManual, "infrastructure", "backup-other", "uid-other", time.Now().Add(-time.Minute))
	w.catalog.SetTerminated(false, false)
	rep := runScheduledFailed(t, e)
	if !strings.Contains(rep.Lease.Error, "held by run other-run") {
		t.Fatalf("the failure does not name the lease holder: %q", rep.Lease.Error)
	}
	ev, ok := w.notifiers["ops"].last()
	if !ok || ev.Name != model.EventFinished {
		t.Fatalf("the failed run was not notified: %v", w.notifiers["ops"].eventNames())
	}
	if ev.Run.Outcome != model.OutcomeFailed || ev.Run.Lease == nil || ev.Run.Lease.Error == "" {
		t.Fatalf("the notification does not carry the failed run and its lease: %+v", ev.Run)
	}
	if _, ok := w.catalog.runs[rep.RunID]; !ok {
		t.Fatal("the failed run was not written to the catalog")
	}
}

// TestManualCapAdmissionRefusesAndDeletesNothing: a manual run whose object
// would cross the storage's configured cap is refused on that storage, the
// refusal is machine-readable, and nothing already there is deleted.
func TestManualCapAdmissionRefusesAndDeletesNothing(t *testing.T) {
	w := newWorld(t)
	cfg := manualConfig(t, map[string]any{"alpha": manualStorage(1)})
	e := w.newEngine(t, cfg)
	// A scheduled run already fills the storage beyond its one-byte cap.
	putCopiedRun(t, w.storage(t, "alpha"), "alpha", "old-scheduled", time.Now().Add(-time.Hour),
		model.OutcomeDelivered, [][2]string{{"databases", "prod"}})
	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindManual})
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
	e := w.newEngine(t, cfg)
	putCopiedRun(t, w.storage(t, "beta"), "beta", "old-scheduled", time.Now().Add(-time.Hour),
		model.OutcomeDelivered, [][2]string{{"databases", "prod"}})
	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindManual})
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
	e := w.newEngine(t, cfg)
	if _, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindManual}); err == nil {
		t.Fatal("a storage without a manual partition accepted a manual run")
	}
}

// TestManualRunNeedsALeaseCapableCatalog: with a catalog that cannot hold the
// writer lease, a manual run is refused outright.
func TestManualRunNeedsALeaseCapableCatalog(t *testing.T) {
	w := newWorld(t)
	registerPlainCatalog(t, w)
	cfg := manualConfig(t, map[string]any{"alpha": manualStorage(1 << 40)})
	cfg["catalog"] = instance("plaincatalog")
	e := w.newEngine(t, cfg)
	if _, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindManual}); err == nil {
		t.Fatal("a manual run started without a lease-capable catalog")
	}
}

// plainCatalog implements catalog.Catalog and NOT the Leaser capability, so a
// manual run has no writer lease to take.
type plainCatalog struct{ inner fakeCatalog }

func (c *plainCatalog) Save(ctx context.Context, r model.RunReport) error {
	return c.inner.Save(ctx, r)
}
func (c *plainCatalog) Load(ctx context.Context, req catalog.LoadRequest) ([]model.RunReport, error) {
	return c.inner.Load(ctx, req)
}
func (c *plainCatalog) Forget(ctx context.Context, ids []string) error {
	return c.inner.Forget(ctx, ids)
}

func registerPlainCatalog(t *testing.T, w *world) {
	t.Helper()
	err := plugin.Register(w.registry, catalog.Kind, plugin.Spec[map[string]jsontext.Value, catalog.Catalog]{
		Type: "plaincatalog", Example: []byte(`{}`),
		New: func(_ context.Context, _ map[string]jsontext.Value, _ plugin.Deps) (catalog.Catalog, error) {
			return &plainCatalog{}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestRetentionPartitionsByKind: a manual run never takes a scheduled GFS slot —
// the engine hands each partition its own homogeneous list. Feeding both kinds to
// one gfs policy would drop one of the two same-day runs; the partition keeps
// both.
func TestRetentionPartitionsByKind(t *testing.T) {
	w := newWorld(t)
	cfg := guardConfig(t,
		map[string]any{"alpha": manualStorage(1 << 40)},
		map[string]any{"alpha": gfsDaily1()},
	)
	withManualRetention(cfg, "alpha", keepLastCfg(5))
	e := w.newEngine(t, cfg)
	f := w.storage(t, "alpha")
	at := time.Date(2026, 10, 1, 4, 0, 0, 0, time.UTC)
	putCopiedRun(t, f, "alpha", "20261001T040000Z", at, model.OutcomeDelivered, [][2]string{{"databases", "prod"}})
	putCopiedRun(t, f, "alpha", "20261001T040000Z-manual", at, model.OutcomeDelivered, [][2]string{{"databases", "prod"}})

	_, results := sweepOnce(t, e, at)
	if results["alpha"].retentionErr != nil {
		t.Fatal(results["alpha"].retentionErr)
	}
	ids := runIDsOn(f)
	if !ids["20261001T040000Z"] || !ids["20261001T040000Z-manual"] {
		t.Fatalf("a same-day manual run took the scheduled GFS slot: %v", ids)
	}
}

// TestManualCopyDoesNotReleaseAScheduledPin: the per-artifact floor is per kind,
// so a newer MANUAL copy of an artifact does not let the older scheduled run be
// deleted — the scheduled partition still holds the newest copy of that artifact.
func TestManualCopyDoesNotReleaseAScheduledPin(t *testing.T) {
	w := newWorld(t)
	cfg := guardConfig(t,
		map[string]any{"alpha": manualStorage(1 << 40)},
		map[string]any{"alpha": keepLastN(1)},
	)
	withManualRetention(cfg, "alpha", keepLastCfg(5))
	e := w.newEngine(t, cfg)
	f := w.storage(t, "alpha")
	base := time.Date(2026, 10, 1, 4, 0, 0, 0, time.UTC)
	// s-old holds X; s-new (newer) does not — so the policy wants s-old gone, and
	// the floor pins it for holding the newest SCHEDULED copy of X.
	putCopiedRun(t, f, "alpha", "s-old", base, model.OutcomeDelivered, [][2]string{{"volumes", "uploads"}})
	putCopiedRun(t, f, "alpha", "s-new", base.AddDate(0, 0, 1), model.OutcomeDelivered, [][2]string{{"databases", "prod"}})
	// A manual run holds X too, newer than s-old: it must not release s-old.
	putCopiedRun(t, f, "alpha", "20261003T040000Z-manual", base.AddDate(0, 0, 2), model.OutcomeDelivered, [][2]string{{"volumes", "uploads"}})

	_, results := sweepOnce(t, e, base.AddDate(0, 0, 3))
	if results["alpha"].retentionErr != nil {
		t.Fatal(results["alpha"].retentionErr)
	}
	if !runIDsOn(f)["s-old"] {
		t.Fatalf("a manual copy released the scheduled pin: %v", runIDsOn(f))
	}
}

// TestScheduledEvictsManualBeforeScheduledHistory (owner decision D1): a
// scheduled run over its cap frees room by dropping the oldest manual run before
// it touches its own history.
func TestScheduledEvictsManualBeforeScheduledHistory(t *testing.T) {
	w := newWorld(t)
	// The cap fits one run's worth; two runs are present.
	cfg := guardConfig(t,
		map[string]any{"alpha": manualStorage(1)},
		map[string]any{"alpha": keepLastN(10)},
	)
	withManualRetention(cfg, "alpha", keepLastCfg(5))
	e := w.newEngine(t, cfg)
	f := w.storage(t, "alpha")
	base := time.Date(2026, 10, 1, 4, 0, 0, 0, time.UTC)
	// Both hold the same artifact, so neither is the storage's only copy.
	putCopiedRun(t, f, "alpha", "s-keep", base.AddDate(0, 0, 1), model.OutcomeDelivered, [][2]string{{"databases", "prod"}})
	putCopiedRun(t, f, "alpha", "20261001T040000Z-manual", base, model.OutcomeDelivered, [][2]string{{"databases", "prod"}})

	_, results := sweepOnce(t, e, base.AddDate(0, 0, 2))
	if results["alpha"].retentionErr != nil {
		t.Fatal(results["alpha"].retentionErr)
	}
	ids := runIDsOn(f)
	if ids["20261001T040000Z-manual"] {
		t.Fatalf("the manual run was not evicted under cap pressure: %v", ids)
	}
	if !ids["s-keep"] {
		t.Fatalf("the scheduled history was cut before the manual run: %v", ids)
	}
}

// TestPreviousUsesScheduledHistoryOnly: the "previous run did not report" signal
// reads SCHEDULED history — a running manual run is neither the previous run nor
// an unfinished one.
func TestPreviousUsesScheduledHistoryOnly(t *testing.T) {
	w := newWorld(t)
	cfg := manualConfig(t, map[string]any{"alpha": manualStorage(1 << 40)})
	e := w.newEngine(t, cfg)
	at := time.Date(2026, 10, 1, 4, 0, 0, 0, time.UTC)
	for _, r := range []model.RunReport{
		{Schema: model.ManifestSchema, RunID: "20261001T040000Z", Kind: model.RunKindScheduled,
			StartedAt: at, Outcome: model.OutcomeDelivered},
		{Schema: model.ManifestSchema, RunID: "20261001T050000Z-manual", Kind: model.RunKindManual,
			StartedAt: at.Add(time.Hour), Outcome: model.OutcomeRunning},
	} {
		if err := w.catalog.Save(context.Background(), r); err != nil {
			t.Fatal(err)
		}
	}
	prev := e.previous(context.Background())
	if prev.RunID != "20261001T040000Z" {
		t.Fatalf("previous = %q, want the scheduled run", prev.RunID)
	}
	if prev.Unfinished {
		t.Fatalf("a running manual run read as an unfinished scheduled one: %+v", prev)
	}
	if !prev.LastGoodAt.Equal(at) {
		t.Fatalf("last good = %s, want the scheduled run's start %s", prev.LastGoodAt, at)
	}
}

// TestRunRefusesACollidingRunID: two runs of one id would overwrite each other's
// objects; the engine refuses the collision before the first upload.
func TestRunRefusesACollidingRunID(t *testing.T) {
	w := newWorld(t)
	cfg := manualConfig(t, map[string]any{"alpha": manualStorage(1 << 40)})
	cfg["run"].(map[string]any)["id_layout"] = "fixedrun"
	e := w.newEngine(t, cfg)
	putCopiedRun(t, w.storage(t, "alpha"), "alpha", "fixedrun", time.Now().Add(-time.Hour),
		model.OutcomeDelivered, [][2]string{{"databases", "prod"}})
	if _, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled}); err == nil {
		t.Fatal("a colliding run id was accepted")
	} else if !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("the refusal does not name the collision: %v", err)
	}
}

// TestManifestKindMustAgreeWithTheRunID: a manifest whose kind disagrees with
// the id it lives under is a problem run — the policy's drop is refused, because
// the survey cannot trust what the run says about itself.
func TestManifestKindMustAgreeWithTheRunID(t *testing.T) {
	w := newWorld(t)
	cfg := guardConfig(t,
		map[string]any{"alpha": manualStorage(1 << 40)},
		map[string]any{"alpha": keepLastN(10)},
	)
	withManualRetention(cfg, "alpha", keepLastCfg(1))
	e := w.newEngine(t, cfg)
	f := w.storage(t, "alpha")
	base := time.Date(2026, 10, 1, 4, 0, 0, 0, time.UTC)
	// A manual-suffixed id whose manifest claims to be scheduled: a mismatch.
	mismatched := model.RunReport{Schema: model.ManifestSchema, RunID: "20261001T040000Z-manual",
		Kind: model.RunKindScheduled, StartedAt: base, Outcome: model.OutcomeDelivered,
		Storages: []model.StorageResult{{Storage: "alpha", Outcome: model.StorageVerified}}}
	f.PutObject("runs/20261001T040000Z-manual/manifest.json", mustMarshal(t, mismatched), base)
	f.PutObject("runs/20261001T040000Z-manual/databases/prod.bin", []byte("x"), base)
	putCopiedRun(t, f, "alpha", "20261002T040000Z-manual", base.AddDate(0, 0, 1),
		model.OutcomeDelivered, [][2]string{{"databases", "prod"}})

	_, results := sweepOnce(t, e, base.AddDate(0, 0, 2))
	if results["alpha"].retentionErr != nil {
		t.Fatal(results["alpha"].retentionErr)
	}
	if !runIDsOn(f)["20261001T040000Z-manual"] {
		t.Fatalf("a run whose manifest disagrees with its id was deleted: %v", runIDsOn(f))
	}
}

// TestScheduledNeverFailsOnTheCap: a scheduled run is never refused on the cap —
// it delivers and frees room instead, even when the storage is far over it.
func TestScheduledNeverFailsOnTheCap(t *testing.T) {
	w := newWorld(t)
	cfg := manualConfig(t, map[string]any{"alpha": manualStorage(1)})
	e := w.newEngine(t, cfg)
	// The old run holds the artifact the run itself produces, so the new copy
	// releases its pin and the eviction can reclaim it.
	putCopiedRun(t, w.storage(t, "alpha"), "alpha", "old", time.Now().Add(-time.Hour),
		model.OutcomeDelivered, [][2]string{{"vpn", "wireguard"}})
	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
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
}
