package assembly

import (
	"errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage/storagetest"
	"strings"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
)

func TestRetentionPartitionsByKind(t *testing.T) {
	w := newWorld(t)
	cfg := guardConfig(t,
		map[string]any{"alpha": manualStorage(1 << 40)},
		map[string]any{"alpha": gfsDaily1()},
	)
	withManualRetention(cfg, "alpha", keepLastCfg(5))
	e := w.newAssembly(t, cfg)
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
	e := w.newAssembly(t, cfg)
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
	e := w.newAssembly(t, cfg)
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

// TestRunRefusesACollidingRunID: two runs of one id would overwrite each other's
// objects; the backup workflow refuses the collision before the first upload.

func TestManifestKindMustAgreeWithTheRunID(t *testing.T) {
	w := newWorld(t)
	cfg := guardConfig(t,
		map[string]any{"alpha": manualStorage(1 << 40)},
		map[string]any{"alpha": keepLastN(10)},
	)
	withManualRetention(cfg, "alpha", keepLastCfg(1))
	e := w.newAssembly(t, cfg)
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
func TestPartialDeleteStopsAndTheOtherCopiesSurvive(t *testing.T) {
	w := newWorld(t)
	cfg := manualConfig(t, map[string]any{"alpha": manualStorage(1)})
	withRetention(cfg, "alpha", keepLastN(10))
	withManualRetention(cfg, "alpha", keepLastN(10))
	e := w.newAssembly(t, cfg)
	f := w.storage(t, "alpha")
	first := manualID("20261001T040000Z")
	f.Fault = func(op storagetest.Op, key string, call int) error {
		if op == storagetest.OpDelete && strings.HasSuffix(key, "databases/prod.bin") && strings.Contains(key, first) {
			return errors.New("the delete failed")
		}
		return nil
	}
	base := time.Date(2026, 10, 1, 4, 0, 0, 0, time.UTC)
	// Three copies of one artifact; the cap evicts oldest first, so after the
	// first one's delete fails there are two more the sweep would reach.
	second, third := manualID("20261002T040000Z"), manualID("20261003T040000Z")
	putCopiedRun(t, f, "alpha", first, base, model.OutcomeDelivered, [][2]string{{"databases", "prod"}})
	putCopiedRun(t, f, "alpha", second, base.AddDate(0, 0, 1), model.OutcomeDelivered, [][2]string{{"databases", "prod"}})
	putCopiedRun(t, f, "alpha", third, base.AddDate(0, 0, 2), model.OutcomeDelivered, [][2]string{{"databases", "prod"}})

	_, results := sweepOnce(t, e, base.AddDate(0, 0, 3))
	if results["alpha"].retentionErr == nil {
		t.Fatalf("the partial deletion was not reported: %+v", results["alpha"].result)
	}
	// The manifest went first and the artifact delete failed: the copy is half
	// torn down, and the sweep stopped there.
	if _, ok := f.Bytes("runs/" + first + "/manifest.json"); ok {
		t.Fatalf("the failed deletion left the first run's manifest: %v", f.Keys())
	}
	if _, ok := f.Bytes("runs/" + first + "/databases/prod.bin"); !ok {
		t.Fatalf("the fixture did not leave the first object half deleted: %v", f.Keys())
	}
	for _, id := range []string{second, third} {
		if _, ok := f.Bytes("runs/" + id + "/manifest.json"); !ok {
			t.Fatalf("a later copy was deleted after the first one's delete failed: %v", f.Keys())
		}
	}
}

// TestInFlightRunOfAnotherWriterIsNeverDeleted (C1d): a run with no manifest is
// another writer's in-flight work until it is older than the longest a live run
// can last — then the cap eviction may reclaim it.
func TestInFlightRunOfAnotherWriterIsNeverDeleted(t *testing.T) {
	w := newWorld(t)
	cfg := manualConfig(t, map[string]any{"alpha": manualStorage(1)})
	withRetention(cfg, "alpha", keepLastN(10))
	e := w.newAssembly(t, cfg)
	f := w.storage(t, "alpha")
	now := time.Now().UTC()
	// A newer run takes the policy's min_keep floor, so the in-flight one below is
	// a real eviction candidate and only the cutoff protects it.
	f.PutObject("runs/newer/databases/prod.bin", []byte("partial"), now.Add(-5*time.Second))
	// Young: its writer may still be alive, whatever the lease says.
	f.PutObject("runs/inflight/databases/prod.bin", []byte("partial"), now.Add(-20*time.Second))
	// Old: past deadline + finish allowance, so the pod cannot be alive.
	f.PutObject("runs/dead/databases/prod.bin", []byte("partial"), now.Add(-time.Hour))

	_, results := sweepOnce(t, e, now)
	if results["alpha"].retentionErr != nil {
		t.Fatal(results["alpha"].retentionErr)
	}
	ids := runIDsOn(f)
	if !ids["inflight"] {
		t.Fatalf("an in-flight run of another writer was deleted: %v", ids)
	}
	if ids["dead"] {
		t.Fatalf("an inactive incomplete run was not reclaimed: %v", ids)
	}
}

// TestManualCapStopsAtTheManifest (C5): the manifest is an object write like any
// other — a manual run whose artifacts fit but whose manifest would cross the cap
// keeps its artifacts and refuses the manifest.
func TestManualPartitionOnlyRunsOnTheManualPath(t *testing.T) {
	w := newWorld(t)
	cfg := manualConfig(t, map[string]any{"alpha": manualStorage(1 << 40)})
	withRetention(cfg, "alpha", keepLastN(10))
	withManualRetention(cfg, "alpha", keepLastN(1))
	e := w.newAssembly(t, cfg)
	f := w.storage(t, "alpha")
	base := time.Date(2026, 10, 1, 4, 0, 0, 0, time.UTC)
	for i, id := range []string{manualID("20261001T040000Z"), manualID("20261002T040000Z")} {
		putCopiedRun(t, f, "alpha", id, base.AddDate(0, 0, i), model.OutcomeDelivered, [][2]string{{"databases", "prod"}})
	}
	_, results := sweepOnce(t, e, base.AddDate(0, 0, 2))
	if results["alpha"].retentionErr != nil {
		t.Fatal(results["alpha"].retentionErr)
	}
	if got := len(runIDsOn(f)); got != 2 {
		t.Fatalf("a scheduled sweep trimmed the manual history to %d runs", got)
	}
}

// TestPolicyAndCapShareOneDeletionState (K5/K6): the policy's drops and the
// cap's evictions run in one pass over one deletion state — a run the policy
// deleted is never a cap candidate again, so it is deleted once and its bytes
// are subtracted once.
