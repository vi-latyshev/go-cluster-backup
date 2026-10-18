package engine

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/storage/storagetest"
)

// These three tests pin the same rule from three angles: the retention floor may
// credit the run being written as a holder of an artifact ONLY where a restore
// can reach it — the artifact verified there AND the run's manifest saved and
// verified there. Before the fix the run was credited as soon as the uploads
// finished, so the previous run was dropped while the new run's manifest was
// still unwritten; a manifest write that then failed left objects with no
// manifest, unfetchable and unprotected, and the whole history of the artifact
// was lost.

// seedProdRun plants one finished run holding databases/prod, the shape every
// fixture here rotates against.
func seedProdRun(t *testing.T, f *storagetest.Fake, storageName string) (string, time.Time) {
	t.Helper()
	at := time.Now().UTC().Add(-time.Hour)
	id := at.Format("20060102T150405Z")
	putCopiedRun(t, f, storageName, id, at, model.OutcomeDelivered, [][2]string{{"databases", "prod"}})
	return id, at
}

// seedOlderVerifiedRun plants an older verified run holding a different
// artifact. It exists so the RUN-level floor ("leave at least one verified run")
// is permissive: with it present, only the per-artifact pin stands between the
// policy and the predecessor's copy, which is the rule these tests pin.
func seedOlderVerifiedRun(t *testing.T, f *storagetest.Fake, storageName string) {
	t.Helper()
	at := time.Now().UTC().Add(-2 * time.Hour)
	putCopiedRun(t, f, storageName, at.Format("20060102T150405Z"), at, model.OutcomeDelivered,
		[][2]string{{"volumes", "uploads"}})
}

// prodRunConfig is one storage keeping the newest run, one source producing
// databases/prod, and a restore section so the artifact can be fetched back.
func prodRunConfig(t *testing.T, w *world, storages map[string]any) map[string]any {
	t.Helper()
	cfg := baseConfig(t, storages)
	cfg["sources"] = map[string]any{"db": sourceInstance("fake", "databases", "prod", []string{"zstd3"}, "required", "plain")}
	withRestore(t, cfg, "alpha")
	return cfg
}

// TestManifestWriteFailureKeepsThePredecessor: the manifest Put fails on the
// storage. The current run's objects are there but no manifest records them, so
// the run must NOT be credited as a holder of the artifact — the previous run,
// whose copy a restore can still find, stays.
//
// An older verified run sits beside the predecessor so the RUN-level floor is
// permissive (dropping the predecessor would leave a verified run): only the
// per-artifact pin stands between the policy and the predecessor's copy. That
// is the rule markCurrent feeds.
//
// Mutant: credit the run whatever the manifest did (drop the manifestSaved
// guard in markCurrent) — the predecessor is dropped and Fetch(newest-ok) can
// no longer reach the artifact.
func TestManifestWriteFailureKeepsThePredecessor(t *testing.T) {
	w := newWorld(t)
	cfg := prodRunConfig(t, w, map[string]any{"alpha": storageInstance("fake", "keep_last")})
	withRetention(cfg, "alpha", keepLastN(1))
	e := w.newEngine(t, cfg)
	f := w.storage(t, "alpha")
	prevID, _ := seedProdRun(t, f, "alpha")
	seedOlderVerifiedRun(t, f, "alpha")

	f.Fault = func(op storagetest.Op, key string, _ int) error {
		if op == storagetest.OpPut && strings.HasSuffix(key, "manifest.json") {
			return plugin.Permanent(errors.New("the bucket refused the manifest"))
		}
		return nil
	}

	if _, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled}); err != nil {
		t.Fatal(err)
	}
	if !runIDsOn(f)[prevID] {
		t.Fatalf("the predecessor was dropped though the new run has no manifest: %v", runIDsOn(f))
	}

	var buf bytes.Buffer
	fr, err := e.Fetch(context.Background(), FetchRequest{Newest: true, Artifact: "prod"}, &buf)
	if err != nil {
		t.Fatalf("Fetch(newest-ok) cannot reach the artifact: %v", err)
	}
	if fr.RunID != prevID {
		t.Fatalf("fetch resolved to %s, want the predecessor %s", fr.RunID, prevID)
	}
}

// TestFailedSourceKeepsTheRestorableCopy: the night's source for the artifact
// fails while another source delivers, so the run reaches the storage and
// writes its manifest — but the failed artifact has no copy. The run must not be
// credited as a holder of the artifact it never made, and the previous run's
// copy stays restorable. An older verified run keeps the run-level floor
// permissive, so the per-artifact pin is the rule under test.
//
// Mutant: credit the run for every planned artifact regardless of what verified
// — the failed artifact releases the pin and the copy is dropped.
func TestFailedSourceKeepsTheRestorableCopy(t *testing.T) {
	w := newWorld(t)
	w.sources["db"] = &fakeSource{dumpErr: errors.New("the database refused the connection")}
	cfg := prodRunConfig(t, w, map[string]any{"alpha": storageInstance("fake", "keep_last")})
	// A second source that delivers, so the run is a run a storage actually
	// holds: without it the failed night writes no manifest and the fixture
	// would prove nothing.
	cfg["sources"].(map[string]any)["vol"] = sourceInstance("fake", "volumes", "uploads", []string{"zstd3"}, "required", "plain")
	withRetention(cfg, "alpha", keepLastN(1))
	e := w.newEngine(t, cfg)
	f := w.storage(t, "alpha")
	prevID, _ := seedProdRun(t, f, "alpha")
	seedOlderVerifiedRun(t, f, "alpha")

	if _, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled}); err != nil {
		t.Fatal(err)
	}
	if !runIDsOn(f)[prevID] {
		t.Fatalf("the predecessor was dropped though the artifact failed: %v", runIDsOn(f))
	}

	var buf bytes.Buffer
	fr, err := e.Fetch(context.Background(), FetchRequest{Newest: true, Artifact: "prod"}, &buf)
	if err != nil {
		t.Fatalf("Fetch(newest-ok) cannot reach the artifact: %v", err)
	}
	if fr.RunID != prevID {
		t.Fatalf("fetch resolved to %s, want the predecessor %s", fr.RunID, prevID)
	}
}

// TestOneStorageVerificationKeepsTheOthersCopy: the new copy of the artifact is
// verified on alpha only — beta's upload of it keeps failing. Beta must keep its
// own predecessor: the per-artifact pin is per storage, so a newer copy on
// alpha does not release beta's copy of the same name.
//
// A second artifact delivers to both storages, so beta really holds the run and
// writes its manifest; the pin under test is the artifact's, not the manifest's.
//
// Mutant: credit the run on every storage where the artifact verified anywhere
// (a global ledger) — beta's predecessor is dropped and beta is left with no
// copy of the artifact.
func TestOneStorageVerificationKeepsTheOthersCopy(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{
		"alpha": storageInstance("fake", "keep_last"),
		"beta":  storageInstance("fake", "keep_last"),
	})
	cfg["sources"] = map[string]any{
		"db":  sourceInstance("fake", "databases", "prod", []string{"zstd3"}, "required", "plain"),
		"vol": sourceInstance("fake", "volumes", "uploads", []string{"zstd3"}, "required", "plain"),
	}
	withRestore(t, cfg, "alpha", "beta")
	withRetention(cfg, "alpha", keepLastN(1))
	withRetention(cfg, "beta", keepLastN(1))
	e := w.newEngine(t, cfg)
	alpha, beta := w.storage(t, "alpha"), w.storage(t, "beta")
	prevID, prevAt := seedProdRun(t, alpha, "alpha")
	putCopiedRun(t, beta, "beta", prevID, prevAt, model.OutcomeDelivered, [][2]string{{"databases", "prod"}})

	// The artifact's upload to beta keeps failing — transiently, so the second
	// artifact still lands there and beta writes its manifest.
	beta.Fault = func(op storagetest.Op, key string, _ int) error {
		if op == storagetest.OpPut && strings.Contains(key, "databases/prod") {
			return plugin.Transient(errors.New("beta refused the object"))
		}
		return nil
	}

	if _, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled}); err != nil {
		t.Fatal(err)
	}
	if runIDsOn(alpha)[prevID] {
		t.Fatalf("alpha kept its predecessor though it holds a newer verified copy: %v", runIDsOn(alpha))
	}
	if !runIDsOn(beta)[prevID] {
		t.Fatalf("beta dropped its only copy of the artifact: %v", runIDsOn(beta))
	}
}
