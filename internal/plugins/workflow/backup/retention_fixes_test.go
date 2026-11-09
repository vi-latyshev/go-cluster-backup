package backup_test

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"strings"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/workflow/backup"
	"testing"
	"testing/synctest"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/catalog"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage/storagetest"
)

// twoArtifactConfig is baseConfig with a second source: two artifacts of one
// run, which is what a cap boundary between them needs.
func twoArtifactConfig(t *testing.T, storages map[string]any) map[string]any {
	t.Helper()
	cfg := manualConfig(t, storages)
	cfg["sources"].(map[string]any)["vpn2"] = sourceInstance("fake", "vpn", "wireguard-2", []string{"zstd3"}, "required", "plain")
	return cfg
}

// manualID is one manual run id for a fixture: the suffix is what the survey
// derives the kind from.
func manualID(stamp string) string { return stamp + "-manual" }

// TestManualCapRefusalRunsNoRetention (C4): a manual run refused by the cap on a
// storage deletes nothing there — its own keep-last never runs, because the copy
// it would replace was not made.
func TestManualCapRefusalRunsNoRetention(t *testing.T) {
	w := newWorld(t)
	cfg := manualConfig(t, map[string]any{"alpha": manualStorage(1)})
	withRetention(cfg, "alpha", keepLastN(10))
	withManualRetention(cfg, "alpha", keepLastN(1))
	e := w.newAssembly(t, cfg)
	f := w.storage(t, "alpha")
	base := time.Date(2026, 10, 1, 4, 0, 0, 0, time.UTC)
	ids := []string{manualID("20261001T040000Z"), manualID("20261002T040000Z"), manualID("20261003T040000Z")}
	for i, id := range ids {
		putCopiedRun(t, f, "alpha", id, base.AddDate(0, 0, i), model.OutcomeDelivered, [][2]string{{"databases", "prod"}})
	}
	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindManual})
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Storages) != 1 || !rep.Storages[0].CapExceeded {
		t.Fatalf("the run was not refused by the cap: %+v", rep.Storages)
	}
	if got := runIDsOn(f); len(got) != len(ids) {
		t.Fatalf("the cap-refused manual run deleted history: %v", got)
	}
}

// TestPartialDeleteStopsAndTheOtherCopySurvives (C3): the first copy's deletion
// fails after its manifest is gone; the sweep stops there, and the second copy —
// which the ledger no longer has a newer holder for — is not touched either.
func TestManualCapStopsAtTheManifest(t *testing.T) {
	w := newWorld(t)
	// The fake artifact is ~22 bytes; the manifest is several hundred. A cap of
	// 100 fits the artifact and not the manifest.
	cfg := manualConfig(t, map[string]any{"alpha": manualStorage(100)})
	e := w.newAssembly(t, cfg)
	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindManual})
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Storages) != 1 || !rep.Storages[0].CapExceeded {
		t.Fatalf("the manifest was not refused on the cap: %+v", rep.Storages)
	}
	if strings.Contains(rep.Storages[0].Error, "manifest.json") == false {
		t.Fatalf("the refusal does not name the manifest: %q", rep.Storages[0].Error)
	}
	f := w.storage(t, "alpha")
	if _, ok := f.Bytes("runs/" + rep.RunID + "/manifest.json"); ok {
		t.Fatal("the manifest was written past the cap")
	}
	if len(runIDsOn(f)) != 1 {
		t.Fatalf("the artifacts were not kept: %v", runIDsOn(f))
	}
}

// TestCommittedButUnverifiedWriteStaysCounted (C5): a Put that returned ok but
// whose Stat failed may have committed — the ledger keeps its bytes, so the next
// artifact is refused rather than sneaking past the cap.
func TestCommittedButUnverifiedWriteStaysCounted(t *testing.T) {
	w := newWorld(t)
	cfg := twoArtifactConfig(t, map[string]any{"alpha": manualStorage(30)})
	e := w.newAssembly(t, cfg)
	f := w.storage(t, "alpha")
	f.Fault = func(op storagetest.Op, key string, call int) error {
		if op == storagetest.OpStat && strings.HasSuffix(key, "wireguard.dat.zst") {
			return errors.New("stat exploded after the commit")
		}
		return nil
	}
	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindManual})
	if err != nil {
		t.Fatal(err)
	}
	byName := map[string]model.ArtifactResult{}
	for _, a := range rep.Artifacts {
		byName[a.Artifact.Name] = a
	}
	if byName["wireguard"].Outcome != model.ArtifactFailed {
		t.Fatalf("the unverified artifact did not fail: %+v", byName["wireguard"])
	}
	if byName["wireguard-2"].Outcome != model.ArtifactFailed {
		t.Fatalf("the second artifact was not refused: %+v", byName["wireguard-2"])
	}
	if len(rep.Storages) != 1 || !rep.Storages[0].CapExceeded {
		t.Fatalf("the committed-but-unverified write was not counted: %+v", rep.Storages)
	}
	// The first object really is on the storage: the reservation was not a guess.
	if _, ok := f.Bytes("runs/" + rep.RunID + "/vpn/wireguard.dat.zst"); !ok {
		t.Fatal("the fixture did not commit the first object")
	}
}

// TestCapEvictionHonoursThePolicyFloor (C8): the policy's min_keep is part of its
// Decision, and the protection ledger's cap eviction never cuts below it.
func TestCapEvictionHonoursThePolicyFloor(t *testing.T) {
	w := newWorld(t)
	storages := map[string]any{"alpha": storageInstance("fake", "keep_last")}
	floorCfg := map[string]any{"type": "keep_last", "config": map[string]any{
		"count": 10, "min_keep": 3, "count_incomplete": true}}
	storages["alpha"].(map[string]any)["retention"] = map[string]any{"scheduled": floorCfg}
	storages["alpha"].(map[string]any)["max_total_bytes"] = int64(1)
	cfg := manualConfig(t, storages)
	e := w.newAssembly(t, cfg)
	f := w.storage(t, "alpha")
	base := time.Date(2026, 10, 1, 4, 0, 0, 0, time.UTC)
	ids := []string{"r1", "r2", "r3", "r4", "r5"}
	for i, id := range ids {
		putCopiedRun(t, f, "alpha", id, base.AddDate(0, 0, i), model.OutcomeDelivered, [][2]string{{"vpn", "wireguard"}})
	}
	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	// The floor is the newest min_keep runs of the partition, the current run
	// included: r5 and r4 stay beside it, and the older three may go.
	idsOn := runIDsOn(f)
	for _, id := range []string{"r4", "r5"} {
		if !idsOn[id] {
			t.Fatalf("the cap cut below min_keep: r4..r5 = %v", idsOn)
		}
	}
	for _, id := range []string{"r1", "r2", "r3"} {
		if idsOn[id] {
			t.Fatalf("the eviction did not reclaim the runs outside the floor: %v", idsOn)
		}
	}
	if rep.Outcome == model.OutcomeFailed {
		t.Fatalf("the scheduled run failed on the cap: %+v", rep.Storages)
	}
}

// TestManualPartitionOnlyRunsOnTheManualPath: a scheduled run does not apply
// the manual keep-last (its own partition is the only one it runs), so a manual
// history is reclaimed by manual runs, not silently by the night.
func TestPolicyAndCapShareOneDeletionState(t *testing.T) {
	w := newWorld(t)
	storages := map[string]any{"alpha": storageInstance("fake", "keep_last")}
	storages["alpha"].(map[string]any)["retention"] = map[string]any{
		"scheduled": map[string]any{"type": "keep_last", "config": map[string]any{
			"count": 2, "min_keep": 1, "count_incomplete": true}},
	}
	storages["alpha"].(map[string]any)["max_total_bytes"] = int64(1)
	cfg := manualConfig(t, storages)
	e := w.newAssembly(t, cfg)
	f := w.storage(t, "alpha")
	base := time.Date(2026, 10, 1, 4, 0, 0, 0, time.UTC)
	for i, id := range []string{"r1", "r2", "r3", "r4"} {
		putCopiedRun(t, f, "alpha", id, base.AddDate(0, 0, i), model.OutcomeDelivered, [][2]string{{"vpn", "wireguard"}})
	}
	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome == model.OutcomeFailed {
		t.Fatalf("the scheduled run failed: %+v", rep.Storages)
	}
	seen := map[string]int{}
	for _, ev := range f.Events() {
		if ev.Op == storagetest.OpDelete && strings.HasSuffix(ev.Key, "manifest.json") {
			seen[ev.Key]++
		}
	}
	for key, n := range seen {
		if n != 1 {
			t.Fatalf("%s was deleted %d times: the cap re-ran the policy's deletions", key, n)
		}
	}
}

// TestCommittedButUnverifiedScheduledWriteStaysCounted (M-ledger): a scheduled
// write is reserved in the ledger before its Put too, so a Put that committed but
// whose Stat failed still counts against the cap.
func TestCommittedButUnverifiedScheduledWriteStaysCounted(t *testing.T) {
	w := newWorld(t)
	f := storagetest.NewFake()
	w.Storages["alpha"] = f
	// A pre-existing run fills the storage; the cap leaves room for a little more,
	// so only the new run's own write can push the ledger over.
	putCopiedRun(t, f, "alpha", "old", time.Now().Add(-time.Hour), model.OutcomeDelivered, [][2]string{{"databases", "prod"}})
	var oldBytes int64
	for _, k := range f.Keys() {
		if b, ok := f.Bytes(k); ok {
			oldBytes += int64(len(b))
		}
	}
	storages := map[string]any{"alpha": storageInstance("fake", "keep_last")}
	storages["alpha"].(map[string]any)["max_total_bytes"] = oldBytes + 10
	cfg := manualConfig(t, storages)
	var logBuf bytes.Buffer
	e := newAssemblyLogged(t, w, cfg, &logBuf)
	f.Fault = func(op storagetest.Op, key string, call int) error {
		if op == storagetest.OpStat && strings.HasSuffix(key, "wireguard.dat.zst") {
			return errors.New("stat exploded after the commit")
		}
		return nil
	}
	if _, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(logBuf.String(), "over the configured cap") {
		t.Fatalf("the committed-but-unverified scheduled write was not counted:\n%s", logBuf.String())
	}
}

// inventoryStorage is a storage whose full multipart inventory a test sets: the
// capability transfer uses to know an earlier writer's parts make occupancy
// unknown.
type inventoryStorage struct {
	*storagetest.Fake
	uploads []storage.StaleUpload
}

func (s *inventoryStorage) Inventory(context.Context) ([]storage.StaleUpload, error) {
	return s.uploads, nil
}

// TestForeignMultipartInventoryMakesOccupancyUnknown: a young or an undated
// in-progress upload of an earlier writer bills bytes no listing shows, so a
// manual run is refused on that storage; the contract lists no size, so unknown
// is used. A scheduled run still proceeds (soft cap).
func TestForeignMultipartInventoryMakesOccupancyUnknown(t *testing.T) {
	for name, at := range map[string]time.Time{"young": time.Now(), "undated": {}} {
		t.Run(name, func(t *testing.T) {
			w := newWorld(t)
			f := storagetest.NewFake()
			w.ExtraStorages["alpha"] = &inventoryStorage{Fake: f,
				uploads: []storage.StaleUpload{{Key: "runs/x/big.bin", UploadID: "u-1", Initiated: at}}}
			e := w.newAssembly(t, manualConfig(t, map[string]any{"alpha": manualStorage(1 << 40)}))
			rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindManual})
			if err != nil {
				t.Fatal(err)
			}
			if len(rep.Storages) != 1 || !rep.Storages[0].CapExceeded {
				t.Fatalf("a %s foreign upload did not make occupancy unknown: %+v", name, rep.Storages)
			}
			if keys := f.Keys(); len(keys) != 0 {
				t.Fatalf("the refused manual run wrote: %v", keys)
			}
		})
	}
	// A scheduled run is soft-capped and proceeds.
	w := newWorld(t)
	f := storagetest.NewFake()
	w.ExtraStorages["alpha"] = &inventoryStorage{Fake: f,
		uploads: []storage.StaleUpload{{Key: "runs/x/big.bin", UploadID: "u-1", Initiated: time.Now()}}}
	e := w.newAssembly(t, manualConfig(t, map[string]any{"alpha": manualStorage(1 << 40)}))
	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome == model.OutcomeFailed {
		t.Fatalf("a scheduled run failed on a foreign upload: %+v", rep.Storages)
	}
}

// TestUncertaintySurvivesTheSuccessor (iteration-4 critical): the uncertainty
// belongs to the catalog's lease record, not to the run that armed it. A dead
// holder's accepted provider operations may still land; a scheduled successor
// takes over and releases, and a manual run that follows must still see
// occupancy as unknown — no clock expires it, and a successor has no proof that
// the dead holder's late commit will not land.
func TestUncertaintySurvivesTheSuccessor(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newWorld(t)
		cfg := manualConfig(t, map[string]any{"alpha": manualStorage(1 << 40)})
		run := cfg["run"].(map[string]any)
		run["lease_poll"] = "5ms"
		run["lease_wait"] = "20ms"
		e := w.newAssembly(t, cfg)

		// A: killed by the kubelet just now; its accepted uploads may still commit.
		w.Catalog.Hold("A", model.RunKindScheduled, "infrastructure", "backup-a", "uid-a", time.Now())
		w.Catalog.SetTerminated(true, true)
		// B: a scheduled run takes over and releases.
		if rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled}); err != nil {
			t.Fatal(err)
		} else if rep.Outcome == model.OutcomeFailed {
			t.Fatalf("the successor failed: %+v", rep.Storages)
		}
		unc := w.Catalog.Uncertainty()
		if unc == nil || !slices.Contains(unc.RunIDs, "A") {
			t.Fatalf("the successor's release dropped the predecessor's uncertainty: %+v", unc)
		}
		// C: a manual run under the dead holder's uncertainty is refused on
		// occupancy it cannot trust.
		rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindManual})
		if err != nil {
			t.Fatal(err)
		}
		if len(rep.Storages) != 1 || !rep.Storages[0].CapExceeded {
			t.Fatalf("a manual run under the uncertainty was admitted: %+v", rep.Storages)
		}
		// D: time is not proof — the uncertainty does not expire.
		time.Sleep(time.Hour)
		rep, err = e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindManual})
		if err != nil {
			t.Fatal(err)
		}
		if len(rep.Storages) != 1 || !rep.Storages[0].CapExceeded {
			t.Fatalf("a manual run was admitted after the uncertainty aged: %+v %+v", rep.Outcome, rep.Storages)
		}
	})
}

// flakyInventory is the full multipart inventory with a transient failure: the
// transfer must retry it and, only when the retries are spent, leave occupancy
// unknown and say why.
type flakyInventory struct {
	*storagetest.Fake
	Fails   int
	entries []storage.StaleUpload
	calls   int
}

func (s *flakyInventory) Inventory(context.Context) ([]storage.StaleUpload, error) {
	s.calls++
	if s.calls <= s.Fails {
		return nil, pluginerrors.Transient(errors.New("HTTP 503"))
	}
	return s.entries, nil
}

// TestInventoryRetriesTransientFailures: the full inventory runs through the
// execution's retry pipeline. One 503 is retried; the successful empty answer leaves
// occupancy known, so a manual run is admitted.
func TestInventoryRetriesTransientFailures(t *testing.T) {
	w := newWorld(t)
	inv := &flakyInventory{Fake: storagetest.NewFake(), Fails: 1}
	w.ExtraStorages["alpha"] = inv
	e := w.newAssembly(t, manualConfig(t, map[string]any{"alpha": manualStorage(1 << 40)}))
	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindManual})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeDelivered {
		t.Fatalf("a transient inventory failure refused the manual run: %+v %+v", rep.Outcome, rep.Storages)
	}
	if inv.calls != 2 {
		t.Fatalf("the inventory was called %d times, want one retry", inv.calls)
	}
}

// TestInventoryExhaustionLeavesOccupancyUnknown: when the retries are spent the
// manual run is refused and the exhausted error is logged.
func TestInventoryExhaustionLeavesOccupancyUnknown(t *testing.T) {
	w := newWorld(t)
	inv := &flakyInventory{Fake: storagetest.NewFake(), Fails: 10}
	w.ExtraStorages["alpha"] = inv
	var logBuf bytes.Buffer
	e := newAssemblyLogged(t, w, manualConfig(t, map[string]any{"alpha": manualStorage(1 << 40)}), &logBuf)
	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindManual})
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Storages) != 1 || !rep.Storages[0].CapExceeded {
		t.Fatalf("an unreadable inventory did not leave occupancy unknown: %+v", rep.Storages)
	}
	if !strings.Contains(logBuf.String(), "multipart.inventory.failed") {
		t.Fatalf("the exhausted inventory error was not logged:\n%s", logBuf.String())
	}
}

func TestUncertaintyIsDecidedAtAcquisition(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newWorld(t)
		w.ExtraStorages["alpha"] = &slowListStorage{Fake: storagetest.NewFake(), delay: 200 * time.Millisecond}
		e := w.newAssembly(t, manualConfig(t, map[string]any{"alpha": manualStorage(1 << 40)}))
		w.Catalog.SetUncertainty(&catalog.Uncertainty{RunIDs: []string{"dead-run"}, Since: time.Now().UTC()})
		rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindManual})
		if err != nil {
			t.Fatal(err)
		}
		if len(rep.Storages) != 1 || !rep.Storages[0].CapExceeded {
			t.Fatalf("a survey taken under the uncertainty was treated as known: %+v", rep.Storages)
		}
	})
}

func TestSuccessorNeverClearsAnInheritedUncertainty(t *testing.T) {
	w := newWorld(t)
	w.ExtraStorages["alpha"] = asyncStorage{Fake: storagetest.NewFake()}
	e := w.newAssembly(t, manualConfig(t, map[string]any{"alpha": manualStorage(1 << 40)}))
	w.Catalog.SetUncertainty(&catalog.Uncertainty{RunIDs: []string{"dead-run"}, Since: time.Now().UTC()})
	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome == model.OutcomeFailed {
		t.Fatalf("a scheduled run failed on an inherited uncertainty: %+v", rep.Storages)
	}
	unc := w.Catalog.Uncertainty()
	if unc == nil || !slices.Contains(unc.RunIDs, "dead-run") {
		t.Fatalf("the successor cleared the dead holder's uncertainty: %+v", unc)
	}
	if slices.Contains(unc.RunIDs, rep.RunID) {
		t.Fatalf("the settled successor kept its own id in the pending set: %+v", unc)
	}
}

// TestCapEvictionIsClosedWhileOccupancyIsUnknown: with the record's uncertainty
// standing, the cap total is not trustworthy — bytes a late commit may add are
// uncounted — so the scheduled run reports the overflow and deletes nothing.
func TestCapEvictionIsClosedWhileOccupancyIsUnknown(t *testing.T) {
	w := newWorld(t)
	cfg := manualConfig(t, map[string]any{"alpha": manualStorage(1)})
	withRetention(cfg, "alpha", keepLastN(10))
	var logBuf bytes.Buffer
	e := newAssemblyLogged(t, w, cfg, &logBuf)
	f := w.storage(t, "alpha")
	base := time.Date(2026, 10, 1, 4, 0, 0, 0, time.UTC)
	putCopiedRun(t, f, "alpha", "old", base, model.OutcomeDelivered, [][2]string{{"databases", "prod"}})
	w.Catalog.SetUncertainty(&catalog.Uncertainty{RunIDs: []string{"dead-run"}, Since: base})
	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome == model.OutcomeFailed {
		t.Fatalf("a scheduled run failed on an unknown occupancy: %+v", rep.Storages)
	}
	if !runIDsOn(f)["old"] {
		t.Fatalf("a verified run was evicted on an unknown occupancy: %v", runIDsOn(f))
	}
	if !strings.Contains(logBuf.String(), "storage.cap.unknown") {
		t.Fatalf("the closed eviction was not reported:\n%s", logBuf.String())
	}
}
