package assembly

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/human"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/inventory"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/protection"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/retention"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage/guard"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage/storagetest"
)

// putRun fabricates one run on a fake storage the way an earlier night left it:
// an artifact object, and — when verified — a manifest that calls this storage
// verified.
func putRun(t *testing.T, f *storagetest.Fake, storageName, runID string, at time.Time, verified bool) {
	t.Helper()
	f.PutObject("runs/"+runID+"/artifact.bin", []byte(runID), at)
	if !verified {
		return
	}
	rep := model.RunReport{
		Schema: model.ManifestSchema, RunID: runID, Kind: kindOfID(runID), StartedAt: at, Outcome: model.OutcomeDelivered,
		Storages: []model.StorageResult{{Storage: storageName, Outcome: model.StorageVerified}},
	}
	f.PutObject("runs/"+runID+"/manifest.json", mustMarshal(t, rep), at)
}

// putFailedRun fabricates a run that came out failed — a required artifact of
// it failed — while its storage outcome still claims this storage verified: the
// shape that must not stand in for "the last verified run".
func putFailedRun(t *testing.T, f *storagetest.Fake, storageName, runID string, at time.Time) {
	t.Helper()
	f.PutObject("runs/"+runID+"/artifact.bin", []byte(runID), at)
	rep := model.RunReport{
		Schema: model.ManifestSchema, RunID: runID, Kind: kindOfID(runID), StartedAt: at, Outcome: model.OutcomeFailed,
		Storages: []model.StorageResult{{Storage: storageName, Outcome: model.StorageVerified}},
		Artifacts: []model.ArtifactResult{{Artifact: model.Artifact{Name: "db", Group: "databases",
			Importance: model.ImportanceRequired}, Outcome: model.ArtifactFailed, Error: "boom"}},
	}
	f.PutObject("runs/"+runID+"/manifest.json", mustMarshal(t, rep), at)
}

// runIDsOn lists the run directories a fake currently holds.
func runIDsOn(f *storagetest.Fake) map[string]bool {
	out := map[string]bool{}
	for _, key := range f.Keys() {
		rest, ok := strings.CutPrefix(key, "runs/")
		if !ok {
			continue
		}
		if id, _, found := strings.Cut(rest, "/"); found {
			out[id] = true
		}
	}
	return out
}

// sweepState is what one storage's sweep produced, as a test reads it.
type sweepState struct {
	result       model.StorageResult
	retentionErr error
}

// sweepOnce runs one non-evicting retention sweep over every storage of the
// assembly, the way the end of a run does, and returns what each storage saw.
func sweepOnce(t *testing.T, e *Assembly, now time.Time) (map[string]inventory.Survey, map[string]*sweepState) {
	t.Helper()
	ctx := context.Background()
	surveys := map[string]inventory.Survey{}
	results := map[string]*sweepState{}
	destinations := make([]guard.Destination, 0, len(e.storages))
	policies := map[string]map[model.RunKind]retention.Retention{}
	caps := map[string]int64{}
	for _, sb := range e.storages {
		destinations = append(destinations, sb.dest)
		policies[sb.name] = sb.retention
		if c := sb.cfg.MaxTotalBytes.Int64(); c > 0 {
			caps[sb.name] = c
		}
		results[sb.name] = &sweepState{}
		surveys[sb.name] = e.inventory.SurveyStorage(ctx, "runs/", inventory.Storage{
			Name: sb.name, Reader: sb.dest.Port(), CallParallelism: sb.cfg.CallParallelism,
			Log: e.deps("storage", sb.cfg.Type, sb.name).Log,
		})
	}
	staleCutoff := now.UTC()
	if e.cfg.Run != nil {
		staleCutoff = staleCutoff.Add(-(e.cfg.Run.Deadline.Std() + e.cfg.Run.FinishAllowance.Std()))
	}
	ledger := protection.NewLedger(protection.LedgerOptions{
		Destinations: destinations, Policies: policies, Caps: caps,
		StaleCutoff: staleCutoff,
		Retry:       e.retry,
	})
	ledger.Start(surveys)
	for _, sb := range e.storages {
		res := ledger.Sweep(ctx, protection.Sweep{RunStart: now, Kind: model.RunKindScheduled, Storage: sb.name, Survey: surveys[sb.name]})
		st := results[sb.name]
		st.result.Kept, st.result.Deleted, st.result.Freed = res.Kept, res.Deleted, res.Freed
		if res.Err != nil {
			st.retentionErr = res.Err
		}
	}
	return surveys, results
}

func policyFor(t *testing.T, w *world, typ string, cfg map[string]any) retention.Retention {
	t.Helper()
	raw, err := jsonMarshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	p, err := instances.Build(context.Background(), w.Registry, retention.Kind, typ, raw,
		instances.Deps{Instance: typ, Log: testLogger()})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func expectedKept(t *testing.T, pol retention.Retention, now time.Time, sur inventory.Survey) int {
	t.Helper()
	var runs []model.StoredRun
	for _, id := range sortedKeys(sur.Runs) {
		runs = append(runs, sur.Runs[id].Stored)
	}
	d, err := pol.Decide(context.Background(), retention.DecideRequest{Now: now, Runs: runs})
	if err != nil {
		t.Fatal(err)
	}
	return len(d.Keep)
}

func gfs7plus4Cfg() map[string]any {
	return map[string]any{"type": "gfs", "config": map[string]any{
		"daily": 7, "weekly": 4, "monthly": 0, "timezone": "UTC",
		"min_keep": 1, "count_incomplete": true,
	}}
}

// gfsDaily1 keeps only the newest run of the newest day: two runs on one day
// leave exactly one, which is what makes the same-day rotation observable.
func gfsDaily1() map[string]any {
	return map[string]any{"type": "gfs", "config": map[string]any{
		"daily": 1, "weekly": 0, "monthly": 0, "timezone": "UTC",
		"min_keep": 1, "count_incomplete": true,
	}}
}

func keepLastN(n int) map[string]any {
	return map[string]any{"type": "keep_last", "config": map[string]any{
		"count": n, "min_keep": 1, "count_incomplete": true,
	}}
}

// guardConfig is the guard tests' fixture: the storages by name, and the
// scheduled retention policy of each, keyed by the same storage name — a policy
// belongs to its storage, one per run kind, so there is no name for one to be
// referenced by.
func guardConfig(t *testing.T, storages, retentions map[string]any) map[string]any {
	t.Helper()
	for name, policy := range retentions {
		storages[name].(map[string]any)["retention"] = map[string]any{"scheduled": policy}
	}
	return map[string]any{
		"layout":   configLayout(),
		"storages": storages,
	}
}

// TestGuardDaySequenceNoGrowth simulates 30 consecutive days: storage alpha
// keeps gfs 7+4, storage beta keeps the last 3. Every day each storage must end
// exactly at its own policy's set — no run survives past its policy, and a run
// alpha drops that beta let go long ago is not protected by the guard.
func TestGuardDaySequenceNoGrowth(t *testing.T) {
	w := newWorld(t)
	cfg := guardConfig(t,
		map[string]any{
			"alpha": storageInstance("fake", "keep_last"),
			"beta":  storageInstance("fake", "keep_last"),
		},
		map[string]any{"alpha": gfs7plus4Cfg(), "beta": keepLastN(3)},
	)
	e := w.newAssembly(t, cfg)
	gfsPolicy := policyFor(t, w, "gfs", gfs7plus4Cfg()["config"].(map[string]any))
	keepPolicy := policyFor(t, w, "keep_last", keepLastN(3)["config"].(map[string]any))

	alpha, beta := w.storage(t, "alpha"), w.storage(t, "beta")
	base := time.Date(2026, 9, 1, 4, 0, 0, 0, time.UTC)
	for day := range 30 {
		at := base.AddDate(0, 0, day)
		id := fmt.Sprintf("day-%02d", day)
		putRun(t, alpha, "alpha", id, at, true)
		putRun(t, beta, "beta", id, at, true)

		surveys, results := sweepOnce(t, e, at)
		if got, want := len(runIDsOn(alpha)), expectedKept(t, gfsPolicy, at, surveys["alpha"]); got != want {
			t.Fatalf("day %d: alpha holds %d runs, its policy keeps %d", day, got, want)
		}
		if got, want := len(runIDsOn(beta)), expectedKept(t, keepPolicy, at, surveys["beta"]); got != want {
			t.Fatalf("day %d: beta holds %d runs, its policy keeps %d", day, got, want)
		}
		if results["alpha"].retentionErr != nil || results["beta"].retentionErr != nil {
			t.Fatalf("day %d: retention errors %v / %v", day, results["alpha"].retentionErr, results["beta"].retentionErr)
		}
	}
	if got := len(runIDsOn(beta)); got != 3 {
		t.Fatalf("beta holds %d runs at the end, want exactly 3", got)
	}
	if got := len(runIDsOn(alpha)); got > 11 {
		t.Fatalf("alpha holds %d runs at the end, more than gfs 7+4 can keep", got)
	}
	// The regression case: the oldest run was dropped by beta early and by
	// alpha eventually — the guard must not have frozen it on alpha.
	if runIDsOn(alpha)["day-00"] || runIDsOn(beta)["day-00"] {
		t.Fatalf("day-00 survived retention: alpha=%v beta=%v", runIDsOn(alpha), runIDsOn(beta))
	}
}

// TestGuardSingleStorageRotates: one storage, eight verified runs, policy keeps
// five — three deletions happen, and the floor keeps the newest five.
func TestGuardSingleStorageRotates(t *testing.T) {
	w := newWorld(t)
	cfg := guardConfig(t,
		map[string]any{"alpha": storageInstance("fake", "keep_last")},
		map[string]any{"alpha": keepLastN(5)},
	)
	e := w.newAssembly(t, cfg)
	f := w.storage(t, "alpha")
	base := time.Date(2026, 9, 1, 4, 0, 0, 0, time.UTC)
	for day := range 8 {
		putRun(t, f, "alpha", fmt.Sprintf("day-%02d", day), base.AddDate(0, 0, day), true)
	}
	_, results := sweepOnce(t, e, base.AddDate(0, 0, 7))
	if got := results["alpha"].result.Deleted; len(got) != 3 {
		t.Fatalf("deleted %v, want the 3 oldest runs", got)
	}
	ids := runIDsOn(f)
	if len(ids) != 5 {
		t.Fatalf("single storage holds %d runs, want 5", len(ids))
	}
	for day := range 3 {
		if ids[fmt.Sprintf("day-%02d", day)] {
			t.Fatalf("day-%02d was not rotated away: %v", day, ids)
		}
	}
}

// TestGuardLastVerifiedRunSurvives: the only verified run anywhere is the oldest
// one, and the policy would drop it in favour of an unverified newer run. The
// floor refuses, the run stays, and the protection log says why.
func TestGuardLastVerifiedRunSurvives(t *testing.T) {
	w := newWorld(t)
	cfg := guardConfig(t,
		map[string]any{"alpha": storageInstance("fake", "keep_last")},
		map[string]any{"alpha": keepLastN(1)},
	)
	raw, err := jsonMarshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var logBuf bytes.Buffer
	e, err := New(context.Background(), raw, nil, Deps{Factories: w.Registry, Loader: w.Loader, Log: slog.New(human.NewHandler(&logBuf, slog.LevelInfo))})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close(context.Background()) })
	f := w.storage(t, "alpha")
	base := time.Date(2026, 9, 1, 4, 0, 0, 0, time.UTC)
	putRun(t, f, "alpha", "day-good", base, true)
	putRun(t, f, "alpha", "day-unverified", base.AddDate(0, 0, 1), false)

	_, results := sweepOnce(t, e, base.AddDate(0, 0, 2))
	if got := results["alpha"].result.Deleted; len(got) != 0 {
		t.Fatalf("the last verified run was deleted: %v", got)
	}
	if !runIDsOn(f)["day-good"] {
		t.Fatal("the last verified run did not survive")
	}
	if got := results["alpha"].result.Kept; got != 2 {
		t.Fatalf("kept %d runs, want both", got)
	}
	if !strings.Contains(logBuf.String(), "keeping the last verified run") {
		t.Fatalf("the log does not explain the refusal:\n%s", logBuf.String())
	}
}

// TestGuardSharedVerifiedCopyDeletedOnce: one verified run on two storages, both
// policies drop it — the first storage may delete its copy (the second remains),
// the second is refused. Exactly one copy goes.
func TestGuardSharedVerifiedCopyDeletedOnce(t *testing.T) {
	w := newWorld(t)
	cfg := guardConfig(t,
		map[string]any{
			"alpha": storageInstance("fake", "keep_last"),
			"beta":  storageInstance("fake", "keep_last"),
		},
		map[string]any{"alpha": keepLastN(1), "beta": keepLastN(1)},
	)
	e := w.newAssembly(t, cfg)
	base := time.Date(2026, 9, 1, 4, 0, 0, 0, time.UTC)
	for name, f := range w.Storages {
		putRun(t, f, name, "day-good", base, true)
		putRun(t, f, name, "day-unverified", base.AddDate(0, 0, 1), false)
	}
	_, results := sweepOnce(t, e, base.AddDate(0, 0, 2))
	if got := len(results["alpha"].result.Deleted) + len(results["beta"].result.Deleted); got != 1 {
		t.Fatalf("deleted %d copies of the only verified run, want exactly 1", got)
	}
	survivors := 0
	for _, name := range []string{"alpha", "beta"} {
		if runIDsOn(w.storage(t, name))["day-good"] {
			survivors++
		}
	}
	if survivors != 1 {
		t.Fatalf("%d verified copies survived, want exactly 1", survivors)
	}
}

// TestGuardUnreachableStoragePeerStillRotates: one storage's survey fails; the
// other still rotates down to its policy as long as it keeps a verified run.
func TestGuardUnreachableStoragePeerStillRotates(t *testing.T) {
	w := newWorld(t)
	down := storagetest.NewFake()
	down.Fault = func(storagetest.Op, string, int) error { return pluginerrors.Transient(fmt.Errorf("unreachable")) }
	w.Storages["alpha"] = down
	cfg := guardConfig(t,
		map[string]any{
			"alpha": storageInstance("fake", "keep_last"),
			"beta":  storageInstance("fake", "keep_last"),
		},
		map[string]any{"alpha": keepLastN(3), "beta": keepLastN(3)},
	)
	e := w.newAssembly(t, cfg)
	beta := w.storage(t, "beta")
	base := time.Date(2026, 9, 1, 4, 0, 0, 0, time.UTC)
	for day := range 4 {
		putRun(t, beta, "beta", fmt.Sprintf("day-%02d", day), base.AddDate(0, 0, day), true)
	}
	_, results := sweepOnce(t, e, base.AddDate(0, 0, 4))
	if results["alpha"].retentionErr == nil {
		t.Fatal("the unreachable storage reported no retention error")
	}
	if got := len(results["beta"].result.Deleted); got != 1 {
		t.Fatalf("beta deleted %d runs while alpha was down, want 1", got)
	}
	if got := len(runIDsOn(beta)); got != 3 {
		t.Fatalf("beta holds %d runs, want 3", got)
	}
}

// TestGuardUnreachableStorageCountsAsHoldingNothing: the unreachable storage may
// hold a copy of the last verified run, but the floor covers only surveyed
// storages — the reachable one must not delete its only verified run.
func TestGuardUnreachableStorageCountsAsHoldingNothing(t *testing.T) {
	w := newWorld(t)
	down := storagetest.NewFake()
	down.Fault = func(storagetest.Op, string, int) error { return pluginerrors.Transient(fmt.Errorf("unreachable")) }
	w.Storages["alpha"] = down
	cfg := guardConfig(t,
		map[string]any{
			"alpha": storageInstance("fake", "keep_last"),
			"beta":  storageInstance("fake", "keep_last"),
		},
		map[string]any{"alpha": keepLastN(1), "beta": keepLastN(1)},
	)
	e := w.newAssembly(t, cfg)
	beta := w.storage(t, "beta")
	base := time.Date(2026, 9, 1, 4, 0, 0, 0, time.UTC)
	putRun(t, beta, "beta", "day-good", base, true)
	putRun(t, beta, "beta", "day-unverified", base.AddDate(0, 0, 1), false)

	_, results := sweepOnce(t, e, base.AddDate(0, 0, 2))
	if got := results["beta"].result.Deleted; len(got) != 0 {
		t.Fatalf("the only surveyed verified run was deleted: %v", got)
	}
	if !runIDsOn(beta)["day-good"] {
		t.Fatal("the last surveyed verified run did not survive")
	}
}

// TestGuardFailedRunsAreNotVerifiedCopies: a week of runs that came out failed
// must never rotate out the last complete run — a failed run is missing
// something required, whatever its storage outcome says.
func TestGuardFailedRunsAreNotVerifiedCopies(t *testing.T) {
	w := newWorld(t)
	cfg := guardConfig(t,
		map[string]any{"alpha": storageInstance("fake", "keep_last")},
		map[string]any{"alpha": keepLastN(3)},
	)
	e := w.newAssembly(t, cfg)
	f := w.storage(t, "alpha")
	base := time.Date(2026, 9, 1, 4, 0, 0, 0, time.UTC)
	putRun(t, f, "alpha", "day-good", base, true)
	for day := range 7 {
		putFailedRun(t, f, "alpha", fmt.Sprintf("day-%02d", day), base.AddDate(0, 0, day+1))
	}

	_, results := sweepOnce(t, e, base.AddDate(0, 0, 8))
	if !runIDsOn(f)["day-good"] {
		t.Fatalf("a week of failed runs rotated out the last complete run: %v", runIDsOn(f))
	}
	if got := results["alpha"].result.Deleted; len(got) != 4 {
		t.Fatalf("deleted %v, want the four failed runs the policy drops", got)
	}
	if got := results["alpha"].result.Kept; got != 4 {
		t.Fatalf("kept %d runs, want the three newest and the last complete one", got)
	}
}

// TestGuardDeletesTheManifestFirst: when a run's deletion stops half-way, the
// manifest is already gone — a survey reads that as "not verified", which is
// true, instead of a manifest promising objects that no longer exist.
func TestGuardDeletesTheManifestFirst(t *testing.T) {
	w := newWorld(t)
	cfg := guardConfig(t,
		map[string]any{"alpha": storageInstance("fake", "keep_last")},
		map[string]any{"alpha": keepLastN(1)},
	)
	e := w.newAssembly(t, cfg)
	f := w.storage(t, "alpha")
	base := time.Date(2026, 9, 1, 4, 0, 0, 0, time.UTC)
	putRun(t, f, "alpha", "day-old", base, true)
	putRun(t, f, "alpha", "day-new", base.AddDate(0, 0, 1), true)
	f.Fault = func(op storagetest.Op, key string, _ int) error {
		if op == storagetest.OpDelete && strings.HasSuffix(key, "/artifact.bin") {
			return pluginerrors.Transient(fmt.Errorf("delete refused"))
		}
		return nil
	}

	_, results := sweepOnce(t, e, base.AddDate(0, 0, 2))
	if results["alpha"].retentionErr == nil {
		t.Fatal("a failed deletion was not reported")
	}
	// The order itself is the invariant: the manifest is the first thing gone.
	firstDelete := ""
	for _, ev := range f.Events() {
		if ev.Op == storagetest.OpDelete {
			firstDelete = ev.Key
			break
		}
	}
	if firstDelete != "runs/day-old/manifest.json" {
		t.Fatalf("the first deletion was %q, want the manifest of the dropped run", firstDelete)
	}
	if _, ok := f.Bytes("runs/day-old/manifest.json"); ok {
		t.Fatal("the manifest of a half-deleted run is still there")
	}
	if _, ok := f.Bytes("runs/day-old/artifact.bin"); !ok {
		t.Fatal("fixture drift: the deletion was supposed to fail on the object")
	}
}

// TestGuardCurrentRunCountsAsItsOwnCopy: the run being written is a verified
// copy on its storage, so the night's previous run rotates away in the same run
// — the storage does not freeze forever around it.
func TestGuardCapEvictionYieldsToTheFloor(t *testing.T) {
	w := newWorld(t)
	storages := map[string]any{"alpha": storageInstance("fake", "keep_last")}
	// A cap of one byte: every run is over it, so the protection ledger's eviction wants
	// everything gone.
	storages["alpha"].(map[string]any)["max_total_bytes"] = int64(1)
	cfg := guardConfig(t, storages, map[string]any{"alpha": keepLastN(10)})
	raw, err := jsonMarshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var logBuf bytes.Buffer
	e, err := New(context.Background(), raw, nil, Deps{Factories: w.Registry, Loader: w.Loader, Log: slog.New(human.NewHandler(&logBuf, slog.LevelInfo))})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close(context.Background()) })
	f := w.storage(t, "alpha")
	base := time.Date(2026, 10, 1, 4, 0, 0, 0, time.UTC)
	// Each run holds the newest copy of its own artifact, so both are pinned.
	putCopiedRun(t, f, "alpha", "old", base, model.OutcomeDelivered, [][2]string{{"volumes", "uploads"}})
	putCopiedRun(t, f, "alpha", "new", base.AddDate(0, 0, 1), model.OutcomeDelivered, [][2]string{{"databases", "prod"}})

	_, results := sweepOnce(t, e, base.AddDate(0, 0, 2))
	if !runIDsOn(f)["old"] || !runIDsOn(f)["new"] {
		t.Fatalf("a pinned run was deleted: %v", runIDsOn(f))
	}
	if got := results["alpha"].result.Deleted; len(got) != 0 {
		t.Fatalf("deleted %v, want nothing (both runs are pinned)", got)
	}
	if !strings.Contains(logBuf.String(), "over the configured cap") {
		t.Fatalf("the cap overflow is not reported:\n%s", logBuf.String())
	}
}

// TestGuardDeliveredRunSurvivesFailedNights (truth of the run-level floor):
// one delivered run followed by failed nights. The failed runs are not verified
// runs, so the floor has exactly one verified run to protect — the delivered
// one — and it refuses to drop it.
func TestGuardDeliveredRunSurvivesFailedNights(t *testing.T) {
	w := newWorld(t)
	cfg := guardConfig(t,
		map[string]any{"alpha": storageInstance("fake", "keep_last")},
		map[string]any{"alpha": keepLastN(1)},
	)
	raw, err := jsonMarshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var logBuf bytes.Buffer
	e, err := New(context.Background(), raw, nil, Deps{Factories: w.Registry, Loader: w.Loader, Log: slog.New(human.NewHandler(&logBuf, slog.LevelInfo))})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close(context.Background()) })
	f := w.storage(t, "alpha")
	base := time.Date(2026, 10, 1, 4, 0, 0, 0, time.UTC)
	// The failed nights carry the database forward too, so the per-artifact
	// floor is not what protects the delivered run here — the run-level rule
	// is, and this test binds exactly that.
	putCopiedRun(t, f, "alpha", "delivered", base, model.OutcomeDelivered,
		[][2]string{{"databases", "prod"}})
	for night := 1; night <= 3; night++ {
		putCopiedRun(t, f, "alpha", fmt.Sprintf("failed-%d", night), base.AddDate(0, 0, night),
			model.OutcomeFailed, [][2]string{{"databases", "prod"}})
	}

	_, results := sweepOnce(t, e, base.AddDate(0, 0, 3))
	ids := runIDsOn(f)
	if !ids["delivered"] {
		t.Fatalf("the last verified run was deleted: %v", ids)
	}
	if len(ids) != 2 || !ids["failed-3"] {
		t.Fatalf("storage holds %v, want the newest failed run and the protected delivered one", ids)
	}
	if got := results["alpha"].result.Deleted; len(got) != 2 {
		t.Fatalf("deleted %v, want the two superseded failed runs", got)
	}
	if !strings.Contains(logBuf.String(), "deleting it would leave no verified run anywhere") {
		t.Fatalf("the log does not carry the run-level reason:\n%s", logBuf.String())
	}
}

// TestGuardNewestArtifactCopyOutlivesAStandingFailed is the decision-1 shape:
// EVERY night is failed (one undeclared volume fails each run while the
// databases are delivered), so no run is a verified run and the run-level
// floor protects nothing. The volume's newest verified copy sits in an old run
// that the count policy would drop; the per-artifact floor must refuse, and it
// must release once a later run carries a verified copy of the volume again.
func TestGuardNewestArtifactCopyOutlivesAStandingFailed(t *testing.T) {
	w := newWorld(t)
	cfg := guardConfig(t,
		map[string]any{"alpha": storageInstance("fake", "keep_last")},
		map[string]any{"alpha": keepLastN(1)},
	)
	raw, err := jsonMarshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var logBuf bytes.Buffer
	e, err := New(context.Background(), raw, nil, Deps{Factories: w.Registry, Loader: w.Loader, Log: slog.New(human.NewHandler(&logBuf, slog.LevelInfo))})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close(context.Background()) })
	f := w.storage(t, "alpha")
	base := time.Date(2026, 10, 1, 4, 0, 0, 0, time.UTC)

	// Night 1 failed: database and volume were both copied.
	putCopiedRun(t, f, "alpha", "night-1", base, model.OutcomeFailed,
		[][2]string{{"databases", "prod"}, {"volumes", "uploads"}})
	// Nights 2 and 3 failed with the volume unreadable: every later run is
	// failed (the standing failed) and holds no copy of the volume.
	for night := 2; night <= 3; night++ {
		putCopiedRun(t, f, "alpha", fmt.Sprintf("night-%d", night), base.AddDate(0, 0, night-1),
			model.OutcomeFailed, [][2]string{{"databases", "prod"}})
	}

	_, results := sweepOnce(t, e, base.AddDate(0, 0, 2))
	ids := runIDsOn(f)
	if !ids["night-1"] {
		t.Fatalf("the newest verified copy of the volume was deleted: %v", ids)
	}
	if len(ids) != 2 || !ids["night-3"] {
		t.Fatalf("storage holds %v, want the newest failed night and the protected copy", ids)
	}
	if got := results["alpha"].result.Deleted; len(got) != 1 || got[0] != "night-2" {
		t.Fatalf("deleted %v, want only the superseded night-2", got)
	}
	if !strings.Contains(logBuf.String(), "newest verified copy of volumes/uploads") {
		t.Fatalf("the log does not name the protected artifact:\n%s", logBuf.String())
	}

	// Night 4 fails, but the volume is readable again: a newer run holds a
	// verified copy, the pin releases, and the policy keeps only night-4.
	putCopiedRun(t, f, "alpha", "night-4", base.AddDate(0, 0, 3), model.OutcomeFailed,
		[][2]string{{"databases", "prod"}, {"volumes", "uploads"}})
	_, results = sweepOnce(t, e, base.AddDate(0, 0, 3))
	ids = runIDsOn(f)
	if len(ids) != 1 || !ids["night-4"] {
		t.Fatalf("after a newer volume copy the old runs must go: %v (deleted %v)", ids, results["alpha"].result.Deleted)
	}
}

// kindOfID is the kind a fabricated run id names, the way a survey derives it:
// the manual suffix marks a manual run.
func kindOfID(id string) model.RunKind {
	if strings.HasSuffix(id, "-manual") {
		return model.RunKindManual
	}
	return model.RunKindScheduled
}

// putCopiedRun writes one run of the given outcome holding a verified copy of
// every named artifact: the object at the artifact key and a manifest that
// calls it ok.
func putCopiedRun(t *testing.T, f *storagetest.Fake, storageName, runID string, at time.Time,
	outcome model.Outcome, artifacts [][2]string) {
	t.Helper()
	rep := model.RunReport{
		Schema: model.ManifestSchema, RunID: runID, Kind: kindOfID(runID), StartedAt: at, Outcome: outcome,
		Storages: []model.StorageResult{{Storage: storageName, Outcome: model.StorageVerified}},
	}
	for _, a := range artifacts {
		key := "runs/" + runID + "/" + a[0] + "/" + a[1] + ".bin"
		body := []byte(runID + "/" + a[0] + "/" + a[1])
		f.PutObject(key, body, at)
		rep.Artifacts = append(rep.Artifacts, model.ArtifactResult{
			Artifact: model.Artifact{Group: a[0], Name: a[1], Importance: model.ImportanceRequired},
			Outcome:  model.ArtifactOK, Key: key, Bytes: int64(len(body)), SHA256: model.SHA256Hex(body),
		})
	}
	f.PutObject("runs/"+runID+"/manifest.json", mustMarshal(t, rep), at)
}

// standingFailedFixture arranges the decision-1 shape on one storage: every
// night failed; night-1 holds the volume (the artifact later nights fail),
// nights 2 and 3 hold only the database. The count policy keeps night-3, so
// night-1 survives only through the floor.
func standingFailedFixture(t *testing.T, f *storagetest.Fake) time.Time {
	t.Helper()
	base := time.Date(2026, 10, 1, 4, 0, 0, 0, time.UTC)
	putCopiedRun(t, f, "alpha", "night-1", base, model.OutcomeFailed,
		[][2]string{{"databases", "prod"}, {"volumes", "uploads"}})
	for night := 2; night <= 3; night++ {
		putCopiedRun(t, f, "alpha", fmt.Sprintf("night-%d", night), base.AddDate(0, 0, night-1),
			model.OutcomeFailed, [][2]string{{"databases", "prod"}})
	}
	return base
}

func guardAssembly(t *testing.T, w *world, logBuf *bytes.Buffer) *Assembly {
	t.Helper()
	cfg := guardConfig(t,
		map[string]any{"alpha": storageInstance("fake", "keep_last")},
		map[string]any{"alpha": keepLastN(1)},
	)
	raw, err := jsonMarshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	e, err := New(context.Background(), raw, nil, Deps{Factories: w.Registry, Loader: w.Loader, Log: slog.New(human.NewHandler(logBuf, slog.LevelInfo))})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close(context.Background()) })
	return e
}

// TestGuardUnreadableManifestNeverReleasesThePin (review probe): one transient
// manifest read failure on night-1 — the run holding the volume's newest
// verified copy — must not turn into "night-1 has no manifest". Folding the
// read failure into absence is what let the fixture delete the only copy.
func TestGuardUnreadableManifestNeverReleasesThePin(t *testing.T) {
	w := newWorld(t)
	var logBuf bytes.Buffer
	e := guardAssembly(t, w, &logBuf)
	f := w.storage(t, "alpha")
	base := standingFailedFixture(t, f)
	f.Fault = func(op storagetest.Op, key string, _ int) error {
		if op == storagetest.OpGet && key == "runs/night-1/manifest.json" {
			return pluginerrors.Transient(errors.New("connection reset"))
		}
		return nil
	}

	_, results := sweepOnce(t, e, base.AddDate(0, 0, 2))
	ids := runIDsOn(f)
	if !ids["night-1"] {
		t.Fatalf("a manifest that could not be read released the pin: %v", ids)
	}
	if got := results["alpha"].result.Deleted; len(got) != 1 || got[0] != "night-2" {
		t.Fatalf("deleted %v, want only the superseded night-2", got)
	}
	if results["alpha"].retentionErr != nil {
		t.Fatalf("retentionErr = %v, want nil (the refusal is not an error)", results["alpha"].retentionErr)
	}
	if !strings.Contains(logBuf.String(), "could not be read") {
		t.Fatalf("the log does not name the unreadable manifest:\n%s", logBuf.String())
	}
}

// TestGuardUnparsableAndUnknownSchemaManifestsNeverRelease: a body that does
// not parse and a schema this build does not read are the same class as a
// failed read — the run is refused for the sweep, with its own reason.
func TestGuardUnparsableAndUnknownSchemaManifestsNeverRelease(t *testing.T) {
	cases := []struct {
		name string
		body string
		want string
	}{
		{"unparsable", "{not json", "parsing the manifest"},
		{"unknown schema", string(mustMarshal(t, model.RunReport{
			Schema: 99, RunID: "night-1", Kind: model.RunKindScheduled,
			StartedAt: time.Date(2026, 10, 1, 4, 0, 0, 0, time.UTC), Outcome: model.OutcomeDelivered,
		})), "schema 99"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := newWorld(t)
			var logBuf bytes.Buffer
			e := guardAssembly(t, w, &logBuf)
			f := w.storage(t, "alpha")
			base := standingFailedFixture(t, f)
			f.PutObject("runs/night-1/manifest.json", []byte(tc.body), base)

			_, results := sweepOnce(t, e, base.AddDate(0, 0, 2))
			if ids := runIDsOn(f); !ids["night-1"] {
				t.Fatalf("an unreadable manifest released the pin: %v", ids)
			}
			if got := results["alpha"].result.Deleted; len(got) != 1 || got[0] != "night-2" {
				t.Fatalf("deleted %v, want only the superseded night-2", got)
			}
			if !strings.Contains(logBuf.String(), tc.want) {
				t.Fatalf("the log does not carry %q:\n%s", tc.want, logBuf.String())
			}
		})
	}
}

// TestGuardStorageWideManifestReadFailureDeletesNothing: when every manifest
// read on a storage fails, nothing is deleted that night and every run says
// why — silence here would be the same loss, only larger.
func TestGuardStorageWideManifestReadFailureDeletesNothing(t *testing.T) {
	w := newWorld(t)
	var logBuf bytes.Buffer
	e := guardAssembly(t, w, &logBuf)
	f := w.storage(t, "alpha")
	base := standingFailedFixture(t, f)
	f.Fault = func(op storagetest.Op, key string, _ int) error {
		if op == storagetest.OpGet && strings.HasSuffix(key, "/manifest.json") {
			return pluginerrors.Transient(errors.New("connection reset"))
		}
		return nil
	}

	_, results := sweepOnce(t, e, base.AddDate(0, 0, 2))
	if ids := runIDsOn(f); len(ids) != 3 {
		t.Fatalf("a night of unreadable manifests deleted runs: %v", ids)
	}
	if got := results["alpha"].result.Deleted; len(got) != 0 {
		t.Fatalf("deleted %v, want nothing", got)
	}
	if results["alpha"].retentionErr != nil {
		t.Fatalf("retentionErr = %v, want nil", results["alpha"].retentionErr)
	}
	for _, id := range []string{"night-1", "night-2", "night-3"} {
		// The rendered line names the run in its sentence; the object key is
		// debug-only machine detail.
		if !strings.Contains(logBuf.String(), "storage fake/alpha: ") || !strings.Contains(logBuf.String(), id) {
			t.Fatalf("the log does not name %s:\n%s", id, logBuf.String())
		}
	}
}

// TestGuardTruncatedObjectIsNotAHeldCopy: the ledger counts an object only at
// the size its manifest records — a half-written upload is not evidence of a
// copy, so it neither pins its run nor can it be mistaken for one.
func TestGuardTruncatedObjectIsNotAHeldCopy(t *testing.T) {
	w := newWorld(t)
	var logBuf bytes.Buffer
	e := guardAssembly(t, w, &logBuf)
	f := w.storage(t, "alpha")
	base := standingFailedFixture(t, f)
	// Truncate night-1's volume object: present under the right key, wrong size.
	f.PutObject("runs/night-1/volumes/uploads.bin", []byte("short"), base)

	_, results := sweepOnce(t, e, base.AddDate(0, 0, 2))
	ids := runIDsOn(f)
	if ids["night-1"] {
		t.Fatalf("a truncated object pinned its run: %v", ids)
	}
	if got := results["alpha"].result.Deleted; len(got) != 2 {
		t.Fatalf("deleted %v, want the two superseded nights", got)
	}
}

// TestListedManifestThatReadsBackAbsentIsNeverDeleted (A8, S2): a manifest in
// the listing whose read answers not_found is a problem run, and keep_last 1
// must not delete it: the survey cannot see which copies it holds, so the run
// is never taken for one with no manifest at all.
