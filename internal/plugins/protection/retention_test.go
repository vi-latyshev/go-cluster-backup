package protection

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
	pluginerrors "github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/execution"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/human"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/inventory"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/retention"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/retention/keepversions"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage/guard"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage/storagetest"
)

// fixedPolicy is a retention policy a test scripts exactly.
type fixedPolicy struct {
	drop      []string
	keepFloor []string
	err       error
}

func (p fixedPolicy) Decide(context.Context, retention.DecideRequest) (retention.Decision, error) {
	return retention.Decision{Drop: p.drop, KeepFloor: p.keepFloor}, p.err
}

// testDestination builds one guarded destination over a fake storage.
func testDestination(t *testing.T, name string, s storage.Storage) guard.Destination {
	t.Helper()
	return testDestinationLogged(t, name, s, discardLog())
}

// testDestinationLogged builds one destination whose instance logger is the
// given one, the way the assembly hands the binding its Deps.
func testDestinationLogged(t *testing.T, name string, s storage.Storage, log *slog.Logger) guard.Destination {
	t.Helper()
	r := instances.NewRegistry()
	type cfg struct{}
	if err := storage.Register(r, storage.FactorySpec[cfg]{
		Type: "fake",
		New: func(context.Context, cfg, instances.Deps, storage.BuildInput) (storage.Storage, error) {
			return s, nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	d, err := guard.Build(context.Background(), r, "fake", jsontext.Value(`{}`),
		storage.BuildInput{CallParallelism: 1}, instances.Deps{Instance: name, Log: log})
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// testLedger builds a ledger over the given storages with one-retry attempts and
// no policies unless the caller adds them.
func testLedger(t *testing.T, storages map[string]storage.Storage, policies map[string]map[model.RunKind]retention.Retention,
	caps map[string]int64, cutoff time.Time) *Ledger {
	t.Helper()
	return testLedgerWriting(t, "", storages, policies, caps, cutoff)
}

// testLedgerWriting is testLedger for a deployment that opted into a stable
// writer identity.
func testLedgerWriting(t *testing.T, writer string, storages map[string]storage.Storage,
	policies map[string]map[model.RunKind]retention.Retention, caps map[string]int64, cutoff time.Time) *Ledger {
	t.Helper()
	destinations := make([]guard.Destination, 0, len(storages))
	names := make([]string, 0, len(storages))
	for name := range storages {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		destinations = append(destinations, testDestination(t, name, storages[name]))
	}
	return NewLedger(LedgerOptions{
		Destinations:   destinations,
		Policies:       policies,
		Caps:           caps,
		StaleCutoff:    cutoff,
		Retry:          execution.Options{Attempts: 1, Unclassified: pluginerrors.ClassPermanent},
		WriterIdentity: writer,
	})
}

// verifiedRun is one delivered run holding one verified artifact.
func verifiedRun(id string, at time.Time, kind model.RunKind, storageName string) inventory.Run {
	key := "runs/" + id + "/databases/prod"
	body := []byte(id)
	return inventory.Run{
		Stored:      model.StoredRun{RunID: id, Kind: kind, At: at, Bytes: int64(len(body)), Verified: true, Manifest: true},
		Objects:     []model.ObjectInfo{{Key: key, Size: int64(len(body))}},
		ManifestKey: "runs/" + id + "/manifest.json",
		Manifest: &model.RunReport{RunID: id, Kind: kind, StartedAt: at, Outcome: model.OutcomeDelivered,
			Storages: []model.StorageResult{{Storage: storageName, Outcome: model.StorageVerified}},
			Artifacts: []model.ArtifactResult{{Artifact: model.Artifact{Group: "databases", Name: "prod"},
				Outcome: model.ArtifactOK, Key: key, Bytes: int64(len(body))}}},
	}
}

// withManifest adds the manifest object the deletion order looks for.
func withManifest(r inventory.Run) inventory.Run {
	r.Objects = append(r.Objects, model.ObjectInfo{Key: r.ManifestKey, Size: 2})
	r.Stored.Bytes += 2
	return r
}

func surveyOf(runs map[string]inventory.Run) inventory.Survey {
	return inventory.Survey{Storage: "alpha", OK: true, Runs: runs}
}

// TestMarkCurrentSkipsAnUnnamedArtifact: a nameless artifact carries no identity
// to credit, so it never enters the copy ledger.
func TestMarkCurrentSkipsAnUnnamedArtifact(t *testing.T) {
	st := newRetentionState(nil, "")
	st.markCurrent("run-1", model.RunKindScheduled, time.Now().UTC(),
		[]model.ArtifactResult{{Artifact: model.Artifact{Group: "g"}, Outcome: model.ArtifactOK}}, nil, nil)
	if len(st.held) != 0 {
		t.Fatalf("an unnamed artifact was credited: %+v", st.held)
	}
	if st.kinds["run-1"] != model.RunKindScheduled {
		t.Fatalf("the run's kind was not recorded: %+v", st.kinds)
	}
}

// TestMarkCurrentCreditsOnlySavedVerifiedCopies: the run being written holds an
// artifact only where it was verified and its manifest saved.
func TestMarkCurrentCreditsOnlySavedVerifiedCopies(t *testing.T) {
	st := newRetentionState(nil, "")
	artifacts := []model.ArtifactResult{{Artifact: model.Artifact{Group: "g", Name: "n"}, Outcome: model.ArtifactOK}}
	st.markCurrent("run-1", model.RunKindScheduled, time.Now().UTC(), artifacts,
		map[string]map[string]bool{"n": {"alpha": true, "beta": true}}, map[string]bool{"alpha": true})
	if _, ok := st.held["g/n"]["alpha"][model.RunKindScheduled]["run-1"]; !ok {
		t.Fatalf("the saved and verified copy was not credited: %+v", st.held)
	}
	if _, ok := st.held["g/n"]["beta"]; ok {
		t.Fatal("an unsaved copy was credited")
	}
}

// TestLatestCopyManualOnlyHolder: a manual run is pinned only while it is the
// storage's only verified copy of the artifact.
func TestLatestCopyManualOnlyHolder(t *testing.T) {
	st := newRetentionState(nil, "")
	st.kinds["m1"] = model.RunKindManual
	st.markHeld("g/n", "alpha", model.RunKindManual, "m1", time.Now().UTC())
	if got := st.latestCopy("m1", "alpha"); got != "it holds the only verified copy of g/n" {
		t.Fatalf("latestCopy = %q, want the only-copy pin", got)
	}
	st.markHeld("g/n", "alpha", model.RunKindScheduled, "s1", time.Now().UTC())
	if got := st.latestCopy("m1", "alpha"); got != "" {
		t.Fatalf("latestCopy with another holder = %q, want no pin", got)
	}
}

// TestLatestCopyNewestWins: among scheduled holders the newest copy is pinned,
// ties broken by id the way NewestFirst does.
func TestLatestCopyNewestWins(t *testing.T) {
	st := newRetentionState(nil, "")
	base := time.Date(2026, 10, 1, 4, 0, 0, 0, time.UTC)
	st.kinds["old"], st.kinds["new"] = model.RunKindScheduled, model.RunKindScheduled
	st.markHeld("g/n", "alpha", model.RunKindScheduled, "old", base)
	st.markHeld("g/n", "alpha", model.RunKindScheduled, "new", base.Add(time.Hour))
	if got := st.latestCopy("old", "alpha"); got != "" {
		t.Fatalf("the older copy = %q, want no pin", got)
	}
	if got := st.latestCopy("new", "alpha"); got != "it holds the newest verified copy of g/n" {
		t.Fatalf("the newest copy = %q, want the newest pin", got)
	}
}

// TestUncertainRunIsNeverDropped: the run the record's uncertainty names is the
// inventory the settlement proof compares, so no sweep may delete it.
func TestUncertainRunIsNeverDropped(t *testing.T) {
	surveys := map[string]inventory.Survey{"alpha": surveyOf(map[string]inventory.Run{
		"dead": {Stored: model.StoredRun{RunID: "dead", Kind: model.RunKindScheduled}},
		"old":  {Stored: model.StoredRun{RunID: "old", Kind: model.RunKindScheduled}},
	})}
	st := newRetentionState(surveys, "")
	if ok, _ := st.canDrop("dead", "alpha"); !ok {
		t.Fatal("the uncertain run was droppable before it was named")
	}
	st.markUncertain("dead")
	if ok, reason := st.canDrop("dead", "alpha"); ok || reason == "" {
		t.Fatalf("the uncertain run = (%v, %q), want a refusal", ok, reason)
	}
	if ok, reason := st.canDrop("old", "alpha"); !ok {
		t.Fatalf("an unnamed run was refused: %q", reason)
	}
}

// TestCanDropFloors: a problem manifest refuses, the last verified run survives,
// and a run verified on another storage may go.
func TestCanDropFloors(t *testing.T) {
	st := newRetentionState(map[string]inventory.Survey{"alpha": surveyOf(map[string]inventory.Run{
		"bad":  {ManifestProblem: "unreadable"},
		"only": {Stored: model.StoredRun{RunID: "only", Verified: true}},
	})}, "")
	if ok, reason := st.canDrop("bad", "alpha"); ok || reason == "" {
		t.Fatalf("a problem run = (%v, %q), want a refusal", ok, reason)
	}
	if ok, reason := st.canDrop("only", "alpha"); ok || reason == "" {
		t.Fatalf("the last verified run = (%v, %q), want a refusal", ok, reason)
	}
	st.mark("only", "beta")
	if ok, reason := st.canDrop("only", "alpha"); !ok {
		t.Fatalf("a run verified elsewhere was refused: %q", reason)
	}
}

// TestCapStateAdmission: no cap admits everything, a retry of a reserved key is
// never refused, a manual write on unknown occupancy or past the cap is refused,
// and only a confirmed deletion lowers the ledger.
func TestCapStateAdmission(t *testing.T) {
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
	if c.reserve("big", 1000, true) {
		t.Fatal("a manual write past the cap was admitted")
	}
	c.shrink("k", 4)
	c.shrink("k", 100)
	if got := c.used(); got != 4 {
		t.Fatalf("used after shrink = %d, want 4", got)
	}
	c.commit("k", 4)
	if got := c.used(); got != 4 {
		t.Fatalf("used after commit = %d, want 4", got)
	}
	c.unreserve("k")
	c.releaseBytes(1000)
	if got := c.used(); got != 0 {
		t.Fatalf("used = %d, want 0 (clamped)", got)
	}

	unknown := newCapState(100, 0, false)
	if unknown.reserve("k", 1, true) {
		t.Fatal("a manual write on an unknown occupancy was admitted")
	}
	if !unknown.reserve("k", 1, false) {
		t.Fatal("a scheduled write on an unknown occupancy was refused")
	}
	if unknown.occupancyKnown() {
		t.Fatal("an unknown occupancy reported itself known")
	}
	limit, used := unknown.limits()
	if limit != 100 || used != 1 {
		t.Fatalf("limits = (%d, %d), want (100, 1)", limit, used)
	}
	unknown.markUnknown()
	if unknown.occupancyKnown() {
		t.Fatal("markUnknown left the ledger known")
	}
}

// TestLedgerStartAndAdmission: the survey supplies occupancy, a failed survey
// leaves the storage unknown, and admission follows the cap.
func TestLedgerStartAndAdmission(t *testing.T) {
	alpha := storagetest.NewFake()
	alpha.PutObject("runs/a/x", make([]byte, 10), time.Now())
	l := testLedger(t, map[string]storage.Storage{"alpha": alpha}, nil, map[string]int64{"alpha": 100}, time.Time{})
	if got := l.Storages(); len(got) != 1 || got[0] != "alpha" {
		t.Fatalf("Storages = %v", got)
	}
	l.Start(map[string]inventory.Survey{"alpha": surveyOf(nil)})
	if got := l.Used("alpha"); got != 0 {
		t.Fatalf("used from an empty survey = %d", got)
	}
	if !l.Admit("alpha", "k", 10, true) {
		t.Fatal("a write within the cap was refused")
	}
	l.Shrink("alpha", "k", 5)
	l.Commit("alpha", "k", 5)
	if got := l.Used("alpha"); got != 5 {
		t.Fatalf("used = %d, want 5", got)
	}
	l.Unreserve("alpha", "never-reserved")
	if limit, _ := l.Limits("alpha"); limit != 100 {
		t.Fatalf("limit = %d, want 100", limit)
	}
	l.Unknown("alpha")
	if l.Admit("alpha", "k2", 1, true) {
		t.Fatal("a manual write on an unknown occupancy was admitted")
	}
	l.MarkUncertain("dead")
	if ok, _ := l.state.canDrop("dead", "alpha"); ok {
		t.Fatal("the ledger's uncertainty guard was not applied")
	}
}

// TestSweepSurveyFailure: a failed survey is refused before any deletion.
func TestSweepSurveyFailure(t *testing.T) {
	l := testLedger(t, map[string]storage.Storage{"alpha": storagetest.NewFake()}, nil, nil, time.Time{})
	res := l.Sweep(context.Background(), Sweep{Storage: "alpha", Survey: inventory.Survey{OK: false, Err: errors.New("listing failed")}})
	if res.Err == nil {
		t.Fatal("a failed survey was swept")
	}
}

// TestSweepSkip: a manual run that did not complete on a storage deletes nothing
// there.
func TestSweepSkip(t *testing.T) {
	f := storagetest.NewFake()
	f.PutObject("runs/old/x", []byte("x"), time.Now())
	l := testLedger(t, map[string]storage.Storage{"alpha": f}, nil, nil, time.Time{})
	res := l.Sweep(context.Background(), Sweep{Storage: "alpha", Skip: "the manual run did not complete"})
	if res.Err != nil || len(res.Deleted) != 0 {
		t.Fatalf("skipped sweep = %+v", res)
	}
	if _, ok := f.Bytes("runs/old/x"); !ok {
		t.Fatal("a skipped sweep deleted an object")
	}
}

// TestSweepPolicyDrops: the policy's drops go through the ledger's remover,
// manifest first, and a failed deletion stops the sweep.
func TestSweepPolicyDrops(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2026, 10, 1, 4, 0, 0, 0, time.UTC)
	f := storagetest.NewFake()
	for i, id := range []string{"r1", "r2"} {
		run := verifiedRun(id, base.AddDate(0, 0, i), model.RunKindScheduled, "alpha")
		for _, o := range run.Objects {
			f.PutObject(o.Key, []byte(id), run.Stored.At)
		}
		f.PutObject(run.ManifestKey, []byte("{}"), run.Stored.At)
	}
	l := testLedger(t, map[string]storage.Storage{"alpha": f},
		map[string]map[model.RunKind]retention.Retention{"alpha": {model.RunKindScheduled: fixedPolicy{drop: []string{"r1"}}}},
		nil, base)
	survey := surveyOf(map[string]inventory.Run{
		"r1": withManifest(verifiedRun("r1", base, model.RunKindScheduled, "alpha")),
		"r2": withManifest(verifiedRun("r2", base.AddDate(0, 0, 1), model.RunKindScheduled, "alpha")),
	})
	l.Start(map[string]inventory.Survey{"alpha": survey})
	res := l.Sweep(ctx, Sweep{RunStart: base, Kind: model.RunKindScheduled, Storage: "alpha", Survey: survey})
	if res.Err != nil || len(res.Deleted) != 1 || res.Deleted[0] != "r1" {
		t.Fatalf("sweep = %+v", res)
	}
	if _, ok := f.Bytes("runs/r1/databases/prod"); ok {
		t.Fatal("the dropped run's object survived")
	}
	if got := f.CallCount(storagetest.OpDelete, "runs/r1/manifest.json"); got != 1 {
		t.Fatalf("the manifest was not deleted first: %d calls", got)
	}

	// A delete failure ends the sweep: the runs after it stay untouched.
	failing := storagetest.NewFake()
	for i, id := range []string{"a", "b"} {
		run := verifiedRun(id, base.AddDate(0, 0, i+2), model.RunKindScheduled, "alpha")
		failing.PutObject(run.ManifestKey, []byte("{}"), run.Stored.At)
		failing.PutObject("runs/"+id+"/databases/prod", []byte(id), run.Stored.At)
	}
	failing.Fault = func(op storagetest.Op, _ string, _ int) error {
		if op == storagetest.OpDelete {
			return pluginerrors.Permanent(errors.New("delete refused"))
		}
		return nil
	}
	lf := testLedger(t, map[string]storage.Storage{"alpha": failing},
		map[string]map[model.RunKind]retention.Retention{"alpha": {model.RunKindScheduled: fixedPolicy{drop: []string{"a", "b"}}}},
		nil, base)
	surveyFailing := surveyOf(map[string]inventory.Run{
		"a": withManifest(verifiedRun("a", base.AddDate(0, 0, 2), model.RunKindScheduled, "alpha")),
		"b": withManifest(verifiedRun("b", base.AddDate(0, 0, 3), model.RunKindScheduled, "alpha")),
	})
	lf.Start(map[string]inventory.Survey{"alpha": surveyFailing})
	res = lf.Sweep(ctx, Sweep{RunStart: base, Kind: model.RunKindScheduled, Storage: "alpha", Survey: surveyFailing})
	if res.Err == nil {
		t.Fatal("a failed deletion was hidden")
	}
	if len(res.Deleted) != 0 {
		t.Fatalf("deleted %v after the first delete failed", res.Deleted)
	}
}

// TestSweepPolicyError: a policy failure ends the storage's sweep without a
// deletion.
func TestSweepPolicyError(t *testing.T) {
	l := testLedger(t, map[string]storage.Storage{"alpha": storagetest.NewFake()},
		map[string]map[model.RunKind]retention.Retention{"alpha": {model.RunKindScheduled: fixedPolicy{err: errors.New("policy failed")}}},
		nil, time.Time{})
	survey := surveyOf(nil)
	l.Start(map[string]inventory.Survey{"alpha": survey})
	res := l.Sweep(context.Background(), Sweep{Kind: model.RunKindScheduled, Storage: "alpha", Survey: survey})
	if res.Err == nil {
		t.Fatal("a policy failure was hidden")
	}
}

// TestDropGuards: drop is a no-op after a failure, for the current run, for an id
// the survey does not hold, for an in-flight manifestless run, and for a run the
// floor protects.
func TestDropGuards(t *testing.T) {
	base := time.Now().UTC()
	f := storagetest.NewFake()
	run := verifiedRun("r1", base, model.RunKindScheduled, "alpha")
	for _, o := range run.Objects {
		f.PutObject(o.Key, []byte("r1"), base)
	}
	l := testLedger(t, map[string]storage.Storage{"alpha": f}, nil, nil, base)
	runs := map[string]inventory.Run{
		"r1":   run,
		"live": {Stored: model.StoredRun{RunID: "live", Kind: model.RunKindScheduled, At: base.Add(time.Hour)}},
	}
	newDeleter := func(cap *capState) *deleter {
		return &deleter{ctx: context.Background(), l: l, storage: "alpha", sur: surveyOf(runs),
			cutoff: base, cap: cap}
	}
	failed := newDeleter(nil)
	failed.failed = errors.New("already failed")
	if got := failed.drop("r1"); got != 0 {
		t.Fatalf("drop after a failure freed %d", got)
	}
	current := newDeleter(nil)
	current.current = "r1"
	if got := current.drop("r1"); got != 0 {
		t.Fatalf("drop of the current run freed %d", got)
	}
	if got := newDeleter(nil).drop("nope"); got != 0 {
		t.Fatalf("drop of an unknown run freed %d", got)
	}
	if got := newDeleter(nil).drop("live"); got != 0 {
		t.Fatalf("drop of an in-flight run freed %d", got)
	}
	floor := newDeleter(nil)
	l.state = newRetentionState(map[string]inventory.Survey{"alpha": surveyOf(runs)}, "")
	l.state.mark("r1", "alpha")
	if got := floor.drop("r1"); got != 0 {
		t.Fatalf("drop of the only verified run freed %d", got)
	}
}

// TestEvictForCapStopsOnceTheIncompleteRunsFit: the cap's first pass drops the
// oldest incomplete attempt at a time and stops when the occupancy fits.
func TestEvictForCapStopsOnceTheIncompleteRunsFit(t *testing.T) {
	now := time.Now().UTC()
	f := storagetest.NewFake()
	runs := map[string]inventory.Run{
		"a": {Stored: model.StoredRun{RunID: "a", Kind: model.RunKindScheduled, At: now, Bytes: 60},
			Objects: []model.ObjectInfo{{Key: "runs/a/x", Size: 60}}},
		"b": {Stored: model.StoredRun{RunID: "b", Kind: model.RunKindScheduled, At: now, Bytes: 60},
			Objects: []model.ObjectInfo{{Key: "runs/b/x", Size: 60}}},
	}
	for _, r := range runs {
		for _, o := range r.Objects {
			f.PutObject(o.Key, make([]byte, o.Size), now)
		}
	}
	l := testLedger(t, map[string]storage.Storage{"alpha": f}, nil, nil, now.Add(time.Hour))
	l.state = newRetentionState(nil, "")
	d := &deleter{ctx: context.Background(), l: l, storage: "alpha", sur: surveyOf(runs),
		cutoff: now.Add(time.Hour),
		cap:    &capState{cap: 100, occupancy: 150, known: true, reserved: map[string]int64{}}}
	d.evictForCap(d.cap, []model.StoredRun{runs["a"].Stored, runs["b"].Stored})
	if d.cap.used() != 90 {
		t.Fatalf("occupancy after eviction = %d, want 90", d.cap.used())
	}
	if len(d.deleted) != 1 {
		t.Fatalf("evicted %v, want exactly one incomplete run", d.deleted)
	}
}

// TestEvictForCapStopsMidScheduledHistory: the byte-budget pass drops the oldest
// scheduled runs until the occupancy fits, then stops.
func TestEvictForCapStopsMidScheduledHistory(t *testing.T) {
	base := time.Date(2026, 10, 1, 4, 0, 0, 0, time.UTC)
	f := storagetest.NewFake()
	sizes := map[string]int64{"R1": 1, "R2": 1, "R3": 150, "R4": 1, "R5": 1}
	runs := map[string]inventory.Run{}
	var list []model.StoredRun
	i := 0
	for _, id := range []string{"R1", "R2", "R3", "R4", "R5"} {
		at := base.Add(time.Duration(5-i) * time.Hour)
		i++
		r := inventory.Run{
			Stored:  model.StoredRun{RunID: id, Kind: model.RunKindScheduled, At: at, Bytes: sizes[id], Manifest: true},
			Objects: []model.ObjectInfo{{Key: "runs/" + id + "/x", Size: sizes[id]}},
		}
		f.PutObject("runs/"+id+"/x", make([]byte, sizes[id]), at)
		runs[id] = r
		list = append(list, r.Stored)
	}
	l := testLedger(t, map[string]storage.Storage{"alpha": f}, nil, nil, base)
	l.state = newRetentionState(nil, "")
	d := &deleter{ctx: context.Background(), l: l, storage: "alpha", sur: surveyOf(runs),
		cutoff: base,
		cap:    &capState{cap: 100, occupancy: 154, known: true, reserved: map[string]int64{}}}
	d.evictForCap(d.cap, list)
	if d.cap.used() != 4 {
		t.Fatalf("occupancy after eviction = %d, want 4", d.cap.used())
	}
	if len(d.deleted) != 1 || d.deleted[0] != "R3" {
		t.Fatalf("evicted %v, want exactly the 150-byte scheduled run", d.deleted)
	}
}

// TestEvictForCapClosedOnUnknownAndFloor: an unknown occupancy closes eviction,
// and the policy's floor wins the byte-budget pass.
func TestEvictForCapClosedOnUnknownAndFloor(t *testing.T) {
	base := time.Date(2026, 10, 1, 4, 0, 0, 0, time.UTC)
	f := storagetest.NewFake()
	runs := map[string]inventory.Run{
		"kept": {Stored: model.StoredRun{RunID: "kept", Kind: model.RunKindScheduled, At: base, Bytes: 150, Manifest: true}},
	}
	l := testLedger(t, map[string]storage.Storage{"alpha": f}, nil, nil, base)
	unknown := &capState{cap: 100, occupancy: 154, known: false, reserved: map[string]int64{}}
	l.state = newRetentionState(nil, "")
	d := &deleter{ctx: context.Background(), l: l, storage: "alpha", sur: surveyOf(runs),
		cutoff: base, cap: unknown}
	d.evictForCap(unknown, []model.StoredRun{runs["kept"].Stored})
	if len(d.deleted) != 0 {
		t.Fatalf("eviction ran on an unknown occupancy: %v", d.deleted)
	}

	known := &capState{cap: 100, occupancy: 154, known: true, reserved: map[string]int64{}}
	l.state = newRetentionState(nil, "")
	d = &deleter{ctx: context.Background(), l: l, storage: "alpha", sur: surveyOf(runs),
		cutoff: base, cap: known, protected: map[string]bool{"kept": true}}
	d.evictForCap(known, []model.StoredRun{runs["kept"].Stored})
	if len(d.deleted) != 0 {
		t.Fatalf("a floored run was evicted: %v", d.deleted)
	}
}

// TestSweepOverlayAndCurrent: the run being written is part of the policy's
// input and is never dropped by the sweep itself.
func TestSweepOverlayAndCurrent(t *testing.T) {
	base := time.Date(2026, 10, 1, 4, 0, 0, 0, time.UTC)
	f := storagetest.NewFake()
	stored := model.StoredRun{RunID: "current", Kind: model.RunKindScheduled, At: base, Manifest: true}
	l := testLedger(t, map[string]storage.Storage{"alpha": f},
		map[string]map[model.RunKind]retention.Retention{"alpha": {model.RunKindScheduled: fixedPolicy{drop: []string{"current"}}}},
		nil, base)
	survey := surveyOf(map[string]inventory.Run{})
	l.Start(map[string]inventory.Survey{"alpha": survey})
	res := l.Sweep(context.Background(), Sweep{RunStart: base, Kind: model.RunKindScheduled,
		Storage: "alpha", Survey: survey, Overlay: &stored})
	if res.Err != nil || len(res.Deleted) != 0 || res.Kept != 1 {
		t.Fatalf("sweep = %+v", res)
	}
}

// TestSweepCapOverflowReported: a scheduled sweep over the cap frees room and
// reports what the floor keeps over it.
func TestSweepCapOverflowReported(t *testing.T) {
	base := time.Date(2026, 10, 1, 4, 0, 0, 0, time.UTC)
	f := storagetest.NewFake()
	for i, id := range []string{"old", "new"} {
		run := verifiedRun(id, base.AddDate(0, 0, i), model.RunKindScheduled, "alpha")
		f.PutObject(run.ManifestKey, []byte("{}"), run.Stored.At)
		for _, o := range run.Objects {
			f.PutObject(o.Key, []byte(id), run.Stored.At)
		}
	}
	l := testLedger(t, map[string]storage.Storage{"alpha": f}, nil, map[string]int64{"alpha": 5}, base)
	survey := surveyOf(map[string]inventory.Run{
		"old": verifiedRun("old", base, model.RunKindScheduled, "alpha"),
		"new": verifiedRun("new", base.AddDate(0, 0, 1), model.RunKindScheduled, "alpha"),
	})
	l.Start(map[string]inventory.Survey{"alpha": survey})
	res := l.Sweep(context.Background(), Sweep{RunStart: base.AddDate(0, 0, 2), Kind: model.RunKindScheduled, Storage: "alpha", Survey: survey})
	if res.Err != nil {
		t.Fatalf("sweep = %+v", res)
	}
	if ok, _ := l.state.canDrop("new", "alpha"); ok {
		t.Fatal("the newest verified run was made droppable")
	}
}

// TestLedgerStartMarksUnknownSurveys: a storage with a failed survey and one the
// survey does not mention are both unknown for admission.
func TestLedgerStartMarksUnknownSurveys(t *testing.T) {
	l := testLedger(t, map[string]storage.Storage{
		"alpha": storagetest.NewFake(),
		"beta":  storagetest.NewFake(),
	}, nil, map[string]int64{"alpha": 10, "beta": 10}, time.Time{})
	l.Start(map[string]inventory.Survey{
		"alpha": {Storage: "alpha", OK: false, Err: errors.New("listing failed")},
	})
	for _, name := range []string{"alpha", "beta"} {
		if l.Admit(name, "k", 1, true) {
			t.Fatalf("a manual write on %q's unknown occupancy was admitted", name)
		}
	}
}

// TestRetentionStateReadsProblemRuns: a problem manifest and a manifest whose
// artifacts do not match the listing contribute no copy evidence.
func TestRetentionStateReadsProblemRuns(t *testing.T) {
	key := "runs/bad/x"
	runs := map[string]inventory.Run{
		"bad": {
			Stored:          model.StoredRun{RunID: "bad", Kind: model.RunKindScheduled},
			ManifestProblem: "unreadable",
			Objects:         []model.ObjectInfo{{Key: key, Size: 1}},
			Manifest: &model.RunReport{Artifacts: []model.ArtifactResult{
				{Artifact: model.Artifact{Group: "g", Name: "n"}, Outcome: model.ArtifactOK, Key: key, Bytes: 1},
			}},
		},
		"mismatch": {
			Stored:  model.StoredRun{RunID: "mismatch", Kind: model.RunKindScheduled},
			Objects: []model.ObjectInfo{{Key: key, Size: 1}},
			Manifest: &model.RunReport{Artifacts: []model.ArtifactResult{
				{Artifact: model.Artifact{Group: "g", Name: "failed"}, Outcome: model.ArtifactFailed, Key: key, Bytes: 1},
				{Artifact: model.Artifact{Group: "g", Name: "nokey"}, Outcome: model.ArtifactOK, Bytes: 1},
				{Artifact: model.Artifact{Group: "g", Name: "size"}, Outcome: model.ArtifactOK, Key: key, Bytes: 99},
			}},
		},
	}
	st := newRetentionState(map[string]inventory.Survey{"alpha": surveyOf(runs)}, "")
	if st.problems["bad"] == "" {
		t.Fatal("the problem run was not recorded")
	}
	if len(st.held) != 0 {
		t.Fatalf("a copy was credited without a matching object: %+v", st.held)
	}
}

// TestLatestCopyTieBreaksByID: equal instants order by run id the way
// NewestFirst does, so only the lower id is the newest copy.
func TestLatestCopyTieBreaksByID(t *testing.T) {
	st := newRetentionState(nil, "")
	at := time.Now().UTC()
	st.kinds["a"], st.kinds["b"] = model.RunKindScheduled, model.RunKindScheduled
	st.markHeld("g/n", "alpha", model.RunKindScheduled, "b", at)
	st.markHeld("g/n", "alpha", model.RunKindScheduled, "a", at)
	if got := st.latestCopy("b", "alpha"); got != "" {
		t.Fatalf("the tied higher id = %q, want no pin", got)
	}
	if got := st.latestCopy("a", "alpha"); got == "" {
		t.Fatal("the tied lower id is not the newest copy")
	}
	// A run that holds no copy of the artifact is skipped without a pin.
	if got := st.latestCopy("absent", "alpha"); got != "" {
		t.Fatalf("a run holding nothing = %q", got)
	}
}

// TestEvictForCapGuards: no cap, a spent cap and an already failed sweep all
// return before any deletion.
func TestEvictForCapGuards(t *testing.T) {
	l := testLedger(t, map[string]storage.Storage{"alpha": storagetest.NewFake()}, nil, nil, time.Time{})
	l.state = newRetentionState(nil, "")
	d := &deleter{ctx: context.Background(), l: l, storage: "alpha"}
	d.evictForCap(nil, nil)
	d.evictForCap(&capState{cap: 0}, nil)
	failed := &deleter{ctx: context.Background(), l: l, storage: "alpha", failed: errors.New("x")}
	failed.evictForCap(&capState{cap: 10, occupancy: 100, known: true}, nil)
	d.evictForCap(&capState{cap: 10, occupancy: 0, known: true}, nil)
	if len(d.deleted) != 0 {
		t.Fatalf("a guarded eviction deleted %v", d.deleted)
	}
}

// TestEvictForCapManualRunsBeforeHistory: a manual copy that no scheduled copy
// covers is evicted before the scheduled history is touched.
func TestEvictForCapManualRunsBeforeHistory(t *testing.T) {
	now := time.Now().UTC()
	f := storagetest.NewFake()
	f.PutObject("runs/m/x", make([]byte, 60), now)
	runs := map[string]inventory.Run{
		"m": {Stored: model.StoredRun{RunID: "m", Kind: model.RunKindManual, At: now, Bytes: 60, Manifest: true},
			Objects: []model.ObjectInfo{{Key: "runs/m/x", Size: 60}}},
		"n": {Stored: model.StoredRun{RunID: "n", Kind: model.RunKindManual, At: now.Add(-time.Hour), Bytes: 60, Manifest: true},
			Objects: []model.ObjectInfo{{Key: "runs/n/x", Size: 60}}},
	}
	l := testLedger(t, map[string]storage.Storage{"alpha": f}, nil, nil, now)
	l.state = newRetentionState(nil, "")
	d := &deleter{ctx: context.Background(), l: l, storage: "alpha", sur: surveyOf(runs),
		cutoff: now, cap: &capState{cap: 100, occupancy: 150, known: true, reserved: map[string]int64{}}}
	d.evictForCap(d.cap, []model.StoredRun{runs["m"].Stored, runs["n"].Stored})
	if len(d.deleted) != 1 || d.deleted[0] != "n" {
		t.Fatalf("evicted %v, want the manual copy", d.deleted)
	}
	if d.cap.used() != 90 {
		t.Fatalf("used = %d, want 90", d.cap.used())
	}
}

// TestDeleterHelpers: the small guards the sweep builds on.
func TestDeleterHelpers(t *testing.T) {
	d := &deleter{deleted: []string{"x"}}
	if !d.deletedHas("x") || d.deletedHas("y") {
		t.Fatal("deletedHas is wrong")
	}
	if got := toSet([]string{"a"}); !got["a"] {
		t.Fatalf("toSet = %v", got)
	}
}

// TestSweepEventsCarryTheInstancePrefix: every line the ledger emits for a
// storage goes through that instance's own scoped logger, so it reads
// "storage <type>/<instance>: ..." like every other storage line — never a bare
// storage= attribute naming a second scheme.
func TestSweepEventsCarryTheInstancePrefix(t *testing.T) {
	var buf bytes.Buffer
	instanceLog := slog.New(human.NewHandler(&buf, slog.LevelInfo)).
		With("kind", "storage", "type", "local", "instance", "exchange")
	l := NewLedger(LedgerOptions{
		Destinations: []guard.Destination{testDestinationLogged(t, "exchange", storagetest.NewFake(), instanceLog)},
		Retry:        execution.Options{Attempts: 1, Unclassified: pluginerrors.ClassPermanent},
	})
	survey := inventory.Survey{Storage: "exchange", OK: true}
	l.Start(map[string]inventory.Survey{"exchange": survey})
	if res := l.Sweep(context.Background(), Sweep{Kind: model.RunKindScheduled, Storage: "exchange", Survey: survey}); res.Err != nil {
		t.Fatalf("sweep = %+v", res)
	}
	out := buf.String()
	if !strings.Contains(out, "storage local/exchange: ") {
		t.Fatalf("the ledger line lost the instance prefix:\n%s", out)
	}
	if strings.Contains(out, "storage=") {
		t.Fatalf("the ledger line carries a bare storage attribute:\n%s", out)
	}
}

// TestEventFallsBackWithoutAnInstanceLogger: a ledger whose destination carried
// no Deps logger still emits through the process logger instead of panicking;
// production assemblies always hand the scoped one.
func TestEventFallsBackWithoutAnInstanceLogger(t *testing.T) {
	prev := slog.Default()
	slog.SetDefault(discardLog())
	defer slog.SetDefault(prev)
	l := &Ledger{logs: map[string]*slog.Logger{}}
	l.event("absent", human.New("probe"))
}

// TestSweepCreditsTheCurrentRunAsItsOwnCopy: the run being written is a
// verified run on this storage, so the policy may rotate the previous run it
// has just replaced; without the credit the old run would look like the last
// verified copy and survive.
func TestSweepCreditsTheCurrentRunAsItsOwnCopy(t *testing.T) {
	base := time.Date(2026, 10, 1, 4, 0, 0, 0, time.UTC)
	f := storagetest.NewFake()
	old := withManifest(verifiedRun("old", base, model.RunKindScheduled, "alpha"))
	for _, o := range old.Objects {
		f.PutObject(o.Key, []byte("x"), o.ModTime)
	}
	l := testLedger(t, map[string]storage.Storage{"alpha": f},
		map[string]map[model.RunKind]retention.Retention{"alpha": {model.RunKindScheduled: fixedPolicy{drop: []string{"old"}}}},
		nil, base)
	survey := surveyOf(map[string]inventory.Run{"old": old})
	l.Start(map[string]inventory.Survey{"alpha": survey})
	overlay := &model.StoredRun{RunID: "current", Kind: model.RunKindScheduled,
		At: base.Add(time.Hour), Bytes: old.Stored.Bytes, Verified: true, Manifest: true}
	res := l.Sweep(context.Background(), Sweep{RunStart: base, Kind: model.RunKindScheduled, Storage: "alpha",
		Survey: survey, Overlay: overlay,
		Artifacts: old.Manifest.Artifacts, Verified: map[string]bool{"prod": true}, ManifestSaved: true})
	if res.Err != nil || len(res.Deleted) != 1 || res.Deleted[0] != "old" {
		t.Fatalf("sweep = %+v", res)
	}
	if !l.state.verified["current"]["alpha"] {
		t.Fatal("the current run was not credited as a verified run")
	}
}

// TestSweepCurrentRunWithoutManifestCreditsNoCopy: a run whose manifest is not
// saved and whose artifacts did not all verify is not a verified run, so the
// only verified copy survives.
func TestSweepCurrentRunWithoutManifestCreditsNoCopy(t *testing.T) {
	base := time.Date(2026, 10, 1, 4, 0, 0, 0, time.UTC)
	f := storagetest.NewFake()
	old := withManifest(verifiedRun("old", base, model.RunKindScheduled, "alpha"))
	for _, o := range old.Objects {
		f.PutObject(o.Key, []byte("x"), o.ModTime)
	}
	l := testLedger(t, map[string]storage.Storage{"alpha": f},
		map[string]map[model.RunKind]retention.Retention{"alpha": {model.RunKindScheduled: fixedPolicy{drop: []string{"old"}}}},
		nil, base)
	survey := surveyOf(map[string]inventory.Run{"old": old})
	l.Start(map[string]inventory.Survey{"alpha": survey})
	overlay := &model.StoredRun{RunID: "current", Kind: model.RunKindScheduled, At: base.Add(time.Hour)}
	res := l.Sweep(context.Background(), Sweep{RunStart: base, Kind: model.RunKindScheduled, Storage: "alpha",
		Survey: survey, Overlay: overlay})
	if res.Err != nil || len(res.Deleted) != 0 {
		t.Fatalf("sweep deleted the only verified run: %+v", res)
	}
	if l.state.verified["current"] != nil {
		t.Fatal("an unverified current run was credited as verified")
	}
}

// TestSweepCreditsOnlyVerifiedArtifacts: a partially verified run holds only the
// copies this storage verified, so the newest copy of an unverified artifact is
// still the previous run's.
func TestSweepCreditsOnlyVerifiedArtifacts(t *testing.T) {
	base := time.Date(2026, 10, 1, 4, 0, 0, 0, time.UTC)
	f := storagetest.NewFake()
	l := testLedger(t, map[string]storage.Storage{"alpha": f}, nil, nil, base)
	survey := surveyOf(nil)
	l.Start(map[string]inventory.Survey{"alpha": survey})
	artifacts := []model.ArtifactResult{
		{Artifact: model.Artifact{Group: "databases", Name: "prod"}, Outcome: model.ArtifactOK, Key: "runs/current/databases/prod"},
		{Artifact: model.Artifact{Group: "volumes", Name: "uploads"}, Outcome: model.ArtifactOK, Key: "runs/current/volumes/uploads"},
	}
	overlay := &model.StoredRun{RunID: "current", Kind: model.RunKindScheduled, At: base, Manifest: true}
	res := l.Sweep(context.Background(), Sweep{RunStart: base, Kind: model.RunKindScheduled, Storage: "alpha",
		Survey: survey, Overlay: overlay, Artifacts: artifacts,
		Verified: map[string]bool{"prod": true, "uploads": false}, ManifestSaved: true})
	if res.Err != nil {
		t.Fatalf("sweep = %+v", res)
	}
	held := l.state.held["databases/prod"]["alpha"][model.RunKindScheduled]
	if _, ok := held["current"]; !ok {
		t.Fatal("the verified artifact copy was not credited")
	}
	if _, ok := l.state.held["volumes/uploads"]; ok {
		t.Fatal("an unverified artifact copy was credited")
	}
}

// TestMarkUncertainClosesCapEviction: one MarkUncertain call is the complete
// unknown-occupancy policy — the named run is protected and every storage's cap
// eviction is closed, so a scheduled run over the cap reports the overflow and
// deletes nothing.
func TestMarkUncertainClosesCapEviction(t *testing.T) {
	base := time.Date(2026, 10, 1, 4, 0, 0, 0, time.UTC)
	f := storagetest.NewFake()
	old := withManifest(verifiedRun("old", base, model.RunKindScheduled, "alpha"))
	for _, o := range old.Objects {
		f.PutObject(o.Key, []byte("x"), o.ModTime)
	}
	var buf bytes.Buffer
	instanceLog := slog.New(human.NewHandler(&buf, slog.LevelInfo)).
		With("kind", "storage", "type", "local", "instance", "alpha")
	l := NewLedger(LedgerOptions{
		Destinations: []guard.Destination{testDestinationLogged(t, "alpha", f, instanceLog)},
		Caps:         map[string]int64{"alpha": 1},
		StaleCutoff:  base,
		Retry:        execution.Options{Attempts: 1, Unclassified: pluginerrors.ClassPermanent},
	})
	survey := surveyOf(map[string]inventory.Run{"old": old})
	l.Start(map[string]inventory.Survey{"alpha": survey})
	l.MarkUncertain("dead")
	if l.caps["alpha"].occupancyKnown() {
		t.Fatal("MarkUncertain left a storage's occupancy known")
	}
	res := l.Sweep(context.Background(), Sweep{Kind: model.RunKindScheduled, Storage: "alpha", Survey: survey})
	if res.Err != nil || len(res.Deleted) != 0 {
		t.Fatalf("cap eviction ran under an unknown occupancy: %+v", res)
	}
	if !strings.Contains(buf.String(), "storage.cap.unknown") {
		t.Fatalf("the closed eviction was not reported:\n%s", buf.String())
	}
	if ok, _ := l.state.canDrop("dead", "alpha"); ok {
		t.Fatal("MarkUncertain did not protect the named run")
	}
}

// leaseOf returns a lease a test may bind where nothing else uses it.
func leaseOf(t *testing.T) *Lease {
	t.Helper()
	return testLease(&scripted{}, nil)
}

// TestC2_AuthorityComesFromTheLedger: authority is issued only from verified
// protection state — constructed destinations, the completed survey and a bound
// held claim; before that the ledger mints an expired authority.
func TestC2_AuthorityComesFromTheLedger(t *testing.T) {
	ctx := context.Background()
	l := testLedger(t, map[string]storage.Storage{"alpha": storagetest.NewFake()}, nil, nil, time.Time{})
	var noLedger *Ledger
	noLedger.BindLease(leaseOf(t))
	if a := noLedger.Authorize(time.Now().Add(time.Hour)); !a.Expired() {
		t.Fatal("a nil ledger minted a live authority")
	}
	if a := l.Authorize(time.Now().Add(time.Hour)); !a.Expired() {
		t.Fatal("a ledger without a lease or survey minted a live authority")
	}
	lease := testLease(&scripted{}, nil)
	if _, _, err := lease.Acquire(ctx, model.RunKindScheduled, "run-1"); err != nil {
		t.Fatal(err)
	}
	l.BindLease(lease)
	if a := l.Authorize(time.Now().Add(time.Hour)); !a.Expired() {
		t.Fatal("a ledger without a completed survey minted a live authority")
	}
	l.Start(map[string]inventory.Survey{"alpha": {Storage: "alpha", OK: true}})
	if a := l.Authorize(time.Now().Add(time.Hour)); a == nil || a.Expired() {
		t.Fatal("the verified ledger issued no live authority")
	}
	if a := l.Authorize(time.Now().Add(-time.Second)); !a.Expired() {
		t.Fatal("the ledger issued a live authority for a past window")
	}
	// A released claim is not verified state either.
	if err := lease.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if a := l.Authorize(time.Now().Add(time.Hour)); !a.Expired() {
		t.Fatal("a released claim still minted a live authority")
	}
}

// TestR2_C2_LeaseFreeScheduledLedgerAuthorizes: a scheduled run whose catalog
// cannot hold a claim is lease-free; the verified ledger (destinations +
// completed survey, no bound lease) still issues a usable authority. A bound but
// unheld claim refuses, and a started ledger with no destinations refuses.
func TestR2_C2_LeaseFreeScheduledLedgerAuthorizes(t *testing.T) {
	ctx := context.Background()
	fake := storagetest.NewFake()
	d := testDestination(t, "alpha", fake)
	l := NewLedger(LedgerOptions{
		Destinations: []guard.Destination{d},
		Retry:        execution.Options{Attempts: 1, Unclassified: pluginerrors.ClassPermanent},
	})
	l.Start(map[string]inventory.Survey{"alpha": {Storage: "alpha", OK: true}})
	authority := l.Authorize(time.Now().Add(time.Hour))
	if authority == nil || authority.Expired() {
		t.Fatal("a lease-free scheduled ledger minted no usable authority")
	}
	h, err := d.Port().Open(authority)
	if err != nil {
		t.Fatalf("the lease-free authority opened no handle: %v", err)
	}
	if _, err := h.Put(ctx, storage.PutRequest{Key: "k", Size: 1}, strings.NewReader("x")); err != nil {
		t.Fatalf("the lease-free authority could not write: %v", err)
	}

	// Once a lease is bound, its claim must be held: a lease that never acquired
	// one is not authority.
	l.BindLease(testLease(&scripted{}, nil))
	if a := l.Authorize(time.Now().Add(time.Hour)); !a.Expired() {
		t.Fatal("a bound but never-held claim minted a live authority")
	}

	// A started ledger without destinations is not verified state either.
	empty := NewLedger(LedgerOptions{
		Retry: execution.Options{Attempts: 1, Unclassified: pluginerrors.ClassPermanent},
	})
	empty.Start(nil)
	if a := empty.Authorize(time.Now().Add(time.Hour)); !a.Expired() {
		t.Fatal("a destination-less ledger minted a live authority")
	}
}

// TestR3_C2_AccountingViewIsSealed: the view a transfer receives is a
// protection-owned wrapper, never the ledger. The sealed interface carries only
// the four booking calls; *Ledger cannot stand in for it, and neither a
// concrete-*Ledger assertion nor an issuer-interface assertion succeeds. The
// view still books capacity.
func TestR3_C2_AccountingViewIsSealed(t *testing.T) {
	fake := storagetest.NewFake()
	d := testDestination(t, "alpha", fake)
	l := NewLedger(LedgerOptions{
		Destinations: []guard.Destination{d},
		Retry:        execution.Options{Attempts: 1, Unclassified: pluginerrors.ClassPermanent},
	})
	l.Start(map[string]inventory.Survey{"alpha": {Storage: "alpha", OK: true}})

	view := l.Accounting()
	typ := reflect.TypeOf((*Accounting)(nil)).Elem()
	if reflect.TypeOf((*Ledger)(nil)).Implements(typ) {
		t.Fatal("*Ledger satisfies Accounting; the interface is not sealed")
	}
	if reflect.TypeOf(view) == reflect.TypeOf((*Ledger)(nil)) {
		t.Fatal("the accounting view's dynamic value is the concrete ledger")
	}
	allowed := map[string]bool{"accountingView": true, "Admit": true, "Shrink": true, "Commit": true, "Unreserve": true}
	if typ.NumMethod() != len(allowed) {
		t.Fatalf("Accounting has %d methods, want the sealed marker plus four bookings", typ.NumMethod())
	}
	for i := 0; i < typ.NumMethod(); i++ {
		if name := typ.Method(i).Name; !allowed[name] {
			t.Fatalf("Accounting exposes %q, outside the capacity-only surface", name)
		}
	}
	if _, ok := any(view).(interface {
		Authorize(time.Time) *guard.Authority
	}); ok {
		t.Fatal("the accounting view exposes an issuer through a type assertion")
	}
	if _, ok := any(view).(*Ledger); ok {
		t.Fatal("the accounting view asserts back to the concrete ledger")
	}
	view.(interface{ accountingView() }).accountingView()

	// The view still books capacity for a real upload.
	if !view.Admit("alpha", "k", 10, true) {
		t.Fatal("the view refused a write within the cap")
	}
	view.Shrink("alpha", "k", 5)
	view.Commit("alpha", "k", 5)
	if got := l.Used("alpha"); got != 5 {
		t.Fatalf("used through the view = %d, want 5", got)
	}
	view.Unreserve("alpha", "k")
}

// capturingPolicy records the run list the ledger hands the policy and applies a
// scripted decision.
type capturingPolicy struct {
	drop      []string
	keepFloor []string
	got       []model.StoredRun
}

func (p *capturingPolicy) Decide(_ context.Context, req retention.DecideRequest) (retention.Decision, error) {
	p.got = append([]model.StoredRun(nil), req.Runs...)
	return retention.Decision{Drop: p.drop, KeepFloor: p.keepFloor}, nil
}

// TestSweepProjectsHeldAndObservedIdentities: the ledger is the ONE source of
// the policy's artifact input — the successful copies it holds for this storage,
// and every identity a readable manifest or the current run's own results named,
// whatever the outcome.
func TestSweepProjectsHeldAndObservedIdentities(t *testing.T) {
	base := time.Date(2026, 10, 1, 4, 0, 0, 0, time.UTC)
	f := storagetest.NewFake()
	past := verifiedRun("past", base, model.RunKindScheduled, "alpha")
	past.Manifest.Artifacts = append(past.Manifest.Artifacts,
		model.ArtifactResult{Artifact: model.Artifact{Group: "databases", Name: "dropped"}, Outcome: model.ArtifactFailed})
	policy := &capturingPolicy{}
	l := testLedger(t, map[string]storage.Storage{"alpha": f},
		map[string]map[model.RunKind]retention.Retention{"alpha": {model.RunKindScheduled: policy}}, nil, base)
	survey := surveyOf(map[string]inventory.Run{"past": past})
	l.Start(map[string]inventory.Survey{"alpha": survey})
	current := model.StoredRun{RunID: "current", Kind: model.RunKindScheduled, At: base.Add(time.Hour)}
	res := l.Sweep(context.Background(), Sweep{RunStart: base.Add(time.Hour), Kind: model.RunKindScheduled,
		Storage: "alpha", Survey: survey, Overlay: &current,
		Artifacts: []model.ArtifactResult{
			{Artifact: model.Artifact{Group: "databases", Name: "prod"}, Outcome: model.ArtifactOK},
			{Artifact: model.Artifact{Group: "cache", Name: "redis"}, Outcome: model.ArtifactFailed},
			{Artifact: model.Artifact{Group: "cache", Name: "redis"}, Outcome: model.ArtifactFailed},
			{Artifact: model.Artifact{Group: "unnamed"}, Outcome: model.ArtifactFailed},
		},
		Verified: map[string]bool{"prod": true}, ManifestSaved: true})
	if res.Err != nil {
		t.Fatalf("sweep = %+v", res)
	}
	got := map[string]model.StoredRun{}
	for _, r := range policy.got {
		got[r.RunID] = r
	}
	if !slices.Equal(got["past"].Artifacts, []string{"databases/prod"}) {
		t.Fatalf("past Artifacts = %v, want the one held copy", got["past"].Artifacts)
	}
	if !slices.Equal(got["past"].ObservedArtifacts, []string{"databases/prod", "databases/dropped"}) {
		t.Fatalf("past ObservedArtifacts = %v, want every named identity", got["past"].ObservedArtifacts)
	}
	if !slices.Equal(got["current"].Artifacts, []string{"databases/prod"}) {
		t.Fatalf("current Artifacts = %v, want the verified and saved copy", got["current"].Artifacts)
	}
	if !slices.Equal(got["current"].ObservedArtifacts, []string{"databases/prod", "cache/redis"}) {
		t.Fatalf("current ObservedArtifacts = %v, want every produced identity", got["current"].ObservedArtifacts)
	}
}

// TestForeignRunsAreIsolatedFromCreditPolicyAndDeletion: with a writer identity,
// another writer's runs and runs whose manifest cannot prove ownership are never
// handed to the policy, credited or deleted — a policy naming them changes
// nothing — while the deletion root refuses them by name.
func TestForeignRunsAreIsolatedFromCreditPolicyAndDeletion(t *testing.T) {
	base := time.Date(2026, 10, 1, 4, 0, 0, 0, time.UTC)
	f := storagetest.NewFake()
	ownOld := withManifest(verifiedRun("own-old", base, model.RunKindScheduled, "alpha"))
	ownOld.Manifest.WriterIdentity = "node-a"
	ownNew := withManifest(verifiedRun("own-new", base.Add(time.Hour), model.RunKindScheduled, "alpha"))
	ownNew.Manifest.WriterIdentity = "node-a"
	foreign := withManifest(verifiedRun("foreign", base.Add(2*time.Hour), model.RunKindScheduled, "alpha"))
	foreign.Manifest.WriterIdentity = "node-b"
	unowned := inventory.Run{
		Stored:  model.StoredRun{RunID: "unowned", Kind: model.RunKindScheduled, At: base.Add(3 * time.Hour), Bytes: 4},
		Objects: []model.ObjectInfo{{Key: "runs/unowned/x", Size: 4}},
	}
	runs := map[string]inventory.Run{"own-old": ownOld, "own-new": ownNew, "foreign": foreign, "unowned": unowned}
	for _, r := range runs {
		for _, o := range r.Objects {
			f.PutObject(o.Key, []byte("0123456789")[:o.Size], base)
		}
	}
	policy := &capturingPolicy{drop: []string{"own-old", "foreign", "unowned"}}
	l := testLedgerWriting(t, "node-a", map[string]storage.Storage{"alpha": f},
		map[string]map[model.RunKind]retention.Retention{"alpha": {model.RunKindScheduled: policy}}, nil, base)
	survey := surveyOf(runs)
	l.Start(map[string]inventory.Survey{"alpha": survey})
	res := l.Sweep(context.Background(), Sweep{RunStart: base.Add(4 * time.Hour), Kind: model.RunKindScheduled,
		Storage: "alpha", Survey: survey})
	if res.Err != nil {
		t.Fatalf("sweep = %+v", res)
	}
	for _, r := range policy.got {
		if r.RunID != "own-old" && r.RunID != "own-new" {
			t.Fatalf("a foreign run reached the policy: %+v", policy.got)
		}
	}
	if len(policy.got) != 2 {
		t.Fatalf("the policy saw %d runs, want this writer's two", len(policy.got))
	}
	if !slices.Contains(res.Deleted, "own-old") {
		t.Fatalf("the policy's own drop did not run: %+v", res)
	}
	if _, ok := f.Bytes("runs/foreign/databases/prod"); !ok {
		t.Fatal("a foreign run was deleted")
	}
	if _, ok := f.Bytes("runs/unowned/x"); !ok {
		t.Fatal("a run that cannot prove its writer was deleted")
	}
	if ok, reason := l.state.canDrop("foreign", "alpha"); ok || reason == "" {
		t.Fatalf("the deletion root let a foreign run through: (%v, %q)", ok, reason)
	}
	if ok, reason := l.state.canDrop("unowned", "alpha"); ok || reason == "" {
		t.Fatalf("the deletion root let an unowned run through: (%v, %q)", ok, reason)
	}
	if l.state.verified["foreign"] != nil || l.state.verified["unowned"] != nil {
		t.Fatalf("a foreign run was credited as a verified copy: %+v", l.state.verified)
	}
	for artifact, byStorage := range l.state.held {
		for _, byKind := range byStorage["alpha"] {
			if _, ok := byKind["foreign"]; ok {
				t.Fatalf("%s credited a foreign run's copy: %+v", artifact, byKind)
			}
		}
	}
}

// TestSweepReportsCapEvictedRunsAndOverflow: the cap's deletions are named apart
// from the policy's, and the bytes it cannot free are reported as overflow. A
// foreign run's bytes are part of the occupancy the cap is measured against.
func TestSweepReportsCapEvictedRunsAndOverflow(t *testing.T) {
	base := time.Date(2026, 10, 1, 4, 0, 0, 0, time.UTC)
	makeRuns := func(withForeign bool) (map[string]inventory.Run, *storagetest.Fake) {
		f := storagetest.NewFake()
		old := withManifest(verifiedRun("old", base, model.RunKindScheduled, "alpha"))
		old.Manifest.WriterIdentity = "node-a"
		newer := withManifest(verifiedRun("newer", base.Add(time.Hour), model.RunKindScheduled, "alpha"))
		newer.Manifest.WriterIdentity = "node-a"
		runs := map[string]inventory.Run{"old": old, "newer": newer}
		if withForeign {
			foreign := withManifest(verifiedRun("foreign", base.Add(2*time.Hour), model.RunKindScheduled, "alpha"))
			foreign.Manifest.WriterIdentity = "node-b"
			runs["foreign"] = foreign
		}
		for _, r := range runs {
			for _, o := range r.Objects {
				f.PutObject(o.Key, []byte("0123456789")[:o.Size], base)
			}
		}
		return runs, f
	}
	// Two own runs are 5+7 bytes and the foreign one 9, against an 11-byte cap:
	// the foreign bytes are part of the occupancy, so the cap cannot hold both
	// own runs and still reports the bytes it could not free.
	runs, f := makeRuns(true)
	l := testLedgerWriting(t, "node-a", map[string]storage.Storage{"alpha": f}, nil,
		map[string]int64{"alpha": 11}, base)
	survey := surveyOf(runs)
	l.Start(map[string]inventory.Survey{"alpha": survey})
	res := l.Sweep(context.Background(), Sweep{RunStart: base.Add(3 * time.Hour), Kind: model.RunKindScheduled,
		Storage: "alpha", Survey: survey})
	if res.Err != nil {
		t.Fatalf("sweep = %+v", res)
	}
	if !slices.Equal(res.CapEvicted, []string{"old"}) {
		t.Fatalf("CapEvicted = %v, want the older own run", res.CapEvicted)
	}
	if res.CapOverflow != 5 {
		t.Fatalf("CapOverflow = %d, want the 5 bytes the foreign occupancy leaves over", res.CapOverflow)
	}
	if _, ok := f.Bytes("runs/foreign/databases/prod"); !ok {
		t.Fatal("the cap deleted a foreign run")
	}

	// Without the foreign bytes the same cap fits both own runs instead of
	// spilling over, and the eviction is still named.
	runs, f = makeRuns(false)
	l = testLedger(t, map[string]storage.Storage{"alpha": f}, nil, map[string]int64{"alpha": 11}, base)
	survey = surveyOf(runs)
	l.Start(map[string]inventory.Survey{"alpha": survey})
	res = l.Sweep(context.Background(), Sweep{RunStart: base.Add(3 * time.Hour), Kind: model.RunKindScheduled,
		Storage: "alpha", Survey: survey})
	if res.Err != nil {
		t.Fatalf("sweep = %+v", res)
	}
	if !slices.Equal(res.CapEvicted, []string{"old"}) {
		t.Fatalf("CapEvicted = %v, want the older own run", res.CapEvicted)
	}
	if res.CapOverflow != 0 {
		t.Fatalf("CapOverflow = %d, want none after the eviction", res.CapOverflow)
	}
}

// TestOccupancyKnownFollowsTheSurvey: the authoritative occupancy state is the// ledger's, not a caller's guess — unknown before and after a failed survey, and
// whenever an earlier writer's accepted writes may still land.
func TestOccupancyKnownFollowsTheSurvey(t *testing.T) {
	l := testLedger(t, map[string]storage.Storage{"alpha": storagetest.NewFake()}, nil, nil, time.Time{})
	if l.OccupancyKnown("alpha") {
		t.Fatal("the occupancy was known before any survey")
	}
	l.Start(map[string]inventory.Survey{"alpha": {Storage: "alpha", OK: true}})
	if !l.OccupancyKnown("alpha") {
		t.Fatal("a completed survey left the occupancy unknown")
	}
	l.Unknown("alpha")
	if l.OccupancyKnown("alpha") {
		t.Fatal("an explicitly unknown occupancy reported itself known")
	}
	if (*Ledger)(nil).OccupancyKnown("alpha") {
		t.Fatal("a nil ledger claimed a known occupancy")
	}
}

// TestSweepKeepVersionsRetainsTheNewestCopiesOfLiveArtifacts is the issue #3.5
// reproduction through the one ledger: the newest seven runs delivered both
// databases, three older runs failed the first one but still hold a good copy of
// the second — and the failed history goes, while seven usable copies stay.
func TestSweepKeepVersionsRetainsTheNewestCopiesOfLiveArtifacts(t *testing.T) {
	base := time.Date(2026, 10, 1, 4, 0, 0, 0, time.UTC)
	f := storagetest.NewFake()
	makeRun := func(id string, day int, firstOK, secondOK bool) inventory.Run {
		at := base.AddDate(0, 0, -day)
		outcome := model.OutcomeDelivered
		if !firstOK || !secondOK {
			outcome = model.OutcomeFailed
		}
		r := inventory.Run{
			Stored:      model.StoredRun{RunID: id, Kind: model.RunKindScheduled, At: at, Manifest: true},
			ManifestKey: "runs/" + id + "/manifest.json",
			Manifest:    &model.RunReport{RunID: id, Kind: model.RunKindScheduled, StartedAt: at, Outcome: outcome},
		}
		for _, a := range []struct {
			name string
			ok   bool
		}{{"first", firstOK}, {"second", secondOK}} {
			ar := model.ArtifactResult{Artifact: model.Artifact{Group: "databases", Name: a.name}, Outcome: model.ArtifactFailed}
			if a.ok {
				body := []byte(id + "/" + a.name)
				key := "runs/" + id + "/databases/" + a.name
				ar.Outcome, ar.Key, ar.Bytes, ar.SHA256 = model.ArtifactOK, key, int64(len(body)), model.SHA256Hex(body)
				r.Objects = append(r.Objects, model.ObjectInfo{Key: key, Size: int64(len(body))})
				r.Stored.Bytes += int64(len(body))
			}
			r.Manifest.Artifacts = append(r.Manifest.Artifacts, ar)
		}
		return r
	}
	runs := map[string]inventory.Run{}
	for day := range 7 {
		id := fmt.Sprintf("good-%d", day)
		runs[id] = makeRun(id, day, true, true)
	}
	for day := 7; day < 10; day++ {
		id := fmt.Sprintf("old-%d", day)
		runs[id] = makeRun(id, day, false, true)
	}
	for _, r := range runs {
		for _, o := range r.Objects {
			f.PutObject(o.Key, bytes.Repeat([]byte("x"), int(o.Size)), r.Stored.At)
		}
	}
	lr := loader.New()
	reg := instances.NewRegistry()
	if err := loader.Add(lr, keepversions.Spec, func(lr *loader.Registry) error {
		return keepversions.Apply(lr, reg)
	}); err != nil {
		t.Fatal(err)
	}
	policy, err := instances.Build(context.Background(), reg, retention.Kind, "keep_versions",
		jsontext.Value(`{"count": 7, "min_keep": 1, "count_incomplete": true}`),
		instances.Deps{Instance: "keep_versions", Log: discardLog()})
	if err != nil {
		t.Fatal(err)
	}
	l := testLedger(t, map[string]storage.Storage{"alpha": f},
		map[string]map[model.RunKind]retention.Retention{"alpha": {model.RunKindScheduled: policy}}, nil, base)
	survey := surveyOf(runs)
	l.Start(map[string]inventory.Survey{"alpha": survey})
	res := l.Sweep(context.Background(), Sweep{RunStart: base.AddDate(0, 0, 1), Kind: model.RunKindScheduled,
		Storage: "alpha", Survey: survey})
	if res.Err != nil {
		t.Fatalf("sweep = %+v", res)
	}
	if res.Kept != 7 || len(res.Deleted) != 3 {
		t.Fatalf("kept %d deleted %v, want the seven delivered runs", res.Kept, res.Deleted)
	}
	left := runIDsOf(f)
	for day := range 7 {
		if !left[fmt.Sprintf("good-%d", day)] {
			t.Fatalf("a usable copy was deleted: %v", left)
		}
	}
	if len(left) != 7 {
		t.Fatalf("storage still holds %d runs: %v", len(left), left)
	}
}

// runIDsOf lists the run ids a fake still holds objects for.
func runIDsOf(f *storagetest.Fake) map[string]bool {
	out := map[string]bool{}
	for _, key := range f.Keys() {
		rest, ok := strings.CutPrefix(key, "runs/")
		if !ok {
			continue
		}
		if id, _, found := strings.Cut(rest, "/"); found && id != "" {
			out[id] = true
		}
	}
	return out
}
