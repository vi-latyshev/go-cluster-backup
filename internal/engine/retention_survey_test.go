package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/storage/storagetest"
)

// TestSurveyStoragePaginates: a listing that hands back a token is followed
// until it stops, every page's objects landing in the survey.
func TestSurveyStoragePaginates(t *testing.T) {
	w := newWorld(t)
	e := w.newEngine(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}))
	f := w.storage(t, "primary")
	f.PageSize = 1
	now := time.Now().UTC()
	f.PutObject("runs/r1/a.bin", []byte("x"), now)
	f.PutObject("runs/r1/b.bin", []byte("y"), now)
	sur := e.surveyStorage(context.Background(), e.storages[0], "runs/", e.retry, nil)
	if !sur.ok {
		t.Fatalf("survey failed: %v", sur.err)
	}
	if got := len(sur.runs["r1"].objects); got != 2 {
		t.Fatalf("surveyed %d objects, want 2 across the pages", got)
	}
}

// TestSurveyStorageStopsARunawayListing: a listing that never terminates is
// refused rather than looped forever.
func TestSurveyStorageStopsARunawayListing(t *testing.T) {
	w := newWorld(t)
	e := w.newEngine(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}))
	f := w.storage(t, "primary")
	f.PageSize = 1
	now := time.Now().UTC()
	for i := 0; i < 502; i++ {
		f.PutObject(fmt.Sprintf("runs/r1/o%04d", i), []byte("x"), now)
	}
	sur := e.surveyStorage(context.Background(), e.storages[0], "runs/", e.retry, nil)
	if sur.ok || sur.err == nil || !strings.Contains(sur.err.Error(), "did not terminate") {
		t.Fatalf("survey = ok=%v err=%v, want the runaway-listing refusal", sur.ok, sur.err)
	}
}

// TestSurveyStorageIgnoresObjectsOutsideARun: an object the prefix matches
// but that names no run directory is not the survey's to reason about.
func TestSurveyStorageIgnoresObjectsOutsideARun(t *testing.T) {
	w := newWorld(t)
	e := w.newEngine(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}))
	f := w.storage(t, "primary")
	f.PutObject("runs/toplevel.bin", []byte("x"), time.Now().UTC())
	sur := e.surveyStorage(context.Background(), e.storages[0], "runs/", e.retry, nil)
	if !sur.ok {
		t.Fatalf("survey failed: %v", sur.err)
	}
	if len(sur.runs) != 0 {
		t.Fatalf("survey kept %d runs from an object outside any run dir", len(sur.runs))
	}
}

// TestSurveyStorageManifestKeyFailure: a run id the manifest key cannot be
// rendered from leaves the run's manifest unknown, never "absent" — so it is
// refused deletion for the sweep.
func TestSurveyStorageManifestKeyFailure(t *testing.T) {
	w := newWorld(t)
	e := w.newEngine(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}))
	f := w.storage(t, "primary")
	f.PutObject("runs/../x.bin", []byte("x"), time.Now().UTC())
	sur := e.surveyStorage(context.Background(), e.storages[0], "runs/", e.retry, nil)
	if !sur.ok {
		t.Fatalf("survey failed: %v", sur.err)
	}
	r := sur.runs[".."]
	if r == nil || !strings.Contains(r.manifestProblem, "does not render") {
		t.Fatalf("run %+v, want a manifest-key problem", r)
	}
}

// TestSurveyStorageKindDisagreement: a manifest whose kind disagrees with the
// run id is a problem run, never permission to delete.
func TestSurveyStorageKindDisagreement(t *testing.T) {
	w := newWorld(t)
	e := w.newEngine(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}))
	f := w.storage(t, "primary")
	now := time.Now().UTC()
	id := "20261001T040000Z"
	rep := model.RunReport{
		Schema: model.ManifestSchema, RunID: id, Kind: "weekly", StartedAt: now, Outcome: model.OutcomeDelivered,
		Storages: []model.StorageResult{{Storage: "primary", Outcome: model.StorageVerified}},
	}
	f.PutObject("runs/"+id+"/a.bin", []byte("x"), now)
	f.PutObject("runs/"+id+"/manifest.json", mustMarshal(t, rep), now)
	sur := e.surveyStorage(context.Background(), e.storages[0], "runs/", e.retry, nil)
	r := sur.runs[id]
	if r == nil || !strings.Contains(r.manifestProblem, "the run id says") {
		t.Fatalf("run %+v, want the kind disagreement recorded", r)
	}
}

// TestFetchManifestAbsentIsNilNil: a manifest listed but answered not_found is
// genuinely absent — (nil, nil), never an error the survey would call a problem.
func TestFetchManifestAbsentIsNilNil(t *testing.T) {
	w := newWorld(t)
	e := w.newEngine(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}))
	f := w.storage(t, "primary")
	key := "runs/r1/manifest.json"
	f.PutObject(key, []byte("{}"), time.Now().UTC())
	f.Fault = func(op storagetest.Op, k string, _ int) error {
		if op == storagetest.OpGet && k == key {
			return plugin.NotFound(errors.New("gone"))
		}
		return nil
	}
	rep, err := e.fetchManifest(context.Background(), e.storages[0], key, e.retry)
	if rep != nil || err != nil {
		t.Fatalf("fetchManifest = (%v, %v), want (nil, nil) for an absent manifest", rep, err)
	}
}

// TestMarkCurrentSkipsAnUnnamedArtifact: a nameless artifact carries no
// identity to credit, so it never enters the copy ledger.
func TestMarkCurrentSkipsAnUnnamedArtifact(t *testing.T) {
	st := newRetentionState(nil)
	st.markCurrent("run-1", model.RunKindScheduled, time.Now().UTC(),
		[]model.ArtifactResult{{Artifact: model.Artifact{Group: "g"}, Outcome: model.ArtifactOK}}, nil, nil)
	if len(st.held) != 0 {
		t.Fatalf("an unnamed artifact was credited: %+v", st.held)
	}
	if st.kinds["run-1"] != model.RunKindScheduled {
		t.Fatalf("the run's kind was not recorded: %+v", st.kinds)
	}
}

// TestLatestCopyManualOnlyHolder: a manual run is pinned only while it is the
// storage's only verified copy of the artifact — that is what lets keep-last
// reclaim it once a scheduled copy covers it.
func TestLatestCopyManualOnlyHolder(t *testing.T) {
	st := newRetentionState(nil)
	st.kinds["m1"] = model.RunKindManual
	st.markHeld("g/n", "alpha", model.RunKindManual, "m1", time.Now().UTC())
	if got := st.latestCopy("m1", "alpha"); got != "it holds the only verified copy of g/n" {
		t.Fatalf("latestCopy = %q, want the only-copy pin", got)
	}
	// A second holder of any kind releases the manual pin.
	st.markHeld("g/n", "alpha", model.RunKindScheduled, "s1", time.Now().UTC())
	if got := st.latestCopy("m1", "alpha"); got != "" {
		t.Fatalf("latestCopy with another holder = %q, want no pin", got)
	}
}

// TestRetentionPhaseStopsAfterAFailedDrop: a destructive sweep stops at its
// first error — the runs after the failed one are left untouched.
func TestRetentionPhaseStopsAfterAFailedDrop(t *testing.T) {
	w := newWorld(t)
	cfg := manualConfig(t, map[string]any{"alpha": manualStorage(1 << 40)})
	withRetention(cfg, "alpha", keepLastN(1))
	e := w.newEngine(t, cfg)
	f := w.storage(t, "alpha")
	base := time.Date(2026, 10, 1, 4, 0, 0, 0, time.UTC)
	ids := []string{"r1", "r2", "r3"}
	for i, id := range ids {
		putCopiedRun(t, f, "alpha", id, base.AddDate(0, 0, i), model.OutcomeDelivered, [][2]string{{"databases", "prod"}})
	}
	f.Fault = func(op storagetest.Op, _ string, _ int) error {
		if op == storagetest.OpDelete {
			return plugin.Permanent(errors.New("delete refused"))
		}
		return nil
	}
	sb := e.storages[0]
	sur := e.surveyStorage(context.Background(), sb, "runs/", e.retry, nil)
	state := newRetentionState(map[string]survey{sb.name: sur})
	_, deleted, _, err := e.retentionPhase(context.Background(), base.AddDate(0, 0, 9),
		model.RunKindScheduled, sb, sur, state, nil, nil)
	if err == nil {
		t.Fatal("retentionPhase hid a failed deletion")
	}
	if len(deleted) != 0 {
		t.Fatalf("deleted %v after the first delete failed, want none", deleted)
	}
}

// newDeleter builds a deleter over one hand-built survey, for the guards no run
// path reaches.
func newDeleter(e *Engine, sb *storageBinding, runs map[string]*surveyRun, cap *capState, cutoff time.Time) *deleter {
	return &deleter{ctx: context.Background(), e: e, sb: sb,
		sur: survey{storage: sb.name, ok: true, runs: runs}, state: newRetentionState(nil), cutoff: cutoff, cap: cap}
}

// TestDropGuards: drop is a no-op after a failure, for the current run and for
// an id the survey does not hold.
func TestDropGuards(t *testing.T) {
	w := newWorld(t)
	e := w.newEngine(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}))
	sb := e.storages[0]
	runs := map[string]*surveyRun{
		"r1": {stored: model.StoredRun{RunID: "r1"}, objects: []model.ObjectInfo{{Key: "runs/r1/x", Size: 1}}},
	}

	failed := newDeleter(e, sb, runs, nil, time.Time{})
	failed.failed = errors.New("already failed")
	if got := failed.drop("r1"); got != 0 {
		t.Fatalf("drop after a failure freed %d, want 0", got)
	}

	current := newDeleter(e, sb, runs, nil, time.Time{})
	current.current = "r1"
	if got := current.drop("r1"); got != 0 {
		t.Fatalf("drop of the current run freed %d, want 0", got)
	}

	missing := newDeleter(e, sb, runs, nil, time.Time{})
	if got := missing.drop("nope"); got != 0 {
		t.Fatalf("drop of an unknown run freed %d, want 0", got)
	}
}

// TestEvictForCapStopsOnceTheIncompleteRunsFit: the cap's first pass drops the
// oldest incomplete attempt at a time and stops the moment the occupancy fits.
func TestEvictForCapStopsOnceTheIncompleteRunsFit(t *testing.T) {
	w := newWorld(t)
	e := w.newEngine(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}))
	sb := e.storages[0]
	now := time.Now().UTC()
	runs := map[string]*surveyRun{
		"a": {stored: model.StoredRun{RunID: "a", Kind: model.RunKindScheduled, At: now, Bytes: 60},
			objects: []model.ObjectInfo{{Key: "runs/a/x", Size: 60}}},
		"b": {stored: model.StoredRun{RunID: "b", Kind: model.RunKindScheduled, At: now, Bytes: 60},
			objects: []model.ObjectInfo{{Key: "runs/b/x", Size: 60}}},
	}
	cap := &capState{cap: 100, occupancy: 150, known: true, reserved: map[string]int64{}}
	d := newDeleter(e, sb, runs, cap, now.Add(time.Hour))
	d.evictForCap(cap, []model.StoredRun{runs["a"].stored, runs["b"].stored})
	if cap.used() != 90 {
		t.Fatalf("occupancy after eviction = %d, want 90 (one 60-byte run freed)", cap.used())
	}
	if len(d.deleted) != 1 {
		t.Fatalf("evicted %v, want exactly one incomplete run", d.deleted)
	}
}

// TestEvictForCapStopsMidScheduledHistory: the byte-budget pass drops the
// oldest scheduled runs until the occupancy fits, then stops.
func TestEvictForCapStopsMidScheduledHistory(t *testing.T) {
	w := newWorld(t)
	e := w.newEngine(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}))
	sb := e.storages[0]
	base := time.Date(2026, 10, 1, 4, 0, 0, 0, time.UTC)
	sizes := map[string]int64{"R1": 1, "R2": 1, "R3": 150, "R4": 1, "R5": 1}
	runs := map[string]*surveyRun{}
	var list []model.StoredRun
	i := 0
	for _, id := range []string{"R1", "R2", "R3", "R4", "R5"} {
		at := base.Add(time.Duration(5-i) * time.Hour)
		i++
		r := &surveyRun{
			stored:  model.StoredRun{RunID: id, Kind: model.RunKindScheduled, At: at, Bytes: sizes[id], Manifest: true},
			objects: []model.ObjectInfo{{Key: "runs/" + id + "/x", Size: sizes[id]}},
		}
		runs[id] = r
		list = append(list, r.stored)
	}
	cap := &capState{cap: 100, occupancy: 154, known: true, reserved: map[string]int64{}}
	d := newDeleter(e, sb, runs, cap, base)
	d.evictForCap(cap, list)
	if cap.used() != 4 {
		t.Fatalf("occupancy after eviction = %d, want 4 (the 150-byte run freed)", cap.used())
	}
	if len(d.deleted) != 1 || d.deleted[0] != "R3" {
		t.Fatalf("evicted %v, want exactly the 150-byte scheduled run", d.deleted)
	}
}

// --- preflight.go ------------------------------------------------------------
