package engine

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/storage/storagetest"
)

// twoSources is a run config with two static sources, the first of them small.
func twoSources(t *testing.T, w *world) map[string]any {
	t.Helper()
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["sources"] = map[string]any{
		"a_small": sourceInstance("fake", "data", "small", []string{"zstd3"}, "required", "plain"),
		"b_large": sourceInstance("fake", "data", "large", []string{"zstd3"}, "required", "plain"),
	}
	w.sources["a_small"] = &fakeSource{data: []byte("ok")}
	return cfg
}

// TestArtifactOverTheCapFailsItself: the run's cap stops the source that would
// cross it and fails that one artifact — permanently, so the spool is not
// filled with the same bytes twice — while every other artifact is delivered
// and no object of the oversized artifact reaches a storage.
func TestArtifactOverTheCapFailsItself(t *testing.T) {
	w := newWorld(t)
	cfg := twoSources(t, w)
	cfg["run"].(map[string]any)["max_artifact_bytes"] = 8

	rep, err := w.newEngine(t, cfg).Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Artifacts) != 2 {
		t.Fatalf("artifacts %+v", rep.Artifacts)
	}
	small, large := rep.Artifacts[0], rep.Artifacts[1]
	if small.Outcome != model.ArtifactOK {
		t.Fatalf("the artifact under the cap = %+v", small)
	}
	if large.Outcome != model.ArtifactFailed || !strings.Contains(large.Error, "max_artifact_bytes") {
		t.Fatalf("the artifact over the cap = %+v, want a failure naming the cap", large)
	}
	// Permanent, so the cap costs one attempt: the same bytes would cross it
	// again, and the retry budget is for faults that pass.
	if dumps := w.sources["b_large"].dumpCount(); dumps != 1 {
		t.Fatalf("the oversized artifact was attempted %d times, want the failure to be permanent", dumps)
	}
	keys := w.storage(t, "primary").Keys()
	for _, k := range keys {
		if strings.Contains(k, "large") {
			t.Fatalf("an artifact over the cap reached the storage: %v", keys)
		}
	}
	if rep.Outcome != model.OutcomeFailed {
		t.Fatalf("outcome %q, want failed for a required artifact", rep.Outcome)
	}
}

// TestDumpStatsRideIntoTheReport: what a source left out of the artifact is part
// of the artifact's record — the report, the manifest and the finished event —
// not only of a log line.
func TestDumpStatsRideIntoTheReport(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["notifiers"] = map[string]any{"chat": instance("fake")}
	w.sources["vpn"] = &fakeSource{data: []byte("bytes"), stats: model.DumpStats{SkippedEntries: 3, PaddedFiles: 1}}

	rep, err := w.newEngine(t, cfg).Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	want := model.DumpStats{SkippedEntries: 3, PaddedFiles: 1}
	if rep.Artifacts[0].Stats != want {
		t.Fatalf("report stats = %+v, want %+v", rep.Artifacts[0].Stats, want)
	}
	body, ok := w.storage(t, "primary").Bytes("runs/" + rep.RunID + "/manifest.json")
	if !ok {
		t.Fatal("manifest missing")
	}
	var stored model.RunReport
	if err := jsonUnmarshal(body, &stored); err != nil {
		t.Fatal(err)
	}
	if stored.Artifacts[0].Stats != want {
		t.Fatalf("manifest stats = %+v, want %+v", stored.Artifacts[0].Stats, want)
	}
	ev, ok := w.notifiers["chat"].last()
	if !ok || ev.Name != model.EventFinished || ev.Run.Artifacts[0].Stats != want {
		t.Fatalf("the finished event lost the counts: %+v", ev.Run.Artifacts)
	}
}

// TestDumpStatsFailureFailsTheArtifact: an artifact whose report cannot be
// produced is not a copy anyone can lean on, and the failure is this artifact's
// own.
func TestDumpStatsFailureFailsTheArtifact(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	w.sources["vpn"] = &fakeSource{data: []byte("bytes"), statsErr: errors.New("the tree listing is gone")}

	rep, err := w.newEngine(t, cfg).Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Artifacts[0].Outcome != model.ArtifactFailed || !strings.Contains(rep.Artifacts[0].Error, "the tree listing is gone") {
		t.Fatalf("artifact = %+v", rep.Artifacts[0])
	}
	if len(w.storage(t, "primary").Keys()) != 0 {
		t.Fatalf("an artifact with no report was uploaded: %v", w.storage(t, "primary").Keys())
	}
}

// TestDeadlineExpiryStillReportsTheRun: a run that spends its deadline on
// artifacts still describes what it uploaded — the manifest of every storage
// that took an object, the catalog entry and the finished notification — inside
// the finishing allowance.
func TestDeadlineExpiryStillReportsTheRun(t *testing.T) {
	w := newWorld(t)
	cfg := twoSources(t, w)
	cfg["notifiers"] = map[string]any{"chat": instance("fake")}
	cfg["catalog"] = instance("fake")
	cfg["run"].(map[string]any)["deadline"] = "300ms"
	cfg["run"].(map[string]any)["finish_allowance"] = "3s"
	w.sources["b_large"] = &fakeSource{data: []byte("slow"), dumpDelay: 2 * time.Second}

	rep, err := w.newEngine(t, cfg).Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeFailed {
		t.Fatalf("outcome %q, want failed: the deadline cut the second artifact", rep.Outcome)
	}
	if rep.Artifacts[0].Outcome != model.ArtifactOK {
		t.Fatalf("the artifact uploaded before the deadline = %+v", rep.Artifacts[0])
	}
	s := w.storage(t, "primary")
	if _, ok := s.Bytes("runs/" + rep.RunID + "/manifest.json"); !ok {
		t.Fatalf("the manifest was not written after the deadline: %v", s.Keys())
	}
	// The catalog's entry is the finished one: the run that could no longer
	// work still reported itself, so the next night's "previous run" is this
	// run and not a silence.
	if stored, ok := w.catalog.runs[rep.RunID]; !ok || stored.Outcome != model.OutcomeFailed {
		t.Fatalf("the catalog entry is %+v, want the finished failed report", w.catalog.runs)
	}
	ev, ok := w.notifiers["chat"].last()
	if !ok || ev.Name != model.EventFinished || ev.Run.Outcome != model.OutcomeFailed {
		t.Fatalf("the finished event after the deadline: %+v", ev)
	}
}

// TestExhaustedStorageBudgetStillGetsTheManifest: a storage whose time budget
// ran out during the artifacts still receives the manifest of what it accepted —
// the budget bounds the objects, not the record of them.
func TestExhaustedStorageBudgetStillGetsTheManifest(t *testing.T) {
	w := newWorld(t)
	cfg := twoSources(t, w)
	cfg["storages"] = map[string]any{"primary": func() map[string]any {
		s := storageInstance("fake", "keep_last")
		s["time_budget"] = "150ms"
		return s
	}()}
	e := w.newEngine(t, cfg)
	// One hundred milliseconds per object: the first fits the budget, the second
	// does not, and the manifest is not an object of that kind.
	w.storage(t, "primary").Fault = func(op storagetest.Op, key string, _ int) error {
		if op == storagetest.OpPut && !strings.HasSuffix(key, "manifest.json") {
			time.Sleep(100 * time.Millisecond)
		}
		return nil
	}

	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Artifacts[0].Outcome != model.ArtifactOK {
		t.Fatalf("the first artifact = %+v, want it delivered inside the budget", rep.Artifacts[0])
	}
	if rep.Artifacts[1].Outcome != model.ArtifactFailed {
		t.Fatalf("the second artifact = %+v, want it out of budget", rep.Artifacts[1])
	}
	if _, ok := w.storage(t, "primary").Bytes("runs/" + rep.RunID + "/manifest.json"); !ok {
		t.Fatalf("the manifest was lost with the budget: %v", w.storage(t, "primary").Keys())
	}
}

// TestAStalledManifestLeavesTheOtherStorageAndTheReport: a storage that holds
// its own manifest write past the slice the manifests get must not spend the
// time the other storage, the finished notification and the catalog entry need —
// the night is delivered and describable even when one destination is not.
func TestAStalledManifestLeavesTheOtherStorageAndTheReport(t *testing.T) {
	w := newWorld(t)
	cfg := twoSources(t, w)
	cfg["notifiers"] = map[string]any{"chat": instance("fake")}
	cfg["catalog"] = instance("fake")
	cfg["run"].(map[string]any)["finish_allowance"] = "1s"
	// The stall is longer than the slice the manifests get (half of the
	// allowance) and shorter than the allowance itself: a run that gave the
	// manifests the whole allowance would call it verified.
	stalling := &stallingStorage{Fake: storagetest.NewFake(), manifestDelay: 700 * time.Millisecond}
	w.extraStorages["z_stalling"] = stalling
	w.storages["z_stalling"] = stalling.Fake
	cfg["storages"] = map[string]any{
		"a_quick":    storageInstance("fake", "keep_last"),
		"z_stalling": storageInstance("fake", "keep_last"),
	}

	rep, err := w.newEngine(t, cfg).Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	// The quick storage holds everything, the stalling one failed its own
	// manifest and nothing else: degraded, and — the point — reported.
	if rep.Outcome != model.OutcomeDegraded {
		t.Fatalf("outcome %q, want degraded: %+v", rep.Outcome, rep.Storages)
	}
	key := "runs/" + rep.RunID + "/manifest.json"
	if _, ok := w.storage(t, "a_quick").Bytes(key); !ok {
		t.Fatalf("the storage that did not stall has no manifest: %v", w.storage(t, "a_quick").Keys())
	}
	ev, ok := w.notifiers["chat"].last()
	if !ok || ev.Name != model.EventFinished {
		t.Fatalf("the finished notification did not go out: %+v", w.notifiers["chat"].eventNames())
	}
	if stored, ok := w.catalog.runs[rep.RunID]; !ok || stored.Outcome != model.OutcomeDegraded {
		t.Fatalf("the catalog entry is %+v, want the finished report", w.catalog.runs)
	}
}
