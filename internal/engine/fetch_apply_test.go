package engine

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/discovery"
	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/source"
)

func TestApplyRefusalPaths(t *testing.T) {
	ctx := context.Background()

	w := newWorld(t)
	target := &fakeRestorer{}
	w.extraSources["target"] = target
	cfg := applyConfig(t, w, "target", nil)
	e := runOnce(t, w, cfg)
	runs, _ := e.StoredRuns(ctx, StoredRunsRequest{})
	runID := runs[0].RunID

	// ResolveFetch fails: the artifact is on no run.
	if _, err := e.Apply(ctx, ApplyRunRequest{
		Fetch: FetchRequest{Newest: true, Artifact: "nope"}, Target: "target", Mode: source.ApplySideBySide,
	}); err == nil {
		t.Fatal("Apply accepted an unresolvable fetch")
	}

	// The target cannot be described.
	w.extraSources["target"] = &fakeRestorer{describeErr: plugin.Transient(errors.New("describe failed"))}
	eB := w.newEngine(t, applyConfig(t, w, "target", nil))
	if _, err := eB.Apply(ctx, ApplyRunRequest{
		Fetch: FetchRequest{RunID: runID, Artifact: "wireguard"}, Target: "target", Mode: source.ApplySideBySide,
	}); err == nil || !strings.Contains(err.Error(), "describing the target") {
		t.Fatalf("a target that cannot be described was accepted: %v", err)
	}

	// The target has no restorer.
	w.extraSources["target"] = &fakeSource{}
	eP := w.newEngine(t, applyConfig(t, w, "target", nil))
	if _, err := eP.Apply(ctx, ApplyRunRequest{
		Fetch: FetchRequest{RunID: runID, Artifact: "wireguard"}, Target: "target", Mode: source.ApplySideBySide,
	}); err == nil || !strings.Contains(err.Error(), "no restorer") {
		t.Fatalf("a target without a restorer was accepted: %v", err)
	}

	// In place needs a prober.
	w.extraSources["target"] = &applyOnlySource{}
	eA := w.newEngine(t, applyConfig(t, w, "target", nil))
	if _, err := eA.Apply(ctx, ApplyRunRequest{
		Fetch: FetchRequest{RunID: runID, Artifact: "wireguard"}, Target: "target",
		Mode: source.ApplyInPlace, Confirm: "test://fake/source",
	}); err == nil || !strings.Contains(err.Error(), "Prober") {
		t.Fatalf("an in-place apply without a prober was accepted: %v", err)
	}
}

func TestApplySpoolVanished(t *testing.T) {
	w := newWorld(t)
	target := &fakeRestorer{}
	w.extraSources["target"] = target
	cfg := applyConfig(t, w, "target", nil)
	e := runOnce(t, w, cfg)
	runs, _ := e.StoredRuns(context.Background(), StoredRunsRequest{})
	spoolDir := e.restoreSpoolDir
	// The decode removes the spool file it wrote: the apply's own Open then fails.
	e.processors["zstd3"] = &covProcessor{decodeAfter: covRemoveSpoolFiles(spoolDir)}
	if _, err := e.Apply(context.Background(), ApplyRunRequest{
		Fetch: FetchRequest{RunID: runs[0].RunID, Artifact: "wireguard"}, Target: "target", Mode: source.ApplySideBySide,
	}); err == nil {
		t.Fatal("an apply whose spool file vanished succeeded")
	}
}

func TestApplyInPlaceSeekFails(t *testing.T) {
	w := newWorld(t)
	target := &probeCloseRestorer{}
	w.extraSources["target"] = target
	cfg := applyConfig(t, w, "target", nil)
	e := runOnce(t, w, cfg)
	runs, _ := e.StoredRuns(context.Background(), StoredRunsRequest{})
	if _, err := e.Apply(context.Background(), ApplyRunRequest{
		Fetch: FetchRequest{RunID: runs[0].RunID, Artifact: "wireguard"}, Target: "target",
		Mode: source.ApplyInPlace, Confirm: "test://fake/source",
	}); err == nil {
		t.Fatal("an in-place apply whose probe closed the reader succeeded")
	}
}

// TestApplyClosesTheFailedTarget: a discovered target that fails to close is
// reported, not swallowed.
func TestApplyClosesTheFailedTarget(t *testing.T) {
	w := newWorld(t)
	target := &errCloseRestorer{}
	w.extraSources["found"] = target
	cfg := applyConfig(t, w, "target", func(cfg map[string]any) {
		cfg["discoveries"] = map[string]any{"workloads": instance("fake")}
	})
	e1 := runOnce(t, w, cfg)
	runs, err := e1.StoredRuns(context.Background(), StoredRunsRequest{})
	if err != nil || len(runs) != 1 {
		t.Fatalf("stored runs %v, %v", runs, err)
	}
	w.discoveries["workloads"] = &fakeDiscovery{found: []discovery.Found{{
		Name: "found", Type: "fake", Config: []byte(`{}`),
		Artifact: model.Artifact{Group: "vpn", Name: "wireguard", Importance: model.ImportanceRequired, Sensitivity: model.SensitivityPlain},
		Pipeline: []string{"zstd3"},
	}}}
	e2 := w.newEngine(t, cfg)
	rep, err := e2.Apply(context.Background(), ApplyRunRequest{
		Fetch: FetchRequest{RunID: runs[0].RunID, Artifact: "wireguard"}, Target: "discovered", Mode: source.ApplySideBySide,
	})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Target == "" {
		t.Fatalf("apply report %+v", rep)
	}
}

// --- resolveTarget -----------------------------------------------------------

func TestResolveTargetErrors(t *testing.T) {
	ctx := context.Background()

	newDiscoveryEngine := func(t *testing.T, found []discovery.Found, discErr error) (*world, *Engine) {
		t.Helper()
		w := newWorld(t)
		cfg := withRestore(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}), "primary")
		cfg["discoveries"] = map[string]any{"workloads": instance("fake")}
		w.discoveries["workloads"] = &fakeDiscovery{found: found, err: discErr}
		return w, w.newEngine(t, cfg)
	}

	// The discovery itself fails.
	_, e := newDiscoveryEngine(t, nil, plugin.Permanent(errors.New("api refused")))
	if _, _, err := e.resolveTarget(ctx, "discovered", "wireguard", &closerScope{}); err == nil ||
		!strings.Contains(err.Error(), "discovery") {
		t.Fatalf("a failed discovery was accepted: %v", err)
	}

	// No discovered source holds the artifact.
	_, e2 := newDiscoveryEngine(t, []discovery.Found{{
		Name: "found", Type: "fake", Artifact: model.Artifact{Name: "other"},
	}}, nil)
	if _, _, err := e2.resolveTarget(ctx, "discovered", "wireguard", &closerScope{}); err == nil ||
		!strings.Contains(err.Error(), "no discovered source") {
		t.Fatalf("a non-matching discovery was accepted: %v", err)
	}

	// The discovered target carries a problem.
	_, e3 := newDiscoveryEngine(t, []discovery.Found{{
		Name: "found", Type: "fake", Artifact: model.Artifact{Name: "wireguard"}, Problem: "unavailable",
	}}, nil)
	if _, _, err := e3.resolveTarget(ctx, "discovered", "wireguard", &closerScope{}); err == nil ||
		!strings.Contains(err.Error(), "unavailable") {
		t.Fatalf("a problem target was accepted: %v", err)
	}

	// The discovered target's source type cannot be built.
	_, e4 := newDiscoveryEngine(t, []discovery.Found{{
		Name: "found", Type: "ghost", Artifact: model.Artifact{Name: "wireguard"},
	}}, nil)
	if _, _, err := e4.resolveTarget(ctx, "discovered", "wireguard", &closerScope{}); err == nil {
		t.Fatal("an unbuildable discovered target was accepted")
	}

	// The envelope holds the artifact name as a unit the engine cannot name.
	w5, e5 := newDiscoveryEngine(t, []discovery.Found{{
		Name: "found", Type: "fake", Config: []byte(`{}`), Artifact: model.Artifact{Name: "vpn"},
	}}, nil)
	w5.sources["found"] = &fakeSource{units: []source.Unit{{Name: "x/y"}}}
	if _, _, err := e5.resolveTarget(ctx, "discovered", "vpn/x/y", &closerScope{}); err == nil ||
		!strings.Contains(err.Error(), "unavailable") {
		t.Fatalf("a refused unit was accepted: %v", err)
	}
}

// --- Verify ------------------------------------------------------------------
