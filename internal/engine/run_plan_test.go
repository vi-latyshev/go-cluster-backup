package engine

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"strings"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/discovery"
	"github.com/vi-latyshev/go-cluster-backup/internal/model"
)

// TestRunRefusals covers the four configurations a run refuses before it reads
// a plugin: no run section, an unsurveyable prefix, a storage without a
// scheduled retention policy, and a storage without a byte cap.
func TestRunRefusals(t *testing.T) {
	t.Run("no run section", func(t *testing.T) {
		w := newWorld(t)
		cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
		delete(cfg, "run")
		e := w.newEngine(t, cfg)
		_, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
		if err == nil || !strings.Contains(err.Error(), "needs the run section") {
			t.Fatalf("err = %v, want the missing run section refusal", err)
		}
	})

	t.Run("unsurveyable run prefix", func(t *testing.T) {
		w := newWorld(t)
		cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
		cfg["layout"].(map[string]any)["run_prefix"] = "runs/{{ .Artifact.Name }}"
		e := w.newEngine(t, cfg)
		_, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
		if err == nil || !strings.Contains(err.Error(), "layout.run_prefix") {
			t.Fatalf("err = %v, want the run_prefix refusal", err)
		}
	})

	t.Run("storage without scheduled retention", func(t *testing.T) {
		w := newWorld(t)
		cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
		cfg["storages"].(map[string]any)["primary"].(map[string]any)["retention"] = map[string]any{}
		e := w.newEngine(t, cfg)
		_, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
		if err == nil || !strings.Contains(err.Error(), "retention/scheduled is missing") {
			t.Fatalf("err = %v, want the missing scheduled retention refusal", err)
		}
	})

	t.Run("storage without a byte cap", func(t *testing.T) {
		w := newWorld(t)
		cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
		cfg["storages"].(map[string]any)["primary"].(map[string]any)["max_total_bytes"] = int64(0)
		e := w.newEngine(t, cfg)
		_, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
		if err == nil || !strings.Contains(err.Error(), "max_total_bytes is missing") {
			t.Fatalf("err = %v, want the missing cap refusal", err)
		}
	})
}

// TestRunClosesAFailingDiscoveredSource: the scoped close reports its failure on
// the log, and the run still ends with a report.
func TestRunClosesAFailingDiscoveredSource(t *testing.T) {
	w := newWorld(t)
	src := &failingCloseSource{closeErr: errors.New("close blew up")}
	src.data = []byte("payload")
	w.extraSources["badclose"] = src
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["discoveries"] = map[string]any{"workloads": instance("fake")}
	e := serviceCheck(t, w, cfg)
	w.discoveries["workloads"].found = []discovery.Found{foundSource("badclose", "badclose", model.ImportanceRequired)}

	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatalf("a failing source close must not fail the run: %v", err)
	}
	if rep.Outcome != model.OutcomeDelivered {
		t.Fatalf("outcome %q", rep.Outcome)
	}
	if got := src.closeCount(); got != 1 {
		t.Fatalf("the discovered source was closed %d times, want exactly 1", got)
	}
}

// TestRunWithAFailedCatalogCheck: the catalog's own Check fails, which is scoped
// to the catalog — the run proceeds and its entry is still saved.
func TestRunWithAFailedCatalogCheck(t *testing.T) {
	w := newWorld(t)
	w.catalog = &fakeCatalog{checkErr: errors.New("catalog unreachable")}
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["catalog"] = instance("fake")
	e := w.newEngine(t, cfg)

	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeDelivered {
		t.Fatalf("outcome %q: a failed catalog check must not fail the run", rep.Outcome)
	}
	if _, ok := w.catalog.runs[rep.RunID]; !ok {
		t.Fatalf("the run did not save its entry after a failed catalog check: %v", w.catalog.runs)
	}
}

// --- collectArtifacts: the discovered-object refusals ------------------------

// TestCollectArtifactRefusals drives every refusal a discovered object can carry:
// a missing envelope, a bad enum, an unknown pipeline, an unbuildable source, a
// declared instance whose check failed, and a failed processor's check.
func TestCollectArtifactRefusals(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["sources"] = map[string]any{
		"vpn": sourceInstance("fake", "vpn", "wireguard", []string{"zstd3"}, "required", "plain"),
		"bad": sourceInstance("fake", "badgroup", "badart", []string{"zstd3"}, "required", "plain"),
	}
	w.sources["bad"] = &fakeSource{checkErr: errors.New("source check boom")}
	w.processors["zstd3"] = &fakeProcessor{checkErr: errors.New("processor check boom")}
	cfg["discoveries"] = map[string]any{"workloads": instance("fake")}
	e := serviceCheck(t, w, cfg)
	w.discoveries["workloads"].found = []discovery.Found{
		{Name: "f1", Type: "fake", Artifact: model.Artifact{}},
		{Name: "f2", Type: "fake", Artifact: model.Artifact{Group: "g", Name: "f2", Sensitivity: "weird", Importance: model.ImportanceRequired}},
		{Name: "f3", Type: "fake", Artifact: model.Artifact{Group: "g", Name: "f3", Sensitivity: model.SensitivityPlain, Importance: "weird"}},
		{Name: "f4", Type: "fake", Pipeline: []string{"nope"}, Artifact: model.Artifact{Group: "g", Name: "f4", Sensitivity: model.SensitivityPlain, Importance: model.ImportanceRequired}},
		{Name: "f5", Type: "unknown-type", Artifact: model.Artifact{Group: "g", Name: "f5", Sensitivity: model.SensitivityPlain, Importance: model.ImportanceRequired}},
		{Name: "bad", Type: "fake", Artifact: model.Artifact{Group: "g", Name: "f6", Sensitivity: model.SensitivityPlain, Importance: model.ImportanceRequired}},
		{Name: "f7src", Type: "fake", Pipeline: []string{"zstd3"}, Artifact: model.Artifact{Group: "g", Name: "f7", Sensitivity: model.SensitivityPlain, Importance: model.ImportanceRequired}},
	}

	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	byName := map[string]string{}
	for _, a := range rep.Artifacts {
		if a.Outcome != model.ArtifactFailed {
			t.Fatalf("artifact %q: outcome %q, want failed", a.Artifact.Name, a.Outcome)
		}
		byName[a.Artifact.Name] = a.Error
	}
	want := []struct{ substr, why string }{
		{"returned an artifact without group/name", "an empty envelope"},
		{`has sensitivity "weird"`, "a bad sensitivity"},
		{`has importance "weird"`, "a bad importance"},
		{"unknown processor instance", "a rule's unknown pipeline"},
		{"source check: source check boom", "a declared source whose check failed"},
		{"processor zstd3 check: processor check boom", "a processor whose check failed"},
	}
	all := strings.Join(func() []string {
		var errs []string
		for _, e := range byName {
			errs = append(errs, e)
		}
		return errs
	}(), "\n")
	for _, wnt := range want {
		if !strings.Contains(all, wnt.substr) {
			t.Fatalf("%s: no artifact carried %q\n%s", wnt.why, wnt.substr, all)
		}
	}
	if byName["f5"] == "" {
		t.Fatalf("the unbuildable discovered source was not failed: %v", byName)
	}
	if rep.Outcome != model.OutcomeFailed {
		t.Fatalf("outcome %q", rep.Outcome)
	}
}

// --- discovered sources: no Check, recorded chain, chain failures ------------

// TestDiscoveredSourceWithoutCheckOrPipeline: a discovered source that implements
// neither Check nor Enumerator, but does carry a recorded chain, runs through the
// chain path.
func TestDiscoveredSourceWithoutCheckOrPipeline(t *testing.T) {
	w := newWorld(t)
	w.extraSources["csrc"] = &chainSource{data: []byte("sealed"), ext: ".tar.zst",
		chain: []model.ProcessorStep{{Instance: "zstd", Type: "zstd", Ext: ".zst"}}}
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["discoveries"] = map[string]any{"workloads": instance("fake")}
	e := serviceCheck(t, w, cfg)
	w.discoveries["workloads"].found = []discovery.Found{{
		Name: "csrc", Type: "fake", Config: jsontext.Value(`{}`),
		Artifact: model.Artifact{Group: "databases", Name: "csrc",
			Importance: model.ImportanceRequired, Sensitivity: model.SensitivityPlain},
	}}

	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	got := artifactByName(rep, "csrc")
	if got.Outcome != model.ArtifactOK {
		t.Fatalf("artifact %+v", got)
	}
	if len(got.Chain) != 1 || got.Chain[0].Ext != ".zst" {
		t.Fatalf("the recorded chain was not carried into the manifest: %+v", got.Chain)
	}
	if got.Artifact.Ext != ".tar.zst" {
		t.Fatalf("artifact ext %q", got.Artifact.Ext)
	}
}

// TestRecordedChainUnsupportedIsNoChain: a source that answers ErrUnsupported is
// an ordinary single-artifact source with no chain to record.
func TestRecordedChainUnsupportedIsNoChain(t *testing.T) {
	w := newWorld(t)
	src := &unsupportedChainSource{fakeSource{data: []byte("plain")}}
	w.extraSources["vpn"] = src
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["sources"] = map[string]any{
		"vpn": sourceInstance("fake", "vpn", "wireguard", []string{}, "required", "plain"),
	}
	e := w.newEngine(t, cfg)

	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeDelivered {
		t.Fatalf("outcome %q, artifacts %+v", rep.Outcome, rep.Artifacts)
	}
	if got := artifactByName(rep, "wireguard"); got.Outcome != model.ArtifactOK || len(got.Chain) != 0 {
		t.Fatalf("artifact %+v, want ok with no chain", got)
	}
}

// TestRecordedChainFailureFailsTheArtifact: a chain that cannot be read is the
// artifact's own failure.
func TestRecordedChainFailureFailsTheArtifact(t *testing.T) {
	w := newWorld(t)
	w.extraSources["vpn"] = &failingChainSource{fakeSource{data: []byte("plain")}}
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["sources"] = map[string]any{
		"vpn": sourceInstance("fake", "vpn", "wireguard", []string{}, "required", "plain"),
	}
	e := w.newEngine(t, cfg)

	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	got := artifactByName(rep, "wireguard")
	if got.Outcome != model.ArtifactFailed || !strings.Contains(got.Error, "recorded chain") {
		t.Fatalf("artifact %+v, want the recorded-chain failure", got)
	}
	if len(w.storage(t, "primary").Keys()) != 0 {
		t.Fatal("a failed artifact left objects")
	}
}

// TestDryRunResolveFailureFailsTheArtifact: a plan whose traits cannot be
// resolved is reported as a failed artifact without producing anything.
func TestDryRunResolveFailureFailsTheArtifact(t *testing.T) {
	w := newWorld(t)
	w.sources["vpn"] = &fakeSource{data: []byte("x"), describeErr: errors.New("no describe")}
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	e := w.newEngine(t, cfg)

	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled, DryRun: true})
	if err != nil {
		t.Fatal(err)
	}
	got := artifactByName(rep, "wireguard")
	if got.Outcome != model.ArtifactFailed || !strings.Contains(got.Error, "no describe") {
		t.Fatalf("artifact %+v", got)
	}
	if rep.Outcome != model.OutcomeFailed {
		t.Fatalf("outcome %q", rep.Outcome)
	}
}

// TestDryRunResolveSuccessCarriesTheEnvelope: the resolving path of a dry run
// fills the artifact with the source's extension.
func TestDryRunResolveSuccessCarriesTheEnvelope(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	e := w.newEngine(t, cfg)

	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled, DryRun: true})
	if err != nil {
		t.Fatal(err)
	}
	got := artifactByName(rep, "wireguard")
	if got.Outcome != model.ArtifactOK || got.Artifact.Ext != ".dat.zst" {
		t.Fatalf("artifact %+v", got)
	}
	if rep.Outcome != model.OutcomeDelivered {
		t.Fatalf("outcome %q", rep.Outcome)
	}
}
