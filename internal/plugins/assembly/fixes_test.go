package assembly

// Iteration-1 fixes at the assembly boundary: C6 (the effective selection gates
// every construction and a build without the loader refuses) and M3 (invalid
// effective cleanup budgets refuse before runtime).

import (
	"context"
	"strings"
	"testing"
	"time"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/configuration"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/discovery"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/workflow/backup"
)

// TestC6_DisabledDeclaredSourceIsRefusedBeforeAnyBuild: a source the selection
// turned off is refused by its plugin ID; its factory never runs.
func TestC6_DisabledDeclaredSourceIsRefusedBeforeAnyBuild(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"alpha": storageInstance("fake", "keep_last")})
	cfg["plugins"] = map[string]any{
		"bundles": []any{}, "disabled": []any{"source/fake"}, "config": map[string]any{},
	}
	_, err := w.newAssemblyErr(t, cfg)
	if err == nil || !strings.Contains(err.Error(), "source/fake: plugin is disabled") {
		t.Fatalf("a disabled declared source = %v, want the refusal by plugin ID", err)
	}
	if len(w.Configs) != 0 {
		t.Fatalf("a disabled source's factory ran: %v", w.Configs)
	}
}

// TestC6_DisabledNestedRuleSourceIsRefused: a source a discovery rule names is
// built through the same gate, so the selection reaches nested rule sources
// too; the object fails without its factory ever running.
func TestC6_DisabledNestedRuleSourceIsRefused(t *testing.T) {
	w := newWorld(t)
	nestedBuilds := 0
	if err := instances.Register(w.Registry, source.Kind, instances.FactorySpec[map[string]jsontext.Value, source.Source]{
		Type: "nested", Example: []byte(`{}`),
		New: func(context.Context, map[string]jsontext.Value, instances.Deps) (source.Source, error) {
			nestedBuilds++
			return &fakeSource{}, nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	cfg := baseConfig(t, map[string]any{"alpha": storageInstance("fake", "keep_last")})
	cfg["discoveries"] = map[string]any{"workloads": instance("fake")}
	cfg["plugins"] = map[string]any{
		"bundles": []any{}, "disabled": []any{"source/nested"}, "config": map[string]any{},
	}
	e := w.newAssembly(t, cfg)
	w.Discoveries["workloads"].Found = []discovery.Found{{
		Name:   "child",
		Type:   "nested",
		Config: jsontext.Value(`{}`),
		Artifact: model.Artifact{Group: "databases", Name: "child",
			Importance: model.ImportanceBestEffort, Sensitivity: model.SensitivityPlain},
	}}
	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	var child *model.ArtifactResult
	for i := range rep.Artifacts {
		if rep.Artifacts[i].Artifact.Name == "child" {
			child = &rep.Artifacts[i]
		}
	}
	if child == nil || child.Outcome != model.ArtifactFailed || !strings.Contains(child.Error, "source/nested: plugin is disabled") {
		t.Fatalf("the disabled nested rule source: %+v", child)
	}
	if nestedBuilds != 0 {
		t.Fatalf("the disabled nested source was built %d times", nestedBuilds)
	}
}

// TestC6_ACompiledLoaderIsRequired: a construction without the loader has no
// selection to gate on, so it refuses instead of treating everything as active.
func TestC6_ACompiledLoaderIsRequired(t *testing.T) {
	w := newWorld(t)
	_, err := New(context.Background(), jsontext.Value(`{}`), nil,
		Deps{Factories: w.Registry, Log: testLogger()})
	if err == nil || !strings.Contains(err.Error(), "compiled loader registry is required") {
		t.Fatalf("a construction without the loader = %v, want the refusal", err)
	}
}

// TestM3_InvalidEffectiveBudgetsAreRefused: an effective lifecycle block whose
// grace cannot be derived refuses the whole assembly before any operation; the
// probe values encode the reviewer's negative-join counterexample.
func TestM3_InvalidEffectiveBudgetsAreRefused(t *testing.T) {
	cases := []struct {
		name  string
		block map[string]any
		want  string
	}{
		{"negative join", map[string]any{"join": "-10m", "abort_total": "2m", "close": "30s", "margin": "30s"}, "join"},
		{"zero abort", map[string]any{"join": "1m", "abort_total": "0s", "close": "30s", "margin": "30s"}, "abort_total"},
		{"zero close", map[string]any{"join": "1m", "abort_total": "2m", "close": "0s", "margin": "30s"}, "close"},
		{"negative margin", map[string]any{"join": "1m", "abort_total": "2m", "close": "30s", "margin": "-1s"}, "margin"},
		{"overflow", map[string]any{"join": "1m", "abort_total": "2m", "close": "30s", "margin": "2562047h47m16.854775807s"}, "overflows"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := newWorld(t)
			cfg := baseConfig(t, map[string]any{"alpha": storageInstance("fake", "keep_last")})
			cfg["plugins"] = map[string]any{
				"bundles": []any{}, "disabled": []any{},
				"config": map[string]any{"lifecycle": tc.block},
			}
			_, err := w.newAssemblyErr(t, cfg)
			if err == nil || !strings.Contains(err.Error(), "/plugins/config/lifecycle") || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("invalid effective budgets = %v, want the %s refusal", err, tc.want)
			}
		})
	}
}

// TestC6_ASelectionProblemRefusesTheAssembly: a selection the resolver cannot
// apply refuses the whole assembly before the first build.
func TestC6_ASelectionProblemRefusesTheAssembly(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"alpha": storageInstance("fake", "keep_last")})
	cfg["plugins"] = map[string]any{
		"bundles": []any{}, "disabled": []any{"source/nope"}, "config": map[string]any{},
	}
	_, err := w.newAssemblyErr(t, cfg)
	if err == nil || !strings.Contains(err.Error(), "nope") {
		t.Fatalf("an unknown disabled id = %v, want the selection refusal", err)
	}
}

// TestC6_DisabledDeclaredStorageIsRefusedBeforeAnyBuild: the storage build path
// gates on the loader too; a disabled backend is refused by ID, never built.
func TestC6_DisabledDeclaredStorageIsRefusedBeforeAnyBuild(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"alpha": storageInstance("fake", "keep_last")})
	cfg["plugins"] = map[string]any{
		"bundles": []any{}, "disabled": []any{"storage/fake"}, "config": map[string]any{},
	}
	_, err := w.newAssemblyErr(t, cfg)
	if err == nil || !strings.Contains(err.Error(), "/storages/alpha: storage/fake: plugin is disabled") {
		t.Fatalf("a disabled declared storage = %v, want the refusal by plugin ID", err)
	}
	if len(w.Storages) != 0 {
		t.Fatalf("a disabled storage's factory ran: %v", w.Storages)
	}
}

// TestM3_ValidEffectiveBudgetsBuild: a valid block is accepted and the grace is
// derived with the run's finish allowance — no zero bound is invented.
func TestM3_ValidEffectiveBudgetsBuild(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"alpha": storageInstance("fake", "keep_last")})
	cfg["plugins"] = map[string]any{
		"bundles": []any{}, "disabled": []any{},
		"config": map[string]any{"lifecycle": map[string]any{
			"join": "1m", "abort_total": "2m", "close": "30s", "margin": "0s",
		}},
	}
	e := w.newAssembly(t, cfg)
	if e.lifecycle.Join.Std() != time.Minute || e.lifecycle.AbortTotal.Std() != 2*time.Minute {
		t.Fatalf("the effective lifecycle = %+v", e.lifecycle)
	}
}

// TestM3_ReleaseOverflowRefusesTheAssembly: the lifecycle fields alone fit, and
// the run's finish allowance (the release budget) pushes the checked sum past
// time.Duration — the assembly refuses instead of wrapping to a small bound.
func TestM3_ReleaseOverflowRefusesTheAssembly(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"alpha": storageInstance("fake", "keep_last")})
	cfg["run"].(map[string]any)["finish_allowance"] = "2562047h47m16.854775807s"
	cfg["plugins"] = map[string]any{
		"bundles": []any{}, "disabled": []any{},
		"config": map[string]any{"lifecycle": map[string]any{
			"join": "1m", "abort_total": "2m", "close": "30s", "margin": "0s",
		}},
	}
	_, err := w.newAssemblyErr(t, cfg)
	if err == nil || !strings.Contains(err.Error(), "/plugins/config/lifecycle") || !strings.Contains(err.Error(), "overflows") {
		t.Fatalf("a release overflow = %v, want the grace refusal", err)
	}
}

// TestR2_M13_SelectionRefusalKeepsPureReferenceProblems: a refused selection
// still reports the reference problems judgeable without construction, and no
// plugin is built for diagnostics.
func TestR2_M13_SelectionRefusalKeepsPureReferenceProblems(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"alpha": storageInstance("fake", "keep_last")})
	cfg["plugins"] = map[string]any{
		"bundles": []any{}, "disabled": []any{"source/ghost"}, "config": map[string]any{},
	}
	cfg["sources"].(map[string]any)["vpn"].(map[string]any)["pipeline"] = []any{"ghost_processor"}
	_, err := w.newAssemblyErr(t, cfg)
	if err == nil {
		t.Fatal("a refused selection passed construction")
	}
	msg := err.Error()
	for _, want := range []string{
		`unknown plugin "source/ghost"`,
		`/sources/vpn/pipeline/0: unknown processor instance "ghost_processor"`,
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("the aggregated answer does not carry %q:\n%s", want, msg)
		}
	}
	if len(w.Configs) != 0 || len(w.Processors) != 0 {
		t.Fatalf("a refused selection built a plugin: sources=%v processors=%v", w.Configs, w.Processors)
	}
}

// TestR2_M13_PureChecksCoverEverySection: the no-construction reference pass
// covers every declared section, its pipeline names and retention types, still
// without building a plugin.
func TestR2_M13_PureChecksCoverEverySection(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"alpha": storageInstance("nosuch", "nosuch")})
	cfg["plugins"] = map[string]any{
		"bundles": []any{}, "disabled": []any{"source/ghost"}, "config": map[string]any{},
	}
	cfg["processors"] = map[string]any{"p": instance("nosuch")}
	cfg["sources"] = map[string]any{"s": sourceInstance("nosuch", "g", "n", []string{"ghost"}, "required", "plain")}
	cfg["stage"] = map[string]any{
		"dir": t.TempDir(), "degraded_exit_code": 3, "failed_exit_code": 7,
		"sources": map[string]any{"ss": sourceInstance("nosuch", "g", "n", []string{"ghost2"}, "required", "plain")},
	}
	cfg["discoveries"] = map[string]any{"d": instance("nosuch")}
	cfg["notifiers"] = map[string]any{"n": instance("nosuch")}
	cfg["catalog"] = instance("nosuch")

	_, err := w.newAssemblyErr(t, cfg)
	if err == nil {
		t.Fatal("a refused selection passed construction")
	}
	for _, want := range []string{
		"unknown plugin",
		"/processors/p: unknown processor type",
		"/sources/s: unknown source type",
		`/sources/s/pipeline/0: unknown processor instance "ghost"`,
		"/stage.sources/ss: unknown source type",
		`/stage.sources/ss/pipeline/0: unknown processor instance "ghost2"`,
		"/discoveries/d: unknown discovery type",
		"/notifiers/n: unknown notifier type",
		"/storages/alpha: unknown storage type",
		"/storages/alpha/retention/scheduled: unknown retention type",
		"/catalogs/catalog: unknown catalog type",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the pure answer does not carry %q:\n%s", want, err)
		}
	}
	if len(w.Configs) != 0 || len(w.Processors) != 0 || len(w.Storages) != 0 {
		t.Fatalf("a refused selection built a plugin: %v %v %v", w.Configs, w.Processors, w.Storages)
	}
}

// TestR3_M11_MergeProblemKeepsDecodeSelectionAndReferences: a merge problem (an
// unknown settings block), a decode problem (an unknown root member), a refused
// selection and a dangling pipeline reference all arrive in one answer, and no
// factory runs.
func TestR3_M11_MergeProblemKeepsDecodeSelectionAndReferences(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"alpha": storageInstance("fake", "keep_last")})
	cfg["oops"] = true
	cfg["plugins"] = map[string]any{
		"bundles": []any{}, "disabled": []any{"source/ghost"},
		"config": map[string]any{"storage/nope": map[string]any{"page_size": 1}},
	}
	cfg["sources"].(map[string]any)["vpn"].(map[string]any)["pipeline"] = []any{"ghost_processor"}
	raw, err := jsonMarshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defs, err := configuration.LoadDefaults(w.Registry, []jsontext.Value{
		moduleDefaultsDoc(t, w.Registry, "storage/fake", `{"page_size": 100}`),
	})
	if err != nil {
		t.Fatalf("LoadDefaults: %v", err)
	}
	_, err = New(context.Background(), raw, defs, Deps{Factories: w.Registry, Loader: w.Loader, Log: testLogger()})
	if err == nil {
		t.Fatal("an invalid configuration was accepted")
	}
	for _, want := range []string{
		"storage~1nope",
		`unknown object member name "oops"`,
		`unknown plugin "source/ghost"`,
		`/sources/vpn/pipeline/0: unknown processor instance "ghost_processor"`,
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the aggregated answer does not carry %q:\n%s", want, err)
		}
	}
	if len(w.Configs) != 0 || len(w.Processors) != 0 || len(w.Storages) != 0 {
		t.Fatalf("a refused configuration built a plugin: %v %v %v", w.Configs, w.Processors, w.Storages)
	}
}

// TestR4_M11_TypeErrorKeepsIndependentDiagnostics: one section with the wrong
// JSON kind must not hide the merge, selection and reference problems the other
// readable sections still expose; nothing is built.
func TestR4_M11_TypeErrorKeepsIndependentDiagnostics(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"alpha": storageInstance("fake", "keep_last")})
	cfg["catalog"] = 7
	cfg["plugins"] = map[string]any{
		"bundles": []any{}, "disabled": []any{"source/ghost"},
		"config": map[string]any{"storage/nope": map[string]any{"page_size": 1}},
	}
	cfg["sources"].(map[string]any)["vpn"].(map[string]any)["pipeline"] = []any{"ghost_processor"}
	raw, err := jsonMarshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defs, err := configuration.LoadDefaults(w.Registry, []jsontext.Value{
		moduleDefaultsDoc(t, w.Registry, "storage/fake", `{"page_size": 100}`),
	})
	if err != nil {
		t.Fatalf("LoadDefaults: %v", err)
	}
	_, err = New(context.Background(), raw, defs, Deps{Factories: w.Registry, Loader: w.Loader, Log: testLogger()})
	if err == nil {
		t.Fatal("a malformed configuration was accepted")
	}
	for _, want := range []string{
		"storage~1nope",
		`unknown plugin "source/ghost"`,
		`/sources/vpn/pipeline/0: unknown processor instance "ghost_processor"`,
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the aggregated answer does not carry %q:\n%s", want, err)
		}
	}
	if len(w.Configs) != 0 || len(w.Processors) != 0 || len(w.Storages) != 0 {
		t.Fatalf("a refused configuration built a plugin: %v %v %v", w.Configs, w.Processors, w.Storages)
	}
}

// TestR4_M11_NonObjectDocumentStillRefused: a document that is not an object has
// no readable sections; the decode error alone refuses it.
func TestR4_M11_NonObjectDocumentStillRefused(t *testing.T) {
	w := newWorld(t)
	_, err := New(context.Background(), jsontext.Value(`7`), nil,
		Deps{Factories: w.Registry, Loader: w.Loader, Log: testLogger()})
	if err == nil || !strings.Contains(err.Error(), "config:") {
		t.Fatalf("a non-object document = %v, want the decode refusal", err)
	}
}
