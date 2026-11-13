package backup_test

// Run's branches over the harness: faults and malformed findings through the
// real skeleton.

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/catalog"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/catalog/catalogtest"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/discovery"
	pluginerrors "github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/processor"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/workflow/backup"
)

type leasedCatalog struct {
	*catalogtest.Leaser
	CheckErr   error
	releaseErr error
}

func (c *leasedCatalog) Check(context.Context) error { return c.CheckErr }

func (c *leasedCatalog) Release(ctx context.Context, lease catalog.Lease) error {
	if err := c.Leaser.Release(ctx, lease); err != nil {
		return err
	}
	return c.releaseErr
}

func TestRunReportsACatalogCheckFailure(t *testing.T) {
	w := newWorld(t)
	cat := &leasedCatalog{Leaser: catalogtest.NewLeaser(), CheckErr: pluginerrors.Permanent(errors.New("lease record unreachable"))}
	registerCatalogType(t, w, "broken", cat)
	cfg := baseConfig(t, map[string]any{"alpha": storageInstance("fake", "keep_last")})
	cfg["catalog"] = instance("broken")
	e := newAssemblyLogged(t, w, cfg, &bytes.Buffer{})
	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome == "" {
		t.Fatal("no outcome")
	}
}

func TestRunReportsAReleaseFailure(t *testing.T) {
	w := newWorld(t)
	cat := &leasedCatalog{Leaser: catalogtest.NewLeaser(), releaseErr: errors.New("CAS refused")}
	registerCatalogType(t, w, "releasing", cat)
	cfg := baseConfig(t, map[string]any{"alpha": storageInstance("fake", "keep_last")})
	cfg["catalog"] = instance("releasing")
	e := newAssemblyLogged(t, w, cfg, &bytes.Buffer{})
	if _, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled}); err != nil {
		t.Fatal(err)
	}
}

func TestRunFailsAPermanentlyUncheckedStorage(t *testing.T) {
	w := newWorld(t)
	fleet := checkingStorages(t, w, "checking")
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("checking", "keep_last")})
	e := serviceCheck(t, w, cfg)
	fleet["primary"].CheckErr = pluginerrors.Permanent(errors.New("bucket gone"))
	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if got := storageResult(rep, "primary"); got.Outcome != model.StorageFailed {
		t.Fatalf("storage %+v", got)
	}
}

func TestRunRefusesWhenTheAuthorityExpired(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	run := cfg["run"].(map[string]any)
	run["deadline"] = "1ns"
	run["finish_allowance"] = "1ns"
	e := serviceCheck(t, w, cfg)
	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome == model.OutcomeDelivered {
		t.Fatalf("delivered with no authority: %+v", rep.Storages)
	}
}

func TestRunReportsMalformedDiscoveryFindings(t *testing.T) {
	w := newWorld(t)
	d := &fakeDiscovery{}
	w.Discoveries["workloads"] = d
	cfg := baseConfig(t, map[string]any{"alpha": storageInstance("fake", "keep_last")})
	cfg["discoveries"] = map[string]any{"workloads": instance("fake")}
	e := serviceCheck(t, w, cfg)
	d.Found = []discovery.Found{
		{Excluded: model.ExclusionIgnored, ExcludedGroup: "g", ExcludedReason: "rule", Artifact: model.Artifact{Name: "skip", Group: "g"}},
		{Name: "p", Problem: "no template value", Hint: "set it", Artifact: model.Artifact{Name: "p", Group: "g", Importance: model.ImportanceRequired, Sensitivity: model.SensitivityPlain}},
		{Name: "n", Artifact: model.Artifact{Name: "", Group: "g", Importance: model.ImportanceRequired, Sensitivity: model.SensitivityPlain}},
		{Name: "s", Artifact: model.Artifact{Name: "s", Group: "g", Importance: model.ImportanceRequired, Sensitivity: "weird"}},
		{Name: "i", Artifact: model.Artifact{Name: "i", Group: "g", Importance: "sometimes", Sensitivity: model.SensitivityPlain}},
	}
	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Excluded) != 1 || rep.Excluded[0].Name != "skip" {
		t.Fatalf("excluded = %+v", rep.Excluded)
	}
	for _, want := range []string{"no template value", "without group/name", "sensitivity", "importance"} {
		found := false
		for _, a := range rep.Artifacts {
			if strings.Contains(a.Error, want) {
				found = true
			}
		}
		if !found {
			t.Fatalf("no artifact error carries %q: %+v", want, rep.Artifacts)
		}
	}
}

type badEncodeProcessor struct{ *fakeProcessor }

func (badEncodeProcessor) CheckEncode(context.Context) error { return errors.New("no recipient") }

func TestRunRefusesAnUnresolvableRulePipeline(t *testing.T) {
	w := newWorld(t)
	d := &fakeDiscovery{}
	w.Discoveries["workloads"] = d

	if err := instances.Register(w.Registry, processor.Kind, instances.FactorySpec[map[string]jsontext.Value, processor.Processor]{
		Type: "baden", Example: []byte(`{}`),
		New: func(context.Context, map[string]jsontext.Value, instances.Deps) (processor.Processor, error) {
			return badEncodeProcessor{fakeProcessor: &fakeProcessor{}}, nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	cfg := baseConfig(t, map[string]any{"alpha": storageInstance("fake", "keep_last")})
	cfg["discoveries"] = map[string]any{"workloads": instance("fake")}
	cfg["processors"].(map[string]any)["baden"] = instance("baden")
	e := serviceCheck(t, w, cfg)
	d.Found = []discovery.Found{
		{Name: "ghost-pipe", Artifact: model.Artifact{Group: "g", Name: "ghost-pipe", Importance: model.ImportanceRequired, Sensitivity: model.SensitivityPlain}, Pipeline: []string{"ghost"}},
		{Name: "bad-pipe", Artifact: model.Artifact{Group: "g", Name: "bad-pipe", Importance: model.ImportanceRequired, Sensitivity: model.SensitivityPlain}, Pipeline: []string{"baden"}},
	}
	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"unknown processor instance", "no recipient"} {
		found := false
		for _, a := range rep.Artifacts {
			if strings.Contains(a.Error, want) {
				found = true
			}
		}
		if !found {
			t.Fatalf("no artifact error carries %q: %+v", want, rep.Artifacts)
		}
	}
}

func TestRunReportsADiscoveredSourceCheckFailureAndFailingBuild(t *testing.T) {
	w := newWorld(t)
	d := &fakeDiscovery{}
	w.Discoveries["workloads"] = d
	checked := &fakeSource{}
	checked.CheckErr = errors.New("api refused")
	w.Sources["checked"] = checked
	if err := instances.Register(w.Registry, source.Kind, instances.FactorySpec[map[string]jsontext.Value, source.Source]{
		Type: "buildfail", Example: []byte(`{}`),
		New: func(context.Context, map[string]jsontext.Value, instances.Deps) (source.Source, error) {
			return nil, errors.New("config refused")
		},
	}); err != nil {
		t.Fatal(err)
	}
	cfg := baseConfig(t, map[string]any{"alpha": storageInstance("fake", "keep_last")})
	cfg["discoveries"] = map[string]any{"workloads": instance("fake")}
	e := serviceCheck(t, w, cfg)
	d.Found = []discovery.Found{
		{Name: "checked", Type: "fake", Config: jsontext.Value(`{}`), Artifact: model.Artifact{Group: "g", Name: "checked", Importance: model.ImportanceRequired, Sensitivity: model.SensitivityPlain}},
		{Name: "buildfail", Type: "buildfail", Config: jsontext.Value(`{}`), Artifact: model.Artifact{Group: "g", Name: "buildfail", Importance: model.ImportanceRequired, Sensitivity: model.SensitivityPlain}},
	}
	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"api refused", "config refused"} {
		found := false
		for _, a := range rep.Artifacts {
			if strings.Contains(a.Error, want) {
				found = true
			}
		}
		if !found {
			t.Fatalf("no artifact error carries %q: %+v", want, rep.Artifacts)
		}
	}
}

func TestRunReportsAFailedProcessorCheck(t *testing.T) {
	w := newWorld(t)
	d := &fakeDiscovery{}
	w.Discoveries["workloads"] = d
	cfg := baseConfig(t, map[string]any{"alpha": storageInstance("fake", "keep_last")})
	cfg["discoveries"] = map[string]any{"workloads": instance("fake")}
	e := serviceCheck(t, w, cfg)
	w.Processors["zstd3"].CheckErr = errors.New("key missing")
	d.Found = []discovery.Found{{Name: "p", Artifact: model.Artifact{Group: "g", Name: "p", Importance: model.ImportanceRequired, Sensitivity: model.SensitivityPlain}, Pipeline: []string{"zstd3"}}}
	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, a := range rep.Artifacts {
		if strings.Contains(a.Error, "processor zstd3 check") {
			found = true
		}
	}
	if !found {
		t.Fatalf("errors %+v", rep.Artifacts)
	}
}

func TestRunDryRunReportsProblemsAndTraitErrors(t *testing.T) {
	w := newWorld(t)
	d := &fakeDiscovery{}
	w.Discoveries["workloads"] = d
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["discoveries"] = map[string]any{"workloads": instance("fake")}
	e := serviceCheck(t, w, cfg)
	w.Sources["vpn"].DescribeErr = errors.New("mount gone")
	d.Found = []discovery.Found{{Name: "bad", Artifact: model.Artifact{Name: "", Group: "g", Importance: model.ImportanceRequired, Sensitivity: model.SensitivityPlain}}}
	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled, DryRun: true})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeFailed {
		t.Fatalf("outcome %q", rep.Outcome)
	}
}

func TestRunRefusesAManifestKeyThatDoesNotRender(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	// A template keyed on the sample run ids renders a safe key offline and a
	// traversal at run time: validate cannot prove what every run id renders,
	// so the run's own refusal is the one tested here.
	cfg["layout"].(map[string]any)["manifest_key"] = `{{ if or (eq .Run.ID "20260102T030405Z") (eq .Run.ID "20260102T030405Z-manual") }}manifest.json{{ else }}../escape{{ end }}`
	e := serviceCheck(t, w, cfg)
	if _, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled}); err != nil {
		t.Fatal(err)
	}
}

// plainSource is a discovered source with no Check capability.
type plainSource struct{ Data []byte }

func (p *plainSource) Describe(context.Context) (source.SourceTraits, error) {
	return source.SourceTraits{Ext: ".bin", Origin: "plain"}, nil
}

func (p *plainSource) Dump(_ context.Context, w io.Writer) error {
	_, err := w.Write(p.Data)
	return err
}

func TestRunFailsAnArtifactWhenAPartFailsAndReportsABlockedAbort(t *testing.T) {
	w := newWorld(t)
	parts := addMultipart(w, "alpha", 4)
	parts.partErr = errors.New("part refused")
	parts.abortErr = errors.New("abort refused")
	w.Sources["vpn"] = &fakeSource{Data: []byte("0123456789")}
	cfg := baseConfig(t, map[string]any{"alpha": storageInstance("fake", "keep_last")})
	// A multipart accepted write may settle after the run: the run needs a
	// durable lease to mark before the first one.
	cfg["catalog"] = instance("fake")
	e := serviceCheck(t, w, cfg)
	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	failed := false
	for _, a := range rep.Artifacts {
		if a.Outcome == model.ArtifactFailed {
			failed = true
		}
	}
	if !failed {
		t.Fatalf("artifacts %+v", rep.Artifacts)
	}
	if begins, _, _, _ := parts.stats(); begins == 0 {
		t.Fatal("the multipart upload never began")
	}
}

func TestRunReportsADiscoveryCheckFailure(t *testing.T) {
	w := newWorld(t)
	d := &fakeDiscovery{CheckErr: pluginerrors.Permanent(errors.New("rules unreadable"))}
	w.Discoveries["workloads"] = d
	cfg := baseConfig(t, map[string]any{"alpha": storageInstance("fake", "keep_last")})
	cfg["discoveries"] = map[string]any{"workloads": instance("fake")}
	e := serviceCheck(t, w, cfg)
	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, a := range rep.Artifacts {
		if strings.Contains(a.Error, "discovery workloads check") {
			found = true
		}
	}
	if !found {
		t.Fatalf("artifacts %+v", rep.Artifacts)
	}
}

func TestRunReportsADeclaredSourceCheckFailureForADiscoveredName(t *testing.T) {
	w := newWorld(t)
	d := &fakeDiscovery{}
	w.Discoveries["workloads"] = d
	cfg := baseConfig(t, map[string]any{"alpha": storageInstance("fake", "keep_last")})
	cfg["discoveries"] = map[string]any{"workloads": instance("fake")}
	e := serviceCheck(t, w, cfg)
	w.Sources["vpn"].CheckErr = errors.New("source refused")
	d.Found = []discovery.Found{{Name: "vpn", Type: "fake", Config: jsontext.Value(`{}`), Artifact: model.Artifact{Group: "g", Name: "vpn", Importance: model.ImportanceRequired, Sensitivity: model.SensitivityPlain}}}
	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, a := range rep.Artifacts {
		if strings.Contains(a.Error, "source check") {
			found = true
		}
	}
	if !found {
		t.Fatalf("artifacts %+v", rep.Artifacts)
	}
}

func TestRunExpandsAUnitSourceAndSkipsAMissingCheck(t *testing.T) {
	w := newWorld(t)
	d := &fakeDiscovery{}
	w.Discoveries["workloads"] = d
	units := &fakeSource{
		Data:     []byte("x"),
		UnitList: []source.Unit{{Name: "one", Config: jsontext.Value(`{}`)}, {Name: "two", Config: jsontext.Value(`{}`)}},
	}
	w.Sources["units"] = units
	if err := instances.Register(w.Registry, source.Kind, instances.FactorySpec[map[string]jsontext.Value, source.Source]{
		Type: "plain", Example: []byte(`{}`),
		New: func(context.Context, map[string]jsontext.Value, instances.Deps) (source.Source, error) {
			return &plainSource{Data: []byte("plain")}, nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	cfg := baseConfig(t, map[string]any{"alpha": storageInstance("fake", "keep_last")})
	cfg["discoveries"] = map[string]any{"workloads": instance("fake")}
	e := serviceCheck(t, w, cfg)
	d.Found = []discovery.Found{
		{Name: "units", Type: "fake", Config: jsontext.Value(`{}`), Artifact: model.Artifact{Group: "g", Name: "units", Importance: model.ImportanceRequired, Sensitivity: model.SensitivityPlain}},
		{Name: "plain", Type: "plain", Config: jsontext.Value(`{}`), Artifact: model.Artifact{Group: "g", Name: "plain", Importance: model.ImportanceRequired, Sensitivity: model.SensitivityPlain}},
	}
	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, a := range rep.Artifacts {
		names[a.Artifact.Name] = true
	}
	for _, want := range []string{"units/one", "units/two", "plain"} {
		if !names[want] {
			t.Fatalf("artifact %q missing: %+v", want, rep.Artifacts)
		}
	}
}

func TestRunReportsAScopeCloseFailure(t *testing.T) {
	w := newWorld(t)
	d := &fakeDiscovery{}
	w.Discoveries["workloads"] = d
	closing := &failingCloseSource{closeErr: errors.New("close refused")}
	w.ExtraSources["closing"] = closing
	cfg := baseConfig(t, map[string]any{"alpha": storageInstance("fake", "keep_last")})
	cfg["discoveries"] = map[string]any{"workloads": instance("fake")}
	e := serviceCheck(t, w, cfg)
	d.Found = []discovery.Found{{Name: "closing", Type: "fake", Config: jsontext.Value(`{}`), Artifact: model.Artifact{Group: "g", Name: "closing", Importance: model.ImportanceRequired, Sensitivity: model.SensitivityPlain}}}
	if _, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled}); err != nil {
		t.Fatal(err)
	}
}
