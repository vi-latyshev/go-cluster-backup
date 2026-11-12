package assembly

import (
	"context"
	"errors"
	"strings"
	"testing"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/discovery"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/planning"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/processor"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source"
)

// recordedCredentialSource carries credentials and, like the staged source,
// reports a recorded producer chain — the one shape whose sealing fact the
// offline check cannot compute from the pipeline.
type recordedCredentialSource struct{ *credentialSource }

func (r recordedCredentialSource) RecordedChain(context.Context) ([]model.ProcessorStep, error) {
	return nil, nil
}

// suiteSource is one source that also proves a configured binary suite, the
// optional local capability planning and doctor read.
type suiteSource struct {
	fakeSource
	calls int
	err   error
}

func (s *suiteSource) CheckBinaries(context.Context) error {
	s.calls++
	return s.err
}

// describeFailingProcessor cannot answer its static traits: the offline sealing
// fact is then unknown, and the run — which asks again — reports it.
type describeFailingProcessor struct{ fakeProcessor }

func (describeFailingProcessor) Describe(context.Context) (processor.ProcessorTraits, error) {
	return processor.ProcessorTraits{}, errors.New("no static traits")
}

// credentialConfig is one complete run config whose single source is the one the
// test registered, with the pipeline the case needs.
func credentialConfig(t *testing.T, w *world, pipeline ...string) map[string]any {
	t.Helper()
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["sources"] = map[string]any{"vpn": sourceInstance("fake", "vpn", "wireguard", pipeline, "required", "plain")}
	return cfg
}

// TestOfflineCredentialRuleSharesTheRunsDecision: the configuration check makes
// the same pure decision Producer.Resolve makes — a published warning exception
// stays valid and is reported, unconditional credential bytes are refused before
// the first byte, sealed bytes are quiet, and a policy failure is the source's
// own error, never a silent proceed.
func TestOfflineCredentialRuleSharesTheRunsDecision(t *testing.T) {
	t.Run("a published exception warns and stays valid", func(t *testing.T) {
		w := newWorld(t)
		cs := &credentialSource{
			Source:  fakeSource{Traits: source.SourceTraits{Credentials: true}},
			Verdict: source.CredentialWarn,
		}
		w.ExtraSources["vpn"] = cs
		e := w.newAssembly(t, credentialConfig(t, w, "zstd3"))
		warnings := e.Warnings()
		if len(warnings) != 1 || warnings[0].Code != source.CredentialWarningCode {
			t.Fatalf("warnings = %+v", warnings)
		}
		if cs.PolicyCalls != 1 {
			t.Fatalf("the source type's policy was asked %d times", cs.PolicyCalls)
		}
		// The accessor hands out a copy: a caller printing warnings cannot
		// change what the assembly holds.
		warnings[0].Code = "mutated"
		if e.Warnings()[0].Code != source.CredentialWarningCode {
			t.Fatal("Warnings exposed the assembly's own slice")
		}
	})

	t.Run("unconditional credential bytes are refused", func(t *testing.T) {
		w := newWorld(t)
		w.ExtraSources["vpn"] = &credentialSource{Source: fakeSource{Traits: source.SourceTraits{Credentials: true}}}
		_, err := w.newAssemblyErr(t, credentialConfig(t, w, "zstd3"))
		if err == nil || !strings.Contains(err.Error(), "carries credentials and its pipeline has no sealing processor") {
			t.Fatalf("New = %v, want the credential refusal", err)
		}
	})

	t.Run("sealed bytes stay quiet", func(t *testing.T) {
		w := newWorld(t)
		w.Processors["seal"] = NewFakeProcessor(".age", true, nil)
		cs := &credentialSource{
			Source:  fakeSource{Traits: source.SourceTraits{Credentials: true}},
			Verdict: source.CredentialWarn,
		}
		w.ExtraSources["vpn"] = cs
		cfg := credentialConfig(t, w, "zstd3", "seal")
		cfg["processors"].(map[string]any)["seal"] = instance("fake")
		e := w.newAssembly(t, cfg)
		if len(e.Warnings()) != 0 {
			t.Fatalf("a sealed source carried %+v", e.Warnings())
		}
		if cs.PolicyCalls != 0 {
			t.Fatalf("sealed bytes asked for a verdict %d times", cs.PolicyCalls)
		}
	})

	t.Run("a recorded chain is judged from its own facts", func(t *testing.T) {
		w := newWorld(t)
		cs := &credentialSource{Source: fakeSource{Traits: source.SourceTraits{Credentials: true, Sealed: true}}}
		w.ExtraSources["vpn"] = recordedCredentialSource{credentialSource: cs}
		e := w.newAssembly(t, credentialConfig(t, w))
		if len(e.Warnings()) != 0 {
			t.Fatalf("a sealed recorded chain carried %+v", e.Warnings())
		}
	})

	t.Run("a failing policy is the source's error", func(t *testing.T) {
		w := newWorld(t)
		w.ExtraSources["vpn"] = &credentialSource{
			Source:    fakeSource{Traits: source.SourceTraits{Credentials: true}},
			PolicyErr: errors.New("policy unavailable"),
		}
		_, err := w.newAssemblyErr(t, credentialConfig(t, w, "zstd3"))
		if err == nil || !strings.Contains(err.Error(), "policy unavailable") {
			t.Fatalf("New = %v, want the policy failure", err)
		}
	})

	t.Run("an unsupported policy keeps the refusal", func(t *testing.T) {
		w := newWorld(t)
		w.ExtraSources["vpn"] = &credentialSource{
			Source:    fakeSource{Traits: source.SourceTraits{Credentials: true}},
			PolicyErr: errors.ErrUnsupported,
		}
		_, err := w.newAssemblyErr(t, credentialConfig(t, w, "zstd3"))
		if err == nil || !strings.Contains(err.Error(), "carries credentials") {
			t.Fatalf("New = %v, want the default refusal", err)
		}
	})

	t.Run("a failing Describe leaves the verdict to the run", func(t *testing.T) {
		w := newWorld(t)
		w.ExtraSources["vpn"] = &credentialSource{Source: fakeSource{DescribeErr: errors.New("no static facts")}}
		e := w.newAssembly(t, credentialConfig(t, w, "zstd3"))
		if len(e.Warnings()) != 0 {
			t.Fatalf("a failing Describe produced %+v", e.Warnings())
		}
	})

	t.Run("an unreadable pipeline trait leaves the verdict to the run", func(t *testing.T) {
		w := newWorld(t)
		w.ExtraProcessors["zstd3"] = &describeFailingProcessor{}
		w.ExtraSources["vpn"] = &credentialSource{
			Source:  fakeSource{Traits: source.SourceTraits{Credentials: true}},
			Verdict: source.CredentialWarn,
		}
		e := w.newAssembly(t, credentialConfig(t, w, "zstd3"))
		if len(e.Warnings()) != 0 {
			t.Fatalf("an unreadable sealing fact produced %+v", e.Warnings())
		}
	})
}

// TestInstancesForProjectsTheBinarySuite: the local suite proof is a capability
// of the built instance, and the preflight receives it exactly when the instance
// answers.
func TestInstancesForProjectsTheBinarySuite(t *testing.T) {
	w := newWorld(t)
	suite := &suiteSource{}
	w.ExtraSources["vpn"] = suite
	e := w.newAssembly(t, credentialConfig(t, w, "zstd3"))
	var found bool
	for _, inst := range e.instancesFor(scopeRun) {
		if inst.Kind == "source" && inst.Name == "vpn" {
			found = inst.BinariesCheck == suite
		}
	}
	if !found {
		t.Fatal("the built source's binary-suite capability did not reach the preflight projection")
	}
}

// discoveredConfig is one complete run config with no declared sources: the
// only source is the one the fake discovery returns.
func discoveredConfig(t *testing.T, w *world) map[string]any {
	t.Helper()
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	delete(cfg, "sources")
	cfg["discoveries"] = map[string]any{"workloads": instance("fake")}
	return cfg
}

// TestReviewDiscoveredCredentialsFailPreflight is the review's counterexample:
// a discovered source whose bytes are credentials must fail the preflight the
// same way a declared one does, not report ok to a run that will refuse it.
func TestReviewDiscoveredCredentialsFailPreflight(t *testing.T) {
	w := newWorld(t)
	w.ExtraSources["found"] = &credentialSource{Source: fakeSource{Traits: source.SourceTraits{Credentials: true}}}
	w.Discoveries["workloads"] = &fakeDiscovery{Found: []discovery.Found{{
		Name: "found", Type: "fake", Config: jsontext.Value(`{}`),
		Artifact: model.Artifact{Name: "found", Group: "g", Importance: model.ImportanceRequired, Sensitivity: model.SensitivityPlain},
		Pipeline: []string{},
	}}}
	e := w.newAssembly(t, discoveredConfig(t, w))
	rows := e.PreflightResults(context.Background(), planning.Request{})
	for _, row := range rows {
		if row.Instance == "found" {
			if row.OK {
				t.Fatalf("preflight accepted unsealed credential source: %+v", row)
			}
			if !strings.Contains(row.Error, "carries credentials") {
				t.Fatalf("the refusal does not name the credential rule: %+v", row)
			}
			return
		}
	}
	t.Fatal("discovered source was not checked")
}

// TestReviewDiscoveredCredentialWarningReachesPreflight: the published
// exception stays non-fatal for a discovered source, and its code reaches the
// preflight row instead of being swallowed.
func TestReviewDiscoveredCredentialWarningReachesPreflight(t *testing.T) {
	w := newWorld(t)
	cs := &credentialSource{
		Source:  fakeSource{Traits: source.SourceTraits{Credentials: true}},
		Verdict: source.CredentialWarn,
	}
	w.ExtraSources["found"] = cs
	w.Discoveries["workloads"] = &fakeDiscovery{Found: []discovery.Found{{
		Name: "found", Type: "fake", Config: jsontext.Value(`{}`),
		Artifact: model.Artifact{Name: "found", Group: "g", Importance: model.ImportanceRequired, Sensitivity: model.SensitivityPlain},
		Pipeline: []string{},
	}}}
	e := w.newAssembly(t, discoveredConfig(t, w))
	rows := e.PreflightResults(context.Background(), planning.Request{})
	for _, row := range rows {
		if row.Instance == "found" {
			if !row.OK || len(row.Warnings) != 1 || row.Warnings[0].Code != source.CredentialWarningCode {
				t.Fatalf("the warned discovered row = %+v", row)
			}
			return
		}
	}
	t.Fatal("discovered source was not checked")
}

// TestReviewEnumeratedCredentialsFailPerUnit: a discovered enumerator's child
// units answer the same rule individually — the refused unit fails alone, the
// healthy sibling still reports ok.
func TestReviewEnumeratedCredentialsFailPerUnit(t *testing.T) {
	w := newWorld(t)
	w.ExtraSources["fleet"] = &fakeSource{UnitList: []source.Unit{
		{Name: "good", Config: jsontext.Value(`{}`)},
		{Name: "bad", Config: jsontext.Value(`{}`)},
	}}
	w.ExtraSources["fleet/good"] = &fakeSource{}
	w.ExtraSources["fleet/bad"] = &credentialSource{Source: fakeSource{Traits: source.SourceTraits{Credentials: true}}}
	w.Discoveries["workloads"] = &fakeDiscovery{Found: []discovery.Found{{
		Name: "fleet", Type: "fake", Config: jsontext.Value(`{}`),
		Artifact: model.Artifact{Name: "fleet", Group: "g", Importance: model.ImportanceRequired, Sensitivity: model.SensitivityPlain},
		Pipeline: []string{},
	}}}
	e := w.newAssembly(t, discoveredConfig(t, w))
	rows := e.PreflightResults(context.Background(), planning.Request{})
	seen := map[string]planning.Result{}
	for _, row := range rows {
		seen[row.Instance] = row
	}
	if r := seen["fleet/bad"]; r.OK || !strings.Contains(r.Error, "carries credentials") {
		t.Fatalf("the refused unit = %+v", r)
	}
	if r := seen["fleet/good"]; !r.OK {
		t.Fatalf("the healthy unit = %+v", r)
	}
}

// TestCredentialCheckLeavesUnknownPipelinesToTheRun: a discovered rule naming a
// processor this build does not know cannot be judged offline; the run fails
// that artifact on its own, so the preflight must not invent a verdict.
func TestCredentialCheckLeavesUnknownPipelinesToTheRun(t *testing.T) {
	w := newWorld(t)
	e := w.newAssembly(t, credentialConfig(t, w))
	cs := &credentialSource{Source: fakeSource{Traits: source.SourceTraits{Credentials: true}}}
	warning, err := e.credentialCheck(context.Background(), cs, model.Artifact{Name: "found"}, []string{"ghost"})
	if err != nil || warning.Code != "" {
		t.Fatalf("an unknown pipeline produced %+v, %v", warning, err)
	}
}
