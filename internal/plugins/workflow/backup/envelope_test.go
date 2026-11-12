package backup

// The typed envelope boundaries: the rate limit travels with every pipeline
// plan, the sweep's ownership filter proves a writer by manifest, and the
// discovery summary separates a declared peer's delegation from this run's own
// account.

import (
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/inventory"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/layout"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
)

func TestPipelinePlanCarriesTheEnvelopeRate(t *testing.T) {
	plan := artifactPlan{
		instance: "vpn", srcType: "fake", artifact: model.Artifact{Group: "vpn", Name: "wireguard"},
		rateLimit: model.ByteSize(1 << 20),
	}
	got := pipelinePlan(plan)
	if got.RateLimit != model.ByteSize(1<<20) {
		t.Fatalf("RateLimit = %d, want the envelope's 1MiB/s", got.RateLimit)
	}
	if got.SourceInstance != "vpn" || got.Artifact.Name != "wireguard" {
		t.Fatalf("the pipeline plan lost the artifact envelope: %+v", got)
	}
}

func TestSweepEligibilityProvesWriterOwnership(t *testing.T) {
	survey := inventory.Survey{OK: true, Runs: map[string]inventory.Run{
		"own":     {Manifest: &model.RunReport{WriterIdentity: "node-a"}},
		"foreign": {Manifest: &model.RunReport{WriterIdentity: "node-b"}},
		"unowned": {},
		"unreadable": {
			ManifestProblem: "unreadable",
			Manifest:        &model.RunReport{WriterIdentity: "node-a"},
		},
	}}
	w := &Workflow{run: RunOptions{WriterIdentity: "node-a"}, layout: goodLayout()}
	eligible := w.sweepEligibility(survey)
	if eligible == nil {
		t.Fatal("a configured writer identity produced no ownership filter")
	}
	for key, want := range map[string]bool{
		"runs/own/artifact.bin":        true,
		"runs/own/nested/artifact.bin": true,
		"runs/foreign/artifact.bin":    false,
		"runs/unowned/artifact.bin":    false,
		"runs/unreadable/artifact.bin": false,
		"runs/unknown/artifact.bin":    false,
		"runs/own":                     false, // a run key itself is no upload key
	} {
		if got := eligible(key); got != want {
			t.Errorf("eligible(%q) = %v, want %v", key, got, want)
		}
	}

	w = &Workflow{run: RunOptions{}, layout: goodLayout()}
	if w.sweepEligibility(survey) != nil {
		t.Fatal("no writer identity must keep every upload eligible")
	}
	w.run.WriterIdentity = "node-a"
	failed := w.sweepEligibility(inventory.Survey{})
	if failed == nil {
		t.Fatal("a configured writer identity lost its filter on a failed survey")
	}
	if got := failed("runs/own/artifact.bin"); got {
		t.Fatal("a failed survey made an upload attributable")
	}
	// A layout that cannot render a run key has no prefix to prove ownership
	// with: the filter refuses every upload rather than trusting a guess.
	broken := &Workflow{run: RunOptions{WriterIdentity: "node-a"}, layout: layout.Layout{}}
	if broken.sweepEligibility(survey)("runs/own/artifact.bin") {
		t.Fatal("a renderless run key proved ownership")
	}
}

func TestExclusionCountsSeparateElsewhere(t *testing.T) {
	covered, elsewhere := exclusionCounts([]model.Exclusion{
		{Kind: model.ExclusionIgnored},
		{Kind: model.ExclusionCovered},
		{Kind: model.ExclusionElsewhere, Node: "node-b"},
	})
	if covered != 2 || elsewhere != 1 {
		t.Fatalf("counts = (%d covered, %d elsewhere), want (2, 1)", covered, elsewhere)
	}
}
