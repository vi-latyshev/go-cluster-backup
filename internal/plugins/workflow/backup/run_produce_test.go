package backup_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/discovery"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/workflow/backup"
)

// TestSourceDescribeFailureFailsTheArtifact: a source that cannot describe
// itself fails its artifact before any byte.
func TestSourceDescribeFailureFailsTheArtifact(t *testing.T) {
	w := newWorld(t)
	w.Sources["vpn"] = &fakeSource{Data: []byte("x"), DescribeErr: errors.New("no describe")}
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	e := w.newAssembly(t, cfg)

	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	got := artifactByName(rep, "wireguard")
	if got.Outcome != model.ArtifactFailed || !strings.Contains(got.Error, "no describe") {
		t.Fatalf("artifact %+v", got)
	}
}

// TestProcessorDescribeFailureFailsTheArtifact: a processor whose traits cannot
// be read fails the artifact's chain.
func TestProcessorDescribeFailureFailsTheArtifact(t *testing.T) {
	w := newWorld(t)
	registerProcessorType(t, w, "errd", &errDescribeProcessor{})
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["processors"] = map[string]any{"p": instance("errd")}
	cfg["sources"] = map[string]any{
		"vpn": sourceInstance("fake", "vpn", "wireguard", []string{"p"}, "required", "plain"),
	}
	e := w.newAssembly(t, cfg)

	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	got := artifactByName(rep, "wireguard")
	if got.Outcome != model.ArtifactFailed || !strings.Contains(got.Error, "processor 0 describe") {
		t.Fatalf("artifact %+v", got)
	}
}

// --- dumpStats: ErrUnsupported is no report ----------------------------------

// TestDumpStatsUnsupportedIsEmpty: a source that answers ErrUnsupported leaves
// out nothing the run has to report, and its artifact still runs.
func TestDumpStatsUnsupportedIsEmpty(t *testing.T) {
	w := newWorld(t)
	w.Sources["vpn"] = &fakeSource{Data: []byte("x"), StatsErr: errors.ErrUnsupported}
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	e := w.newAssembly(t, cfg)

	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	got := artifactByName(rep, "wireguard")
	if got.Outcome != model.ArtifactOK {
		t.Fatalf("artifact %+v", got)
	}
	if got.Stats != (model.DumpStats{}) {
		t.Fatalf("stats %+v, want the zero report", got.Stats)
	}
}

func TestProgressIntervalZeroStillRuns(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["run"].(map[string]any)["progress_interval"] = "0s"
	e := w.newAssembly(t, cfg)

	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeDelivered || artifactByName(rep, "wireguard").Outcome != model.ArtifactOK {
		t.Fatalf("report %+v", rep)
	}
}

// --- processArtifact: the artifact key can fail after the bytes --------------

// TestArtifactKeyFailureFailsTheArtifact: the object was produced but its key
// cannot be rendered, so nothing is uploaded and the artifact fails. Validate
// samples fixed artifact names, so a name a discovery renders for itself is the
// run's own refusal: a traversal is refused before the upload, never cleaned
// into a key outside the layout.
func TestArtifactKeyFailureFailsTheArtifact(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["discoveries"] = map[string]any{"workloads": instance("fake")}
	w.Discoveries["workloads"] = &fakeDiscovery{Found: []discovery.Found{{
		Name: "escape", Type: "fake", Config: jsontext.Value(`{}`),
		Artifact: model.Artifact{
			Name: "..", Group: "vpn",
			Importance: model.ImportanceRequired, Sensitivity: model.SensitivityPlain,
		},
	}}}
	e := w.newAssembly(t, cfg)

	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	var got model.ArtifactResult
	for _, a := range rep.Artifacts {
		if strings.Contains(a.Error, "layout.artifact_key") {
			got = a
		}
	}
	if got.Outcome != model.ArtifactFailed {
		t.Fatalf("no artifact failed on its key: %+v", rep.Artifacts)
	}
	// The refusal names no key: nothing was uploaded for this artifact.
	if got.Key != "" {
		t.Fatalf("a refused artifact carries key %q", got.Key)
	}
}

// TestManifestKeyFailureFailsTheStorage: the objects are there but the record of
// them cannot be named, so the copy is incomplete and the run fails. A template
// keyed on the sample run ids renders a safe key offline and an empty one at
// run time — validate cannot prove what every run id renders — so this is the
// run's own defense.
func TestManifestKeyFailureFailsTheStorage(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["layout"].(map[string]any)["manifest_key"] = `{{ if or (eq .Run.ID "20260102T030405Z") (eq .Run.ID "20260102T030405Z-manual") }}manifest.json{{ end }}`
	e := w.newAssembly(t, cfg)

	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeFailed {
		t.Fatalf("outcome %q", rep.Outcome)
	}
	got := storageResult(rep, "primary")
	if got.Outcome != model.StorageFailed || !strings.Contains(got.Error, "layout.manifest_key") {
		t.Fatalf("storage %+v", got)
	}
}
