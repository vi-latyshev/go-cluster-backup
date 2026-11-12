package layout

import (
	"slices"
	"strings"
	"testing"
	"time"

	json "encoding/json/v2"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/configuration"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
)

// testLayout is the reference layout: the defaults document's own values.
func testLayout(t *testing.T) Layout {
	t.Helper()
	return New(Config{
		IDLayout:       "20060102T150405Z",
		RunPrefix:      mustTemplate(t, "runs/{{ .Run.ID }}"),
		ArtifactKey:    mustTemplate(t, "{{ .Artifact.Group }}/{{ .Artifact.Name }}{{ .Artifact.Ext }}"),
		ManifestKey:    mustTemplate(t, "manifest.json"),
		ManualIDSuffix: "-manual",
	})
}

func mustTemplate(t *testing.T, text string) configuration.Template {
	t.Helper()
	raw, err := json.Marshal(map[string]any{"t": text})
	if err != nil {
		t.Fatal(err)
	}
	var cfg struct {
		T configuration.Template `json:"t"`
	}
	if err := configuration.Decode(raw, &cfg); err != nil {
		t.Fatal(err)
	}
	return cfg.T
}

func modelArtifact(name string) model.Artifact { return model.Artifact{Name: name} }

// TestKeysRenderTheStoredLayout pins the whole surface a deployment's templates
// produce: the survey prefix, the run id and kind, and the three object keys.
func TestKeysRenderTheStoredLayout(t *testing.T) {
	l := testLayout(t)
	at := time.Date(2026, 10, 12, 4, 20, 0, 0, time.UTC)
	if got := l.RunID(model.RunKindScheduled, at); got != "20261012T042000Z" {
		t.Errorf("scheduled run id = %q", got)
	}
	if got := l.RunID(model.RunKindManual, at); got != "20261012T042000Z-manual" {
		t.Errorf("manual run id = %q", got)
	}
	if got := l.Kind("20261012T042000Z-manual"); got != model.RunKindManual {
		t.Errorf("manual kind = %q", got)
	}
	if got := l.Kind("20261012T042000Z"); got != model.RunKindScheduled {
		t.Errorf("scheduled kind = %q", got)
	}
	prefix, err := l.Prefix()
	if err != nil || prefix != "runs/" {
		t.Fatalf("prefix = (%q, %v)", prefix, err)
	}
	if got, err := l.RunKey("r1"); err != nil || got != "runs/r1" {
		t.Errorf("run key = (%q, %v)", got, err)
	}
	if got, err := l.ArtifactKey("r1", model.Artifact{Group: "g", Name: "n", Ext: ".bin"}); err != nil || got != "runs/r1/g/n.bin" {
		t.Errorf("artifact key = (%q, %v)", got, err)
	}
	if got, err := l.ManifestKey("r1"); err != nil || got != "runs/r1/manifest.json" {
		t.Errorf("manifest key = (%q, %v)", got, err)
	}
}

func TestRunPrefixLiteralShape(t *testing.T) {
	for _, text := range []string{
		"{{ .Run.ID }}{{ .Run.ID }}",  // first node is an action, not literal text
		"runs/{{if .Run.ID}}{{end}}",  // second node is not an action
		"runs/{{ .Run.ID | printf }}", // the action carries more than one command
		"runs/{{ printf .Run.ID }}",   // the action's single command has several args
		"runs/static",                 // no action at all
		`runs/{{ "x" }}`,              // the action does not read .Run.ID
		"runs/{{",                     // the template does not parse
	} {
		if _, err := runPrefixLiteral(text); err == nil {
			t.Errorf("runPrefixLiteral(%q) accepted a bad prefix", text)
		}
	}
}

func TestRenderKeyReportsATemplateFailure(t *testing.T) {
	l := testLayout(t)
	l.cfg.ArtifactKey = mustTemplate(t, `{{ .Run.Nope }}`)
	if _, err := l.ArtifactKey("20261012T042000Z", modelArtifact("x")); err == nil {
		t.Fatal("a template naming an unknown field rendered")
	}
}

func TestArtifactAndManifestKeysReportABadRunPrefix(t *testing.T) {
	l := testLayout(t)
	l.cfg.RunPrefix = mustTemplate(t, `/abs/{{ .Run.ID }}`)
	if _, err := l.ArtifactKey("20261012T042000Z", modelArtifact("x")); err == nil {
		t.Fatal("ArtifactKey accepted an absolute run prefix")
	}
	if _, err := l.ManifestKey("20261012T042000Z"); err == nil {
		t.Fatal("ManifestKey accepted an absolute run prefix")
	}
}

func TestManifestKeyReportsATemplateFailure(t *testing.T) {
	l := testLayout(t)
	l.cfg.ManifestKey = mustTemplate(t, `{{ .Run.Nope }}`)
	if _, err := l.ManifestKey("20261012T042000Z"); err == nil {
		t.Fatal("a manifest template naming an unknown field rendered")
	}
}

// TestRenderKeyRefusesUnsafeKeys: an empty result and a traversal are refused,
// never cleaned into a key that escapes the layout.
func TestRenderKeyRefusesUnsafeKeys(t *testing.T) {
	if _, err := renderKey(mustTemplate(t, ""), keyData{}, "k"); err == nil {
		t.Fatal("renderKey accepted an empty key")
	}
	l := testLayout(t)
	if _, err := l.ArtifactKey("r1", model.Artifact{Group: "..", Name: "n"}); err == nil {
		t.Fatal("ArtifactKey accepted a traversal")
	}
}

// TestCheckTypeChecksEveryKeyTemplate: validate checks every key template
// against its key data, in every branch and every definition, and refuses a key
// whose every render is provably unsafe. A value-dependent failure stays a
// run-time refusal; the default layout and a "/" separator after an action pass.
func TestCheckTypeChecksEveryKeyTemplate(t *testing.T) {
	if err := testLayout(t).Check(); err != nil {
		t.Fatalf("the default layout failed validate: %v", err)
	}
	cases := map[string]struct {
		mutate func(*Layout)
		want   []string
	}{
		"a misspelled artifact field": {
			mutate: func(l *Layout) { l.cfg.ArtifactKey = mustTemplate(t, "{{ .Artifact.Grup }}/{{ .Artifact.Name }}") },
			want:   []string{"/layout/artifact_key", "Grup"},
		},
		"a dead-branch typo is still refused": {
			mutate: func(l *Layout) {
				l.cfg.ArtifactKey = mustTemplate(t, `{{ if false }}{{ .Artifact.Grup }}{{ end }}{{ .Artifact.Name }}`)
			},
			want: []string{"/layout/artifact_key"},
		},
		"a misspelled manifest field": {
			mutate: func(l *Layout) { l.cfg.ManifestKey = mustTemplate(t, "{{ .Run.Nope }}.json") },
			want:   []string{"/layout/manifest_key"},
		},
		"a misspelled run prefix field": {
			mutate: func(l *Layout) { l.cfg.RunPrefix = mustTemplate(t, "runs/{{ .Run.Id }}") },
			want:   []string{"/layout/run_prefix"},
		},
		"a bad run prefix shape": {
			mutate: func(l *Layout) { l.cfg.RunPrefix = mustTemplate(t, "runs/static") },
			want:   []string{"/layout/run_prefix", "exactly {{ .Run.ID }}"},
		},
		"an artifact key that always starts absolute": {
			mutate: func(l *Layout) { l.cfg.ArtifactKey = mustTemplate(t, "/{{ .Artifact.Name }}") },
			want:   []string{"/layout/artifact_key", "relative"},
		},
		"a top-level traversal": {
			mutate: func(l *Layout) { l.cfg.ArtifactKey = mustTemplate(t, "{{ .Artifact.Group }}/../{{ .Artifact.Name }}") },
			want:   []string{"/layout/artifact_key", "..", "never steps out"},
		},
		"an empty template": {
			mutate: func(l *Layout) { l.cfg.ManifestKey = mustTemplate(t, "") },
			want:   []string{"/layout/manifest_key", "empty"},
		},
		"a comment-only template": {
			mutate: func(l *Layout) { l.cfg.ManifestKey = mustTemplate(t, "{{/* nothing */}}") },
			want:   []string{"/layout/manifest_key", "empty"},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			l := testLayout(t)
			tc.mutate(&l)
			err := l.Check()
			if err == nil {
				t.Fatal("Check accepted a key the run cannot render")
			}
			for _, want := range tc.want {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("the refusal does not name %q: %v", want, err)
				}
			}
		})
	}
}

// TestCheckAcceptsConditionalAndSeparatorLiterals: a "/" after an action is a
// separator, and a literal inside a branch is never judged — only text every
// render carries is.
func TestCheckAcceptsConditionalAndSeparatorLiterals(t *testing.T) {
	passing := map[string]string{
		"a separator after an action": `{{ .Artifact.Group }}/{{ .Artifact.Name }}{{ .Artifact.Ext }}`,
		"a conditional traversal":     `{{ if .Artifact.Sealed }}../{{ end }}{{ .Artifact.Name }}`,
		"an action then a slash":      `{{ .Run.ID }}/manifest.json`,
	}
	for name, key := range passing {
		t.Run(name, func(t *testing.T) {
			l := testLayout(t)
			l.cfg.ArtifactKey = mustTemplate(t, key)
			if err := l.Check(); err != nil {
				t.Fatalf("Check refused a working key: %v", err)
			}
		})
	}
}

// TestReviewLayoutNeverSamplesUnsealedSecret: a key may read any artifact field.
// Validate checks the template, not a sampled artifact, so no secret envelope is
// invented — an unsealed secret artifact is one production refuses, and the old
// sampling refused this working key for it.
func TestReviewLayoutNeverSamplesUnsealedSecret(t *testing.T) {
	l := testLayout(t)
	l.cfg.ArtifactKey = mustTemplate(t,
		`{{ if eq .Artifact.Sensitivity "secret" }}{{ .Artifact.Sealed }}{{ end }}{{ .Artifact.Group }}/{{ .Artifact.Name }}`)
	if err := l.Check(); err != nil {
		t.Fatalf("a key reading the artifact's own fields was refused: %v", err)
	}
}

// TestSampleRunIDsFallBackWithoutARunLayout: a restore-only config carries no id
// layout, and the ids a label is checked against must still be the shape a
// restore uses — a fixed id of each kind, never an empty one that would refuse
// the default side-by-side directory.
func TestSampleRunIDsFallBackWithoutARunLayout(t *testing.T) {
	want := []string{"20260102T030405Z", "20260102T030405Z-manual"}
	if got := New(Config{ManualIDSuffix: "-manual"}).SampleRunIDs(); !slices.Equal(got, want) {
		t.Fatalf("SampleRunIDs = %v, want %v", got, want)
	}
	if got := testLayout(t).SampleRunIDs(); !slices.Equal(got, want) {
		t.Fatalf("SampleRunIDs = %v, want %v", got, want)
	}
}

// TestPrefixReportsTheShape: the run's own survey prefix read keeps naming the
// field, and the offline check reports the same cause under its config path.
func TestPrefixReportsTheShape(t *testing.T) {
	l := testLayout(t)
	l.cfg.RunPrefix = mustTemplate(t, "runs/static")
	if _, err := l.Prefix(); err == nil || !strings.Contains(err.Error(), "layout.run_prefix") {
		t.Fatalf("Prefix = %v, want the field named", err)
	}
}

// TestCheckReportsEveryKeyFailure: one validate pass reports the prefix shape
// and the manifest key together, so the operator fixes the file once.
func TestCheckReportsEveryKeyFailure(t *testing.T) {
	l := testLayout(t)
	l.cfg.RunPrefix = mustTemplate(t, "runs/static")
	l.cfg.ManifestKey = mustTemplate(t, "{{ .Run.Nope }}.json")
	err := l.Check()
	if err == nil || !strings.Contains(err.Error(), "/layout/run_prefix") || !strings.Contains(err.Error(), "/layout/manifest_key") {
		t.Fatalf("Check = %v, want both failures named", err)
	}
}
