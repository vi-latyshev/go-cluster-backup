package layout

import (
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
