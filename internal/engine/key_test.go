package engine

import (
	"testing"
)

func TestRunPrefixLiteralShape(t *testing.T) {
	for _, text := range []string{
		"{{ .Run.ID }}{{ .Run.ID }}",  // first node is an action, not literal text
		"runs/{{if .Run.ID}}{{end}}",  // second node is not an action
		"runs/{{ .Run.ID | printf }}", // the action carries more than one command
		"runs/{{ printf .Run.ID }}",   // the action's single command has several args
	} {
		if _, err := runPrefixLiteral(text); err == nil {
			t.Errorf("runPrefixLiteral(%q) accepted a bad prefix", text)
		}
	}
}

func TestRenderKeyReportsATemplateFailure(t *testing.T) {
	e := newWorld(t).newEngine(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}))
	e.cfg.Layout.ArtifactKey = mustTemplate(t, `{{ .Run.Nope }}`)
	if _, err := e.artifactKey("20261012T042000Z", modelArtifact("x")); err == nil {
		t.Fatal("a template naming an unknown field rendered")
	}
}

func TestArtifactAndManifestKeysReportABadRunPrefix(t *testing.T) {
	e := newWorld(t).newEngine(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}))
	e.cfg.Layout.RunPrefix = mustTemplate(t, `/abs/{{ .Run.ID }}`)
	if _, err := e.artifactKey("20261012T042000Z", modelArtifact("x")); err == nil {
		t.Fatal("artifactKey accepted an absolute run prefix")
	}
	if _, err := e.manifestKey("20261012T042000Z"); err == nil {
		t.Fatal("manifestKey accepted an absolute run prefix")
	}
}

func TestManifestKeyReportsATemplateFailure(t *testing.T) {
	e := newWorld(t).newEngine(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}))
	e.cfg.Layout.ManifestKey = mustTemplate(t, `{{ .Run.Nope }}`)
	if _, err := e.manifestKey("20261012T042000Z"); err == nil {
		t.Fatal("a manifest template naming an unknown field rendered")
	}
}

// --- retry.go ---------------------------------------------------------------
