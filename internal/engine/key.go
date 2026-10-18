package engine

import (
	"fmt"
	"path"
	"strings"
	"text/template"
	"text/template/parse"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
)

// keyData is the flat data key templates render against.
type keyData struct {
	Run      keyRun
	Artifact model.Artifact
}

type keyRun struct {
	ID        string
	StartedAt string
}

// runID builds one run's id: the start time in the configured layout, with the
// manual suffix appended for a manual run. The suffix is what lets a survey tell
// a run's kind from its key alone — a manual run killed before it wrote a
// manifest is still identifiable, and a manifest's kind is checked against it.
func (e *Engine) runID(kind model.RunKind, started time.Time) string {
	id := started.Format(e.cfg.Run.IDLayout)
	if kind == model.RunKindManual {
		id += e.cfg.Layout.ManualIDSuffix
	}
	return id
}

// idKind is the inverse of runID: the kind a run id names. The suffix is read
// from the layout, which every config carries — a restore-only config included
// — so a survey derives the kind even where no run section exists.
func (e *Engine) idKind(id string) model.RunKind {
	if s := e.cfg.Layout.ManualIDSuffix; s != "" && strings.HasSuffix(id, s) {
		return model.RunKindManual
	}
	return model.RunKindScheduled
}

// runPrefixLiteral returns the literal head of layout.run_prefix and checks its
// shape: a non-empty literal followed by exactly {{ .Run.ID }}. That literal is
// what a retention survey lists; anything else cannot be surveyed.
func runPrefixLiteral(text string) (string, error) {
	tmpl, err := template.New("run_prefix").Parse(text)
	if err != nil {
		return "", fmt.Errorf("layout.run_prefix: %w", err)
	}
	nodes := tmpl.Root.Nodes
	if len(nodes) != 2 {
		return "", fmt.Errorf("layout.run_prefix %q must be a literal prefix followed by exactly {{ .Run.ID }}", text)
	}
	lit, ok := nodes[0].(*parse.TextNode)
	if !ok || len(lit.Text) == 0 {
		return "", fmt.Errorf("layout.run_prefix %q must start with literal text (the survey listing prefix)", text)
	}
	act, ok := nodes[1].(*parse.ActionNode)
	if !ok || act.Pipe == nil || len(act.Pipe.Cmds) != 1 || len(act.Pipe.Cmds[0].Args) != 1 {
		return "", fmt.Errorf("layout.run_prefix %q must be a literal prefix followed by exactly {{ .Run.ID }}", text)
	}
	fld, ok := act.Pipe.Cmds[0].Args[0].(*parse.FieldNode)
	if !ok || len(fld.Ident) != 2 || fld.Ident[0] != "Run" || fld.Ident[1] != "ID" {
		return "", fmt.Errorf("layout.run_prefix %q must be a literal prefix followed by exactly {{ .Run.ID }}", text)
	}
	return string(lit.Text), nil
}

// renderKey renders one template and requires the result to be a clean relative
// key: no absolute path, no traversal, no empty result.
func renderKey(t plugin.Template, data keyData, what string) (string, error) {
	s, err := t.Render(data)
	if err != nil {
		return "", fmt.Errorf("%s: %w", what, err)
	}
	if s == "" {
		return "", fmt.Errorf("%s: rendered an empty key", what)
	}
	if strings.HasPrefix(s, "/") || strings.Contains(s, "..") {
		return "", fmt.Errorf("%s: rendered key %q must be a relative key without \"..\"", what, s)
	}
	return path.Clean(s), nil
}

// runKey renders the run prefix for one run id.
func (e *Engine) runKey(runID string) (string, error) {
	return renderKey(e.cfg.Layout.RunPrefix, keyData{Run: keyRun{ID: runID}}, "layout.run_prefix")
}

// artifactKey renders the full key of one artifact of one run.
func (e *Engine) artifactKey(runID string, a model.Artifact) (string, error) {
	prefix, err := e.runKey(runID)
	if err != nil {
		return "", err
	}
	rel, err := renderKey(e.cfg.Layout.ArtifactKey, keyData{Run: keyRun{ID: runID}, Artifact: a}, "layout.artifact_key")
	if err != nil {
		return "", err
	}
	return path.Join(prefix, rel), nil
}

// manifestKey renders the full key of one run's manifest.
func (e *Engine) manifestKey(runID string) (string, error) {
	prefix, err := e.runKey(runID)
	if err != nil {
		return "", err
	}
	rel, err := renderKey(e.cfg.Layout.ManifestKey, keyData{Run: keyRun{ID: runID}}, "layout.manifest_key")
	if err != nil {
		return "", err
	}
	return path.Join(prefix, rel), nil
}
