package layout

import (
	"errors"
	"fmt"
	"path"
	"reflect"
	"strings"
	"text/template"
	"text/template/parse"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/configuration"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/must"
)

// Config is the typed options of this library: the key templates and the run id
// layout. Assembly projects it from the configuration document.
type Config struct {
	// IDLayout is the Go time layout formatting a run id.
	IDLayout string
	// RunPrefix must be a literal prefix followed by exactly {{ .Run.ID }}; the
	// literal part is the listing root of a survey.
	RunPrefix configuration.Template
	// ArtifactKey renders one artifact under the run prefix.
	ArtifactKey configuration.Template
	// ManifestKey renders the run manifest under the run prefix.
	ManifestKey configuration.Template
	// ManualIDSuffix is appended to a manual run's id, so its kind is
	// derivable from the key alone.
	ManualIDSuffix string
}

// Layout is one immutable key renderer: every method reads the projected
// options and returns a fresh value.
type Layout struct{ cfg Config }

// New builds the renderer of one configuration.
func New(cfg Config) Layout { return Layout{cfg: cfg} }

// keyData is the flat data key templates render against.
type keyData struct {
	Run      keyRun
	Artifact model.Artifact
}

type keyRun struct {
	ID        string
	StartedAt string
}

// RunID builds one run's id: the start time in the configured layout, with the
// manual suffix appended for a manual run. The suffix is what lets a survey tell
// a run's kind from its key alone — a manual run killed before it wrote a
// manifest is still identifiable, and a manifest's kind is checked against it.
func (l Layout) RunID(kind model.RunKind, started time.Time) string {
	id := started.Format(l.cfg.IDLayout)
	if kind == model.RunKindManual {
		id += l.cfg.ManualIDSuffix
	}
	return id
}

// Kind is the inverse of RunID: the kind a run id names. The suffix is read
// from the layout, which every config carries — a restore-only config included
// — so a survey derives the kind even where no run section exists.
func (l Layout) Kind(id string) model.RunKind {
	if s := l.cfg.ManualIDSuffix; s != "" && strings.HasSuffix(id, s) {
		return model.RunKindManual
	}
	return model.RunKindScheduled
}

// Prefix returns the literal head of the run prefix — what a survey lists —
// and checks its shape: a non-empty literal followed by exactly
// {{ .Run.ID }}. Anything else cannot be surveyed.
func (l Layout) Prefix() (string, error) {
	lit, err := runPrefixLiteral(l.cfg.RunPrefix.Text())
	if err != nil {
		return "", fmt.Errorf("layout.run_prefix: %w", err)
	}
	return lit, nil
}

// runPrefixLiteral is the shape rule without the config path, so the offline
// check can report it under /layout/run_prefix while a run names the field.
func runPrefixLiteral(text string) (string, error) {
	tmpl, err := template.New("run_prefix").Parse(text)
	if err != nil {
		return "", fmt.Errorf("the template does not parse: %w", err)
	}
	nodes := tmpl.Root.Nodes
	if len(nodes) != 2 {
		return "", fmt.Errorf("%q must be a literal prefix followed by exactly {{ .Run.ID }}", text)
	}
	lit, ok := nodes[0].(*parse.TextNode)
	if !ok || len(lit.Text) == 0 {
		return "", fmt.Errorf("%q must start with literal text (the survey listing prefix)", text)
	}
	act, ok := nodes[1].(*parse.ActionNode)
	if !ok || act.Pipe == nil || len(act.Pipe.Cmds) != 1 || len(act.Pipe.Cmds[0].Args) != 1 {
		return "", fmt.Errorf("%q must be a literal prefix followed by exactly {{ .Run.ID }}", text)
	}
	fld, ok := act.Pipe.Cmds[0].Args[0].(*parse.FieldNode)
	if !ok || len(fld.Ident) != 2 || fld.Ident[0] != "Run" || fld.Ident[1] != "ID" {
		return "", fmt.Errorf("%q must be a literal prefix followed by exactly {{ .Run.ID }}", text)
	}
	return string(lit.Text), nil
}

// renderKey renders one template and requires the result to be a clean relative
// key: no absolute path, no traversal, no empty result.
func renderKey(t configuration.Template, data keyData, what string) (string, error) {
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

// RunKey renders the run prefix for one run id.
func (l Layout) RunKey(runID string) (string, error) {
	return renderKey(l.cfg.RunPrefix, keyData{Run: keyRun{ID: runID}}, "layout.run_prefix")
}

// ArtifactKey renders the full key of one artifact of one run.
func (l Layout) ArtifactKey(runID string, a model.Artifact) (string, error) {
	prefix, err := l.RunKey(runID)
	if err != nil {
		return "", err
	}
	rel, err := renderKey(l.cfg.ArtifactKey, keyData{Run: keyRun{ID: runID}, Artifact: a}, "layout.artifact_key")
	if err != nil {
		return "", err
	}
	return path.Join(prefix, rel), nil
}

// ManifestKey renders the full key of one run's manifest.
func (l Layout) ManifestKey(runID string) (string, error) {
	prefix, err := l.RunKey(runID)
	if err != nil {
		return "", err
	}
	rel, err := renderKey(l.cfg.ManifestKey, keyData{Run: keyRun{ID: runID}}, "layout.manifest_key")
	if err != nil {
		return "", err
	}
	return path.Join(prefix, rel), nil
}

// SampleRunIDs is the fixed pair of run ids validate renders every key template
// against — one of each kind at one instant, in this layout — and that the
// restore.apply check renders a side-by-side label against, so a label rule is
// judged by the same shape production uses. A config without a run section
// carries no id layout; its keys still render, so the fallback is one fixed id
// with the manual suffix, never an empty one.
func (l Layout) SampleRunIDs() []string {
	at := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	if id := l.RunID(model.RunKindScheduled, at); id != "" {
		return []string{id, l.RunID(model.RunKindManual, at)}
	}
	id := "20260102T030405Z"
	return []string{id, id + l.cfg.ManualIDSuffix}
}

// Check type-checks every key template and refuses one whose rendered result
// is provably unsafe: the survey prefix's shape, a field or function the key
// data does not have, and a key that always starts absolute, always carries
// ".." or is empty. It runs at construction — `validate` and `run` alike — so a
// typo in a key template is the configuration's error, never a night's.
func (l Layout) Check() error {
	var errs []error
	add := func(err error) {
		if err != nil {
			errs = append(errs, err)
		}
	}
	if _, err := runPrefixLiteral(l.cfg.RunPrefix.Text()); err != nil {
		add(fmt.Errorf("/layout/run_prefix: %w", err))
	}
	keys := []struct {
		path string
		tmpl configuration.Template
	}{
		{"/layout/run_prefix", l.cfg.RunPrefix},
		{"/layout/manifest_key", l.cfg.ManifestKey},
		{"/layout/artifact_key", l.cfg.ArtifactKey},
	}
	check := func(path string, err error) {
		if err != nil {
			add(fmt.Errorf("%s: %w", path, err))
		}
	}
	for _, key := range keys {
		check(key.path, key.tmpl.Check(reflect.TypeFor[keyData]()))
		check(key.path, unsafeKeyLiteral(key.tmpl))
	}
	return errors.Join(errs...)
}

// unsafeKeyLiteral refuses a key template whose every render is unsafe: it has
// no node at all, its first top-level node is literal text starting with "/",
// or a top-level unconditional TextNode contains "..". That mirrors renderKey's
// run-time rule (key.go:113) on the one part a render provably carries; a "/"
// after an action is a separator, and a literal inside if, range, with or
// define is not judged.
func unsafeKeyLiteral(t configuration.Template) error {
	if t.Text() == "" {
		return errors.New("the template renders an empty key")
	}
	// The decoder parsed the same source with the same functions, so a parse
	// failure here cannot happen.
	tmpl := must.Do(template.New("key").Funcs(configuration.CoreFuncs()).Parse(t.Text()))
	nodes := tmpl.Root.Nodes
	if len(nodes) == 0 {
		return errors.New("the template renders an empty key")
	}
	if text, ok := nodes[0].(*parse.TextNode); ok && strings.HasPrefix(string(text.Text), "/") {
		return fmt.Errorf("the template always starts with %q: a key must be relative", "/")
	}
	for _, node := range nodes {
		if text, ok := node.(*parse.TextNode); ok && strings.Contains(string(text.Text), "..") {
			return fmt.Errorf("the template always contains %q: a key never steps out of its prefix", "..")
		}
	}
	return nil
}
