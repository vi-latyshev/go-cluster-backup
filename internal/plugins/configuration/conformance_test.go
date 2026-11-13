package configuration

import (
	"reflect"
	"strings"
	"testing"
	"text/template"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
)

// confEvent is the representative event the conformance sweep executes: every
// branch a case opens is reachable, so the executor's answer is the real one.
func confEvent(writerIdentity string) model.Event {
	return model.Event{Run: model.RunReport{
		Kind: model.RunKindScheduled, RunID: "run", StartedAt: time.Now(),
		Duration: model.Duration(time.Second), Outcome: model.OutcomeDelivered,
		WriterIdentity: writerIdentity,
		Artifacts: []model.ArtifactResult{{
			Artifact: model.Artifact{Name: "snapshot"}, Outcome: model.ArtifactOK,
			Warnings: []model.Warning{{Code: "a.warning", Message: "a warning"}},
		}},
		Excluded: []model.Exclusion{{Kind: model.ExclusionIgnored}},
		Warnings: []model.Warning{{Code: "a.warning", Message: "a warning"}},
		Lease:    &model.LeaseState{HolderKind: model.RunKindManual},
	}}
}

// checkAgainstExecutor proves the contract: for a template the real executor
// accepts, the checker accepts; for the call-error class the executor rejects,
// the checker refuses.
func checkAgainstExecutor(t *testing.T, fixture model.Event, src string, wantCheck bool) {
	t.Helper()
	tmpl := parseTest(t, src, testExtras())
	var out strings.Builder
	execErr := tmpl.Execute(&out, fixture)
	checkErr := CheckTemplate(tmpl, reflect.TypeFor[model.Event](), mergeFuncs(testExtras()))
	switch {
	case execErr == nil && checkErr != nil:
		t.Fatalf("%s: the executor accepts this template, the checker refuses it: %v", src, checkErr)
	case execErr != nil && wantCheck:
		t.Fatalf("%s: the fixture cannot execute this template: %v", src, execErr)
	}
}

// mergeFuncs is the parser's and the checker's one function set.
func mergeFuncs(extra template.FuncMap) template.FuncMap {
	funcs := CoreFuncs()
	for name, fn := range extra {
		funcs[name] = fn
	}
	return funcs
}

// TestReviewCheckerAcceptsExecutorSemantics is the reviewer's counterexample:
// text/template's real semantics — method sets on every named type, a
// definition-local $, assignments across branches, int constants and ranges,
// and/or, string transforms — must never be refused. The fixture leaves
// WriterIdentity empty so the `or` case falls through to the time value exactly
// as the executor does; with a non-empty identity the case itself does not
// execute.
func TestReviewCheckerAcceptsExecutorSemantics(t *testing.T) {
	fixture := confEvent("")
	cases := map[string]string{
		"unknown method receiver":    `Backup {{ (or .Run.WriterIdentity .Run.StartedAt).Format "2006" }}`,
		"and key constant":           `Backup {{ if eq .Run.Outcome "delivered" }}{{ pick .Run.Outcome (and "unused" "delivered") "ok" }}{{ end }}`,
		"integer variable":           `Backup {{ $n := 1 }}{{ .Run.StartedAt.AddDate $n 0 0 }}`,
		"integer range":              `Backup {{ range 1 }}{{ $.Run.StartedAt.AddDate . 0 0 }}{{ end }}`,
		"duration string":            `Backup {{ .Run.Duration.String }}`,
		"duration standard":          `Backup {{ .Run.Duration.Std.Hours }}`,
		"run kind method":            `Backup {{ .Run.Kind.Valid }}`,
		"exclusion method":           `Backup {{ range .Run.Excluded }}{{ .Kind.Known }}{{ end }}`,
		"pointer receiver":           `Backup {{ .Run.StartedAt.Location.String }}`,
		"template dollar":            `Backup {{ define "artifact" }}{{ $.Artifact.Name }}{{ end }}{{ range .Run.Artifacts }}{{ template "artifact" . }}{{ end }}`,
		"assignment after if":        `Backup {{ $x := .Run.RunID }}{{ if true }}{{ $x = .Run.StartedAt }}{{ end }}{{ date $x }}`,
		"assignment opposite branch": `Backup {{ $x := .Run.StartedAt }}{{ if .Run.WriterIdentity }}{{ $x = .Run.RunID }}{{ else }}{{ date $x }}{{ end }}`,
		"slice erases the enum":      `Backup {{ pick (slice .Run.Outcome 0 3) "del" "ok" "deg" "warning" "fai" "failed" }}`,
		"pointer method set":         `Backup {{ .Run.StartedAt.Location.String }}`,
		"pointer method niladic":     `Backup {{ .Run.Lease.HolderKind.Valid }}`,
	}
	for name, src := range cases {
		t.Run(name, func(t *testing.T) {
			checkAgainstExecutor(t, fixture, src, true)
			if err := CheckTemplate(parseTest(t, src, testExtras()), reflect.TypeFor[model.Event](), mergeFuncs(testExtras())); err != nil {
				t.Fatalf("working template refused: %v", err)
			}
		})
	}
}

// TestNoDataAliasCannotHideData: an endpoint variable bound from the dot or $
// still names no data, in every definition.
func TestNoDataAliasCannotHideData(t *testing.T) {
	funcs := template.FuncMap{"secret": func(string) (string, error) { return "", nil }}
	refused := []string{
		`{{ $p := . }}{{ $p.X }}`,
		`{{ with . }}{{ .X }}{{ end }}`,
		`{{ $p := . }}{{ printf "%s" $p.X }}`,
		`{{ range . }}{{ .X }}{{ end }}`,
	}
	for _, src := range refused {
		if err := CheckTemplate(parseTest(t, src, funcs), nil, funcs); err == nil {
			t.Errorf("%s passed the no-data check", src)
		}
	}
	accepted := []string{`{{ . }}`, `{{ $ }}`, `{{ printf "%s" . }}`}
	for _, src := range accepted {
		if err := CheckTemplate(parseTest(t, src, funcs), nil, funcs); err != nil {
			t.Errorf("%s: printing the empty dot was refused: %v", src, err)
		}
	}
}
