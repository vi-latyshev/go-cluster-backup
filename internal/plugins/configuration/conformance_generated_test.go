package configuration

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"text/template"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
)

// fix2Event is the deterministic event the positive corpus executes.
func fix2Event(writerIdentity string) model.Event {
	return model.Event{Run: model.RunReport{
		Kind: model.RunKindScheduled, RunID: "run", StartedAt: time.Now(),
		Duration: model.Duration(time.Second), Outcome: model.OutcomeDelivered,
		WriterIdentity: writerIdentity,
		Artifacts: []model.ArtifactResult{{
			Artifact: model.Artifact{Name: "snapshot"}, Outcome: model.ArtifactOK,
			Duration: model.Duration(time.Second),
		}},
		Excluded: []model.Exclusion{{Kind: model.ExclusionIgnored}},
		Warnings: []model.Warning{{Code: "a.warning", Message: "a warning"}},
		Lease:    &model.LeaseState{HolderKind: model.RunKindManual},
	}}
}

// acceptedByExecutor holds the positive-conformance obligation: a template the
// executor runs without error on a fixture set that evaluates every node is
// accepted by the checker. P2 dead keys are the declared exception and have
// their own test.
func acceptedByExecutor(t *testing.T, funcs template.FuncMap, event model.Event, src string) {
	t.Helper()
	tmpl, err := template.New("generated").Option("missingkey=error").Funcs(funcs).Parse(src)
	if err != nil {
		t.Fatalf("%s: the generated template does not parse: %v", src, err)
	}
	var out strings.Builder
	if err := tmpl.Execute(&out, event); err != nil {
		return // the positive side judges only templates the executor runs
	}
	if err := CheckTemplate(tmpl, reflect.TypeFor[model.Event](), funcs); err != nil {
		t.Errorf("%s: executor OK, checker refuses: %v", src, err)
	}
}

// generatedConformanceFuncs is the corpus function set.
func generatedConformanceFuncs() template.FuncMap {
	funcs := CoreFuncs()
	funcs["pickdef"] = func(any, string, ...any) (string, error) { return "", nil }
	funcs["bytes"] = funcBytes
	funcs["date"] = funcDate
	return funcs
}

// TestGeneratedConformance is the positive half of I3: a broad generated corpus
// — every reachable path, functions, builtins in argument and pipeline
// positions, comparisons, numeric forms, variables (single, redeclared,
// assigned), with/range/else, define/template/block, and pick/pickdef with
// every first-argument shape — is accepted whenever the executor runs it. The
// corpus carries no dead names, so the P1 rule never fires here.
func TestGeneratedConformance(t *testing.T) {
	funcs := generatedConformanceFuncs()
	fixture := fix2Event("writer")

	// Function and builtin positions; comparisons and numeric syntax.
	values := []string{"0", "1", "-1", "1.0", "1.5", "1e3", "1i", "0xFE", "0XFE", "0b1010", "0o17", "1_000", `'a'`, `"s"`, "nil",
		".Run.ExitCode", ".Run.Duration", ".Run.Kind", ".Run.StartedAt", ".Run.Outcome", ".Run.RunID", ".Run.Artifacts", ".Run.Lease"}
	for _, value := range values {
		for _, src := range []string{
			fmt.Sprintf("Backup {{ bytes %s }}", value),
			fmt.Sprintf("Backup {{ %s | printf \"%%v\" }}", value),
			fmt.Sprintf("Backup {{ len %s }}", value),
			fmt.Sprintf("Backup {{ index %s 0 }}", value),
			fmt.Sprintf("Backup {{ eq %s %s }}", value, value),
			fmt.Sprintf("Backup {{ ne %s 2 }}", value),
			fmt.Sprintf("Backup {{ and %s 1 }}", value),
			fmt.Sprintf("Backup {{ or %s 1 }}", value),
		} {
			acceptedByExecutor(t, funcs, fixture, src)
		}
	}
	for _, op := range []string{"eq", "ne", "lt", "le", "gt", "ge"} {
		for _, pair := range [][2]string{
			{".Run.ExitCode", "1"}, {".Run.Duration", "0"}, {"0xFE", "254"}, {`"a"`, `"b"`},
		} {
			acceptedByExecutor(t, funcs, fixture, fmt.Sprintf("Backup {{ %s %s %s }}", op, pair[0], pair[1]))
		}
	}

	// Variables: single, redeclared, assigned; with/range/else/else-with.
	for _, src := range []string{
		`{{ $x := .Run.RunID }}{{ $x }}`,
		`{{ $x := .Run.RunID }}{{ $x = .Run.StartedAt }}{{ $x }}`,
		`{{ $x := .Run.RunID }}{{ $x := .Run.Kind }}{{ $x }}`,
		`{{ $a := .Run.RunID }}{{ $b := .Run.Kind }}{{ $a }}{{ $b }}`,
		`{{ with .Run }}{{ .RunID }}{{ else }}{{ "empty" }}{{ end }}`,
		`{{ range .Run.Artifacts }}{{ .Artifact.Name }}{{ else }}{{ "empty" }}{{ end }}`,
		`{{ range $a := .Run.Artifacts }}{{ $a.Artifact.Name }}{{ else }}{{ len $a }}{{ end }}`,
		`{{ range $i, $a := .Run.Artifacts }}{{ $i }}{{ $a.Artifact.Name }}{{ end }}`,
		`{{ define "d" }}{{ .RunID }}{{ end }}{{ template "d" .Run }}`,
		`{{ block "b" .Run }}{{ .RunID }}{{ end }}`,
	} {
		acceptedByExecutor(t, funcs, fixture, "Backup "+src)
	}

	// pick/pickdef with every first-argument shape and non-dead keys.
	for _, src := range []string{
		`{{ pick .Run.Outcome "delivered" "x" "degraded" "y" "failed" "z" "running" "w" }}`,
		`{{ pick .Run.Kind "scheduled" "s" "manual" "m" }}`,
		`{{ with .Run.Outcome }}{{ pick . "delivered" "x" }} {{ end }}`,
		`{{ $v := .Run.Outcome }}{{ pick $v "delivered" "x" }}`,
		`{{ pick (printf "%s" .Run.Outcome) "delivered" "x" }}`,
		`{{ pick (and .Run.Outcome "delivered") "delivered" "x" }}`,
		`{{ range .Run.Artifacts }}{{ pick .Outcome "ok" "x" }} {{ end }}`,
		`{{ range .Run.Storages }}{{ pick .Outcome "verified" "x" }} {{ end }}`,
		`{{ pickdef .Run.Outcome "d" "delivered" "x" }}`,
		`{{ pickdef .Run.Outcome "d" }}`,
	} {
		acceptedByExecutor(t, funcs, fixture, "Backup "+src)
	}
}

// TestGeneratedRangeKeepsTheElementType: the corpus covers a range over int64
// and a named integer with a small value.
func TestGeneratedRangeKeepsTheElementType(t *testing.T) {
	type smallIntRoot struct {
		N int64
		M model.Duration
	}
	root := smallIntRoot{N: 2, M: model.Duration(2)}
	funcs := generatedConformanceFuncs()
	for _, src := range []string{
		`{{ range .N }}{{ printf "%T" . }}{{ end }}`,
		`{{ range .M }}{{ printf "%T" . }}{{ end }}`,
	} {
		tmpl, err := template.New("r").Option("missingkey=error").Funcs(funcs).Parse(src)
		if err != nil {
			t.Fatal(err)
		}
		var out strings.Builder
		if err := tmpl.Execute(&out, root); err != nil {
			t.Fatalf("%s: %v", src, err)
		}
		if err := CheckTemplate(tmpl, reflect.TypeFor[smallIntRoot](), funcs); err != nil {
			t.Fatalf("%s: %v", src, err)
		}
	}
}

// TestReviewFix2AddressableMethod: a slice element is addressable, so the
// executor finds pointer-receiver methods on a named value.
func TestReviewFix2AddressableMethod(t *testing.T) {
	event := model.Event{Run: model.RunReport{Artifacts: []model.ArtifactResult{{Duration: model.Duration(time.Second)}}}}
	src := `Backup {{ range .Run.Artifacts }}{{ .Duration.UnmarshalJSON .Duration.MarshalJSON }}{{ end }}`
	tmpl := parseTest(t, src, nil)
	var out strings.Builder
	if err := tmpl.Execute(&out, event); err != nil {
		t.Fatalf("runtime: %v", err)
	}
	if err := CheckTemplate(tmpl, reflect.TypeFor[model.Event](), CoreFuncs()); err != nil {
		t.Fatalf("working template refused: %v", err)
	}
}

// TestReviewFix2NoDataSelectors: every selector in the endpoint mode is refused
// by the mode, whatever marker a binding, or, index or definition lost.
func TestReviewFix2NoDataSelectors(t *testing.T) {
	cases := []string{
		`{{ $p := . }}{{ $p = . }}{{ $p.X }}`,
		`{{ (or . "text").X }}`,
		`{{ (index . 0).X }}`,
		`{{ with (or . "text") }}{{ .X }}{{ end }}`,
		`{{ define "d" }}{{ $p := . }}{{ $p = . }}{{ $p.X }}{{ end }}`,
	}
	for _, src := range cases {
		if err := CheckTemplate(parseTest(t, src, nil), nil, CoreFuncs()); err == nil {
			t.Errorf("%s passed endpoint no-data contract", src)
		}
	}
}
