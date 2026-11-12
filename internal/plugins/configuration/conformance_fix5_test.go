package configuration

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
)

func TestReviewFix5Certainty(t *testing.T) {
	event := model.Event{Run: model.RunReport{RunID: "run", StartedAt: time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC), Artifacts: []model.ArtifactResult{{Artifact: model.Artifact{Name: "snapshot"}}}}}
	cases := map[string]string{
		"dollar assigned in branch":         `Backup {{ if true }}{{ $ = .Run.StartedAt }}{{ end }}{{ $.Format "2006" }}`,
		"range variable assigned in branch": `Backup {{ range $a := .Run.Artifacts }}{{ if true }}{{ $a = $.Run.StartedAt }}{{ end }}{{ $a.Format "2006" }}{{ end }}`,
		"nested pipeline assignment":        `Backup {{ $x := .Run.RunID }}{{ not ($x = .Run.StartedAt) }}{{ $x.Format "2006" }}`,
		"range else index binding":          `Backup {{ define "d" }}{{ range . }}{{ .Artifact.Name }}{{ end }}{{ end }}{{ range $i, $a := .Run.Artifacts }}{{ else }}{{ template "d" $i }}{{ end }}`,
	}
	for name, src := range cases {
		t.Run(name, func(t *testing.T) {
			data := event
			if name == "range else index binding" {
				data.Run.Artifacts = nil
			}
			tmpl := parseTest(t, src, nil)
			var out strings.Builder
			if err := tmpl.Execute(&out, data); err != nil {
				t.Fatalf("runtime: %v", err)
			}
			if err := CheckTemplate(tmpl, reflect.TypeFor[model.Event](), CoreFuncs()); err != nil {
				t.Fatalf("working template refused: %v", err)
			}
		})
	}
}

func TestReviewFix5PipedEnumKeys(t *testing.T) {
	for _, src := range []string{
		`{{ "text" | pick .Run.Kind "typo" }}`,
		`{{ "text" | pickdef .Run.Kind "default" "typo" }}`,
	} {
		if err := CheckTemplate(parseTest(t, src, testExtras()), reflect.TypeFor[model.Event](), mergeCore(testExtras())); err == nil {
			t.Errorf("P2 missed explicit enum key: %s", src)
		}
	}
}

// TestFix5CertaintySweep holds the assignment rule across the measured
// dimensions: branch and parenthesized assignment, non-empty and empty range,
// body and else, and a called definition.
func TestFix5CertaintySweep(t *testing.T) {
	funcs := mergeCore(testExtras())
	cases := []string{
		`Backup {{ define "d" }}{{ $x := .RunID }}{{ if true }}{{ $x = $.StartedAt }}{{ end }}{{ $x.Format "2006" }}{{ end }}{{ template "d" .Run }}`,
		`Backup {{ range $a := .Run.Artifacts }}{{ $a = $.Run.StartedAt }}{{ else }}{{ $a.Format "2006" }}{{ end }}`,
		`Backup {{ range $a := .Run.Artifacts }}{{ $a = $.Run.StartedAt }}{{ $a.Format "2006" }}{{ end }}`,
		`Backup {{ $x := .Run.RunID }}{{ not ($x = .Run.StartedAt) }}{{ $x.Format "2006" }}`,
	}
	for _, src := range cases {
		if err := CheckTemplate(parseTest(t, src, funcs), reflect.TypeFor[model.Event](), funcs); err != nil {
			t.Errorf("%s: the assigned variable stayed certain: %v", src, err)
		}
	}
}

func TestReviewFix5PolymorphicFieldJudge(t *testing.T) {
	helper := `{{ define "d" }}{{ pick .Outcome (print "running") "R" (print "delivered") "D" (print "degraded") "W" (print "failed") "F" }}{{ end }}`
	calls := []string{
		`{{ range .Run.Artifacts }}{{ template "d" . }}{{ end }}{{ template "d" .Run }}`,
		`{{ template "d" .Run }}{{ range .Run.Artifacts }}{{ template "d" . }}{{ end }}`,
	}
	event := model.Event{Run: model.RunReport{Outcome: model.OutcomeDelivered, Artifacts: []model.ArtifactResult{{Outcome: model.ArtifactOK}}}}
	for _, call := range calls {
		tmpl := parseTest(t, "Backup "+helper+call, nil)
		var original strings.Builder
		if err := tmpl.Execute(&original, event); err == nil {
			t.Fatal("original accepted omitted artifact outcome")
		}
		judged, err := JudgeTemplate(tmpl, reflect.TypeFor[model.Event](), CoreFuncs())
		if err != nil {
			t.Fatal(err)
		}
		var out strings.Builder
		if err := judged.Execute(&out, event); err == nil {
			t.Errorf("field judge missed required artifact outcome for call order %s: %q", call, out.String())
		}
	}
}
