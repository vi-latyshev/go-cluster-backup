package configuration

import (
	"reflect"
	"strings"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
)

func TestReviewFix6UnknownFieldContext(t *testing.T) {
	helper := `{{ define "d" }}{{ pick .Outcome (print "ok") "O" (print "failed") "F" }}{{ end }}`
	known := `{{ range .Run.Artifacts }}{{ template "d" . }}{{ end }}`
	unknown := []string{
		`{{ template "d" (or .Run .Run) }}`,
		`{{ template "d" (and .Run .Run) }}`,
		`{{ $r := .Run }}{{ $r = .Run }}{{ template "d" $r }}`,
	}
	event := model.Event{Run: model.RunReport{Outcome: model.OutcomeDelivered, Artifacts: []model.ArtifactResult{{Outcome: model.ArtifactOK}}}}
	for _, call := range unknown {
		for _, calls := range []string{known + call, call + known} {
			tmpl := parseTest(t, "Backup "+helper+calls, nil)
			var original strings.Builder
			if err := tmpl.Execute(&original, event); err == nil {
				t.Fatal("original accepted omitted run outcome")
			}
			judged, err := JudgeTemplate(tmpl, reflect.TypeFor[model.Event](), CoreFuncs())
			if err != nil {
				t.Fatal(err)
			}
			var out strings.Builder
			if err := judged.Execute(&out, event); err == nil {
				t.Errorf("required runtime enum outcome was missed: %s => %q", calls, out.String())
			}
		}
	}
}

func TestReviewFix6DeploymentDependentConflict(t *testing.T) {
	helper := `{{ define "d" }}{{ pick .Kind (print "scheduled") "S" (print "ignored") "I" }}{{ end }}`
	run := `{{ template "d" .Run }}`
	excluded := `{{ range .Run.Excluded }}{{ template "d" . }}{{ end }}`
	event := model.Event{Run: model.RunReport{Kind: model.RunKindManual, Excluded: []model.Exclusion{{Kind: model.ExclusionCovered}}}}
	for _, calls := range []string{run + excluded, excluded + run} {
		tmpl := parseTest(t, "Backup "+helper+calls, nil)
		judged, err := JudgeTemplate(tmpl, reflect.TypeFor[model.Event](), CoreFuncs())
		if err != nil {
			t.Fatal(err)
		}
		var out strings.Builder
		if err := judged.Execute(&out, event); err != nil {
			t.Errorf("deployment-dependent values became required: %s: %v", calls, err)
		}
	}
}
