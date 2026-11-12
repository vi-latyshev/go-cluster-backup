package configuration

import (
	"reflect"
	"strings"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
)

// TestFix8ComputedReceiverMatrix holds the uniform judge across receiver
// forms, report owners, required and optional values and both pick functions:
// a required value is refused through the receiver's own registry field, an
// optional value keeps the tolerant fallback, and pickdef keeps its default.
func TestFix8ComputedReceiverMatrix(t *testing.T) {
	event := model.Event{Run: model.RunReport{
		Kind:     model.RunKindManual,
		Outcome:  model.OutcomeDelivered,
		Excluded: []model.Exclusion{{Kind: model.ExclusionCovered}},
		Artifacts: []model.ArtifactResult{{
			Outcome:  model.ArtifactOK,
			Artifact: model.Artifact{Importance: model.ImportanceBestEffort, Sensitivity: model.SensitivitySecret},
		}},
		Storages: []model.StorageResult{{Outcome: model.StorageVerified}},
	}}
	funcs := mergeCore(testExtras())
	required := []string{
		`{{ pick (or .Run .Run).Outcome "running" "R" "failed" "F" }}`,
		`{{ pick (index .Run.Artifacts 0).Outcome "failed" "F" }}`,
		`{{ range .Run.Storages }}{{ pick (and . .).Outcome "failed" "F" }}{{ end }}`,
		`{{ "R" | pick (or .Run .Run).Outcome "running" }}`,
		`{{ $e := . }}{{ pick ($e = $e).Run.Outcome "running" "R" "failed" "F" }}`,
	}
	for _, src := range required {
		tmpl := parseTest(t, src, funcs)
		var original strings.Builder
		if err := tmpl.Execute(&original, event); err == nil {
			t.Errorf("%s: the original accepted an omitted required value", src)
		}
		judged, err := JudgeTemplate(tmpl, reflect.TypeFor[model.Event](), funcs)
		if err != nil {
			t.Fatalf("%s: %v", src, err)
		}
		var out strings.Builder
		if err := judged.Execute(&out, event); err == nil {
			t.Errorf("%s: the required value became optional => %q", src, out.String())
		}
	}
	optional := []string{
		`{{ pick (or .Run .Run).Kind "scheduled" "S" }}`,
		`{{ pick (and .Run .Run).Kind "scheduled" "S" }}`,
		`{{ range .Run.Excluded }}{{ pick (or . .).Kind "ignored" "I" }}{{ end }}`,
		`{{ pick (index .Run.Artifacts 0).Artifact.Importance "required" "R" }}`,
		`{{ pick (index .Run.Artifacts 0).Artifact.Sensitivity "plain" "P" }}`,
		`{{ $r := . }}{{ pick ($r = $r.Run).Kind "scheduled" "S" }}`,
		`{{ pickdef (or .Run .Run).Kind "default" "scheduled" "S" }}`,
		`{{ "x" | pickdef (or .Run .Run).Kind "default" "scheduled" }}`,
		`{{ pick (.Run.Duration.String) "0s" "zero" }}`,
	}
	for _, src := range optional {
		tmpl := parseTest(t, src, funcs)
		judged, err := JudgeTemplate(tmpl, reflect.TypeFor[model.Event](), funcs)
		if err != nil {
			t.Fatalf("%s: %v", src, err)
		}
		var out strings.Builder
		if err := judged.Execute(&out, event); err != nil {
			t.Errorf("%s: a non-Everywhere value became required: %v", src, err)
		}
	}
}

func TestReviewFix8ComputedReceiverPolicy(t *testing.T) {
	event := model.Event{Run: model.RunReport{
		Kind:      model.RunKindManual,
		Excluded:  []model.Exclusion{{Kind: model.ExclusionCovered}},
		Artifacts: []model.ArtifactResult{{Artifact: model.Artifact{Importance: model.ImportanceBestEffort, Sensitivity: model.SensitivitySecret}}},
	}}
	cases := []string{
		`Backup {{ pick (or .Run .Run).Kind "scheduled" "S" }}`,
		`Backup {{ pick (and .Run .Run).Kind "scheduled" "S" }}`,
		`Backup {{ range .Run.Excluded }}{{ pick (or . .).Kind "ignored" "I" }}{{ end }}`,
		`Backup {{ pick (index .Run.Artifacts 0).Artifact.Importance "required" "R" }}`,
		`Backup {{ pick (index .Run.Artifacts 0).Artifact.Sensitivity "plain" "P" }}`,
	}
	for _, src := range cases {
		tmpl := parseTest(t, src, nil)
		judged, err := JudgeTemplate(tmpl, reflect.TypeFor[model.Event](), CoreFuncs())
		if err != nil {
			t.Fatal(err)
		}
		var out strings.Builder
		if err := judged.Execute(&out, event); err != nil {
			t.Errorf("non-Everywhere value became required: %s: %v", src, err)
		}
	}
}
