package configuration

import (
	"reflect"
	"strings"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
)

func TestReviewFix7SelectorParentheses(t *testing.T) {
	event := model.Event{Run: model.RunReport{Artifacts: []model.ArtifactResult{{Outcome: model.ArtifactOK}}, Storages: []model.StorageResult{{Outcome: model.StorageVerified}}}}
	cases := []string{
		`Backup {{ range .Run.Artifacts }}{{ pick (.Outcome) "failed" "F" }}{{ end }}`,
		`Backup {{ range .Run.Artifacts }}{{ pick ((.Outcome)) "failed" "F" }}{{ end }}`,
		`Backup {{ range $a := .Run.Artifacts }}{{ pick ($a.Outcome) "failed" "F" }}{{ end }}`,
		`Backup {{ range .Run.Artifacts }}{{ "F" | pick (.Outcome) "failed" }}{{ end }}`,
		`Backup {{ range .Run.Storages }}{{ pick (.Outcome) "failed" "F" }}{{ end }}`,
		`Backup {{ range .Run.Storages }}{{ pick ((.Outcome)) "failed" "F" }}{{ end }}`,
	}
	for _, src := range cases {
		tmpl := parseTest(t, src, nil)
		var original strings.Builder
		if err := tmpl.Execute(&original, event); err == nil {
			t.Fatal("original accepted omitted required outcome")
		}
		judged, err := JudgeTemplate(tmpl, reflect.TypeFor[model.Event](), CoreFuncs())
		if err != nil {
			t.Fatal(err)
		}
		var out strings.Builder
		if err := judged.Execute(&out, event); err == nil {
			t.Errorf("parentheses erased the field judge: %s => %q", src, out.String())
		}
	}
}

// TestFix7SelectorMatrix holds the M8/M9 rules across report owners,
// parenthesis depth, receiver forms and both pick functions: a pure selector
// keeps its field judge inside any transparent parentheses, and an effectful
// receiver keeps the template's own call and its single evaluation.
func TestFix7SelectorMatrix(t *testing.T) {
	event := model.Event{Run: model.RunReport{
		Outcome:   model.OutcomeDelivered,
		Artifacts: []model.ArtifactResult{{Outcome: model.ArtifactOK}},
		Storages:  []model.StorageResult{{Outcome: model.StorageVerified}},
	}}
	funcs := mergeCore(testExtras())
	refused := []string{
		`{{ pick (.Run.Outcome) "running" "R" "failed" "F" }}`,
		`{{ pick ((.Run.Outcome)) "running" "R" "failed" "F" }}`,
		`{{ range .Run.Artifacts }}{{ pick (.Outcome) "failed" "F" }}{{ end }}`,
		`{{ range $a := .Run.Artifacts }}{{ pick (($a.Outcome)) "failed" "F" }}{{ end }}`,
		`{{ range .Run.Storages }}{{ pick (.Outcome) "failed" "F" }}{{ end }}`,
		`{{ "F" | pick (.Run.Outcome) "running" "R" "failed" }}`,
	}
	for _, src := range refused {
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
			t.Errorf("%s: the field judge was lost => %q", src, out.String())
		}
	}
	accepted := []string{
		`{{ pick (.Run.Outcome) "delivered" "D" }}`,
		`{{ pickdef ((.Run.Outcome)) "default" "delivered" "D" }}`,
		`{{ $e := . }}{{ pick ($e.Run.Outcome) "delivered" "D" }}`,
		`{{ $r := . }}{{ pick ($r = $r.Run).Outcome "delivered" "D" }}`,
		`{{ $r := . }}{{ "x" | pickdef ($r = $r.Run).Outcome "default" "delivered" }}`,
	}
	for _, src := range accepted {
		tmpl := parseTest(t, src, funcs)
		var original strings.Builder
		if err := tmpl.Execute(&original, event); err != nil {
			t.Fatalf("%s: original: %v", src, err)
		}
		judged, err := JudgeTemplate(tmpl, reflect.TypeFor[model.Event](), funcs)
		if err != nil {
			t.Fatalf("%s: %v", src, err)
		}
		var out strings.Builder
		if err := judged.Execute(&out, event); err != nil {
			t.Errorf("%s: %v", src, err)
		}
	}
}

func TestReviewFix7SingleReceiverEvaluation(t *testing.T) {
	event := model.Event{Run: model.RunReport{Outcome: model.OutcomeDelivered}}
	funcs := mergeCore(testExtras())
	cases := []string{
		`Backup {{ pick ($ = $.Run).Outcome "running" "R" "delivered" "D" "degraded" "G" "failed" "F" }}`,
		`Backup {{ $r := . }}{{ pick ($r = $r.Run).Outcome "running" "R" "delivered" "D" "degraded" "G" "failed" "F" }}`,
		`Backup {{ $r := . }}{{ pickdef ($r = $r.Run).Outcome "default" "delivered" "D" }}`,
		`Backup {{ $r := . }}{{ "F" | pick ($r = $r.Run).Outcome "running" "R" "delivered" "D" "degraded" "G" "failed" }}`,
		`Backup {{ $r := . }}{{ pick ($r = $r.Run).StartedAt.Location.String "UTC" "U" }}`,
	}
	for _, src := range cases {
		tmpl := parseTest(t, src, funcs)
		var original strings.Builder
		if err := tmpl.Execute(&original, event); err != nil {
			t.Fatalf("original: %v", err)
		}
		judged, err := JudgeTemplate(tmpl, reflect.TypeFor[model.Event](), funcs)
		if err != nil {
			t.Fatal(err)
		}
		var out strings.Builder
		if err := judged.Execute(&out, event); err != nil {
			t.Errorf("receiver was evaluated twice: %s: %v", src, err)
		}
	}
}
