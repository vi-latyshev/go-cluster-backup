package configuration

import (
	"reflect"
	"strings"
	"testing"
	"text/template"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
)

// TestReviewFix3EmptyRangeElse is the reviewer's counterexample: an empty range
// keeps the pipeline value in its declared variable for the else branch.
func TestReviewFix3EmptyRangeElse(t *testing.T) {
	event := model.Event{}
	src := `Backup {{ range $a := .Run.Artifacts }}{{ else }}{{ len $a }}{{ end }}`
	tmpl := parseTest(t, src, nil)
	var out strings.Builder
	if err := tmpl.Execute(&out, event); err != nil {
		t.Fatalf("runtime: %v", err)
	}
	if err := CheckTemplate(tmpl, reflect.TypeFor[model.Event](), CoreFuncs()); err != nil {
		t.Fatalf("working template refused: %v", err)
	}
}

// TestReviewFix3InterfaceMethod: an interface method's signature carries no
// receiver, so a niladic method call on an error value is accepted.
func TestReviewFix3InterfaceMethod(t *testing.T) {
	event := model.Event{Run: model.RunReport{Artifacts: []model.ArtifactResult{{Duration: model.Duration(time.Second)}}}}
	src := `Backup {{ range .Run.Artifacts }}{{ (.Duration.UnmarshalJSON nil).Error }}{{ end }}`
	tmpl := parseTest(t, src, nil)
	var out strings.Builder
	if err := tmpl.Execute(&out, event); err != nil {
		t.Fatalf("runtime: %v", err)
	}
	if err := CheckTemplate(tmpl, reflect.TypeFor[model.Event](), CoreFuncs()); err != nil {
		t.Fatalf("working template refused: %v", err)
	}
}

// TestReviewFix3Accepted is the acceptance half of the third review: an early
// true pair of eq, the range-else value, both hex prefixes, short-circuited
// and/or operands and an interface method all execute and are accepted.
func TestReviewFix3Accepted(t *testing.T) {
	funcs := generatedConformanceFuncs()
	event := fix2Event("writer")
	cases := map[string]string{
		"eq returns on an early pair":       `Backup {{ eq "x" "x" 1 }}`,
		"eq returns on an early field pair": `Backup {{ eq .Run.RunID .Run.RunID 1 }}`,
		"uppercase hex":                     `Backup {{ eq 0XFE 254 }}`,
		"lowercase hex":                     `Backup {{ eq 0xFE 254 }}`,
		"hex binding":                       `Backup {{ $n := 0XFE }}{{ printf "%d" $n }}`,
		"short-circuit and":                 `Backup {{ and false 18446744073709551615 }}`,
		"short-circuit or":                  `Backup {{ or true 18446744073709551615 }}`,
		"interface method":                  `Backup {{ define "d" }}{{ (.Duration.UnmarshalJSON nil).Error }}{{ end }}{{ range .Run.Artifacts }}{{ template "d" . }}{{ end }}`,
		"range else keeps the pipeline":     `Backup {{ range $a := .Run.Artifacts }}{{ $a }}{{ else }}{{ len $a }}{{ end }}`,
	}
	for name, src := range cases {
		t.Run(name, func(t *testing.T) {
			tmpl, err := template.New("fix3").Option("missingkey=error").Funcs(funcs).Parse(src)
			if err != nil {
				t.Fatalf("%s: %v", src, err)
			}
			var out strings.Builder
			if err := tmpl.Execute(&out, event); err != nil {
				t.Fatalf("%s: runtime: %v", src, err)
			}
			if err := CheckTemplate(tmpl, reflect.TypeFor[model.Event](), funcs); err != nil {
				t.Fatalf("%s: working template refused: %v", src, err)
			}
		})
	}
}

// carryFix2Cases is the executor-accepted matrix the second review carried: the
// forms a working deployment can write.
func carryFix2Cases() map[string]string {
	return map[string]string{
		"duration compared to a literal":            `Backup {{ gt .Run.Duration 0 }}`,
		"duration compared to an int":               `Backup {{ eq .Run.Duration .Run.ExitCode }}`,
		"int compared to a literal":                 `Backup {{ eq .Run.ExitCode 1 }}`,
		"int ordered":                               `Backup {{ lt .Run.ExitCode 2 }}`,
		"nil slice":                                 `Backup {{ eq .Run.Artifacts nil }}`,
		"nil pointer":                               `Backup {{ eq .Run.StartedAt.Location nil }}`,
		"comparable structs":                        `Backup {{ eq .Run.StartedAt .Run.StartedAt }}`,
		"named strings":                             `Backup {{ eq .Run.Kind .Run.Kind }}`,
		"assignment after if":                       `Backup {{ $x := .Run.RunID }}{{ if true }}{{ $x = .Run.StartedAt }}{{ end }}{{ date $x }}`,
		"assignment opposite branch":                `Backup {{ $x := .Run.StartedAt }}{{ if .Run.WriterIdentity }}{{ $x = .Run.RunID }}{{ else }}{{ date $x }}{{ end }}`,
		"range assignment":                          `Backup {{ $x := "a" }}{{ range $x = .Run.Artifacts }}{{ $x.Artifact.Name }}{{ end }}{{ printf "%s" $x }}`,
		"range two assignments":                     `Backup {{ $i := 0 }}{{ $v := "a" }}{{ range $i, $v = .Run.Artifacts }}{{ $i }}{{ $v.Artifact.Name }}{{ end }}{{ printf "%d %s" $i $v }}`,
		"dollar reassignment":                       `Backup {{ $ = .Run }}{{ $.RunID }}`,
		"integer range":                             `Backup {{ range .Run.ExitCode }}{{ printf "%d" . }}{{ end }}`,
		"duration methods":                          `Backup {{ .Run.Duration.String }}{{ .Run.Duration.Std.Hours }}`,
		"named enum methods":                        `Backup {{ .Run.Kind.Valid }}{{ range .Run.Excluded }}{{ .Kind.Known }}{{ end }}`,
		"addressable pointer receiver":              `Backup {{ range .Run.Artifacts }}{{ .Duration.UnmarshalJSON .Duration.MarshalJSON }}{{ end }}`,
		"pointer receiver through a pointer target": `Backup {{ .Run.StartedAt.Location.String }}`,
	}
}

// TestReviewCarryFix2ExecutorAccepted is the carried accepted matrix, executed
// and checked row by row.
func TestReviewCarryFix2ExecutorAccepted(t *testing.T) {
	funcs := generatedConformanceFuncs()
	event := fix2Event("writer")
	for name, src := range carryFix2Cases() {
		t.Run(name, func(t *testing.T) {
			tmpl, err := template.New("carry").Option("missingkey=error").Funcs(funcs).Parse(src)
			if err != nil {
				t.Fatalf("%v", err)
			}
			var out strings.Builder
			if err := tmpl.Execute(&out, event); err != nil {
				t.Fatalf("runtime: %v", err)
			}
			if err := CheckTemplate(tmpl, reflect.TypeFor[model.Event](), funcs); err != nil {
				t.Fatalf("working template refused: %v", err)
			}
		})
	}
}

// TestReviewFix2ExecutorAccepted keeps the earlier review's accepted matrix
// under the name it was reported with.
func TestReviewFix2ExecutorAccepted(t *testing.T) {
	funcs := generatedConformanceFuncs()
	event := fix2Event("writer")
	for name, src := range carryFix2Cases() {
		t.Run(name, func(t *testing.T) {
			tmpl, err := template.New("fix2").Option("missingkey=error").Funcs(funcs).Parse(src)
			if err != nil {
				t.Fatalf("%v", err)
			}
			var out strings.Builder
			if err := tmpl.Execute(&out, event); err != nil {
				t.Fatalf("runtime: %v", err)
			}
			if err := CheckTemplate(tmpl, reflect.TypeFor[model.Event](), funcs); err != nil {
				t.Fatalf("working template refused: %v", err)
			}
		})
	}
}
