package configuration

import (
	"reflect"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
)

// TestReviewFix4Accepted is the fourth review's accepted matrix at the checker
// level. The narrowed scope judges names and pick parity only, so calls,
// arity, argument types, comparisons and variables with a lost static type are
// accepted; the 2.2.0 sample render still refuses the ones that cannot run.
func TestReviewFix4Accepted(t *testing.T) {
	funcs := mergeCore(testExtras())
	cases := []string{
		`{{ eq .Run.Kind "scheduled" 1 }}`,
		`{{ eq .Run.ExitCode "x" }}`,
		`{{ printf "%d" "x" }}`,
		`{{ printf }}`,
		`{{ bytes }}`,
		`{{ duration 1 2 }}`,
		`{{ lt .Run.StartedAt .Run.Duration }}`,
		`{{ and .Run.RunID 1 }}`,
		`{{ or 0 .Run.Artifacts }}`,
		`{{ index .Run.Artifacts "x" }}`,
		`{{ slice .Run.RunID 1.5 }}`,
		`{{ len .Run.ExitCode }}`,
		`{{ $x := .Run }}{{ $x = .Run.Kind }}{{ $x.NoSuch }}`,
	}
	for _, src := range cases {
		tmpl := parseTest(t, src, funcs)
		if err := CheckTemplate(tmpl, reflect.TypeFor[model.Event](), funcs); err != nil {
			t.Errorf("%s: the checker judged more than its scope: %v", src, err)
		}
	}
}

// fix4AnyMethod answers an empty interface, so no name below its result is
// certain.
type fix4AnyMethod interface {
	Any() any
}

// fix4DynamicRoot carries the interface, slice-of-interface and map shapes a
// template may walk without a certain field list.
type fix4DynamicRoot struct {
	Value  any
	Error  error
	Method fix4AnyMethod
	Items  []any
	Map    map[string]any
}

// TestReviewFix4DynamicInterface: a name below an interface, a map or an
// interface result is never refused — the dynamic value decides at render.
func TestReviewFix4DynamicInterface(t *testing.T) {
	funcs := mergeCore(testExtras())
	cases := []string{
		`{{ .Value.NoSuch }}`,
		`{{ .Error.NoSuch }}`,
		`{{ .Error.Error.NoSuch }}`,
		`{{ .Method.Any.NoSuch }}`,
		`{{ range .Items }}{{ .NoSuch }}{{ end }}`,
		`{{ .Map.anykey.NoSuch }}`,
	}
	for _, src := range cases {
		tmpl := parseTest(t, src, funcs)
		if err := CheckTemplate(tmpl, reflect.TypeFor[fix4DynamicRoot](), funcs); err != nil {
			t.Errorf("%s: a dynamic value was refused: %v", src, err)
		}
	}
}
