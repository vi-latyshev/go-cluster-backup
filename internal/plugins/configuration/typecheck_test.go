package configuration

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"text/template"
	"time"

	json "encoding/json/v2"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
)

// parseTest parses one template with the core function set and any extra
// functions a case needs.
func parseTest(t *testing.T, src string, extra template.FuncMap) *template.Template {
	t.Helper()
	tmpl, err := template.New("t").Option("missingkey=error").Funcs(mergeCore(extra)).Parse(src)
	if err != nil {
		t.Fatalf("%q does not parse: %v", src, err)
	}
	return tmpl
}

// mergeCore merges extra functions into the core set.
func mergeCore(extra template.FuncMap) template.FuncMap {
	funcs := CoreFuncs()
	for name, fn := range extra {
		funcs[name] = fn
	}
	return funcs
}

// testExtras is the plugin-level function set a webhook template has.
func testExtras() template.FuncMap {
	return template.FuncMap{
		"blank": func() string { return "" },
		"pickdef": func(v any, def string, pairs ...any) (string, error) {
			if len(pairs)%2 != 0 {
				return "", fmt.Errorf("pickdef: %d key/text arguments; it takes pairs", len(pairs))
			}
			for i := 0; i < len(pairs); i += 2 {
				if fmt.Sprint(pairs[i]) == fmt.Sprint(v) {
					return fmt.Sprint(pairs[i+1]), nil
				}
			}
			return def, nil
		},
	}
}

// TestCheckTemplateAcceptsWhatExecutes: every form the narrowed checker leaves
// alone is accepted — calls of any signature, builtins, comparisons, numbers,
// and/or, variables, maps, interface values and definitions.
func TestCheckTemplateAcceptsWhatExecutes(t *testing.T) {
	cases := []string{
		`{{ .Run.Outcome }}`,
		`{{ range .Run.Warnings }}{{ .Code }}{{ .Message }}{{ end }}`,
		`{{ range .Run.Artifacts }}{{ .Artifact.Name }}{{ .Outcome }}{{ end }}`,
		`{{ with .Run.Lease }}{{ .HolderKind }}{{ .Error }}{{ end }}`,
		`{{ if .Run.WriterIdentity }}{{ .Run.Lease.Holder }}{{ end }}`,
		`{{ pick .Run.Outcome "delivered" "x" }}`,
		`{{ pickdef .Run.Outcome "d" "delivered" "x" }}`,
		`{{ pick (printf "%s" .Run.Outcome) "delivered" "x" }}`,
		`{{ printf .Run.Outcome }}{{ printf .Run.Kind }}`,
		`{{ bytes .Run.RunID }}{{ bytes .Run.ExitCode }}`,
		`{{ eq .Run.Outcome "delivered" }}{{ lt "a" 1 }}{{ eq .Run.StartedAt .Run.ExitCode }}`,
		`{{ and .Run.WriterIdentity .Run.StartedAt }}`,
		`{{ index .Run.Artifacts 0 "x" }}{{ slice .Run.ExitCode }}`,
		`{{ $x := .Run.Outcome }}{{ $x = .Run.Kind }}{{ $x }}`,
		`{{ $a := .Run.RunID }}{{ $b := .Run.Kind }}{{ $a }}{{ $b }}`,
		`{{ define "artifact" }}{{ .Artifact.Name }}{{ end }}`,
		`{{ define "artifact" }}{{ .Artifact.Name }}{{ end }}{{ range .Run.Artifacts }}{{ template "artifact" . }}{{ end }}`,
		`{{ block "b" .Run }}blocked{{ end }}`,
		`{{ range $i, $a := .Run.Artifacts }}{{ $i }}{{ $a.Artifact.Name }}{{ end }}`,
		`{{ range .Run.ExitCode }}{{ printf "%d" . }}{{ end }}`,
		`{{ range .Run.Duration }}{{ printf "%v" . }}{{ end }}`,
		`{{ .Run.Duration.String }}{{ .Run.Kind.Valid }}`,
		`{{ range .Run.Artifacts }}{{ .Duration.UnmarshalJSON nil }}{{ end }}`,
		`{{ .Run.StartedAt.Location.String }}`,
	}
	funcs := mergeCore(testExtras())
	for _, src := range cases {
		if err := CheckTemplate(parseTest(t, src, testExtras()), reflect.TypeFor[model.Event](), funcs); err != nil {
			t.Errorf("%s: a working template was refused: %v", src, err)
		}
	}
}

// TestCheckTemplateRefusesEveryNameTypo is the red list: a name that names
// nothing on a certain type is refused in every branch and in a called
// definition.
func TestCheckTemplateRefusesEveryNameTypo(t *testing.T) {
	cases := map[string]string{
		"a run warning code":   `{{ range .Run.Warnings }}{{ .Cod }}{{ end }}`,
		"an artifact warning":  `{{ range .Run.Artifacts }}{{ range .Warnings }}{{ .Cod }}{{ end }}{{ end }}`,
		"a retention error":    `{{ range .Run.Storages }}{{ if .RetentionError }}{{ .RetentionErr }}{{ end }}{{ end }}`,
		"a writer identity":    `{{ with .Run.WriterIdentity }}{{ $.Run.WriterIdentiy }}{{ end }}`,
		"a lease error":        `{{ with .Run.Lease }}{{ if .Error }}{{ .Eror }}{{ end }}{{ end }}`,
		"a lease holder error": `{{ with .Run.Lease }}{{ if .HolderKind }}{{ .Eror }}{{ end }}{{ end }}`,
		"an exclusion node":    `{{ range .Run.Excluded }}{{ if .Node }}{{ .Nod }}{{ end }}{{ end }}`,
		"a called define":      `{{ define "d" }}{{ .Run.Outcom }}{{ end }}{{ template "d" . }}`,
		"a dead branch":        `{{ if false }}{{ .Run.NoSuchField }}{{ end }}Backup`,
		"a field on a slice":   `{{ .Run.Artifacts.Name }}`,
		"an unexported field":  `{{ .Run.StartedAt.wall }}`,
	}
	for name, src := range cases {
		t.Run(name, func(t *testing.T) {
			if err := CheckTemplate(parseTest(t, src, nil), reflect.TypeFor[model.Event](), CoreFuncs()); err == nil {
				t.Fatalf("%s passed the checker", src)
			}
		})
	}
}

// TestCheckTemplateLeavesUnknownsAlone is the scope boundary: names below an
// unknown static type are never judged — a reassigned or redeclared variable,
// and/or, a builtin result, a function result, an interface, a map, or an
// uncalled definition's dot.
func TestCheckerScopeResiduals(t *testing.T) {
	cases := map[string]string{
		"a reassigned variable":      `{{ $x := .Run.RunID }}{{ $x = .Run.StartedAt }}{{ $x.NoSuch }}`,
		"a redeclared variable":      `{{ $x := .Run.RunID }}{{ $x := 1 }}{{ $x.NoSuch }}`,
		"an and result":              `{{ (and .Run.RunID .Run.Kind).NoSuch }}`,
		"an or result":               `{{ (or .Run.RunID .Run.Kind).NoSuch }}`,
		"an index result":            `{{ (index .Run.Artifacts 0).NoSuch }}`,
		"a slice result":             `{{ (slice .Run.RunID).NoSuch }}`,
		"a builtin result":           `{{ (len .Run.RunID).NoSuch }}`,
		"a function result":          `{{ (printf "%s" .Run.RunID).NoSuch }}`,
		"a variable from a function": `{{ $x := printf "%s" .Run.RunID }}{{ $x.NoSuch }}`,
		"a map key":                  `{{ .Run.StartedAt }}{{ (index .Run.Artifacts 0).Artifact.Name }}`,
		"an uncalled definition":     `{{ define "d" }}{{ .NoSuch }}{{ end }}`,
	}
	for name, src := range cases {
		t.Run(name, func(t *testing.T) {
			if err := CheckTemplate(parseTest(t, src, nil), reflect.TypeFor[model.Event](), CoreFuncs()); err != nil {
				t.Fatalf("%s: the checker judged an unknown position: %v", src, err)
			}
		})
	}
}

// TestCheckTemplateMapNeverRefused: a name on a map is a key, never a defect.
func TestCheckTemplateMapNeverRefused(t *testing.T) {
	type root struct {
		Labels map[string]string
		At     time.Time
	}
	for _, src := range []string{`{{ .Labels.app }}`, `{{ .Labels.Nope }}`} {
		if err := CheckTemplate(parseTest(t, src, nil), reflect.TypeFor[root](), CoreFuncs()); err != nil {
			t.Errorf("%s: %v", src, err)
		}
	}
	if err := CheckTemplate(parseTest(t, `{{ .At.wall }}`, nil), reflect.TypeFor[root](), CoreFuncs()); err == nil {
		t.Error("an unexported field passed the checker")
	}
}

// TestCheckTemplatePickRules: (c1) parity for every pick and pickdef, explicit
// or piped, in every branch and definition; a dead branch or an uncalled
// definition reports it under P1.
func TestCheckTemplatePickRules(t *testing.T) {
	refused := map[string]string{
		"a pick without pairs":     `{{ pick "x" }}`,
		"a pick with one pair":     `{{ pick "x" "x" }}`,
		"an odd pair count":        `{{ pick "x" "a" "b" "c" }}`,
		"a pickdef without a pair": `{{ pickdef "x" "d" "y" }}`,
		"a piped pick":             `{{ "x" | pick }}`,
		"a dead-branch pair count": `{{ if false }}{{ pick "x" "x" }}{{ end }}`,
		"an uncalled pair count":   `{{ define "d" }}{{ pick "x" "x" }}{{ end }}`,
	}
	for name, src := range refused {
		t.Run(name, func(t *testing.T) {
			if err := CheckTemplate(parseTest(t, src, testExtras()), reflect.TypeFor[model.Event](), mergeCore(testExtras())); err == nil {
				t.Fatalf("%s passed the checker", src)
			}
		})
	}
	accepted := []string{
		`{{ pick .Run.Outcome "delivered" "x" "degraded" "y" }}`,
		`{{ pickdef .Run.Outcome "d" "delivered" "x" }}`,
		`{{ pickdef .Run.Outcome "d" }}`,
		`{{ pick .Run.Outcome "" "x" }}`,
		`{{ .Run.RunID | pick "run" "x" }}`,
	}
	for _, src := range accepted {
		if err := CheckTemplate(parseTest(t, src, testExtras()), reflect.TypeFor[model.Event](), mergeCore(testExtras())); err != nil {
			t.Errorf("%s: %v", src, err)
		}
	}
}

// TestCheckTemplateNoDataMode: an endpoint executes with no data, so every
// selector is refused in every definition; printing the dot and secret calls
// stay accepted.
func TestCheckTemplateNoDataMode(t *testing.T) {
	funcs := template.FuncMap{"secret": func(string) (string, error) { return "", nil }}
	refused := []string{
		`{{ .X }}`,
		`{{ $.X }}`,
		`{{ $p := secret "/p" }}{{ $p.X }}`,
		`{{ if .X }}{{ end }}`,
		`{{ define "d" }}{{ .X }}{{ end }}`,
		`{{ (secret "/p").Missing }}`,
		`{{ $p := . }}{{ $p.X }}`,
		`{{ with . }}{{ .X }}{{ end }}`,
	}
	for _, src := range refused {
		if err := CheckTemplate(parseTest(t, src, funcs), nil, mergeCore(funcs)); err == nil {
			t.Errorf("%s passed the no-data check", src)
		}
	}
	accepted := []string{
		`{{ printf "%s" . }}`,
		`{{ $ }}`,
		`{{ secret "/p" }}`,
		`{{ define "x" }}{{ . }}{{ end }}{{ template "x" (secret "/p") }}`,
	}
	for _, src := range accepted {
		if err := CheckTemplate(parseTest(t, src, funcs), nil, mergeCore(funcs)); err != nil {
			t.Errorf("%s: a working endpoint template was refused: %v", src, err)
		}
	}
}

// TestCheckTemplateReportsEveryDefinition: an uncalled definition still has its
// parity checked, with an unknown dot; its names are not.
func TestCheckTemplateReportsEveryDefinition(t *testing.T) {
	src := `{{ define "a" }}{{ pick "x" "x" }}{{ end }}{{ define "b" }}{{ .NoSuch }}{{ end }}`
	err := CheckTemplate(parseTest(t, src, testExtras()), reflect.TypeFor[model.Event](), mergeCore(testExtras()))
	if err == nil {
		t.Fatal("an uncalled definition's parity passed")
	}
	if !strings.Contains(err.Error(), "a:") || strings.Contains(err.Error(), "b:") {
		t.Fatalf("the refusal does not name exactly the broken definition: %v", err)
	}
}

// TestEnumListsAreWellFormed: every list names values and a producer, and no
// value repeats.
func TestEnumListsAreWellFormed(t *testing.T) {
	lists := map[string][]model.EnumValue{
		"Outcome": model.OutcomeValues, "RunKind": model.RunKindValues,
		"ArtifactOutcome": model.ArtifactOutcomeValues, "StorageOutcome": model.StorageOutcomeValues,
		"ExclusionKind": model.ExclusionKindValues, "Importance": model.ImportanceValues,
		"Sensitivity": model.SensitivityValues,
	}
	for name, values := range lists {
		if len(values) == 0 {
			t.Errorf("%s has no values", name)
		}
		seen := map[string]bool{}
		for _, value := range values {
			if value.Value == "" {
				t.Errorf("%s has an empty value", name)
			}
			if seen[value.Value] {
				t.Errorf("%s lists %q twice", name, value.Value)
			}
			seen[value.Value] = true
			if value.Producer == "" {
				t.Errorf("%s value %q has no producer", name, value.Value)
			}
		}
	}
}

// TestEveryReachableEnumIsListed: every reachable struct field whose type is a
// named model string type has a field-registry entry, every entry names such a
// field, and the two plain-string outcome fields are registered with string
// types.
func TestEveryReachableEnumIsListed(t *testing.T) {
	seen := map[reflect.Type]bool{}
	fields := map[reflect.Type]map[string]reflect.Type{}
	var walk func(reflect.Type)
	walk = func(t reflect.Type) {
		for t.Kind() == reflect.Pointer || t.Kind() == reflect.Slice || t.Kind() == reflect.Array {
			t = t.Elem()
		}
		if seen[t] {
			return
		}
		seen[t] = true
		switch t.Kind() {
		case reflect.Map:
			walk(t.Key())
			walk(t.Elem())
		case reflect.Struct:
			if t.PkgPath() != reflect.TypeFor[model.Event]().PkgPath() {
				return
			}
			fields[t] = map[string]reflect.Type{}
			for i := 0; i < t.NumField(); i++ {
				f := t.Field(i)
				if !f.IsExported() {
					continue
				}
				fields[t][f.Name] = f.Type
				walk(f.Type)
			}
		}
	}
	walk(reflect.TypeFor[model.Event]())
	for owner, byName := range fields {
		for name, typ := range byName {
			named := typ.Kind() == reflect.String && typ.PkgPath() == reflect.TypeFor[model.Event]().PkgPath()
			plainOutcome := (owner.Name() == "ArtifactResult" || owner.Name() == "StorageResult") && name == "Outcome"
			_, registered := enumRegistry[owner][name]
			if named && !registered {
				t.Errorf("%s.%s is a named string type but has no enum registry entry", owner, name)
			}
			if registered && !named && !plainOutcome {
				t.Errorf("%s.%s is registered but is neither a named string field nor a plain outcome string", owner, name)
			}
			if registered && typ.Kind() != reflect.String {
				t.Errorf("%s.%s is registered but is not a string field", owner, name)
			}
		}
	}
	for owner, byName := range enumRegistry {
		for name := range byName {
			if _, ok := fields[owner][name]; !ok {
				t.Errorf("registry entry %s.%s names no reachable field", owner, name)
			}
		}
	}
	for _, pair := range [][2]string{{"ArtifactResult", "Outcome"}, {"StorageResult", "Outcome"}} {
		found := false
		for owner, byName := range fields {
			if owner.Name() == pair[0] {
				if typ, ok := byName[pair[1]]; ok && typ.Kind() == reflect.String {
					found = true
				}
			}
		}
		if !found {
			t.Errorf("%s.%s must stay a plain string field", pair[0], pair[1])
		}
	}
}

// mustMarshalJSON encodes one value for Decode in a test.
func mustMarshalJSON(t *testing.T, v any) ([]byte, error) {
	t.Helper()
	return json.Marshal(v)
}
