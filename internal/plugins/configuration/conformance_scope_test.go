package configuration

import (
	"reflect"
	"strings"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
)

// TestRefusalsAreCertain: every CERTAIN refusal names its class and every
// evaluation of the refused node fails — the template errors on the zero value
// of its static type, on a filled value and behind a pointer.
func TestRefusalsAreCertain(t *testing.T) {
	funcs := mergeCore(testExtras())
	cases := map[string]string{
		"a field name":             `Backup {{ .Run.NoSuch }}`,
		"a root field name":        `Backup {{ .NoSuch }}`,
		"a pick without pairs":     `Backup {{ pick "x" }}`,
		"a pick with one pair":     `Backup {{ pick "x" "x" }}`,
		"a pickdef without a pair": `Backup {{ pickdef "x" "d" "y" }}`,
	}
	for name, src := range cases {
		t.Run(name, func(t *testing.T) {
			tmpl := parseTest(t, src, testExtras())
			err := CheckTemplate(tmpl, reflect.TypeFor[model.Event](), funcs)
			if err == nil || !strings.Contains(err.Error(), "certain:") {
				t.Fatalf("%s = %v, want a CERTAIN refusal", src, err)
			}
			filled := fix2Event("writer")
			for label, data := range map[string]any{
				"zero": model.Event{}, "filled": filled, "pointer": &filled,
			} {
				var out strings.Builder
				if executeErr := tmpl.Execute(&out, data); executeErr == nil {
					t.Fatalf("%s: the refusal is not certain: %s value executed", src, label)
				}
			}
		})
	}
}

// TestEnumKeyPolicy is the declared P2 policy: a string-constant pick key on a
// direct registry field outside its list is refused although the executor can
// run the template; the reviewer's Run.Kind case is the witness.
func TestEnumKeyPolicy(t *testing.T) {
	funcs := mergeCore(testExtras())
	refused := map[string]string{
		"an outcome typo":                 `{{ pick .Run.Outcome "degarded" "x" }}`,
		"the reviewer's case":             `{{ pick .Run.Kind "" "empty" "scheduled" "s" "manual" "m" "typo" "unused" }}`,
		"an artifact typo":                `{{ range .Run.Artifacts }}{{ pick .Outcome "nope" "x" }}{{ end }}`,
		"a storage typo":                  `{{ range .Run.Storages }}{{ pick .Outcome "nope" "x" }}{{ end }}`,
		"a pickdef field typo":            `{{ pickdef .Run.Outcome "d" "degarded" "x" }}`,
		"an exclusion typo":               `{{ range .Run.Excluded }}{{ pick .Kind "nope" "x" }}{{ end }}`,
		"a piped pick key":                `{{ "text" | pick .Run.Kind "typo" }}`,
		"a piped pick earlier key":        `{{ "text" | pick .Run.Kind "typo" "x" "manual" }}`,
		"a piped pick last key":           `{{ "text" | pick .Run.Kind "scheduled" "s" "typo" }}`,
		"a piped pickdef key":             `{{ "text" | pickdef .Run.Kind "d" "typo" }}`,
		"a piped pickdef earlier key":     `{{ "text" | pickdef .Run.Kind "d" "typo" "x" "manual" }}`,
		"a piped pickdef last key":        `{{ "text" | pickdef .Run.Kind "d" "scheduled" "s" "typo" }}`,
		"an explicit pick earlier key":    `{{ pick .Run.Kind "typo" "x" "manual" "m" }}`,
		"an explicit pick last key":       `{{ pick .Run.Kind "scheduled" "s" "typo" "unused" }}`,
		"an explicit pickdef earlier key": `{{ pickdef .Run.Kind "d" "typo" "x" "manual" "m" }}`,
		"an explicit pickdef last key":    `{{ pickdef .Run.Kind "d" "scheduled" "s" "typo" "unused" }}`,
	}
	for name, src := range refused {
		t.Run(name, func(t *testing.T) {
			err := CheckTemplate(parseTest(t, src, testExtras()), reflect.TypeFor[model.Event](), funcs)
			if err == nil || !strings.Contains(err.Error(), "policy P2:") {
				t.Fatalf("%s = %v, want a P2 refusal", src, err)
			}
		})
	}
	// The reviewer's witness runs in the executor.
	witness := `{{ pick .Run.Kind "" "empty" "scheduled" "s" "manual" "m" "typo" "unused" }}`
	tmpl := parseTest(t, witness, testExtras())
	var out strings.Builder
	if err := tmpl.Execute(&out, fix2Event("writer")); err != nil {
		t.Fatalf("the P2 witness does not execute: %v", err)
	}
	accepted := []string{
		`{{ pick .Run.Outcome "delivered" "x" "degraded" "y" "failed" "z" "running" "w" }}`,
		`{{ pick .Run.Kind "scheduled" "s" }}`,
		`{{ "text" | pick .Run.Kind "scheduled" }}`,
		`{{ "text" | pickdef .Run.Kind "d" "scheduled" }}`,
		`{{ $v := .Run.Kind }}{{ pick $v "typo" "x" }}`,
		`{{ pick (printf "%s" .Run.Kind) "typo" "x" }}`,
		`{{ range .Run.Artifacts }}{{ $o := .Outcome }}{{ pick $o "nope" "x" }}{{ end }}`,
	}
	for _, src := range accepted {
		if err := CheckTemplate(parseTest(t, src, testExtras()), reflect.TypeFor[model.Event](), funcs); err != nil {
			t.Errorf("%s: refused outside P2: %v", src, err)
		}
	}
}

// TestFieldSiteAndRuntimeJudges is I9: the render judges a direct field by its
// field list, a typed enum variable by its runtime type, and nothing through a
// string-building function.
func TestFieldSiteAndRuntimeJudges(t *testing.T) {
	degraded := fix2Event("writer")
	degraded.Run.Outcome = model.OutcomeDegraded
	degraded.Run.Artifacts = append(degraded.Run.Artifacts, model.ArtifactResult{
		Artifact: model.Artifact{Name: "failed"}, Outcome: model.ArtifactFailed,
	})
	failed := fix2Event("writer")
	failed.Run.Outcome = model.OutcomeFailed
	cases := map[string]struct {
		src  string
		data model.Event
		want bool // true refuses
	}{
		"a direct artifact outcome omits failed": {
			src:  `{{ range .Run.Artifacts }}{{ pick .Outcome "ok" "x" }}{{ end }}`,
			data: degraded, want: true,
		},
		"a variable from the plain string field is not judged": {
			src:  `{{ range .Run.Artifacts }}{{ $o := .Outcome }}{{ pick $o "ok" "x" }}{{ end }}`,
			data: degraded,
		},
		"a variable from the typed run outcome is judged": {
			src:  `{{ $v := .Run.Outcome }}{{ pick $v "delivered" "x" }}`,
			data: failed, want: true,
		},
		"printf erases the runtime type": {
			src:  `{{ pick (printf "%s" .Run.Outcome) "delivered" "x" }}`,
			data: failed,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			cfg := struct {
				T Template `json:"t"`
			}{}
			raw, err := mustMarshalJSON(t, map[string]string{"t": tc.src})
			if err != nil {
				t.Fatal(err)
			}
			if err := Decode(raw, &cfg); err != nil {
				t.Fatal(err)
			}
			_, err = cfg.T.RenderJudged(reflect.TypeFor[model.Event](), tc.data)
			if (err != nil) != tc.want {
				t.Fatalf("%s = %v, want refusal %t", tc.src, err, tc.want)
			}
		})
	}
}
