package configuration

import (
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
	"text/template"
	"text/template/parse"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
)

// silentMethod has a method with no results: a name the executor resolves, so
// the checker may not refuse it, and its type stays unknown.
type silentMethod struct{}

func (silentMethod) Do() {}

// TestCertaintySources pins the static sources the checker may trust: a
// struct field's declared type, a range element, a with value, a variable
// declared once with no assignment, $ and a called definition's dot.
func TestCertaintySources(t *testing.T) {
	certain := []string{
		`{{ .Run.RunID.NoSuch }}`,
		`{{ with .Run }}{{ .RunID.NoSuch }}{{ end }}`,
		`{{ range .Run.Artifacts }}{{ .NoSuch }}{{ end }}`,
		`{{ range .Run.Warnings }}{{ .NoSuch }}{{ end }}`,
		`{{ $x := .Run }}{{ $x.RunID.NoSuch }}`,
		`{{ define "d" }}{{ .RunID.NoSuch }}{{ end }}{{ template "d" .Run }}`,
	}
	for _, src := range certain {
		if err := CheckTemplate(parseTest(t, src, nil), reflect.TypeFor[model.Event](), CoreFuncs()); err == nil {
			t.Errorf("%s: a certain type did not refuse a missing name", src)
		}
	}
}

// TestRangeTypes pins the element and index types the certainty list grants.
func TestRangeTypes(t *testing.T) {
	if elem, index := rangeTypes(reflect.TypeFor[[]model.ArtifactResult]()); elem != reflect.TypeFor[model.ArtifactResult]() || index != reflect.TypeFor[int]() {
		t.Errorf("slice range = %v, %v", elem, index)
	}
	if elem, index := rangeTypes(reflect.TypeFor[map[string]int]()); elem != reflect.TypeFor[int]() || index != reflect.TypeFor[string]() {
		t.Errorf("map range = %v, %v", elem, index)
	}
	if elem, _ := rangeTypes(reflect.TypeFor[int64]()); elem != reflect.TypeFor[int64]() {
		t.Errorf("int range = %v", elem)
	}
	if elem, _ := rangeTypes(reflect.TypeFor[chan string]()); elem != reflect.TypeFor[string]() {
		t.Errorf("chan range = %v", elem)
	}
	if elem, _ := rangeTypes(reflect.TypeFor[time.Time]()); elem != nil {
		t.Errorf("a struct range = %v, want unknown", elem)
	}
	if elem, index := rangeTypes(reflect.PointerTo(reflect.TypeFor[[]int]())); elem != reflect.TypeFor[int]() || index != reflect.TypeFor[int]() {
		t.Errorf("a pointer-to-slice range = %v, %v", elem, index)
	}
}

// TestResolveName pins the executor's lookup on one static type: methods,
// exported fields, maps, interfaces and unknowns never refuse; a missing name
// on a certain struct or basic type does.
func TestResolveName(t *testing.T) {
	if typ, _, err := resolveName(reflect.TypeFor[model.RunReport](), "RunID"); typ != reflect.TypeFor[string]() || err != nil {
		t.Errorf("a struct field = %v, %v", typ, err)
	}
	if typ, _, err := resolveName(reflect.TypeFor[model.Duration](), "String"); typ != reflect.TypeFor[string]() || err != nil {
		t.Errorf("a value method = %v, %v", typ, err)
	}
	if typ, _, err := resolveName(reflect.TypeFor[model.Duration](), "UnmarshalJSON"); typ != reflect.TypeFor[error]() || err != nil {
		t.Errorf("a pointer method = %v, %v", typ, err)
	}
	if typ, _, err := resolveName(reflect.TypeFor[error](), "Error"); typ != nil || err != nil {
		t.Errorf("an interface method = %v, %v", typ, err)
	}
	if typ, _, err := resolveName(reflect.TypeFor[map[string]int](), "any"); typ != nil || err != nil {
		t.Errorf("a map key = %v, %v", typ, err)
	}
	if _, _, err := resolveName(reflect.TypeFor[model.RunReport](), "Nope"); err == nil {
		t.Error("a missing struct name passed")
	}
	if _, _, err := resolveName(reflect.TypeFor[time.Time](), "wall"); err == nil {
		t.Error("an unexported field passed")
	}
	if typ, _, err := resolveName(reflect.TypeFor[silentMethod](), "Do"); typ != nil || err != nil {
		t.Errorf("a method with no results = %v, %v; it must be accepted as unknown", typ, err)
	}
	if staticType(reflect.TypeFor[any]()) != nil {
		t.Error("an empty interface answered a static type")
	}
}

// TestFieldSelectorRewrite: the judge rewrite injects the receiver and field
// name for every field-chain shape and leaves every other first argument alone.
func TestFieldSelectorRewrite(t *testing.T) {
	funcs := mergeCore(testExtras())
	cases := map[string]string{
		"a root field":         `{{ with .Run }}{{ pick .Outcome "delivered" "x" }}{{ end }}`,
		"a nested field":       `{{ pick .Run.Outcome "delivered" "x" }}`,
		"a variable field":     `{{ $r := .Run }}{{ pick $r.Outcome "delivered" "x" }}`,
		"a chain field":        `{{ pick (.Run).Outcome "delivered" "x" }}`,
		"a longer chain field": `{{ pick (.Run).Duration.String "1s" "x" }}`,
		"a non-field value":    `{{ pick "x" "x" "y" }}`,
		"a whole variable":     `{{ $v := .Run.Kind }}{{ pick $v "scheduled" "s" }}`,
		"a field-less command": `{{ .Run }}`,
		"a non-pick command":   `{{ printf "%s" .Run.RunID }}`,
	}
	for name, src := range cases {
		t.Run(name, func(t *testing.T) {
			judged, err := JudgeTemplate(parseTest(t, src, funcs), reflect.TypeFor[model.Event](), funcs)
			if err != nil {
				t.Fatalf("%s: %v", src, err)
			}
			if err := judged.Execute(io.Discard, fix2Event("writer")); err != nil {
				t.Errorf("%s: %v", src, err)
			}
		})
	}
}

// judgeStruct is a receiver shape the registry does not know: B is a named
// enum, S a plain string.
type judgeStruct struct {
	B model.Outcome
	S string
}

// judgeBadMethod answers two non-error results: the executor refuses to call it.
type judgeBadMethod struct{}

func (judgeBadMethod) Bad() (int, int) { return 0, 0 }

// judgeErrMethod answers a non-nil error: the executor surfaces it.
type judgeErrMethod struct{}

func (judgeErrMethod) Fail() (string, error) { return "", errors.New("boom") }

// judgeValueMethod answers a reflect.Value: the executor unwraps it.
type judgeValueMethod struct{}

func (judgeValueMethod) RV() reflect.Value { return reflect.ValueOf("x") }

// judgeNilEmbed promotes a field through a nil embedded pointer: the executor's
// lookup reports the indirection.
type judgeNilEmbed struct{ *judgeStruct }

// TestJudgeFunctions pins the runtime judges: the runtime-type judge, the
// field judge resolving its selector on the runtime receiver, and the field
// pickdef judge.
func TestJudgeFunctions(t *testing.T) {
	if _, err := JudgedPick(model.OutcomeFailed, "delivered", "x"); err == nil {
		t.Error("a produced-everywhere enum value with no pair was accepted")
	}
	if got, err := JudgedPick(model.RunKindManual, "scheduled", "x"); err != nil || got != "x" {
		t.Fatalf("a deployment-dependent value = %q, %v", got, err)
	}
	if got, err := JudgedPick("failed", "ok", "x"); err != nil || got != "x" {
		t.Fatalf("an erased value = %q, %v", got, err)
	}
	artifact := model.ArtifactResult{Outcome: model.ArtifactFailed}
	if _, err := JudgedFieldPick(artifact, "Outcome", "ok", "x"); err == nil {
		t.Error("a runtime field judge accepted an omitted produced value")
	}
	if got, err := JudgedFieldPick(model.ArtifactResult{Outcome: "custom"}, "Outcome", "ok", "x"); err != nil || got != "x" {
		t.Fatalf("a runtime field judge on a value outside the list = %q, %v", got, err)
	}
	if got, err := JudgedFieldPick(&artifact, "Outcome", "failed", "x"); err != nil || got != "x" {
		t.Fatalf("a pointer receiver = %q, %v", got, err)
	}
	if _, err := JudgedFieldPick(judgeStruct{B: model.OutcomeDelivered}, "B", "ok", "x"); err == nil {
		t.Error("a non-registry field lost its runtime enum judge")
	}
	if got, err := JudgedFieldPick(judgeStruct{S: "custom"}, "S", "ok", "x"); err != nil || got != "x" {
		t.Fatalf("a non-enum runtime field = %q, %v", got, err)
	}
	if got, err := JudgedFieldPick(map[string]string{"Field": "custom"}, "Field", "ok", "x"); err != nil || got != "x" {
		t.Fatalf("a map receiver = %q, %v", got, err)
	}
	if got, err := JudgedFieldPick(model.Duration(0), "String", "0s", "x"); err != nil || got != "x" {
		t.Fatalf("a method receiver = %q, %v", got, err)
	}
	if got, err := JudgedFieldPickDef(&time.Time{}, "GobEncode", "default"); err != nil || got != "default" {
		t.Fatalf("a pointer method receiver = %q, %v", got, err)
	}
	if _, err := JudgedFieldPick(silentMethod{}, "Do", "x", "y"); err == nil {
		t.Error("a method with no result resolved")
	}
	if _, err := JudgedFieldPick(judgeBadMethod{}, "Bad", "x", "y"); err == nil {
		t.Error("a method with two non-error results resolved")
	}
	if _, err := JudgedFieldPick(judgeErrMethod{}, "Fail", "x", "y"); err == nil {
		t.Error("a method's error was swallowed")
	}
	if got, err := JudgedFieldPick(judgeValueMethod{}, "RV", "x", "y"); err != nil || got != "y" {
		t.Fatalf("a reflect.Value method result = %q, %v", got, err)
	}
	if _, err := JudgedFieldPick(judgeNilEmbed{}, "B", "x", "y"); err == nil {
		t.Error("an indirection through a nil embedded pointer resolved")
	}
	if _, err := JudgedFieldPick(map[string]string{}, "Field", "x", "y"); err == nil {
		t.Error("a missing map key resolved")
	}
	var nilLeaseNoField *model.LeaseState
	if _, err := JudgedFieldPick(nilLeaseNoField, "Nope", "x", "y"); err == nil {
		t.Error("a nil pointer without the field resolved")
	}
	if _, err := JudgedFieldPick(artifact, "Nope", "x", "y"); err == nil {
		t.Error("a missing field resolved")
	}
	var nilLease *model.LeaseState
	if _, err := JudgedFieldPick(nilLease, "HolderKind", "x", "y"); err == nil {
		t.Error("a nil pointer resolved")
	}
	if _, err := JudgedFieldPick(nil, "Outcome", "x", "y"); err == nil {
		t.Error("a nil receiver resolved")
	}
	if _, err := JudgedFieldPick(time.Time{}, "wall", "x", "y"); err == nil {
		t.Error("an unexported field resolved")
	}
	if _, err := JudgedFieldPick(artifact, "Outcome"); err == nil {
		t.Error("a runtime field judge accepted no pairs")
	}
	if _, err := JudgedFieldPick(artifact, "Outcome", "ok"); err == nil {
		t.Error("a runtime field judge accepted an odd pair count")
	}
	if got, err := JudgedFieldPickDef(model.ArtifactResult{Outcome: "lost"}, "Outcome", "default", "verified", "x"); err != nil || got != "default" {
		t.Fatalf("a field pickdef = %q, %v", got, err)
	}
	if got, err := JudgedFieldPickDef(artifact, "Outcome", "default", "failed", "x"); err != nil || got != "x" {
		t.Fatalf("a matching field pickdef = %q, %v", got, err)
	}
	if _, err := JudgedFieldPickDef(artifact, "Outcome", "default", "failed", "x", "orphan"); err == nil {
		t.Error("a field pickdef accepted odd pairs")
	}
	if _, err := JudgedFieldPickDef(nil, "Outcome", "default"); err == nil {
		t.Error("a field pickdef resolved a nil receiver")
	}
	if _, err := JudgedPick("x"); err == nil {
		t.Error("JudgedPick accepted no pairs")
	}
	if _, err := JudgedPick("x", "a"); err == nil {
		t.Error("JudgedPick accepted an odd pair count")
	}
}

// TestRenderJudgedLeavesTheOriginal: the validation copy is judged, and the
// original template still renders and still carries the run's own pick.
func TestRenderJudgedLeavesTheOriginal(t *testing.T) {
	cfg := struct {
		T Template `json:"t"`
	}{}
	if err := Decode([]byte(`{"t": "{{ pick .Outcome \"delivered\" \"x\" }}"}`), &cfg); err != nil {
		t.Fatal(err)
	}
	delivered := struct{ Outcome model.Outcome }{Outcome: model.OutcomeDelivered}
	if out, err := cfg.T.RenderJudged(reflect.TypeFor[struct{ Outcome model.Outcome }](), delivered); err != nil || out != "x" {
		t.Fatalf("the judged render = %q, %v", out, err)
	}
	if out, err := cfg.T.Render(delivered); err != nil || out != "x" {
		t.Fatalf("the original render = %q, %v", out, err)
	}
	if err := cfg.T.Check(reflect.TypeFor[struct{ Outcome model.Outcome }]()); err != nil {
		t.Errorf("a parsed template's check = %v", err)
	}
	bad := Template{tmpl: parseTest(t, `{{ pick "x" }}`, testExtras()), funcs: testExtras()}
	if _, err := bad.RenderJudged(reflect.TypeFor[model.Event](), nil); err == nil {
		t.Error("a template whose pick parity is wrong rendered")
	}
	if _, err := JudgeTemplate(bad.tmpl, reflect.TypeFor[model.Event](), bad.funcs); err == nil {
		t.Error("a template whose pick parity is wrong was judged")
	}
	var zero Template
	if err := zero.Check(reflect.TypeFor[model.Event]()); err != nil {
		t.Errorf("a zero template = %v", err)
	}
	if out, err := zero.RenderJudged(reflect.TypeFor[model.Event](), nil); err != nil || out != "" {
		t.Errorf("a zero template's judged render = %q, %v", out, err)
	}
	unparsed := Template{text: "x"}
	if _, err := unparsed.RenderJudged(reflect.TypeFor[model.Event](), nil); err == nil {
		t.Error("an unparsed template rendered")
	}
}

// TestJudgeTemplateKeepsTheOriginalTree: rewriting a field selector never
// mutates the template a run executes.
func TestJudgeTemplateKeepsTheOriginalTree(t *testing.T) {
	tmpl := parseTest(t, `{{ range .Run.Artifacts }}{{ pick .Outcome "ok" "x" }}{{ end }}{{ range .Run.Storages }}{{ pickdef .Outcome "d" }}{{ end }}`, testExtras())
	original := reflect.ValueOf(tmpl.Tree).Pointer()
	judged, err := JudgeTemplate(tmpl, reflect.TypeFor[model.Event](), mergeCore(testExtras()))
	if err != nil {
		t.Fatal(err)
	}
	if reflect.ValueOf(tmpl.Tree).Pointer() != original {
		t.Fatal("the original tree changed")
	}
	var out = &strings.Builder{}
	if err := judged.Execute(out, fix2Event("writer")); err != nil {
		t.Fatalf("the judged copy = %v", err)
	}
	if reflect.ValueOf(tmpl.Tree).Pointer() != original {
		t.Fatal("the original tree changed after render")
	}
}

// TestJudgeTemplateRewritesEveryNodeForm: the validation copy rewrites a pick
// wherever the grammar holds it — if, with, a template invocation's argument,
// a chain — and survives a template call with no argument.
func TestJudgeTemplateRewritesEveryNodeForm(t *testing.T) {
	funcs := mergeCore(testExtras())
	funcs["idAny"] = func(v any) any { return v }
	cases := []string{
		`{{ if true }}{{ pick "a" "a" "b" }}{{ end }}`,
		`{{ with .Run }}{{ pick "a" "a" "b" }}{{ end }}`,
		`{{ define "d" }}{{ pick "a" "a" "b" }}{{ end }}{{ template "d" "x" }}`,
		`{{ (idAny .Run).RunID }}`,
		`{{ define "e" }}{{ end }}{{ template "e" }}`,
	}
	for _, src := range cases {
		judged, err := JudgeTemplate(parseTest(t, src, funcs), reflect.TypeFor[model.Event](), funcs)
		if err != nil {
			t.Errorf("%s: %v", src, err)
			continue
		}
		if err := judged.Execute(io.Discard, fix2Event("writer")); err != nil {
			t.Errorf("%s: %v", src, err)
		}
	}
}

// TestJudgeTemplateHonoursAnUnparsedTemplate: a template with no parse tree —
// the associated empty one here — answers an empty judged copy instead of
// failing.
func TestJudgeTemplateHonoursAnUnparsedTemplate(t *testing.T) {
	root := template.New("root")
	if _, err := root.AddParseTree("empty", nil); err != nil {
		t.Fatal(err)
	}
	judged, err := JudgeTemplate(root, reflect.TypeFor[model.Event](), template.FuncMap{})
	if err != nil {
		t.Fatalf("an unparsed template = %v", err)
	}
	if judged == nil {
		t.Fatal("no judged copy")
	}
}

// TestCheckTemplateWalksRepeatedAndUnknownCalls: one definition called twice is
// walked once, and a call to a name with no definition is accepted — the
// executor refuses it at render, which is no certain type defect.
func TestCheckTemplateWalksRepeatedAndUnknownCalls(t *testing.T) {
	src := `{{ define "d" }}{{ .RunID }}{{ end }}{{ template "d" .Run }}{{ template "d" .Run }}{{ template "missing" }}{{ printf "%s" blank }}`
	if err := CheckTemplate(parseTest(t, src, testExtras()), reflect.TypeFor[model.Event](), mergeCore(testExtras())); err != nil {
		t.Fatalf("%v", err)
	}
}

// TestCheckTemplateNestedPipelineErrors: a name defect inside a with, a range,
// a template argument and a chain is reported, not lost.
func TestCheckTemplateNestedPipelineErrors(t *testing.T) {
	for _, src := range []string{
		`{{ with .Nope }}{{ end }}`,
		`{{ range .Nope }}{{ end }}`,
		`{{ define "d" }}{{ end }}{{ template "d" .Nope }}`,
		`{{ (.Nope).X }}`,
	} {
		if err := CheckTemplate(parseTest(t, src, testExtras()), reflect.TypeFor[model.Event](), mergeCore(testExtras())); err == nil {
			t.Errorf("%s: the nested defect was accepted", src)
		}
	}
}

// TestCheckerDirectFallbacks pins the branches no production parse tree can
// reach: an unknown node, a nil pipeline, an unknown argument node and an empty
// selector chain.
func TestCheckerDirectFallbacks(t *testing.T) {
	c := &checker{funcs: template.FuncMap{}}
	if _, err := c.node(&parse.IdentifierNode{Ident: "x"}, scope{vars: map[string]binding{}}); err != nil {
		t.Errorf("an unknown node = %v", err)
	}
	if out, err := c.pipeline(nil, scope{}); out.typ != nil || err != nil {
		t.Errorf("a nil pipeline = %+v, %v", out, err)
	}
	if out, err := c.arg(&parse.TextNode{Text: []byte("x")}, scope{}); out.typ != nil || err != nil {
		t.Errorf("an unknown argument = %+v, %v", out, err)
	}
	if out, err := c.selector(scope{}, nil, reflect.TypeFor[string]()); out.typ != reflect.TypeFor[string]() || err != nil {
		t.Errorf("an empty selector chain = %+v, %v", out, err)
	}
	if _, _, ok := fieldSelector(&parse.FieldNode{}); ok {
		t.Error("an empty field chain qualified")
	}
	if _, _, ok := fieldSelector(&parse.ChainNode{}); ok {
		t.Error("an empty chain qualified")
	}
}
