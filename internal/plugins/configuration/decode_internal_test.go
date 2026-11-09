package configuration

import (
	"reflect"
	"strings"
	"testing"
	"text/template"

	"encoding/json/jsontext"
)

// TestParseTemplatesLeavesANonAddressableTemplateAlone: a Template that is not
// addressable cannot be finalized in place, and the walk reports nothing rather
// than panicking on Addr. reflect.ValueOf yields such a value; a struct reached
// through an interface field is the other way to get one.
func TestParseTemplatesLeavesANonAddressableTemplateAlone(t *testing.T) {
	errs := parseTemplates(reflect.ValueOf(Template{text: "{{ .X }}"}), template.FuncMap{}, "/t")
	if len(errs) != 0 {
		t.Fatalf("parseTemplates = %v, want none", errs)
	}
}

// TestMissingSkipsWhatTheDecoderSkipsAndEntersMapStructs: a field the decoder
// ignores is not required, a map of structs has its elements' fields required,
// and a wrong-shaped value is left to the strict decoder.
func TestMissingSkipsWhatTheDecoderSkipsAndEntersMapStructs(t *testing.T) {
	type inner struct {
		A string `json:"a"`
	}
	type ignored struct {
		Hidden     string `json:"-"`
		NoTag      string
		Unexported string
	}
	if errs := missing(reflect.TypeFor[ignored](), jsontext.Value(`{}`), "/h"); errs != nil {
		t.Errorf("missing required a field the decoder ignores: %v", errs)
	}
	type holder struct {
		M map[string]inner `json:"m"`
	}
	errs := missing(reflect.TypeFor[holder](), jsontext.Value(`{"m":{"x":{}}}`), "/h")
	if len(errs) != 1 || !strings.Contains(errs[0].Error(), "/h/m/x/a") {
		t.Errorf("missing did not enter a map of structs: %v", errs)
	}
	if errs := missing(reflect.TypeFor[holder](), jsontext.Value(`{"m":[]}`), "/h"); errs != nil {
		t.Errorf("missing walked a wrong-shaped map: %v", errs)
	}
	if errs := missing(reflect.TypeFor[holder](), jsontext.Value(`{"m":{"x":[]}}`), "/h"); errs != nil {
		t.Errorf("missing walked a wrong-shaped element: %v", errs)
	}
	// A map of scalars has nothing to require.
	type scalars struct {
		M map[string]string `json:"m"`
	}
	if errs := missing(reflect.TypeFor[scalars](), jsontext.Value(`{"m":{"a":"x"}}`), "/s"); errs != nil {
		t.Errorf("missing required a member of a map of scalars: %v", errs)
	}
}

// TestNullsSkipsWhatTheDecoderSkipsAndWalksEveryShape.
func TestNullsSkipsWhatTheDecoderSkipsAndWalksEveryShape(t *testing.T) {
	type ignored struct {
		Hidden     string `json:"-"`
		NoTag      string
		Unexported string
	}
	if errs := nulls(reflect.TypeFor[ignored](), jsontext.Value(`{"Hidden":null,"NoTag":null}`), "/n"); errs != nil {
		t.Errorf("nulls judged a field the decoder ignores: %v", errs)
	}
	errs := nulls(reflect.TypeFor[map[string]string](), jsontext.Value(`{"a":null}`), "/m")
	if len(errs) != 1 || !strings.Contains(errs[0].Error(), "/m/a is null") {
		t.Errorf("nulls did not walk a map: %v", errs)
	}
	if errs := nulls(reflect.TypeFor[[]byte](), jsontext.Value(`[1]`), "/b"); errs != nil {
		t.Errorf("nulls walked raw bytes: %v", errs)
	}
	if errs := nulls(reflect.TypeFor[[2]string](), jsontext.Value(`["a",null]`), "/a"); errs != nil {
		t.Errorf("nulls walked an array type: %v", errs)
	}
	if errs := nulls(reflect.TypeFor[struct{ A string }](), jsontext.Value(`5`), "/s"); errs != nil {
		t.Errorf("nulls walked a wrong-shaped struct: %v", errs)
	}
	if errs := nulls(reflect.TypeFor[[]string](), jsontext.Value(`{}`), "/s"); errs != nil {
		t.Errorf("nulls walked a wrong-shaped list: %v", errs)
	}
	if errs := nulls(reflect.TypeFor[map[string]string](), jsontext.Value(`[]`), "/s"); errs != nil {
		t.Errorf("nulls walked a wrong-shaped map: %v", errs)
	}
}

// TestParseTemplatesWalksSlicesMapsAndUntaggedFields: every reachable Template
// is finalized, a parse failure keeps the map entry as written, and a field the
// decoder ignores is not touched.
func TestParseTemplatesWalksSlicesMapsAndUntaggedFields(t *testing.T) {
	type item struct {
		Body Template `json:"body"`
	}
	type holder struct {
		NoTag      Template
		Hidden     Template `json:"-"`
		unexported Template
		Items      []item
		ByName     map[string]Template
		ByStruct   map[string]item
	}
	h := holder{
		NoTag:      Template{text: "{{ .X }}"},
		unexported: Template{text: "{{"}, // the walk must never parse it
		ByName:     map[string]Template{"keep": {text: "{{ .X }}"}, "drop": {text: "{{"}},
		ByStruct:   map[string]item{"ok": {Body: Template{text: "{{ .X }}"}}, "bad": {Body: Template{text: "{{"}}},
	}
	if h.unexported.text != "{{" {
		t.Fatal("the unexported field did not keep its value")
	}
	errs := parseTemplates(reflect.ValueOf(&h), template.FuncMap{}, "")
	if len(errs) != 2 {
		t.Fatalf("parseTemplates = %v, want the two broken templates", errs)
	}
	joined := errorsText(errs)
	for _, want := range []string{"/ByName/drop", "/ByStruct/bad/body"} {
		if !strings.Contains(joined, want) {
			t.Errorf("parseTemplates lacks %q:\n%s", want, joined)
		}
	}
	if h.ByName["keep"].text != "{{ .X }}" || h.ByStruct["ok"].Body.text != "{{ .X }}" {
		t.Error("parseTemplates did not keep the valid map entries")
	}
}
