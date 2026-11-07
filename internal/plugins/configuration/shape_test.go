package configuration

import (
	"reflect"
	"strings"
	"testing"

	"encoding/json/jsontext"
	json "encoding/json/v2"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
)

// TestDecodableKindsClassifiesEveryFieldKind: the framework's own types are
// named explicitly, every other self-unmarshalling type stays open, and a plain
// kind follows reflect.
func TestDecodableKindsClassifiesEveryFieldKind(t *testing.T) {
	cases := []struct {
		name string
		typ  reflect.Type
		want jsonKinds
	}{
		{"a template", reflect.TypeFor[Template](), kindString},
		{"a duration", reflect.TypeFor[model.Duration](), kindString},
		{"a byte size", reflect.TypeFor[model.ByteSize](), kindString | kindNumber},
		{"opaque bytes", reflect.TypeFor[jsontext.Value](), kindAny},
		{"a self-unmarshalling type", reflect.TypeFor[timeLayout](), kindAny},
		{"a string", reflect.TypeFor[string](), kindString},
		{"a bool", reflect.TypeFor[bool](), kindBool},
		{"an integer", reflect.TypeFor[int64](), kindNumber},
		{"an unsigned integer", reflect.TypeFor[uint32](), kindNumber},
		{"a float", reflect.TypeFor[float64](), kindNumber},
		{"a struct", reflect.TypeFor[sourceConfig](), kindObject},
		{"a map", reflect.TypeFor[map[string]string](), kindObject},
		{"a list", reflect.TypeFor[[]string](), kindArray},
		{"an array", reflect.TypeFor[[2]string](), kindArray},
		{"raw bytes", reflect.TypeFor[[]byte](), kindAny},
		{"an interface", reflect.TypeFor[any](), kindAny},
		{"a pointer", reflect.TypeFor[*sourceConfig](), kindObject},
		{"a channel", reflect.TypeFor[chan int](), kindAny},
		{"a function", reflect.TypeFor[func()](), kindAny},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := decodableKinds(c.typ); got != c.want {
				t.Errorf("decodableKinds(%s) = %d, want %d", c.typ, got, c.want)
			}
		})
	}
}

// timeLayout unmarshals itself from a string; only it knows what it takes.
type timeLayout struct{ text string }

func (t *timeLayout) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	t.text = s
	return nil
}

// TestShapeJudgesTheKindAndTheMembers: a wrong kind is refused with its path, an
// unknown member is named, and every value the check cannot judge stays open.
func TestShapeJudgesTheKindAndTheMembers(t *testing.T) {
	type inner struct {
		Depth int `json:"depth"`
	}
	type config struct {
		Name   string            `json:"name"`
		Nested inner             `json:"nested"`
		Labels map[string]string `json:"labels"`
		List   []inner           `json:"list"`
	}
	typ := reflect.TypeFor[config]()
	errs := shape(typ, jsontext.Value(`{
		"name": 5,
		"nested": {"depth": "x", "ghost": 1},
		"labels": {"a": 1},
		"list": [{"depth": "y"}]
	}`), "/c")
	msg := errorsText(errs)
	for _, want := range []string{
		"/c/name must be a JSON string, got a JSON number",
		"/c/nested/depth must be a JSON number, got a JSON string",
		"unknown field /c/nested/ghost",
		"/c/labels/a must be a JSON string, got a JSON number",
		"/c/list/0/depth must be a JSON number, got a JSON string",
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("shape error lacks %q:\n%s", want, msg)
		}
	}

	// A value the check cannot judge is not walked into at all.
	if errs := shape(reflect.TypeFor[any](), jsontext.Value(`{"anything":1}`), "/c"); errs != nil {
		t.Errorf("shape of an unjudged type = %v", errs)
	}
	// A null and nothing at all are unjudged.
	if errs := shape(typ, jsontext.Value(`null`), "/c"); errs != nil {
		t.Errorf("shape of a null = %v", errs)
	}
	if errs := shape(typ, nil, "/c"); errs != nil {
		t.Errorf("shape of nothing = %v", errs)
	}
	// A pointer type is judged by what it points at.
	if errs := shape(reflect.TypeFor[*inner](), jsontext.Value(`{"depth":"x"}`), "/p"); len(errs) != 1 || !strings.Contains(errs[0].Error(), "/p/depth") {
		t.Errorf("shape of a pointer = %v, want the element judged", errs)
	}
	// A malformed object or list is the strict decoder's to report.
	if errs := shape(typ, jsontext.Value(`{not json`), "/c"); errs != nil {
		t.Errorf("shape of a malformed object = %v", errs)
	}
	if errs := shape(reflect.TypeFor[[]inner](), jsontext.Value(`[1,`), "/c"); errs != nil {
		t.Errorf("shape of a malformed list = %v", errs)
	}
	if errs := shape(reflect.TypeFor[map[string]string](), jsontext.Value(`{not json`), "/c"); errs != nil {
		t.Errorf("shape of a malformed map = %v", errs)
	}
}

func errorsText(errs []error) string {
	parts := make([]string, 0, len(errs))
	for _, err := range errs {
		parts = append(parts, err.Error())
	}
	return strings.Join(parts, "\n")
}

// TestCheckNullsWalksListsAndMaps.
func TestCheckNullsWalksListsAndMaps(t *testing.T) {
	errs := CheckNulls(jsontext.Value(`{"a":[1,null]}`), "")
	if len(errs) != 1 || !strings.Contains(errs[0].Error(), "/a/1 is null") {
		t.Fatalf("CheckNulls = %v, want the null inside the list named", errs)
	}
	if errs := CheckNulls(jsontext.Value(`{"a":1}`), ""); errs != nil {
		t.Errorf("CheckNulls of a clean value = %v", errs)
	}
}

// TestRawKindClassifiesTheFirstByte.
func TestRawKindClassifiesTheFirstByte(t *testing.T) {
	cases := []struct {
		raw  string
		want jsonKinds
	}{
		{` "x"`, kindString},
		{`{"a":1}`, kindObject},
		{`[1]`, kindArray},
		{`true`, kindBool},
		{`false`, kindBool},
		{`null`, 0},
		{`5`, kindNumber},
		{"", 0},
	}
	for _, c := range cases {
		if got := rawKind(jsontext.Value(c.raw)); got != c.want {
			t.Errorf("rawKind(%q) = %d, want %d", c.raw, got, c.want)
		}
	}
}

// TestKindNameNamesEveryKind: the message a refusal carries names the expected
// and the actual kind.
func TestKindNameNamesEveryKind(t *testing.T) {
	cases := map[jsonKinds]string{
		kindString:              "string",
		kindNumber:              "number",
		kindBool:                "boolean",
		kindObject:              "object",
		kindArray:               "array",
		kindString | kindNumber: "value",
		kindString | kindNumber | kindBool | kindObject | kindArray: "value",
	}
	for kind, want := range cases {
		if got := kindName(kind); got != want {
			t.Errorf("kindName(%d) = %q, want %q", kind, got, want)
		}
	}
}

// TestArrayOfRefusesAMalformedList.
func TestArrayOfRefusesAMalformedList(t *testing.T) {
	if _, ok := arrayOf(jsontext.Value(`{not json`)); ok {
		t.Error("arrayOf accepted a malformed list")
	}
	if items, ok := arrayOf(jsontext.Value(`[1,2]`)); !ok || len(items) != 2 {
		t.Errorf("arrayOf = (%v, %v)", items, ok)
	}
}

// TestFieldTypeResolvesMembersTheWayTheDecoderDoes: an ignored field, a tagged
// one, an embedded pointer promoted, and an untagged field by its Go name.
func TestFieldTypeResolvesMembersTheWayTheDecoderDoes(t *testing.T) {
	type Embedded struct {
		Promoted string `json:"promoted"`
	}
	type config struct {
		Ignored  string `json:"-"`
		Hidden   string
		Tagged   string `json:"tagged"`
		Untagged string
		*Embedded
	}
	typ := reflect.TypeFor[config]()
	for _, name := range []string{"tagged", "Untagged", "Hidden", "promoted"} {
		if _, ok := fieldType(typ, name); !ok {
			t.Errorf("fieldType(%q) did not resolve the field", name)
		}
	}
	for _, name := range []string{"-", "absent"} {
		if _, ok := fieldType(typ, name); ok {
			t.Errorf("fieldType(%q) resolved a field it must not", name)
		}
	}
	// An embedded value that is not a struct field of the name ends the walk.
	type plain struct {
		Value string
	}
	if _, ok := fieldType(reflect.TypeFor[plain](), "absent"); ok {
		t.Error("fieldType resolved a missing field")
	}
}

// TestShapeErrorsCombinesMissingAndShape: one call reports every gap with its
// pointer, the missing members and the wrongly-typed ones together.
func TestShapeErrorsCombinesMissingAndShape(t *testing.T) {
	err := ShapeErrors(reflect.TypeFor[sourceConfig](), jsontext.Value(`{"timeout":5}`), "/config")
	if err == nil {
		t.Fatal("ShapeErrors accepted an incomplete config")
	}
	msg := err.Error()
	for _, want := range []string{"missing field /config/address", "/config/timeout must be a JSON string, got a JSON number"} {
		if !strings.Contains(msg, want) {
			t.Errorf("ShapeErrors lacks %q:\n%s", want, msg)
		}
	}
}
