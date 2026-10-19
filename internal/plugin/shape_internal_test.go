package plugin

import (
	"reflect"
	"testing"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/model"
)

// TestDecodableKindsDereferencesPointers: the classifier unwraps a pointer to
// the type it points at, so a *string is judged a string. shape() dereferences
// before it calls here, which is why this exercises the classifier's own guard.
func TestDecodableKindsDereferencesPointers(t *testing.T) {
	if got := decodableKinds(reflect.TypeFor[*string]()); got != kindString {
		t.Fatalf("decodableKinds(*string) = %d, want %d", got, kindString)
	}
}

// TestShapeLeavesUnjudgedAndMalformedValuesAlone: a null is nobody's to judge
// here, and a value that does not parse is the strict decoder's to report. The
// walker answers no error for either, at every position it walks — reporting one
// would double up on the decoder's own message.
func TestShapeLeavesUnjudgedAndMalformedValuesAlone(t *testing.T) {
	cases := []struct {
		name string
		typ  reflect.Type
		raw  string
	}{
		{"a null where a number goes", reflect.TypeFor[int](), "null"},
		{"a malformed object against a struct", reflect.TypeFor[struct{ A int }](), "{"},
		{"a malformed object against a map", reflect.TypeFor[map[string]string](), "{"},
		{"a malformed array against a slice", reflect.TypeFor[[]string](), "["},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if errs := shape(tc.typ, jsontext.Value(tc.raw), "/x"); len(errs) != 0 {
				t.Fatalf("shape reported %v, want none", errs)
			}
		})
	}
}

// TestShapeRefusesAKindNoFieldOfThisTypeDecodes: a ByteSize takes a string or a
// number, so a boolean is refused and the message names the accepted kind as
// "value" — the combination, not one of its parts.
func TestShapeRefusesAKindNoFieldOfThisTypeDecodes(t *testing.T) {
	errs := shape(reflect.TypeFor[model.ByteSize](), jsontext.Value("true"), "/size")
	if len(errs) != 1 {
		t.Fatalf("shape = %v, want one error", errs)
	}
	if got := errs[0].Error(); got != "/size must be a JSON value, got a JSON boolean" {
		t.Fatalf("shape error = %q", got)
	}
}

// TestRawKindSkipsWhitespaceAndAnswersZeroForWhatItCannotJudge: leading
// whitespace is skipped, and a null and an empty value are both unjudged (0).
func TestRawKindSkipsWhitespaceAndAnswersZeroForWhatItCannotJudge(t *testing.T) {
	if got := rawKind(jsontext.Value(" 1")); got != kindNumber {
		t.Errorf("rawKind(\" 1\") = %d, want a number", got)
	}
	if got := rawKind(jsontext.Value("null")); got != 0 {
		t.Errorf("rawKind(null) = %d, want 0", got)
	}
	if got := rawKind(jsontext.Value("")); got != 0 {
		t.Errorf("rawKind(empty) = %d, want 0", got)
	}
}

// TestFieldTypeResolvesEveryMemberEncodingJSONDoes: an unexported field is
// skipped, a json:"-" member is not a name, an untagged member matches by its Go
// name, and a field promoted through an embedded pointer is found.
func TestFieldTypeResolvesEveryMemberEncodingJSONDoes(t *testing.T) {
	type Inner struct {
		Depth int `json:"depth"`
	}
	type cfg struct {
		hidden string
		Skip   string `json:"-"`
		Tagged string `json:"tagged"`
		Plain  string
		*Inner
	}
	var probe cfg
	_ = probe.hidden // the field is walked, never read
	typ := reflect.TypeFor[cfg]()
	if ft, ok := fieldType(typ, "depth"); !ok || ft != reflect.TypeFor[int]() {
		t.Fatalf("fieldType(depth) = %v, %v", ft, ok)
	}
	if ft, ok := fieldType(typ, "tagged"); !ok || ft != reflect.TypeFor[string]() {
		t.Fatalf("fieldType(tagged) = %v, %v", ft, ok)
	}
	if ft, ok := fieldType(typ, "Plain"); !ok || ft != reflect.TypeFor[string]() {
		t.Fatalf("fieldType(Plain) = %v, %v", ft, ok)
	}
	if _, ok := fieldType(typ, "Skip"); ok {
		t.Fatal(`a json:"-" member was resolved as a name`)
	}
}
