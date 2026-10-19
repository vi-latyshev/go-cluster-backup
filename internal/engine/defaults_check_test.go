package engine

import (
	"encoding/json/jsontext"
	"reflect"
	"strings"
	"testing"
)

func TestDefaultsDocumentsNilAndCount(t *testing.T) {
	var absent *Defaults
	if got := absent.Documents(); got != 0 {
		t.Errorf("a nil Defaults reports %d documents, want 0", got)
	}
	defs := loadDefaults(t, defaultsRegistry(t), testBlocks)
	if got := defs.Documents(); got != 1 {
		t.Errorf("Documents = %d, want the one document it was loaded from", got)
	}
}

func TestLoadDefaultsRefusesNilRegistryAndNonObjectDocument(t *testing.T) {
	if _, err := LoadDefaults(nil, nil); err == nil || !strings.Contains(err.Error(), "registry is nil") {
		t.Fatalf("LoadDefaults(nil, ...) = %v, want the nil registry refused", err)
	}
	_, err := LoadDefaults(defaultsRegistry(t), []jsontext.Value{jsontext.Value(`[]`)})
	if err == nil || !strings.Contains(err.Error(), "must be a JSON object") {
		t.Fatalf("LoadDefaults with an array document = %v, want the shape refused", err)
	}
}

// TestCheckDefaultsRefusesAnUnknownInstanceKindAndAConfigInAnEnvelope.
func TestCheckDefaultsRefusesAnUnknownInstanceKindAndAConfigInAnEnvelope(t *testing.T) {
	reg := defaultsRegistry(t)
	if errs := checkDefaults(reg, jsontext.Value(`{"instance":{"bogus":{}}}`)); !anyErrHas(errs, "no such section") {
		t.Fatalf("checkDefaults accepted an unknown instance kind: %v", errs)
	}
	if errs := checkDefaults(reg, jsontext.Value(`{"instance":{"source":{"config":{}}}}`)); !anyErrHas(errs, "belongs under plugins/<kind>/<type>") {
		t.Fatalf("checkDefaults accepted a config in an envelope: %v", errs)
	}
}

func anyErrHas(errs []error, sub string) bool {
	for _, err := range errs {
		if strings.Contains(err.Error(), sub) {
			return true
		}
	}
	return false
}

// TestWalkValueEntersAMapAndLeavesRawBytesAlone.
func TestWalkValueEntersAMapAndLeavesRawBytesAlone(t *testing.T) {
	if errs := walkValue(reflect.TypeFor[[]byte](), nil, "/raw"); errs != nil {
		t.Errorf("walkValue of raw bytes = %v, want nothing", errs)
	}
	errs := walkValue(reflect.TypeFor[map[string]int](), jsontext.Value(`{"a":"x"}`), "/m")
	if len(errs) != 1 || !strings.Contains(errs[0].Error(), "/m/a") {
		t.Errorf("walkValue of a map = %v, want the element judged at its own path", errs)
	}
}

// TestWalkStructRefusesAnUnclassifiedFieldAndANameBlockOnANonList.
func TestWalkStructRefusesAnUnclassifiedFieldAndANameBlockOnANonList(t *testing.T) {
	type unclassified struct {
		A string `json:"a"`
	}
	errs := walkStruct(reflect.TypeFor[unclassified](), nil, "")
	if len(errs) != 1 || !strings.Contains(errs[0].Error(), "neither required") {
		t.Fatalf("walkStruct accepted an unclassified field: %v", errs)
	}

	type notAList struct {
		X string `json:"x" required:"true"`
	}
	errs = walkStruct(reflect.TypeFor[notAList](), map[string]jsontext.Value{"x[]": jsontext.Value(`{}`)}, "")
	if len(errs) != 1 || !strings.Contains(errs[0].Error(), "is not a list") {
		t.Fatalf("walkStruct accepted a name[] block on a non-list: %v", errs)
	}
}

// TestDocNullsFindsANullInsideAList.
func TestDocNullsFindsANullInsideAList(t *testing.T) {
	errs := docNulls(jsontext.Value(`{"a":[1,null]}`), "")
	if len(errs) != 1 || !strings.Contains(errs[0].Error(), "/a/1 is null") {
		t.Fatalf("docNulls = %v, want the null inside the list named", errs)
	}
}

// TestSetAtReplacesOneValueAndRefusesAMissingPath.
func TestSetAtReplacesOneValueAndRefusesAMissingPath(t *testing.T) {
	got, err := setAt(jsontext.Value(`{"a":1}`), "/a", jsontext.Value(`2`))
	if err != nil || string(got) != `{"a":2}` {
		t.Fatalf("setAt = (%s, %v), want the value replaced", got, err)
	}
	got, err = setAt(jsontext.Value(`[1,2]`), "/1", jsontext.Value(`9`))
	if err != nil || string(got) != `[1,9]` {
		t.Fatalf("setAt of a list element = (%s, %v)", got, err)
	}
	cases := []struct {
		name    string
		doc     string
		pointer string
		want    string
	}{
		{name: "an index past the end", doc: `[1,2]`, pointer: "/5", want: "no such element"},
		{name: "a member the object lacks", doc: `{"a":1}`, pointer: "/b", want: "no such member"},
		{name: "a scalar in the way", doc: `"x"`, pointer: "/a", want: "no such member"},
		{name: "a bad index deeper down", doc: `{"a":[1]}`, pointer: "/a/5", want: "no such element"},
		{name: "a missing member deeper down", doc: `[{"a":1}]`, pointer: "/0/b", want: "no such member"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := setAt(jsontext.Value(c.doc), c.pointer, jsontext.Value(`0`))
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("setAt(%s, %s) = %v, want %q", c.doc, c.pointer, err, c.want)
			}
		})
	}
}

func TestSectionForKindAndKindNames(t *testing.T) {
	sec, ok := sectionForKind("retention")
	if !ok || sec.kind != "retention" {
		t.Fatalf("sectionForKind(retention) = (%v, %v)", sec, ok)
	}
	if _, ok := sectionForKind("bogus"); ok {
		t.Error("sectionForKind accepted an unknown kind")
	}
	names := strings.Join(kindNames(), ",")
	for _, want := range []string{"discovery", "source", "processor", "storage", "retention", "notifier", "catalog"} {
		if !strings.Contains(names, want) {
			t.Errorf("kindNames = %s, want it to name %s", names, want)
		}
	}
}

// TestFieldByNameSkipsAnIgnoredFieldAndReportsAMiss.
func TestFieldByNameSkipsAnIgnoredFieldAndReportsAMiss(t *testing.T) {
	type tagged struct {
		A      string `json:"a"`
		Hidden string `json:"-"`
	}
	if f, ok := fieldByName(reflect.TypeFor[tagged](), "a"); !ok || f.Name != "A" {
		t.Fatalf("fieldByName(a) = (%v, %v)", f, ok)
	}
	if _, ok := fieldByName(reflect.TypeFor[tagged](), "zzz"); ok {
		t.Error("fieldByName found a field that is not there")
	}
}

// TestEncodePanicsOnAValueItCannotReencode: the bytes come from a document that
// already parsed, so a failure here is a bug in this package, not input.
func TestEncodePanicsOnAValueItCannotReencode(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("encode accepted a value it cannot re-encode")
		}
	}()
	encode(make(chan int))
}

func TestJSONStringRefusesANonString(t *testing.T) {
	if s, ok := jsonString(jsontext.Value(`"x"`)); !ok || s != "x" {
		t.Errorf("jsonString of a string = (%q, %v)", s, ok)
	}
	if _, ok := jsonString(jsontext.Value(`{}`)); ok {
		t.Error("jsonString accepted an object")
	}
}
