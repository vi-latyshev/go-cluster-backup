package configuration

import (
	"reflect"
	"strings"
	"testing"

	"encoding/json/jsontext"
)

func TestDefaultsDocumentsNilAndCount(t *testing.T) {
	var absent *Defaults
	if got := absent.Documents(); got != 0 {
		t.Errorf("a nil Defaults reports %d documents, want 0", got)
	}
	defs := loadDefaults(t, testKit(), testBlocks)
	if got := defs.Documents(); got != 1 {
		t.Errorf("Documents = %d, want the one document it was loaded from", got)
	}
}

func TestLoadDefaultsRefusesNilSchemaAndNonObjectDocument(t *testing.T) {
	if _, err := LoadDefaults(nil, nil); err == nil || !strings.Contains(err.Error(), "schema is nil") {
		t.Fatalf("LoadDefaults(nil, ...) = %v, want the nil schema refused", err)
	}
	_, err := LoadDefaults(testKit(), []jsontext.Value{jsontext.Value(`[]`)})
	if err == nil || !strings.Contains(err.Error(), "must be a JSON object") {
		t.Fatalf("LoadDefaults with an array document = %v, want the shape refused", err)
	}
}

// TestCheckDefaultsRefusesAnUnknownInstanceKindAndAConfigInAnEnvelope.
func TestCheckDefaultsRefusesAnUnknownInstanceKindAndAConfigInAnEnvelope(t *testing.T) {
	schema := testKit()
	if errs := checkDefaults(schema, jsontext.Value(`{"instance":{"bogus":{}}}`)); !anyErrHas(errs, "no such section") {
		t.Fatalf("checkDefaults accepted an unknown instance kind: %v", errs)
	}
	if errs := checkDefaults(schema, jsontext.Value(`{"instance":{"source":{"config":{}}}}`)); !anyErrHas(errs, "belongs under plugins.config/<ID>") {
		t.Fatalf("checkDefaults accepted a config in an envelope: %v", errs)
	}
	if errs := checkDefaults(schema, jsontext.Value(`{"instance":{"catalog":{"type":"x"}}}`)); anyErrHas(errs, "belongs under plugins.config") {
		t.Fatalf("checkDefaults refused a type in an envelope: %v", errs)
	}
}

// TestCheckDefaultsJudgesProviderAndUnknownBlocks: a provider's settings are
// walked against the type it registered; a stable name no layer knows is left
// to the distribution that carries it, and a malformed ID is refused.
func TestCheckDefaultsJudgesProviderAndUnknownBlocks(t *testing.T) {
	schema := testKit()
	if errs := checkDefaults(schema, jsontext.Value(`{"plugins":{"config":{"reporting":{"timeout":"soon"}}}}`)); !anyErrHas(errs, "/plugins/config/reporting/timeout") {
		t.Fatalf("checkDefaults accepted a bad provider setting: %v", errs)
	}
	if errs := checkDefaults(schema, jsontext.Value(`{"plugins":{"config":{"elsewhere":{"timeout":"soon"}}}}`)); anyErrHas(errs, "/plugins/config/elsewhere") {
		t.Fatalf("checkDefaults judged a provider of another distribution: %v", errs)
	}
	if errs := checkDefaults(noProvidersSchema{testKit()}, jsontext.Value(`{"plugins":{"config":{"reporting":{"timeout":"soon"}}}}`)); anyErrHas(errs, "/plugins/config/reporting") {
		t.Fatalf("checkDefaults judged providers without a provider schema: %v", errs)
	}
	if errs := checkDefaults(schema, jsontext.Value(`{"plugins":{"config":{"source/":{"timeout":"soon"}}}}`)); !anyErrHas(errs, "not a plugin ID") {
		t.Fatalf("checkDefaults accepted a malformed plugin ID: %v", errs)
	}
}

// noProvidersSchema is a schema without provider settings: stable names are
// opaque to it.
type noProvidersSchema struct{ Schema }

// TestCheckDefaultsWalksTheRootAndThePluginLists.
func TestCheckDefaultsWalksTheRootAndThePluginLists(t *testing.T) {
	schema := testKit()
	if errs := checkDefaults(schema, jsontext.Value(`{"bogus":1}`)); !anyErrHas(errs, "/bogus: unknown key") {
		t.Fatalf("checkDefaults accepted an unknown root key: %v", errs)
	}
	if errs := checkDefaults(schema, jsontext.Value(`{"plugins":{"bundles":["b"],"disabled":["source/testkit"]}}`)); anyErrHas(errs, "/plugins/") {
		t.Fatalf("checkDefaults refused the plugin lists: %v", errs)
	}
	if errs := checkDefaults(schema, jsontext.Value(`{"plugins":{"unknown":1}}`)); !anyErrHas(errs, "/plugins/unknown: unknown key") {
		t.Fatalf("checkDefaults accepted an unknown plugins key: %v", errs)
	}
}

// TestLoadDefaultsChecksEveryDocumentOfADistribution: a distribution's own
// document may only add plugins.config blocks.
func TestLoadDefaultsChecksEveryDocumentOfADistribution(t *testing.T) {
	cases := []struct {
		name string
		doc  string
		want string
	}{
		{name: "a plugins root that is not an object", doc: `{"plugins":[]}`, want: "/plugins must be an object"},
		{name: "a selection list", doc: `{"plugins":{"bundles":[]}}`, want: "/plugins/bundles"},
		{name: "a section beyond plugins", doc: `{"run":{}}`, want: "/run"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := LoadDefaults(testKit(), []jsontext.Value{defaultsDoc(t, testBlocks), jsontext.Value(c.doc)})
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("LoadDefaults reported %v, want %q", err, c.want)
			}
		})
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
	type classified struct {
		A string `json:"a" required:"true"`
	}
	errs = walkValue(reflect.TypeFor[[]classified](), nil, "/list")
	if len(errs) != 0 {
		t.Errorf("walkValue of a list without a value = %v, want the element type walked", errs)
	}
	if errs := walkValue(reflect.TypeFor[sourceConfig](), jsontext.Value(`"x"`), "/s"); len(errs) != 1 {
		t.Errorf("walkValue of a scalar where a struct is = %v, want it refused", errs)
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

// TestSetAtCreateErrorMatrix: the paths the one merge setter cannot place a
// value on, and the members it creates instead.
func TestSetAtCreateErrorMatrix(t *testing.T) {
	got, err := setAtCreate(jsontext.Value(`{"a":1}`), "/a", jsontext.Value(`2`))
	if err != nil || string(got) != `{"a":2}` {
		t.Fatalf("setAtCreate = (%s, %v), want the value replaced", got, err)
	}
	got, err = setAtCreate(jsontext.Value(`[1,2]`), "/1", jsontext.Value(`9`))
	if err != nil || string(got) != `[1,9]` {
		t.Fatalf("setAtCreate of a list element = (%s, %v)", got, err)
	}
	got, err = setAtCreate(jsontext.Value(`[{"a":1}]`), "/0/b", jsontext.Value(`0`))
	if err != nil || string(got) != `[{"a":1,"b":0}]` {
		t.Fatalf("setAtCreate of a member inside a list element = (%s, %v)", got, err)
	}
	cases := []struct {
		name    string
		doc     string
		pointer string
		want    string
	}{
		{name: "an index past the end", doc: `[1,2]`, pointer: "/5", want: "no such element"},
		{name: "a bad index", doc: `[1,2]`, pointer: "/x", want: "no such element"},
		{name: "a scalar in the way", doc: `"x"`, pointer: "/a", want: "no such member"},
		{name: "a bad index deeper down", doc: `{"a":[1]}`, pointer: "/a/5", want: "no such element"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := setAtCreate(jsontext.Value(c.doc), c.pointer, jsontext.Value(`0`))
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("setAtCreate(%s, %s) = %v, want %q", c.doc, c.pointer, err, c.want)
			}
		})
	}
}

func TestSectionForKindAndKindNames(t *testing.T) {
	sec, ok := sectionForKind(KindRetention)
	if !ok || sec.kind != KindRetention {
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
