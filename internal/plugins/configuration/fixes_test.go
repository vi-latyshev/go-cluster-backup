package configuration

import (
	"reflect"
	"strings"
	"testing"

	"encoding/json/jsontext"
)

// TestM1_MergePreservesAnInvalidPluginsShape: a plugins root the deployment
// wrote wrong — a null, a scalar, an unknown member, a settings block that is
// not an object — is handed on exactly as written, so the strict decoder
// reports it with its own pointer and the merge never turns a broken file into
// a valid one.
func TestM1_MergePreservesAnInvalidPluginsShape(t *testing.T) {
	defs := loadDefaults(t, testKit(), testBlocks)
	cases := []struct {
		name       string
		raw        string
		wantSame   string
		wantDecode string
	}{
		{name: "a null plugins root", raw: `{"plugins":null}`, wantSame: `null`, wantDecode: "/plugins"},
		{name: "a scalar plugins root", raw: `{"plugins":5}`, wantSame: `5`, wantDecode: "/plugins"},
		{name: "an array plugins root", raw: `{"plugins":[]}`, wantSame: `[]`, wantDecode: "/plugins"},
		{name: "a misspelled control", raw: `{"plugins":{"disabledd":["processor/age"]}}`, wantSame: `"disabledd"`, wantDecode: "disabledd"},
		{name: "a null settings block", raw: `{"plugins":{"config":null}}`, wantSame: `null`, wantDecode: "/plugins/config"},
		{name: "a scalar settings block", raw: `{"plugins":{"config":5}}`, wantSame: `5`, wantDecode: "/plugins/config"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			merged, pointers, problems := defs.Merge(testKit(), jsontext.Value(c.raw))
			if len(problems) != 0 {
				t.Fatalf("Merge: %v", problems)
			}
			if !strings.Contains(string(merged), c.wantSame) {
				t.Fatalf("the merge rewrote the invalid plugins root: %s", merged)
			}
			// No default leaked into the broken root.
			obj, _ := ObjectOf(merged)
			if plugins, ok := ObjectOf(obj["plugins"]); ok {
				if _, present := plugins["bundles"]; present {
					t.Fatalf("the merge filled defaults into a broken plugins root: %s", merged)
				}
			}
			for _, pointer := range pointers {
				if strings.HasPrefix(pointer, "/plugins") {
					t.Errorf("the merge reported a plugins pointer for a broken root: %s (%v)", pointer, pointers)
				}
			}
			// The strict decoder names the problem where it is.
			var cfg Config
			if err := Decode(merged, &cfg); err == nil || !strings.Contains(err.Error(), c.wantDecode) {
				t.Fatalf("Decode of the preserved shape = %v, want %q", err, c.wantDecode)
			}
		})
	}
}

// TestM2_EveryPluginSettingsBlockIsJudged: every plugins.config block is judged
// against the compiled schema before any command runs — unknown factories,
// unknown providers, misspelled members and wrong JSON kinds — whether an
// instance uses the block or not.
func TestM2_EveryPluginSettingsBlockIsJudged(t *testing.T) {
	defs := loadDefaults(t, testKit(), testBlocks)
	cases := []struct {
		name string
		raw  string
		want string
	}{
		{
			name: "an unknown factory type",
			raw:  `{"plugins":{"config":{"source/nope":{"timeout":"30s"}}}}`,
			want: "/plugins/config/source~1nope: unknown plugin",
		},
		{
			name: "an unknown provider",
			raw:  `{"plugins":{"config":{"unknown_provider":{"typo":true}}}}`,
			want: `/plugins/config/unknown_provider: unknown plugin "unknown_provider"`,
		},
		{
			name: "a misspelled member of an unused block",
			raw:  `{"plugins":{"config":{"storage/testkit":{"page_sise":1}}}}`,
			want: "unknown field /plugins/config/storage~1testkit/page_sise",
		},
		{
			name: "a wrongly typed member of an unused block",
			raw:  `{"plugins":{"config":{"source/testkit":{"timeout":5}}}}`,
			want: "/plugins/config/source~1testkit/timeout must be a JSON string, got a JSON number",
		},
		{
			name: "a malformed settings key",
			raw:  `{"plugins":{"config":{"source/":{"timeout":"30s"}}}}`,
			want: "/plugins/config/source~1: not a plugin ID",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// The candidate survives the semantic problem: a diagnostic command
			// still prints it, and the problem refuses operations.
			merged, _, problems := defs.Merge(testKit(), jsontext.Value(c.raw))
			if len(merged) == 0 {
				t.Fatalf("Merge withheld the candidate for %s", c.name)
			}
			if !anyErrHas(problems, c.want) {
				t.Fatalf("Merge = %v, want %q", problems, c.want)
			}
		})
	}
	// A block naming the block's own required fields as the deployment states
	// them is still partial: a missing required member is assembly's to judge,
	// not the merge's.
	partial := jsontext.Value(`{"plugins":{"config":{"source/testkit":{"timeout":"30s"}}}}`)
	if _, _, problems := defs.Merge(testKit(), partial); len(problems) != 0 {
		t.Fatalf("Merge refused a partial block: %v", problems)
	}
	// A stable name is judged only where the schema carries provider types.
	schema := noProvidersSchema{testKit()}
	if _, _, problems := defs.Merge(schema, jsontext.Value(`{"plugins":{"config":{"unknown_provider":{"typo":true}}}}`)); len(problems) != 0 {
		t.Fatalf("Merge judged a provider without a provider schema: %v", problems)
	}
}

// TestM5_DeploymentSettingsAreNotDefaulted: the middle layer's values are the
// deployment's, even though they merge over the defaults; only leaves the
// embedded document actually filled are reported as defaulted. The same holds
// inside a declared instance and inside a nested rule source.
func TestM5_DeploymentSettingsAreNotDefaulted(t *testing.T) {
	defs := loadDefaults(t, testKit(), testBlocks)
	user := jsontext.Value(`{
	  "plugins": {"config": {
	    "storage/testkit": {"root": "/example-backups"},
	    "source/testkit": {"timeout": "45s"}
	  }},
	  "storages": {"local": {"type": "testkit", "time_budget": "1m"}},
	  "discoveries": {"d": {"type": "testkit", "config": {"rules": [{"name": "r", "source": {"type": "testkit", "config": {"address": "x"}}, "pipeline": ["pack"], "min_matches": 0}]}}}
	}`)
	merged, pointers, problems := defs.Merge(testKit(), user)
	if len(problems) != 0 {
		t.Fatalf("Merge: %v", problems)
	}
	for _, want := range []string{
		"/storages/local/config/page_size",
	} {
		if !containsPointer(pointers, want) {
			t.Errorf("the pointers do not name the defaulted %s: %v", want, pointers)
		}
	}
	for _, unwanted := range []string{
		"/storages/local/config/root",
		"/plugins/config/storage~1testkit/root",
		"/discoveries/d/config/rules/0/source/config/address",
		"/discoveries/d/config/rules/0/source/config/timeout",
	} {
		for _, p := range pointers {
			if p == unwanted {
				t.Errorf("the pointers mark the deployment's own value as defaulted: %s (%v)", p, pointers)
			}
		}
	}
	// The deployment's value is nevertheless the effective one.
	mergedObj, _ := ObjectOf(merged)
	storage, _ := ObjectOf(mergedObj["storages"])
	entry, _ := ObjectOf(storage["local"])
	cfg, _ := ObjectOf(entry["config"])
	if string(cfg["root"]) != `"/example-backups"` || string(cfg["page_size"]) != `1000` {
		t.Fatalf("the merged storage config = %s", entry["config"])
	}
}

func containsPointer(pointers []string, want string) bool {
	for _, p := range pointers {
		if p == want {
			return true
		}
	}
	return false
}

// TestM9_OneArrayHelperAndSortedKeys: the defaults walk, the shape walk and the
// null walk share one array reader and one key order, so every diagnostic of
// one document reads alike.
func TestM9_OneArrayHelperAndSortedKeys(t *testing.T) {
	got := sortedKeys(map[string]int{"b": 2, "a": 1, "c": 3})
	if strings.Join(got, ",") != "a,b,c" {
		t.Fatalf("sortedKeys = %v, want a,b,c", got)
	}
	// One array reader: both walks judge the same document the same way.
	raw := jsontext.Value(`[1,null]`)
	if items, ok := arrayOf(raw); !ok || len(items) != 2 {
		t.Fatalf("arrayOf = (%v, %v)", items, ok)
	}
	if errs := CheckNulls(raw, "/a"); len(errs) != 1 || !strings.Contains(errs[0].Error(), "/a/1 is null") {
		t.Fatalf("CheckNulls = %v, want the element named", errs)
	}
	if errs := shape(reflect.TypeFor[[]map[string]string](), raw, "/s"); len(errs) != 1 || !strings.Contains(errs[0].Error(), "/s/0 must be a JSON object") {
		t.Fatalf("shape = %v, want the element judged", errs)
	}
}

// TestM9_NameBlocksFillElements: the shape walk understands a `name[]` list
// block exactly as the merge does, so a deployment's per-element template is
// neither unknown nor unchecked.
func TestM9_NameBlocksFillElements(t *testing.T) {
	type item struct {
		Count int `json:"count"`
	}
	type holder struct {
		Items []item `json:"items"`
		Name  string `json:"name"`
	}
	if errs := shape(reflect.TypeFor[holder](), jsontext.Value(`{"items[]":{"count":"x"}}`), "/h"); len(errs) != 1 {
		t.Fatalf("shape = %v, want the element judged", errs)
	}
	if errs := shape(reflect.TypeFor[holder](), jsontext.Value(`{"name[]":"x"}`), "/h"); len(errs) != 1 || !strings.Contains(errs[0].Error(), "is not a list") {
		t.Fatalf("shape = %v, want a name[] block on a non-list refused", errs)
	}
	if errs := shape(reflect.TypeFor[holder](), jsontext.Value(`{"items[]":{"count":1}}`), "/h"); len(errs) != 0 {
		t.Fatalf("shape = %v, want a valid element block accepted", errs)
	}
	// A pointer to the list is followed before the element block is judged.
	type ptrHolder struct {
		Items *[]item `json:"items"`
	}
	if errs := shape(reflect.TypeFor[ptrHolder](), jsontext.Value(`{"items[]":{"count":"x"}}`), "/h"); len(errs) != 1 {
		t.Fatalf("shape of a pointer list = %v, want the element judged", errs)
	}
}

// Test_m2_MergeNeedsNoContextOrLogger: the merge is a pure function of the
// documents and the compiled schema, so it is called with neither.
func Test_m2_MergeNeedsNoContextOrLogger(t *testing.T) {
	defs := loadDefaults(t, testKit(), testBlocks)
	merged, pointers, problems := defs.Merge(testKit(), testConfig(t, `{"address": "db:5432"}`))
	if len(problems) != 0 || len(merged) == 0 || len(pointers) == 0 {
		t.Fatalf("Merge = (%s, %v, %v)", merged, pointers, problems)
	}
}
