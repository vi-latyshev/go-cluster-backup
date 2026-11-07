package configuration

import (
	"reflect"
	"slices"
	"strings"
	"testing"

	"encoding/json/jsontext"
)

// TestR2_M5_MiddleLayerOverrideIsNotDefaulted is the reviewer's origin probe:
// a deployment per-type value — even one equal to the embedded value — is the
// deployment's, and defaulted pointers name only what the embedded document
// filled.
func TestR2_M5_MiddleLayerOverrideIsNotDefaulted(t *testing.T) {
	defs := loadDefaults(t, testKit(), testBlocks)
	raw := jsontext.Value(`{"plugins":{"config":{"source/testkit":{"timeout":"45s"}}},"sources":{"one":{"type":"testkit","config":{"address":"x"}}}}`)
	merged, pointers, problems := defs.Merge(testKit(), raw)
	if len(problems) != 0 {
		t.Fatalf("Merge: %v", problems)
	}
	cfg, _ := ObjectOf(at(t, merged, "/sources/one/config"))
	if string(cfg["timeout"]) != `"45s"` {
		t.Fatalf("effective timeout = %s, want the deployment's 45s", cfg["timeout"])
	}
	if containsPointer(pointers, "/sources/one/config/timeout") {
		t.Fatal("deployment timeout 45s marked defaulted (embedded timeout is 30s)")
	}
	// A value the deployment does not state is still a default.
	raw = jsontext.Value(`{"sources":{"one":{"type":"testkit","config":{"address":"x"}}}}`)
	_, pointers, problems = defs.Merge(testKit(), raw)
	if len(problems) != 0 || !containsPointer(pointers, "/sources/one/config/timeout") {
		t.Fatalf("the embedded timeout is not defaulted: %v (%v)", pointers, problems)
	}
	// And a deployment value equal to the embedded one is still the
	// deployment's: equality never makes it a default.
	raw = jsontext.Value(`{"plugins":{"config":{"source/testkit":{"timeout":"30s"}}},"sources":{"one":{"type":"testkit","config":{"address":"x"}}}}`)
	_, pointers, problems = defs.Merge(testKit(), raw)
	if len(problems) != 0 || containsPointer(pointers, "/sources/one/config/timeout") {
		t.Fatalf("a deployment value equal to the embedded one was marked defaulted: %v", pointers)
	}
}

// TestR2_M12_DeploymentRequiredFieldReachesNestedSource is the reviewer's
// nested-merge probe: the per-type layer applies to a rule source even when the
// instance already overrides every embedded default, and provenance does not
// gate the application of values.
func TestR2_M12_DeploymentRequiredFieldReachesNestedSource(t *testing.T) {
	defs := loadDefaults(t, testKit(), testBlocks)
	raw := jsontext.Value(`{"plugins":{"config":{"source/testkit":{"address":"db"}}},"discoveries":{"d":{"type":"testkit","config":{"rules":[{"name":"r","source":{"type":"testkit","config":{"timeout":"5s"}},"pipeline":[],"min_matches":0}]}}}}`)
	merged, pointers, problems := defs.Merge(testKit(), raw)
	if len(problems) != 0 {
		t.Fatalf("Merge: %v", problems)
	}
	config := at(t, merged, "/discoveries/d/config/rules/0/source/config")
	obj, _ := ObjectOf(config)
	if string(obj["address"]) != `"db"` {
		t.Fatalf("deployment address lost in nested source: %s", config)
	}
	if string(obj["timeout"]) != `"5s"` {
		t.Fatalf("the rule's own timeout was not kept: %s", config)
	}
	if containsPointer(pointers, "/discoveries/d/config/rules/0/source/config/address") {
		t.Fatalf("the deployment's required field was marked defaulted: %v", pointers)
	}
}

// TestR2_M11_SemanticProblemKeepsTheCandidate: a mergeable document with a
// semantic settings problem still yields its candidate; the problem is reported
// separately and refuses operations.
func TestR2_M11_SemanticProblemKeepsTheCandidate(t *testing.T) {
	defs := loadDefaults(t, testKit(), testBlocks)
	raw := jsontext.Value(`{"plugins":{"config":{"unknown_provider":{"typo":true}}},"sources":{"one":{"type":"testkit","config":{"address":"x"}}}}`)
	merged, _, problems := defs.Merge(testKit(), raw)
	if len(merged) == 0 {
		t.Fatal("the merge withheld a mergeable candidate")
	}
	if len(problems) == 0 || !anyErrHas(problems, "/plugins/config/unknown_provider") {
		t.Fatalf("Merge = %v, want the unknown provider reported", problems)
	}
	// The candidate carries the defaults, so a diagnostic command prints a whole
	// document beside the problem.
	obj, _ := ObjectOf(merged)
	if _, present := obj["layout"]; !present {
		t.Fatalf("the candidate lost its defaults: %s", merged)
	}
	// An unmergeable shape still has no candidate (M1 stays closed).
	merged, _, problems = defs.Merge(testKit(), jsontext.Value(`{"plugins":null}`))
	if len(problems) != 0 || !strings.Contains(string(merged), `null`) {
		t.Fatalf("an invalid plugins shape = (%s, %v), want it preserved and the decoder's to report", merged, problems)
	}
}

// TestR2_M11_SetAtCreateCreatesMembersButNeverElements: the merge creates a
// config member the document omits, while a rule the document does not hold
// stays a problem.
func TestR2_M11_SetAtCreateCreatesMembersButNeverElements(t *testing.T) {
	got, err := setAtCreate(jsontext.Value(`{}`), "/a", jsontext.Value(`1`))
	if err != nil || string(got) != `{"a":1}` {
		t.Fatalf("setAtCreate leaf = (%s, %v)", got, err)
	}
	got, err = setAtCreate(jsontext.Value(`{}`), "/a/b", jsontext.Value(`2`))
	if err != nil || string(got) != `{"a":{"b":2}}` {
		t.Fatalf("setAtCreate nested = (%s, %v)", got, err)
	}
	if _, err := setAtCreate(jsontext.Value(`{"a":[]}`), "/a/9", jsontext.Value(`1`)); err == nil || !strings.Contains(err.Error(), "no such element") {
		t.Fatalf("setAtCreate accepted a missing array element: %v", err)
	}
	if _, err := setAtCreate(jsontext.Value(`{"a":[]}`), "/a/x", jsontext.Value(`1`)); err == nil || !strings.Contains(err.Error(), "no such element") {
		t.Fatalf("setAtCreate accepted a non-index array token: %v", err)
	}
	if _, err := setAtCreate(jsontext.Value(`[{"a":[]}]`), "/0/a/x", jsontext.Value(`1`)); err == nil || !strings.Contains(err.Error(), "no such element") {
		t.Fatalf("setAtCreate lost the element error below an array: %v", err)
	}
	if _, err := setAtCreate(jsontext.Value(`"x"`), "/a", jsontext.Value(`1`)); err == nil || !strings.Contains(err.Error(), "no such member") {
		t.Fatalf("setAtCreate walked into a scalar: %v", err)
	}
	if got, err := setAtCreate(jsontext.Value(`[1,2]`), "/1", jsontext.Value(`9`)); err != nil || string(got) != `[1,9]` {
		t.Fatalf("setAtCreate list element = (%s, %v)", got, err)
	}
}

// TestR2_M11_UnplaceableRuleKeepsTheCandidate: a rule hook whose ConfigPath the
// document cannot hold is a merge problem — the candidate survives with every
// other default, and the problem refuses operations. With the old
// `out, err = setAtCreate(...)` assignment the document became nil exactly on
// this path, so no caller could print the candidate (RED before the fix).
func TestR2_M11_UnplaceableRuleKeepsTheCandidate(t *testing.T) {
	schema := testKit()
	schema.configs["discovery/rulefive"] = reflect.TypeFor[ruleConfig]()
	schema.hooks["discovery/rulefive"] = func(jsontext.Value) ([]RuleSource, error) {
		return []RuleSource{{
			Path: "/rules/0", ConfigPath: "/rules/5/source/config",
			Type: "testkit", Config: jsontext.Value(`{"address":"x"}`),
		}}, nil
	}
	defs := loadDefaults(t, schema, testBlocks)
	user := jsontext.Value(`{"discoveries":{"d":{"type":"rulefive","config":{"rules":[{"name":"r","source":{"type":"testkit","config":{}},"pipeline":[],"min_matches":0}]}}}}`)
	merged, pointers, problems := defs.Merge(schema, user)
	if len(merged) == 0 {
		t.Fatal("the unplaceable rule left no candidate: the merge document became nil")
	}
	if !anyErrHas(problems, "no such element") {
		t.Fatalf("Merge = %v, want the unplaceable path reported", problems)
	}
	// The candidate is the whole merged document: the discovery entry the file
	// declared and the defaults every command reads.
	body := string(merged)
	for _, want := range []string{`"discoveries"`, `"rulefive"`, `"layout"`, `"plugins"`} {
		if !strings.Contains(body, want) {
			t.Errorf("the candidate lacks %q: %s", want, body)
		}
	}
	if len(pointers) == 0 {
		t.Error("the candidate lost its defaulted pointers")
	}
}

// TestR9_M20_SelectionRulesAreOneDefinition runs every rule for both lists
// through the one definition twice: a normal input whose positions are the
// document indices, and a tolerant input that skipped a malformed element so
// the reported index is the original one. Same errors, same indices, same
// valid entries, same effects — and Select agrees with the definition.
func TestR9_M20_SelectionRulesAreOneDefinition(t *testing.T) {
	compiled := []string{"source/a", "source/b", "processor/x"}
	table := []Bundle{{Name: "minimal", Disables: []string{"processor/x"}}}
	cases := []struct {
		name     string
		bundles  []SelectionRef
		disabled []SelectionRef
		want     []string
	}{
		{
			name:     "known entries are valid",
			bundles:  []SelectionRef{{Index: 0, Name: "minimal"}},
			disabled: []SelectionRef{{Index: 0, Name: "source/a"}},
		},
		{
			name:    "an empty bundle name",
			bundles: []SelectionRef{{Index: 2, Name: ""}},
			want:    []string{"/plugins/bundles/2 is empty"},
		},
		{
			name:    "an unknown bundle",
			bundles: []SelectionRef{{Index: 4, Name: "nope"}},
			want:    []string{`/plugins/bundles/4: unknown bundle "nope" (known: [minimal])`},
		},
		{
			name:    "a repeated bundle keeps the first index valid",
			bundles: []SelectionRef{{Index: 1, Name: "minimal"}, {Index: 3, Name: "minimal"}},
			want:    []string{`/plugins/bundles/3: bundle "minimal" is listed twice`},
		},
		{
			name:     "an empty disabled entry is not a plugin ID",
			disabled: []SelectionRef{{Index: 1, Name: ""}},
			want:     []string{`/plugins/disabled/1: "" is not a plugin ID`},
		},
		{
			name:     "a malformed disabled ID",
			disabled: []SelectionRef{{Index: 0, Name: "source/"}},
			want:     []string{`/plugins/disabled/0: "source/" is not a plugin ID`},
		},
		{
			name:     "an unknown plugin",
			disabled: []SelectionRef{{Index: 2, Name: "source/ghost"}},
			want:     []string{`/plugins/disabled/2: unknown plugin "source/ghost"`},
		},
		{
			name:     "a repeated plugin",
			disabled: []SelectionRef{{Index: 0, Name: "source/a"}, {Index: 5, Name: "source/a"}},
			want:     []string{`/plugins/disabled/5: plugin "source/a" is listed twice`},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			validBundles, validDisabled, errs := CheckSelection(c.bundles, c.disabled, compiled, []string{"minimal"})
			for _, want := range c.want {
				if !anyErrHas(errs, want) {
					t.Fatalf("CheckSelection = %v, want %q", errs, want)
				}
			}
			if len(errs) != len(c.want) {
				t.Fatalf("CheckSelection = %v, want exactly %d problems", errs, len(c.want))
			}
			// The normal path projects the same entries from a decoded value; the
			// tolerant path keeps its original indices. Both reach this one
			// definition and produce the same effects.
			normal := &Plugins{Bundles: refNames(c.bundles), Disabled: refNames(c.disabled)}
			bundleRefs, disabledRefs := normal.SelectionRefs()
			nBundles, nDisabled, nErrs := CheckSelection(bundleRefs, disabledRefs, compiled, []string{"minimal"})
			if len(nErrs) != len(errs) {
				t.Fatalf("the normal path reports %v, the tolerant path %v", nErrs, errs)
			}
			if effects := SelectionEffects(validBundles, validDisabled, table); !slices.Equal(effects, SelectionEffects(nBundles, nDisabled, table)) {
				t.Fatalf("the two paths select differently: %v vs %v", effects, SelectionEffects(nBundles, nDisabled, table))
			}
			// Select applies through the same definition: its active set is the
			// compiled set minus the effects.
			active, selErrs := normal.Select(compiled, table)
			if len(selErrs) != len(errs) {
				t.Fatalf("Select = %v, the definition = %v", selErrs, errs)
			}
			off := SelectionEffects(validBundles, validDisabled, table)
			for _, id := range compiled {
				if got, want := active.Enabled(id), !slices.Contains(off, id); got != want {
					t.Errorf("Select left %s active=%v, want %v", id, got, want)
				}
			}
		})
	}
	// A tolerant read that skipped a malformed element keeps the later entry's
	// original index: [7, "source/ghost"] reports index 1, never 0.
	_, _, errs := CheckSelection(nil, []SelectionRef{{Index: 1, Name: "source/ghost"}}, compiled, nil)
	if !anyErrHas(errs, "/plugins/disabled/1") {
		t.Fatalf("the tolerant index was renumbered: %v", errs)
	}
}

func refNames(refs []SelectionRef) []string {
	out := make([]string, 0, len(refs))
	for _, r := range refs {
		out = append(out, r.Name)
	}
	return out
}

// TestR9_M20_SelectionEffectsDedupe: the one application turns an ID off once —
// a bundle disabling it twice, an empty table entry, and a disabled entry that
// repeats a bundle's disable all collapse to one effect, first writer wins.
func TestR9_M20_SelectionEffectsDedupe(t *testing.T) {
	table := []Bundle{{Name: "both", Disables: []string{"source/b", "", "source/b"}}}
	effects := SelectionEffects([]SelectionRef{{Index: 0, Name: "both"}}, []SelectionRef{{Index: 0, Name: "source/b"}}, table)
	if !slices.Equal(effects, []string{"source/b"}) {
		t.Fatalf("SelectionEffects = %v, want one source/b", effects)
	}
	if got := SelectionEffects(nil, nil, nil); len(got) != 0 {
		t.Fatalf("SelectionEffects of nothing = %v", got)
	}
}
