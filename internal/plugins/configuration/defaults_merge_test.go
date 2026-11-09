package configuration

import (
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"

	"encoding/json/jsontext"
)

// TestDefaultsGet: a pointer walks the unioned document; anything the document
// does not hold there is nil.
func TestDefaultsGet(t *testing.T) {
	var absent *Defaults
	if got := absent.Get("/plugins"); got != nil {
		t.Errorf("a nil Defaults returns %s, want nil", got)
	}
	defs := loadDefaults(t, testKit(), testBlocks)
	cases := []struct {
		name    string
		pointer string
		want    string
		missing bool
	}{
		{name: "a leaf", pointer: "/run/finish_allowance", want: `"2m"`},
		{name: "the root", pointer: "/", want: ""},
		{name: "an absent member", pointer: "/plugins/config/source/absent", missing: true},
		{name: "a member of a scalar", pointer: "/run/id_layout/x", missing: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := defs.Get(c.pointer)
			if c.missing {
				if got != nil {
					t.Fatalf("Get(%q) = %s, want nil", c.pointer, got)
				}
				return
			}
			if c.want != "" && string(got) != c.want {
				t.Fatalf("Get(%q) = %s, want %s", c.pointer, got, c.want)
			}
			if c.want == "" && len(got) == 0 {
				t.Fatalf("Get(%q) returned nothing", c.pointer)
			}
		})
	}
}

func TestDefaultsPluginBlock(t *testing.T) {
	defs := loadDefaults(t, testKit(), testBlocks)
	got, ok := ObjectOf(defs.PluginBlock("source", "testkit"))
	if !ok || string(got["timeout"]) != `"30s"` {
		t.Fatalf("PluginBlock(source, testkit) = %s, want the block's timeout", defs.PluginBlock("source", "testkit"))
	}
	if b := defs.PluginBlock("source", "absent"); b != nil {
		t.Errorf("PluginBlock of an undeclared type = %s, want nil", b)
	}
	// A document with no plugins object at all: the block is nil, not a panic.
	bare := &Defaults{doc: jsontext.Value(`{"run":{"deadline":"1m"}}`)}
	if b := bare.PluginBlock("source", "testkit"); b != nil {
		t.Errorf("PluginBlock without a plugins object = %s, want nil", b)
	}
	// A defaults block lives under /plugins/config, keyed by the whole ID.
	if b := defs.ConfigBlock("source/testkit"); b == nil {
		t.Error("ConfigBlock does not read the ID-keyed block")
	}
}

func TestDefaultsInstanceBlock(t *testing.T) {
	defs := loadDefaults(t, testKit(), testBlocks)
	block, ok := ObjectOf(defs.InstanceBlock("source"))
	if !ok {
		t.Fatalf("InstanceBlock(source) = %s, want the envelope object", defs.InstanceBlock("source"))
	}
	if _, present := block["artifact"]; !present {
		t.Errorf("the source envelope lost its artifact block: %s", defs.InstanceBlock("source"))
	}
	if b := defs.InstanceBlock("bogus"); b != nil {
		t.Errorf("InstanceBlock(bogus) = %s, want nil", b)
	}
}

// TestDefaultsConfigBuildsTheBlockOverTheExample: the block is removed from the
// example first, then merged under it, so the plugin judges the document's value
// and not the example's.
func TestDefaultsConfigBuildsTheBlockOverTheExample(t *testing.T) {
	schema := testKit()
	defs := loadDefaults(t, schema, testBlocks)
	example := jsontext.Value(`{"address": "db:5432", "timeout": "1h"}`)

	got, err := defs.DefaultsConfig(schema, "source", "testkit", example)
	if err != nil {
		t.Fatalf("DefaultsConfig: %v", err)
	}
	if string(got) != `{"address":"db:5432","timeout":"30s"}` {
		t.Errorf("DefaultsConfig = %s, want the block's 30s over the example's own address", got)
	}

	// A registered type the document has no block for: the example is returned
	// untouched.
	bare := &Defaults{doc: jsontext.Value(`{}`)}
	if got, err := bare.DefaultsConfig(schema, "source", "testkit", example); err != nil || string(got) != string(example) {
		t.Errorf("DefaultsConfig with no block = (%s, %v), want the example unchanged", got, err)
	}

	// An unregistered plugin is refused with its name.
	if _, err := defs.DefaultsConfig(schema, "source", "ghost", example); err == nil || !strings.Contains(err.Error(), "not a registered plugin") {
		t.Errorf("DefaultsConfig of an unregistered type = %v, want it refused", err)
	}
}

// TestWithoutValues: what a block states is removed from the example it merges
// over — a member, a nested object's member, and a `name[]` block's members
// inside every element of the list.
func TestWithoutValues(t *testing.T) {
	type inner struct {
		A string `json:"a"`
		B string `json:"b"`
	}
	type outer struct {
		Name  string  `json:"name"`
		Inner inner   `json:"inner"`
		Items []inner `json:"items"`
	}
	typ := reflect.TypeFor[outer]()
	cases := []struct {
		name    string
		example string
		block   string
		want    string
	}{
		{
			name:    "a member is removed",
			example: `{"name":"x","inner":{"a":"1","b":"2"}}`,
			block:   `{"name":"y"}`,
			want:    `{"inner":{"a":"1","b":"2"}}`,
		},
		{
			name:    "a nested object's member is removed",
			example: `{"inner":{"a":"1","b":"2"}}`,
			block:   `{"inner":{"a":"z"}}`,
			want:    `{"inner":{"b":"2"}}`,
		},
		{
			name:    "a name[] block trims every element",
			example: `{"items":[{"a":"1","b":"2"},{"a":"3"}]}`,
			block:   `{"items[]":{"a":"z"}}`,
			want:    `{"items":[{"b":"2"},{}]}`,
		},
		{
			name:    "a name[] block over no list leaves the example alone",
			example: `{"name":"x"}`,
			block:   `{"items[]":{"a":"z"}}`,
			want:    `{"name":"x"}`,
		},
		{
			name:    "a name[] key on a non-list field is ignored",
			example: `{"name":"x"}`,
			block:   `{"name[]":{"a":"z"}}`,
			want:    `{"name":"x"}`,
		},
		{
			name:    "a key no field has is ignored",
			example: `{"name":"x"}`,
			block:   `{"ghost":"z"}`,
			want:    `{"name":"x"}`,
		},
		{
			name:    "a member the example lacks is left alone",
			example: `{"name":"x"}`,
			block:   `{"inner":{"a":"z"}}`,
			want:    `{"name":"x"}`,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := withoutValues(typ, jsontext.Value(c.example), jsontext.Value(c.block))
			if string(got) != c.want {
				t.Fatalf("withoutValues = %s, want %s", got, c.want)
			}
		})
	}

	// Neither side an object: the example comes back whole.
	if got := withoutValues(typ, jsontext.Value(`"scalar"`), jsontext.Value(`{"name":"y"}`)); string(got) != `"scalar"` {
		t.Errorf("withoutValues with a non-object example = %s, want it unchanged", got)
	}
	if got := withoutValues(typ, jsontext.Value(`{"name":"x"}`), jsontext.Value(`"scalar"`)); string(got) != `{"name":"x"}` {
		t.Errorf("withoutValues with a non-object block = %s, want the example unchanged", got)
	}
}

// TestMergeLeavesANonObjectUserDocumentAlone: the strict decoder reports the
// shape, so the merge hands the bytes back untouched.
func TestMergeLeavesANonObjectUserDocumentAlone(t *testing.T) {
	defs := loadDefaults(t, testKit(), testBlocks)
	raw := jsontext.Value(`[]`)
	got, pointers, problems := defs.Merge(testKit(), raw)
	if len(problems) != 0 || string(got) != string(raw) || pointers != nil {
		t.Fatalf("Merge of an array = (%s, %v, %v), want the bytes back unchanged", got, pointers, problems)
	}
	// A nil Defaults is the distribution without a document.
	var absent *Defaults
	got, pointers, problems = absent.Merge(testKit(), raw)
	if len(problems) != 0 || string(got) != string(raw) || pointers != nil {
		t.Fatalf("Merge through a nil Defaults = (%s, %v, %v), want the bytes back unchanged", got, pointers, problems)
	}
}

// TestMergeInstancesReturnsANonObjectDocumentWhole.
func TestMergeInstancesReturnsANonObjectDocumentWhole(t *testing.T) {
	got, pointers := mergeInstances(testKit(), nil, jsontext.Value(`[]`), nil, nil)
	if string(got) != `[]` || pointers != nil {
		t.Fatalf("mergeInstances of an array = (%s, %v), want it back unchanged", got, pointers)
	}
}

// TestMergeInstancesReachesTheSingleSectionAndSkipsWhatIsNotAnEntry: the
// catalog is one instance rather than a map, and an entry or a retention that
// is not an object is left alone.
func TestMergeInstancesReachesTheSingleSectionAndSkipsWhatIsNotAnEntry(t *testing.T) {
	schema := testKit()

	out, _ := mergeInstances(schema, nil, jsontext.Value(`{"catalog":{"type":"testkit"}}`), nil, nil)
	obj, _ := ObjectOf(out)
	if _, present := obj["catalog"]; !present {
		t.Fatalf("mergeInstances dropped the single catalog section: %s", out)
	}
	if out, _ := mergeInstances(schema, nil, jsontext.Value(`{"storages":{"s":[]}}`), nil, nil); string(out) != `{"storages":{"s":[]}}` {
		t.Fatalf("mergeInstances changed a storage that is not an object: %s", out)
	}
	out, _ = mergeInstances(schema, nil, jsontext.Value(`{"storages":{"s":{"retention":{"scheduled":[]}}}}`), nil, nil)
	entry, _ := ObjectOf(at(t, out, "/storages/s/retention"))
	if _, present := entry["scheduled"]; !present {
		t.Fatalf("mergeInstances dropped a retention that is not an object: %s", out)
	}
}

// TestMergeEntriesSkipsANonObjectEntry.
func TestMergeEntriesSkipsANonObjectEntry(t *testing.T) {
	sec, _ := sectionForKind(KindSource)
	entries := map[string]jsontext.Value{"x": jsontext.Value(`[]`)}
	out, pointers := mergeEntries(testKit(), nil, nil, nil, sec, entries, "/sources")
	if string(out) != `{"x":[]}` || pointers != nil {
		t.Fatalf("mergeEntries = (%s, %v), want the non-object entry left alone", out, pointers)
	}
}

// TestMergeInstanceShapes: an entry with no type, an unregistered type, a type
// with no block, and an entry the file states no config for at all.
func TestMergeInstanceShapes(t *testing.T) {
	schema := testKit()
	sec, _ := sectionForKind(KindSource)

	t.Run("no type", func(t *testing.T) {
		out, _ := mergeInstance(map[string]jsontext.Value{}, schema, nil, nil, sec,
			map[string]jsontext.Value{"config": jsontext.Value(`{"address":"x"}`)}, "/sources/x")
		if !strings.Contains(string(out), `"address":"x"`) {
			t.Fatalf("mergeInstance without a type = %s, want the entry itself", out)
		}
	})
	t.Run("a type with no block", func(t *testing.T) {
		out, pointers := mergeInstance(map[string]jsontext.Value{}, schema, nil, nil, sec,
			map[string]jsontext.Value{"type": jsontext.Value(`"testkit"`)}, "/sources/x")
		if len(pointers) != 0 || !strings.Contains(string(out), `"testkit"`) {
			t.Fatalf("mergeInstance without a block = (%s, %v), want no pointers", out, pointers)
		}
	})
	t.Run("a block for an unregistered type", func(t *testing.T) {
		blocks := map[string]jsontext.Value{"source/ghost": jsontext.Value(`{"a":1}`)}
		out, pointers := mergeInstance(map[string]jsontext.Value{}, schema, blocks, nil, sec,
			map[string]jsontext.Value{"type": jsontext.Value(`"ghost"`), "config": jsontext.Value(`{}`)}, "/sources/x")
		if len(pointers) != 0 {
			t.Fatalf("mergeInstance with an unregistered type = (%s, %v), want no pointers", out, pointers)
		}
	})
	t.Run("a config the file does not declare", func(t *testing.T) {
		blocks := map[string]jsontext.Value{"source/testkit": jsontext.Value(`{"timeout":"30s"}`)}
		out, pointers := mergeInstance(map[string]jsontext.Value{}, schema, nil, blocks, sec,
			map[string]jsontext.Value{"type": jsontext.Value(`"testkit"`)}, "/sources/x")
		obj, _ := ObjectOf(out)
		cfg, ok := ObjectOf(obj["config"])
		if !ok || string(cfg["timeout"]) != `"30s"` {
			t.Fatalf("mergeInstance did not create the config from the block: %s", out)
		}
		if len(pointers) == 0 || pointers[0] != "/sources/x/config/timeout" {
			t.Fatalf("the created config's leaves are not pointed at: %v", pointers)
		}
	})
}

// ruleDefaults is a defaults document whose only content is one source block:
// the provenance side of the rule merge.
func ruleDefaults(block jsontext.Value) *Defaults {
	return &Defaults{doc: encode(map[string]jsontext.Value{
		"plugins": encode(map[string]jsontext.Value{
			"config": encode(map[string]jsontext.Value{"source/testkit": block}),
		}),
	})}
}

func ruleSource(typ, config, configPath string) RuleSource {
	return RuleSource{
		Path:       "/rules/0",
		ConfigPath: configPath,
		Type:       typ,
		Config:     jsontext.Value(config),
	}
}

// failingHook is a rule hook that cannot read its config: the merge leaves the
// document alone and the offline check reports the failure.
func failingHook(jsontext.Value) ([]RuleSource, error) {
	return nil, errors.New("rule read failed")
}

// TestMergeRuleSourcesBranches walks the paths a discovery's own declaration
// opens: no blocks, a non-object document, a rule list whose entries and types
// the merge cannot use, and a rule whose config the document fills or refuses.
func TestMergeRuleSourcesBranches(t *testing.T) {
	user := jsontext.Value(`{"discoveries":{"d":{"type":"testkit","config":{"rules":[]}}}}`)
	block := jsontext.Value(`{"timeout":"30s"}`)

	t.Run("no blocks at all", func(t *testing.T) {
		defs := &Defaults{doc: jsontext.Value(`{"run":{"deadline":"1m"}}`)}
		out, pointers, err := defs.mergeRuleSources(testKit(), user, nil, nil)
		if err != nil || len(pointers) != 0 || string(out) != string(user) {
			t.Fatalf("mergeRuleSources without blocks = (%s, %v, %v)", out, pointers, err)
		}
	})
	t.Run("a non-object document", func(t *testing.T) {
		out, pointers, err := (&Defaults{}).mergeRuleSources(testKit(), jsontext.Value(`[]`), map[string]jsontext.Value{"source/testkit": block}, map[string]jsontext.Value{"source/testkit": block})
		if err != nil || len(pointers) != 0 || string(out) != `[]` {
			t.Fatalf("mergeRuleSources of an array = (%s, %v, %v)", out, pointers, err)
		}
	})
	t.Run("no discoveries", func(t *testing.T) {
		out, pointers, err := (&Defaults{}).mergeRuleSources(testKit(), jsontext.Value(`{}`), map[string]jsontext.Value{"source/testkit": block}, map[string]jsontext.Value{"source/testkit": block})
		if err != nil || len(pointers) != 0 {
			t.Fatalf("mergeRuleSources without discoveries = (%s, %v, %v)", out, pointers, err)
		}
	})
	t.Run("a schema without the hook", func(t *testing.T) {
		out, pointers, err := (&Defaults{}).mergeRuleSources(noHookSchema{testKit()}, jsontext.Value(`{"discoveries":{"d":{"type":"testkit","config":{"rules":[]}}}}`), map[string]jsontext.Value{"source/testkit": block}, map[string]jsontext.Value{"source/testkit": block})
		if err != nil || len(pointers) != 0 || string(out) != `{"discoveries":{"d":{"type":"testkit","config":{"rules":[]}}}}` {
			t.Fatalf("mergeRuleSources without the hook = (%s, %v, %v)", out, pointers, err)
		}
	})
	t.Run("an entry that is not an object", func(t *testing.T) {
		if _, _, err := (&Defaults{}).mergeRuleSources(testKit(), jsontext.Value(`{"discoveries":{"d":[]}}`), map[string]jsontext.Value{"source/testkit": block}, map[string]jsontext.Value{"source/testkit": block}); err != nil {
			t.Fatalf("mergeRuleSources: %v", err)
		}
	})
	t.Run("an entry with no type", func(t *testing.T) {
		if _, _, err := (&Defaults{}).mergeRuleSources(testKit(), jsontext.Value(`{"discoveries":{"d":{"config":{"rules":[]}}}}`), map[string]jsontext.Value{"source/testkit": block}, map[string]jsontext.Value{"source/testkit": block}); err != nil {
			t.Fatalf("mergeRuleSources: %v", err)
		}
	})
	t.Run("a discovery whose hook does not answer", func(t *testing.T) {
		if _, _, err := (&Defaults{}).mergeRuleSources(testKit(), jsontext.Value(`{"discoveries":{"d":{"type":"ghost","config":{}}}}`), map[string]jsontext.Value{"source/testkit": block}, map[string]jsontext.Value{"source/testkit": block}); err != nil {
			t.Fatalf("mergeRuleSources: %v", err)
		}
	})
	t.Run("a discovery whose rule read fails", func(t *testing.T) {
		schema := testKit()
		schema.hooks["discovery/testkit"] = failingHook
		if _, _, err := (&Defaults{}).mergeRuleSources(schema, user, map[string]jsontext.Value{"source/testkit": block}, map[string]jsontext.Value{"source/testkit": block}); err != nil {
			t.Fatalf("mergeRuleSources: %v", err)
		}
	})
	t.Run("a rule whose source has no block", func(t *testing.T) {
		if _, _, err := (&Defaults{}).mergeRuleSources(testKit(), user, map[string]jsontext.Value{"source/other": block}, map[string]jsontext.Value{"source/other": block}); err != nil {
			t.Fatalf("mergeRuleSources: %v", err)
		}
	})
	t.Run("a rule with an empty source type", func(t *testing.T) {
		rules := []RuleSource{ruleSource("", `{}`, "/rules/0/source/config")}
		schema := testKit()
		schema.hooks["discovery/testkit"] = func(jsontext.Value) ([]RuleSource, error) { return rules, nil }
		if _, _, err := (&Defaults{}).mergeRuleSources(schema, user, map[string]jsontext.Value{"source/testkit": block}, map[string]jsontext.Value{"source/testkit": block}); err != nil {
			t.Fatalf("mergeRuleSources: %v", err)
		}
	})
	t.Run("a rule whose source type is not registered", func(t *testing.T) {
		rules := []RuleSource{ruleSource("ghost", `{}`, "/rules/0/source/config")}
		schema := testKit()
		schema.hooks["discovery/testkit"] = func(jsontext.Value) ([]RuleSource, error) { return rules, nil }
		if _, _, err := (&Defaults{}).mergeRuleSources(schema, user, map[string]jsontext.Value{"source/ghost": block}, map[string]jsontext.Value{"source/ghost": block}); err != nil {
			t.Fatalf("mergeRuleSources: %v", err)
		}
	})
	t.Run("a rule the block fills nothing in", func(t *testing.T) {
		rules := []RuleSource{ruleSource("testkit", `{"address":"x","timeout":"30s"}`, "/rules/0/source/config")}
		schema := testKit()
		schema.hooks["discovery/testkit"] = func(jsontext.Value) ([]RuleSource, error) { return rules, nil }
		if _, _, err := (&Defaults{}).mergeRuleSources(schema, user, map[string]jsontext.Value{"source/testkit": block}, map[string]jsontext.Value{"source/testkit": block}); err != nil {
			t.Fatalf("mergeRuleSources: %v", err)
		}
	})
	t.Run("a rule whose path the document does not hold", func(t *testing.T) {
		rules := []RuleSource{ruleSource("testkit", `{"address":"x"}`, "/rules/9/source/config")}
		schema := testKit()
		schema.hooks["discovery/testkit"] = func(jsontext.Value) ([]RuleSource, error) { return rules, nil }
		if _, _, err := ruleDefaults(block).mergeRuleSources(schema, user, map[string]jsontext.Value{"source/testkit": block}, map[string]jsontext.Value{"source/testkit": block}); err == nil {
			t.Fatal("mergeRuleSources accepted a rule whose path the document does not hold")
		}
	})
	t.Run("a rule without a config gets the deployment settings", func(t *testing.T) {
		rules := []RuleSource{ruleSource("testkit", "", "/rules/0/source/config")}
		schema := testKit()
		schema.hooks["discovery/testkit"] = func(jsontext.Value) ([]RuleSource, error) { return rules, nil }
		doc := jsontext.Value(`{"discoveries":{"d":{"type":"testkit","config":{"rules":[{"name":"r","source":{"type":"testkit"},"pipeline":[],"min_matches":0}]}}}}`)
		out, _, problems := (&Defaults{}).mergeRuleSources(schema, doc, map[string]jsontext.Value{"source/testkit": block}, nil)
		if len(problems) != 0 || !strings.Contains(string(out), `"timeout":"30s"`) {
			t.Fatalf("mergeRuleSources = (%s, %v), want the deployment settings created", out, problems)
		}
	})
	t.Run("a rule the block fills", func(t *testing.T) {
		filled := jsontext.Value(`{"discoveries":{"d":{"type":"testkit","config":{"rules":[{"name":"r","source":{"type":"testkit","config":{"address":"x"}},"pipeline":["pack"],"min_matches":0}]}}}}`)
		// The block as the deployment layer: the value applies but is the
		// deployment's, so no pointer names it (M5).
		out, pointers, problems := ruleDefaults(block).mergeRuleSources(testKit(), filled, map[string]jsontext.Value{"source/testkit": block}, nil)
		if len(problems) != 0 {
			t.Fatalf("mergeRuleSources: %v", problems)
		}
		if !strings.Contains(string(out), `"timeout":"30s"`) {
			t.Fatalf("the rule's source config was not filled: %s", out)
		}
		if len(pointers) != 0 {
			t.Fatalf("the rule's deployment value was reported as defaulted: %v", pointers)
		}
		// The block as the embedded layer alone: the value is a default and the
		// pointer names its escaped-free path (M12: applied even without it).
		out, pointers, problems = ruleDefaults(block).mergeRuleSources(testKit(), filled, nil, map[string]jsontext.Value{"source/testkit": block})
		if len(problems) != 0 || !strings.Contains(string(out), `"timeout":"30s"`) {
			t.Fatalf("mergeRuleSources with the embedded layer = (%s, %v)", out, problems)
		}
		if len(pointers) != 1 || pointers[0] != "/discoveries/d/config/rules/0/source/config/timeout" {
			t.Fatalf("the rule's pointer is %v, want the nested path", pointers)
		}
	})
}

// noHookSchema is a schema without the pure rule hook: the merge cannot see any
// nested source config and leaves the document alone.
type noHookSchema struct{ Schema }

// TestMergeValueShapes: the value-level branches of the merge — a null user
// value, a key no field has, and a type that is neither a struct nor a map.
func TestMergeValueShapes(t *testing.T) {
	typ := reflect.TypeFor[sourceConfig]()
	if got, ptrs := mergeValue(typ, jsontext.Value(`{"timeout":"30s"}`), jsontext.Value(`null`), ""); string(got) != `null` || ptrs != nil {
		t.Errorf("mergeValue under a null = (%s, %v), want the null back", got, ptrs)
	}
	if got, ptrs := mergeValue(typ, jsontext.Value(`{"ghost":1}`), jsontext.Value(`{"address":"x"}`), ""); string(got) != `{"address":"x"}` || ptrs != nil {
		t.Errorf("mergeValue with an unknown key = (%s, %v), want the user's value", got, ptrs)
	}
	if got, ptrs := mergeValue(typ, jsontext.Value(`{"ghost[]":{}}`), jsontext.Value(`{"address":"x"}`), ""); string(got) != `{"address":"x"}` || ptrs != nil {
		t.Errorf("mergeValue with an unknown name[] key = (%s, %v), want the user's value", got, ptrs)
	}
	if got, ptrs := mergeValue(reflect.TypeFor[int](), jsontext.Value(`{"a":1}`), jsontext.Value(`{"b":2}`), ""); string(got) != `{"b":2}` || ptrs != nil {
		t.Errorf("mergeValue of a scalar type = (%s, %v), want the user's value", got, ptrs)
	}
	if got, ptrs := mergeValue(reflect.TypeFor[sourceConfig](), jsontext.Value(`{}`), jsontext.Value(`[]`), ""); string(got) != `[]` || ptrs != nil {
		t.Errorf("mergeValue with no defaults object = (%s, %v), want the user's value", got, ptrs)
	}
}

// TestMergeElementsLeavesANonListAlone.
func TestMergeElementsLeavesANonListAlone(t *testing.T) {
	got, ptrs := mergeElements(reflect.TypeFor[sourceConfig](), jsontext.Value(`{}`), jsontext.Value(`"x"`), "")
	if string(got) != `"x"` || ptrs != nil {
		t.Fatalf("mergeElements of a non-list = (%s, %v), want it back unchanged", got, ptrs)
	}
}

// TestMergeValueMergesAMapKeyByKey: a plugin config carrying a map gets the
// block's keys added beside the file's own, and each added key is pointed at.
func TestMergeValueMergesAMapKeyByKey(t *testing.T) {
	got, ptrs := mergeValue(reflect.TypeFor[map[string]string](),
		jsontext.Value(`{"a":"1","b":"2"}`), jsontext.Value(`{"b":"9"}`), "/cfg")
	want := `{"a":"1","b":"9"}`
	if string(got) != want {
		t.Fatalf("mergeValue of a map = %s, want %s", got, want)
	}
	if len(ptrs) != 1 || ptrs[0] != "/cfg/a" {
		t.Fatalf("the added key is not pointed at: %v", ptrs)
	}
}

// TestLeafPointers: a value created whole is pointed at leaf by leaf, and a
// `name[]` key carries none.
func TestLeafPointers(t *testing.T) {
	if got := leafPointers(jsontext.Value(`{}`), "/p"); len(got) != 1 || got[0] != "/p" {
		t.Errorf("leafPointers of an empty object = %v, want the path itself", got)
	}
	got := leafPointers(jsontext.Value(`{"a":1,"b":{"c":2},"d[]":{}}`), "/p")
	want := map[string]bool{"/p/a": true, "/p/b/c": true}
	if len(got) != len(want) {
		t.Fatalf("leafPointers = %v, want %v", got, want)
	}
	for _, p := range got {
		if !want[p] {
			t.Errorf("leafPointers named %s, want only the leaves", p)
		}
	}
	if got := leafPointers(nil, "/p"); len(got) != 1 || got[0] != "/p" {
		t.Errorf("leafPointers of nothing = %v, want the path itself", got)
	}
}

// TestMergeInstanceKeepsAStatedConfigWhole: an entry whose config the file
// states and no block fills keeps its bytes as written.
func TestMergeInstanceKeepsAStatedConfigWhole(t *testing.T) {
	sec, _ := sectionForKind(KindSource)
	out, pointers := mergeInstance(map[string]jsontext.Value{}, testKit(), nil, nil, sec,
		map[string]jsontext.Value{"type": jsontext.Value(`"testkit"`), "config": jsontext.Value(`{"address":"x"}`)}, "/sources/x")
	obj, _ := ObjectOf(out)
	if got := string(obj["config"]); got != `{"address":"x"}` || len(pointers) != 0 {
		t.Fatalf("mergeInstance changed a stated config = (%s, %v)", got, pointers)
	}
}

// TestWalkStructSkipsWhatTheDecoderSkips: an ignored or untagged field is not
// classified here, exactly as the decoder ignores it.
func TestWalkStructSkipsWhatTheDecoderSkips(t *testing.T) {
	type skipped struct {
		Ignored string `json:"-"`
		NoTag   string
		hidden  string
	}
	// The unexported field holds a value and the walk still never sees it: the
	// decoder ignores it for the same reason.
	cfg := skipped{Ignored: "i", NoTag: "n", hidden: "h"}
	if cfg.hidden != "h" {
		t.Fatal("the unexported field is unreadable inside its own package")
	}
	if errs := walkStruct(reflect.TypeFor[skipped](), nil, ""); errs != nil {
		t.Fatalf("walkStruct classified a field the decoder skips: %v", errs)
	}
}

// TestMergeRefusesARulePathTheDocumentDoesNotHold: the rule merge error reaches
// the caller of Merge.
func TestMergeRefusesARulePathTheDocumentDoesNotHold(t *testing.T) {
	schema := testKit()
	schema.hooks["discovery/testkit"] = func(jsontext.Value) ([]RuleSource, error) {
		return []RuleSource{ruleSource("testkit", `{"address":"x"}`, "/rules/9/source/config")}, nil
	}
	defs := loadDefaults(t, schema, testBlocks)
	user := jsontext.Value(`{"discoveries":{"d":{"type":"testkit","config":{"rules":[{"name":"r","source":{"type":"testkit","config":{"address":"x"}},"pipeline":["pack"],"min_matches":0}]}}}}`)
	if _, _, problems := defs.Merge(schema, user); !anyErrHas(problems, "no such element") {
		t.Fatalf("Merge = %v, want the missing rule path reported", problems)
	}
}

// TestMergePluginsWithoutProviderSchema: a stable-ID block merges by
// replacement when the schema carries no provider types.
func TestMergePluginsWithoutProviderSchema(t *testing.T) {
	def := jsontext.Value(`{"config":{"reporting":{"timeout":"30s"}}}`)
	merged, _ := mergePlugins(noProvidersSchema{testKit()}, def, jsontext.Value(`{"config":{"reporting":{"timeout":"5s"}}}`))
	blocks, _ := ObjectOf(merged["config"])
	if got := string(blocks["reporting"]); got != `{"timeout":"5s"}` {
		t.Fatalf("a provider block without a provider schema = %s, want the deployment's whole", got)
	}
}

// TestTheCheckAndTheMergeWalkAMapConfig: a plugin config whose field is a map is
// classified key by key, and the block's keys merge beside the file's own.
func TestTheCheckAndTheMergeWalkAMapConfig(t *testing.T) {
	schema := testKit()
	defs, err := LoadDefaults(schema, []jsontext.Value{defaultsDoc(t, map[string]string{
		"processor/mapper": `labels: {a: "1"}`,
	})})
	if err != nil {
		t.Fatalf("the document must be one the check accepts: %v", err)
	}
	user := jsontext.Value(`{"processors":{"m":{"type":"mapper","config":{"labels":{"b":"2"}}}}}`)
	merged, pointers, problems := defs.Merge(schema, user)
	if len(problems) != 0 {
		t.Fatalf("Merge: %v", problems)
	}
	cfg, _ := ObjectOf(at(t, merged, "/processors/m/config/labels"))
	if string(cfg["a"]) != `"1"` || string(cfg["b"]) != `"2"` {
		t.Fatalf("the merged map = %s, want the block's key beside the file's", at(t, merged, "/processors/m/config/labels"))
	}
	if !slices.Contains(pointers, "/processors/m/config/labels/a") {
		t.Fatalf("the merged map's pointer is %v, want the block's key", pointers)
	}
}

// TestMergePluginsListsAndBlocks walks the plugins root itself: the deployment's
// lists replace the document's whole, a settings block the document does not
// carry is the deployment's alone, and one both carry merges through the type.
func TestMergePluginsListsAndBlocks(t *testing.T) {
	def := jsontext.Value(`{"bundles":["compiled"],"disabled":["processor/zstd"],"config":{"source/testkit":{"timeout":"30s"}}}`)

	merged, pointers := mergePlugins(testKit(), def, jsontext.Value(`{"bundles":["local-only"]}`))
	if got := string(merged["bundles"]); got != `["local-only"]` {
		t.Errorf("the deployment's bundles = %s, want its own list", got)
	}
	if got := string(merged["disabled"]); got != `["processor/zstd"]` {
		t.Errorf("the defaults' disabled list = %s, want it carried when the file states none", got)
	}
	if !slices.Contains(pointers, "/plugins/disabled") {
		t.Errorf("the defaulted list is not pointed at: %v", pointers)
	}

	merged, pointers = mergePlugins(testKit(), def, nil)
	if got := string(merged["bundles"]); got != `["compiled"]` {
		t.Errorf("a file with no plugins root keeps the defaults' bundles: %s", got)
	}
	if !slices.Contains(pointers, "/plugins/bundles") {
		t.Errorf("the defaulted bundles are not pointed at: %v", pointers)
	}

	// A block only the deployment carries, and one only the document carries.
	merged, pointers = mergePlugins(testKit(), def, jsontext.Value(`{"config":{"processor/testkit":{"concurrency":3},"source/testkit":{"address":"db"}}}`))
	blocks, _ := ObjectOf(merged["config"])
	if got := string(blocks["processor/testkit"]); got != `{"concurrency":3}` {
		t.Errorf("the deployment-only block = %s, want it whole", got)
	}
	cfg, _ := ObjectOf(blocks["source/testkit"])
	if string(cfg["timeout"]) != `"30s"` || string(cfg["address"]) != `"db"` {
		t.Errorf("the shared block = %s, want the default and the file's value", blocks["source/testkit"])
	}
	if !slices.Contains(pointers, "/plugins/config/source~1testkit/timeout") {
		t.Errorf("the defaulted setting is not pointed at: %v", pointers)
	}

	// A block only the document carries is defaulted whole.
	merged, pointers = mergePlugins(testKit(), def, nil)
	blocks, _ = ObjectOf(merged["config"])
	if got := string(blocks["source/testkit"]); got != `{"timeout":"30s"}` {
		t.Errorf("the document-only block = %s, want it carried", got)
	}
	if !slices.Contains(pointers, "/plugins/config/source~1testkit/timeout") {
		t.Errorf("the carried block is not pointed at: %v", pointers)
	}

	// A block whose type the schema does not know merges by replacement.
	unknown := jsontext.Value(`{"config":{"source/ghost":{"a":1}}}`)
	merged, _ = mergePlugins(testKit(), unknown, jsontext.Value(`{"config":{"source/ghost":{"b":2}}}`))
	blocks, _ = ObjectOf(merged["config"])
	if got := string(blocks["source/ghost"]); got != `{"b":2}` {
		t.Errorf("an unknown type's block = %s, want the deployment's whole", got)
	}
	// And with no plugins root in either layer there is nothing to carry.
	merged, pointers = mergePlugins(testKit(), nil, nil)
	if len(merged) != 0 || len(pointers) != 0 {
		t.Errorf("mergePlugins of two absent roots = (%v, %v), want nothing", merged, pointers)
	}
}

// TestPointsEscapeEveryReferenceToken: a name carrying a slash is one JSON
// pointer member, so provenance escapes it (RFC 6901), Get reads the escaped
// token, and the rule merge writes through it to the right member.
func TestPointsEscapeEveryReferenceToken(t *testing.T) {
	defs := loadDefaults(t, testKit(), testBlocks)
	// The escaped pointer reads the defaulted setting back.
	if got := string(defs.Get("/plugins/config/source~1testkit/timeout")); got != `"30s"` {
		t.Errorf("Get of an escaped factory ID = %s, want the block's timeout", got)
	}

	user := jsontext.Value(`{
	  "storages": {"s/1": {"type": "testkit", "time_budget": "1m", "config": {"root": "/r"}, "retention": {"manual": {"type": "testkit", "config": {"count": 3}}}}},
	  "discoveries": {"d/e": {"type": "testkit", "config": {"rules": [{"name": "r", "source": {"type": "testkit", "config": {"address": "x"}}, "pipeline": ["pack"], "min_matches": 0}]}}}
	}`)
	merged, pointers := merge(t, defs, user)
	for _, want := range []string{
		"/storages/s~11/retention/manual/config/floor",
		"/discoveries/d~1e/config/rules/0/source/config/timeout",
	} {
		if !slices.Contains(pointers, want) {
			t.Errorf("the pointers do not name %s: %v", want, pointers)
		}
	}
	// setAt decoded the discovery name: the rule's nested config carries the
	// deployment's own value.
	if body := string(merged); !strings.Contains(body, `"timeout":"30s"`) {
		t.Errorf("the rule's nested config was not filled through the escaped name: %s", body)
	}
	if body := string(merged); !strings.Contains(body, `"floor":1`) {
		t.Errorf("the retention inside a slash-named storage was not filled: %s", body)
	}
}
