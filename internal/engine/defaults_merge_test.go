package engine

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"log/slog"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/discovery"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/processor"
	"github.com/vi-latyshev/go-cluster-backup/internal/source"
)

// TestDefaultsGet: a pointer walks the unioned document; anything the document
// does not hold there is nil.
func TestDefaultsGet(t *testing.T) {
	var absent *Defaults
	if got := absent.Get("/plugins"); got != nil {
		t.Errorf("a nil Defaults returns %s, want nil", got)
	}
	defs := loadDefaults(t, defaultsRegistry(t), testBlocks)
	cases := []struct {
		name    string
		pointer string
		want    string
		missing bool
	}{
		{name: "a leaf", pointer: "/run/finish_allowance", want: `"2m"`},
		{name: "the root", pointer: "/", want: ""},
		{name: "an absent member", pointer: "/plugins/source/absent", missing: true},
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
	defs := loadDefaults(t, defaultsRegistry(t), testBlocks)
	got, ok := plugin.ObjectOf(defs.PluginBlock("source", "testkit"))
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
}

func TestDefaultsInstanceBlock(t *testing.T) {
	defs := loadDefaults(t, defaultsRegistry(t), testBlocks)
	block, ok := plugin.ObjectOf(defs.InstanceBlock("source"))
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
	reg := defaultsRegistry(t)
	defs := loadDefaults(t, reg, testBlocks)
	example := jsontext.Value(`{"address": "db:5432", "timeout": "1h"}`)

	got, err := defs.DefaultsConfig(reg, "source", "testkit", example)
	if err != nil {
		t.Fatalf("DefaultsConfig: %v", err)
	}
	if string(got) != `{"address":"db:5432","timeout":"30s"}` {
		t.Errorf("DefaultsConfig = %s, want the block's 30s over the example's own address", got)
	}

	// A registered type the document has no block for: the example is returned
	// untouched.
	bare := &Defaults{doc: jsontext.Value(`{}`)}
	if got, err := bare.DefaultsConfig(reg, "source", "testkit", example); err != nil || string(got) != string(example) {
		t.Errorf("DefaultsConfig with no block = (%s, %v), want the example unchanged", got, err)
	}

	// An unregistered plugin is refused with its name.
	if _, err := defs.DefaultsConfig(reg, "source", "ghost", example); err == nil || !strings.Contains(err.Error(), "not a registered plugin") {
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
	defs := loadDefaults(t, defaultsRegistry(t), testBlocks)
	raw := jsontext.Value(`[]`)
	got, pointers, err := defs.Merge(context.Background(), defaultsRegistry(t), slog.New(slog.DiscardHandler), raw)
	if err != nil || string(got) != string(raw) || pointers != nil {
		t.Fatalf("Merge of an array = (%s, %v, %v), want the bytes back unchanged", got, pointers, err)
	}
}

// TestMergeInstancesReturnsANonObjectDocumentWhole.
func TestMergeInstancesReturnsANonObjectDocumentWhole(t *testing.T) {
	defs := loadDefaults(t, defaultsRegistry(t), testBlocks)
	got, pointers := defs.mergeInstances(defaultsRegistry(t), jsontext.Value(`[]`))
	if string(got) != `[]` || pointers != nil {
		t.Fatalf("mergeInstances of an array = (%s, %v), want it back unchanged", got, pointers)
	}
}

// TestMergeInstancesReachesTheSingleSectionAndSkipsWhatIsNotAnEntry: the
// catalog is one instance rather than a map, and an entry or a retention that
// is not an object is left alone.
func TestMergeInstancesReachesTheSingleSectionAndSkipsWhatIsNotAnEntry(t *testing.T) {
	defs := loadDefaults(t, defaultsRegistry(t), testBlocks)
	reg := defaultsRegistry(t)

	out, _ := defs.mergeInstances(reg, jsontext.Value(`{"catalog":{"type":"testkit"}}`))
	obj, _ := plugin.ObjectOf(out)
	if _, present := obj["catalog"]; !present {
		t.Fatalf("mergeInstances dropped the single catalog section: %s", out)
	}
	if out, _ := defs.mergeInstances(reg, jsontext.Value(`{"storages":{"s":[]}}`)); string(out) != `{"storages":{"s":[]}}` {
		t.Fatalf("mergeInstances changed a storage that is not an object: %s", out)
	}
	out, _ = defs.mergeInstances(reg, jsontext.Value(`{"storages":{"s":{"retention":{"scheduled":[]}}}}`))
	entry, _ := plugin.ObjectOf(at(t, out, "/storages/s/retention"))
	if _, present := entry["scheduled"]; !present {
		t.Fatalf("mergeInstances dropped a retention that is not an object: %s", out)
	}
}

// TestMergeEntriesSkipsANonObjectEntry.
func TestMergeEntriesSkipsANonObjectEntry(t *testing.T) {
	defs := loadDefaults(t, defaultsRegistry(t), testBlocks)
	sec, _ := sectionForKind("source")
	entries := map[string]jsontext.Value{"x": jsontext.Value(`[]`)}
	out, pointers := defs.mergeEntries(defaultsRegistry(t), nil, sec, entries, "/sources")
	if string(out) != `{"x":[]}` || pointers != nil {
		t.Fatalf("mergeEntries = (%s, %v), want the non-object entry left alone", out, pointers)
	}
}

// TestMergeInstanceShapes: an entry with no type, an unregistered type, a type
// with no block, and an entry the file states no config for at all.
func TestMergeInstanceShapes(t *testing.T) {
	reg := defaultsRegistry(t)
	sec, _ := sectionForKind("source")

	t.Run("no type", func(t *testing.T) {
		out, _ := mergeInstance(map[string]jsontext.Value{}, reg, sec,
			map[string]jsontext.Value{"config": jsontext.Value(`{"address":"x"}`)}, "/sources/x")
		if !strings.Contains(string(out), `"address":"x"`) {
			t.Fatalf("mergeInstance without a type = %s, want the entry itself", out)
		}
	})
	t.Run("a type with no block", func(t *testing.T) {
		out, pointers := mergeInstance(map[string]jsontext.Value{}, reg, sec,
			map[string]jsontext.Value{"type": jsontext.Value(`"testkit"`)}, "/sources/x")
		if len(pointers) != 0 || !strings.Contains(string(out), `"testkit"`) {
			t.Fatalf("mergeInstance without a block = (%s, %v), want no pointers", out, pointers)
		}
	})
	t.Run("a block for an unregistered type", func(t *testing.T) {
		def := map[string]jsontext.Value{"plugins": jsontext.Value(`{"source/ghost":{"a":1}}`)}
		out, pointers := mergeInstance(def, reg, sec,
			map[string]jsontext.Value{"type": jsontext.Value(`"ghost"`), "config": jsontext.Value(`{}`)}, "/sources/x")
		if len(pointers) != 0 {
			t.Fatalf("mergeInstance with an unregistered type = (%s, %v), want no pointers", out, pointers)
		}
	})
	t.Run("a config the file does not declare", func(t *testing.T) {
		def := map[string]jsontext.Value{"plugins": jsontext.Value(`{"source/testkit":{"timeout":"30s"}}`)}
		out, pointers := mergeInstance(def, reg, sec,
			map[string]jsontext.Value{"type": jsontext.Value(`"testkit"`)}, "/sources/x")
		obj, _ := plugin.ObjectOf(out)
		cfg, ok := plugin.ObjectOf(obj["config"])
		if !ok || string(cfg["timeout"]) != `"30s"` {
			t.Fatalf("mergeInstance did not create the config from the block: %s", out)
		}
		if len(pointers) == 0 || pointers[0] != "/sources/x/config/timeout" {
			t.Fatalf("the created config's leaves are not pointed at: %v", pointers)
		}
	})
}

// covDiscovery is a discovery whose rule sources the test states, so the merge's
// per-rule branches are reachable without a cluster.
type covDiscovery struct {
	rules  []discovery.RuleSource
	err    error
	closed *bool
}

func (d *covDiscovery) Discover(context.Context) ([]discovery.Found, error) { return nil, nil }

func (d *covDiscovery) RuleSources(context.Context) ([]discovery.RuleSource, error) {
	return d.rules, d.err
}

func (d *covDiscovery) Close() error {
	if d.closed != nil {
		*d.closed = true
	}
	return nil
}

// plainDiscovery is a discovery that does not report its rules at all.
type plainDiscovery struct{}

func (plainDiscovery) Discover(context.Context) ([]discovery.Found, error) { return nil, nil }

// covDiscoveryRegistry registers one discovery type whose instance the caller
// supplies, so a test can hand the merge a rule list of its choosing.
func covDiscoveryRegistry(t *testing.T, inst discovery.Discoverer) *plugin.Registry {
	t.Helper()
	reg := plugin.NewRegistry()
	err := plugin.Register(reg, discovery.Kind, plugin.Spec[ruleConfig, discovery.Discoverer]{
		Type:    "testkit",
		Example: []byte(`{"rules": []}`),
		New: func(context.Context, ruleConfig, plugin.Deps) (discovery.Discoverer, error) {
			return inst, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = plugin.Register(reg, source.Kind, plugin.Spec[sourceConfig, source.Source]{
		Type:    "testkit",
		Example: []byte(`{"address": "db:5432", "timeout": "30s"}`),
		New: func(context.Context, sourceConfig, plugin.Deps) (source.Source, error) {
			return &quietSource{}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return reg
}

func ruleSource(typ, config, configPath string) discovery.RuleSource {
	return discovery.RuleSource{
		Path:       "/rules/0",
		ConfigPath: configPath,
		Type:       typ,
		Config:     jsontext.Value(config),
	}
}

// TestMergeRuleSourcesBranches walks the paths a discovery's own declaration
// opens: no blocks, a non-object document, a rule list whose entries and types
// the merge cannot use, and a rule whose config the document fills or refuses.
func TestMergeRuleSourcesBranches(t *testing.T) {
	ctx := context.Background()
	log := slog.New(slog.DiscardHandler)
	user := jsontext.Value(`{"version":1,"discoveries":{"d":{"type":"testkit","config":{"rules":[]}}}}`)

	t.Run("no blocks at all", func(t *testing.T) {
		defs := &Defaults{doc: jsontext.Value(`{"run":{"deadline":"1m"}}`)}
		out, pointers, err := defs.mergeRuleSources(ctx, defaultsRegistry(t), log, user)
		if err != nil || len(pointers) != 0 || string(out) != string(user) {
			t.Fatalf("mergeRuleSources without blocks = (%s, %v, %v)", out, pointers, err)
		}
	})
	t.Run("a non-object document", func(t *testing.T) {
		defs := &Defaults{doc: jsontext.Value(`{"plugins":{"source/testkit":{"timeout":"30s"}}}`)}
		out, pointers, err := defs.mergeRuleSources(ctx, defaultsRegistry(t), log, jsontext.Value(`[]`))
		if err != nil || len(pointers) != 0 || string(out) != `[]` {
			t.Fatalf("mergeRuleSources of an array = (%s, %v, %v)", out, pointers, err)
		}
	})
	t.Run("no discoveries", func(t *testing.T) {
		defs := &Defaults{doc: jsontext.Value(`{"plugins":{"source/testkit":{"timeout":"30s"}}}`)}
		out, pointers, err := defs.mergeRuleSources(ctx, defaultsRegistry(t), log, jsontext.Value(`{"version":1}`))
		if err != nil || len(pointers) != 0 {
			t.Fatalf("mergeRuleSources without discoveries = (%s, %v, %v)", out, pointers, err)
		}
	})
	t.Run("an entry that is not an object", func(t *testing.T) {
		defs := &Defaults{doc: jsontext.Value(`{"plugins":{"source/testkit":{"timeout":"30s"}}}`)}
		doc := jsontext.Value(`{"discoveries":{"d":[]}}`)
		if _, _, err := defs.mergeRuleSources(ctx, defaultsRegistry(t), log, doc); err != nil {
			t.Fatalf("mergeRuleSources: %v", err)
		}
	})
	t.Run("an entry with no type", func(t *testing.T) {
		defs := &Defaults{doc: jsontext.Value(`{"plugins":{"source/testkit":{"timeout":"30s"}}}`)}
		doc := jsontext.Value(`{"discoveries":{"d":{"config":{"rules":[]}}}}`)
		if _, _, err := defs.mergeRuleSources(ctx, defaultsRegistry(t), log, doc); err != nil {
			t.Fatalf("mergeRuleSources: %v", err)
		}
	})
	t.Run("a discovery that cannot be built", func(t *testing.T) {
		defs := &Defaults{doc: jsontext.Value(`{"plugins":{"source/testkit":{"timeout":"30s"}}}`)}
		doc := jsontext.Value(`{"discoveries":{"d":{"type":"ghost","config":{}}}}`)
		if _, _, err := defs.mergeRuleSources(ctx, defaultsRegistry(t), log, doc); err != nil {
			t.Fatalf("mergeRuleSources: %v", err)
		}
	})
	t.Run("a discovery that does not report its sources", func(t *testing.T) {
		reg := covDiscoveryRegistry(t, plainDiscovery{})
		defs := &Defaults{doc: jsontext.Value(`{"plugins":{"source/testkit":{"timeout":"30s"}}}`)}
		if _, _, err := defs.mergeRuleSources(ctx, reg, log, user); err != nil {
			t.Fatalf("mergeRuleSources: %v", err)
		}
	})
	t.Run("a discovery whose rule read fails", func(t *testing.T) {
		reg := covDiscoveryRegistry(t, &covDiscovery{err: errors.New("rule read failed")})
		defs := &Defaults{doc: jsontext.Value(`{"plugins":{"source/testkit":{"timeout":"30s"}}}`)}
		if _, _, err := defs.mergeRuleSources(ctx, reg, log, user); err != nil {
			t.Fatalf("mergeRuleSources: %v", err)
		}
	})
	t.Run("a rule whose source has no block", func(t *testing.T) {
		reg := covDiscoveryRegistry(t, &covDiscovery{rules: []discovery.RuleSource{
			ruleSource("testkit", `{}`, "/rules/0/source/config"),
		}})
		defs := &Defaults{doc: jsontext.Value(`{"plugins":{"source/other":{"timeout":"30s"}}}`)}
		if _, _, err := defs.mergeRuleSources(ctx, reg, log, user); err != nil {
			t.Fatalf("mergeRuleSources: %v", err)
		}
	})
	t.Run("a rule with an empty source type", func(t *testing.T) {
		reg := covDiscoveryRegistry(t, &covDiscovery{rules: []discovery.RuleSource{
			ruleSource("", `{}`, "/rules/0/source/config"),
		}})
		defs := &Defaults{doc: jsontext.Value(`{"plugins":{"source/testkit":{"timeout":"30s"}}}`)}
		if _, _, err := defs.mergeRuleSources(ctx, reg, log, user); err != nil {
			t.Fatalf("mergeRuleSources: %v", err)
		}
	})
	t.Run("a rule whose source type is not registered", func(t *testing.T) {
		reg := covDiscoveryRegistry(t, &covDiscovery{rules: []discovery.RuleSource{
			ruleSource("ghost", `{}`, "/rules/0/source/config"),
		}})
		defs := &Defaults{doc: jsontext.Value(`{"plugins":{"source/ghost":{"timeout":"30s"}}}`)}
		if _, _, err := defs.mergeRuleSources(ctx, reg, log, user); err != nil {
			t.Fatalf("mergeRuleSources: %v", err)
		}
	})
	t.Run("a rule the block fills nothing in", func(t *testing.T) {
		reg := covDiscoveryRegistry(t, &covDiscovery{rules: []discovery.RuleSource{
			ruleSource("testkit", `{"address":"x","timeout":"30s"}`, "/rules/0/source/config"),
		}})
		defs := &Defaults{doc: jsontext.Value(`{"plugins":{"source/testkit":{"timeout":"30s"}}}`)}
		if _, _, err := defs.mergeRuleSources(ctx, reg, log, user); err != nil {
			t.Fatalf("mergeRuleSources: %v", err)
		}
	})
	t.Run("a rule whose path the document does not hold", func(t *testing.T) {
		reg := covDiscoveryRegistry(t, &covDiscovery{rules: []discovery.RuleSource{
			ruleSource("testkit", `{"address":"x"}`, "/rules/9/source/config"),
		}})
		defs := &Defaults{doc: jsontext.Value(`{"plugins":{"source/testkit":{"timeout":"30s"}}}`)}
		if _, _, err := defs.mergeRuleSources(ctx, reg, log, user); err == nil {
			t.Fatal("mergeRuleSources accepted a rule whose path the document does not hold")
		}
	})
	t.Run("a rule the block fills", func(t *testing.T) {
		reg := covDiscoveryRegistry(t, &covDiscovery{rules: []discovery.RuleSource{
			ruleSource("testkit", `{"address":"x"}`, "/rules/0/source/config"),
		}})
		defs := &Defaults{doc: jsontext.Value(`{"plugins":{"source/testkit":{"timeout":"30s"}}}`)}
		doc := jsontext.Value(`{"discoveries":{"d":{"type":"testkit","config":{"rules":[{"name":"r","source":{"type":"testkit","config":{"address":"x"}},"pipeline":["pack"],"min_matches":0}]}}}}`)
		out, pointers, err := defs.mergeRuleSources(ctx, reg, log, doc)
		if err != nil {
			t.Fatalf("mergeRuleSources: %v", err)
		}
		if !strings.Contains(string(out), `"timeout":"30s"`) {
			t.Fatalf("the rule's source config was not filled: %s", out)
		}
		if len(pointers) != 1 || pointers[0] != "/discoveries/d/config/rules/0/source/config/timeout" {
			t.Fatalf("the rule's pointer is %v, want the nested path", pointers)
		}
	})
	t.Run("a discovery that closes", func(t *testing.T) {
		var closed bool
		reg := covDiscoveryRegistry(t, &covDiscovery{closed: &closed})
		defs := &Defaults{doc: jsontext.Value(`{"plugins":{"source/testkit":{"timeout":"30s"}}}`)}
		if _, _, err := defs.mergeRuleSources(ctx, reg, log, user); err != nil {
			t.Fatalf("mergeRuleSources: %v", err)
		}
		if !closed {
			t.Error("the discovery the merge built was never closed")
		}
	})
}

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
}

// mapConfig is a plugin config carrying a map, so the walk and the merge both
// enter the map branch a config with only scalars never reaches.
type mapConfig struct {
	Labels map[string]string `json:"labels"`
}

func mapConfigRegistry(t *testing.T) *plugin.Registry {
	t.Helper()
	reg := plugin.NewRegistry()
	err := plugin.Register(reg, processor.Kind, plugin.Spec[mapConfig, processor.Processor]{
		Type:    "mapper",
		Example: []byte(`{"labels":{"a":"1"}}`),
		New: func(context.Context, mapConfig, plugin.Deps) (processor.Processor, error) {
			return quietProcessor{}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return reg
}

// TestTheCheckAndTheMergeWalkAMapConfig: a plugin config whose field is a map is
// classified key by key, and the block's keys merge beside the file's own.
func TestTheCheckAndTheMergeWalkAMapConfig(t *testing.T) {
	reg := mapConfigRegistry(t)
	defs, err := LoadDefaults(reg, []jsontext.Value{defaultsDoc(t, map[string]string{
		"processor/mapper": `labels: {a: "1"}`,
	})})
	if err != nil {
		t.Fatalf("the document must be one the check accepts: %v", err)
	}
	user := jsontext.Value(`{"version":1,"processors":{"m":{"type":"mapper","config":{"labels":{"b":"2"}}}}}`)
	merged, pointers, err := defs.Merge(context.Background(), reg, slog.New(slog.DiscardHandler), user)
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}
	cfg, _ := plugin.ObjectOf(at(t, merged, "/processors/m/config/labels"))
	if string(cfg["a"]) != `"1"` || string(cfg["b"]) != `"2"` {
		t.Fatalf("the merged map = %s, want the block's key beside the file's", at(t, merged, "/processors/m/config/labels"))
	}
	if !slices.Contains(pointers, "/processors/m/config/labels/a") {
		t.Fatalf("the merged map's pointer is %v, want the block's key", pointers)
	}
}
