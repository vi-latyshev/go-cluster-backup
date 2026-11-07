package configuration

import (
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"

	"encoding/json/jsontext"
	json "encoding/json/v2"

	"go.yaml.in/yaml/v4"

	confdefaults "github.com/vi-latyshev/go-cluster-backup/internal/plugins/configuration/defaults"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
)

// The defaults tests: one tiny distribution whose plugin configs are typed, so
// the check and the merge have real fields to classify, and the module's own
// document as the base — the configuration sections have to be complete (every
// field required or defaulted) and the module's document is the one place they
// are.

type sourceConfig struct {
	Address string         `json:"address" required:"true"`
	Timeout model.Duration `json:"timeout"`
}

type procConfig struct {
	Level       string `json:"level" required:"true"`
	Concurrency int    `json:"concurrency"`
}

type storageConfig struct {
	Root     string `json:"root" required:"true"`
	PageSize int    `json:"page_size"`
}

type retentionConfig struct {
	Count int `json:"count" required:"true"`
	Floor int `json:"floor"`
}

// mapConfig is a plugin config carrying a map, so the walk and the merge both
// enter the map branch a config with only scalars never reaches.
type mapConfig struct {
	Labels map[string]string `json:"labels"`
}

// providerConfig is the settings type of the one provider plugin the test
// schema carries.
type providerConfig struct {
	Timeout model.Duration `json:"timeout"`
	Retries int            `json:"retries"`
}

// ruleConfig is the test discovery's config: a list of rules, each with a
// nested source config — the shape whose bytes the strict decoder never walks.
type ruleConfig struct {
	Rules []rule `json:"rules" required:"true"`
}

type rule struct {
	Name       string     `json:"name" required:"true"`
	Source     sourceRule `json:"source" required:"true"`
	Pipeline   []string   `json:"pipeline" required:"true"`
	MinMatches int        `json:"min_matches"`
}

type sourceRule struct {
	Type   string         `json:"type" required:"true"`
	Config jsontext.Value `json:"config"`
}

// testRuleHook is the pure compiled hook the test discovery registers: it reads
// the nested rule sources out of the raw config without building anything.
func testRuleHook(raw jsontext.Value) ([]RuleSource, error) {
	var cfg ruleConfig
	if err := Decode(raw, &cfg); err != nil {
		return nil, err
	}
	out := make([]RuleSource, 0, len(cfg.Rules))
	for i, r := range cfg.Rules {
		out = append(out, RuleSource{
			Path:        fmt.Sprintf("/rules/%d", i),
			ConfigPath:  fmt.Sprintf("/rules/%d/source/config", i),
			Type:        r.Source.Type,
			Config:      r.Source.Config,
			Pipeline:    r.Pipeline,
			Importance:  model.ImportanceRequired,
			Sensitivity: model.SensitivityPlain,
		})
	}
	return out, nil
}

// testSchema is the compiled schema the merge and the check read: the config
// type of every test factory, the pure rule hook of the test discovery, and the
// settings type of one provider.
type testSchema struct {
	configs   map[string]reflect.Type
	hooks     map[string]RuleExtractor
	providers map[string]reflect.Type
}

func testKit() *testSchema {
	return &testSchema{
		configs: map[string]reflect.Type{
			"discovery/testkit": reflect.TypeFor[ruleConfig](),
			"source/testkit":    reflect.TypeFor[sourceConfig](),
			"processor/testkit": reflect.TypeFor[procConfig](),
			"storage/testkit":   reflect.TypeFor[storageConfig](),
			"retention/testkit": reflect.TypeFor[retentionConfig](),
			"processor/mapper":  reflect.TypeFor[mapConfig](),
		},
		hooks:     map[string]RuleExtractor{"discovery/testkit": testRuleHook},
		providers: map[string]reflect.Type{"reporting": reflect.TypeFor[providerConfig]()},
	}
}

func (s *testSchema) ConfigType(kind, typ string) (reflect.Type, bool) {
	t, ok := s.configs[ID(Kind(kind), typ)]
	return t, ok
}

func (s *testSchema) Types(kind string) []string {
	var out []string
	for id := range s.configs {
		k, typ, _ := ParseID(id)
		if string(k) == kind {
			out = append(out, typ)
		}
	}
	slices.Sort(out)
	return out
}

func (s *testSchema) CheckShape(kind, typ string, raw jsontext.Value, path string) error {
	t, ok := s.ConfigType(kind, typ)
	if !ok {
		return fmt.Errorf("%s: unknown %s type %q", path, kind, typ)
	}
	return ShapeErrors(t, raw, path)
}

func (s *testSchema) RuleSources(kind, typ string, raw jsontext.Value) ([]RuleSource, error) {
	hook, ok := s.hooks[ID(Kind(kind), typ)]
	if !ok {
		return nil, nil
	}
	return hook(raw)
}

func (s *testSchema) ProviderConfigType(id string) (reflect.Type, bool) {
	t, ok := s.providers[id]
	return t, ok
}

// testBlocks is what the test's own plugins are classified by.
var testBlocks = map[string]string{
	"discovery/testkit": `rules[]: {min_matches: 0}`,
	"source/testkit":    `timeout: 30s`,
	"processor/testkit": `concurrency: 1`,
	"storage/testkit":   `page_size: 1000`,
	"retention/testkit": `floor: 1`,
}

// defaultsDoc builds one document from the module's own, with the plugins
// config replaced: the module's document carries the configuration sections,
// the test carries the plugins — and the check has to accept the result, so a
// test that breaks the document breaks here.
func defaultsDoc(t *testing.T, blocks map[string]string) jsontext.Value {
	t.Helper()
	var doc map[string]any
	if err := yaml.Unmarshal(confdefaults.Defaults, &doc); err != nil {
		t.Fatal(err)
	}
	plugins, _ := doc["plugins"].(map[string]any)
	if plugins == nil {
		plugins = map[string]any{}
	}
	config := map[string]any{}
	for id, block := range blocks {
		var b map[string]any
		if err := yaml.Unmarshal([]byte(block), &b); err != nil {
			t.Fatal(err)
		}
		config[id] = b
	}
	plugins["config"] = config
	doc["plugins"] = plugins
	raw, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// loadDefaults builds one document from the module's own, with the plugin
// blocks replaced: the module's document carries the configuration sections,
// the test carries the plugins — and the check has to accept the result, so a
// test that breaks the document breaks here.
func loadDefaults(t *testing.T, schema Schema, blocks map[string]string) *Defaults {
	t.Helper()
	defs, err := LoadDefaults(schema, []jsontext.Value{defaultsDoc(t, blocks)})
	if err != nil {
		t.Fatalf("the test document must be one the check accepts: %v", err)
	}
	return defs
}

// testConfig is one configuration declaring: a source instance of the test
// source, a processor and its pipeline, and a discovery whose one rule nests a
// source config — everything the merge has to reach.
func testConfig(t *testing.T, sourceJSON string) jsontext.Value {
	t.Helper()
	raw := fmt.Sprintf(`{
	  "run": {"spool_dir": "/spool", "spool_reserve_bytes": 0, "max_artifact_bytes": 1024, "deadline": "1m", "lease_hard_bound": "8400s"},
	  "sources": {"app": {"type": "testkit", "artifact": {"group": "g", "name": "n", "sensitivity": "plain"}, "pipeline": ["pack"], "config": %s}},
	  "processors": {"pack": {"type": "testkit", "config": {"level": "best"}}},
	  "discoveries": {"d": {"type": "testkit", "config": {"rules": [{"name": "r", "source": {"type": "testkit", "config": {"address": "rule:5432"}}, "pipeline": ["pack"]}]}}}
	}`, sourceJSON)
	return jsontext.Value(raw)
}

func merge(t *testing.T, defs *Defaults, user jsontext.Value) (jsontext.Value, []string) {
	t.Helper()
	merged, pointers, problems := defs.Merge(testKit(), user)
	if len(problems) != 0 {
		t.Fatalf("Merge: %v", problems)
	}
	return merged, pointers
}

// at returns the value at one JSON pointer of a merged document.
func at(t *testing.T, doc jsontext.Value, pointer string) jsontext.Value {
	t.Helper()
	cur := doc
	for _, seg := range strings.Split(strings.TrimPrefix(pointer, "/"), "/") {
		if seg == "" {
			continue
		}
		obj, ok := ObjectOf(cur)
		if ok {
			next, present := obj[seg]
			if !present {
				t.Fatalf("%s: no member %q in %s", pointer, seg, cur)
			}
			cur = next
			continue
		}
		items, ok := arrayOf(cur)
		if !ok {
			t.Fatalf("%s: %s is neither an object nor an array", pointer, cur)
		}
		var idx int
		if _, err := fmt.Sscanf(seg, "%d", &idx); err != nil || idx < 0 || idx >= len(items) {
			t.Fatalf("%s: %q is not an index into %s", pointer, seg, cur)
		}
		cur = items[idx]
	}
	return cur
}

// TestMergeFillsWhatTheFileOmits is the merge at work: an instance config, an
// envelope field, a configuration section and a rule's nested source config,
// each carrying the value of its defaults block and each named by a pointer.
func TestMergeFillsWhatTheFileOmits(t *testing.T) {
	defs := loadDefaults(t, testKit(), testBlocks)
	// The source's own config states neither the defaulted timeout nor the
	// artifact's importance; the configuration states no finish_allowance.
	merged, pointers := merge(t, defs, testConfig(t, `{"address": "db:5432"}`))

	var cfg sourceConfig
	if err := Decode(at(t, merged, "/sources/app/config"), &cfg); err != nil {
		t.Fatalf("the merged source config does not decode: %v", err)
	}
	if cfg.Timeout.Std().Seconds() != 30 {
		t.Errorf("sources/app/config/timeout = %v, want the default 30s", cfg.Timeout)
	}
	if got := string(at(t, merged, "/sources/app/artifact/importance")); got != `"required"` {
		t.Errorf("sources/app/artifact/importance = %s, want the envelope's default", got)
	}
	if got := string(at(t, merged, "/run/finish_allowance")); got != `"2m"` {
		t.Errorf("run/finish_allowance = %s, want the document's 2m", got)
	}
	// The rule's nested source config is reached through the discovery plugin's
	// own pure hook, which names where its rules live.
	var ruleCfg sourceConfig
	if err := Decode(at(t, merged, "/discoveries/d/config/rules/0/source/config"), &ruleCfg); err != nil {
		t.Fatalf("the merged rule source config does not decode: %v", err)
	}
	if ruleCfg.Timeout.Std().Seconds() != 30 {
		t.Errorf("the rule's source timeout = %v, want the default 30s", ruleCfg.Timeout)
	}
	for _, want := range []string{
		"/sources/app/config/timeout",
		"/sources/app/artifact/importance",
		"/discoveries/d/config/rules/0/source/config/timeout",
	} {
		if !slices.Contains(pointers, want) {
			t.Errorf("the pointers do not name %s: %v", want, pointers)
		}
	}
}

// TestMergeCreatesNothingTheFileDoesNotDeclare: an optional section, an
// instance and a list element are never created — the document only fills what
// the file declares, and a nested object of a present parent it does create.
func TestMergeCreatesNothingTheFileDoesNotDeclare(t *testing.T) {
	blocks := map[string]string{
		"discovery/testkit": `rules[]: {min_matches: 0}`,
		"source/testkit":    `timeout: 30s`,
		"processor/testkit": `concurrency: 1`,
		"storage/testkit":   `page_size: 1000`,
	}
	defs := loadDefaults(t, testKit(), blocks)

	// A file that declares no stage, no storage, no notifier and no catalog:
	// nothing may appear for them, though the document's configuration sections
	// have values for stage.
	merged, _ := merge(t, defs, testConfig(t, `{"address": "db:5432"}`))
	obj, _ := ObjectOf(merged)
	for _, absent := range []string{"stage", "storages", "notifiers", "catalog"} {
		if _, present := obj[absent]; present {
			t.Errorf("the merge created /%s, which the file does not declare: %s", absent, merged)
		}
	}

	// A list the file declares gains no element beyond the ones it has, even
	// when the document has a `rules[]` block.
	listDefs := loadDefaults(t, testKit(), map[string]string{
		"discovery/testkit": "rules[]: {min_matches: 7}",
		"source/testkit":    `timeout: 30s`,
		"processor/testkit": `concurrency: 1`,
		"storage/testkit":   `page_size: 1000`,
	})
	merged, _ = merge(t, listDefs, testConfig(t, `{"address": "db:5432"}`))
	rules, ok := arrayOf(at(t, merged, "/discoveries/d/config/rules"))
	if !ok || len(rules) != 1 {
		t.Fatalf("the rule list is %s, want the file's one element and no other", at(t, merged, "/discoveries/d/config/rules"))
	}
	if got := string(at(t, merged, "/discoveries/d/config/rules/0/name")); got != `"r"` {
		t.Errorf("rules/0/name = %s, want the file's own value", got)
	}

	// A file with no list at all does not get one.
	noList := jsontext.Value(`{"discoveries": {"d": {"type": "testkit", "config": {}}}}`)
	merged, _ = merge(t, listDefs, noList)
	if got := string(at(t, merged, "/discoveries/d/config")); got != `{}` {
		t.Errorf("discoveries/d/config = %s, want the file's empty object without a rules list", got)
	}
}

// TestMergeReplacesAWholeListAndANestedObject: a list the file carries is the
// file's, and an object a parent holds merges key by key.
func TestMergeReplacesAWholeListAndANestedObject(t *testing.T) {
	defs := loadDefaults(t, testKit(), map[string]string{
		"discovery/testkit": `rules[]: {min_matches: 0}`,
		"source/testkit":    `timeout: 30s`,
		"processor/testkit": `concurrency: 1`,
		"storage/testkit":   `page_size: 1000`,
	})
	// The pipeline is the file's: the document states none, and a list never
	// merges element by element from a plain default.
	merged, _ := merge(t, defs, testConfig(t, `{"address": "db:5432"}`))
	if got := string(at(t, merged, "/sources/app/pipeline")); got != `["pack"]` {
		t.Errorf("sources/app/pipeline = %s, want the file's own list", got)
	}
	// The storage's envelope fills a nested object the file's entry holds.
	withStorage := jsontext.Value(`{"storages": {"t": {"type": "testkit", "time_budget": "1m", "config": {"root": "/r"}}}}`)
	merged, pointers := merge(t, defs, withStorage)
	if got := string(at(t, merged, "/storages/t/config/page_size")); got != `1000` {
		t.Errorf("storages/t/config/page_size = %s, want the block's 1000", got)
	}
	for _, want := range []string{"/storages/t/config/page_size"} {
		if !slices.Contains(pointers, want) {
			t.Errorf("the pointers do not name %s: %v", want, pointers)
		}
	}
}

// TestMergedBytesStillDemandEveryFieldFromAPlugin: Decode is untouched. A
// plugin receives complete bytes for what the document fills, and a field
// neither the file nor the document carries is still missing, with its pointer
// — the merge never weakens the decoder.
func TestMergedBytesStillDemandEveryFieldFromAPlugin(t *testing.T) {
	defs := loadDefaults(t, testKit(), testBlocks)

	merged, _ := merge(t, defs, testConfig(t, `{"address": "db:5432"}`))
	var cfg sourceConfig
	if err := Decode(at(t, merged, "/sources/app/config"), &cfg); err != nil {
		t.Fatalf("a complete merged config must decode: %v", err)
	}

	// The file states no address, and no document may: the decoder names it.
	merged, _ = merge(t, defs, testConfig(t, `{"timeout": "30s"}`))
	err := Decode(at(t, merged, "/sources/app/config"), &cfg)
	if err == nil || !strings.Contains(err.Error(), "missing field /address") {
		t.Fatalf("decoding a config without the required address: %v, want a missing-field error", err)
	}

	// An unknown key the file carries survives the merge and is still refused
	// by the strict decoder, with its pointer.
	merged, _ = merge(t, defs, testConfig(t, `{"address": "db:5432", "adress": "typo"}`))
	err = Decode(at(t, merged, "/sources/app/config"), &cfg)
	if err == nil || !strings.Contains(err.Error(), "adress") {
		t.Fatalf("decoding a config with an unknown key: %v, want the key named", err)
	}
}

// TestMergeLeavesOpaqueBytesAlone: the configuration layer mirrors no plugin
// field. A type the schema does not know — an unregistered source inside a rule
// — keeps its bytes exactly as the file wrote them.
func TestMergeLeavesOpaqueBytesAlone(t *testing.T) {
	defs := loadDefaults(t, testKit(), testBlocks)
	user := jsontext.Value(`{"discoveries": {"d": {"type": "testkit", "config": {"rules": [{"name": "r", "source": {"type": "unknown", "config": {"keep": ["this"], "and": {"this": true}}}, "pipeline": ["pack"]}]}}}}`)
	merged, _ := merge(t, defs, user)
	if got := string(at(t, merged, "/discoveries/d/config/rules/0/source/config")); got != `{"keep":["this"],"and":{"this":true}}` {
		t.Errorf("an unregistered rule source's config changed: %s", got)
	}
}

// TestLoadDefaultsRefuses is the check's rulebook, one rule per row.
func TestLoadDefaultsRefuses(t *testing.T) {
	cases := []struct {
		name   string
		blocks map[string]string
		want   string
	}{
		{
			name:   "a block naming an unregistered type",
			blocks: map[string]string{"source/nope": `{}`},
			want:   "/plugins/config/source~1nope: unknown plugin",
		},
		{
			name:   "a block named with no type",
			blocks: map[string]string{"source/": `{}`},
			want:   "not a plugin ID",
		},
		{
			name:   "a key no field has",
			blocks: map[string]string{"source/testkit": `timeout: 30s` + "\n" + `adress: db:5432`},
			want:   "/plugins/config/source~1testkit/adress: unknown key",
		},
		{
			name:   "a value that does not decode into its field",
			blocks: map[string]string{"source/testkit": `timeout: soon`},
			want:   "/plugins/config/source~1testkit/timeout",
		},
		{
			name:   "a default for a field the deployment states",
			blocks: map[string]string{"source/testkit": `address: db:5432` + "\n" + `timeout: 30s`},
			want:   "/plugins/config/source~1testkit/address: a default for a field the deployment states",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			blocks := map[string]string{}
			for k, v := range testBlocks {
				blocks[k] = v
			}
			for k, v := range c.blocks {
				blocks[k] = v
			}
			_, err := LoadDefaults(testKit(), []jsontext.Value{defaultsDoc(t, blocks)})
			if err == nil {
				t.Fatalf("the check accepted the document")
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Fatalf("the check reported:\n%v\nwant it to name %q", err, c.want)
			}
		})
	}
}

// TestLoadDefaultsRefusesNullInADocument.
func TestLoadDefaultsRefusesNullInADocument(t *testing.T) {
	blocks := map[string]string{
		"source/testkit": `timeout: null`,
	}
	_, err := LoadDefaults(testKit(), []jsontext.Value{defaultsDoc(t, blocks)})
	if err == nil || !strings.Contains(err.Error(), "is null") {
		t.Fatalf("the check reported %v, want a null refused with its pointer", err)
	}
}

// TestLoadDefaultsRefusesTwoDocumentsSettingOneKey: a distribution's document
// may add blocks of its own plugins, never a second value for a key another
// document already sets.
func TestLoadDefaultsRefusesTwoDocumentsSettingOneKey(t *testing.T) {
	first := defaultsDoc(t, testBlocks)
	second := jsontext.Value(`{"plugins": {"config": {"source/testkit": {"timeout": "1m"}}}}`)
	_, err := LoadDefaults(testKit(), []jsontext.Value{first, second})
	if err == nil || !strings.Contains(err.Error(), "/plugins/config/source~1testkit/timeout") {
		t.Fatalf("the check reported %v, want the doubly set key named", err)
	}
}

// TestLoadDefaultsRefusesADistributionDocumentBeyondPlugins.
func TestLoadDefaultsRefusesADistributionDocumentBeyondPlugins(t *testing.T) {
	first := defaultsDoc(t, testBlocks)
	second := jsontext.Value(`{"run": {"finish_allowance": "1m"}}`)
	_, err := LoadDefaults(testKit(), []jsontext.Value{first, second})
	if err == nil || !strings.Contains(err.Error(), "/run") {
		t.Fatalf("the check reported %v, want the configuration section refused in a distribution document", err)
	}
}

// TestMergeReachesTheRetentionInsideAStorage: a retention policy is an instance
// inside its storage, one per run kind — it has no name of its own — so the merge
// grows the entry and its block is applied where it sits.
func TestMergeReachesTheRetentionInsideAStorage(t *testing.T) {
	defs := loadDefaults(t, testKit(), testBlocks)
	user := jsontext.Value(`{"storages": {"t": {"type": "testkit", "time_budget": "1m", "config": {"root": "/r"}, "retention": {"scheduled": {"type": "testkit", "config": {"count": 3}}}}}}`)
	merged, pointers := merge(t, defs, user)
	if got := string(at(t, merged, "/storages/t/retention/scheduled/config/floor")); got != `1` {
		t.Errorf("storages/t/retention/scheduled/config/floor = %s, want the block's 1", got)
	}
	if !slices.Contains(pointers, "/storages/t/retention/scheduled/config/floor") {
		t.Errorf("the pointers do not name the retention's own field: %v", pointers)
	}
	// A storage without a retention gains none: the section is the deployment's.
	bare := jsontext.Value(`{"storages": {"t": {"type": "testkit", "time_budget": "1m", "config": {"root": "/r"}}}}`)
	merged, _ = merge(t, defs, bare)
	obj, _ := ObjectOf(at(t, merged, "/storages/t"))
	if _, present := obj["retention"]; present {
		t.Errorf("the merge created a retention the file does not declare: %s", merged)
	}
}

// TestMergeReachesTheStageSources: the stage's own sources are instances of the
// same kind as the run's — the same block and the same envelope apply — and they
// live inside the stage section.
func TestMergeReachesTheStageSources(t *testing.T) {
	defs := loadDefaults(t, testKit(), testBlocks)
	user := jsontext.Value(`{"stage": {"dir": "/staged", "sources": {"control": {"type": "testkit", "artifact": {"group": "g", "name": "n", "sensitivity": "plain"}, "pipeline": [], "config": {"address": "db:5432"}}}}}`)
	merged, pointers := merge(t, defs, user)
	if got := string(at(t, merged, "/stage/sources/control/config/timeout")); got != `"30s"` {
		t.Errorf("stage.sources.control.config.timeout = %s, want the block's 30s", got)
	}
	if got := string(at(t, merged, "/stage/sources/control/artifact/importance")); got != `"required"` {
		t.Errorf("stage.sources.control.artifact.importance = %s, want the envelope's default", got)
	}
	if !slices.Contains(pointers, "/stage/sources/control/config/timeout") {
		t.Errorf("the pointers do not name a stage source field: %v", pointers)
	}
}

// TestMergeProviderSettings: the settings of a plugin that declares a stable ID
// rather than a factory merge through its own type, and the check judges them
// like a factory's.
func TestMergeProviderSettings(t *testing.T) {
	doc := defaultsDoc(t, testBlocks)
	obj, _ := ObjectOf(doc)
	plugins, _ := ObjectOf(obj["plugins"])
	blocks, _ := ObjectOf(plugins["config"])
	blocks["reporting"] = jsontext.Value(`{"timeout": "30s", "retries": 2}`)
	plugins["config"] = encode(blocks)
	obj["plugins"] = encode(plugins)
	defs, err := LoadDefaults(testKit(), []jsontext.Value{encode(obj)})
	if err != nil {
		t.Fatalf("a provider block the schema knows must pass the check: %v", err)
	}
	user := jsontext.Value(`{"plugins": {"config": {"reporting": {"timeout": "5s"}}}}`)
	merged, pointers := merge(t, defs, user)
	if got := string(at(t, merged, "/plugins/config/reporting/timeout")); got != `"5s"` {
		t.Errorf("the provider's own setting = %s, want the file's", got)
	}
	if got := string(at(t, merged, "/plugins/config/reporting/retries")); got != `2` {
		t.Errorf("the provider's defaulted setting = %s, want the document's 2", got)
	}
	if !slices.Contains(pointers, "/plugins/config/reporting/retries") {
		t.Errorf("the pointers do not name the defaulted provider setting: %v", pointers)
	}
}
