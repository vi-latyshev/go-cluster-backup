package engine

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strings"
	"testing"

	"encoding/json/jsontext"
	json "encoding/json/v2"

	"go.yaml.in/yaml/v3"

	"github.com/vi-latyshev/go-cluster-backup/internal/defaults"
	"github.com/vi-latyshev/go-cluster-backup/internal/discovery"
	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/discovery/kuberneteslabels"
	"github.com/vi-latyshev/go-cluster-backup/internal/processor"
	"github.com/vi-latyshev/go-cluster-backup/internal/retention"
	"github.com/vi-latyshev/go-cluster-backup/internal/source"
	"github.com/vi-latyshev/go-cluster-backup/internal/storage"
	"github.com/vi-latyshev/go-cluster-backup/internal/storage/storagetest"
)

// The defaults tests: one tiny distribution whose plugin configs are typed, so
// the check and the merge have real fields to classify, and the module's own
// document as the base — the engine sections have to be complete (every field
// required or defaulted) and the module's document is the one place they are.

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

// ruleDiscovery reports its rules' sources, the way a real discovery declares
// where the config the strict decoder cannot see is nested.
type ruleDiscovery struct{ rules []rule }

func (d *ruleDiscovery) Discover(ctx context.Context) ([]discovery.Found, error) {
	return nil, ctx.Err()
}

func (d *ruleDiscovery) RuleSources(context.Context) ([]discovery.RuleSource, error) {
	out := make([]discovery.RuleSource, 0, len(d.rules))
	for i, r := range d.rules {
		out = append(out, discovery.RuleSource{
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

// quietSource and quietProcessor are the plugins these tests build: the engine
// is never asked to run one, only to construct it, so every method is a stub.
type quietSource struct{}

func (*quietSource) Describe(context.Context) (source.SourceTraits, error) {
	return source.SourceTraits{Ext: ".dat"}, nil
}

func (*quietSource) Dump(context.Context, io.Writer) error { return nil }

type quietRetention struct{}

func (quietRetention) Decide(context.Context, retention.DecideRequest) (retention.Decision, error) {
	return retention.Decision{}, nil
}

type quietProcessor struct{}

func (quietProcessor) Describe(context.Context) (processor.ProcessorTraits, error) {
	return processor.ProcessorTraits{}, nil
}

func (quietProcessor) Encode(context.Context, io.Writer, io.Reader) error { return nil }
func (quietProcessor) Decode(context.Context, io.Writer, io.Reader) error { return nil }

func defaultsRegistry(t *testing.T) *plugin.Registry { return registryRecording(t, nil) }

func registryRecording(t *testing.T, record func(sourceConfig)) *plugin.Registry {
	t.Helper()
	reg := plugin.NewRegistry()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(plugin.Register(reg, source.Kind, plugin.Spec[sourceConfig, source.Source]{
		Type: "testkit", Example: []byte(`{"address": "db:5432", "timeout": "30s"}`),
		New: func(_ context.Context, cfg sourceConfig, _ plugin.Deps) (source.Source, error) {
			if record != nil {
				record(cfg)
			}
			return &quietSource{}, nil
		},
	}))
	must(plugin.Register(reg, processor.Kind, plugin.Spec[procConfig, processor.Processor]{
		Type: "testkit", Example: []byte(`{"level": "best", "concurrency": 1}`),
		New: func(context.Context, procConfig, plugin.Deps) (processor.Processor, error) {
			return quietProcessor{}, nil
		},
	}))
	must(plugin.Register(reg, storage.Kind, plugin.Spec[storageConfig, storage.Storage]{
		Type: "testkit", Example: []byte(`{"root": "/backup", "page_size": 1000}`),
		New: func(context.Context, storageConfig, plugin.Deps) (storage.Storage, error) {
			return storagetest.NewFake(), nil
		},
	}))
	must(plugin.Register(reg, retention.Kind, plugin.Spec[retentionConfig, retention.Retention]{
		Type: "testkit", Example: []byte(`{"count": 3, "floor": 1}`),
		New: func(context.Context, retentionConfig, plugin.Deps) (retention.Retention, error) {
			return quietRetention{}, nil
		},
	}))
	must(plugin.Register(reg, discovery.Kind, plugin.Spec[ruleConfig, discovery.Discoverer]{
		Type: "testkit", Example: []byte(`{"rules": [{"name": "r", "source": {"type": "testkit", "config": {}}, "pipeline": ["pack"], "min_matches": 0}]}`),
		New: func(_ context.Context, cfg ruleConfig, _ plugin.Deps) (discovery.Discoverer, error) {
			return &ruleDiscovery{rules: cfg.Rules}, nil
		},
	}))
	return reg
}

// testBlocks is what the test's own plugins are classified by.
var testBlocks = map[string]string{
	"discovery/testkit": `rules[]: {min_matches: 0}`,
	"source/testkit":    `timeout: 30s`,
	"processor/testkit": `concurrency: 1`,
	"storage/testkit":   `page_size: 1000`,
	"retention/testkit": `floor: 1`,
}

// loadDefaults builds one document from the module's own, with the plugin
// blocks replaced: the module's document carries the engine sections, the test
// carries the plugins — and the check has to accept the result, so a test that
// breaks the document breaks here.
func loadDefaults(t *testing.T, reg *plugin.Registry, blocks map[string]string) *Defaults {
	t.Helper()
	defs, err := LoadDefaults(reg, []jsontext.Value{defaultsDoc(t, blocks)})
	if err != nil {
		t.Fatalf("the test document must be one the check accepts: %v", err)
	}
	return defs
}

func defaultsDoc(t *testing.T, blocks map[string]string) jsontext.Value {
	t.Helper()
	var doc map[string]any
	if err := yaml.Unmarshal(defaults.Defaults, &doc); err != nil {
		t.Fatal(err)
	}
	plugins := map[string]any{}
	for name, block := range blocks {
		var b map[string]any
		if err := yaml.Unmarshal([]byte(block), &b); err != nil {
			t.Fatal(err)
		}
		plugins[name] = b
	}
	doc["plugins"] = plugins
	raw, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// testConfig is one configuration declaring: a source instance of the test
// source, a processor and its pipeline, and a discovery whose one rule nests a
// source config — everything the merge has to reach.
func testConfig(t *testing.T, sourceJSON string) jsontext.Value {
	t.Helper()
	raw := fmt.Sprintf(`{
	  "version": 1,
	  "run": {"spool_dir": "/spool", "spool_reserve_bytes": 0, "max_artifact_bytes": 1024, "deadline": "1m", "lease_hard_bound": "8400s"},
	  "sources": {"app": {"type": "testkit", "artifact": {"group": "g", "name": "n", "sensitivity": "plain"}, "pipeline": ["pack"], "config": %s}},
	  "processors": {"pack": {"type": "testkit", "config": {"level": "best"}}},
	  "discoveries": {"d": {"type": "testkit", "config": {"rules": [{"name": "r", "source": {"type": "testkit", "config": {"address": "rule:5432"}}, "pipeline": ["pack"]}]}}}
	}`, sourceJSON)
	return jsontext.Value(raw)
}

func merge(t *testing.T, defs *Defaults, user jsontext.Value) (jsontext.Value, []string) {
	t.Helper()
	merged, pointers, err := defs.Merge(context.Background(), defaultsRegistry(t), slog.New(slog.DiscardHandler), user)
	if err != nil {
		t.Fatalf("Merge: %v", err)
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
		obj, ok := plugin.ObjectOf(cur)
		if ok {
			next, present := obj[seg]
			if !present {
				t.Fatalf("%s: no member %q in %s", pointer, seg, cur)
			}
			cur = next
			continue
		}
		items, ok := jsonArray(cur)
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
// envelope field, an engine section and a rule's nested source config, each
// carrying the value of its defaults block and each named by a pointer.
func TestMergeFillsWhatTheFileOmits(t *testing.T) {
	defs := loadDefaults(t, defaultsRegistry(t), testBlocks)
	// The source's own config states neither the defaulted timeout nor the
	// artifact's importance; the engine config states no finish_allowance.
	merged, pointers := merge(t, defs, testConfig(t, `{"address": "db:5432"}`))

	var cfg sourceConfig
	if err := plugin.Decode(at(t, merged, "/sources/app/config"), &cfg); err != nil {
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
	// own declaration of where it lives.
	var ruleCfg sourceConfig
	if err := plugin.Decode(at(t, merged, "/discoveries/d/config/rules/0/source/config"), &ruleCfg); err != nil {
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
	defs := loadDefaults(t, defaultsRegistry(t), blocks)

	// A file that declares no stage, no storage, no notifier and no catalog:
	// nothing may appear for them, though the document's engine sections have
	// values for stage.
	merged, _ := merge(t, defs, testConfig(t, `{"address": "db:5432"}`))
	obj, _ := plugin.ObjectOf(merged)
	for _, absent := range []string{"stage", "storages", "notifiers", "catalog"} {
		if _, present := obj[absent]; present {
			t.Errorf("the merge created /%s, which the file does not declare: %s", absent, merged)
		}
	}

	// A list the file declares gains no element beyond the ones it has, even
	// when the document has a `rules[]` block.
	listDefs := loadDefaults(t, defaultsRegistry(t), map[string]string{
		"discovery/testkit": "rules[]: {min_matches: 7}",
		"source/testkit":    `timeout: 30s`,
		"processor/testkit": `concurrency: 1`,
		"storage/testkit":   `page_size: 1000`,
	})
	merged, _ = merge(t, listDefs, testConfig(t, `{"address": "db:5432"}`))
	rules, ok := jsonArray(at(t, merged, "/discoveries/d/config/rules"))
	if !ok || len(rules) != 1 {
		t.Fatalf("the rule list is %s, want the file's one element and no other", at(t, merged, "/discoveries/d/config/rules"))
	}
	if got := string(at(t, merged, "/discoveries/d/config/rules/0/name")); got != `"r"` {
		t.Errorf("rules/0/name = %s, want the file's own value", got)
	}

	// A file with no list at all does not get one.
	noList := jsontext.Value(`{"version": 1, "discoveries": {"d": {"type": "testkit", "config": {}}}}`)
	merged, _ = merge(t, listDefs, noList)
	if got := string(at(t, merged, "/discoveries/d/config")); got != `{}` {
		t.Errorf("discoveries/d/config = %s, want the file's empty object without a rules list", got)
	}
}

// TestMergeReplacesAWholeListAndANestedObject: a list the file carries is the
// file's, and an object a parent holds merges key by key.
func TestMergeReplacesAWholeListAndANestedObject(t *testing.T) {
	defs := loadDefaults(t, defaultsRegistry(t), map[string]string{
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
	withStorage := jsontext.Value(`{"version": 1, "storages": {"t": {"type": "testkit", "time_budget": "1m", "config": {"root": "/r"}}}}`)
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

// TestMergedBytesStillDemandEveryFieldFromAPlugin: plugin.Decode is untouched.
// A plugin receives complete bytes for what the document fills, and a field
// neither the file nor the document carries is still missing, with its pointer
// — the merge never weakens the decoder.
func TestMergedBytesStillDemandEveryFieldFromAPlugin(t *testing.T) {
	defs := loadDefaults(t, defaultsRegistry(t), testBlocks)

	merged, _ := merge(t, defs, testConfig(t, `{"address": "db:5432"}`))
	var cfg sourceConfig
	if err := plugin.Decode(at(t, merged, "/sources/app/config"), &cfg); err != nil {
		t.Fatalf("a complete merged config must decode: %v", err)
	}

	// The file states no address, and no document may: the decoder names it.
	merged, _ = merge(t, defs, testConfig(t, `{"timeout": "30s"}`))
	err := plugin.Decode(at(t, merged, "/sources/app/config"), &cfg)
	if err == nil || !strings.Contains(err.Error(), "missing field /address") {
		t.Fatalf("decoding a config without the required address: %v, want a missing-field error", err)
	}

	// An unknown key the file carries survives the merge and is still refused
	// by the strict decoder, with its pointer.
	merged, _ = merge(t, defs, testConfig(t, `{"address": "db:5432", "adress": "typo"}`))
	err = plugin.Decode(at(t, merged, "/sources/app/config"), &cfg)
	if err == nil || !strings.Contains(err.Error(), "adress") {
		t.Fatalf("decoding a config with an unknown key: %v, want the key named", err)
	}
}

// TestMergeLeavesOpaqueBytesAlone: the engine mirrors no plugin field. A type
// the registry does not know — an unregistered source inside a rule — keeps its
// bytes exactly as the file wrote them.
func TestMergeLeavesOpaqueBytesAlone(t *testing.T) {
	defs := loadDefaults(t, defaultsRegistry(t), testBlocks)
	user := jsontext.Value(`{"version": 1, "discoveries": {"d": {"type": "testkit", "config": {"rules": [{"name": "r", "source": {"type": "unknown", "config": {"keep": ["this"], "and": {"this": true}}}, "pipeline": ["pack"]}]}}}}`)
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
			want:   "/plugins/source/nope: unknown plugin",
		},
		{
			name:   "a block named without a kind",
			blocks: map[string]string{"source": `{}`},
			want:   "a block is named kind/type",
		},
		{
			name:   "a key no field has",
			blocks: map[string]string{"source/testkit": `timeout: 30s` + "\n" + `adress: db:5432`},
			want:   "/plugins/source/testkit/adress: unknown key",
		},
		{
			name:   "a value that does not decode into its field",
			blocks: map[string]string{"source/testkit": `timeout: soon`},
			want:   "/plugins/source/testkit/timeout",
		},
		{
			name:   "a default for a field the deployment states",
			blocks: map[string]string{"source/testkit": `address: db:5432` + "\n" + `timeout: 30s`},
			want:   "/plugins/source/testkit/address: a default for a field the deployment states",
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
			_, err := LoadDefaults(defaultsRegistry(t), []jsontext.Value{defaultsDoc(t, blocks)})
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
	_, err := LoadDefaults(defaultsRegistry(t), []jsontext.Value{defaultsDoc(t, blocks)})
	if err == nil || !strings.Contains(err.Error(), "is null") {
		t.Fatalf("the check reported %v, want a null refused with its pointer", err)
	}
}

// TestLoadDefaultsRefusesTwoDocumentsSettingOneKey: a distribution's document
// may add blocks of its own plugins, never a second value for a key another
// document already sets.
func TestLoadDefaultsRefusesTwoDocumentsSettingOneKey(t *testing.T) {
	reg := defaultsRegistry(t)
	first := defaultsDoc(t, testBlocks)
	second := jsontext.Value(`{"plugins": {"source/testkit": {"timeout": "1m"}}}`)
	_, err := LoadDefaults(reg, []jsontext.Value{first, second})
	if err == nil || !strings.Contains(err.Error(), "/plugins/source/testkit/timeout") {
		t.Fatalf("the check reported %v, want the doubly set key named", err)
	}
}

// TestLoadDefaultsRefusesADistributionDocumentBeyondPlugins.
func TestLoadDefaultsRefusesADistributionDocumentBeyondPlugins(t *testing.T) {
	reg := defaultsRegistry(t)
	first := defaultsDoc(t, testBlocks)
	second := jsontext.Value(`{"run": {"finish_allowance": "1m"}}`)
	_, err := LoadDefaults(reg, []jsontext.Value{first, second})
	if err == nil || !strings.Contains(err.Error(), "/run") {
		t.Fatalf("the check reported %v, want the engine section refused in a distribution document", err)
	}
}

// TestTheMergeReachesAPluginBeforeItDecodes: the engine merges the defaults
// under the file before any byte of an instance config crosses a plugin
// boundary, and what the plugin's constructor receives is the merged config —
// not the file. The constructor runs after plugin.Decode, inside the registry,
// so a value it sees can only have come from the merge.
func TestTheMergeReachesAPluginBeforeItDecodes(t *testing.T) {
	var got sourceConfig
	var built bool
	reg := registryRecording(t, func(cfg sourceConfig) { got, built = cfg, true })
	// The file states no timeout for this instance; only the document does.
	merged, _ := merge(t, loadDefaults(t, reg, testBlocks), testConfig(t, `{"address": "db:5432"}`))
	if _, err := New(context.Background(), merged, nil, reg, slog.New(slog.DiscardHandler)); err != nil {
		t.Fatalf("New: %v", err)
	}
	if !built {
		t.Fatal("the source instance was never built")
	}
	if got.Timeout.Std().Seconds() != 30 {
		t.Errorf("the plugin decoded timeout %v, want the document's 30s", got.Timeout)
	}
	if got.Address != "db:5432" {
		t.Errorf("the plugin decoded address %q, want the file's own", got.Address)
	}
}

// TestMergeReachesTheRetentionInsideAStorage: a retention policy is an instance
// inside its storage, one per run kind — it has no name of its own — so the merge
// grows the entry and its block is applied where it sits.
func TestMergeReachesTheRetentionInsideAStorage(t *testing.T) {
	defs := loadDefaults(t, defaultsRegistry(t), testBlocks)
	user := jsontext.Value(`{"version": 1, "storages": {"t": {"type": "testkit", "time_budget": "1m", "config": {"root": "/r"}, "retention": {"scheduled": {"type": "testkit", "config": {"count": 3}}}}}}`)
	merged, pointers := merge(t, defs, user)
	if got := string(at(t, merged, "/storages/t/retention/scheduled/config/floor")); got != `1` {
		t.Errorf("storages/t/retention/scheduled/config/floor = %s, want the block's 1", got)
	}
	if !slices.Contains(pointers, "/storages/t/retention/scheduled/config/floor") {
		t.Errorf("the pointers do not name the retention's own field: %v", pointers)
	}
	// A storage without a retention gains none: the section is the deployment's.
	bare := jsontext.Value(`{"version": 1, "storages": {"t": {"type": "testkit", "time_budget": "1m", "config": {"root": "/r"}}}}`)
	merged, _ = merge(t, defs, bare)
	obj, _ := plugin.ObjectOf(at(t, merged, "/storages/t"))
	if _, present := obj["retention"]; present {
		t.Errorf("the merge created a retention the file does not declare: %s", merged)
	}
}

// TestMergeReachesTheStageSources: the stage's own sources are instances of the
// same kind as the run's — the same block and the same envelope apply — and they
// live inside the stage section.
func TestMergeReachesTheStageSources(t *testing.T) {
	defs := loadDefaults(t, defaultsRegistry(t), testBlocks)
	user := jsontext.Value(`{"version": 1, "stage": {"dir": "/staged", "sources": {"control": {"type": "testkit", "artifact": {"group": "g", "name": "n", "sensitivity": "plain"}, "pipeline": [], "config": {"address": "db:5432"}}}}}`)
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

// TestTheRuleBlocksFillTheRealDiscovery: the pods and volumes blocks of the
// module's document are what fills a real kubernetes_labels rule — the phase, the
// min_matches and the artifact's importance — and what the file states is never
// touched. The plugin is the one a deployment runs, not a fixture.
func TestTheRuleBlocksFillTheRealDiscovery(t *testing.T) {
	reg := plugin.NewRegistry()
	if err := kuberneteslabels.Register(reg); err != nil {
		t.Fatal(err)
	}
	// The module's document, narrowed to the one plugin this registry holds.
	var doc map[string]any
	if err := yaml.Unmarshal(defaults.Defaults, &doc); err != nil {
		t.Fatal(err)
	}
	plugins, _ := doc["plugins"].(map[string]any)
	doc["plugins"] = map[string]any{"discovery/kubernetes_labels": plugins["discovery/kubernetes_labels"]}
	raw, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	defs, err := LoadDefaults(reg, []jsontext.Value{raw})
	if err != nil {
		t.Fatalf("the module's document must hold up: %v", err)
	}
	user := jsontext.Value(`{"version": 1, "discoveries": {"workloads": {"type": "kubernetes_labels", "config": {
	  "secrets_path": "/var/run/discovered",
	  "exclude": {"label": "backup.test/ignore", "value": "true"},
	  "undeclared_volumes": {"importance": "required"},
	  "pods": [{"name": "db", "selector": "app=db", "covers_volumes": true, "artifact": {"group": "d", "name": "{{ .Name }}", "sensitivity": "plain"}, "pipeline": [], "source": {"type": "files", "config": {"root": "/x"}}}],
	  "volumes": [{"name": "vols", "selector": "app=files", "min_matches": 7, "artifact": {"group": "v", "name": "{{ .Name }}", "sensitivity": "plain"}, "pipeline": [], "source": {"type": "files", "config": {"root": "/x"}}}]
	}}}}`)
	merged, pointers, err := defs.Merge(context.Background(), reg, slog.New(slog.DiscardHandler), user)
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}
	cfg := at(t, merged, "/discoveries/workloads/config")
	for _, c := range []struct{ path, want string }{
		// From the pods[] block: the phase, the empty min_matches and the
		// artifact's importance.
		{"/pods/0/phase", `"Running"`},
		{"/pods/0/min_matches", `0`},
		{"/pods/0/artifact/importance", `"required"`},
		// From the volumes[] block, which states Bound.
		{"/volumes/0/phase", `"Bound"`},
		{"/volumes/0/artifact/importance", `"required"`},
		// The file's own value wins over the block.
		{"/volumes/0/min_matches", `7`},
		// The setting the accounting runs on: the file states the importance, the
		// document the name.
		{"/undeclared_volumes/name", `"undeclared"`},
		{"/undeclared_volumes/importance", `"required"`},
	} {
		got := string(at(t, cfg, c.path))
		if got != c.want {
			t.Errorf("config%s = %s, want %s", c.path, got, c.want)
		}
	}
	marked := strings.Join(pointers, " ")
	for _, want := range []string{"/pods/0/phase", "/pods/0/min_matches", "/pods/0/artifact/importance", "/volumes/0/phase", "/volumes/0/artifact/importance", "/undeclared_volumes/name"} {
		if !strings.Contains(marked, want) {
			t.Errorf("the pointers do not name the block-filled %s: %v", want, pointers)
		}
	}
	// What the file states is the file's, and is not a defaulted pointer.
	for _, gone := range []string{"/volumes/0/min_matches", "/undeclared_volumes/importance"} {
		if strings.Contains(marked, gone) {
			t.Errorf("the pointers name %s, which the file states itself: %v", gone, pointers)
		}
	}
}
