package assembly

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"encoding/json/jsontext"
	json "encoding/json/v2"

	"go.yaml.in/yaml/v4"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/configuration"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/configuration/defaults"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/processor"
)

func TestNewRefusesNilRegistryAndLogger(t *testing.T) {
	w := newWorld(t)
	if _, err := New(context.Background(), jsontext.Value(`{}`), nil, Deps{Loader: w.Loader, Log: testLogger()}); err == nil || !strings.Contains(err.Error(), "registry is nil") {
		t.Fatalf("nil registry: %v", err)
	}
	if _, err := New(context.Background(), jsontext.Value(`{}`), nil, Deps{Factories: w.Registry, Loader: w.Loader}); err == nil || !strings.Contains(err.Error(), "logger is nil") {
		t.Fatalf("nil logger: %v", err)
	}
}

func TestNewCollectsEveryUnknownInstanceType(t *testing.T) {
	w := newWorld(t)
	cfg := map[string]any{
		"layout":      configLayout(),
		"processors":  map[string]any{"p": instance("nosuch")},
		"discoveries": map[string]any{"d": instance("nosuch")},
		"notifiers":   map[string]any{"n": instance("nosuch")},
		"catalog":     instance("nosuch"),
	}
	raw, err := jsonMarshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	_, err = New(context.Background(), raw, nil, Deps{Factories: w.Registry, Loader: w.Loader, Log: testLogger()})
	if err == nil {
		t.Fatal("unknown instance types were accepted")
	}
	msg := err.Error()
	for _, want := range []string{
		"/processors/p: unknown processor type",
		"/discoveries/d: unknown discovery type",
		"/notifiers/n: unknown notifier type",
		"/catalogs/catalog: unknown catalog type",
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("error lacks %q:\n%s", want, msg)
		}
	}
}

func TestNewReportsADefaultsMergeFailure(t *testing.T) {
	w := newWorld(t)
	// A settings block naming a plugin the compiled schema does not carry is a
	// merge problem: New refuses with it before any construction.
	cfg := map[string]any{
		"layout": configLayout(),
		"plugins": map[string]any{
			"config": map[string]any{"storage/nope": map[string]any{"page_size": 1}},
		},
	}
	raw, err := jsonMarshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	// Merge judges plugin settings only with a defaults document present; the
	// distribution's own document is the one a real deployment carries.
	defs, err := configuration.LoadDefaults(w.Registry, []jsontext.Value{
		moduleDefaultsDoc(t, w.Registry, "storage/fake", `{"page_size": 100}`),
	})
	if err != nil {
		t.Fatalf("LoadDefaults: %v", err)
	}
	_, err = New(context.Background(), raw, defs, Deps{Factories: w.Registry, Loader: w.Loader, Log: testLogger()})
	if err == nil || !strings.Contains(err.Error(), "config:") || !strings.Contains(err.Error(), "storage~1nope") {
		t.Fatalf("a merge failure was not reported: %v", err)
	}
}

func TestNewRefusesUnknownSourceType(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["sources"] = map[string]any{"vpn": sourceInstance("nosuch", "vpn", "wireguard", []string{}, "required", "plain")}
	if _, err := w.newAssemblyErr(t, cfg); err == nil || !strings.Contains(err.Error(), "/sources/vpn") {
		t.Fatalf("unknown source type accepted: %v", err)
	}
}

// TestNewRefusesAChainSourceWithAPipeline: the staged source carries the chain
// of the process that produced its bytes, so its own pipeline must be empty.
func TestNewRefusesAChainSourceWithAPipeline(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	w.ExtraSources["vpn"] = &chainSource{Data: []byte("staged"), Ext: ".bin"}

	_, err := w.newAssemblyErr(t, cfg)
	if err == nil || !strings.Contains(err.Error(), "pipeline must be empty") {
		t.Fatalf("a chain source with a pipeline was accepted: %v", err)
	}
}

// TestNewRefusesASecretPipelineThatCannotSeal: the assembly asks every processor
// of a secret pipeline whether it seals while the configuration is read, and a
// processor that cannot answer fails the construction.
func TestNewRefusesASecretPipelineThatCannotSeal(t *testing.T) {
	w := newWorld(t)
	registerProcessor(t, w, "zcdesc", describeErrProcessor{})
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["processors"] = map[string]any{"df": instance("zcdesc")}
	cfg["sources"] = map[string]any{"vpn": sourceInstance("fake", "vpn", "wireguard", []string{"df"}, "required", "secret")}

	_, err := w.newAssemblyErr(t, cfg)
	if err == nil || !strings.Contains(err.Error(), "pipeline") {
		t.Fatalf("a secret pipeline that cannot seal was accepted: %v", err)
	}
}

// TestNewRefusesAProcessorThatCannotEncode: a processor that says it cannot
// encode fails the pipeline at construction, before anything runs.
func TestNewRefusesAProcessorThatCannotEncode(t *testing.T) {
	w := newWorld(t)
	registerProcessor(t, w, "zcecheck", encodeErrProcessor{})
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["processors"] = map[string]any{"ce": instance("zcecheck")}
	cfg["sources"] = map[string]any{"vpn": sourceInstance("fake", "vpn", "wireguard", []string{"ce"}, "required", "plain")}

	_, err := w.newAssemblyErr(t, cfg)
	if err == nil || !strings.Contains(err.Error(), "no recipient") {
		t.Fatalf("a processor that cannot encode was accepted: %v", err)
	}
}

// TestNewTracksAndClosesClosableInstances: a plugin instance that is an
// io.Closer joins the assembly's lifetime list and is closed with it.
func TestNewTracksAndClosesClosableInstances(t *testing.T) {
	w := newWorld(t)
	cs := &closerSource{}
	w.ExtraSources["vpn"] = cs

	e := w.newAssembly(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}))
	if cs.closed {
		t.Fatal("the invocation closed its source before Close")
	}
	if err := e.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !cs.closed {
		t.Fatal("Close did not close the owned source")
	}
	if err := e.Close(context.Background()); err != nil {
		t.Fatalf("a second Close: %v", err)
	}
}

func TestEffectiveConfigReturnsTheMergedDocument(t *testing.T) {
	w := newWorld(t)
	e := w.newAssembly(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}))
	merged, pointers := e.EffectiveConfig()
	if len(merged) == 0 {
		t.Fatal("EffectiveConfig returned an empty document")
	}
	if pointers != nil {
		t.Fatalf("pointers %v, want nil for a config merged with no defaults", pointers)
	}
}

// --- rules.go ---------------------------------------------------------------

// closerSource is a source that is also an io.Closer, the shape the assembly's
// lifetime list holds.
type closerSource struct {
	fakeSource
	closed bool
}

func (s *closerSource) Close() error {
	s.closed = true
	return nil
}

// describeErrProcessor cannot describe itself, so a pipeline holding it cannot
// answer whether it seals.
type describeErrProcessor struct{}

func (describeErrProcessor) Describe(context.Context) (processor.ProcessorTraits, error) {
	return processor.ProcessorTraits{}, errors.New("describe failed")
}

func (describeErrProcessor) Encode(context.Context, io.Writer, io.Reader) error { return nil }

func (describeErrProcessor) Decode(context.Context, io.Writer, io.Reader) error { return nil }

// encodeErrProcessor says it cannot encode: an EncodeChecker that refuses.
type encodeErrProcessor struct{}

func (encodeErrProcessor) Describe(context.Context) (processor.ProcessorTraits, error) {
	return processor.ProcessorTraits{Ext: ".x"}, nil
}

func (encodeErrProcessor) Encode(context.Context, io.Writer, io.Reader) error { return nil }

func (encodeErrProcessor) Decode(context.Context, io.Writer, io.Reader) error { return nil }

func (encodeErrProcessor) CheckEncode(context.Context) error {
	return errors.New("no recipient is configured")
}

func registerProcessor(t *testing.T, w *world, typ string, p processor.Processor) {
	t.Helper()
	err := instances.Register(w.Registry, processor.Kind, instances.FactorySpec[map[string]jsontext.Value, processor.Processor]{
		Type: typ, Example: []byte(`{}`),
		New: func(context.Context, map[string]jsontext.Value, instances.Deps) (processor.Processor, error) {
			return p, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
}

// moduleDefaultsDoc is the distribution's defaults document as one test needs
// it: the module's own document, with the given block added under
// plugins.config for source/fake.
func moduleDefaultsDoc(t *testing.T, reg *instances.Registry, id, block string) jsontext.Value {
	t.Helper()
	var doc map[string]any
	if err := yaml.Unmarshal(defaults.Defaults, &doc); err != nil {
		t.Fatal(err)
	}
	plugins, _ := doc["plugins"].(map[string]any)
	if plugins == nil {
		plugins = map[string]any{}
		doc["plugins"] = plugins
	}
	if _, ok := plugins["bundles"]; !ok {
		plugins["bundles"] = []any{}
	}
	if _, ok := plugins["disabled"]; !ok {
		plugins["disabled"] = []any{}
	}
	config, _ := plugins["config"].(map[string]any)
	if config == nil {
		config = map[string]any{}
	}
	plugins["config"] = config
	// The module's document names every first-party plugin; this test's registry
	// compiles only the fakes, and the check refuses a block for a plugin no
	// registry can answer for.
	for id := range config {
		kind, typ, ok := configuration.ParseID(id)
		if !ok {
			delete(config, id)
			continue
		}
		if _, ok := reg.ConfigType(string(kind), typ); !ok {
			delete(config, id)
		}
	}
	var b map[string]any
	if err := yaml.Unmarshal([]byte(block), &b); err != nil {
		t.Fatal(err)
	}
	config[id] = b
	raw, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// TestNoReportingBlockLeavesTheRecordUnavailable: a config whose plugins root
// carries no reporting settings builds no reporter, and the run reports no
// previous-run data rather than guessing one.
func TestNoReportingBlockLeavesTheRecordUnavailable(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["plugins"] = map[string]any{"bundles": []any{}, "disabled": []any{}, "config": map[string]any{}}
	if e := w.newAssembly(t, cfg); e.reporter != nil {
		t.Fatal("a config without reporting settings built a reporter")
	}
}

// TestNewRefusesAMalformedReportingBlock: the reporting settings decode
// strictly like every other plugin block.
func TestNewRefusesAMalformedReportingBlock(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["plugins"] = map[string]any{"bundles": []any{}, "disabled": []any{}, "config": map[string]any{
		"reporting": map[string]any{
			"api_host_env": 5, "api_port_env": "PORT", "token_file": "/token", "ca_file": "/ca",
			"request_timeout": "30s", "walk_timeout": "30s", "namespace_env": "NAMESPACE", "pod_env": "POD",
		},
	}}
	_, err := w.newAssemblyErr(t, cfg)
	if err == nil || !strings.Contains(err.Error(), "/plugins/config/reporting") {
		t.Fatalf("a malformed reporting block = %v, want it refused", err)
	}
}

// TestNewRefusesInvalidReportingSettings: a block that decodes but cannot work
// fails the configuration read before any plugin is built.
func TestNewRefusesInvalidReportingSettings(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["plugins"] = map[string]any{"bundles": []any{}, "disabled": []any{}, "config": map[string]any{
		"reporting": map[string]any{
			"api_host_env": "", "api_port_env": "PORT", "token_file": "/token", "ca_file": "/ca",
			"request_timeout": "30s", "walk_timeout": "30s", "namespace_env": "NAMESPACE", "pod_env": "POD",
		},
	}}
	_, err := w.newAssemblyErr(t, cfg)
	if err == nil || !strings.Contains(err.Error(), "api_host_env is empty") {
		t.Fatalf("an unusable reporting block = %v, want it refused", err)
	}
}

// TestBuildReporterRefusesACancelledStartup: the reporter is built under the
// caller's context, and a startup already cancelled never installs one.
func TestBuildReporterRefusesACancelledStartup(t *testing.T) {
	e := &Assembly{merged: jsontext.Value(`{"plugins":{"bundles":[],"disabled":[],"config":{"reporting":{` +
		`"api_host_env":"HOST","api_port_env":"PORT","token_file":"/token","ca_file":"/ca",` +
		`"request_timeout":"30s","walk_timeout":"30s","namespace_env":"NAMESPACE","pod_env":"POD"}}}}`)}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := e.buildReporter(ctx); err == nil || !strings.Contains(err.Error(), "context canceled") {
		t.Fatalf("a cancelled startup = %v, want the reporter refused", err)
	}
	if e.reporter != nil {
		t.Fatal("a cancelled startup installed a reporter")
	}
}
