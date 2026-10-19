package engine

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/discovery"
	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/processor"
)

func TestNewRefusesNilRegistryAndLogger(t *testing.T) {
	w := newWorld(t)
	if _, err := New(context.Background(), jsontext.Value(`{}`), nil, nil, testLogger()); err == nil || !strings.Contains(err.Error(), "registry is nil") {
		t.Fatalf("nil registry: %v", err)
	}
	if _, err := New(context.Background(), jsontext.Value(`{}`), nil, w.registry, nil); err == nil || !strings.Contains(err.Error(), "logger is nil") {
		t.Fatalf("nil logger: %v", err)
	}
}

func TestNewCollectsEveryUnknownInstanceType(t *testing.T) {
	w := newWorld(t)
	cfg := map[string]any{
		"version":     1,
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
	_, err = New(context.Background(), raw, nil, w.registry, testLogger())
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
	w.discoveries["workloads"] = &fakeDiscovery{rules: []discovery.RuleSource{{
		Path: "/rules/0", ConfigPath: "/rules/0/source/config", Type: "fake",
		Config: jsontext.Value(`{}`), Pipeline: []string{},
		Importance: model.ImportanceRequired, Sensitivity: model.SensitivityPlain,
	}}}
	cfg := map[string]any{
		"version":     1,
		"layout":      configLayout(),
		"discoveries": map[string]any{"workloads": instance("fake")},
	}
	raw, err := jsonMarshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	// A defaults block for the rule's source type whose only key is absent from
	// the rule's own config, against a rule config path that does not exist: the
	// merge cannot place the defaulted value and reports it.
	defs := &Defaults{doc: jsontext.Value(`{"plugins":{"source/fake":{"injected":"v"}}}`)}
	_, err = New(context.Background(), raw, defs, w.registry, testLogger())
	if err == nil || !strings.Contains(err.Error(), "config:") {
		t.Fatalf("a merge failure was not reported: %v", err)
	}
}

func TestNewRefusesUnknownSourceType(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["sources"] = map[string]any{"vpn": sourceInstance("nosuch", "vpn", "wireguard", []string{}, "required", "plain")}
	if _, err := w.newEngineErr(t, cfg); err == nil || !strings.Contains(err.Error(), "/sources/vpn") {
		t.Fatalf("unknown source type accepted: %v", err)
	}
}

// TestNewRefusesAChainSourceWithAPipeline: the staged source carries the chain
// of the process that produced its bytes, so its own pipeline must be empty.
func TestNewRefusesAChainSourceWithAPipeline(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	w.extraSources["vpn"] = &chainSource{data: []byte("staged"), ext: ".bin"}

	_, err := w.newEngineErr(t, cfg)
	if err == nil || !strings.Contains(err.Error(), "pipeline must be empty") {
		t.Fatalf("a chain source with a pipeline was accepted: %v", err)
	}
}

// TestNewRefusesASecretPipelineThatCannotSeal: the engine asks every processor
// of a secret pipeline whether it seals while the configuration is read, and a
// processor that cannot answer fails the construction.
func TestNewRefusesASecretPipelineThatCannotSeal(t *testing.T) {
	w := newWorld(t)
	registerEngineProcessor(t, w, "zcdesc", describeErrProcessor{})
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["processors"] = map[string]any{"df": instance("zcdesc")}
	cfg["sources"] = map[string]any{"vpn": sourceInstance("fake", "vpn", "wireguard", []string{"df"}, "required", "secret")}

	_, err := w.newEngineErr(t, cfg)
	if err == nil || !strings.Contains(err.Error(), "pipeline") {
		t.Fatalf("a secret pipeline that cannot seal was accepted: %v", err)
	}
}

// TestNewRefusesAProcessorThatCannotEncode: a processor that says it cannot
// encode fails the pipeline at construction, before anything runs.
func TestNewRefusesAProcessorThatCannotEncode(t *testing.T) {
	w := newWorld(t)
	registerEngineProcessor(t, w, "zcecheck", encodeErrProcessor{})
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["processors"] = map[string]any{"ce": instance("zcecheck")}
	cfg["sources"] = map[string]any{"vpn": sourceInstance("fake", "vpn", "wireguard", []string{"ce"}, "required", "plain")}

	_, err := w.newEngineErr(t, cfg)
	if err == nil || !strings.Contains(err.Error(), "no recipient") {
		t.Fatalf("a processor that cannot encode was accepted: %v", err)
	}
}

// TestNewTracksAndClosesClosableInstances: a plugin instance that is an
// io.Closer joins the engine's lifetime list and is closed with it.
func TestNewTracksAndClosesClosableInstances(t *testing.T) {
	w := newWorld(t)
	cs := &closerSource{}
	w.extraSources["vpn"] = cs

	e := w.newEngine(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}))
	if len(e.closers) != 1 {
		t.Fatalf("the closable source was not tracked: %d closers", len(e.closers))
	}
	if err := e.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !cs.closed {
		t.Fatal("Close did not close the tracked source")
	}
}

func TestEffectiveConfigReturnsTheMergedDocument(t *testing.T) {
	w := newWorld(t)
	e := w.newEngine(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}))
	merged, pointers := e.EffectiveConfig()
	if len(merged) == 0 {
		t.Fatal("EffectiveConfig returned an empty document")
	}
	if pointers != nil {
		t.Fatalf("pointers %v, want nil for a config merged with no defaults", pointers)
	}
}

// --- rules.go ---------------------------------------------------------------

// closerSource is a source that is also an io.Closer, the shape the engine's
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

func registerEngineProcessor(t *testing.T, w *world, typ string, p processor.Processor) {
	t.Helper()
	err := plugin.Register(w.registry, processor.Kind, plugin.Spec[map[string]jsontext.Value, processor.Processor]{
		Type: typ, Example: []byte(`{}`),
		New: func(context.Context, map[string]jsontext.Value, plugin.Deps) (processor.Processor, error) {
			return p, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
}
