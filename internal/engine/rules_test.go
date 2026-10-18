package engine

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"strings"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/discovery"
	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/source"
)

// shapedConfig is a source config with the shape a discovery rule nests: what
// the offline rule check walks without rendering a single template.
type shapedConfig struct {
	Address  string            `json:"address"`
	Database string            `json:"database"`
	Password plugin.Secret     `json:"password"`
	Retries  int               `json:"retries"`
	Labels   map[string]string `json:"labels"`
}

func registerShaped(t *testing.T, w *world) {
	t.Helper()
	err := plugin.Register(w.registry, source.Kind, plugin.Spec[shapedConfig, source.Source]{
		Type:    "shaped",
		Example: []byte(`{"address":"a:5432","database":"db","password":{"file":"/f"},"retries":1,"labels":{}}`),
		New: func(_ context.Context, _ shapedConfig, deps plugin.Deps) (source.Source, error) {
			s, ok := w.sources[deps.Instance]
			if !ok {
				s = &fakeSource{data: []byte("x")}
				w.sources[deps.Instance] = s
			}
			return s, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
}

func rulesConfig(t *testing.T, w *world) map[string]any {
	t.Helper()
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["processors"] = map[string]any{"zstd3": instance("fake"), "seal": instance("fake")}
	cfg["discoveries"] = map[string]any{"workloads": instance("fake")}
	return cfg
}

// TestDiscoveryRulesAreCheckedOffline: a typo inside a rule's nested source
// config, an unregistered source type, an unknown pipeline and a secret
// artifact in a pipeline that does not seal are all reported by construction —
// each with its path, all at once, before anything runs in the cluster.
func TestDiscoveryRulesAreCheckedOffline(t *testing.T) {
	w := newWorld(t)
	registerShaped(t, w)
	cfg := rulesConfig(t, w)
	w.discoveries["workloads"] = &fakeDiscovery{rules: []discovery.RuleSource{
		{
			Path: "/rules/0", ConfigPath: "/rules/0/source/config", Type: "shaped",
			Config:   []byte(`{"address":"a:1","databse":"db","password":{"file":"/f"},"retries":"{{ port 0 0 }}","labels":{},"bogus":true}`),
			Pipeline: []string{"zstd3"}, Importance: model.ImportanceRequired, Sensitivity: model.SensitivitySecret,
		},
		{
			Path: "/rules/1", ConfigPath: "/rules/1/source/config", Type: "ghost",
			Config: []byte(`{}`), Pipeline: []string{"nope"},
			Importance: model.ImportanceRequired, Sensitivity: model.SensitivityPlain,
		},
		{
			// A member the file left empty: YAML writes that as a null, and a
			// null read as "absent" is a list the run silently does without.
			Path: "/rules/2", ConfigPath: "/rules/2/source/config", Type: "shaped",
			Config:   []byte(`{"address":"a:1","database":"db","password":{"file":"/f"},"retries":1,"labels":{"app":null}}`),
			Pipeline: []string{"zstd3"}, Importance: model.ImportanceRequired, Sensitivity: model.SensitivityPlain,
		},
	}}
	raw, err := jsonMarshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	_, err = New(context.Background(), raw, nil, w.registry, testLogger())
	if err == nil {
		t.Fatal("a broken discovery rule passed construction")
	}
	msg := err.Error()
	for _, want := range []string{
		"missing field /discoveries/workloads/rules/0/source/config/database",
		"unknown field /discoveries/workloads/rules/0/source/config/databse",
		"unknown field /discoveries/workloads/rules/0/source/config/bogus",
		"/discoveries/workloads/rules/0/source/config/retries must be a JSON number, got a JSON string",
		`/discoveries/workloads/rules/0: a secret artifact's pipeline [zstd3] has no sealing processor`,
		`/discoveries/workloads/rules/1: unknown source type "ghost" (known: [fake shaped])`,
		`/discoveries/workloads/rules/1/pipeline/0: unknown processor instance "nope" (known: [seal zstd3])`,
		`/discoveries/workloads/rules/2/source/config/labels/app is null; a member that is present carries a value, and a section that is out is written by removing it`,
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("construction error lacks %q:\n%s", want, msg)
		}
	}
}

// TestDiscoveryRulesAcceptTemplatesAndSealedRoutes: the check walks shapes, not
// values — an unrendered template is fine — and a secret artifact routed
// through a pipeline that seals passes.
func TestDiscoveryRulesAcceptTemplatesAndSealedRoutes(t *testing.T) {
	w := newWorld(t)
	registerShaped(t, w)
	cfg := rulesConfig(t, w)
	w.processors["seal"] = &fakeProcessor{seals: true}
	w.discoveries["workloads"] = &fakeDiscovery{rules: []discovery.RuleSource{
		{
			Path: "/rules/0", ConfigPath: "/rules/0/source/config", Type: "shaped",
			Config: []byte(`{"address":"{{ label \"svc\" }}:5432","database":"{{ .Namespace }}/db",
				"password":{"file":"{{ envFile 0 \"PGPASSWORD\" }}"},"retries":1,"labels":{"app":"{{ .Name }}"}}`),
			Pipeline: []string{"zstd3"}, Importance: model.ImportanceBestEffort, Sensitivity: model.SensitivityPlain,
		},
		{
			Path: "/rules/1", ConfigPath: "/rules/1/source/config", Type: "shaped",
			Config:   []byte(`{"address":"a:1","database":"db","password":{"file":"/f"},"retries":1,"labels":{}}`),
			Pipeline: []string{"zstd3", "seal"}, Importance: model.ImportanceRequired, Sensitivity: model.SensitivitySecret,
		},
	}}
	w.newEngine(t, cfg)
}

// plainRuleDiscovery is a Discoverer with no rules: the offline rule check skips it.
type plainRuleDiscovery struct{}

func (plainRuleDiscovery) Discover(context.Context) ([]discovery.Found, error) { return nil, nil }

// ruleErrDiscovery reports its rules with an error, the shape a discoverer
// whose rules cannot be read has.
type ruleErrDiscovery struct{}

func (ruleErrDiscovery) Discover(context.Context) ([]discovery.Found, error) { return nil, nil }

func (ruleErrDiscovery) RuleSources(context.Context) ([]discovery.RuleSource, error) {
	return nil, errors.New("the rules are unreadable")
}

func registerDiscoveryType(t *testing.T, w *world, typ string, d discovery.Discoverer) {
	t.Helper()
	err := plugin.Register(w.registry, discovery.Kind, plugin.Spec[map[string]jsontext.Value, discovery.Discoverer]{
		Type: typ, Example: []byte(`{}`),
		New: func(context.Context, map[string]jsontext.Value, plugin.Deps) (discovery.Discoverer, error) {
			return d, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
}

func rulesConfigDoc() map[string]any {
	return map[string]any{
		"version":     1,
		"layout":      configLayout(),
		"discoveries": map[string]any{"workloads": instance("fake")},
	}
}

func TestDiscoveryWithoutRulesIsSkipped(t *testing.T) {
	w := newWorld(t)
	registerDiscoveryType(t, w, "zcplain", plainRuleDiscovery{})
	cfg := map[string]any{
		"version":     1,
		"layout":      configLayout(),
		"discoveries": map[string]any{"workloads": instance("zcplain")},
	}
	if _, err := w.newEngineErr(t, cfg); err != nil {
		t.Fatalf("a discovery with no rules was refused: %v", err)
	}
}

func TestRuleReadFailureIsReported(t *testing.T) {
	w := newWorld(t)
	registerDiscoveryType(t, w, "zcruleerr", ruleErrDiscovery{})
	cfg := map[string]any{
		"version":     1,
		"layout":      configLayout(),
		"discoveries": map[string]any{"workloads": instance("zcruleerr")},
	}
	_, err := w.newEngineErr(t, cfg)
	if err == nil || !strings.Contains(err.Error(), "reading the rules") {
		t.Fatalf("an unreadable rule set was accepted: %v", err)
	}
}

func TestRuleEnvelopeEnumsAreChecked(t *testing.T) {
	w := newWorld(t)
	cfg := rulesConfigDoc()
	w.discoveries["workloads"] = &fakeDiscovery{rules: []discovery.RuleSource{{
		Path: "/rules/0", Type: "", Importance: "", Sensitivity: "",
	}}}
	_, err := w.newEngineErr(t, cfg)
	if err == nil {
		t.Fatal("a rule with no envelope enums was accepted")
	}
	msg := err.Error()
	for _, want := range []string{"importance must be", "sensitivity must be"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error lacks %q:\n%s", want, msg)
		}
	}
}

func TestRuleSecretPipelineThatCannotSealIsReported(t *testing.T) {
	w := newWorld(t)
	registerEngineProcessor(t, w, "zcdesc", describeErrProcessor{})
	cfg := rulesConfigDoc()
	cfg["processors"] = map[string]any{"df": instance("zcdesc")}
	w.discoveries["workloads"] = &fakeDiscovery{rules: []discovery.RuleSource{{
		Path: "/rules/0", Type: "fake", Config: jsontext.Value(`{}`), Pipeline: []string{"df"},
		Importance: model.ImportanceRequired, Sensitivity: model.SensitivitySecret,
	}}}
	_, err := w.newEngineErr(t, cfg)
	if err == nil || !strings.Contains(err.Error(), "pipeline") {
		t.Fatalf("a rule whose sealing processor cannot be described was accepted: %v", err)
	}
}

// --- units.go ---------------------------------------------------------------
