package instances_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	"encoding/json/jsontext"
	json "encoding/json/v2"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/configuration"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
)

type sample interface{ Sample() string }

type treeConfig struct {
	Name   string               `json:"name" doc:"a name"`
	Region string               `json:"region" doc:"a region"`
	Token  configuration.Secret `json:"token" doc:"a secret file"`
	Nested *nestedConfig        `json:"nested" doc:"nested thing"`
	List   []leafConfig         `json:"list" doc:"list of things"`
	Maybe  string               `json:"maybe" doc:"may be absent" optional:"true"`
	Values map[string]any       `json:"values" doc:"free-form values"`
}

type nestedConfig struct {
	Depth int `json:"depth" doc:"a depth"`
}

type leafConfig struct {
	Key string `json:"key" doc:"a key"`
}

func newSample(_ context.Context, cfg treeConfig, _ instances.Deps) (sample, error) {
	return sampleImpl(cfg.Name), nil
}

type sampleImpl string

func (s sampleImpl) Sample() string { return string(s) }

var sampleKind = instances.NewKind[sample]("sample")

func example() []byte {
	return []byte(`{"name":"x","region":"r","token":{"file":"/f"},"nested":{"depth":1},"list":[{"key":"k"}],"values":{},"maybe":"m"}`)
}

func registerSample(t *testing.T, extra func(*instances.Registry)) *instances.Registry {
	t.Helper()
	r := instances.NewRegistry()
	err := instances.Register(r, sampleKind, instances.FactorySpec[treeConfig, sample]{
		Type: "tree", Summary: "a sample", Stability: instances.Stable, Example: example(),
		New: newSample,
	})
	if err != nil {
		t.Fatal(err)
	}
	if extra != nil {
		extra(r)
	}
	return r
}

func TestRegisterRejectsBadTypes(t *testing.T) {
	cases := []struct {
		name string
		spec instances.FactorySpec[treeConfig, sample]
	}{
		{"bad name", instances.FactorySpec[treeConfig, sample]{Type: "Bad Name", New: newSample}},
		{"empty name", instances.FactorySpec[treeConfig, sample]{Type: "", New: newSample}},
		{"nil constructor", instances.FactorySpec[treeConfig, sample]{Type: "tree"}},
		{"bad example", instances.FactorySpec[treeConfig, sample]{Type: "tree", New: newSample, Example: []byte(`{"name":42}`)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := instances.NewRegistry()
			if err := instances.Register(r, sampleKind, tc.spec); err == nil {
				t.Fatal("Register accepted a broken spec")
			}
		})
	}
	r := registerSample(t, nil)
	if err := instances.Register(r, sampleKind, instances.FactorySpec[treeConfig, sample]{Type: "tree", New: newSample}); err == nil {
		t.Fatal("duplicate registration accepted")
	}
	// A kind token with no name cannot key the registry.
	err := instances.Register(instances.NewRegistry(), instances.NewKind[sample](""), instances.FactorySpec[treeConfig, sample]{Type: "tree", New: newSample})
	if err == nil || !strings.Contains(err.Error(), "kind name is empty") {
		t.Fatalf("a kind with no name was registered: %v", err)
	}
}

// TestCheckConfigTypeAndBinariesAnswerForKnownAndUnknownTypes: the three
// registry lookups a defaults check and `doctor` make, on both sides of the
// known/unknown boundary.
func TestCheckConfigTypeAndBinariesAnswerForKnownAndUnknownTypes(t *testing.T) {
	r := instances.NewRegistry()
	err := instances.Register(r, sampleKind, instances.FactorySpec[treeConfig, sample]{
		Type: "tree", Binaries: []string{"zstd"}, New: newSample,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Check("sample", "ghost", example()); err == nil || !strings.Contains(err.Error(), "unknown sample type") {
		t.Fatalf("Check on an unknown type = %v", err)
	}
	if _, ok := r.ConfigType("sample", "ghost"); ok {
		t.Fatal("ConfigType resolved an unknown type")
	}
	if ft, ok := r.ConfigType("sample", "tree"); !ok || ft != reflect.TypeFor[treeConfig]() {
		t.Fatalf("ConfigType(tree) = %v, %v", ft, ok)
	}
	if got := r.Binaries("sample", "tree"); len(got) != 1 || got[0] != "zstd" {
		t.Fatalf("Binaries(tree) = %v", got)
	}
	if got := r.Binaries("sample", "ghost"); got != nil {
		t.Fatalf("Binaries(ghost) = %v, want nil", got)
	}
}

// TestIDIsDerivedFromKindAndType: a factory's identity is its kind and its type
// and nothing else — one spelling, shared by the descriptor and the method a
// registration calls, so no literal can drift from the registered pair.
func TestIDIsDerivedFromKindAndType(t *testing.T) {
	r := registerSample(t, nil)
	descs := r.Descriptors()
	if len(descs) != 1 {
		t.Fatalf("Descriptors = %d", len(descs))
	}
	if got, want := descs[0].ID, "sample/tree"; got != want {
		t.Fatalf("descriptor ID = %q, want %q", got, want)
	}
	if got, want := instances.ID("sample", "tree"), "sample/tree"; got != want {
		t.Fatalf("ID = %q, want %q", got, want)
	}
	if got, want := sampleKind.ID("tree"), descs[0].ID; got != want {
		t.Fatalf("Kind.ID = %q, want the descriptor's %q", got, want)
	}
}

// TestFieldTypeIsTheGoTypeName: the descriptor prints the Go type exactly as
// reflect does — the names a deployment reads in `plugins describe` and
// `schema`, with no translation layer that could disagree with the config.
func TestFieldTypeIsTheGoTypeName(t *testing.T) {
	type labelled struct {
		Password configuration.Secret   `json:"password" doc:"a secret file"`
		When     configuration.Template `json:"when" doc:"a template"`
		Class    pluginerrors.Class     `json:"class" doc:"an error class"`
	}
	r := instances.NewRegistry()
	err := instances.Register(r, sampleKind, instances.FactorySpec[labelled, sample]{
		Type: "labelled",
		New:  func(context.Context, labelled, instances.Deps) (sample, error) { return sampleImpl("l"), nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"password": "configuration.Secret",
		"when":     "configuration.Template",
		"class":    "pluginerrors.Class",
	}
	descs := r.Descriptors()
	if len(descs) != 1 {
		t.Fatalf("Descriptors = %d", len(descs))
	}
	for _, f := range descs[0].Fields {
		if w, ok := want[f.Name]; ok && f.Type != w {
			t.Errorf("field %q type = %q, want %q", f.Name, f.Type, w)
		}
	}
}

// TestBuildRefusesAValueOfAnotherSeamToken: a Kind carries only its name, so two
// tokens with the same name but different interfaces address the same entry. The
// constructed value satisfies the token that registered it and not the other,
// and Build refuses rather than handing a caller the wrong seam.
func TestBuildRefusesAValueOfAnotherSeamToken(t *testing.T) {
	type other interface{ Other() string }
	r := registerSample(t, nil)
	_, err := instances.Build(context.Background(), r, instances.NewKind[other]("sample"), "tree", example(), instances.Deps{Instance: "one"})
	if err == nil || !strings.Contains(err.Error(), "constructed") {
		t.Fatalf("Build with a mismatched seam token = %v", err)
	}
}

func TestBuildAndCheck(t *testing.T) {
	r := registerSample(t, nil)
	ctx := context.Background()
	deps := instances.Deps{Instance: "one", Log: slog.New(slog.DiscardHandler)}
	v, err := instances.Build(ctx, r, sampleKind, "tree", example(), deps)
	if err != nil {
		t.Fatal(err)
	}
	if v.Sample() != "x" {
		t.Fatalf("Sample = %q", v.Sample())
	}
	if _, err := r.BuildAny(ctx, "sample", "nope", example(), deps); err == nil {
		t.Fatal("BuildAny accepted an unknown type")
	}
}

func TestBuildUnknownTypeListsKnown(t *testing.T) {
	r := registerSample(t, func(r *instances.Registry) {
		err := instances.Register(r, sampleKind, instances.FactorySpec[treeConfig, sample]{
			Type: "leaf", New: newSample, Example: example(),
		})
		if err != nil {
			t.Fatal(err)
		}
	})
	_, err := instances.Build(context.Background(), r, sampleKind, "missing", example(), instances.Deps{Instance: "one"})
	if err == nil {
		t.Fatal("unknown type accepted")
	}
	if !strings.Contains(err.Error(), `unknown sample type "missing"`) ||
		!strings.Contains(err.Error(), "leaf, tree") {
		t.Fatalf("error does not list known types: %v", err)
	}
	if types := r.Types("sample"); len(types) != 2 || types[0] != "leaf" || types[1] != "tree" {
		t.Fatalf("Types(sample) = %v", types)
	}
	if types := r.Types("ghost"); types != nil {
		t.Fatalf("Types(ghost) = %v", types)
	}
}

func TestBuildErrorNamesInstance(t *testing.T) {
	r := registerSample(t, nil)
	_, err := instances.Build(context.Background(), r, sampleKind, "tree", jsontext.Value(`{}`), instances.Deps{Instance: "primary"})
	if err == nil {
		t.Fatal("broken config accepted")
	}
	if !strings.Contains(err.Error(), `sample "primary"`) {
		t.Fatalf("error does not name the instance: %v", err)
	}
}

func TestDecodeReportsEveryMissingFieldWithPath(t *testing.T) {
	r := registerSample(t, nil)
	err := r.Check("sample", "tree", jsontext.Value(`{"token":{},"list":[{"key":"a"},{}],"nested":{},"values":{"a":{"b":1}}}`))
	if err == nil {
		t.Fatal("missing fields accepted")
	}
	msg := err.Error()
	for _, want := range []string{
		"missing field /name", "missing field /region", "missing field /token/file",
		"missing field /nested/depth", "missing field /list/1/key",
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("error does not contain %q:\n%s", want, msg)
		}
	}
	if strings.Contains(msg, "/maybe") {
		t.Errorf("optional field reported missing:\n%s", msg)
	}
	// An absent nested struct is itself the missing field.
	err = r.Check("sample", "tree", jsontext.Value(`{"name":"x","region":"r","list":[],"values":{}}`))
	if err == nil || !strings.Contains(err.Error(), "missing field /token") || !strings.Contains(err.Error(), "missing field /nested") {
		t.Fatalf("nested structs not reported: %v", err)
	}
}

func TestDecodeRejectsUnknownMembersWithPath(t *testing.T) {
	r := registerSample(t, nil)
	raw := jsontext.Value(`{"name":"x","region":"r","token":{"file":"/f"},"nested":{"depth":1,"dpeth":2},"list":[],"values":{}}`)
	err := r.Check("sample", "tree", raw)
	if err == nil {
		t.Fatal("unknown member accepted")
	}
	if !strings.Contains(err.Error(), "dpeth") || !strings.Contains(err.Error(), "/nested") {
		t.Fatalf("error lacks member or path: %v", err)
	}
}

func TestOptionalFieldWhenPresentIsWalked(t *testing.T) {
	r := registerSample(t, nil)
	raw := jsontext.Value(`{"name":"x","region":"r","token":{"file":"/f"},"nested":{"depth":1},"list":[],"values":{},"maybe":{"inner":1}}`)
	err := r.Check("sample", "tree", raw)
	if err == nil || !strings.Contains(err.Error(), "maybe") {
		t.Fatalf("present optional field of the wrong type accepted: %v", err)
	}
}

func TestSecretRejectsLiteral(t *testing.T) {
	r := registerSample(t, nil)
	raw := jsontext.Value(`{"name":"x","region":"r","token":"/literal/path","nested":{"depth":1},"list":[],"values":{}}`)
	if err := r.Check("sample", "tree", raw); err == nil {
		t.Fatal("a literal string decoded into configuration.Secret")
	}
}

type validatingConfig struct {
	Name string `json:"name" doc:"a name"`
}

func (c validatingConfig) Validate() error {
	if c.Name == "bad" {
		return errors.New("name is bad")
	}
	return nil
}

func TestValidatorRunsOnlyOnCleanDecode(t *testing.T) {
	r := instances.NewRegistry()
	err := instances.Register(r, sampleKind, instances.FactorySpec[validatingConfig, sample]{
		Type: "v", Example: []byte(`{"name":"ok"}`),
		New: func(context.Context, validatingConfig, instances.Deps) (sample, error) { return sampleImpl("v"), nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Check("sample", "v", jsontext.Value(`{"name":"bad"}`)); err == nil || !strings.Contains(err.Error(), "name is bad") {
		t.Fatalf("validator did not run: %v", err)
	}
	// A structural error suppresses the validator: decoding was not clean.
	err = r.Check("sample", "v", jsontext.Value(`{"nome":"bad"}`))
	if err == nil || !strings.Contains(err.Error(), "nome") {
		t.Fatalf("structural error missing: %v", err)
	}
	if err != nil && strings.Contains(err.Error(), "name is bad") {
		t.Fatalf("validator ran on a broken decode: %v", err)
	}
}

func TestDescriptorsCarryFieldsDocsAndSecrecy(t *testing.T) {
	r := registerSample(t, nil)
	descs := r.Descriptors()
	if len(descs) != 1 {
		t.Fatalf("Descriptors = %d", len(descs))
	}
	d := descs[0]
	if d.Kind != "sample" || d.Type != "tree" || d.Summary != "a sample" || d.Stability != instances.Stable {
		t.Fatalf("descriptor: %+v", d)
	}
	byName := map[string]instances.Field{}
	var walk func([]instances.Field)
	walk = func(fs []instances.Field) {
		for _, f := range fs {
			byName[f.Name] = f
			walk(f.Fields)
		}
	}
	walk(d.Fields)
	if f, ok := byName["region"]; !ok || f.Doc != "a region" {
		t.Fatalf("region field: %+v", f)
	}
	if f, ok := byName["token"]; !ok || !f.Secret {
		t.Fatalf("secret field not marked: %+v", f)
	}
	if f, ok := byName["depth"]; !ok || f.Type != "int" {
		t.Fatalf("nested field: %+v", f)
	}
	if _, ok := byName["maybe"]; !ok {
		t.Fatal("optional field missing from descriptor")
	}
}

func TestErrorClasses(t *testing.T) {
	base := errors.New("boom")
	if class, _ := pluginerrors.ClassOf(base); class != "" {
		t.Fatalf("unclassified error reports %q", class)
	}
	if class, _ := pluginerrors.ClassOf(fmt.Errorf("wrap: %w", pluginerrors.Permanent(base))); class != pluginerrors.ClassPermanent {
		t.Fatalf("Permanent inside a wrap is not found")
	}
	if class, _ := pluginerrors.ClassOf(pluginerrors.Transient(base)); class != pluginerrors.ClassTransient {
		t.Fatal("Transient")
	}
	if class, _ := pluginerrors.ClassOf(pluginerrors.NotFound(base)); class != pluginerrors.ClassNotFound {
		t.Fatal("NotFound")
	}
	class, after := pluginerrors.ClassOf(pluginerrors.RetryAfter(base, 30*time.Second))
	if class != pluginerrors.ClassTransient || after != 30*time.Second {
		t.Fatalf("RetryAfter: %v %v", class, after)
	}
	if !errors.Is(pluginerrors.Permanent(base), base) {
		t.Fatal("class markers must unwrap to the cause")
	}
}

// ruleConfig nests one rule source, the way a discovery config does.
type ruleConfig struct {
	Rules []ruleEntry `json:"rules" doc:"the rules"`
}

type ruleEntry struct {
	Token string `json:"token" doc:"a token"`
}

func ruleSources(raw jsontext.Value) ([]configuration.RuleSource, error) {
	var cfg ruleConfig
	if err := configuration.Decode(raw, &cfg); err != nil {
		return nil, err
	}
	out := make([]configuration.RuleSource, 0, len(cfg.Rules))
	for i := range cfg.Rules {
		out = append(out, configuration.RuleSource{
			Path:   fmt.Sprintf("/rules/%d", i),
			Type:   "sample",
			Config: jsontext.Value(`{"token":"t"}`),
		})
	}
	return out, nil
}

// TestRuleSourcesRunsTheRegisteredPureHook: the configuration layer reads a
// type's nested rules through the hook it registered, without constructing the
// plugin; a type that declares no hook contributes nothing and an unregistered
// type is still an error.
func TestRuleSourcesRunsTheRegisteredPureHook(t *testing.T) {
	var schema configuration.Schema = instances.NewRegistry()
	_ = schema
	var ruleSchema configuration.RuleSchema = instances.NewRegistry()
	_ = ruleSchema
	var providerSchema configuration.ProviderSchema = instances.NewRegistry()
	_ = providerSchema

	r := instances.NewRegistry()
	built := 0
	err := instances.Register(r, sampleKind, instances.FactorySpec[ruleConfig, sample]{
		Type:  "rules",
		Rules: ruleSources,
		New: func(context.Context, ruleConfig, instances.Deps) (sample, error) {
			built++
			return sampleImpl("r"), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = instances.Register(r, sampleKind, instances.FactorySpec[ruleConfig, sample]{
		Type: "plain",
		New:  func(context.Context, ruleConfig, instances.Deps) (sample, error) { return sampleImpl("p"), nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	rules, err := r.RuleSources("sample", "rules", jsontext.Value(`{"rules":[{"token":"t"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(rules) != 1 || rules[0].Path != "/rules/0" || rules[0].Type != "sample" {
		t.Fatalf("rules = %+v", rules)
	}
	if built != 0 {
		t.Fatal("the rule hook constructed the plugin")
	}
	if rules, err := r.RuleSources("sample", "plain", example()); err != nil || rules != nil {
		t.Fatalf("a type without a rule hook = %+v, %v", rules, err)
	}
	if _, err := r.RuleSources("sample", "ghost", jsontext.Value(`{}`)); err == nil ||
		!strings.Contains(err.Error(), `unknown sample type "ghost"`) {
		t.Fatalf("an unknown type = %v", err)
	}
}

type providerConfig struct {
	Namespace string `json:"namespace" doc:"a namespace"`
}

// TestRegisterProviderPublishesTheConfigType: a plugin that declares a stable ID
// instead of a factory registers the type its settings decode into, so the
// defaults merge and the offline walk judge its plugins.config block; a factory
// ID, a malformed ID and a duplicate are refused at startup.
func TestRegisterProviderPublishesTheConfigType(t *testing.T) {
	r := instances.NewRegistry()
	if err := instances.RegisterProvider[providerConfig](r, "reporting"); err != nil {
		t.Fatal(err)
	}
	got, ok := r.ProviderConfigType("reporting")
	if !ok || got != reflect.TypeFor[providerConfig]() {
		t.Fatalf("ProviderConfigType(reporting) = %v, %v", got, ok)
	}
	if _, ok := r.ProviderConfigType("source/postgres"); ok {
		t.Fatal("a factory ID resolved as a provider")
	}
	if err := instances.RegisterProvider[providerConfig](r, "source/postgres"); err == nil ||
		!strings.Contains(err.Error(), "stable ID") {
		t.Fatalf("a factory-shaped provider ID = %v", err)
	}
	if err := instances.RegisterProvider[providerConfig](r, "Bad Name"); err == nil ||
		!strings.Contains(err.Error(), "must match") {
		t.Fatalf("a malformed stable ID = %v", err)
	}
	if err := instances.RegisterProvider[providerConfig](r, "reporting"); err == nil ||
		!strings.Contains(err.Error(), "registered twice") {
		t.Fatalf("a duplicate provider = %v", err)
	}
}

func TestDurationDecode(t *testing.T) {
	type cfg struct {
		Timeout model.Duration `json:"timeout" doc:"a timeout"`
	}
	for _, tc := range []struct {
		raw  string
		want time.Duration
		bad  bool
	}{
		{`{"timeout":"30s"}`, 30 * time.Second, false},
		{`{"timeout":"85m"}`, 85 * time.Minute, false},
		{`{"timeout":30}`, 0, true},
		{`{"timeout":"soon"}`, 0, true},
	} {
		var c cfg
		err := configuration.Decode(jsontext.Value(tc.raw), &c)
		if tc.bad {
			if err == nil {
				t.Errorf("%s decoded without error", tc.raw)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: %v", tc.raw, err)
			continue
		}
		if c.Timeout.Std() != tc.want {
			t.Errorf("%s decoded to %v", tc.raw, c.Timeout.Std())
		}
	}
	b, err := json.Marshal(cfg{Timeout: model.Duration(90 * time.Second)})
	if err != nil || string(b) != `{"timeout":"1m30s"}` {
		t.Fatalf("marshal: %s %v", b, err)
	}
}
