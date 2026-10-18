package plugin_test

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

	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
)

type sample interface{ Sample() string }

type treeConfig struct {
	Name   string         `json:"name" doc:"a name"`
	Region string         `json:"region" doc:"a region"`
	Token  plugin.Secret  `json:"token" doc:"a secret file"`
	Nested *nestedConfig  `json:"nested" doc:"nested thing"`
	List   []leafConfig   `json:"list" doc:"list of things"`
	Maybe  string         `json:"maybe" doc:"may be absent" optional:"true"`
	Values map[string]any `json:"values" doc:"free-form values"`
}

type nestedConfig struct {
	Depth int `json:"depth" doc:"a depth"`
}

type leafConfig struct {
	Key string `json:"key" doc:"a key"`
}

func newSample(_ context.Context, cfg treeConfig, _ plugin.Deps) (sample, error) {
	return sampleImpl(cfg.Name), nil
}

type sampleImpl string

func (s sampleImpl) Sample() string { return string(s) }

var sampleKind = plugin.NewKind[sample]("sample")

func example() []byte {
	return []byte(`{"name":"x","region":"r","token":{"file":"/f"},"nested":{"depth":1},"list":[{"key":"k"}],"values":{},"maybe":"m"}`)
}

func registerSample(t *testing.T, extra func(*plugin.Registry)) *plugin.Registry {
	t.Helper()
	r := plugin.NewRegistry()
	err := plugin.Register(r, sampleKind, plugin.Spec[treeConfig, sample]{
		Type: "tree", Summary: "a sample", Stability: plugin.Stable, Example: example(),
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
		spec plugin.Spec[treeConfig, sample]
	}{
		{"bad name", plugin.Spec[treeConfig, sample]{Type: "Bad Name", New: newSample}},
		{"empty name", plugin.Spec[treeConfig, sample]{Type: "", New: newSample}},
		{"nil constructor", plugin.Spec[treeConfig, sample]{Type: "tree"}},
		{"bad example", plugin.Spec[treeConfig, sample]{Type: "tree", New: newSample, Example: []byte(`{"name":42}`)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := plugin.NewRegistry()
			if err := plugin.Register(r, sampleKind, tc.spec); err == nil {
				t.Fatal("Register accepted a broken spec")
			}
		})
	}
	r := registerSample(t, nil)
	if err := plugin.Register(r, sampleKind, plugin.Spec[treeConfig, sample]{Type: "tree", New: newSample}); err == nil {
		t.Fatal("duplicate registration accepted")
	}
	// A kind token with no name cannot key the registry.
	err := plugin.Register(plugin.NewRegistry(), plugin.NewKind[sample](""), plugin.Spec[treeConfig, sample]{Type: "tree", New: newSample})
	if err == nil || !strings.Contains(err.Error(), "kind name is empty") {
		t.Fatalf("a kind with no name was registered: %v", err)
	}
}

// TestCheckConfigTypeAndRequiresAnswerForKnownAndUnknownTypes: the three
// registry lookups a defaults check and `doctor` make, on both sides of the
// known/unknown boundary.
func TestCheckConfigTypeAndRequiresAnswerForKnownAndUnknownTypes(t *testing.T) {
	r := plugin.NewRegistry()
	err := plugin.Register(r, sampleKind, plugin.Spec[treeConfig, sample]{
		Type: "tree", Requires: []string{"zstd"}, New: newSample,
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
	if got := r.Requires("sample", "tree"); len(got) != 1 || got[0] != "zstd" {
		t.Fatalf("Requires(tree) = %v", got)
	}
	if got := r.Requires("sample", "ghost"); got != nil {
		t.Fatalf("Requires(ghost) = %v, want nil", got)
	}
}

// TestBuildRefusesAValueOfAnotherSeamToken: a Kind carries only its name, so two
// tokens with the same name but different interfaces address the same entry. The
// constructed value satisfies the token that registered it and not the other,
// and Build refuses rather than handing a caller the wrong seam.
func TestBuildRefusesAValueOfAnotherSeamToken(t *testing.T) {
	type other interface{ Other() string }
	r := registerSample(t, nil)
	_, err := plugin.Build(context.Background(), r, plugin.NewKind[other]("sample"), "tree", example(), plugin.Deps{Instance: "one"})
	if err == nil || !strings.Contains(err.Error(), "constructed") {
		t.Fatalf("Build with a mismatched seam token = %v", err)
	}
}

func TestBuildAndCheck(t *testing.T) {
	r := registerSample(t, nil)
	ctx := context.Background()
	deps := plugin.Deps{Instance: "one", Log: slog.New(slog.DiscardHandler)}
	v, err := plugin.Build(ctx, r, sampleKind, "tree", example(), deps)
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
	r := registerSample(t, func(r *plugin.Registry) {
		err := plugin.Register(r, sampleKind, plugin.Spec[treeConfig, sample]{
			Type: "leaf", New: newSample, Example: example(),
		})
		if err != nil {
			t.Fatal(err)
		}
	})
	_, err := plugin.Build(context.Background(), r, sampleKind, "missing", example(), plugin.Deps{Instance: "one"})
	if err == nil {
		t.Fatal("unknown type accepted")
	}
	if !strings.Contains(err.Error(), `unknown sample type "missing"`) ||
		!strings.Contains(err.Error(), "leaf, tree") {
		t.Fatalf("error does not list known types: %v", err)
	}
}

func TestBuildErrorNamesInstance(t *testing.T) {
	r := registerSample(t, nil)
	_, err := plugin.Build(context.Background(), r, sampleKind, "tree", jsontext.Value(`{}`), plugin.Deps{Instance: "primary"})
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
		t.Fatal("a literal string decoded into plugin.Secret")
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
	r := plugin.NewRegistry()
	err := plugin.Register(r, sampleKind, plugin.Spec[validatingConfig, sample]{
		Type: "v", Example: []byte(`{"name":"ok"}`),
		New: func(context.Context, validatingConfig, plugin.Deps) (sample, error) { return sampleImpl("v"), nil },
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
	if d.Kind != "sample" || d.Type != "tree" || d.Summary != "a sample" || d.Stability != plugin.Stable {
		t.Fatalf("descriptor: %+v", d)
	}
	byName := map[string]plugin.Field{}
	var walk func([]plugin.Field)
	walk = func(fs []plugin.Field) {
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
	if class, _ := plugin.ClassOf(base); class != "" {
		t.Fatalf("unclassified error reports %q", class)
	}
	if class, _ := plugin.ClassOf(fmt.Errorf("wrap: %w", plugin.Permanent(base))); class != plugin.ClassPermanent {
		t.Fatalf("Permanent inside a wrap is not found")
	}
	if class, _ := plugin.ClassOf(plugin.Transient(base)); class != plugin.ClassTransient {
		t.Fatal("Transient")
	}
	if class, _ := plugin.ClassOf(plugin.NotFound(base)); class != plugin.ClassNotFound {
		t.Fatal("NotFound")
	}
	class, after := plugin.ClassOf(plugin.RetryAfter(base, 30*time.Second))
	if class != plugin.ClassTransient || after != 30*time.Second {
		t.Fatalf("RetryAfter: %v %v", class, after)
	}
	if !errors.Is(plugin.Permanent(base), base) {
		t.Fatal("class markers must unwrap to the cause")
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
		err := plugin.Decode(jsontext.Value(tc.raw), &c)
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
