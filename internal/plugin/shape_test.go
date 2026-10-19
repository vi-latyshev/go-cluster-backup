package plugin_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"encoding/json/jsontext"
	json "encoding/json/v2"

	"github.com/vi-latyshev/go-cluster-backup/internal/cli"
	"github.com/vi-latyshev/go-cluster-backup/internal/defaults"
	"github.com/vi-latyshev/go-cluster-backup/internal/discovery"
	"github.com/vi-latyshev/go-cluster-backup/internal/engine"
	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"

	"log/slog"

	"go.yaml.in/yaml/v3"
)

// yamlToJSON is the YAML layer the command performs, for a test reading a
// YAML document into the JSON bytes the engine decodes.
func yamlToJSON(t *testing.T, data []byte) (jsontext.Value, error) {
	t.Helper()
	var doc any
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return nil, err
	}
	return json.Marshal(doc)
}

// TestCheckShapeReportsUnknownAndMissingMembers: a nested config's shape is
// checked without decoding a single value, every gap with its JSON-pointer path.
func TestCheckShapeReportsUnknownAndMissingMembers(t *testing.T) {
	r := registerSample(t, nil)
	raw := jsontext.Value(`{
		"nmae": "x", "region": "r",
		"token": {"file": "/f", "value": "literal"},
		"nested": {"deph": 1},
		"list": [{"ke": "k"}],
		"values": {"anything": {"goes": true}}
	}`)
	err := r.CheckShape("sample", "tree", raw, "/rules/0/source/config")
	if err == nil {
		t.Fatal("a malformed nested config passed the shape check")
	}
	msg := err.Error()
	for _, want := range []string{
		"missing field /rules/0/source/config/name",
		"unknown field /rules/0/source/config/nmae",
		"unknown field /rules/0/source/config/token/value",
		"missing field /rules/0/source/config/nested/depth",
		"unknown field /rules/0/source/config/nested/deph",
		"missing field /rules/0/source/config/list/0/key",
		"unknown field /rules/0/source/config/list/0/ke",
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("shape error lacks %q:\n%s", want, msg)
		}
	}
	if strings.Contains(msg, "values") {
		t.Errorf("a map of free-form values was constrained:\n%s", msg)
	}
}

// TestCheckShapeDoesNotEvaluateValues: a string value is judged as a string and
// never parsed — a template may be anything at run time — while the full
// decoder, which does evaluate, refuses the same bytes.
func TestCheckShapeDoesNotEvaluateValues(t *testing.T) {
	type templated struct {
		When  model.Duration  `json:"when"`
		Where plugin.Template `json:"where"`
		Port  int             `json:"port"`
	}
	r := newSampleRegistry(t, "templated", func(cfg templated) string { return cfg.Where.Text() })
	raw := jsontext.Value(`{"when": "{{ port 0 0 }}", "where": "{{ .Namespace }}", "port": 1}`)
	if err := r.CheckShape("sample", "templated", raw, "/config"); err != nil {
		t.Fatalf("the shape check evaluated a value: %v", err)
	}
	// The decoder does evaluate: a template standing where a duration goes
	// fails to parse there, which is why a nested config needs this check.
	if err := r.Check("sample", "templated", raw); err == nil {
		t.Fatal("the strict decoder accepted an unrendered duration")
	}
}

// TestCheckShapeRefusesKindsTheFieldCannotDecode: real plugin configs, real
// types. The reviewer's two typos — connect_attempts and password given a
// template — are refused offline with the path the rule check prints, and the
// literal counterparts of a wrong kind are refused too.
func TestCheckShapeRefusesKindsTheFieldCannotDecode(t *testing.T) {
	r := plugin.NewRegistry()
	if err := cli.Generic(r); err != nil {
		t.Fatal(err)
	}
	const sourcePath = "/discoveries/workloads/rules/0/source/config"
	cases := []struct {
		name, kind, typ, path, raw string
		want                       []string
	}{
		{
			name: "a template in a number and a secret object", kind: "source", typ: "postgres", path: sourcePath,
			raw: `{"address": "a:1", "database": "db", "username": "u",
				"password": "{{ envFile 0 \"PW\" }}", "format": "custom", "compression": "none",
				"connect_attempts": "{{ port 0 0 }}", "connect_interval": "5s"}`,
			want: []string{
				sourcePath + "/connect_attempts must be a JSON number, got a JSON string",
				sourcePath + "/password must be a JSON object, got a JSON string",
			},
		},
		{
			name: "a template in a boolean", kind: "source", typ: "files", path: sourcePath,
			raw:  `{"root": "/volumes/x", "ignore_vanished": "{{ .Phase }}"}`,
			want: []string{sourcePath + "/ignore_vanished must be a JSON boolean, got a JSON string"},
		},
		{
			name: "a template where a list goes", kind: "processor", typ: "age", path: "/processors/seal/config",
			raw:  `{"recipients": ["age1q"], "identities": "{{ json .Ids }}"}`,
			want: []string{"/processors/seal/config/identities must be a JSON array, got a JSON string"},
		},
		{
			name: "numbers where strings go, a duration included", kind: "source", typ: "postgres", path: sourcePath,
			raw: `{"address": "a:1", "database": 5, "username": "u", "password": {"file": "/f"},
				"format": "custom", "compression": "none", "connect_attempts": 6, "connect_interval": 5}`,
			want: []string{
				sourcePath + "/database must be a JSON string, got a JSON number",
				sourcePath + "/connect_interval must be a JSON string, got a JSON number",
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := r.CheckShape(tc.kind, tc.typ, jsontext.Value(tc.raw), tc.path)
			if err == nil {
				t.Fatal("values the fields cannot decode passed the shape check")
			}
			msg := err.Error()
			for _, want := range tc.want {
				if !strings.Contains(msg, want) {
					t.Errorf("shape error lacks %q:\n%s", want, msg)
				}
			}
		})
	}
}

// TestCheckShapeAcceptsTemplatesInEveryFieldThatTakesAString: the values a
// discovery rule legitimately carries — templates in string fields, a secret
// file path, a duration, a numeric literal, a list of strings.
func TestCheckShapeAcceptsTemplatesInEveryFieldThatTakesAString(t *testing.T) {
	type okConfig struct {
		Root     string            `json:"root"`
		Password plugin.Secret     `json:"password"`
		Timeout  model.Duration    `json:"timeout"`
		Retries  int               `json:"retries"`
		Hosts    []string          `json:"hosts"`
		Labels   map[string]string `json:"labels"`
	}
	r := newSampleRegistry(t, "ok", func(cfg okConfig) string { return cfg.Root })
	raw := jsontext.Value(`{
		"root": "/volumes/{{ .Namespace }}/{{ .Name }}",
		"password": {"file": "{{ envFile 0 \"PW\" }}"},
		"timeout": "{{ port 0 0 }}",
		"retries": 6,
		"hosts": ["a", "{{ label \"svc\" }}"],
		"labels": {"app": "{{ .Name }}"}
	}`)
	if err := r.CheckShape("sample", "ok", raw, "/config"); err != nil {
		t.Fatalf("a config of templates was refused: %v", err)
	}
}

// fromString unmarshals itself from a JSON string, the way plugin.Template and
// model.Duration do — a type whose accepted kinds only it knows.
type fromString struct{ text string }

func (f *fromString) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	f.text = s
	return nil
}

// TestCheckShapeLeavesUnclassifiableFieldsOpen: a field this check cannot
// classify is not judged — a false refusal blocks a correct production config,
// which is worse than a miss.
func TestCheckShapeLeavesUnclassifiableFieldsOpen(t *testing.T) {
	type opaque struct {
		Any    any            `json:"any"`
		Raw    jsontext.Value `json:"raw"`
		Custom *fromString    `json:"custom"`
		Bytes  []byte         `json:"bytes"`
		Free   map[string]any `json:"free"`
		Slabs  []fromString   `json:"slabs"`
	}
	r := newSampleRegistry(t, "opaque", func(cfg opaque) string { return cfg.Custom.text })
	// A type that unmarshals itself is judged by nobody here — not even when
	// this value would fail its own UnmarshalJSON at run time.
	raw := jsontext.Value(`{
		"any": 5,
		"raw": [1, 2, 3],
		"custom": {"x": 1},
		"bytes": "aGk=",
		"free": {"x": [1, "y"]},
		"slabs": ["one", "two"]
	}`)
	if err := r.CheckShape("sample", "opaque", raw, "/config"); err != nil {
		t.Fatalf("a field this check cannot classify was judged: %v", err)
	}
}

// newSampleRegistry registers one sample kind whose config is C — the shape the
// test states, since the checks are about shapes.
func newSampleRegistry[C any](t *testing.T, typ string, project func(C) string) *plugin.Registry {
	t.Helper()
	r := plugin.NewRegistry()
	err := plugin.Register(r, sampleKind, plugin.Spec[C, sample]{
		Type: typ,
		New: func(_ context.Context, cfg C, _ plugin.Deps) (sample, error) {
			return sampleImpl(project(cfg)), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestCheckShapeUnknownTypeAndValidConfig(t *testing.T) {
	r := registerSample(t, nil)
	if err := r.CheckShape("sample", "ghost", jsontext.Value(`{}`), "/config"); err == nil {
		t.Fatal("an unregistered type passed the shape check")
	} else if !strings.Contains(err.Error(), "known: tree") {
		t.Fatalf("unknown type error does not name the known ones: %v", err)
	}
	if err := r.CheckShape("sample", "tree", example(), "/config"); err != nil {
		t.Fatalf("the example config failed the shape check: %v", err)
	}
}

// TestCheckShapeAcceptsUntaggedAndOptionalFields: the walk resolves a member
// the way encoding/json does, and honours the engine's optional tag.
func TestCheckShapeAcceptsUntaggedAndOptionalFields(t *testing.T) {
	type untagged struct {
		Plain  string        `json:"plain"`
		Bare   string        // no tag: encoding/json accepts "Bare", so the check must
		Secret plugin.Secret `json:"secret"`
	}
	r := plugin.NewRegistry()
	err := plugin.Register(r, sampleKind, plugin.Spec[untagged, sample]{
		Type: "bare", Example: []byte(`{"plain":"p","Bare":"b","secret":{"file":"/f"}}`),
		New: func(_ context.Context, cfg untagged, _ plugin.Deps) (sample, error) {
			return sampleImpl(cfg.Plain), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	raw := jsontext.Value(`{"plain":"p","Bare":"b","secret":{"file":"/f"},"ghost":1}`)
	err = r.CheckShape("sample", "bare", raw, "/config")
	if err == nil || !strings.Contains(err.Error(), "unknown field /config/ghost") {
		t.Fatalf("untagged member handling: %v", err)
	}
	if strings.Contains(err.Error(), "/Bare") || strings.Contains(err.Error(), "/plain") {
		t.Fatalf("a present member was reported: %v", err)
	}
}

// everyPlugin is the registry of the distribution: what a guard over "every
// registered plugin" has to hold.
func everyPlugin(t *testing.T) *plugin.Registry {
	t.Helper()
	r := plugin.NewRegistry()
	if err := cli.Generic(r); err != nil {
		t.Fatal(err)
	}
	return r
}

// TestCheckShapeAcceptsEveryRegisteredExample: the check must never refuse what
// the real decoder accepts. Register decodes every Spec.Example by
// construction, so a refusal here is this check's bug, never the example's.
func TestCheckShapeAcceptsEveryRegisteredExample(t *testing.T) {
	reg := everyPlugin(t)
	checked := 0
	for _, d := range reg.Descriptors() {
		if d.Example == nil {
			continue
		}
		checked++
		if err := reg.CheckShape(d.Kind, d.Type, jsontext.Value(d.Example), "/example"); err != nil {
			t.Errorf("%s/%s: the shape check refuses the plugin's own example: %v", d.Kind, d.Type, err)
		}
	}
	if checked < 15 {
		t.Fatalf("only %d examples were checked; the registry did not register", checked)
	}
}

// TestCheckShapeReachesIntoARealDiscoveryRule: the walk the guard above relies
// on is not vacuous — a template in a real kubernetes_labels rule's number field
// is refused, at the path the engine prints, while the untouched example rule
// passes the same walk.
func TestCheckShapeReachesIntoARealDiscoveryRule(t *testing.T) {
	reg := everyPlugin(t)
	var example jsontext.Value
	for _, d := range reg.Descriptors() {
		if d.Type == "kubernetes_labels" {
			example = jsontext.Value(d.Example)
		}
	}
	broken := bytes.Replace(example, []byte(`"connect_attempts": 6`), []byte(`"connect_attempts": "{{ port 0 0 }}"`), 1)
	if bytes.Equal(broken, example) {
		t.Fatal("the example no longer carries connect_attempts: 6; update this probe")
	}
	firstRule := func(raw jsontext.Value) discovery.RuleSource {
		t.Helper()
		inst, err := plugin.Build(context.Background(), reg, discovery.Kind, "kubernetes_labels", raw,
			plugin.Deps{Instance: "workloads"})
		if err != nil {
			t.Fatal(err)
		}
		reporter, ok := inst.(discovery.RuleSourceReporter)
		if !ok {
			t.Fatal("kubernetes_labels does not expose its rules")
		}
		rules, err := reporter.RuleSources(context.Background())
		if err != nil || len(rules) == 0 {
			t.Fatalf("rules: %+v, %v", rules, err)
		}
		return rules[0]
	}

	rule := firstRule(broken)
	err := reg.CheckShape("source", rule.Type, rule.Config, "/discoveries/workloads"+rule.ConfigPath)
	if err == nil || !strings.Contains(err.Error(),
		"/discoveries/workloads/pods/0/source/config/connect_attempts must be a JSON number, got a JSON string") {
		t.Fatalf("a template in a real rule's number field slipped through: %v", err)
	}
	intact := firstRule(example)
	if err := reg.CheckShape("source", intact.Type, intact.Config, "/discoveries/workloads"+intact.ConfigPath); err != nil {
		t.Fatalf("the untouched example rule was refused: %v", err)
	}
}

// TestCheckShapeAcceptsTheReferenceConfigRules: the reference configuration's
// discovery rules — a real postgres config and a real files config, templates
// and all — pass the offline rule check the engine runs at construction.
func TestCheckShapeAcceptsTheReferenceConfigRules(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "cmd", "go-cluster-backup", "testdata", "config.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := yamlToJSON(t, data)
	if err != nil {
		t.Fatal(err)
	}
	// The reference configuration states what a deployment states; the rest of
	// it is the module's own document, which is what a run merges under the
	// file before the offline rule check ever sees a rule.
	service, err := yamlToJSON(t, defaults.Defaults)
	if err != nil {
		t.Fatal(err)
	}
	reg := everyPlugin(t)
	defs, err := engine.LoadDefaults(reg, []jsontext.Value{service})
	if err != nil {
		t.Fatal(err)
	}
	e, err := engine.New(context.Background(), raw, defs, reg, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("the reference config's rules failed the offline check: %v", err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
}
