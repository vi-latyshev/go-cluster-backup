package plugin_test

import (
	"strings"
	"testing"
	"text/template"
	"time"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
)

type tmplConfig struct {
	Body plugin.Template `json:"body" doc:"a template"`
}

func decodeTmpl(t *testing.T, raw string) (tmplConfig, error) {
	t.Helper()
	var c tmplConfig
	err := plugin.Decode(jsontext.Value(raw), &c)
	return c, err
}

func TestTemplateParsedAtDecode(t *testing.T) {
	_, err := decodeTmpl(t, `{"body":"hello {{ .Name"}`)
	if err == nil {
		t.Fatal("a broken template decoded without error")
	}
	if !strings.Contains(err.Error(), "/body") {
		t.Fatalf("parse error does not carry the field path: %v", err)
	}
}

func TestTemplateRenderAndMissingKey(t *testing.T) {
	c, err := decodeTmpl(t, `{"body":"hi {{ .ID }}"}`)
	if err != nil {
		t.Fatal(err)
	}
	out, err := c.Body.Render(struct{ ID string }{ID: "abc"})
	if err != nil || out != "hi abc" {
		t.Fatalf("Render = %q, %v", out, err)
	}
	// A map with the key absent is an error, not a silent "<no value>".
	c, err = decodeTmpl(t, `{"body":"{{ .Data.k }}"}`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Body.Render(struct{ Data map[string]string }{Data: map[string]string{}}); err == nil {
		t.Fatal("missingkey=error did not fire")
	}
	// Struct field present: renders even when the value is empty.
	if out, err := c.Body.Render(struct{ Data map[string]string }{Data: map[string]string{"k": "v"}}); err != nil || out != "v" {
		t.Fatalf("Render = %q, %v", out, err)
	}
}

func TestTemplateUnparsedValue(t *testing.T) {
	var t0 plugin.Template
	if out, err := t0.Render(nil); err != nil || out != "" {
		t.Fatalf("zero template renders %q, %v", out, err)
	}
	// A template that was never decoded but carries text cannot render.
	var c tmplConfig
	if err := c.Body.UnmarshalJSON([]byte(`"x"`)); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Body.Render(nil); err == nil {
		t.Fatal("an unparsed template rendered")
	}
}

func TestCoreFunctions(t *testing.T) {
	c, err := decodeTmpl(t, `{"body":"{{ json .Name }}|{{ date .At }}|{{ bytes .N }}|{{ duration .D }}|{{ date .Zero }}"}`)
	if err != nil {
		t.Fatal(err)
	}
	data := struct {
		Name string
		At   time.Time
		Zero time.Time
		N    int64
		D    time.Duration
	}{Name: `a"b`, At: time.Date(2026, 10, 12, 4, 20, 0, 0, time.UTC), N: 5 * (1 << 30), D: 4*time.Minute + 39*time.Second}
	out, err := c.Body.Render(data)
	if err != nil {
		t.Fatal(err)
	}
	want := `"a\"b"|2026-10-12T04:20:00Z|5.00 GiB|4m39s|never`
	if out != want {
		t.Fatalf("Render = %q, want %q", out, want)
	}
}

// TestSIBytes pins the SI renderer a cap's own config speaks in: 14GB is
// 14·10⁹, and the message must say 14.0 GB, never the binary reading of the same
// number. `bytes` keeps rendering artifact sizes in binary units, so the two
// cannot drift into one another.
func TestSIBytes(t *testing.T) {
	c, err := decodeTmpl(t, `{"body":"{{ sibytes . }}"}`)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		n    int64
		want string
	}{
		{0, "0 B"},
		{1, "1 B"},
		{999, "999 B"},
		{1000, "1.0 kB"},
		{999_999, "1000.0 kB"}, // the raw value is under a MB, so kB is the unit
		{1_000_000, "1.0 MB"},
		{1_048_576, "1.0 MB"}, // an artifact's MiB, read in the SI base
		{1_500_000_000, "1.5 GB"},
		{13_800_000_000, "13.8 GB"},
		{14_000_000_000, "14.0 GB"}, // max_total_bytes: 14GB
		{999_999_999_999, "1000.0 GB"},
		{1_000_000_000_000, "1.0 TB"},
	}
	for _, tc := range cases {
		out, err := c.Body.Render(tc.n)
		if err != nil {
			t.Fatalf("sibytes %d: %v", tc.n, err)
		}
		if out != tc.want {
			t.Errorf("sibytes %d = %q, want %q", tc.n, out, tc.want)
		}
	}
	// The binary helper is unchanged: the same cap reads smaller in GiB, which
	// is exactly the confusion sibytes exists to remove.
	b, err := decodeTmpl(t, `{"body":"{{ bytes . }}"}`)
	if err != nil {
		t.Fatal(err)
	}
	if out, err := b.Body.Render(int64(14_000_000_000)); err != nil || out != "13.04 GiB" {
		t.Fatalf("bytes 14GB = %q, %v; want the binary reading unchanged", out, err)
	}
}

type scopedConfig struct {
	Body plugin.Template `json:"body" doc:"a template"`
}

func (scopedConfig) TemplateFuncs() template.FuncMap {
	return template.FuncMap{
		"label": func(s string) string { return "label:" + s },
	}
}

func TestPluginTemplateFuncs(t *testing.T) {
	var c scopedConfig
	err := plugin.Decode(jsontext.Value(`{"body":"{{ label .Name }}"}`), &c)
	if err != nil {
		t.Fatal(err)
	}
	out, err := c.Body.Render(struct{ Name string }{Name: "x"})
	if err != nil || out != "label:x" {
		t.Fatalf("Render = %q, %v", out, err)
	}
}

type collidingConfig struct {
	Body plugin.Template `json:"body" doc:"a template"`
}

func (collidingConfig) TemplateFuncs() template.FuncMap {
	return template.FuncMap{"json": func() string { return "nope" }}
}

func TestPluginTemplateFuncsCollision(t *testing.T) {
	var c collidingConfig
	err := plugin.Decode(jsontext.Value(`{"body":"{{ json . }}"}`), &c)
	if err == nil || !strings.Contains(err.Error(), `"json"`) {
		t.Fatalf("collision not reported: %v", err)
	}
}

// TestPickIsTheMessageFunction: pick names the text of the pair whose key is
// the value, matched as its string form — an enum like "failed" is a model
// value, and the keys are written as plain strings.
func TestPick(t *testing.T) {
	c, err := decodeTmpl(t, `{"body":"{{ pick .Outcome \"delivered\" \"✅\" \"failed\" \"❌\" }}"}`)
	if err != nil {
		t.Fatal(err)
	}
	out, err := c.Body.Render(struct{ Outcome model.Outcome }{Outcome: model.OutcomeFailed})
	if err != nil || out != "❌" {
		t.Fatalf("Render = %q, %v, want the matching text", out, err)
	}
	out, err = c.Body.Render(struct{ Outcome model.Outcome }{Outcome: model.OutcomeDelivered})
	if err != nil || out != "✅" {
		t.Fatalf("Render = %q, %v, want the matching text", out, err)
	}
	// Nothing matching is an error naming the value, never a silent empty line.
	_, err = c.Body.Render(struct{ Outcome model.Outcome }{Outcome: model.OutcomeRunning})
	if err == nil || !strings.Contains(err.Error(), "running") {
		t.Fatalf("pick with no match: %v, want the value named", err)
	}
	// An odd number of pairs is an error too.
	odd, err := decodeTmpl(t, `{"body":"{{ pick .Outcome \"ok\" }}"}`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := odd.Body.Render(struct{ Outcome string }{Outcome: "ok"}); err == nil {
		t.Fatal("pick accepted an odd number of arguments")
	}
}

// TestTemplateUnmarshalJSONRejectsNonString: a template field is a JSON string
// and nothing else; called directly with a number the method reports it rather
// than storing an empty template.
func TestTemplateUnmarshalJSONRejectsNonString(t *testing.T) {
	var t0 plugin.Template
	if err := t0.UnmarshalJSON([]byte(`1`)); err == nil {
		t.Fatal("a number decoded into a template field")
	}
}

// TestDurationAcceptsEveryValueCarryingADuration: the helper renders a
// time.Duration, a model.Duration (through its Std), any other Stringer, and a
// plain value, so a template may hold whichever of them a consumer supplies.
func TestDurationAcceptsEveryValueCarryingADuration(t *testing.T) {
	c, err := decodeTmpl(t, `{"body":"{{ duration . }}"}`)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		in   any
		want string
	}{
		{4*time.Minute + 39*time.Second, "4m39s"},
		{model.Duration(90 * time.Second), "1m30s"},
		{time.Date(2026, 10, 12, 4, 20, 0, 0, time.UTC), "2026-10-12 04:20:00 +0000 UTC"},
		{7, "7"},
	} {
		out, err := c.Body.Render(tc.in)
		if err != nil {
			t.Fatalf("duration %v: %v", tc.in, err)
		}
		if out != tc.want {
			t.Errorf("duration %v = %q, want %q", tc.in, out, tc.want)
		}
	}
}

func TestTemplateMarshal(t *testing.T) {
	c, err := decodeTmpl(t, `{"body":"runs/{{ .ID }}/x"}`)
	if err != nil {
		t.Fatal(err)
	}
	b, err := c.Body.MarshalJSON()
	if err != nil || string(b) != `"runs/{{ .ID }}/x"` {
		t.Fatalf("MarshalJSON = %s, %v", b, err)
	}
	if c.Body.Text() != "runs/{{ .ID }}/x" {
		t.Fatalf("Text = %q", c.Body.Text())
	}
}
