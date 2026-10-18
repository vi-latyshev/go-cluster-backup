package plugin_test

import (
	"context"
	"strings"
	"testing"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
)

type nullConfig struct {
	Name    string         `json:"name"`
	Body    jsontext.Value `json:"body"`
	Numbers []int          `json:"numbers"`
	Holder  *nullInner     `json:"holder" optional:"true"`
}

type nullInner struct {
	Value string `json:"value"`
}

// TestDecodeRefusesNull: a member that is present carries a value, and null is
// not one — an optional section is taken out by removing it. Every layer the
// defaults mechanism works in refuses a null, this one included, and it names
// the pointer.
func TestDecodeRefusesNull(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want string
	}{
		{"a field", `{"name": null, "body": {}, "numbers": []}`, "/name is null"},
		{"an optional section", `{"name": "x", "body": {}, "numbers": [], "holder": null}`, "/holder is null"},
		{"an element of a list", `{"name": "x", "body": {}, "numbers": [1, null]}`, "/numbers/1 is null"},
		{"a nested field", `{"name": "x", "body": {}, "numbers": [], "holder": {"value": null}}`, "/holder/value is null"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var cfg nullConfig
			err := plugin.Decode(jsontext.Value(c.raw), &cfg)
			if err == nil {
				t.Fatalf("the decoder accepted %s", c.raw)
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Fatalf("decode of %s reported %v, want it to name %q", c.raw, err, c.want)
			}
		})
	}

	// A null inside an opaque value is the plugin's own to read: the framework
	// walks the type, and an opaque field is a leaf.
	var cfg nullConfig
	if err := plugin.Decode(jsontext.Value(`{"name": "x", "body": {"keep": null}, "numbers": []}`), &cfg); err != nil {
		t.Fatalf("a null inside an opaque config was refused here: %v", err)
	}
}

// requiredConfig is the classification a descriptor carries: what the deployment
// states, what a defaults document fills, and a secret reference, which is
// always the deployment's.
type requiredConfig struct {
	Stated string        `json:"stated" required:"true" doc:"the deployment states this one"`
	Any    int           `json:"any" doc:"a defaults document fills this one"`
	Secret plugin.Secret `json:"secret" doc:"a mounted file"`
}

func TestDescriptorsCarryRequired(t *testing.T) {
	kind := plugin.NewKind[error]("test")
	reg := plugin.NewRegistry()
	if err := plugin.Register(reg, kind, plugin.Spec[requiredConfig, error]{
		Type: "classified", Example: []byte(`{"stated": "x", "any": 1, "secret": {"file": "/run/secret"}}`),
		New: func(context.Context, requiredConfig, plugin.Deps) (error, error) { return nil, nil },
	}); err != nil {
		t.Fatal(err)
	}
	for _, d := range reg.Descriptors() {
		if d.Type != "classified" {
			continue
		}
		want := map[string]bool{"stated": true, "any": false, "secret": true}
		for _, f := range d.Fields {
			required, ok := want[f.Name]
			if !ok {
				t.Fatalf("unexpected field %q", f.Name)
			}
			if f.Required != required {
				t.Errorf("field %s: Required = %v, want %v", f.Name, f.Required, required)
			}
			if f.Secret != (f.Name == "secret") {
				t.Errorf("field %s: Secret = %v", f.Name, f.Secret)
			}
		}
		return
	}
	t.Fatal("the plugin was not registered")
}

// TestAbsolutePathRequiresAnAbsoluteNonEmptyPath: a plugin runs in a container
// with no useful working directory, so a relative or empty path is refused with
// the field named, and an absolute one passes.
func TestAbsolutePathRequiresAnAbsoluteNonEmptyPath(t *testing.T) {
	if errs := plugin.AbsolutePath("", "scratch_dir"); len(errs) != 1 || !strings.Contains(errs[0].Error(), "scratch_dir is empty") {
		t.Fatalf("empty path = %v", errs)
	}
	if errs := plugin.AbsolutePath("var/scratch", "scratch_dir"); len(errs) != 1 ||
		!strings.Contains(errs[0].Error(), `"var/scratch" must be absolute`) {
		t.Fatalf("relative path = %v", errs)
	}
	if errs := plugin.AbsolutePath("/var/scratch", "scratch_dir"); len(errs) != 0 {
		t.Fatalf("absolute path = %v", errs)
	}
}

// TestDecodeOptions: an instance's option block is never defaulted — an absent
// value is an error naming the block, a bad member is refused with it, and a
// complete block decodes.
func TestDecodeOptions(t *testing.T) {
	type opts struct {
		Mode string `json:"mode" doc:"a mode"`
	}
	if _, err := plugin.DecodeOptions[opts](nil, "restore.apply.sqlite"); err == nil ||
		!strings.Contains(err.Error(), "restore.apply.sqlite is not configured") {
		t.Fatalf("absent options = %v", err)
	}
	if _, err := plugin.DecodeOptions[opts](jsontext.Value(`{"mdoe":"x"}`), "restore.apply.sqlite"); err == nil ||
		!strings.Contains(err.Error(), "restore.apply.sqlite") {
		t.Fatalf("bad member = %v", err)
	}
	got, err := plugin.DecodeOptions[opts](jsontext.Value(`{"mode":"overwrite"}`), "restore.apply.sqlite")
	if err != nil || got.Mode != "overwrite" {
		t.Fatalf("DecodeOptions = %+v, %v", got, err)
	}
}

type walkThing struct {
	Value string `json:"value"`
}

type walkConfig struct {
	Map  map[string]walkThing `json:"map"`
	List []walkThing          `json:"list"`
}

// TestDecodeWalksWrongShapedMembers: a member of the wrong shape is the strict
// decoder's to report. The walk over missing fields and nulls reaches it without
// panicking and without inventing a gap of its own.
func TestDecodeWalksWrongShapedMembers(t *testing.T) {
	for _, raw := range []string{
		`{"map": [], "list": {}}`,
		`{"map": "x", "list": 5}`,
	} {
		var cfg walkConfig
		err := plugin.Decode(jsontext.Value(raw), &cfg)
		if err == nil {
			t.Fatalf("%s decoded without error", raw)
		}
		if strings.Contains(err.Error(), "missing field") {
			t.Errorf("%s reported a missing field for a wrong-shaped member:\n%v", raw, err)
		}
	}
}

// TestCheckNullsReportsANullAtTheRoot: there is no value to check, so the root
// null is reported like any other.
func TestCheckNullsReportsANullAtTheRoot(t *testing.T) {
	errs := plugin.CheckNulls(jsontext.Value(" null "), "/config")
	if len(errs) != 1 || !strings.Contains(errs[0].Error(), "/config is null") {
		t.Fatalf("CheckNulls(root null) = %v", errs)
	}
}

// TestIsNullReadsWhitespace: the check for "no value" is the raw JSON, so a
// pretty-printed document behaves like a compact one.
func TestIsNullReadsWhitespace(t *testing.T) {
	for _, raw := range []string{"null", " null ", "\nnull"} {
		if !plugin.IsNull(jsontext.Value(raw)) {
			t.Errorf("%q is not read as a null", raw)
		}
	}
	for _, raw := range []string{`"null"`, "{}", "0", "false", ""} {
		if plugin.IsNull(jsontext.Value(raw)) {
			t.Errorf("%q is read as a null", raw)
		}
	}
}
