package main

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/tools/internal/pluginmeta"
)

// TestM9_CompositionRootMetadata proves the generator's plugin view over the
// shared corpus: ids, Apply shapes, declared Floor/Requires/After and the
// production import edges — all read through pluginmeta.
func TestM9_CompositionRootMetadata(t *testing.T) {
	plugins, err := load(copyTree(t, fixtureModule))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	ids := make([]string, len(plugins))
	for i, p := range plugins {
		ids[i] = p.id
	}
	if want := []string{"cli", "configuration", "errors", "instances", "model", "notifier/webhook", "reporting", "source/files", "source/postgres"}; !reflect.DeepEqual(ids, want) {
		t.Fatalf("ids = %v, want %v", ids, want)
	}
	byID := make(map[string]*plugin, len(plugins))
	for _, p := range plugins {
		byID[p.id] = p
	}
	if !byID["notifier/webhook"].floor {
		t.Error("webhook must declare Floor")
	}
	if got := byID["notifier/webhook"].apply; got != applyFactory {
		t.Errorf("webhook apply = %v, want factory", got)
	}
	if got := byID["model"].apply; got != applyNone {
		t.Errorf("model apply = %v, want none", got)
	}
	if got := byID["reporting"].apply; got != applyPlugin {
		t.Errorf("reporting apply = %v, want plugin", got)
	}
	if want := []string{"instances", "source/postgres"}; !reflect.DeepEqual(byID["source/files"].imports, want) {
		t.Errorf("files imports = %v, want %v (build-tagged file included)", byID["source/files"].imports, want)
	}
	if want := []string{"instances", "model"}; !reflect.DeepEqual(byID["source/postgres"].imports, want) {
		t.Errorf("postgres imports = %v, want %v (duplicates removed)", byID["source/postgres"].imports, want)
	}
	if want := []string{"configuration"}; !reflect.DeepEqual(byID["source/postgres"].requires, want) {
		t.Errorf("postgres requires = %v, want %v", byID["source/postgres"].requires, want)
	}
	if want := []string{"discovery/kubernetes_labels"}; !reflect.DeepEqual(byID["source/postgres"].after, want) {
		t.Errorf("postgres after = %v, want %v", byID["source/postgres"].after, want)
	}
	if _, kit := byID["kit"]; kit {
		t.Error("a conformance kit must not be a plugin")
	}
}

// TestLoadRejectsProtocolProblems proves a metadata violation refuses the whole
// generation, rendered file, package and rule.
func TestLoadRejectsProtocolProblems(t *testing.T) {
	const header = `package p

import (
	"example.test/app/internal/loader"
	"example.test/app/internal/plugins/instances"
)

`
	tests := []struct {
		name  string
		files map[string]string
		want  string
	}{
		{
			name:  "Spec problem",
			files: map[string]string{"internal/plugins/p/plugin.go": header + "var Spec = loader.Spec{ID: \"p\", Summary: \"p\", Bogus: 1}\n"},
			want:  "internal/plugins/p/plugin.go: example.test/app/internal/plugins/p: Spec.Bogus: unknown Spec field Bogus",
		},
		{
			name:  "Apply problem",
			files: map[string]string{"internal/plugins/p/plugin.go": header + "var Spec = loader.Spec{ID: \"p\", Summary: \"p\"}\n\nfunc Apply() error { return nil }\n"},
			want:  "Apply must take a *loader.Registry",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := load(moduleTree(t, test.files))
			wantSubstring(t, err, test.want)
		})
	}

	t.Run("plain parse error", func(t *testing.T) {
		_, err := load(moduleTree(t, map[string]string{"internal/plugins/p/plugin.go": "package p\nfunc {\n"}))
		wantSubstring(t, err, "parse internal/plugins/p/plugin.go")
	})
	t.Run("missing summary", func(t *testing.T) {
		_, err := load(moduleTree(t, map[string]string{"internal/plugins/p/plugin.go": header + "var Spec = loader.Spec{ID: \"p\"}\n"}))
		wantSubstring(t, err, "Spec.Summary is required")
	})
	t.Run("Spec is not a composite literal", func(t *testing.T) {
		_, err := load(moduleTree(t, map[string]string{"internal/plugins/p/plugin.go": "package p\n\nimport \"example.test/app/internal/loader\"\n\nvar Spec = loader.Spec\n"}))
		wantSubstring(t, err, "Spec must be a loader.Spec composite literal")
	})
}

// TestProblemErrorWithoutFile proves the package-level rendering.
func TestProblemErrorWithoutFile(t *testing.T) {
	err := problemError(&pluginmeta.Package{
		Path:     "example.test/app/internal/plugins/p",
		Problems: []pluginmeta.Problem{{Msg: "package problem"}},
	})
	wantSubstring(t, err, "example.test/app/internal/plugins/p: package problem")
}

// TestLoadRejectsADuplicateID proves one id belongs to one plugin.
func TestLoadRejectsADuplicateID(t *testing.T) {
	files := map[string]string{
		"internal/plugins/p/plugin.go": "package p\n\nimport \"example.test/app/internal/loader\"\n\nvar Spec = loader.Spec{ID: \"same\", Summary: \"p\"}\n",
		"internal/plugins/q/plugin.go": "package q\n\nimport \"example.test/app/internal/loader\"\n\nvar Spec = loader.Spec{ID: \"same\", Summary: \"q\"}\n",
	}
	_, err := load(moduleTree(t, files))
	wantSubstring(t, err, `both use id "same"`)
}

// TestIsPluginDir proves the generator's plugin policy: everything inside
// internal/plugins, but never the composition root or a conformance kit.
func TestIsPluginDir(t *testing.T) {
	tests := []struct {
		name string
		pkg  *pluginmeta.Package
		want bool
	}{
		{"no Spec", &pluginmeta.Package{Dir: "internal/plugins/p"}, true},
		{"outside the plugin tree", &pluginmeta.Package{Dir: "internal/loader", Spec: &pluginmeta.Spec{ID: "loader"}}, false},
		{"the tree root", &pluginmeta.Package{Dir: "internal/plugins", Spec: &pluginmeta.Spec{ID: "root"}}, false},
		{"the composition root", &pluginmeta.Package{Dir: "internal/plugins/all", Spec: &pluginmeta.Spec{ID: "all"}}, false},
		{"a conformance kit", &pluginmeta.Package{Dir: "internal/plugins/source/sourcetest", Spec: &pluginmeta.Spec{ID: "kit"}}, false},
		{"a nested plugin", &pluginmeta.Package{Dir: "internal/plugins/source/files", Spec: &pluginmeta.Spec{ID: "source/files"}}, true},
		{"a top-level plugin", &pluginmeta.Package{Dir: "internal/plugins/reporting", Spec: &pluginmeta.Spec{ID: "reporting"}}, true},
	}
	for _, test := range tests {
		if got := isPluginDir(test.pkg.Dir); got != test.want {
			t.Errorf("%s: isPluginDir = %v, want %v", test.name, got, test.want)
		}
	}
}

// TestM1_PluginImportsAreDeduped proves the edge builder keeps one edge per
// target when several files import it (slices.Sort + slices.Compact).
func TestM1_PluginImportsAreDeduped(t *testing.T) {
	files := map[string]string{
		"internal/plugins/a/plugin.go": "package a\n\nimport (\n\t\"example.test/app/internal/loader\"\n\t\"example.test/app/internal/plugins/b\"\n)\n\nvar Spec = loader.Spec{ID: \"a\", Summary: \"a\"}\n\nvar _ = b.Spec\n",
		"internal/plugins/a/extra.go":  "package a\n\nimport \"example.test/app/internal/plugins/b\"\n\nvar _ = b.Spec\n",
		"internal/plugins/b/plugin.go": "package b\n\nimport \"example.test/app/internal/loader\"\n\nvar Spec = loader.Spec{ID: \"b\", Summary: \"b\"}\n",
	}
	plugins, err := load(moduleTree(t, files))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(plugins) != 2 || plugins[0].id != "a" || plugins[1].id != "b" {
		t.Fatalf("plugins = %+v, want a then b", plugins)
	}
	if want := []string{"b"}; !reflect.DeepEqual(plugins[0].imports, want) {
		t.Errorf("a imports = %v, want %v", plugins[0].imports, want)
	}
}

// TestSpecLiteralIDs covers the literal spelling of a non-factory plugin id.
func TestSpecLiteralIDs(t *testing.T) {
	root := moduleTree(t, map[string]string{
		"internal/plugins/p/plugin.go": `package p

import (
	"example.test/app/internal/loader"
	"example.test/app/internal/plugins/instances"
)

var Spec = loader.Spec{ID: "reporting", Summary: "reports"}

func Apply(lr *loader.Registry) error { return nil }

var _ = instances.ID
`,
	})
	outputs, err := render(root)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if want := "{p.Spec, p.Apply},"; !bytes.Contains(outputs[1].data, []byte(want)) {
		t.Errorf("hostless half misses %q:\n%s", want, outputs[1].data)
	}
}

// TestLiteralKindArgument proves instances.ID accepts a literal kind.
func TestLiteralKindArgument(t *testing.T) {
	root := moduleTree(t, map[string]string{
		"internal/plugins/p/plugin.go": `package p

import (
	"example.test/app/internal/loader"
	"example.test/app/internal/plugins/instances"
)

var Spec = loader.Spec{ID: instances.ID("source", "files"), Summary: "p"}

func Apply(lr *loader.Registry, factories *instances.Registry) error { return nil }
`,
	})
	outputs, err := render(root)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if want := `"source/files"`; !bytes.Contains(outputs[2].data, []byte(want)) {
		t.Errorf("floor manifest misses %q:\n%s", want, outputs[2].data)
	}
}

// TestC6_ConfigurationContributesItsApply proves a library plugin may also
// contribute behaviour: the generated root calls its Apply, so the
// distribution's bundle table exists in every build and `local-only` resolves.
func TestC6_ConfigurationContributesItsApply(t *testing.T) {
	outputs, err := render(copyTree(t, fixtureModule))
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if want := "{configuration.Spec, configuration.Apply},"; !bytes.Contains(outputs[1].data, []byte(want)) {
		t.Errorf("hostless half misses %q:\n%s", want, outputs[1].data)
	}
}

// TestR5_GeneratedRootComposesTheHost proves the two entry points the CLI-host
// contract requires: Register creates the factory registry itself, Compose takes
// one the caller owns so a harness can add its doubles to the same set.
func TestR5_GeneratedRootComposesTheHost(t *testing.T) {
	outputs, err := render(copyTree(t, fixtureModule))
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	code := string(outputs[0].data)
	for _, want := range []string{
		"func Register(r *loader.Registry) error {",
		"return Compose(r, instances.NewRegistry())",
		"func Compose(r *loader.Registry, factories *instances.Registry) error {",
	} {
		if !strings.Contains(code, want) {
			t.Errorf("generated root misses %q:\n%s", want, code)
		}
	}
}

// TestR5b_HostlessHalfExcludesTheHost proves the split the white-box CLI tests
// need: `components` registers every non-host plugin and imports no host, while
// the composition root installs the hostless half and then each host's Apply.
func TestR5b_HostlessHalfExcludesTheHost(t *testing.T) {
	outputs, err := render(copyTree(t, fixtureModule))
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	root, components := string(outputs[0].data), string(outputs[1].data)
	if want := "func Compose(r *loader.Registry, factories *instances.Registry) error {"; !strings.Contains(components, want) {
		t.Errorf("hostless half misses %q:\n%s", want, components)
	}
	for _, forbidden := range []string{"internal/plugins/cli", "cli.Spec", "cli.Apply"} {
		if strings.Contains(components, forbidden) {
			t.Errorf("hostless half names the host: %q\n%s", forbidden, components)
		}
	}
	for _, want := range []string{
		"components.Compose(r, factories)",
		"{cli.Spec, func(lr *loader.Registry) error { return cli.Apply(lr, factories) }},",
	} {
		if !strings.Contains(root, want) {
			t.Errorf("composition root misses %q:\n%s", want, root)
		}
	}
}

// TestR5b_HostNamesAreReserved proves the composition root cannot import a host
// under a name it already writes itself.
func TestR5b_HostNamesAreReserved(t *testing.T) {
	root := moduleTree(t, map[string]string{
		"internal/plugins/cli/plugin.go": "package components\n\nimport \"example.test/app/internal/loader\"\n\nvar Spec = loader.Spec{ID: \"cli\", Summary: \"host\"}\n",
	})
	_, err := render(root)
	wantSubstring(t, err, `uses the reserved package name "components"`)
}

// TestCheckNamesAndWriteImports proves the two generated-file helpers directly:
// a reserved name, two plugins sharing a name and duplicate import paths.
func TestCheckNamesAndWriteImports(t *testing.T) {
	reserved := map[string]bool{"components": true}
	if err := checkNames([]*plugin{{path: "a", name: "components"}}, reserved); err == nil {
		t.Error("a reserved name was accepted")
	}
	if err := checkNames([]*plugin{{path: "a", name: "same"}, {path: "b", name: "same"}}, nil); err == nil {
		t.Error("a duplicate package name was accepted")
	}
	if err := checkNames([]*plugin{{path: "a", name: "a"}, {path: "b", name: "b"}}, nil); err != nil {
		t.Errorf("distinct names refused: %v", err)
	}
	var b bytes.Buffer
	writeImports(&b, []string{"b", "a", "b"})
	if got, want := b.String(), "import (\n\t\"a\"\n\t\"b\"\n)\n\n"; got != want {
		t.Errorf("writeImports = %q, want %q", got, want)
	}
}
