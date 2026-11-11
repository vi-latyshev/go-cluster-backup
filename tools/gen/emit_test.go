package main

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/tools/internal/pluginmeta/malformed"
)

// TestRenderMatchesTheGoldenCompositionRoot proves the whole pipeline: the
// fixture module's plugin set, its production import graph and every tracked
// output — the composition root, its hostless half and the Floor manifest.
func TestRenderMatchesTheGoldenCompositionRoot(t *testing.T) {
	root := copyTree(t, fixtureModule)
	outputs, err := render(root)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if len(outputs) != 3 {
		t.Fatalf("want 3 outputs, got %d", len(outputs))
	}
	cases := []struct {
		path string
		want string
	}{
		{filepath.Join(root, "internal/plugins/all/zz_generated.go"), "testdata/want/zz_generated.go"},
		{filepath.Join(root, "internal/plugins/all/components/zz_generated.go"), "testdata/want/components/zz_generated.go"},
		{filepath.Join(root, "internal/plugins/all/floor.json"), "testdata/want/floor.json"},
	}
	for i, c := range cases {
		if outputs[i].path != c.path {
			t.Errorf("output %d is %s, want %s", i, outputs[i].path, c.path)
		}
		if got, want := string(outputs[i].data), mustRead(t, c.want); got != want {
			t.Errorf("%s is not the golden file %s:\n%s", c.path, c.want, got)
		}
	}
}

// TestGenerateWritesThenChecks proves the write path, the freshness check and
// the three ways a tracked file can disagree with its sources.
func TestGenerateWritesThenChecks(t *testing.T) {
	root := copyTree(t, fixtureModule)
	if err := generate(root, false); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := generate(root, true); err != nil {
		t.Fatalf("check after write: %v", err)
	}

	generated := filepath.Join(root, "internal/plugins/all/zz_generated.go")
	if err := os.WriteFile(generated, []byte("stale"), 0o644); err != nil {
		t.Fatalf("make stale: %v", err)
	}
	wantSubstring(t, generate(root, true), "is stale")

	if err := os.Remove(generated); err != nil {
		t.Fatalf("remove: %v", err)
	}
	wantSubstring(t, generate(root, true), "is missing")

	if err := os.Mkdir(generated, 0o755); err != nil {
		t.Fatalf("mkdir generated path: %v", err)
	}
	wantSubstring(t, generate(root, true), "read "+generated)
}

// TestGenerateReportsWriteFailures covers both filesystem failures: the output
// directory cannot be created, and the output file cannot be written.
func TestGenerateReportsWriteFailures(t *testing.T) {
	root := copyTree(t, fixtureModule)
	allDir := filepath.Join(root, "internal/plugins/all")
	if err := os.RemoveAll(allDir); err != nil {
		t.Fatalf("remove all: %v", err)
	}
	if err := os.WriteFile(allDir, []byte("not a directory"), 0o644); err != nil {
		t.Fatalf("block all dir: %v", err)
	}
	wantSubstring(t, generate(root, false), "create "+allDir)

	root = copyTree(t, fixtureModule)
	generated := filepath.Join(root, "internal/plugins/all/zz_generated.go")
	if err := os.Remove(generated); err != nil {
		t.Fatalf("remove generated: %v", err)
	}
	if err := os.Mkdir(generated, 0o755); err != nil {
		t.Fatalf("mkdir generated: %v", err)
	}
	wantSubstring(t, generate(root, false), "write "+generated)
}

// TestRenderEmptyDistribution proves a module whose packages declare no Spec is
// a valid, if empty, distribution.
func TestRenderEmptyDistribution(t *testing.T) {
	root := moduleTree(t, map[string]string{
		"internal/plugins/instances/instances.go": instancesSource,
	})
	outputs, err := render(root)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if want := "loader.SetImports(r, loader.Imports{})"; !bytes.Contains(outputs[1].data, []byte(want)) {
		t.Errorf("empty distribution misses %q in the hostless half:\n%s", want, outputs[1].data)
	}
	if want := "\"plugins\": []"; !bytes.Contains(outputs[2].data, []byte(want)) {
		t.Errorf("empty manifest misses %q:\n%s", want, outputs[2].data)
	}
}

// TestRenderFloorManifestError proves the unprovokable encoder failure is
// reported, not swallowed.
func TestRenderFloorManifestError(t *testing.T) {
	original := marshalIndent
	marshalIndent = func(any, string, string) ([]byte, error) {
		return nil, errors.New("boom")
	}
	t.Cleanup(func() { marshalIndent = original })
	_, err := renderFloor(nil, nil)
	wantSubstring(t, err, "encode floor manifest: boom")
	_, err = render(copyTree(t, fixtureModule))
	wantSubstring(t, err, "encode floor manifest: boom")
}

// TestRenderImportsJoinsEdges proves one row lists every edge with separators.
func TestRenderImportsJoinsEdges(t *testing.T) {
	var b bytes.Buffer
	renderImports(&b, []*plugin{{id: "a", imports: []string{"b", "c"}}})
	if want := `"a": {"b", "c"},`; !bytes.Contains(b.Bytes(), []byte(want)) {
		t.Errorf("import graph misses %q:\n%s", want, b.String())
	}
}

// TestMainExitsWithTheRunCode proves main passes the run result to the exit
// seam.
func TestMainExitsWithTheRunCode(t *testing.T) {
	originalExit, originalArgs := osExit, os.Args
	code := -1
	osExit = func(got int) { code = got }
	os.Args = []string{"gen", "-unknown"}
	t.Cleanup(func() { osExit, os.Args = originalExit, originalArgs })
	main()
	if code != 2 {
		t.Errorf("exit code = %d, want 2", code)
	}
}

// TestRenderErrors proves every protocol violation a plugin package can state
// is refused with the package and the offending field named.
func TestRenderErrors(t *testing.T) {
	tests := []struct {
		name  string
		files map[string]string
		want  string
	}{
		{
			name:  "go.mod is missing",
			files: map[string]string{"internal/plugins/p/plugin.go": validPlugin},
			want:  "read go.mod",
		},
		{
			name:  "go.mod has no module directive",
			files: map[string]string{"go.mod": "go 1.27.4\n", "internal/plugins/p/plugin.go": validPlugin},
			want:  "no module directive",
		},
		{
			name:  "go.mod has an empty module path",
			files: map[string]string{"go.mod": "module \"\"\n", "internal/plugins/p/plugin.go": validPlugin},
			want:  "no module directive",
		},
		{
			name:  "plugin tree is missing",
			files: map[string]string{"go.mod": moduleGoMod},
			want:  "read ",
		},
		{
			name:  "unparsable file",
			files: map[string]string{"internal/plugins/p/plugin.go": "package p\nfunc {\n"},
			want:  "parse ",
		},
		{
			name: "two packages in one directory",
			files: map[string]string{
				"internal/plugins/p/a.go": "package p\n",
				"internal/plugins/p/b.go": "package q\n",
			},
			want: "more than one package",
		},
		{
			name:  "Summary is missing",
			files: map[string]string{"internal/plugins/p/plugin.go": "package p\n\nimport \"example.test/app/internal/loader\"\n\nvar Spec = loader.Spec{ID: \"p\"}\n"},
			want:  "Spec.Summary is required",
		},
		{
			name:  "dependency on an unregistered plugin",
			files: map[string]string{"internal/plugins/p/plugin.go": "package p\n\nimport \"example.test/app/internal/loader\"\n\nvar Spec = loader.Spec{ID: \"p\", Summary: \"p\", Requires: []string{\"ghost\"}}\n"},
			want:  `plugin "p" depends on unregistered plugin "ghost"`,
		},
		{
			name: "two packages use one id",
			files: map[string]string{
				"internal/plugins/p/plugin.go": "package p\n\nimport \"example.test/app/internal/loader\"\n\nvar Spec = loader.Spec{ID: \"same\", Summary: \"p\"}\n",
				"internal/plugins/q/plugin.go": "package q\n\nimport \"example.test/app/internal/loader\"\n\nvar Spec = loader.Spec{ID: \"same\", Summary: \"q\"}\n",
			},
			want: `both use id "same"`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := moduleTree(t, test.files)
			if test.name == "go.mod is missing" {
				root = writeTree(t, test.files)
			}
			_, err := render(root)
			wantSubstring(t, err, test.want)
		})
	}

	// The malformed-metadata matrix is shared with the reader: the generator
	// renders the same cases through its error path, so the reader's rule and
	// the generator's diagnostic cannot drift apart.
	for _, test := range malformed.Cases() {
		t.Run("metadata/"+test.Name, func(t *testing.T) {
			_, err := render(moduleTree(t, test.Files))
			wantSubstring(t, err, test.Want)
		})
	}
}

// TestRenderRejectsReservedAndDuplicatePackageNames proves the composition root
// cannot be generated into a file the compiler would refuse.
func TestRenderRejectsReservedAndDuplicatePackageNames(t *testing.T) {
	tests := []struct {
		name  string
		files map[string]string
		want  string
	}{
		{
			name: "reserved package name",
			files: map[string]string{
				"internal/plugins/x/loader/plugin.go": "package loader\n\nimport \"example.test/app/internal/loader\"\n\nvar Spec = loader.Spec{ID: \"x\", Summary: \"x\"}\n",
			},
			want: `reserved package name "loader"`,
		},
		{
			name:  "another package named instances",
			files: map[string]string{"internal/plugins/x/instances/plugin.go": "package instances\n\nimport \"example.test/app/internal/loader\"\n\nvar Spec = loader.Spec{ID: \"x\", Summary: \"x\"}\n"},
			want:  `uses the reserved package name "instances"`,
		},
		{
			name: "duplicate package name",
			files: map[string]string{
				"internal/plugins/a/same/plugin.go": "package same\n\nimport \"example.test/app/internal/loader\"\n\nvar Spec = loader.Spec{ID: \"a\", Summary: \"a\"}\n",
				"internal/plugins/b/same/plugin.go": "package same\n\nimport \"example.test/app/internal/loader\"\n\nvar Spec = loader.Spec{ID: \"b\", Summary: \"b\"}\n",
			},
			want: `share the package name "same"`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := render(moduleTree(t, test.files))
			wantSubstring(t, err, test.want)
		})
	}
}

// TestRunExitCodes proves the command's whole surface: flag errors, bad
// arguments, generation failures, writes and the freshness check.
func TestRunExitCodes(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"-unknown"}, &stdout, &stderr); code != 2 {
		t.Errorf("unknown flag: want 2, got %d", code)
	}
	if code := run([]string{"-h"}, &stdout, &stderr); code != 2 {
		t.Errorf("help: want 2, got %d", code)
	}
	stderr.Reset()
	if code := run([]string{"unexpected"}, &stdout, &stderr); code != 2 {
		t.Errorf("unexpected argument: want 2, got %d", code)
	}
	if got := stderr.String(); !bytes.Contains([]byte(got), []byte("unexpected arguments")) {
		t.Errorf("stderr misses the argument refusal: %q", got)
	}

	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"-root", filepath.Join(t.TempDir(), "absent")}, &stdout, &stderr); code != 1 {
		t.Errorf("generation failure: want 1, got %d", code)
	}
	if got := stderr.String(); !bytes.Contains([]byte(got), []byte("gen: ")) {
		t.Errorf("stderr misses the failure: %q", got)
	}

	root := copyTree(t, fixtureModule)
	stdout.Reset()
	if code := run([]string{"-root", root}, &stdout, &stderr); code != 0 {
		t.Fatalf("write: want 0, got %d: %s", code, stderr.String())
	}
	if got := stdout.String(); !bytes.Contains([]byte(got), []byte("written")) {
		t.Errorf("stdout misses the write report: %q", got)
	}

	stdout.Reset()
	if code := run([]string{"-root", root, "-check"}, &stdout, &stderr); code != 0 {
		t.Fatalf("check: want 0, got %d: %s", code, stderr.String())
	}
	if got := stdout.String(); !bytes.Contains([]byte(got), []byte("current")) {
		t.Errorf("stdout misses the check report: %q", got)
	}
}

// TestFloorClosurePromotesDependencies proves the effective floor set: a
// declared-Floor plugin makes every effective dependency mandatory, and a
// dependency reachable twice is listed once.
func TestFloorClosurePromotesDependencies(t *testing.T) {
	plugins := []*plugin{
		{id: "core", floor: true, imports: []string{"a"}},
		{id: "a", imports: []string{"b", "c"}},
		{id: "b", imports: []string{"leaf"}},
		{id: "c", requires: []string{"leaf"}},
		{id: "leaf"},
		{id: "optional"},
	}
	set, err := floorClosure(plugins)
	if err != nil {
		t.Fatalf("closure: %v", err)
	}
	for _, id := range []string{"core", "a", "b", "c", "leaf"} {
		if !set[id] {
			t.Errorf("%s is not in the floor closure", id)
		}
	}
	if set["optional"] {
		t.Error("optional is in the floor closure")
	}
}

// TestFloorClosureErrors proves the three broken dependency graphs the closure
// refuses: a self edge, an unregistered plugin and a cycle.
func TestFloorClosureErrors(t *testing.T) {
	tests := []struct {
		name    string
		plugins []*plugin
		want    string
	}{
		{
			name:    "self dependency",
			plugins: []*plugin{{id: "a", requires: []string{"a"}}},
			want:    `plugin "a" depends on itself`,
		},
		{
			name:    "unregistered dependency",
			plugins: []*plugin{{id: "a", requires: []string{"ghost"}}},
			want:    `plugin "a" depends on unregistered plugin "ghost"`,
		},
		{
			name: "cycle",
			plugins: []*plugin{
				{id: "a", requires: []string{"b"}},
				{id: "b", requires: []string{"a"}},
			},
			want: "dependency cycle: a -> b -> a",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := floorClosure(test.plugins)
			wantSubstring(t, err, test.want)
		})
	}
}

// TestRenderRejectsAPluginInsideAnInternalBoundary proves the composition root
// never emits an import the Go internal rule forbids: a plugin below a nested
// `internal` directory is reachable only from that directory's own tree.
func TestRenderRejectsAPluginInsideAnInternalBoundary(t *testing.T) {
	root := moduleTree(t, map[string]string{
		"internal/plugins/storage/internal/plugin.go": "package internal\n\nimport \"example.test/app/internal/loader\"\n\nvar Spec = loader.Spec{ID: \"storage-internal\", Summary: \"hidden\"}\n",
	})
	_, err := render(root)
	wantSubstring(t, err, "plugin example.test/app/internal/plugins/storage/internal is inside a Go internal boundary")
}

// TestImportableFrom proves the Go internal rule the generator enforces.
func TestImportableFrom(t *testing.T) {
	const importer = "example.test/app/internal/plugins/all"
	tests := []struct {
		target string
		want   bool
	}{
		{"example.test/app/internal/loader", true},
		{"example.test/app/internal/plugins/storage/local", true},
		{"example.test/app/internal/plugins/storage/internal/raw", false},
		{"example.test/app/internal/plugins/internal", true},
		{"other.test/app/internal/plugins/x", false},
	}
	for _, test := range tests {
		if got := importableFrom(importer, test.target); got != test.want {
			t.Errorf("importableFrom(%s) = %v, want %v", test.target, got, test.want)
		}
	}
}

// TestCompositionRootReusesTheInstancesImport proves the registry package is a
// plugin like any other: its one import serves both the Register signature and
// its own registration entry.
func TestCompositionRootReusesTheInstancesImport(t *testing.T) {
	root := copyTree(t, fixtureModule)
	outputs, err := render(root)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	code := string(outputs[1].data)
	if got := strings.Count(code, `"example.test/app/internal/plugins/instances"`); got != 1 {
		t.Errorf("the instances import appears %d times, want 1:\n%s", got, code)
	}
	if want := "{instances.Spec, nil},"; !strings.Contains(code, want) {
		t.Errorf("the registry plugin has no registration entry %q:\n%s", want, code)
	}
}
