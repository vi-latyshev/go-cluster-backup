package pluginmeta

import (
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/tools/internal/pluginmeta/malformed"
)

// The corpus is the one metadata fixture both consumers read: tools/gen renders
// its golden composition root from it, tools/rules checks its rules against it.
const corpus = "testdata/corpus"

const moduleGoMod = "module example.test/app\n\ngo 1.27.4\n"

// writeTree materialises one module fixture in a temp directory.
func writeTree(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	for name, content := range files {
		target := filepath.Join(root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			t.Fatalf("create %s: %v", filepath.Dir(target), err)
		}
		if err := os.WriteFile(target, []byte(content), 0o644); err != nil {
			t.Fatalf("write %s: %v", target, err)
		}
	}
	return root
}

// moduleTree is writeTree with the go.mod every fixture needs.
func moduleTree(t *testing.T, files map[string]string) string {
	t.Helper()
	all := make(map[string]string, len(files)+1)
	for name, content := range files {
		all[name] = content
	}
	if _, ok := all["go.mod"]; !ok {
		all["go.mod"] = moduleGoMod
	}
	return writeTree(t, all)
}

// copyTree copies a checked-in fixture into a temp directory.
func copyTree(t *testing.T, src string) string {
	t.Helper()
	dst := t.TempDir()
	err := filepath.WalkDir(src, func(name string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, name)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if entry.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		data, err := os.ReadFile(name)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, 0o644)
	})
	if err != nil {
		t.Fatalf("copy %s: %v", src, err)
	}
	return dst
}

// readFixture reads a fixture module and fails the test on error.
func readFixture(t *testing.T, files map[string]string) *Tree {
	t.Helper()
	tree, err := Read(moduleTree(t, files))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return tree
}

// packageBySuffix finds the fixture package whose dir ends with suffix.
func packageBySuffix(t *testing.T, tree *Tree, suffix string) *Package {
	t.Helper()
	for _, p := range tree.Packages {
		if p.Dir == suffix || strings.HasSuffix(p.Dir, "/"+suffix) {
			return p
		}
	}
	t.Fatalf("package %q not found", suffix)
	return nil
}

// wantSubstring fails the test when a string does not name the cause.
func wantSubstring(t *testing.T, got, want string) {
	t.Helper()
	if !strings.Contains(got, want) {
		t.Fatalf("want %q in %q", want, got)
	}
}

// wantProblems asserts the package's problems name one cause.
func wantProblems(t *testing.T, files map[string]string, want string) {
	t.Helper()
	tree := readFixture(t, files)
	pkg := packageBySuffix(t, tree, "p")
	var all []string
	for _, problem := range pkg.Problems {
		all = append(all, problem.Msg)
	}
	wantSubstring(t, strings.Join(all, "\n"), want)
}

// TestM9_CorpusMetadata is the one shared corpus test: both consumers read this
// corpus, so the metadata rules are proved once for tools/gen and tools/rules.
func TestM9_CorpusMetadata(t *testing.T) {
	tree, err := Read(copyTree(t, corpus))
	if err != nil {
		t.Fatalf("read corpus: %v", err)
	}
	if tree.Module != "example.test/app" {
		t.Fatalf("module = %q", tree.Module)
	}
	var ids []string
	for _, p := range tree.Packages {
		if p.Spec != nil {
			ids = append(ids, p.Spec.ID)
		}
	}
	// The conformance kit is metadata like any other package; excluding kits is
	// the generator's policy, not the reader's.
	want := []string{"cli", "configuration", "errors", "instances", "model", "notifier/webhook", "reporting", "source/files", "source/postgres", "kit"}
	if !reflect.DeepEqual(ids, want) {
		t.Fatalf("plugin ids = %v, want %v", ids, want)
	}

	byID := make(map[string]*Package, len(ids))
	for _, p := range tree.Packages {
		if p.Spec != nil {
			byID[p.Spec.ID] = p
		}
	}
	if !byID["notifier/webhook"].Spec.Floor {
		t.Error("webhook must declare Floor")
	}
	if !byID["cli"].Spec.Floor {
		t.Error("the CLI host must declare Floor")
	}
	if want := []string{"model"}; !reflect.DeepEqual(byID["notifier/webhook"].Spec.Requires, want) {
		t.Errorf("webhook requires = %v, want %v", byID["notifier/webhook"].Spec.Requires, want)
	}
	if want := []string{"configuration"}; !reflect.DeepEqual(byID["source/postgres"].Spec.Requires, want) {
		t.Errorf("postgres requires = %v, want %v", byID["source/postgres"].Spec.Requires, want)
	}
	if want := []string{"discovery/kubernetes_labels"}; !reflect.DeepEqual(byID["source/postgres"].Spec.After, want) {
		t.Errorf("postgres after = %v, want %v", byID["source/postgres"].Spec.After, want)
	}
	if got := byID["notifier/webhook"].Apply; got == nil || got.Kind != ApplyFactory {
		t.Errorf("webhook apply = %+v, want factory", got)
	}
	if got := byID["reporting"].Apply; got == nil || got.Kind != ApplyPlugin {
		t.Errorf("reporting apply = %+v, want plugin", got)
	}
	if got := byID["model"].Apply; got != nil {
		t.Errorf("model apply = %+v, want none", got)
	}
	if got := byID["configuration"].Apply; got == nil || got.Kind != ApplyPlugin {
		t.Errorf("configuration apply = %+v, want plugin", got)
	}
	if got := tree.ByDir["internal/plugins/source"].Kind; got != "source" {
		t.Errorf("source kind = %q", got)
	}
	if got := byID["instances"].Kind; got != "" {
		t.Errorf("instances kind = %q, want none", got)
	}
	if got := byID["kit"].Dir; got != "internal/plugins/source/sourcetest" {
		t.Errorf("kit dir = %q", got)
	}
	for _, p := range tree.Packages {
		if len(p.Problems) > 0 {
			t.Errorf("%s has problems: %v", p.Dir, p.Problems)
		}
	}

	// The implicit-alias case: a file importing the standard errors package next
	// to internal/plugins/errors (package pluginerrors) keeps both bindings.
	files := byID["reporting"].Files
	var reportingImports map[string]string
	for _, f := range files {
		if strings.HasSuffix(f.Rel, "reporting/plugin.go") {
			reportingImports = f.Imports
		}
	}
	if got := reportingImports["errors"]; got != "errors" {
		t.Errorf("errors import = %q, want the standard package", got)
	}
	if got := reportingImports["pluginerrors"]; got != "example.test/app/internal/plugins/errors" {
		t.Errorf("pluginerrors import = %q", got)
	}

	// The build-tagged production file of source/files imports source/postgres.
	var filesPkg *Package
	for _, p := range tree.Packages {
		if p.Dir == "internal/plugins/source/files" {
			filesPkg = p
		}
	}
	if filesPkg == nil {
		t.Fatal("source/files not parsed")
	}

	// Positions survive the shared reader: a consumer names file and line, not
	// just the file, so a rule finding keeps `file:line`.
	if tree.FileSet == nil {
		t.Fatal("tree has no FileSet")
	}
	reporting := byID["reporting"]
	if len(reporting.Files) == 0 {
		t.Fatal("reporting has no files")
	}
	position := tree.Position(reporting.Files[0].AST.Name)
	if position.Line != 1 {
		t.Errorf("package clause line = %d, want 1", position.Line)
	}
	if !strings.HasSuffix(position.Filename, "internal/plugins/reporting/plugin.go") {
		t.Errorf("position filename = %q", position.Filename)
	}
	if _, ok := tree.ByPath["example.test/app/internal/plugins/source/postgres"]; !ok {
		t.Error("tree misses source/postgres")
	}
}

// TestModulePath proves the go.mod reader.
func TestModulePath(t *testing.T) {
	root := moduleTree(t, nil)
	module, err := ModulePath(root)
	if err != nil || module != "example.test/app" {
		t.Fatalf("ModulePath = %q, %v", module, err)
	}
	_, err = ModulePath(writeTree(t, nil))
	wantSubstring(t, err.Error(), "read go.mod")
	_, err = ModulePath(writeTree(t, map[string]string{"go.mod": "go 1.27.4\n"}))
	wantSubstring(t, err.Error(), "no module directive")
	_, err = ModulePath(writeTree(t, map[string]string{"go.mod": "module \"\"\n"}))
	wantSubstring(t, err.Error(), "no module directive")
}

// TestReadErrors proves the reader names I/O and syntax failures, and refuses a
// directory holding two packages.
func TestReadErrors(t *testing.T) {
	_, err := Read(writeTree(t, map[string]string{"internal/plugins/p/plugin.go": "package p\n"}))
	wantSubstring(t, err.Error(), "read go.mod")

	_, err = Read(writeTree(t, map[string]string{"go.mod": moduleGoMod}))
	wantSubstring(t, err.Error(), "read ")

	root := moduleTree(t, map[string]string{"internal/plugins/p/plugin.go": "package p\nfunc {\n"})
	_, err = Read(root)
	wantSubstring(t, err.Error(), "parse internal/plugins/p/plugin.go")

	root = moduleTree(t, map[string]string{
		"internal/plugins/p/a.go": "package p\n",
		"internal/plugins/p/b.go": "package q\n",
	})
	_, err = Read(root)
	wantSubstring(t, err.Error(), "more than one package")
}

// TestReadWalksProductionFilesOnly proves the walk skips testdata, vendored,
// hidden, underscore, editor and test files, but keeps build-tagged files.
func TestReadWalksProductionFilesOnly(t *testing.T) {
	root := moduleTree(t, map[string]string{
		"go.mod":                               moduleGoMod,
		"cmd/program/main.go":                  "package main\n",
		"internal/plugins/p/plugin.go":         "package p\n",
		"internal/plugins/p/platform_linux.go": "//go:build linux\n\npackage p\n",
		"internal/plugins/p/plugin_test.go":    "package p\n",
		"internal/plugins/p/testdata/x.go":     "package x\n",
		"internal/plugins/p/vendor/v.go":       "package v\n",
		"internal/plugins/p/.cache/c.go":       "package c\n",
		"internal/plugins/p/_scratch/s.go":     "package s\n",
		"internal/plugins/p/.hidden.go":        "package h\n",
		"internal/plugins/p/_hidden.go":        "package h\n",
		"internal/plugins/p/notes.txt":         "not go\n",
		"internal/plugins/all/bad.go":          "this is generated output, not a package\n",
	})
	tree, err := Read(root)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var dirs []string
	for _, p := range tree.Packages {
		dirs = append(dirs, p.Dir)
	}
	want := []string{"cmd/program", "internal/plugins/p"}
	if !reflect.DeepEqual(dirs, want) {
		t.Fatalf("packages = %v, want %v", dirs, want)
	}
	if got := len(tree.ByDir["internal/plugins/p"].Files); got != 2 {
		t.Errorf("p files = %d, want 2 (build-tagged file included)", got)
	}
}

// fileEntry is the DirEntry a fake walker hands the callback for one file.
type fileEntry struct{}

func (fileEntry) Name() string               { return "plugin.go" }
func (fileEntry) IsDir() bool                { return false }
func (fileEntry) Type() fs.FileMode          { return 0 }
func (fileEntry) Info() (fs.FileInfo, error) { return nil, errors.New("unused") }

// TestWalkErrors proves the walker's own failures are named, both when the walk
// cannot start and when one entry cannot be read.
func TestWalkErrors(t *testing.T) {
	original := walkDir
	t.Cleanup(func() { walkDir = original })

	walkDir = func(string, fs.WalkDirFunc) error { return errors.New("boom") }
	_, err := Read(moduleTree(t, map[string]string{"internal/plugins/p/plugin.go": "package p\n"}))
	wantSubstring(t, err.Error(), "boom")

	walkDir = func(_ string, fn fs.WalkDirFunc) error { return fn("broken", nil, errors.New("boom")) }
	_, err = Read(moduleTree(t, map[string]string{"internal/plugins/p/plugin.go": "package p\n"}))
	wantSubstring(t, err.Error(), "boom")
}

// TestAddFileRelativePathError proves the one unprovokable path failure is
// reported.
func TestAddFileRelativePathError(t *testing.T) {
	originalPath, originalWalk := relPath, walkDir
	relPath = func(string, string) (string, error) { return "", errors.New("boom") }
	walkDir = func(dir string, fn fs.WalkDirFunc) error {
		return fn(filepath.Join(dir, "plugins", "p", "plugin.go"), fileEntry{}, nil)
	}
	t.Cleanup(func() { relPath, walkDir = originalPath, originalWalk })
	root := moduleTree(t, map[string]string{"internal/plugins/p/plugin.go": "package p\n"})
	_, err := Read(root)
	wantSubstring(t, err.Error(), "relative path")
}

// TestSpecProblems proves every malformed Spec in the shared metadata corpus is
// recorded with its rule.
func TestSpecProblems(t *testing.T) {
	for _, test := range malformed.Cases() {
		t.Run(test.Name, func(t *testing.T) {
			wantProblems(t, test.Files, test.Want)
		})
	}
}

// TestR2_M9_SharedMalformedCorpus pins the one malformed-metadata matrix both
// consumers run: the generator renders the same cases through its error path,
// so a rule cannot drift between the reader and the generator.
func TestR2_M9_SharedMalformedCorpus(t *testing.T) {
	cases := malformed.Cases()
	if len(cases) < 29 {
		t.Fatalf("corpus has %d cases, want the whole malformed-metadata matrix", len(cases))
	}
	seen := make(map[string]bool, len(cases))
	for _, test := range cases {
		if test.Name == "" || test.Want == "" || len(test.Files) == 0 {
			t.Errorf("incomplete case %+v", test)
		}
		if seen[test.Name] {
			t.Errorf("case %q appears twice", test.Name)
		}
		seen[test.Name] = true
	}
}

// TestSpecResolution proves the two positive derivations the corpus cannot
// express: a literal kind and a kind declared by another package.
func TestSpecResolution(t *testing.T) {
	t.Run("literal kind resolves", func(t *testing.T) {
		tree := readFixture(t, map[string]string{
			"internal/plugins/p/plugin.go": "package p\n\nimport (\n\t\"example.test/app/internal/loader\"\n\t\"example.test/app/internal/plugins/instances\"\n)\n\nvar Spec = loader.Spec{ID: instances.ID(\"source\", \"files\"), Summary: \"p\"}\n",
		})
		if got := packageBySuffix(t, tree, "p").Spec.ID; got != "source/files" {
			t.Errorf("id = %q, want source/files", got)
		}
	})
	t.Run("kind from another package resolves", func(t *testing.T) {
		tree := readFixture(t, map[string]string{
			"internal/plugins/p/plugin.go":      "package p\n\nimport (\n\t\"example.test/app/internal/loader\"\n\t\"example.test/app/internal/plugins/instances\"\n\t\"example.test/app/internal/plugins/source\"\n)\n\nvar Spec = loader.Spec{ID: instances.ID(source.Kind.Name(), \"files\"), Summary: \"p\"}\n",
			"internal/plugins/source/source.go": "package source\n\nimport \"example.test/app/internal/plugins/instances\"\n\nvar Kind = instances.NewKind[any](\"source\")\n",
		})
		if got := packageBySuffix(t, tree, "p").Spec.ID; got != "source/files" {
			t.Errorf("id = %q, want source/files", got)
		}
	})
}

// TestApplyProblems proves every malformed Apply is named, never guessed.
func TestApplyProblems(t *testing.T) {
	const header = `package p

import (
	"example.test/app/internal/loader"
	"example.test/app/internal/plugins/instances"
)

var Spec = loader.Spec{ID: "p", Summary: "p"}

`
	tests := []struct {
		name  string
		apply string
		want  string
	}{
		{"no parameters", "func Apply() error { return nil }", "Apply must take a *loader.Registry"},
		{"first parameter is not a registry", "func Apply(x int) error { return nil }", "first parameter must be *loader.Registry"},
		{"registry is not a pointer", "func Apply(lr loader.Registry) error { return nil }", "first parameter must be *loader.Registry"},
		{"registry is unqualified", "func Apply(lr *Registry) error { return nil }", "first parameter must be *loader.Registry"},
		{"registry has another name", "func Apply(lr *loader.RegistryX) error { return nil }", "first parameter must be *loader.Registry"},
		{"registry is parenthesized", "func Apply(lr *(loader.Registry)) error { return nil }", "first parameter must be *loader.Registry"},
		{"second parameter is not a factory registry", "func Apply(lr *loader.Registry, x int) error { return nil }", "second parameter must be *instances.Registry"},
		{"second parameter is the loader registry", "func Apply(lr *loader.Registry, other *loader.Registry) error { return nil }", "second parameter must be *instances.Registry"},
		{"three named parameters", "func Apply(a, b, c *loader.Registry) error { return nil }", "Apply takes one or two parameters, not 3"},
		{"three unnamed parameters", "func Apply(*loader.Registry, *instances.Registry, int) error { return nil }", "Apply takes one or two parameters, not 3"},
		{"no results", "func Apply(lr *loader.Registry) {}", "Apply must return error"},
		{"two results", "func Apply(lr *loader.Registry) (error, error) { return nil, nil }", "Apply must return error"},
		{"result is not error", "func Apply(lr *loader.Registry) int { return 0 }", "Apply must return error"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			wantProblems(t, map[string]string{"internal/plugins/p/plugin.go": header + test.apply + "\n"}, test.want)
		})
	}

	t.Run("loader is not imported by the Apply file", func(t *testing.T) {
		wantProblems(t, map[string]string{
			"internal/plugins/p/plugin.go": "package p\n\nimport \"example.test/app/internal/loader\"\n\nvar Spec = loader.Spec{ID: \"p\", Summary: \"p\"}\n",
			"internal/plugins/p/apply.go":  "package p\n\nfunc Apply(lr *loader.Registry) error { return nil }\n",
		}, "first parameter must be *loader.Registry")
	})

	t.Run("unnamed single parameter", func(t *testing.T) {
		tree := readFixture(t, map[string]string{
			"internal/plugins/p/plugin.go": "package p\n\nimport \"example.test/app/internal/loader\"\n\nvar Spec = loader.Spec{ID: \"p\", Summary: \"p\"}\n\nfunc Apply(*loader.Registry) error { return nil }\n",
		})
		pkg := packageBySuffix(t, tree, "p")
		if len(pkg.Problems) != 0 {
			t.Fatalf("problems = %v", pkg.Problems)
		}
		if pkg.Apply == nil || pkg.Apply.Kind != ApplyPlugin {
			t.Fatalf("apply = %+v, want plugin", pkg.Apply)
		}
	})

	t.Run("Apply declared twice", func(t *testing.T) {
		wantProblems(t, map[string]string{
			"internal/plugins/p/one.go": header + "func Apply(lr *loader.Registry) error { return nil }\n",
			"internal/plugins/p/two.go": "package p\n\nfunc Apply(lr *loader.Registry) error { return nil }\n",
		}, "declares Apply twice")
	})
}

// TestKindProblems proves every malformed kind declaration is recorded.
func TestKindProblems(t *testing.T) {
	const header = `package p

import "example.test/app/internal/plugins/instances"

type Seam interface{ Name() string }

`
	tests := []struct {
		name string
		decl string
		want string
	}{
		{"not a call", "var Kind = 42", "must be a instances.NewKind call"},
		{"not a generic call", `var Kind = instances.NewKind("k")`, "must be a instances.NewKind call"},
		{"another constructor", "var Kind = instances.Other[Seam](\"k\")", "must be a instances.NewKind call"},
		{"another package", `var Kind = other.NewKind[Seam]("k")`, "NewKind must come from the instances package"},
		{"no argument", "var Kind = instances.NewKind[Seam]()", "NewKind takes exactly one argument"},
		{"argument is not a literal", "var Kind = instances.NewKind[Seam](name)", "must be a string literal"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			files := map[string]string{"internal/plugins/p/plugin.go": header + test.decl + "\n"}
			if test.name == "another package" {
				files["internal/plugins/p/plugin.go"] = "package p\n\nimport other \"example.test/app/internal/other\"\n\ntype Seam interface{ Name() string }\n\n" + test.decl + "\n"
			}
			wantProblems(t, files, test.want)
		})
	}

	t.Run("nested call", func(t *testing.T) {
		wantProblems(t, map[string]string{"internal/plugins/p/plugin.go": header + "var Kind = get().NewKind[Seam](\"k\")\n"}, "NewKind must come from the instances package")
	})

	t.Run("multi-type instantiation", func(t *testing.T) {
		tree := readFixture(t, map[string]string{"internal/plugins/p/plugin.go": header + "var Kind = instances.NewKind[Seam, any](\"k\")\n"})
		if got := packageBySuffix(t, tree, "p").Kind; got != "k" {
			t.Errorf("kind = %q, want k", got)
		}
	})

	t.Run("declared twice", func(t *testing.T) {
		wantProblems(t, map[string]string{
			"internal/plugins/p/one.go": header + "var Kind = instances.NewKind[Seam](\"a\")\n",
			"internal/plugins/p/two.go": "package p\n\nimport \"example.test/app/internal/plugins/instances\"\n\ntype Seam interface{ Name() string }\n\nvar Kind = instances.NewKind[Seam](\"b\")\n",
		}, "declares Kind twice")
	})
}

// TestCollectRecordsDeclarations proves the package-level facts the rules read:
// type names, string constants, and ignoring everything that is not protocol.
func TestCollectRecordsDeclarations(t *testing.T) {
	tree := readFixture(t, map[string]string{
		"internal/plugins/p/plugin.go": `package p

const (
	ID      = "p"
	Missing
)

type Value struct{}

func helper() error { return nil }

func (Value) Apply() error { return nil }

var A, Spec = 1
`,
	})
	pkg := packageBySuffix(t, tree, "p")
	if _, ok := pkg.Types["Value"]; !ok {
		t.Error("type Value not recorded")
	}
	if got := pkg.Consts["ID"]; got != "p" {
		t.Errorf("const ID = %q", got)
	}
	if pkg.Spec != nil || pkg.Apply != nil {
		t.Errorf("collect recorded a plugin declaration: %+v %+v", pkg.Spec, pkg.Apply)
	}
}

// TestImportNamesSemantics proves alias resolution order and the blank/dot skips.
func TestImportNamesSemantics(t *testing.T) {
	src := `package p

import (
	"errors"
	first "example.test/app/internal/first"
	first "example.test/app/internal/second"
	_ "example.test/app/internal/unused"
	. "example.test/app/internal/dotted"
	"example.test/app/internal/plugins/errors"
	"example.test/app/internal/plugins/instances"
)
`
	tree, err := parser.ParseFile(token.NewFileSet(), "fixture.go", src, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	names := map[string]string{
		"example.test/app/internal/plugins/errors":    "pluginerrors",
		"example.test/app/internal/plugins/instances": "instances",
	}
	imports := importNames(tree, names)
	want := map[string]string{
		"errors":       "errors",
		"first":        "example.test/app/internal/first",
		"pluginerrors": "example.test/app/internal/plugins/errors",
		"instances":    "example.test/app/internal/plugins/instances",
	}
	if !reflect.DeepEqual(imports, want) {
		t.Fatalf("imports = %v, want %v", imports, want)
	}
	paths := importPaths(tree)
	if len(paths) != 7 {
		t.Errorf("paths = %v, want 7 entries", paths)
	}
}

// TestApplyKindOfSyntheticExpressions proves the registry predicates on shapes
// the parser cannot produce in a parameter list.
func TestApplyKindOfSyntheticExpressions(t *testing.T) {
	f := &File{Imports: map[string]string{"loader": "example.test/app/internal/loader"}}
	qualified := &ast.StarExpr{X: &ast.SelectorExpr{X: &ast.CallExpr{Fun: ast.NewIdent("get")}, Sel: ast.NewIdent("Registry")}}
	if isRegistry(f, qualified, isLoader, "Registry") {
		t.Error("a qualified call is not a registry type")
	}
	inner := &ast.StarExpr{X: &ast.SelectorExpr{X: ast.NewIdent("loader"), Sel: ast.NewIdent("Registry")}}
	if isRegistry(f, &ast.StarExpr{X: inner}, isLoader, "Registry") {
		t.Error("a pointer to a pointer is not a registry type")
	}
}

// TestLiteralReaders proves the small readers under the protocol.
func TestLiteralReaders(t *testing.T) {
	if got, ok := StringLiteral(&ast.BasicLit{Kind: token.STRING, Value: `"ok"`}); !ok || got != "ok" {
		t.Errorf("StringLiteral = %q, %v", got, ok)
	}
	original := unquote
	unquote = func(string) (string, error) { return "", errors.New("boom") }
	t.Cleanup(func() { unquote = original })
	if _, ok := StringLiteral(&ast.BasicLit{Kind: token.STRING, Value: `"ok"`}); ok {
		t.Error("a failing unquote must report no value")
	}
	unquote = original

	if _, ok := StringLiteral(&ast.BasicLit{Kind: token.INT, Value: "42"}); ok {
		t.Error("an int literal is not a string")
	}
	if _, ok := BoolLiteral(ast.NewIdent("maybe")); ok {
		t.Error("maybe is not a bool literal")
	}
	if value, ok := BoolLiteral(ast.NewIdent("true")); !ok || !value {
		t.Errorf("BoolLiteral(true) = %v, %v", value, ok)
	}
	if value, ok := BoolLiteral(ast.NewIdent("false")); !ok || value {
		t.Errorf("BoolLiteral(false) = %v, %v", value, ok)
	}
	if _, ok := StringList(&ast.BasicLit{Kind: token.STRING, Value: `"x"`}); ok {
		t.Error("a string is not a list")
	}
	if values, ok := StringList(&ast.CompositeLit{Elts: []ast.Expr{
		&ast.BasicLit{Kind: token.STRING, Value: `"a"`},
		&ast.BasicLit{Kind: token.STRING, Value: `"b"`},
	}}); !ok || !reflect.DeepEqual(values, []string{"a", "b"}) {
		t.Errorf("StringList = %v, %v", values, ok)
	}
	if _, err := stringList(&ast.CompositeLit{Elts: []ast.Expr{&ast.BasicLit{Kind: token.INT, Value: "1"}}}); err == nil {
		t.Error("a non-string element must be refused")
	}
	if got := stringValue(&ast.BasicLit{Kind: token.STRING, Value: `"path"`}); got != "path" {
		t.Errorf("stringValue = %q", got)
	}
}

// TestPackagePredicates proves the two import-path predicates.
func TestPackagePredicates(t *testing.T) {
	if !isLoader("example.test/app/internal/loader") || isLoader("example.test/app/internal/plugins/loader") {
		t.Error("isLoader disagrees")
	}
	if !isInstances("example.test/app/internal/plugins/instances") || isInstances("example.test/app/internal/instances") {
		t.Error("isInstances disagrees")
	}
}
