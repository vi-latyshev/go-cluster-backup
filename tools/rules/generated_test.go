package main

import (
	"strings"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/tools/internal/pluginmeta"
)

// TestGeneratedReadsTheGraph proves the composition root's parsed view: its
// imports and the loader.Imports map.
func TestGeneratedReadsTheGraph(t *testing.T) {
	root := moduleTree(t, map[string]string{
		"internal/plugins/all/zz_generated.go":            compositionRoot(hostRegistration),
		"internal/plugins/all/components/zz_generated.go": generatedRoot(`loader.Imports{"thing/backend": {"thing"}}`),
	})
	graph, err := testRepo(root).generated()
	if err != nil {
		t.Fatalf("generated: %v", err)
	}
	if len(graph.missing) != 0 {
		t.Fatalf("missing = %v", graph.missing)
	}
	want := fixtureModule + "/internal/plugins/thing"
	found := false
	for _, importPath := range graph.imports {
		found = found || importPath == want
	}
	if !found || len(graph.imports) != 9 {
		t.Fatalf("imports = %v", graph.imports)
	}
	if deps := graph.edges["thing/backend"]; len(deps) != 1 || deps[0] != "thing" {
		t.Fatalf("edges = %v", graph.edges)
	}
}

// TestGeneratedMissingIsAFinding proves an absent generated file does not stop
// the scan: the import-coverage rule reports it.
func TestGeneratedMissingIsAFinding(t *testing.T) {
	graph, err := testRepo(moduleTree(t, map[string]string{})).generated()
	if err != nil {
		t.Fatalf("generated: %v", err)
	}
	if len(graph.missing) != 2 || len(graph.edges) != 0 {
		t.Fatalf("generated = %+v", graph)
	}
}

// TestGeneratedErrors proves every malformed composition root is refused with
// the cause named.
func TestGeneratedErrors(t *testing.T) {
	cases := []struct {
		name  string
		path  string
		root  string
		want  string
		extra map[string]string
	}{
		{"unparseable", generatedFile, "package components func", "parse", nil},
		{"twice", generatedFile, `package components

func Compose(r *loader.Registry) error {
	_ = loader.SetImports(r, loader.Imports{})
	return loader.SetImports(r, loader.Imports{})
}
`, "calls loader.SetImports twice", nil},
		{"twice across halves", generatedFile, `package components

func Compose(r *loader.Registry) error { return loader.SetImports(r, loader.Imports{}) }
`, "calls loader.SetImports twice", map[string]string{
			compositionRootFile: `package all

func Compose(r *loader.Registry) error { return loader.SetImports(r, loader.Imports{}) }
`}},
		{"wrong arity", generatedFile, `package components

func Compose(r *loader.Registry) error { return loader.SetImports(r) }
`, "takes one graph", nil},
		{"not a graph literal", generatedFile, `package components

func Compose(r *loader.Registry, graph loader.Imports) error { return loader.SetImports(r, graph) }
`, "not a loader.Imports literal", nil},
		{"no call", generatedFile, "package components\n", "no loader.SetImports call", map[string]string{
			compositionRootFile: "package all\n"}},
		{"another call", generatedFile, "package components\n\nfunc Compose() { println() }\n", "no loader.SetImports call", map[string]string{
			compositionRootFile: "package all\n"}},
		{"positional entry", generatedFile, `package components

func Compose(r *loader.Registry) error { return loader.SetImports(r, loader.Imports{"a"}) }
`, "must use plugin ids as keys", nil},
		{"non-literal id", generatedFile, `package components

func Compose(r *loader.Registry, key string) error { return loader.SetImports(r, loader.Imports{key: {"a"}}) }
`, "non-literal plugin id", nil},
		{"non-list dependencies", generatedFile, `package components

func Compose(r *loader.Registry, deps []string) error { return loader.SetImports(r, loader.Imports{"a": deps}) }
`, "not a []string literal", nil},
		{"non-literal dependency", generatedFile, `package components

func Compose(r *loader.Registry, dep string) error { return loader.SetImports(r, loader.Imports{"a": {dep}}) }
`, "not string literals", nil},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			files := map[string]string{test.path: test.root}
			for name, content := range test.extra {
				files[name] = content
			}
			_, err := testRepo(moduleTree(t, files)).generated()
			wantError(t, err, test.want)
		})
	}
}

// TestCheckReportsAGeneratedFailure proves the scan error reaches the command.
func TestCheckReportsAGeneratedFailure(t *testing.T) {
	root := moduleTree(t, map[string]string{
		"internal/plugins/cli/cli.go":                     "package cli\n",
		"internal/plugins/all/components/zz_generated.go": "package components\n\nfunc Compose(r *loader.Registry) error { return loader.SetImports(r) }\n",
	})
	_, err := check(root)
	wantError(t, err, "takes one graph")
}

// TestCheckReportsAScanFailure proves a parse failure outside the generated
// root also reaches the command.
func TestCheckReportsAScanFailure(t *testing.T) {
	root := moduleTree(t, map[string]string{"internal/plugins/cli/cli.go": "package cli func"})
	_, err := check(root)
	wantError(t, err, "parse")
}

// TestStringExprForms proves every static form a role name may use.
func TestStringExprForms(t *testing.T) {
	scanned := mustScan(t, map[string]string{
		"internal/plugins/library/plugin.go": `package library

const RoleFound = "found"

var Spec = loader.Spec{ID: "library", Summary: "a library"}
`,
		"internal/plugins/cli/cli.go": `package cli

import (
	"example.test/app/internal/loader"
	"example.test/app/internal/plugins/library"
)

const RoleLocal = "local"

var HostSpec = loader.Spec{ID: "cli", Summary: "host"}

var roles = []loader.RoleSpec{
	loader.RoleSpec{Name: "literal-name"},
	loader.RoleSpec{Name: RoleLocal},
	loader.RoleSpec{Name: library.RoleFound},
	loader.RoleSpec{Name: library.Missing},
	loader.RoleSpec{Name: missing.Role},
	loader.RoleSpec{Name: other.Pkg.Name},
	loader.RoleSpec{Name: 1},
	loader.RoleSpec{Default: true},
	loader.RoleSpec{"positional"},
	[]loader.RoleSpec{},
}
`,
	})
	roles := scanned.roleNames()
	for _, want := range []string{"literal-name", "local", "found"} {
		if _, ok := roles[want]; !ok {
			t.Fatalf("roles = %v; want %q", roles, want)
		}
	}
	if len(roles) != 3 {
		t.Fatalf("roles = %v", roles)
	}
}

// TestStringsAreNotIDsOutsideInstances proves a lookalike ID helper does not
// suppress the literal check.
func TestStringsAreNotIDsOutsideInstances(t *testing.T) {
	scanned := mustScan(t, map[string]string{
		"internal/plugins/thing/thing.go": `package thing

import "example.test/app/internal/plugins/instances"

type Thing interface{ Do() }

var Kind = instances.NewKind[Thing]("thing")
`,
		"internal/plugins/library/stray.go": `package library

var derived = other.ID("thing/backend")
`,
	})
	findings := ruleFactoryIDs(scanned, nil)
	if len(findings) != 1 || !strings.Contains(findings[0], `literal factory id "thing/backend"`) {
		t.Fatalf("findings = %v", findings)
	}
}

// TestVarWithoutValues proves a declaration that omits a value for one name is
// skipped instead of misread.
func TestVarWithoutValues(t *testing.T) {
	scanned := mustScan(t, map[string]string{
		"internal/plugins/library/plugin.go": `package library

import "example.test/app/internal/loader"

var (
	A, B = 1
	Spec = loader.Spec{ID: "library", Summary: "a library"}
)

var C = 1
`,
	})
	if p := scanned.ByDir["internal/plugins/library"]; p.Spec == nil {
		t.Fatal("the trailing declaration must still resolve")
	}
}

// TestNonLiteralSpecIsReported proves a Spec that is not a composite literal is
// refused by the protocol rule instead of silently dropping the package.
func TestNonLiteralSpecIsReported(t *testing.T) {
	scanned := mustScan(t, map[string]string{
		"internal/plugins/library/plugin.go": `package library

var Spec = notAComposite
`,
	})
	findings := ruleRequires(scanned, &generated{edges: map[string][]string{}})
	wantFinding(t, findings, "Spec must be a loader.Spec composite literal")
}

// TestRequiresCoverageEdges proves the loops tolerate a malformed id, a
// self-import and a plugin without an id.
func TestRequiresCoverageEdges(t *testing.T) {
	scanned := mustScan(t, map[string]string{
		"internal/plugins/library/plugin.go": `package library

import (
	"example.test/app/internal/loader"
	_ "example.test/app/internal/plugins/library"
)

var Spec = loader.Spec{ID: "library", Summary: "a library", Requires: []string{"thing"}}
`,
		"internal/plugins/thing/plugin.go": `package thing

import "example.test/app/internal/loader"

var Spec = loader.Spec{Summary: "no id"}
`,
	})
	findings := ruleRequires(scanned, &generated{edges: map[string][]string{
		"library": {"thing"},
	}})
	wantFinding(t, findings, "Spec.ID is required")
	wantFinding(t, findings, `Spec.Requires names "thing", which is not a compiled plugin`)
}

// TestProblemLocationPrefixesAPackageLevelProblem proves the protocol report
// names the package when the shared reader attaches no file.
func TestProblemLocationPrefixesAPackageLevelProblem(t *testing.T) {
	p := &pluginmeta.Package{Dir: "internal/plugins/twice"}
	got := problemLocation(p, pluginmeta.Problem{Msg: "package twice declares Kind twice"})
	if got != "internal/plugins/twice: package twice declares Kind twice" {
		t.Fatalf("problemLocation = %q", got)
	}
}

// TestCompositionSplit proves the two generated halves: the host lives only in
// the root half and is registered there, the root imports the hostless half, and
// the components directory is a generated output rather than a plugin package.
func TestCompositionSplit(t *testing.T) {
	componentsPath := fixtureModule + "/internal/plugins/all/components"
	t.Run("host stays out of the hostless half", func(t *testing.T) {
		files := healthyTree()
		files["internal/plugins/all/components/zz_generated.go"] = `package components

import (
	_ "example.test/app/internal/plugins/cli"
	"example.test/app/internal/loader"
)

func Compose(r *loader.Registry) error { return loader.SetImports(r, loader.Imports{}) }
`
		findings := checkTree(t, files)
		wantFinding(t, findings, "is a host and must not be imported by the hostless components half")
	})
	t.Run("root imports and registers the host", func(t *testing.T) {
		files := healthyTree()
		files["internal/plugins/all/zz_generated.go"] = compositionRoot("")
		findings := checkTree(t, files)
		wantFinding(t, findings, "is a host but the generated composition root does not register it")
	})
	t.Run("root imports the host", func(t *testing.T) {
		files := healthyTree()
		files["internal/plugins/all/zz_generated.go"] = strings.Replace(compositionRoot(hostRegistration),
			"\t\"example.test/app/internal/plugins/cli\"\n", "", 1)
		findings := checkTree(t, files)
		wantFinding(t, findings, "is a host but the generated composition root does not import it")
	})
	t.Run("root imports the hostless half", func(t *testing.T) {
		files := healthyTree()
		files["internal/plugins/all/zz_generated.go"] = strings.Replace(compositionRoot(hostRegistration),
			"\t\"example.test/app/internal/plugins/all/components\"\n", "", 1)
		findings := checkTree(t, files)
		wantFinding(t, findings, "the generated composition root does not import "+componentsPath)
	})
	t.Run("missing root half", func(t *testing.T) {
		files := healthyTree()
		delete(files, "internal/plugins/all/zz_generated.go")
		findings := checkTree(t, files)
		wantFinding(t, findings, compositionRootFile+" is missing")
		wantFinding(t, findings, "does not import "+componentsPath)
	})
	t.Run("components is not a plugin package", func(t *testing.T) {
		if pluginPackageDir("internal/plugins/all/components") {
			t.Fatal("the generated components half must not demand plugin.go")
		}
	})
}

// TestGeneratedRootParseError proves a malformed root half stops the scan with
// the file named.
func TestGeneratedRootParseError(t *testing.T) {
	files := healthyTree()
	files["internal/plugins/all/zz_generated.go"] = "package all func"
	_, err := check(writeTree(t, files))
	wantError(t, err, compositionRootFile)
}

// TestRegistersSpecIgnoresBlankAndDotImports proves only a named import can
// register a plugin.
func TestRegistersSpecIgnoresBlankAndDotImports(t *testing.T) {
	root := parseFile(t, `package all

import (
	_ "example.test/app/internal/plugins/cli"
	. "example.test/app/internal/plugins/library"
)

var _ = cli.Spec
`)
	if registersSpec(root, fixtureModule+"/internal/plugins/cli") {
		t.Fatal("a blank or dot import registers no plugin")
	}
}
