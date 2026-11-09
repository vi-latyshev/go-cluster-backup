package main

import (
	"go/ast"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestHealthyTreeHasNoFindings is the baseline: the rules accept a module that
// declares its identity, derives its factory ids, keeps roles and commands
// disjoint, and matches its generated graph.
func TestHealthyTreeHasNoFindings(t *testing.T) {
	findings := checkTree(t, healthyTree())
	if len(findings) != 0 {
		t.Fatalf("the healthy tree has findings: %v", findings)
	}
}

// TestScanReadsTheSharedTree proves the rules consume the shared reader's tree:
// production files only, per-file import aliases and skipped testdata/hidden
// directories.
func TestScanReadsTheSharedTree(t *testing.T) {
	root := moduleTree(t, map[string]string{
		"internal/plugins/cli/cli.go": `package cli

import (
	"example.test/app/internal/loader"
	blank "example.test/app/internal/plugins/library"
	. "example.test/app/internal/plugins/thing"
)

var Default = loader.Spec{}
`,
		"internal/plugins/cli/testdata/skip.go": "package cli\n",
		"internal/plugins/cli/cli_test.go":      "package cli\n",
		"internal/.hidden/skip.go":              "package hidden\n",
		"internal/_skip/skip.go":                "package skip\n",
	})
	scanned, err := scan(root)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(scanned.Packages) != 1 {
		t.Fatalf("want one production package, got %d", len(scanned.Packages))
	}
	p := scanned.Packages[0]
	if p.Dir != "internal/plugins/cli" || p.Path != fixtureModule+"/internal/plugins/cli" {
		t.Fatalf("package is %s (%s)", p.Dir, p.Path)
	}
	imports := p.Files[0].Imports
	if imports["loader"] != fixtureModule+"/internal/loader" {
		t.Fatalf("loader alias is %q", imports["loader"])
	}
	if imports["blank"] != fixtureModule+"/internal/plugins/library" {
		t.Fatalf("named import alias is %q", imports["blank"])
	}
	if _, dot := imports["thing"]; dot {
		t.Fatalf("a dot import binds no name: %v", imports)
	}
}

// TestScanErrors proves the shared reader's failures reach the command.
func TestScanErrors(t *testing.T) {
	t.Run("missing module", func(t *testing.T) {
		_, err := scan(t.TempDir())
		wantError(t, err, "read go.mod")
	})
	t.Run("missing internal directory", func(t *testing.T) {
		root := writeTree(t, map[string]string{"go.mod": "module " + fixtureModule + "\n"})
		_, err := scan(root)
		wantError(t, err, "read")
	})
	t.Run("unparseable production file", func(t *testing.T) {
		root := moduleTree(t, map[string]string{"internal/plugins/cli/cli.go": "package cli func"})
		_, err := scan(root)
		wantError(t, err, "parse")
	})
	t.Run("unreadable directory", func(t *testing.T) {
		root := moduleTree(t, map[string]string{
			"internal/locked/file.go":     "package locked\n",
			"internal/plugins/thing/x.go": "package thing\n",
		})
		locked := filepath.Join(root, "internal", "locked")
		if err := os.Chmod(locked, 0o000); err != nil {
			t.Fatalf("chmod: %v", err)
		}
		t.Cleanup(func() { _ = os.Chmod(locked, 0o755) })
		_, err := scan(root)
		wantError(t, err, "permission denied")
	})
}

// TestScanWithoutCmd proves cmd is optional: a fixture that only holds internal
// packages still scans.
func TestScanWithoutCmd(t *testing.T) {
	scanned, err := scan(moduleTree(t, map[string]string{"internal/plugins/cli/cli.go": "package cli\n"}))
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(scanned.Packages) != 1 {
		t.Fatalf("want one package, got %d", len(scanned.Packages))
	}
}

// TestM9_RulesUseTheSharedMetadataReader is the DRY invariant: the rules keep no
// private copy of the metadata protocol — module path, import resolution, Kind,
// Spec, Apply — and read it all through pluginmeta.
func TestM9_RulesUseTheSharedMetadataReader(t *testing.T) {
	scanSource, err := os.ReadFile("scan.go")
	if err != nil {
		t.Fatalf("read scan.go: %v", err)
	}
	if !strings.Contains(string(scanSource), "pluginmeta.Read(root)") {
		t.Fatal("scan.go must read the tree through pluginmeta.Read")
	}
	for _, duplicate := range []string{
		"func modulePath(",
		"func importNames(",
		"func importPaths(",
		"func kindName(",
		"func resolveSpec(",
		"func specID(",
		"func kindArgument(",
		"func stringValueOK(",
		"func boolValue(",
		"func stringList(",
	} {
		if strings.Contains(string(scanSource), duplicate) {
			t.Fatalf("scan.go keeps a private metadata implementation: %s", duplicate)
		}
	}
	for _, source := range []string{"rules.go", "restricted.go"} {
		data, readErr := os.ReadFile(source)
		if readErr != nil {
			t.Fatalf("read %s: %v", source, readErr)
		}
		for _, duplicate := range []string{"func importNames(", "func modulePath(", "func specID("} {
			if strings.Contains(string(data), duplicate) {
				t.Fatalf("%s keeps a private metadata implementation: %s", source, duplicate)
			}
		}
	}
}

func TestImportable(t *testing.T) {
	cases := []struct {
		importer string
		target   string
		want     bool
	}{
		{"internal/plugins/all", "internal/plugins/library", true},
		{"internal/plugins/all", "internal/plugins/storage/internal", false},
		{"internal/plugins/storage/guard", "internal/plugins/storage/internal", true},
		{"internal/plugins/storage", "internal/plugins/storage/internal", true},
		{"internal/plugins/other", "internal/plugins/storage/internal/deep", false},
	}
	for _, test := range cases {
		if got := importable(test.importer, test.target); got != test.want {
			t.Fatalf("importable(%q, %q) = %v, want %v", test.importer, test.target, got, test.want)
		}
	}
}

func TestDirAllowed(t *testing.T) {
	if !dirAllowed("internal/plugins/storage", "internal/plugins/storage") {
		t.Fatal("the exact package must be allowed")
	}
	if !dirAllowed("internal/plugins/storage/local", "internal/plugins/storage") {
		t.Fatal("a subtree must be allowed")
	}
	if dirAllowed("internal/plugins/storagex", "internal/plugins/storage") {
		t.Fatal("a lookalike prefix must not be allowed")
	}
	if dirAllowed("internal/plugins/assembly", "internal/plugins/protection") {
		t.Fatal("an unrelated package must not be allowed")
	}
}

func TestPackagePredicates(t *testing.T) {
	if !isLoader(fixtureModule + "/internal/loader") {
		t.Fatal("isLoader must accept the loader")
	}
	if isLoader(fixtureModule + "/internal/plugins/loader") {
		t.Fatal("isLoader must refuse another loader")
	}
	if !isInstances(fixtureModule + "/internal/plugins/instances") {
		t.Fatal("isInstances must accept instances")
	}
	if isInstances(fixtureModule + "/internal/plugins/instance") {
		t.Fatal("isInstances must refuse a lookalike")
	}
	if !inPlugins("internal/plugins/thing") || inPlugins("internal/loader") {
		t.Fatal("inPlugins must bound the plugin tree")
	}
	if !isKitDir("internal/plugins/source/sourcetest") || isKitDir("internal/plugins/source") {
		t.Fatal("isKitDir must name conformance kits only")
	}
	if !isHostDir("internal/plugins/cli") || !isHostDir("internal/plugins/cli/sub") || isHostDir("internal/plugins/thing") {
		t.Fatal("isHostDir must name the host package and its subtree only")
	}
}

func TestCommandPackage(t *testing.T) {
	for _, dir := range []string{"internal/plugins/cli", "internal/plugins/cli/run"} {
		if !isCommandPackage(dir) {
			t.Fatalf("%s must be the command layer", dir)
		}
	}
	for _, dir := range []string{"internal/plugins/thing", "internal/plugins/cliextra"} {
		if isCommandPackage(dir) {
			t.Fatalf("%s must not be the command layer", dir)
		}
	}
}

func TestUnwrapGenerics(t *testing.T) {
	if _, ok := unwrapGenerics(parseExpr(t, "NewKind[T]")).(*ast.Ident); !ok {
		t.Fatal("an index expression must unwrap to its function")
	}
	if _, ok := unwrapGenerics(parseExpr(t, "NewKind[T, U]")).(*ast.Ident); !ok {
		t.Fatal("an index list expression must unwrap to its function")
	}
	if _, ok := unwrapGenerics(parseExpr(t, "NewKind")).(*ast.Ident); !ok {
		t.Fatal("an uninstantiated function must pass through")
	}
}

// TestScanAcceptsGenericMethods proves the parser sees the Go version the
// compiler accepts, and that test-only initializers are not shipped code.
func TestScanAcceptsGenericMethods(t *testing.T) {
	scanned := mustScan(t, map[string]string{
		"internal/plugins/library/plugin.go": `package library

import "example.test/app/internal/loader"

var Spec = loader.Spec{ID: "library", Summary: "a library"}

type item struct{}

func (i *item) Convert[A any](value A) A { return value }
`,
		"internal/plugins/library/plugin_test.go": `package library

import "regexp"

var re = regexp.MustCompile("x")
`,
	})
	if findings := ruleEagerInit(scanned, nil); len(findings) != 0 {
		t.Fatalf("findings = %v", findings)
	}
}
