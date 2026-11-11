package main

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"strings"

	"github.com/vi-latyshev/go-cluster-backup/tools/internal/pluginmeta"
	"go/ast"
	"go/token"
	"go/types"
	"testing"
)

// TestTypeCheckerSeams proves the impossible-checker path: a checker that
// returns no package leaves a usable placeholder instead of a panic.
func TestTypeCheckerSeams(t *testing.T) {
	scanned := mustScan(t, map[string]string{
		"internal/plugins/alternative/write.go": "package alternative\n\nfunc f() {}\n",
	})
	checker := fixtureChecker(t, scanned.Tree)
	restore := checkPackage
	t.Cleanup(func() { checkPackage = restore })
	checkPackage = func(*types.Config, string, *token.FileSet, []*ast.File, *types.Info) (*types.Package, error) {
		return nil, errors.New("boom")
	}
	if got := checker.typesFor(scanned.Packages[0]); got.pkg == nil {
		t.Fatal("a failed check must keep a placeholder package")
	}
}

// rulesChildEnv marks the rerun of a test with the environment the CI shape has.
const rulesChildEnv = "RULES_TEST_CHILD"

// TestImportStdlib covers the one resolver: an import outside the go list
// closure, the cached standard package, and a child process with GOROOT removed
// from the environment — the CI shape, where a binary built with -trimpath has
// no GOROOT but still finds the standard library through the go on PATH.
func TestImportStdlib(t *testing.T) {
	if os.Getenv(rulesChildEnv) == "" {
		command := exec.Command(os.Args[0], "-test.run=^TestImportStdlib$")
		command.Env = append(withoutGOROOT(os.Environ()), rulesChildEnv+"=1")
		if out, err := command.CombinedOutput(); err != nil {
			t.Fatalf("rerun without GOROOT: %v\n%s", err, out)
		}
	}
	scanned := mustScan(t, map[string]string{
		"internal/plugins/alternative/a.go": "package alternative\n\nimport _ \"fmt\"\n",
	})
	checker := fixtureChecker(t, scanned.Tree)
	if _, err := checker.Import("example.test/no-such-package"); err == nil {
		t.Fatal("an unknown import must fail")
	}
	first, err := checker.Import("fmt")
	if err != nil {
		t.Fatalf("fmt: %v", err)
	}
	second, err := checker.Import("fmt")
	if err != nil || first != second {
		t.Fatalf("cached fmt = %v, %v", second, err)
	}
}

// withoutGOROOT drops the GOROOT entries from a process environment.
func withoutGOROOT(env []string) []string {
	out := make([]string, 0, len(env))
	for _, entry := range env {
		if strings.HasPrefix(entry, "GOROOT=") {
			continue
		}
		out = append(out, entry)
	}
	return out
}

// TestMutationPredicates covers the nil and default shapes of the type
// predicates so an unknown type never becomes a finding.
func TestMutationPredicates(t *testing.T) {
	if isNamedIn(nil, "/x", "Y") || isNamedIn(types.Typ[types.Int], "/x", "Y") {
		t.Fatal("a non-named or nil type must not match")
	}
	if holdsType(nil, ledgerNamed) || holdsType(types.Typ[types.Int], ledgerNamed) {
		t.Fatal("an unrelated type must not hold the ledger")
	}
	if isIssuerInterface(nil) || isIssuerInterface(types.Typ[types.Int]) || hasAuthorizeMethod(nil) {
		t.Fatal("nil and non-interface types carry no issuer")
	}
}

// TestLocateSeam proves the relative-path seam degrades to the raw filename
// instead of failing a diagnostic.
func TestLocateSeam(t *testing.T) {
	restore := locateRel
	locateRel = func(string, string) (string, error) { return "", errors.New("boom") }
	t.Cleanup(func() { locateRel = restore })
	scanned := mustScan(t, map[string]string{"internal/plugins/alternative/write.go": "package alternative\n"})
	if file, _ := scanned.locate(token.Pos(1)); file == "" {
		t.Fatal("locate must fall back to the filename")
	}
}

// TestAssertionToUnrelatedTypeIsClean proves an assertion to an ordinary type is
// not an authority issuer.
func TestAssertionToUnrelatedTypeIsClean(t *testing.T) {
	scanned := mustScan(t, map[string]string{
		"internal/plugins/alternative/write.go": `package alternative

func f(a any) { _ = a.(string) }
`,
	})
	if findings := ruleLeakedMutations(scanned, nil); len(findings) != 0 {
		t.Fatalf("findings = %v", findings)
	}
}

// TestAssertionAndArrayOfRemover proves an assertion to a Remover and an array
// that holds one are both refused, covering the array containment branch.
func TestAssertionAndArrayOfRemover(t *testing.T) {
	scanned := mustScan(t, map[string]string{
		"internal/plugins/alternative/write.go": `package alternative

import "example.test/app/internal/plugins/storage/guard"

var array [2]guard.Remover

func f(a any) {
	_ = a.(guard.Remover)
	_ = a.(guard.Destination)
}
`,
	})
	findings := ruleLeakedMutations(scanned, nil)
	wantFinding(t, findings, "only protection, assembly and the Floor may hold guard.Remover")
	wantFinding(t, findings, "guard.Destination is restricted to internal/plugins/assembly and internal/plugins/protection")
	if len(findings) != 3 {
		t.Fatalf("findings = %v", findings)
	}
}

// TestHostHoldsItsBuildResult proves the CLI host may hold the destination a
// guard.Build returns (it keeps only the port) but may not remove through it.
func TestHostHoldsItsBuildResult(t *testing.T) {
	scanned := mustScan(t, map[string]string{
		"internal/plugins/cli/doctor.go": `package cli

import "example.test/app/internal/plugins/storage/guard"

func doctor(dest guard.Destination, a any) {
	_ = dest.Port()
	_ = dest
	_ = a.(guard.Destination)
}
`,
	})
	if findings := ruleLeakedMutations(scanned, nil); len(findings) != 0 {
		t.Fatalf("findings = %v", findings)
	}
}

// TestDeclaringReceiverInterface proves an interface method selection resolves to
// its declared receiver, like every other compiler-resolved method.
func TestDeclaringReceiverInterface(t *testing.T) {
	scanned := mustScan(t, map[string]string{
		"internal/plugins/alternative/write.go": `package alternative

import "time"

func f(i interface{ Authorize(time.Time) }) { _ = i.Authorize }
`,
	})
	info := fixtureChecker(t, scanned.Tree).typesFor(scanned.Packages[0]).info
	if len(info.Selections) != 1 {
		t.Fatalf("selections = %d", len(info.Selections))
	}
	for _, selected := range info.Selections {
		if declaringReceiver(selected) != selected.Recv() {
			t.Fatalf("declaringReceiver = %v, want %v", declaringReceiver(selected), selected.Recv())
		}
	}
}

// TestPrivateFieldIsReachableWithinItsPackage proves a private ledger field is
// selectable by its own package, so an aggregate local of that package is a
// holder too: both the field type and the aggregate are refused.
func TestPrivateFieldIsReachableWithinItsPackage(t *testing.T) {
	scanned := mustScan(t, map[string]string{
		"internal/plugins/alternative/write.go": `package alternative

import "example.test/app/internal/plugins/protection"

type hidden struct{ ledger *protection.Ledger }

func hold(h hidden) { _ = h }
`,
	})
	findings := ruleLeakedMutations(scanned, nil)
	if len(findings) != 2 {
		t.Fatalf("findings = %v", findings)
	}
	wantFinding(t, findings, "only protection, assembly and the Floor may hold protection.Ledger")
}

// TestPrivateFieldIsOpaqueToAnotherPackage proves the compiler's lookup keeps a
// private named field unreachable across packages: only the declaring package's
// type expression is refused, the consumer aggregate is not.
func TestPrivateFieldIsOpaqueToAnotherPackage(t *testing.T) {
	scanned := mustScan(t, map[string]string{
		"internal/plugins/other/holder.go": `package other

import "example.test/app/internal/plugins/protection"

type Hidden struct{ ledger *protection.Ledger }
`,
		"internal/plugins/alternative/write.go": `package alternative

import "example.test/app/internal/plugins/other"

func hold(h other.Hidden) { _ = h }
`,
	})
	findings := ruleLeakedMutations(scanned, nil)
	if len(findings) != 1 || strings.Contains(findings[0], "alternative/write.go") {
		t.Fatalf("findings = %v", findings)
	}
}

// TestAmbiguousPromotionIsOpaque proves an ambiguous promoted field is not
// selectable, so the consumer aggregate stays clean.
func TestAmbiguousPromotionIsOpaque(t *testing.T) {
	scanned := mustScan(t, map[string]string{
		"internal/plugins/other/holder.go": `package other

import "example.test/app/internal/plugins/protection"

type first struct{ Ledger *protection.Ledger }
type second struct{ Ledger *protection.Ledger }

type Ambiguous struct {
	first
	second
}
`,
		"internal/plugins/alternative/write.go": `package alternative

import "example.test/app/internal/plugins/other"

func hold(a other.Ambiguous) { _ = a }
`,
	})
	for _, finding := range ruleLeakedMutations(scanned, nil) {
		if strings.Contains(finding, "alternative/write.go") {
			t.Fatalf("an ambiguous promotion must not be reachable: %q", finding)
		}
	}
}

// TestRecursiveContainerIsFinite runs the recursive container in-process (the
// reviewer probe runs it in a stack-limited subprocess) so the traversal's
// visited-set branch is exercised directly.
func TestRecursiveContainerIsFinite(t *testing.T) {
	scanned := mustScan(t, map[string]string{
		"internal/plugins/library/node.go": "package library\n\ntype Node []Node\n",
	})
	if findings := ruleRestrictedSymbols(scanned, nil); len(findings) != 0 {
		t.Fatalf("findings = %v", findings)
	}
}

// TestAccessibilityEdgeShapes proves the compiler-lookup branches: a shallower
// method shadowing a promoted field name and an embedded non-struct are not
// mutation holders, while a self-pointer embedding stays finite.
func TestAccessibilityEdgeShapes(t *testing.T) {
	t.Run("method shadows promoted field", func(t *testing.T) {
		scanned := mustScan(t, map[string]string{
			"internal/plugins/other/holder.go": `package other

import "example.test/app/internal/plugins/protection"

type inner struct{ Ledger *protection.Ledger }

type Outer struct{ inner }

func (Outer) Ledger() {}
`,
			"internal/plugins/alternative/write.go": `package alternative

import "example.test/app/internal/plugins/other"

func hold(o other.Outer) { _ = o.Ledger() }
`,
		})
		for _, finding := range ruleLeakedMutations(scanned, nil) {
			if strings.Contains(finding, "alternative/write.go") {
				t.Fatalf("a shadowing method is not a field holder: %q", finding)
			}
		}
	})
	t.Run("embedded non-struct", func(t *testing.T) {
		scanned := mustScan(t, map[string]string{
			"internal/plugins/other/holder.go": `package other

type Embedded int

type WithBasic struct{ Embedded }
`,
			"internal/plugins/alternative/write.go": `package alternative

import "example.test/app/internal/plugins/other"

func hold(w other.WithBasic) { _ = w }
`,
		})
		if findings := ruleLeakedMutations(scanned, nil); len(findings) != 0 {
			t.Fatalf("findings = %v", findings)
		}
	})
	t.Run("self-pointer embedding", func(t *testing.T) {
		scanned := mustScan(t, map[string]string{
			"internal/plugins/other/holder.go": `package other

import "example.test/app/internal/plugins/protection"

type Node struct {
	*Node
	Ledger *protection.Ledger
}
`,
			"internal/plugins/alternative/write.go": `package alternative

import "example.test/app/internal/plugins/other"

func hold(n other.Node) { _ = n }
`,
		})
		wantFinding(t, ruleLeakedMutations(scanned, nil), "alternative/write.go")
	})
	structFieldNames(nil, map[string]struct{}{}, map[types.Type]bool{})
}

// TestThirdPartyModuleLeak proves a mutation value that reaches a consumer
// through a replace-directory module type argument is still found: the
// dependency is resolved from the go list closure, not from a stub.
func TestThirdPartyModuleLeak(t *testing.T) {
	scanned := mustScan(t, map[string]string{
		"go.mod": `module example.test/app

go 1.27.4

require example.test/dep v0.0.0

replace example.test/dep => ./third_party/dep
`,
		"third_party/dep/go.mod": "module example.test/dep\n\ngo 1.27.4\n",
		"third_party/dep/box.go": `package dep

type Box[T any] struct{ V T }

func (b *Box[T]) Get() T { return b.V }
`,
		"internal/plugins/other/holder.go": `package other

import (
	"example.test/app/internal/plugins/protection"
	"example.test/dep"
)

var Shared dep.Box[*protection.Ledger]
`,
		"internal/plugins/alternative/write.go": `package alternative

import "example.test/app/internal/plugins/other"

func f() { _ = other.Shared.Get() }
`,
	})
	wantFinding(t, ruleLeakedMutations(scanned, nil), "alternative/write.go")
}

// TestUnresolvedImportRefusesCheck proves an import no module supplies stops
// the command instead of leaving the rule to judge a partial model.
func TestUnresolvedImportRefusesCheck(t *testing.T) {
	root := moduleTree(t, map[string]string{
		"internal/plugins/alternative/write.go": "package alternative\n\nimport _ \"example.test/missing\"\n",
	})
	_, err := check(root)
	wantError(t, err, "example.test/missing")
}

// TestGoListDecodeFailure proves a go list answer that is not JSON is a model
// error, not a silently empty closure.
func TestGoListDecodeFailure(t *testing.T) {
	restore := goList
	t.Cleanup(func() { goList = restore })
	goList = func(string, []string, ...string) ([]byte, error) { return []byte("{"), nil }
	if _, err := listPackages(t.TempDir(), platforms[0], nil); err == nil {
		t.Fatal("a malformed go list answer must fail")
	} else if !strings.Contains(err.Error(), "go list (linux/amd64)") {
		t.Fatalf("err = %v", err)
	}
}

// TestPlatformFileSelection proves the type pass builds a package per platform:
// a linux/darwin pair of the same declaration is not a redeclaration, while a
// mutation value only the darwin file declares is still found.
func TestPlatformFileSelection(t *testing.T) {
	t.Run("paired files", func(t *testing.T) {
		findings := checkTree(t, checkedTree(map[string]string{
			"internal/plugins/library/key_linux.go":  "package library\n\nfunc key() int { return 1 }\n",
			"internal/plugins/library/key_darwin.go": "package library\n\nfunc key() int { return 2 }\n",
		}))
		if len(findings) != 0 {
			t.Fatalf("findings = %v", findings)
		}
	})
	t.Run("darwin-only mutation value", func(t *testing.T) {
		findings := checkTree(t, checkedTree(map[string]string{
			"internal/plugins/library/key_linux.go": "package library\n",
			"internal/plugins/library/key_darwin.go": `package library

import "example.test/app/internal/plugins/protection"

var Shared *protection.Ledger
`,
		}))
		wantFinding(t, findings, "internal/plugins/library/key_darwin.go")
	})
}

// TestExcludedFileIsAModelError proves a production file no checked platform
// builds refuses the tree by name: no rule ever judges it.
func TestExcludedFileIsAModelError(t *testing.T) {
	root := writeTree(t, checkedTree(map[string]string{
		"internal/plugins/library/key_windows.go": "package library\n",
	}))
	_, err := check(root)
	wantError(t, err, "internal/plugins/library/key_windows.go: excluded by build constraints on every checked platform ("+platformList()+")")
}

// TestImportListedSeams proves the resolver's unprovoked branches: an entry the
// closure omits, a concurrent store that loses, a dependency the listing
// remaps, an unparseable dependency source and a checker that returns no
// package.
func TestImportListedSeams(t *testing.T) {
	t.Run("missing closure entry", func(t *testing.T) {
		var errs []string
		checker := newTypeChecker(&pluginmeta.Tree{FileSet: token.NewFileSet()}, platforms[0], nil, &errs)
		if files := checker.selected(&pluginmeta.Package{Path: "example.test/missing"}); files != nil {
			t.Fatalf("selected = %v", files)
		}
		if _, err := checker.Import("example.test/missing"); err == nil {
			t.Fatal("an import outside the closure must fail")
		}
	})
	t.Run("concurrent store keeps the first", func(t *testing.T) {
		pl := platform{"plan9", "arm"}
		first := types.NewPackage("example.test/first", "first")
		second := types.NewPackage("example.test/second", "second")
		if got := stdlib.put(pl, "example.test/first", first); got != first {
			t.Fatalf("put = %v", got)
		}
		if got := stdlib.put(pl, "example.test/first", second); got != first {
			t.Fatalf("second put = %v, want the first", got)
		}
	})
	t.Run("import map", func(t *testing.T) {
		root := writeTree(t, map[string]string{
			"go.mod":      "module example.test/remapped\n\ngo 1.27.4\n",
			"remapped.go": "package remapped\n\nimport _ \"original\"\n",
		})
		listed := map[string]*listedPackage{
			"example.test/remapped": {
				ImportPath: "example.test/remapped",
				Dir:        root,
				GoFiles:    []string{"remapped.go"},
				ImportMap:  map[string]string{"original": "unsafe"},
			},
		}
		var errs []string
		checker := newTypeChecker(&pluginmeta.Tree{FileSet: token.NewFileSet()}, platforms[0], listed, &errs)
		checked, err := checker.Import("example.test/remapped")
		if err != nil || checked == nil || checked.Name() != "remapped" {
			t.Fatalf("remapped = %v, %v", checked, err)
		}
	})
	t.Run("unparseable dependency", func(t *testing.T) {
		root := writeTree(t, map[string]string{
			"go.mod":    "module example.test/broken\n\ngo 1.27.4\n",
			"broken.go": "package broken func",
		})
		listed := map[string]*listedPackage{
			"example.test/broken": {ImportPath: "example.test/broken", Dir: root, GoFiles: []string{"broken.go"}},
		}
		var errs []string
		checker := newTypeChecker(&pluginmeta.Tree{FileSet: token.NewFileSet()}, platforms[0], listed, &errs)
		if _, err := checker.Import("example.test/broken"); err == nil {
			t.Fatal("an unparseable dependency must fail")
		}
	})
	t.Run("checker without a package", func(t *testing.T) {
		root := writeTree(t, map[string]string{
			"go.mod":   "module example.test/empty\n\ngo 1.27.4\n",
			"empty.go": "package empty\n",
		})
		listed := map[string]*listedPackage{
			"example.test/empty": {ImportPath: "example.test/empty", Dir: root, GoFiles: []string{"empty.go"}},
		}
		var errs []string
		checker := newTypeChecker(&pluginmeta.Tree{FileSet: token.NewFileSet()}, platforms[0], listed, &errs)
		restore := checkPackage
		t.Cleanup(func() { checkPackage = restore })
		checkPackage = func(*types.Config, string, *token.FileSet, []*ast.File, *types.Info) (*types.Package, error) {
			return nil, errors.New("boom")
		}
		checked, err := checker.Import("example.test/empty")
		if err != nil || checked == nil {
			t.Fatalf("placeholder = %v, %v", checked, err)
		}
	})
}

// TestR1_ModelRejectsSelfImport is the reviewer's probe for M1: a tree package
// that imports itself gets a go list load error, and the command must refuse
// the incomplete model instead of checking a half-built placeholder.
func TestR1_ModelRejectsSelfImport(t *testing.T) {
	root := writeTree(t, checkedTree(map[string]string{
		"internal/plugins/library/cycle.go": "package library\n\nimport _ \"example.test/app/internal/plugins/library\"\n",
	}))
	var stdout, stderr bytes.Buffer
	code := run([]string{"-root", root}, &stdout, &stderr)
	if code != 1 || !strings.Contains(stderr.String(), "the type model is incomplete") || !strings.Contains(stderr.String(), "import cycle") {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
}

// TestR1_ModelRejectsDistinctStructTags is the reviewer's probe for M2: the
// rule walk must not strip struct tags from the AST, or two struct types with
// different tags become identical and a real assignment error disappears.
func TestR1_ModelRejectsDistinctStructTags(t *testing.T) {
	root := writeTree(t, checkedTree(map[string]string{
		"internal/plugins/library/tags.go": "package library\nfunc f() {\nvar a struct { Value int `json:\"a\"` }\nvar b struct { Value int `json:\"b\"` }\na = b\n_ = a\n}\n",
	}))
	var stdout, stderr bytes.Buffer
	code := run([]string{"-root", root}, &stdout, &stderr)
	if code != 1 || !strings.Contains(stderr.String(), "the type model is incomplete") {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
}

// TestR1_ModelPreservesIdenticalStructTags is the M2 false-refusal half: a tree
// anonymous struct literal and an external dependency struct that carry the
// same tag are assignable, so the call type-checks on every checked platform.
func TestR1_ModelPreservesIdenticalStructTags(t *testing.T) {
	root := writeTree(t, checkedTree(map[string]string{
		"go.mod": `module example.test/app

go 1.27.4

require example.test/dep v0.0.0

replace example.test/dep => ./third_party/dep
`,
		"third_party/dep/go.mod":     "module example.test/dep\n\ngo 1.27.4\n",
		"third_party/dep/payload.go": "package dep\n\ntype Payload struct {\n\tValue int `json:\"value\"`\n}\n\nfunc Accept(Payload) {}\n",
		"internal/fixture/consumer/use.go": `package consumer

import "example.test/dep"

func f() { dep.Accept(struct {
	Value int ` + "`json:\"value\"`" + `
}{}) }
`,
	}))
	if findings, err := check(root); err != nil || len(findings) != 0 {
		t.Fatalf("findings=%v err=%v", findings, err)
	}
}
