package main

import (
	"errors"
	"strings"

	"github.com/vi-latyshev/go-cluster-backup/tools/internal/pluginmeta"
	"go/ast"
	"go/token"
	"go/types"
	"testing"
)

// TestTypeCheckerSeams proves the impossible-checker paths: a checker that
// returns no package leaves a usable placeholder instead of a panic.
func TestTypeCheckerSeams(t *testing.T) {
	scanned := mustScan(t, map[string]string{
		"internal/plugins/alternative/write.go": "package alternative\n\nfunc f() {}\n",
	})
	restore := checkPackage
	t.Cleanup(func() { checkPackage = restore })
	checkPackage = func(*types.Config, string, *token.FileSet, []*ast.File, *types.Info) (*types.Package, error) {
		return nil, errors.New("boom")
	}
	checker := newTypeChecker(scanned.Tree)
	if got := checker.typesFor(scanned.Packages[0]); got.pkg == nil {
		t.Fatal("a failed check must keep a placeholder package")
	}
	if got, err := checker.stub("example.test/stub", "package stub\n"); err != nil || got == nil {
		t.Fatalf("stub = %v, %v", got, err)
	}
}

// TestStubParseError proves a malformed canonical stub is an error, not a guess.
func TestStubParseError(t *testing.T) {
	checker := newTypeChecker(&pluginmeta.Tree{FileSet: token.NewFileSet()})
	if _, err := checker.stub("example.test/broken", "package broken func"); err == nil {
		t.Fatal("a malformed stub must fail")
	}
}

// TestImportStdlib covers the shared source importer: one unknown path error
// and the cached success path.
func TestImportStdlib(t *testing.T) {
	if _, err := importStdlib("example.test/no-such-package"); err == nil {
		t.Fatal("an unknown import must fail")
	}
	first, err := importStdlib("fmt")
	if err != nil {
		t.Fatalf("fmt: %v", err)
	}
	second, err := importStdlib("fmt")
	if err != nil || first != second {
		t.Fatalf("cached fmt = %v, %v", second, err)
	}
}

// TestStubSourceSuffixes proves every canonical boundary package is recognized
// and an unrelated path is not.
func TestStubSourceSuffixes(t *testing.T) {
	for _, path := range []string{
		"example.test/app/internal/loader",
		"example.test/app/internal/plugins/instances",
		"example.test/app/internal/plugins/protection",
		"example.test/app/internal/plugins/storage/guard",
		"example.test/app/internal/plugins/storage",
		"example.test/app/internal/plugins/library",
	} {
		if _, ok := stubSource(path); !ok {
			t.Fatalf("stubSource(%q) = false", path)
		}
	}
	if _, ok := stubSource("example.test/app/internal/plugins/model"); ok {
		t.Fatal("an unrelated path must not resolve to a stub")
	}
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
	info := newTypeChecker(scanned.Tree).typesFor(scanned.Packages[0]).info
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
