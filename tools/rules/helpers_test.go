package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/tools/internal/pluginmeta"
)

const fixtureModule = "example.test/app"

// healthyTree is one module that satisfies every rule: a kind package, a factory
// backend that imports it, a library plugin, the CLI command layer and the
// generated composition root that ties them together.
func healthyTree() map[string]string {
	return map[string]string{
		"go.mod":                    "module " + fixtureModule + "\n\ngo 1.27.4\n",
		"internal/loader/loader.go": loaderStub,
		"internal/plugins/thing/thing.go": `package thing

import "example.test/app/internal/plugins/instances"

// Thing is the seam this package owns.
type Thing interface{ Do() }

// Kind is the thing seam.
var Kind = instances.NewKind[Thing]("thing")
`,
		"internal/plugins/thing/plugin.go": `package thing

import "example.test/app/internal/loader"

// Spec is the compiled identity of the thing kinds.
var Spec = loader.Spec{ID: "thing", Summary: "the thing kind"}
`,
		"internal/plugins/thing/backend/plugin.go": `package backend

import (
	"example.test/app/internal/loader"
	"example.test/app/internal/plugins/instances"
	"example.test/app/internal/plugins/thing"
)

// Spec is the compiled identity of the backend.
var Spec = loader.Spec{ID: instances.ID(thing.Kind.Name(), "backend"), Summary: "a backend"}

// Apply contributes the backend.
func Apply(_ *loader.Registry, _ *instances.Registry) error { return nil }
`,
		"internal/plugins/library/plugin.go": `package library

import "example.test/app/internal/loader"

// Spec is the compiled identity of the library.
var Spec = loader.Spec{ID: "library", Summary: "a library"}
`,
		"internal/plugins/cli/plugin.go": `package cli

import "example.test/app/internal/loader"

// Spec is the compiled identity of the CLI host.
var Spec = loader.Spec{ID: "cli", Summary: "the command host", Floor: true}
`,
		"internal/plugins/cli/cli.go": `package cli

import "example.test/app/internal/loader"

// Role names are the process entrypoints.
const (
	RoleCLI = "cli"
	RoleJob = "backup-job"
)

// Roles is the compiled role table.
func Roles() []loader.RoleSpec {
	return []loader.RoleSpec{{
		Name:     RoleCLI,
		Default:  true,
		Run:      func(loader.Session) int { return 0 },
		Disabled: func(loader.Session) int { return 1 },
	}}
}

// Main dispatches one command.
func Main(command string) int {
	switch command {
	case "run", "help":
		return 0
	default:
		return 2
	}
}
`,
		"internal/plugins/all/zz_generated.go": `package all

import (
	"example.test/app/internal/loader"
	"example.test/app/internal/plugins/all/components"
	"example.test/app/internal/plugins/cli"
	"example.test/app/internal/plugins/instances"
)

// Register installs every compiled plugin over one fresh factory registry.
func Register(r *loader.Registry) error {
	return Compose(r, instances.NewRegistry())
}

// Compose installs the hostless half, then each host.
func Compose(r *loader.Registry, factories *instances.Registry) error {
	if err := components.Compose(r, factories); err != nil {
		return err
	}
	return loader.Add(r, cli.Spec, func(lr *loader.Registry) error { return cli.Apply(lr, factories) })
}
`,
		"internal/plugins/all/components/zz_generated.go": `package components

import (
	"example.test/app/internal/loader"
	"example.test/app/internal/plugins/instances"
	"example.test/app/internal/plugins/library"
	"example.test/app/internal/plugins/thing"
	"example.test/app/internal/plugins/thing/backend"
)

// Compose installs every hostless plugin.
func Compose(r *loader.Registry, factories *instances.Registry) error {
	if err := loader.SetImports(r, loader.Imports{"thing/backend": {"thing"}}); err != nil {
		return err
	}
	_ = library.Spec
	_ = thing.Spec
	return loader.Add(r, backend.Spec, func(lr *loader.Registry) error { return backend.Apply(lr, factories) })
}
`,
	}
}

// writeTree materialises one fixture module in a temp directory.
func writeTree(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	for name, content := range files {
		target := filepath.Join(root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			t.Fatalf("create %s: %v", filepath.Dir(target), err)
		}
		if err := os.WriteFile(target, []byte(content), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	if _, ok := files["go.mod"]; ok {
		addStubModules(t, root, files)
	}
	return root
}

// stubSources are the canonical boundary packages a synthetic fixture imports
// but rarely carries, keyed by module-relative dir. The type model resolves
// each one like any module: addStubModules writes it as a separate module
// required and replaced by directory.
var stubSources = map[string]string{
	"internal/loader":                loaderStub,
	"internal/plugins/instances":     instancesStub,
	"internal/plugins/protection":    protectionStub,
	"internal/plugins/storage/guard": guardStub,
	"internal/plugins/storage":       storageStub,
	"internal/plugins/library":       libraryStub,
}

// addStubModules supplies each canonical boundary package a fixture imports but
// does not carry as its own package, as a separate module required and replaced
// by directory. No production code resolves a boundary package any other way.
func addStubModules(t *testing.T, root string, files map[string]string) {
	t.Helper()
	stubs := t.TempDir()
	var mod strings.Builder
	for _, dir := range slices.Sorted(maps.Keys(stubSources)) {
		importPath := fixtureModule + "/" + dir
		provided, imported := false, false
		for name, content := range files {
			if strings.HasPrefix(name, dir+"/") && !strings.Contains(strings.TrimPrefix(name, dir+"/"), "/") {
				provided = true
			}
			if strings.Contains(content, `"`+importPath+`"`) {
				imported = true
			}
		}
		if provided || !imported {
			continue
		}
		source := stubSources[dir]
		target := filepath.Join(stubs, filepath.FromSlash(dir))
		if err := os.MkdirAll(target, 0o755); err != nil {
			t.Fatalf("create stub module %s: %v", dir, err)
		}
		if err := os.WriteFile(filepath.Join(target, "go.mod"), []byte("module "+importPath+"\n\ngo 1.27.4\n"), 0o644); err != nil {
			t.Fatalf("write stub module %s: %v", dir, err)
		}
		if err := os.WriteFile(filepath.Join(target, "stub.go"), []byte(source), 0o644); err != nil {
			t.Fatalf("write stub source %s: %v", dir, err)
		}
		mod.WriteString("require " + importPath + " v0.0.0\nreplace " + importPath + " => " + target + "\n")
	}
	if mod.Len() == 0 {
		return
	}
	file, err := os.OpenFile(filepath.Join(root, "go.mod"), os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("append stub modules: %v", err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			t.Fatalf("close go.mod: %v", err)
		}
	}()
	if _, err := file.WriteString("\n" + mod.String()); err != nil {
		t.Fatalf("append stub modules: %v", err)
	}
}

// moduleTree is writeTree with the module file every fixture needs.
func moduleTree(t *testing.T, files map[string]string) string {
	t.Helper()
	all := make(map[string]string, len(files)+1)
	for name, content := range files {
		all[name] = content
	}
	if _, ok := all["go.mod"]; !ok {
		all["go.mod"] = "module " + fixtureModule + "\n\ngo 1.27.4\n"
	}
	return writeTree(t, all)
}

// checkedTree is healthyTree with files replaced, added or removed, so a
// negative case names only what it changes.
func checkedTree(overrides map[string]string) map[string]string {
	files := healthyTree()
	for name, content := range overrides {
		if content == "" {
			delete(files, name)
			continue
		}
		files[name] = content
	}
	return files
}

// checkTree runs every rule over a fixture and returns its findings.
func checkTree(t *testing.T, files map[string]string) []string {
	t.Helper()
	findings, err := check(writeTree(t, files))
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	return findings
}

// wantFinding fails the test when no finding contains the wanted text.
func wantFinding(t *testing.T, findings []string, want string) {
	t.Helper()
	for _, finding := range findings {
		if strings.Contains(finding, want) {
			return
		}
	}
	t.Fatalf("want a finding containing %q, got %v", want, findings)
}

// wantNone fails the test when any finding contains the unwanted text.
func wantNone(t *testing.T, findings []string, unwanted string) {
	t.Helper()
	for _, finding := range findings {
		if strings.Contains(finding, unwanted) {
			t.Fatalf("unexpected finding %q in %v", finding, findings)
		}
	}
}

// wantError fails the test when err is nil or does not name the cause.
func wantError(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("want an error containing %q, got nil", want)
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("want an error containing %q, got %q", want, err)
	}
}

// parseExpr parses one Go expression for a unit test.
func parseExpr(t *testing.T, source string) ast.Expr {
	t.Helper()
	expr, err := parser.ParseExpr(source)
	if err != nil {
		t.Fatalf("parse %s: %v", source, err)
	}
	return expr
}

// testRepo builds the rules' policy view for a generated-root unit test.
func testRepo(root string) *repo {
	return &repo{Tree: &pluginmeta.Tree{Root: root}}
}

// parseFile parses one Go file for a unit test.
func parseFile(t *testing.T, source string) *ast.File {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "fixture.go", source, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	return file
}

// fixtureChecker type-checks one fixture tree for the first checked platform.
// The errors are collected, not asserted: rule-level tests call the rule
// functions directly on fixtures that are deliberately incorrect in type.
func fixtureChecker(t *testing.T, tree *pluginmeta.Tree) *typeChecker {
	t.Helper()
	paths := make([]string, 0, len(tree.Packages))
	for _, p := range tree.Packages {
		paths = append(paths, p.Path)
	}
	listed, err := listPackages(tree.Root, platforms[0], paths)
	if err != nil {
		t.Fatalf("list packages: %v", err)
	}
	var errs []string
	return newTypeChecker(tree, platforms[0], listed, &errs)
}

// Boundary stub sources: the packages a synthetic fixture imports but rarely
// carries. They model exactly the declarations the fixtures use and import
// nothing, so a fixture module stays self-contained. addStubModules installs
// each one as a replaced module, so the production resolver never sees a stub.
const (
	loaderStub = `package loader

import "errors"

// ErrSomething is a loader-owned error.
var ErrSomething = errors.New("loader: something")

const separator = "/"

type Imports map[string][]string

type Spec struct {
	ID, Summary string
	Floor       bool
	Requires    []string
	After       []string
}

type Registry struct{}

type Session struct{}

type RoleSpec struct {
	Name       string
	Default    bool
	Diagnostic bool
	Run        func(Session) int
	Disabled   func(Session) int
}

func Add(*Registry, Spec, func(*Registry) error) error { return nil }

func SetImports(*Registry, Imports) error { return nil }

func Role(*Registry, RoleSpec) error { return nil }

type KindOptions[T any] struct {
	Unique, Schema bool
	Validate       func([]T) error
}

type Kind[T any] struct{}

func NewKind[T any](string, KindOptions[T]) Kind[T] { var k Kind[T]; return k }

type Service[T any] struct{}

func NewService[T any](string) Service[T] { var s Service[T]; return s }
`

	instancesStub = `package instances

type Registry struct{}

func NewRegistry() *Registry { return &Registry{} }

func ID(kind, typ string) string { return kind + "/" + typ }

type Kind[T any] struct{}

func (Kind[T]) Name() string { return "" }

func NewKind[T any](string) Kind[T] { var k Kind[T]; return k }

func RegisterProvider[T any](*Registry, string) error { return nil }
`

	protectionStub = `package protection

type Ledger struct{}

type LedgerOptions struct{}

func NewLedger(LedgerOptions) *Ledger { return nil }

func (*Ledger) Authorize(any) any { return nil }

type Accounting interface {
	Admit(string, string, int64, bool) bool
	Shrink(string, string, int64)
	Commit(string, string, int64)
	Unreserve(string, string)
}
`

	guardStub = `package guard

type Authority struct{}

type Port struct{}

type Handle struct{}

func (Port) Open(*Authority) (Handle, error) { return Handle{}, nil }

type Remover struct{}

func (Remover) Delete(any, string) error { return nil }

type Destination struct{}

func (Destination) Remover() Remover { return Remover{} }

func Build(...any) (Destination, error) { var d Destination; return d, nil }

func NewAuthority(any) *Authority { return nil }

func Read(...any) Port { return Port{} }
`

	storageStub = `package storage

type BuildInput struct{ CallParallelism int }

type Storage interface{}

func Build(...any) (Storage, error) { return nil, nil }

func Register(...any) error { return nil }

func AddFactory(...any) error { return nil }
`

	libraryStub = `package library

const RoleExtra = "extra"

var Spec struct{}

func Authorize() {}
`
)
