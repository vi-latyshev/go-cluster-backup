package main

import (
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"strings"
	"sync"

	"github.com/vi-latyshev/go-cluster-backup/tools/internal/pluginmeta"
)

// packageTypes is one package's go/types view: the checked package and the
// expression, object and selection tables the mutation rule reads.
type packageTypes struct {
	pkg  *types.Package
	info *types.Info
}

// typeChecker type-checks the scanned tree with a fake importer: an in-module
// import resolves from the scanned sources, a GOROOT import from the standard
// library source, and the fixture boundary packages that synthetic modules omit
// from canonical stub source. Packages that fail to resolve keep whatever the
// checker learned; the rule never falls back to a hand-written type model.
type typeChecker struct {
	tree   *pluginmeta.Tree
	fset   *token.FileSet
	stubs  map[string]*types.Package
	byPath map[string]*packageTypes
	active map[string]bool
}

// newTypeChecker builds a checker over one scanned tree.
func newTypeChecker(tree *pluginmeta.Tree) *typeChecker {
	return &typeChecker{
		tree:   tree,
		fset:   tree.FileSet,
		stubs:  map[string]*types.Package{},
		byPath: map[string]*packageTypes{},
		active: map[string]bool{},
	}
}

// checkPackage is the unprovokable failure seam: the type checker always returns
// a package unless the process is out of memory, so a test overrides it to prove
// the placeholder paths.
var checkPackage = func(config *types.Config, path string, fset *token.FileSet, files []*ast.File, info *types.Info) (*types.Package, error) {
	return config.Check(path, fset, files, info)
}

// typesFor returns the checked view of one package, checking it on first use.
func (c *typeChecker) typesFor(p *pluginmeta.Package) *packageTypes {
	if checked, ok := c.byPath[p.Path]; ok {
		return checked
	}
	info := &types.Info{
		Types:      map[ast.Expr]types.TypeAndValue{},
		Defs:       map[*ast.Ident]types.Object{},
		Uses:       map[*ast.Ident]types.Object{},
		Selections: map[*ast.SelectorExpr]*types.Selection{},
	}
	placeholder := &packageTypes{pkg: types.NewPackage(p.Path, p.Name), info: info}
	c.byPath[p.Path] = placeholder
	c.active[p.Path] = true
	files := make([]*ast.File, 0, len(p.Files))
	for _, f := range p.Files {
		files = append(files, f.AST)
	}
	config := types.Config{Importer: c, Error: func(error) {}}
	checked, _ := checkPackage(&config, p.Path, c.fset, files, info)
	delete(c.active, p.Path)
	if checked == nil {
		return placeholder
	}
	placeholder.pkg = checked
	return placeholder
}

// Import satisfies types.Importer for the checker's own configuration.
func (c *typeChecker) Import(path string) (*types.Package, error) {
	if p, ok := c.tree.ByPath[path]; ok {
		return c.typesFor(p).pkg, nil
	}
	if source, ok := stubSource(path); ok {
		return c.stub(path, source)
	}
	return importStdlib(path)
}

// stub type-checks one canonical boundary package that a synthetic fixture does
// not carry itself.
func (c *typeChecker) stub(path, source string) (*types.Package, error) {
	if checked, ok := c.stubs[path]; ok {
		return checked, nil
	}
	file, err := parser.ParseFile(c.fset, "stub:"+path, source, parser.SkipObjectResolution)
	if err != nil {
		return nil, err
	}
	config := types.Config{Importer: c, Error: func(error) {}}
	checked, _ := checkPackage(&config, path, c.fset, []*ast.File{file}, nil)
	if checked == nil {
		checked = types.NewPackage(path, "")
	}
	c.stubs[path] = checked
	return checked, nil
}

// stdlib is the process-wide source importer for GOROOT packages. Compiling the
// standard library from source once per test binary keeps every fixture module
// loadable without a shared build cache.
var stdlib = struct {
	mu   sync.Mutex
	imp  types.Importer
	seen map[string]*types.Package
}{
	imp:  importer.ForCompiler(token.NewFileSet(), "source", nil),
	seen: map[string]*types.Package{},
}

// importStdlib imports one GOROOT package through the shared source importer.
func importStdlib(path string) (*types.Package, error) {
	stdlib.mu.Lock()
	defer stdlib.mu.Unlock()
	if checked, ok := stdlib.seen[path]; ok {
		return checked, nil
	}
	checked, err := stdlib.imp.Import(path)
	if err != nil {
		return nil, err
	}
	stdlib.seen[path] = checked
	return checked, nil
}

// Boundary stub sources: the packages a synthetic fixture imports but rarely
// carries. They model exactly the declarations the fixtures use and import
// nothing, so type checking stays self-contained.
const (
	loaderStub = `package loader

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

// stubSource names the canonical source of one boundary package.
func stubSource(path string) (string, bool) {
	switch {
	case isLoader(path):
		return loaderStub, true
	case isInstances(path):
		return instancesStub, true
	case strings.HasSuffix("/"+path, "/internal/plugins/protection"):
		return protectionStub, true
	case strings.HasSuffix("/"+path, "/internal/plugins/storage/guard"):
		return guardStub, true
	case strings.HasSuffix("/"+path, "/internal/plugins/storage"):
		return storageStub, true
	case strings.HasSuffix("/"+path, "/internal/plugins/library"):
		return libraryStub, true
	default:
		return "", false
	}
}
