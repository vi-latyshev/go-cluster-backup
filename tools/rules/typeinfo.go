package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"slices"
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

// platform is one GOOS/GOARCH build the type pass models. The list is the two
// shipped architectures and the development host; a file that no listed
// platform builds is a model gap, not a silently skipped file.
type platform struct{ goos, goarch string }

func (p platform) String() string { return p.goos + "/" + p.goarch }

// platforms are the builds the type pass models.
var platforms = []platform{
	{"linux", "amd64"},
	{"linux", "arm64"},
	{"darwin", "arm64"},
}

// platformList renders the checked platforms for a diagnostic.
func platformList() string {
	names := make([]string, 0, len(platforms))
	for _, pl := range platforms {
		names = append(names, pl.String())
	}
	return strings.Join(names, ", ")
}

// listedPackage is the part of one `go list -json` entry the type pass reads.
type listedPackage struct {
	ImportPath, Dir string
	GoFiles         []string
	ImportMap       map[string]string
	Standard        bool
	Error           *struct{ Err string }
}

// goList runs the go command. It is the tool's only exec: the standard library
// and every module dependency resolve through the go on PATH, never through
// this binary's own GOROOT, which -trimpath leaves empty. A test overrides it
// to prove the decode failure.
var goList = func(dir string, env []string, args ...string) ([]byte, error) {
	command := exec.Command("go", args...)
	command.Dir = dir
	command.Env = append(os.Environ(), env...)
	return command.Output()
}

// listPackages lists the dependency closure of paths for one platform. Every
// import the tree makes resolves from this one listing: the standard library,
// third-party modules from the module cache, replace-directory modules and
// in-module packages outside the scanned tree.
func listPackages(root string, pl platform, paths []string) (map[string]*listedPackage, error) {
	args := append([]string{
		"list", "-e", "-deps", "-json=ImportPath,Dir,GoFiles,ImportMap,Standard,Error", "--",
	}, paths...)
	out, err := goList(root, []string{"GOOS=" + pl.goos, "GOARCH=" + pl.goarch, "CGO_ENABLED=0"}, args...)
	if err != nil {
		return nil, fmt.Errorf("go list (%s): %w", pl, err)
	}
	listed := map[string]*listedPackage{}
	for decoder := json.NewDecoder(bytes.NewReader(out)); ; {
		var entry listedPackage
		err := decoder.Decode(&entry)
		if errors.Is(err, io.EOF) {
			return listed, nil
		}
		if err != nil {
			return nil, fmt.Errorf("go list (%s): %w", pl, err)
		}
		listed[entry.ImportPath] = &entry
	}
}

// typeChecker type-checks the scanned tree for one platform. Every import takes
// exactly one path: a tree package from the tree's parsed sources, anything
// else from the go list closure. There is no stub or fake fallback: an import
// the closure cannot supply is reported to errs, and check refuses the run.
type typeChecker struct {
	tree     *pluginmeta.Tree
	fset     *token.FileSet
	pl       platform
	listed   map[string]*listedPackage
	byPath   map[string]*packageTypes
	external map[string]*types.Package
	active   map[string]bool
	errs     *[]string
}

// newTypeChecker builds a checker over one listed platform closure.
func newTypeChecker(tree *pluginmeta.Tree, pl platform, listed map[string]*listedPackage, errs *[]string) *typeChecker {
	return &typeChecker{
		tree:     tree,
		fset:     tree.FileSet,
		pl:       pl,
		listed:   listed,
		byPath:   map[string]*packageTypes{},
		external: map[string]*types.Package{},
		active:   map[string]bool{},
		errs:     errs,
	}
}

// checkPackage is the unprovokable failure seam: the type checker always returns
// a package unless the process is out of memory, so a test overrides it to prove
// the placeholder paths.
var checkPackage = func(config *types.Config, path string, fset *token.FileSet, files []*ast.File, info *types.Info) (*types.Package, error) {
	return config.Check(path, fset, files, info)
}

// config builds the checker's type configuration. Every type error is
// collected: the rule functions still judge the partial model they can see
// (unit tests call them on deliberately incorrect fixtures), while check()
// refuses a tree whose model has any error.
func (c *typeChecker) config() *types.Config {
	return &types.Config{
		Importer: c,
		Sizes:    types.SizesFor("gc", c.pl.goarch),
		Error: func(err error) {
			*c.errs = append(*c.errs, c.pl.String()+": "+err.Error())
		},
	}
}

// selected returns the files of p this platform builds, as go list named them.
func (c *typeChecker) selected(p *pluginmeta.Package) []*pluginmeta.File {
	listed := c.listed[p.Path]
	if listed == nil {
		return nil
	}
	var out []*pluginmeta.File
	for _, f := range p.Files {
		if slices.Contains(listed.GoFiles, path.Base(f.Rel)) {
			out = append(out, f)
		}
	}
	return out
}

// typesFor returns the checked view of one package, checking it on first use.
// A go list load error on a package that has files to check is a model gap:
// the partial view is still built for the rule functions, but check refuses
// the run. A package no listed platform builds is not a gap here — its files
// are judged by the completeness check instead.
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
	selected := c.selected(p)
	if listed := c.listed[p.Path]; listed != nil && listed.Error != nil && len(selected) > 0 {
		*c.errs = append(*c.errs, fmt.Sprintf("%s: %s: %s", c.pl, p.Path, listed.Error.Err))
	}
	files := make([]*ast.File, 0, len(selected))
	for _, f := range selected {
		files = append(files, f.AST)
	}
	checked, _ := checkPackage(c.config(), p.Path, c.fset, files, info)
	delete(c.active, p.Path)
	if checked == nil {
		return placeholder
	}
	placeholder.pkg = checked
	return placeholder
}

// Import satisfies types.Importer for the checker's own configuration. A tree
// package that is still being checked is an import cycle: Go has no cyclic
// imports, so the model is incomplete and the error reaches the error sink.
func (c *typeChecker) Import(importPath string) (*types.Package, error) {
	if p, ok := c.tree.ByPath[importPath]; ok {
		if c.active[p.Path] {
			return nil, fmt.Errorf("%s: import cycle", importPath)
		}
		return c.typesFor(p).pkg, nil
	}
	return c.importListed(importPath)
}

// stdlib caches the standard packages per platform so every fixture shares one
// type universe per platform instead of re-checking the library per module.
// The mutex guards the map only: checking a package recurses through Import,
// so holding it across a check would deadlock.
var stdlib = &stdlibCache{
	pkgs: map[platform]map[string]*types.Package{},
	fset: token.NewFileSet(),
}

// stdlibCache is the process-wide standard library cache.
type stdlibCache struct {
	mu   sync.Mutex
	pkgs map[platform]map[string]*types.Package
	fset *token.FileSet
}

// get returns one cached standard package.
func (s *stdlibCache) get(pl platform, importPath string) (*types.Package, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	checked, ok := s.pkgs[pl][importPath]
	return checked, ok
}

// put stores one checked standard package, or returns the one a concurrent
// check stored first, so every importer of a package sees the same type object.
func (s *stdlibCache) put(pl platform, importPath string, checked *types.Package) *types.Package {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pkgs[pl] == nil {
		s.pkgs[pl] = map[string]*types.Package{}
	}
	if existing, ok := s.pkgs[pl][importPath]; ok {
		return existing
	}
	s.pkgs[pl][importPath] = checked
	return checked
}

// importListed resolves one import from the go list closure.
func (c *typeChecker) importListed(importPath string) (*types.Package, error) {
	if importPath == "unsafe" {
		return types.Unsafe, nil
	}
	listed := c.listed[importPath]
	if listed == nil {
		return nil, fmt.Errorf("%s is not in the go list closure", importPath)
	}
	if listed.Error != nil {
		return nil, errors.New(listed.Error.Err)
	}
	fset := c.fset
	if listed.Standard {
		fset = stdlib.fset
		if checked, ok := stdlib.get(c.pl, importPath); ok {
			return checked, nil
		}
	} else if checked, ok := c.external[importPath]; ok {
		return checked, nil
	}
	files := make([]*ast.File, 0, len(listed.GoFiles))
	for _, name := range listed.GoFiles {
		file, err := parser.ParseFile(fset, filepath.Join(listed.Dir, name), nil, parser.SkipObjectResolution)
		if err != nil {
			return nil, err
		}
		files = append(files, file)
	}
	config := c.config()
	config.Importer = importerFunc(func(dependency string) (*types.Package, error) {
		if mapped, ok := listed.ImportMap[dependency]; ok {
			dependency = mapped
		}
		return c.Import(dependency)
	})
	checked, _ := checkPackage(config, importPath, fset, files, nil)
	if checked == nil {
		checked = types.NewPackage(importPath, "")
	}
	if listed.Standard {
		return stdlib.put(c.pl, importPath, checked), nil
	}
	c.external[importPath] = checked
	return checked, nil
}

// importerFunc adapts a function to types.Importer.
type importerFunc func(string) (*types.Package, error)

// Import satisfies types.Importer.
func (f importerFunc) Import(importPath string) (*types.Package, error) { return f(importPath) }
