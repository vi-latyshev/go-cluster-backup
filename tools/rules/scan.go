package main

import (
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"

	"github.com/vi-latyshev/go-cluster-backup/tools/internal/pluginmeta"
)

// Paths the rules name. They are module-relative and slash-separated.
const (
	loaderDir           = "internal/loader"
	pluginsDir          = "internal/plugins"
	cliPluginDir        = "internal/plugins/cli"
	generatedFile       = "internal/plugins/all/components/zz_generated.go"
	compositionRootFile = "internal/plugins/all/zz_generated.go"
	compositionDir      = "internal/plugins/all"
	componentsDir       = "internal/plugins/all/components"
	defaultsDataDir     = "internal/plugins/configuration/defaults"
	storageOwnerDir     = "internal/plugins/storage"
	guardDir            = "internal/plugins/storage/guard"
	rawStorageDir       = "internal/plugins/storage/internal"
	assemblyDir         = "internal/plugins/assembly"
	protectionDir       = "internal/plugins/protection"
)

// repo is the rules' policy view: the shared metadata tree plus the one tracked
// output that tree deliberately skips, the generated composition root.
type repo struct {
	*pluginmeta.Tree

	// authority caches the one go/types pass both mutation rules share.
	authority          []string
	checkedAuthorities bool
}

// scan reads the module's metadata through the one shared reader. Test files
// never enter the tree: the rules describe the shipped binary, and a fixture
// string in a test is data, not a dependency.
func scan(root string) (*repo, error) {
	tree, err := pluginmeta.Read(root)
	if err != nil {
		return nil, err
	}
	return &repo{Tree: tree}, nil
}

// generated is the tracked composition root's parsed view: the package imports
// and the loader.Imports graph it installs.
type generated struct {
	missing           []string
	rootImports       []string
	componentsImports []string
	imports           []string
	edges             map[string][]string
	root              *ast.File
	components        *ast.File
	rootMissing       bool
	componentsMissing bool
}

// generated reads both halves of the tracked composition root: the hostless
// components half carries the loader.SetImports graph, the root half registers
// the hosts. Missing files are findings of the import-coverage rule; a syntax
// or graph error stops the scan.
func (r *repo) generated() (*generated, error) {
	out := &generated{edges: map[string][]string{}}
	var err error
	out.components, out.componentsMissing, err = r.parseGenerated(generatedFile, &out.componentsImports)
	if err != nil {
		return nil, err
	}
	out.root, out.rootMissing, err = r.parseGenerated(compositionRootFile, &out.rootImports)
	if err != nil {
		return nil, err
	}
	if out.componentsMissing {
		out.missing = append(out.missing, generatedFile)
	}
	if out.rootMissing {
		out.missing = append(out.missing, compositionRootFile)
	}
	out.imports = append(append([]string(nil), out.rootImports...), out.componentsImports...)
	if err := out.readGraph(); err != nil {
		return nil, err
	}
	return out, nil
}

// readGraph finds the one loader.SetImports call across both halves. The
// components half carries it in the current layout; the scan accepts either
// half but never two.
func (g *generated) readGraph() error {
	found := false
	var (
		owner    string
		graphErr error
	)
	scan := func(relative string, parsed *ast.File) {
		if parsed == nil || graphErr != nil {
			return
		}
		ast.Inspect(parsed, func(node ast.Node) bool {
			if graphErr != nil {
				return false
			}
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || selector.Sel.Name != "SetImports" {
				return true
			}
			if found {
				graphErr = errors.New("the generated composition calls loader.SetImports twice")
				return false
			}
			found = true
			owner = relative
			if len(call.Args) != 2 {
				graphErr = errors.New("the generated loader.SetImports call takes one graph")
				return false
			}
			g.edges, graphErr = importGraph(call.Args[1])
			return true
		})
	}
	scan(generatedFile, g.components)
	scan(compositionRootFile, g.root)
	if graphErr != nil {
		return fmt.Errorf("%s: %w", owner, graphErr)
	}
	if !found && !g.componentsMissing && !g.rootMissing {
		return fmt.Errorf("%s: no loader.SetImports call", generatedFile)
	}
	return nil
}

// parseGenerated parses one tracked generated file and records its imports. A
// missing file is returned as nil with missing set; any other parse failure is
// an error naming the file.
func (r *repo) parseGenerated(relative string, imports *[]string) (*ast.File, bool, error) {
	name := filepath.Join(r.Root, filepath.FromSlash(relative))
	parsed, err := parser.ParseFile(token.NewFileSet(), name, nil, parser.SkipObjectResolution)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, true, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("parse %s: %w", relative, err)
	}
	for _, spec := range parsed.Imports {
		importPath, _ := pluginmeta.StringLiteral(spec.Path)
		*imports = append(*imports, importPath)
	}
	return parsed, false, nil
}

// importGraph reads a `loader.Imports{...}` composite literal.
func importGraph(expr ast.Expr) (map[string][]string, error) {
	literal, ok := expr.(*ast.CompositeLit)
	if !ok {
		return nil, errors.New("the generated import graph is not a loader.Imports literal")
	}
	out := make(map[string][]string, len(literal.Elts))
	for _, element := range literal.Elts {
		field, ok := element.(*ast.KeyValueExpr)
		if !ok {
			return nil, errors.New("the generated import graph must use plugin ids as keys")
		}
		id, ok := pluginmeta.StringLiteral(field.Key)
		if !ok {
			return nil, errors.New("the generated import graph has a non-literal plugin id")
		}
		list, ok := field.Value.(*ast.CompositeLit)
		if !ok {
			return nil, fmt.Errorf("the generated dependencies of %q are not a []string literal", id)
		}
		for _, dependency := range list.Elts {
			dep, ok := pluginmeta.StringLiteral(dependency)
			if !ok {
				return nil, fmt.Errorf("the generated dependencies of %q are not string literals", id)
			}
			out[id] = append(out[id], dep)
		}
	}
	return out, nil
}

// pluginPkgs lists the packages that declare a plugin Spec, in directory order.
func (r *repo) pluginPkgs() []*pluginmeta.Package {
	out := make([]*pluginmeta.Package, 0, len(r.Packages))
	for _, p := range r.Packages {
		if p.Spec != nil {
			out = append(out, p)
		}
	}
	return out
}

// kindNames is the set of kind names the module declares.
func (r *repo) kindNames() map[string]struct{} {
	out := map[string]struct{}{}
	for _, p := range r.Packages {
		if p.Kind != "" {
			out[p.Kind] = struct{}{}
		}
	}
	return out
}

// roleNames is the set of process entrypoints: every `Role*` string constant an
// owner declares, and every RoleSpec.Name that resolves to a literal.
func (r *repo) roleNames() map[string]struct{} {
	out := map[string]struct{}{}
	for _, p := range r.Packages {
		for name, value := range p.Consts {
			if strings.HasPrefix(name, "Role") {
				out[value] = struct{}{}
			}
		}
		for _, f := range p.Files {
			inspectDecls(f, func(node ast.Node) bool {
				literal, ok := node.(*ast.CompositeLit)
				if !ok {
					return true
				}
				if !r.isRoleSpec(f, literal) {
					return true
				}
				for _, element := range literal.Elts {
					field, ok := element.(*ast.KeyValueExpr)
					if !ok {
						continue
					}
					if key, ok := field.Key.(*ast.Ident); !ok || key.Name != "Name" {
						continue
					}
					if name, ok := r.stringExpr(p, f, field.Value); ok {
						out[name] = struct{}{}
					}
				}
				return true
			})
		}
	}
	return out
}

// isRoleSpec reports whether a composite literal is a loader.RoleSpec.
func (r *repo) isRoleSpec(f *pluginmeta.File, literal *ast.CompositeLit) bool {
	selector, ok := literal.Type.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != "RoleSpec" {
		return false
	}
	alias, ok := selector.X.(*ast.Ident)
	return ok && isLoader(f.Imports[alias.Name])
}

// stringExpr resolves a string expression the rules can see statically: a
// literal, a package constant, or another module package's constant.
func (r *repo) stringExpr(p *pluginmeta.Package, f *pluginmeta.File, expr ast.Expr) (string, bool) {
	if literal, ok := pluginmeta.StringLiteral(expr); ok {
		return literal, true
	}
	switch value := expr.(type) {
	case *ast.Ident:
		out, ok := p.Consts[value.Name]
		return out, ok
	case *ast.SelectorExpr:
		alias, ok := value.X.(*ast.Ident)
		if !ok {
			return "", false
		}
		target := r.ByPath[f.Imports[alias.Name]]
		if target == nil {
			return "", false
		}
		out, ok := target.Consts[value.Sel.Name]
		return out, ok
	default:
		return "", false
	}
}

// commandNames is the set of CLI commands: the string cases of the command
// dispatch switch in the command layer.
func (r *repo) commandNames() map[string]struct{} {
	out := map[string]struct{}{}
	for _, p := range r.Packages {
		if !isCommandPackage(p.Dir) {
			continue
		}
		for _, f := range p.Files {
			inspectDecls(f, func(node ast.Node) bool {
				statement, ok := node.(*ast.SwitchStmt)
				if !ok {
					return true
				}
				tag, ok := statement.Tag.(*ast.Ident)
				if !ok || !commandTagNames[tag.Name] {
					return true
				}
				for _, clause := range statement.Body.List {
					for _, expr := range clause.(*ast.CaseClause).List {
						if name, ok := pluginmeta.StringLiteral(expr); ok {
							out[name] = struct{}{}
						}
					}
				}
				return true
			})
		}
	}
	return out
}

// commandTagNames are the switch tags the command layer dispatches on.
var commandTagNames = map[string]bool{"command": true, "cmd": true, "subcommand": true}

// isCommandPackage reports whether a directory belongs to the command layer.
func isCommandPackage(dir string) bool {
	return dir == cliPluginDir || strings.HasPrefix(dir, cliPluginDir+"/")
}

// isKitDir reports whether a directory is a conformance kit rather than a
// shipped plugin: the generator skips these from every composition.
func isKitDir(dir string) bool {
	for _, segment := range strings.Split(dir, "/") {
		if strings.HasSuffix(segment, "test") {
			return true
		}
	}
	return false
}

// inPlugins reports whether a package lives under internal/plugins.
func inPlugins(dir string) bool {
	return strings.HasPrefix(dir, pluginsDir+"/")
}

// importable reports whether Go's internal rule lets importerDir import
// targetDir: every "internal" path element confines its subtree to the tree
// rooted at its parent.
func importable(importerDir, targetDir string) bool {
	segments := strings.Split(targetDir, "/")
	for i, segment := range segments {
		if segment != "internal" {
			continue
		}
		parent := strings.Join(segments[:i], "/")
		if parent == "" {
			continue
		}
		if importerDir != parent && !strings.HasPrefix(importerDir, parent+"/") {
			return false
		}
	}
	return true
}

// dirAllowed reports whether a package directory may use a restricted symbol:
// an exact package or a subtree.
func dirAllowed(dir string, allowed ...string) bool {
	for _, prefix := range allowed {
		if dir == prefix || strings.HasPrefix(dir, prefix+"/") {
			return true
		}
	}
	return false
}

// isLoader reports whether an import path is the loader package.
func isLoader(importPath string) bool {
	return strings.HasSuffix("/"+importPath, "/"+loaderDir)
}

// isInstances reports whether an import path is the instances package.
func isInstances(importPath string) bool {
	return strings.HasSuffix("/"+importPath, "/internal/plugins/instances")
}

// line is the one-based diagnostics position of a node in this module.
func (r *repo) line(node ast.Node) int {
	return r.Position(node).Line
}

// inspectDecls walks the declarations of one file, skipping imports and struct
// tags: an import string is a package path, and a tag is data, so neither can be
// a feature name the rules judge.
func inspectDecls(f *pluginmeta.File, visit func(ast.Node) bool) {
	for _, decl := range f.AST.Decls {
		if gen, ok := decl.(*ast.GenDecl); ok && gen.Tok == token.IMPORT {
			continue
		}
		ast.Inspect(decl, func(node ast.Node) bool {
			if field, ok := node.(*ast.Field); ok {
				field.Tag = nil
			}
			return visit(node)
		})
	}
}

// unwrapGenerics peels the type-argument brackets off an instantiated function
// expression: `NewKind[T]` reads as `NewKind`.
func unwrapGenerics(expr ast.Expr) ast.Expr {
	switch typed := expr.(type) {
	case *ast.IndexExpr:
		return typed.X
	case *ast.IndexListExpr:
		return typed.X
	default:
		return expr
	}
}
