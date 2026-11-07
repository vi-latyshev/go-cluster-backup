// Package pluginmeta is the one metadata reader the tools share: it walks a
// module's production packages, resolves imports and parses the plugin
// protocol — Kind, loader.Spec and Apply. tools/gen and tools/rules consume it;
// neither keeps a private copy of these rules.
package pluginmeta

import (
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
)

// Tree is one module's parsed packages.
type Tree struct {
	Root     string
	Module   string
	FileSet  *token.FileSet      // positions of every parsed node
	Packages []*Package          // sorted by Dir
	ByPath   map[string]*Package // import path -> package
	ByDir    map[string]*Package // module-relative slash dir -> package
}

// Position maps a node of this tree to its file and line, for diagnostics that
// name where a rule was broken.
func (t *Tree) Position(node ast.Node) token.Position {
	return t.FileSet.Position(node.Pos())
}

// Package is one production Go package.
type Package struct {
	Path     string  // import path
	Dir      string  // module-relative slash dir
	Name     string  // real package name from the package clause
	Files    []*File // sorted by Rel
	Types    map[string]struct{}
	Consts   map[string]string // string-valued constants only
	Kind     string            // instances.NewKind name, "" when none
	KindDecl ast.Expr          // the declaration expression, nil when none
	KindFile *File             // file declaring Kind, nil when none
	Spec     *Spec             // nil when the package declares no literal Spec
	Apply    *Apply            // nil when the package declares no Apply
	Problems []Problem         // every protocol violation, in file order

	specDecl   *specDecl
	applyDecls []applyDecl
}

// specDecl keeps the Spec composite literal beside the file that declares it,
// because the ID expression resolves aliases against that file's imports.
type specDecl struct {
	file *File
	lit  *ast.CompositeLit
}

// applyDecl keeps one top-level Apply declaration beside its file for import
// resolution.
type applyDecl struct {
	file *File
	decl *ast.FuncDecl
}

// File is one parsed production file: imports resolve per file, as Go scopes
// them.
type File struct {
	Rel     string // module-relative slash path
	AST     *ast.File
	Imports map[string]string // local name -> import path
	Paths   []string          // every imported path, blank/dot included
}

// Spec is the resolved loader.Spec of one plugin package.
type Spec struct {
	File     string // module-relative slash path of the literal
	ID       string // the resolved id, as the generator records it
	Summary  string
	Floor    bool
	Requires []string
	After    []string
}

// ApplyKind is the shape of a plugin's Apply function.
type ApplyKind int

const (
	ApplyNone    ApplyKind = iota
	ApplyPlugin            // Apply(*loader.Registry) error
	ApplyFactory           // Apply(*loader.Registry, *instances.Registry) error
)

// Apply is the resolved Apply declaration of one package.
type Apply struct {
	Kind ApplyKind
	File string // module-relative slash path
}

// Problem is one protocol violation. The consumers prefix their own location
// and decide whether it refuses the operation.
type Problem struct {
	File string // module-relative slash path, "" for a package-level problem
	Msg  string
}

// walkDir and relPath are the unprovokable filesystem failure seams: the tree
// the walker visits always exists and lies under the root the caller passed, so
// a test overrides them to prove the error paths.
var (
	walkDir = filepath.WalkDir
	relPath = filepath.Rel
)

// ModulePath returns the module directive of root/go.mod.
func ModulePath(root string) (string, error) {
	data, err := os.ReadFile(filepath.Join(root, "go.mod"))
	if err != nil {
		return "", fmt.Errorf("read go.mod: %w", err)
	}
	for _, line := range strings.Split(string(data), "\n") {
		rest, ok := strings.CutPrefix(strings.TrimSpace(line), "module ")
		if !ok {
			continue
		}
		p := strings.Trim(strings.TrimSpace(rest), `"`)
		if p == "" {
			break
		}
		return p, nil
	}
	return "", errors.New("go.mod has no module directive")
}

// Read walks root/internal and root/cmd (a missing cmd is not an error) and
// parses every production Go file. Packages are sorted by module-relative dir.
func Read(root string) (*Tree, error) {
	module, err := ModulePath(root)
	if err != nil {
		return nil, err
	}
	tree := &Tree{
		Root:    root,
		Module:  module,
		FileSet: token.NewFileSet(),
		ByPath:  map[string]*Package{},
		ByDir:   map[string]*Package{},
	}
	for _, top := range []string{"internal", "cmd"} {
		topDir := filepath.Join(root, top)
		if _, err := os.Stat(topDir); err != nil {
			if top == "cmd" && errors.Is(err, fs.ErrNotExist) {
				continue
			}
			return nil, fmt.Errorf("read %s: %w", topDir, err)
		}
		if err := tree.walk(topDir, top); err != nil {
			return nil, err
		}
	}
	tree.finish()
	return tree, nil
}

// compositionDir is the generated composition root: a tracked output, never a
// metadata source. The walk skips it so a stale or corrupt generated file can be
// regenerated instead of breaking the read.
const compositionDir = "internal/plugins/all"

// skipDir names directories the reader does not enter: test data, vendored
// copies and editor noise. A conformance kit is a normal package here; each
// consumer decides whether a kit is a plugin.
func skipDir(name string) bool {
	return name == "testdata" || name == "vendor" ||
		strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_")
}

// walk parses every production file under one top-level directory.
func (t *Tree) walk(topDir, top string) error {
	return walkDir(topDir, func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if name != topDir {
				rel, err := relPath(t.Root, name)
				if err == nil && filepath.ToSlash(rel) == compositionDir {
					return fs.SkipDir
				}
				if skipDir(entry.Name()) {
					return fs.SkipDir
				}
			}
			return nil
		}
		base := entry.Name()
		if strings.HasPrefix(base, "_") || strings.HasPrefix(base, ".") ||
			!strings.HasSuffix(base, ".go") || strings.HasSuffix(base, "_test.go") {
			return nil
		}
		return t.addFile(name)
	})
}

// addFile parses one production file into the package its directory names.
func (t *Tree) addFile(name string) error {
	rel, err := relPath(t.Root, name)
	if err != nil {
		return fmt.Errorf("relative path of %s: %w", name, err)
	}
	rel = filepath.ToSlash(rel)
	dir := path.Dir(rel)
	tree, err := parser.ParseFile(t.FileSet, name, nil, parser.SkipObjectResolution)
	if err != nil {
		return fmt.Errorf("parse %s: %w", rel, err)
	}
	p := t.ByDir[dir]
	if p == nil {
		p = &Package{
			Path:   t.Module + "/" + dir,
			Dir:    dir,
			Name:   tree.Name.Name,
			Types:  map[string]struct{}{},
			Consts: map[string]string{},
		}
		t.ByDir[dir] = p
		t.Packages = append(t.Packages, p)
	} else if p.Name != tree.Name.Name {
		return fmt.Errorf("%s: more than one package (%s, %s)", dir, p.Name, tree.Name.Name)
	}
	p.Files = append(p.Files, &File{
		Rel:   rel,
		AST:   tree,
		Paths: importPaths(tree),
	})
	return nil
}

// finish resolves the declarations that need the whole package set: import
// names first, then every Kind, Spec and Apply with the package map in scope.
func (t *Tree) finish() {
	sort.Slice(t.Packages, func(i, j int) bool { return t.Packages[i].Dir < t.Packages[j].Dir })
	names := make(map[string]string, len(t.Packages))
	for _, p := range t.Packages {
		t.ByPath[p.Path] = p
		names[p.Path] = p.Name
	}
	for _, p := range t.Packages {
		for _, f := range p.Files {
			f.Imports = importNames(f.AST, names)
			p.collect(f)
		}
	}
	for _, p := range t.Packages {
		if p.KindDecl != nil {
			name, err := kindName(p.KindDecl, p.KindFile)
			if err != nil {
				p.problems(p.KindFile, "Kind: %v", err)
				continue
			}
			p.Kind = name
		}
	}
	for _, p := range t.Packages {
		if p.specDecl != nil {
			p.resolveSpec(t)
		}
	}
	for _, p := range t.Packages {
		for _, decl := range p.applyDecls {
			if p.Apply != nil {
				p.problems(decl.file, "package %s declares Apply twice", p.Path)
				continue
			}
			kind, err := applyKindOf(decl.file, decl.decl)
			if err != nil {
				p.problems(decl.file, "%v", err)
				continue
			}
			p.Apply = &Apply{Kind: kind, File: decl.file.Rel}
		}
	}
}

// problems records one violation with its file location.
func (p *Package) problems(f *File, format string, args ...any) {
	problem := Problem{Msg: fmt.Sprintf(format, args...)}
	if f != nil {
		problem.File = f.Rel
	}
	p.Problems = append(p.Problems, problem)
}

// importNames maps the name each file binds an import to onto its path. An
// explicit alias wins, then the imported package's real name from names, then
// the path base. The real name matters where a plugin package and a standard
// package share a base: internal/plugins/errors is package pluginerrors, so a
// file may import it beside the standard errors package without a collision. A
// blank or dot import binds no name, so only the declarations that use an alias
// matter.
func importNames(parsed *ast.File, names map[string]string) map[string]string {
	out := make(map[string]string, len(parsed.Imports))
	for _, spec := range parsed.Imports {
		imported := stringValue(spec.Path)
		alias := path.Base(imported)
		if spec.Name != nil {
			alias = spec.Name.Name
			if alias == "_" || alias == "." {
				continue
			}
		} else if real, ok := names[imported]; ok {
			alias = real
		}
		// Two imports under one name cannot compile, so the alias is ambiguous
		// only while a file is mid-edit: keep the first and let the build report
		// the real error instead of failing the whole scan.
		if _, duplicate := out[alias]; !duplicate {
			out[alias] = imported
		}
	}
	return out
}

// importPaths lists every imported path, including a blank or dot import: the
// dependency graph and the boundary rules read paths, not aliases.
func importPaths(parsed *ast.File) []string {
	out := make([]string, 0, len(parsed.Imports))
	for _, spec := range parsed.Imports {
		out = append(out, stringValue(spec.Path))
	}
	return out
}

// collect records the package-level declarations the protocol reads.
func (p *Package) collect(f *File) {
	for _, decl := range f.AST.Decls {
		switch decl := decl.(type) {
		case *ast.GenDecl:
			p.collectGenDecl(f, decl)
		case *ast.FuncDecl:
			if decl.Recv == nil && decl.Name.Name == "Apply" {
				p.applyDecls = append(p.applyDecls, applyDecl{file: f, decl: decl})
			}
		}
	}
}

// collectGenDecl records types, string constants and the two protocol
// declarations a package may hold: Kind and the loader.Spec literal.
func (p *Package) collectGenDecl(f *File, decl *ast.GenDecl) {
	switch decl.Tok {
	case token.TYPE:
		for _, spec := range decl.Specs {
			p.Types[spec.(*ast.TypeSpec).Name.Name] = struct{}{}
		}
	case token.CONST:
		for _, spec := range decl.Specs {
			value := spec.(*ast.ValueSpec)
			for i, name := range value.Names {
				if i >= len(value.Values) {
					continue
				}
				if literal, ok := StringLiteral(value.Values[i]); ok {
					p.Consts[name.Name] = literal
				}
			}
		}
	case token.VAR:
		for _, spec := range decl.Specs {
			value := spec.(*ast.ValueSpec)
			for i, name := range value.Names {
				if i >= len(value.Values) {
					continue
				}
				switch name.Name {
				case "Kind":
					if p.KindDecl != nil {
						p.problems(f, "package %s declares Kind twice", p.Path)
						continue
					}
					p.KindDecl, p.KindFile = value.Values[i], f
				case "Spec":
					p.collectSpec(f, value.Values[i])
				}
			}
		}
	}
}

// collectSpec records the loader.Spec composite literal of one declaration.
func (p *Package) collectSpec(f *File, value ast.Expr) {
	literal, ok := value.(*ast.CompositeLit)
	if !ok {
		p.problems(f, "Spec must be a loader.Spec composite literal")
		return
	}
	if p.specDecl != nil {
		p.problems(f, "package %s declares Spec twice", p.Path)
		return
	}
	p.specDecl = &specDecl{file: f, lit: literal}
}

// resolveSpec reads one loader.Spec literal into typed fields, recording a
// problem for every form the generator refuses or the rules cannot check.
func (p *Package) resolveSpec(t *Tree) {
	decl := p.specDecl
	spec := &Spec{File: decl.file.Rel}
	p.Spec = spec
	selector, ok := decl.lit.Type.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != "Spec" {
		p.problems(decl.file, "Spec must be loader.Spec")
		return
	}
	alias, ok := selector.X.(*ast.Ident)
	if !ok || !isLoader(decl.file.Imports[alias.Name]) {
		p.problems(decl.file, "Spec must be loader.Spec")
		return
	}
	seen := map[string]bool{}
	for _, element := range decl.lit.Elts {
		field, ok := element.(*ast.KeyValueExpr)
		if !ok {
			p.problems(decl.file, "Spec must use field names")
			continue
		}
		key, ok := field.Key.(*ast.Ident)
		if !ok {
			p.problems(decl.file, "Spec field name is not an identifier")
			continue
		}
		if seen[key.Name] {
			p.problems(decl.file, "Spec field %s is set twice", key.Name)
			continue
		}
		seen[key.Name] = true
		var err error
		switch key.Name {
		case "ID":
			spec.ID, err = specID(decl.file, field.Value, t)
		case "Summary":
			var ok bool
			if spec.Summary, ok = StringLiteral(field.Value); !ok {
				err = errors.New("must be a string literal")
			}
		case "Floor":
			var ok bool
			if spec.Floor, ok = BoolLiteral(field.Value); !ok {
				err = errors.New("must be true or false")
			}
		case "Requires":
			spec.Requires, err = stringList(field.Value)
		case "After":
			spec.After, err = stringList(field.Value)
		default:
			err = fmt.Errorf("unknown Spec field %s", key.Name)
		}
		if err != nil {
			p.problems(decl.file, "Spec.%s: %v", key.Name, err)
		}
	}
	if !seen["ID"] || spec.ID == "" {
		p.problems(decl.file, "Spec.ID is required")
	}
}

// specID evaluates the one derivation of a plugin id: a literal for a
// non-factory plugin, instances.ID(kind, type) for a factory.
func specID(f *File, expr ast.Expr, t *Tree) (string, error) {
	if literal, ok := StringLiteral(expr); ok {
		return literal, nil
	}
	call, ok := expr.(*ast.CallExpr)
	if !ok {
		return "", errors.New("must be a string literal or instances.ID(...)")
	}
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != "ID" {
		return "", errors.New("must be a string literal or instances.ID(...)")
	}
	alias, ok := selector.X.(*ast.Ident)
	if !ok || !isInstances(f.Imports[alias.Name]) {
		return "", errors.New("ID must come from the instances package")
	}
	if len(call.Args) != 2 {
		return "", errors.New("instances.ID takes a kind and a type")
	}
	typ, ok := StringLiteral(call.Args[1])
	if !ok {
		return "", errors.New("type: must be a string literal")
	}
	kind, err := kindArgument(f, call.Args[0], t)
	if err != nil {
		return "", fmt.Errorf("kind: %w", err)
	}
	return kind + "/" + typ, nil
}

// kindArgument evaluates the kind argument of instances.ID: a literal or
// `<pkg>.Kind.Name()`, where the imported package's Kind declares the name.
func kindArgument(f *File, expr ast.Expr, t *Tree) (string, error) {
	if literal, ok := StringLiteral(expr); ok {
		return literal, nil
	}
	call, ok := expr.(*ast.CallExpr)
	if !ok || len(call.Args) != 0 {
		return "", errors.New("must be a string literal or <pkg>.Kind.Name()")
	}
	name, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || name.Sel.Name != "Name" {
		return "", errors.New("must be a string literal or <pkg>.Kind.Name()")
	}
	kind, ok := name.X.(*ast.SelectorExpr)
	if !ok || kind.Sel.Name != "Kind" {
		return "", errors.New("must be a string literal or <pkg>.Kind.Name()")
	}
	alias, ok := kind.X.(*ast.Ident)
	if !ok {
		return "", errors.New("must be a string literal or <pkg>.Kind.Name()")
	}
	target := t.ByPath[f.Imports[alias.Name]]
	if target == nil {
		return "", fmt.Errorf("%s does not name a package under internal/plugins", alias.Name)
	}
	if target.Kind == "" {
		return "", fmt.Errorf("%s declares no Kind", target.Dir)
	}
	return target.Kind, nil
}

// kindName extracts the name from `instances.NewKind[T]("<name>")`.
func kindName(expr ast.Expr, f *File) (string, error) {
	call, ok := expr.(*ast.CallExpr)
	if !ok {
		return "", errors.New("must be a instances.NewKind call")
	}
	var fun ast.Expr
	switch fn := call.Fun.(type) {
	case *ast.IndexExpr:
		fun = fn.X
	case *ast.IndexListExpr:
		fun = fn.X
	default:
		return "", errors.New("must be a instances.NewKind call")
	}
	selector, ok := fun.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != "NewKind" {
		return "", errors.New("must be a instances.NewKind call")
	}
	ident, ok := selector.X.(*ast.Ident)
	if !ok || !isInstances(f.Imports[ident.Name]) {
		return "", errors.New("NewKind must come from the instances package")
	}
	if len(call.Args) != 1 {
		return "", errors.New("NewKind takes exactly one argument")
	}
	value, ok := StringLiteral(call.Args[0])
	if !ok {
		return "", errors.New("must be a string literal")
	}
	return value, nil
}

// applyKindOf validates the Apply signature the protocol defines: the loader
// registry first, an optional factory registry second, one error result.
func applyKindOf(f *File, decl *ast.FuncDecl) (ApplyKind, error) {
	params := decl.Type.Params
	if params == nil || len(params.List) == 0 {
		return ApplyNone, errors.New("Apply must take a *loader.Registry")
	}
	count := 0
	for _, field := range params.List {
		if len(field.Names) == 0 {
			count++
			continue
		}
		count += len(field.Names)
	}
	if !isRegistry(f, params.List[0].Type, isLoader, "Registry") {
		return ApplyNone, errors.New("first parameter must be *loader.Registry")
	}
	switch count {
	case 1:
	case 2:
		if len(params.List) < 2 || !isRegistry(f, params.List[1].Type, isInstances, "Registry") {
			return ApplyNone, errors.New("second parameter must be *instances.Registry")
		}
	default:
		return ApplyNone, fmt.Errorf("Apply takes one or two parameters, not %d", count)
	}
	results := decl.Type.Results
	if results == nil || len(results.List) != 1 {
		return ApplyNone, errors.New("Apply must return error")
	}
	if ident, ok := results.List[0].Type.(*ast.Ident); !ok || ident.Name != "error" {
		return ApplyNone, errors.New("Apply must return error")
	}
	if count == 2 {
		return ApplyFactory, nil
	}
	return ApplyPlugin, nil
}

// isRegistry reports whether expr is *<pkg>.<name>, with pkg accepted by check.
func isRegistry(f *File, expr ast.Expr, check func(string) bool, name string) bool {
	star, ok := expr.(*ast.StarExpr)
	if !ok {
		return false
	}
	selector, ok := star.X.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != name {
		return false
	}
	ident, ok := selector.X.(*ast.Ident)
	if !ok {
		return false
	}
	imported, ok := f.Imports[ident.Name]
	if !ok {
		return false
	}
	return check(imported)
}

// isLoader reports whether an import path is the loader package.
func isLoader(importPath string) bool {
	return strings.HasSuffix("/"+importPath, "/internal/loader")
}

// isInstances reports whether an import path is the instances package.
func isInstances(importPath string) bool {
	return strings.HasSuffix("/"+importPath, "/internal/plugins/instances")
}
