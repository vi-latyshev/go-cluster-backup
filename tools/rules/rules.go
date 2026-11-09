package main

import (
	"fmt"
	"go/ast"
	"go/token"
	"path"
	"sort"
	"strings"

	"github.com/vi-latyshev/go-cluster-backup/tools/internal/pluginmeta"
)

// ruleImportCoverage proves the generated composition root reflects the
// production import graph: every compiled plugin package is imported by it, and
// every production import of another plugin package is covered by the generated
// edge or by a runtime-only Spec.Requires entry.
func ruleImportCoverage(r *repo, generated *generated) []string {
	var findings []string
	for _, missing := range generated.missing {
		findings = append(findings, missing+" is missing: run 'make generate'")
	}
	imported := importSet(generated.imports)
	rootImported := importSet(generated.rootImports)
	componentsImported := importSet(generated.componentsImports)
	componentsPath := r.Module + "/" + componentsDir
	if !rootImported[componentsPath] {
		findings = append(findings, fmt.Sprintf(
			"the generated composition root does not import %s: run 'make generate'", componentsPath))
	}
	plugins := make(map[string]*pluginmeta.Package, len(r.Packages))
	for _, p := range r.pluginPkgs() {
		if p.Spec.ID != "" {
			plugins[p.Spec.ID] = p
		}
	}
	for id, p := range plugins {
		if !importable(compositionDir, p.Dir) {
			findings = append(findings, fmt.Sprintf(
				"%s is inside a Go internal boundary the composition root cannot import: it must not declare a loader.Spec",
				p.Dir))
			continue
		}
		if _, ok := imported[p.Path]; !ok {
			findings = append(findings,
				fmt.Sprintf("%s is a compiled plugin but the generated composition root does not import it: run 'make generate'", p.Dir))
		}
		effective := make(map[string]struct{}, len(generated.edges[id])+len(p.Spec.Requires))
		for _, dependency := range generated.edges[id] {
			effective[dependency] = struct{}{}
		}
		for _, dependency := range p.Spec.Requires {
			effective[dependency] = struct{}{}
		}
		for _, f := range p.Files {
			for _, importPath := range f.Paths {
				dependency := r.ByPath[importPath]
				if dependency == nil || dependency == p || dependency.Spec == nil || dependency.Spec.ID == "" {
					continue
				}
				if _, ok := effective[dependency.Spec.ID]; ok {
					continue
				}
				findings = append(findings, fmt.Sprintf(
					"%s: production import of %s is in neither the generated graph nor Spec.Requires: run 'make generate' or name it in Spec.Requires",
					f.Rel, dependency.Dir))
			}
		}
	}
	for _, p := range plugins {
		if !isHostDir(p.Dir) {
			continue
		}
		if componentsImported[p.Path] {
			findings = append(findings, fmt.Sprintf(
				"%s is a host and must not be imported by the hostless components half: run 'make generate'", p.Dir))
		}
		if !rootImported[p.Path] {
			findings = append(findings, fmt.Sprintf(
				"%s is a host but the generated composition root does not import it: run 'make generate'", p.Dir))
		}
		if !registersSpec(generated.root, p.Path) {
			findings = append(findings, fmt.Sprintf(
				"%s is a host but the generated composition root does not register it: run 'make generate'", p.Dir))
		}
	}
	for id, dependencies := range generated.edges {
		if plugins[id] == nil {
			findings = append(findings,
				fmt.Sprintf("the generated graph names plugin %q, which is not compiled: run 'make generate'", id))
		}
		for _, dependency := range dependencies {
			if plugins[dependency] == nil {
				findings = append(findings, fmt.Sprintf(
					"the generated graph makes %q depend on %q, which is not compiled: run 'make generate'", id, dependency))
			}
		}
	}
	return findings
}

// importSet indexes a generated half's imports.
func importSet(imports []string) map[string]bool {
	out := make(map[string]bool, len(imports))
	for _, importPath := range imports {
		out[importPath] = true
	}
	return out
}

// isHostDir reports whether a package is a process host, the split half the
// composition root registers itself rather than through the hostless components.
func isHostDir(dir string) bool {
	return dir == cliPluginDir || strings.HasPrefix(dir, cliPluginDir+"/")
}

// registersSpec reports whether the composition root registers the plugin the
// import path names, through its literal Spec value.
func registersSpec(root *ast.File, importPath string) bool {
	if root == nil {
		return false
	}
	aliases := make(map[string]string, len(root.Imports))
	for _, spec := range root.Imports {
		imported, _ := pluginmeta.StringLiteral(spec.Path)
		alias := path.Base(imported)
		if spec.Name != nil {
			alias = spec.Name.Name
			if alias == "_" || alias == "." {
				continue
			}
		}
		aliases[alias] = imported
	}
	found := false
	ast.Inspect(root, func(node ast.Node) bool {
		selector, ok := node.(*ast.SelectorExpr)
		if !ok || selector.Sel.Name != "Spec" {
			return true
		}
		alias, ok := selector.X.(*ast.Ident)
		if ok && aliases[alias.Name] == importPath {
			found = true
			return false
		}
		return true
	})
	return found
}

// ruleRequires proves the plugin protocol: one id per package, every Spec field
// in its literal form, and a runtime-only Requires list that names compiled
// plugins without repeating an import edge or closing a cycle.
func ruleRequires(r *repo, generated *generated) []string {
	var findings []string
	for _, p := range r.Packages {
		for _, problem := range p.Problems {
			findings = append(findings, problemLocation(p, problem))
		}
	}
	plugins := make(map[string]*pluginmeta.Package, len(r.Packages))
	for _, p := range r.pluginPkgs() {
		if p.Spec.ID == "" {
			continue
		}
		if other, duplicate := plugins[p.Spec.ID]; duplicate {
			findings = append(findings,
				fmt.Sprintf("%s and %s both declare plugin id %q", other.Dir, p.Dir, p.Spec.ID))
			continue
		}
		plugins[p.Spec.ID] = p
	}
	for _, p := range r.pluginPkgs() {
		if p.Spec.ID == "" {
			continue
		}
		imports := make(map[string]struct{}, len(p.Files))
		for _, f := range p.Files {
			for _, importPath := range f.Paths {
				dependency := r.ByPath[importPath]
				if dependency == nil || dependency == p || dependency.Spec == nil || dependency.Spec.ID == "" {
					continue
				}
				imports[dependency.Spec.ID] = struct{}{}
			}
		}
		seen := make(map[string]struct{}, len(p.Spec.Requires))
		for _, dependency := range p.Spec.Requires {
			_, duplicate := seen[dependency]
			_, inherent := imports[dependency]
			switch {
			case dependency == "":
				findings = append(findings, fmt.Sprintf("%s: Spec.Requires has an empty plugin id", p.Dir))
			case dependency == p.Spec.ID:
				findings = append(findings, fmt.Sprintf("%s: Spec.Requires names the plugin itself", p.Dir))
			case plugins[dependency] == nil:
				findings = append(findings,
					fmt.Sprintf("%s: Spec.Requires names %q, which is not a compiled plugin", p.Dir, dependency))
			case duplicate:
				findings = append(findings, fmt.Sprintf("%s: Spec.Requires names %q twice", p.Dir, dependency))
			case inherent:
				findings = append(findings,
					fmt.Sprintf("%s: Spec.Requires names %q, already a generated import edge", p.Dir, dependency))
			}
			seen[dependency] = struct{}{}
		}
	}
	return append(findings, r.dependencyCycles(generated)...)
}

// problemLocation prints one protocol violation with the file that caused it,
// falling back to the package directory for a package-level problem.
func problemLocation(p *pluginmeta.Package, problem pluginmeta.Problem) string {
	location := problem.File
	if location == "" {
		location = p.Dir
	}
	return fmt.Sprintf("%s: %s", location, problem.Msg)
}

// dependencyCycles refuses a cycle in the effective dependency graph: the
// generated imports united with the runtime-only Requires.
func (r *repo) dependencyCycles(generated *generated) []string {
	dependencies := make(map[string][]string, len(r.Packages))
	plugins := make(map[string]struct{}, len(r.Packages))
	for _, p := range r.pluginPkgs() {
		if p.Spec.ID == "" {
			continue
		}
		plugins[p.Spec.ID] = struct{}{}
	}
	for _, p := range r.pluginPkgs() {
		if p.Spec.ID == "" {
			continue
		}
		seen := make(map[string]struct{}, len(generated.edges[p.Spec.ID])+len(p.Spec.Requires))
		list := make([]string, 0, len(generated.edges[p.Spec.ID])+len(p.Spec.Requires))
		for _, dependency := range append(append([]string(nil), generated.edges[p.Spec.ID]...), p.Spec.Requires...) {
			if _, known := plugins[dependency]; !known {
				continue
			}
			if _, duplicate := seen[dependency]; duplicate {
				continue
			}
			seen[dependency] = struct{}{}
			list = append(list, dependency)
		}
		sort.Strings(list)
		dependencies[p.Spec.ID] = list
	}
	const (
		visiting = iota + 1
		visited
	)
	color := make(map[string]int, len(dependencies))
	var findings []string
	var visit func(id string, trail []string) bool
	visit = func(id string, trail []string) bool {
		switch color[id] {
		case visiting:
			findings = append(findings, fmt.Sprintf("dependency cycle: %s", strings.Join(append(trail, id), " -> ")))
			return true
		case visited:
			return false
		}
		color[id] = visiting
		for _, dependency := range dependencies[id] {
			if visit(dependency, append(append([]string(nil), trail...), id)) {
				color[id] = visited
				return true
			}
		}
		color[id] = visited
		return false
	}
	ids := make([]string, 0, len(dependencies))
	for id := range dependencies {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		visit(id, nil)
	}
	return findings
}

// ruleFactoryIDs proves a factory contribution has exactly one spelling: an id
// is derived with instances.ID, never repeated as a "kind/type" literal that
// could drift from its kind and type.
func ruleFactoryIDs(r *repo, _ *generated) []string {
	kinds := r.kindNames()
	var findings []string
	for _, p := range r.Packages {
		if isKitDir(p.Dir) {
			continue
		}
		for _, f := range p.Files {
			derived := instancesIDLiterals(f)
			inspectDecls(f, func(node ast.Node) bool {
				literal, ok := node.(*ast.BasicLit)
				if !ok || literal.Kind != token.STRING {
					return true
				}
				if _, ok := derived[literal.Pos()]; ok {
					return true
				}
				value, _ := pluginmeta.StringLiteral(literal)
				kind, typ, found := strings.Cut(value, "/")
				if !found || typ == "" || strings.Contains(typ, "/") {
					return true
				}
				if _, ok := kinds[kind]; !ok {
					return true
				}
				findings = append(findings, fmt.Sprintf(
					"%s:%d: literal factory id %q; derive it with instances.ID", f.Rel, r.line(literal), value))
				return true
			})
		}
	}
	return findings
}

// instancesIDLiterals marks the literals that are arguments of instances.ID,
// the one spelling a factory id may use.
func instancesIDLiterals(f *pluginmeta.File) map[token.Pos]struct{} {
	out := map[token.Pos]struct{}{}
	inspectDecls(f, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || selector.Sel.Name != "ID" {
			return true
		}
		alias, ok := selector.X.(*ast.Ident)
		if !ok || !isInstances(f.Imports[alias.Name]) {
			return true
		}
		for _, argument := range call.Args {
			ast.Inspect(argument, func(inner ast.Node) bool {
				if inner == nil {
					return true
				}
				out[inner.Pos()] = struct{}{}
				return true
			})
		}
		return true
	})
	return out
}

// ruleLoaderPurity proves the kernel names no feature: no compiled plugin id, no
// kind, role or command name appears in a loader string literal.
func ruleLoaderPurity(r *repo, _ *generated) []string {
	vocabulary := map[string]string{}
	add := func(value, what string) {
		if _, exists := vocabulary[value]; !exists {
			vocabulary[value] = what
		}
	}
	for _, p := range r.pluginPkgs() {
		if p.Spec.ID != "" {
			add(p.Spec.ID, "plugin id")
		}
	}
	for kind := range r.kindNames() {
		add(kind, "kind name")
	}
	for role := range r.roleNames() {
		add(role, "role name")
	}
	for command := range r.commandNames() {
		add(command, "command name")
	}
	loaderPath := r.Module + "/" + loaderDir
	var findings []string
	for _, p := range r.Packages {
		if p.Path != loaderPath {
			continue
		}
		for _, f := range p.Files {
			inspectDecls(f, func(node ast.Node) bool {
				literal, ok := node.(*ast.BasicLit)
				if !ok || literal.Kind != token.STRING {
					return true
				}
				value, _ := pluginmeta.StringLiteral(literal)
				if what, ok := vocabulary[value]; ok {
					findings = append(findings, fmt.Sprintf(
						"%s:%d: the loader names %s %q; it belongs to a plugin", f.Rel, r.line(literal), what, value))
				}
				return true
			})
		}
	}
	return findings
}

// ruleRoleCommands proves the role token namespace stays disjoint from the
// command namespace: a role selects a process entrypoint, a command stays the
// first argument of the default role.
func ruleRoleCommands(r *repo, _ *generated) []string {
	commands := r.commandNames()
	if len(commands) == 0 {
		return []string{fmt.Sprintf(
			"no command names found under %s: the role/command conflict rule cannot run", cliPluginDir)}
	}
	var findings []string
	for role := range r.roleNames() {
		if _, conflict := commands[role]; conflict {
			findings = append(findings, fmt.Sprintf(
				"role %q has the same name as a CLI command; a role token must not select a command", role))
		}
	}
	return findings
}

// ruleRawStorage confines the raw storage boundary to its owner and guard: every
// other consumer works through the guarded handles.
func ruleRawStorage(r *repo, _ *generated) []string {
	raw := r.Module + "/" + rawStorageDir
	owner := r.Module + "/" + storageOwnerDir
	guard := owner + "/guard"
	var findings []string
	for _, p := range r.Packages {
		if p.Path == owner || p.Path == guard || strings.HasPrefix(p.Path, guard+"/") {
			continue
		}
		for _, f := range p.Files {
			for _, importPath := range f.Paths {
				if importPath != raw {
					continue
				}
				findings = append(findings, fmt.Sprintf(
					"%s imports %s; only the storage owner and its guard may import the raw boundary", f.Rel, rawStorageDir))
			}
		}
	}
	return findings
}

// ruleEagerInit proves no package-level initializer does eager work: only error
// markers, kind and service declarations, factory ids and lazily-built singletons
// may appear there. Literals, function values, interface assertions and test
// seams are expressions, not work, and stay allowed.
func ruleEagerInit(r *repo, _ *generated) []string {
	var findings []string
	for _, p := range r.Packages {
		if isKitDir(p.Dir) {
			continue
		}
		for _, f := range p.Files {
			for _, decl := range f.AST.Decls {
				gen, ok := decl.(*ast.GenDecl)
				if !ok || gen.Tok != token.VAR {
					continue
				}
				for _, spec := range gen.Specs {
					for _, expr := range spec.(*ast.ValueSpec).Values {
						findings = append(findings, r.eagerCalls(p, f, expr)...)
					}
				}
			}
		}
	}
	return findings
}

// eagerCalls reports every call inside one initializer expression that is work,
// not a static declaration.
func (r *repo) eagerCalls(p *pluginmeta.Package, f *pluginmeta.File, expr ast.Expr) []string {
	var findings []string
	ast.Inspect(expr, func(node ast.Node) bool {
		if _, literal := node.(*ast.FuncLit); literal {
			return false
		}
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		if protocolDeclaration(f, call) {
			return false
		}
		if r.allowedInitializerCall(p, f, call) {
			return true
		}
		findings = append(findings, fmt.Sprintf(
			"%s:%d: package-level initializer calls %s; defer the work behind sync.OnceValue",
			f.Rel, r.line(call), callName(call.Fun)))
		return true
	})
	return findings
}

// allowedInitializerCall names the calls a package initializer may make without
// doing work: an error marker, a lazily-initialised value, a type token, or a
// type conversion.
func (r *repo) allowedInitializerCall(p *pluginmeta.Package, f *pluginmeta.File, call *ast.CallExpr) bool {
	fun := unwrapGenerics(call.Fun)
	if selector, ok := fun.(*ast.SelectorExpr); ok {
		alias, ok := selector.X.(*ast.Ident)
		if !ok {
			return false
		}
		importPath := f.Imports[alias.Name]
		name := selector.Sel.Name
		switch {
		case importPath == "errors" && name == "New":
			return true
		case importPath == "sync" && (name == "OnceValue" || name == "OnceValues"):
			return true
		case importPath == "reflect" && name == "TypeFor":
			return true
		}
		if target := r.ByPath[importPath]; target != nil {
			if _, conversion := target.Types[name]; conversion {
				return true
			}
		}
		return false
	}
	if isConversionTarget(fun) {
		return true
	}
	ident, ok := fun.(*ast.Ident)
	if !ok {
		return false
	}
	if _, conversion := p.Types[ident.Name]; conversion {
		return true
	}
	return ident.Name == "make" || ident.Name == "new"
}

// protocolDeclaration reports the calls that declare a kind, a service or a
// factory id. Their arguments are protocol expressions — a kind name, a type
// literal — so the walk does not descend into them.
func protocolDeclaration(f *pluginmeta.File, call *ast.CallExpr) bool {
	fun := unwrapGenerics(call.Fun)
	selector, ok := fun.(*ast.SelectorExpr)
	if !ok {
		ident, ok := fun.(*ast.Ident)
		return ok && (ident.Name == "NewKind" || ident.Name == "NewService")
	}
	alias, ok := selector.X.(*ast.Ident)
	if !ok {
		return false
	}
	importPath := f.Imports[alias.Name]
	switch selector.Sel.Name {
	case "NewKind", "NewService":
		return isLoader(importPath) || isInstances(importPath)
	case "ID":
		return isInstances(importPath)
	default:
		return false
	}
}

// predeclaredTypes are the built-in types a conversion may name.
var predeclaredTypes = map[string]bool{
	"any": true, "bool": true, "byte": true, "complex64": true, "complex128": true,
	"error": true, "float32": true, "float64": true, "int": true, "int8": true,
	"int16": true, "int32": true, "int64": true, "rune": true, "string": true,
	"uint": true, "uint8": true, "uint16": true, "uint32": true, "uint64": true,
	"uintptr": true,
}

// isConversionTarget reports the CallExpr fun positions that denote a type:
// a parenthesised pointer, a slice, map, channel, function or interface type.
// Those calls convert, they do not run code.
func isConversionTarget(expr ast.Expr) bool {
	switch typed := expr.(type) {
	case *ast.StarExpr:
		return true
	case *ast.ParenExpr:
		return isConversionTarget(typed.X)
	case *ast.ArrayType, *ast.MapType, *ast.ChanType,
		*ast.FuncType, *ast.InterfaceType, *ast.StructType:
		return true
	case *ast.Ident:
		_, conversion := predeclaredTypes[typed.Name]
		return conversion
	default:
		return false
	}
}

// callName prints the callee of an initializer call for a diagnostic.
func callName(expr ast.Expr) string {
	switch typed := unwrapGenerics(expr).(type) {
	case *ast.Ident:
		return typed.Name
	case *ast.SelectorExpr:
		if alias, ok := typed.X.(*ast.Ident); ok {
			return alias.Name + "." + typed.Sel.Name
		}
		return typed.Sel.Name
	case *ast.FuncLit:
		return "a function literal"
	default:
		return "an expression"
	}
}

// rulePluginFiles proves every plugin package, libraries included, declares its
// identity as a literal loader.Spec in plugin.go.
func rulePluginFiles(r *repo, _ *generated) []string {
	var findings []string
	for _, p := range r.Packages {
		if !pluginPackageDir(p.Dir) {
			continue
		}
		switch {
		case p.Spec == nil:
			findings = append(findings,
				fmt.Sprintf("%s: plugin package has no plugin.go with a literal loader.Spec", p.Dir))
		case path.Base(p.Spec.File) != "plugin.go":
			findings = append(findings, fmt.Sprintf(
				"%s: the loader.Spec literal must be declared in plugin.go, not %s", p.Dir, p.Spec.File))
		}
	}
	return findings
}

// pluginPackageDir reports whether a directory is a plugin package. Conformance
// kits, test data and a Go internal boundary are not plugins, and neither is the
// data-only defaults subpackage owned by the configuration plugin.
func pluginPackageDir(dir string) bool {
	if !inPlugins(dir) || isKitDir(dir) || dir == defaultsDataDir {
		return false
	}
	if dir == compositionDir || strings.HasPrefix(dir, compositionDir+"/") {
		return false
	}
	for _, segment := range strings.Split(strings.TrimPrefix(dir, pluginsDir+"/"), "/") {
		if segment == "internal" {
			return false
		}
	}
	return true
}
