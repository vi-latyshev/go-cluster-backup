package main

import (
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"path/filepath"
	"strings"

	"github.com/vi-latyshev/go-cluster-backup/tools/internal/pluginmeta"
)

// floorDir is the one package allowed to mint this run's write authority: the
// Floor issues it only after lease acquisition, the full survey and admission.
const floorDir = "internal/plugins/workflow/backup"

// ruleRestrictedSymbols enforces the package-qualified mutation boundary and,
// through go/types, every authority expression the compiler can type: the
// concrete ledger, issuer interfaces, assertions, method values and the
// NewLedger constructor.
func ruleRestrictedSymbols(r *repo, _ *generated) []string {
	return uniqueSorted(append(symbolFindings(r), r.authorityFindings()...))
}

// ruleLeakedMutations reports the declarations, type expressions and assertions
// whose static type holds a mutation value outside the boundary packages. It
// shares the go/types pass with the symbol rule.
func ruleLeakedMutations(r *repo, _ *generated) []string {
	return r.authorityFindings()
}

// symbolFindings enforces the qualified symbols the loader boundary names:
// storage construction, guard construction and authority, destination removal
// and the structural guard.Admit ban.
func symbolFindings(r *repo) []string {
	guardPath := r.Module + "/" + guardDir
	storagePath := r.Module + "/" + storageOwnerDir
	var findings []string
	for _, p := range r.Packages {
		for _, f := range p.Files {
			inspectDecls(f, func(node ast.Node) bool {
				selector, ok := node.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				findings = append(findings, restrictedSelector(r, p, f, selector, guardPath, storagePath)...)
				return true
			})
		}
		findings = append(findings, guardAdmitDeclaration(r, p)...)
	}
	return findings
}

// restrictedSelector judges one package-qualified restricted symbol.
func restrictedSelector(r *repo, p *pluginmeta.Package, f *pluginmeta.File, selector *ast.SelectorExpr, guardPath, storagePath string) []string {
	name := selector.Sel.Name
	alias, qualified := selector.X.(*ast.Ident)
	if !qualified {
		return nil
	}
	switch f.Imports[alias.Name] {
	case storagePath:
		switch name {
		case "Build", "Register", "AddFactory":
			if !dirAllowed(p.Dir, storageOwnerDir) {
				return restricted(r, f, selector, "storage."+name, storageOwnerDir+"/**")
			}
		}
	case guardPath:
		switch name {
		case "Build":
			if !dirAllowed(p.Dir, assemblyDir, cliPluginDir) {
				return restricted(r, f, selector, "guard.Build", assemblyDir+" and "+cliPluginDir)
			}
		case "NewAuthority":
			if !dirAllowed(p.Dir, protectionDir) {
				return restricted(r, f, selector, "guard.NewAuthority", protectionDir)
			}
		case "Destination", "Remover":
			if !dirAllowed(p.Dir, assemblyDir, protectionDir) {
				return restricted(r, f, selector, "guard."+name, assemblyDir+" and "+protectionDir)
			}
		case "Admit":
			return []string{fmt.Sprintf(
				"%s:%d: guard.Admit must not exist; the protection ledger issues this run's authority",
				f.Rel, r.line(selector))}
		}
	}
	return nil
}

// authorityFindings is the one go/types pass over every non-boundary package:
// every expression, type expression, selection and assertion whose static type
// exposes the ledger, a Remover/Destination, or an Authorize method.
func (r *repo) authorityFindings() []string {
	if r.checkedAuthorities {
		return r.authority
	}
	r.checkedAuthorities = true
	checker := newTypeChecker(r.Tree)
	var findings []string
	for _, p := range r.Packages {
		// The declaring and assembling packages are exempt; the Floor is not,
		// because adoption of a Remover/Destination is closed to it as well.
		if dirAllowed(p.Dir, protectionDir, assemblyDir, guardDir) {
			continue
		}
		findings = append(findings, r.packageAuthorities(checker, p,
			dirAllowed(p.Dir, floorDir), dirAllowed(p.Dir, cliPluginDir))...)
	}
	r.authority = uniqueSorted(findings)
	return r.authority
}

// packageAuthorities scans one package's checked view.
func (r *repo) packageAuthorities(checker *typeChecker, p *pluginmeta.Package, floor, host bool) []string {
	checked := checker.typesFor(p)
	info := checked.info
	var findings []string
	for _, f := range p.Files {
		for _, decl := range f.AST.Decls {
			ast.Inspect(decl, func(node ast.Node) bool {
				switch typed := node.(type) {
				case *ast.SelectorExpr:
					findings = append(findings, r.selectionFindings(info, typed, floor, host)...)
				case *ast.TypeAssertExpr:
					findings = append(findings, r.assertionFindings(info, typed.Type, floor, host, checked.pkg)...)
				case *ast.TypeSwitchStmt:
					for _, clause := range typed.Body.List {
						for _, asserted := range clause.(*ast.CaseClause).List {
							findings = append(findings, r.assertionFindings(info, asserted, floor, host, checked.pkg)...)
						}
					}
				}
				return true
			})
		}
	}
	for expr, value := range info.Types {
		findings = append(findings, r.expressionFindings(expr, value, floor, host, checked.pkg)...)
	}
	if !floor {
		for ident, used := range info.Uses {
			function, ok := used.(*types.Func)
			if !ok || !isProtectionFunc(function, "NewLedger") {
				continue
			}
			findings = append(findings, r.findingAt(ident,
				"protection.NewLedger is restricted to "+protectionDir+", "+assemblyDir+" and "+floorDir)...)
		}
	}
	return findings
}

// selectionFindings judges one selector through its compiler-resolved method
// selection: Authorize, Destination.Remover, Remover.Delete and NewLedger.
func (r *repo) selectionFindings(info *types.Info, selector *ast.SelectorExpr, floor, host bool) []string {
	if selected, ok := info.Selections[selector]; ok {
		receiver := declaringReceiver(selected)
		switch selected.Obj().Name() {
		case "Authorize":
			if floor {
				return nil
			}
			if isNamedIn(receiver, "/internal/plugins/protection", "Ledger") {
				return r.findingAt(selector, "protection.Ledger.Authorize is restricted to "+floorDir)
			}
			if isIssuerInterface(receiver) {
				return r.findingAt(selector, "an authority issuer assertion is restricted to "+floorDir)
			}
		case "Remover":
			if isNamedIn(receiver, "/internal/plugins/storage/guard", "Destination") {
				return r.findingAt(selector, "Destination.Remover is restricted to "+assemblyDir+" and "+protectionDir)
			}
		case "Delete":
			if isNamedIn(receiver, "/internal/plugins/storage/guard", "Remover") {
				return r.findingAt(selector, "Remover.Delete is restricted to "+assemblyDir+" and "+protectionDir)
			}
		}
	}
	return nil
}

// declaringReceiver returns the receiver type the selected method is declared
// on, so a method promoted through an embedded field is judged by its owner.
func declaringReceiver(selected *types.Selection) types.Type {
	function, ok := selected.Obj().(*types.Func)
	if !ok {
		return selected.Recv()
	}
	return function.Type().(*types.Signature).Recv().Type()
}

// assertionFindings refuses an assertion whose asserted type is the ledger, an
// interface or concrete type that can issue authority, or holds a mutation value.
func (r *repo) assertionFindings(info *types.Info, asserted ast.Expr, floor, host bool, consumer *types.Package) []string {
	value, ok := info.Types[asserted]
	if !ok || value.Type == nil {
		return nil
	}
	typ := value.Type
	switch {
	case !floor && holdsTypeIn(typ, ledgerNamed, consumer):
		return r.findingAt(asserted, "only protection, assembly and the Floor may hold protection.Ledger; a cross-package declaration could launder it")
	case !floor && hasAuthorizeMethod(typ):
		return r.findingAt(asserted, "an authority issuer assertion is restricted to "+floorDir)
	case holdsTypeIn(typ, guardNamed("Remover"), consumer):
		return r.findingAt(asserted, "only protection, assembly and the Floor may hold guard.Remover; a cross-package declaration could launder it")
	case !host && holdsTypeIn(typ, guardNamed("Destination"), consumer):
		return r.findingAt(asserted, "guard.Destination is restricted to "+assemblyDir+" and "+protectionDir)
	default:
		return nil
	}
}

// expressionFindings judges one typed expression: a written type that holds a
// mutation value, a value whose type is the ledger or an issuer interface.
func (r *repo) expressionFindings(expr ast.Expr, value types.TypeAndValue, floor, host bool, consumer *types.Package) []string {
	typ := value.Type
	if value.IsType() {
		switch {
		case !floor && holdsTypeIn(typ, ledgerNamed, consumer):
			return r.findingAt(expr, "only protection, assembly and the Floor may hold protection.Ledger; a cross-package declaration could launder it")
		case holdsTypeIn(typ, guardNamed("Remover"), consumer):
			return r.findingAt(expr, "only protection, assembly and the Floor may hold guard.Remover; a cross-package declaration could launder it")
		case !host && holdsTypeIn(typ, guardNamed("Destination"), consumer):
			return r.findingAt(expr, "guard.Destination is restricted to "+assemblyDir+" and "+protectionDir)
		default:
			return nil
		}
	}
	switch {
	case !floor && holdsTypeIn(typ, ledgerNamed, consumer):
		return r.findingAt(expr, "only protection, assembly and the Floor may hold protection.Ledger; a cross-package declaration could launder it")
	case !floor && isIssuerInterface(typ):
		return r.findingAt(expr, "an authority issuer type is restricted to "+floorDir)
	case holdsTypeIn(typ, guardNamed("Remover"), consumer):
		return r.findingAt(expr, "only protection, assembly and the Floor may hold guard.Remover; a cross-package declaration could launder it")
	case !host && holdsTypeIn(typ, guardNamed("Destination"), consumer):
		return r.findingAt(expr, "guard.Destination is restricted to "+assemblyDir+" and "+protectionDir)
	default:
		return nil
	}
}

// findingAt renders one finding at a syntax position.
func (r *repo) findingAt(node ast.Node, message string) []string {
	file, line := r.locate(node.Pos())
	return []string{fmt.Sprintf("%s:%d: %s", file, line, message)}
}

// locateRel is the unprovokable relative-path seam: every scanned file is under
// the module root.
var locateRel = filepath.Rel

// locate maps a token position to the module-relative file and one-based line.
func (r *repo) locate(pos token.Pos) (string, int) {
	position := r.FileSet.Position(pos)
	rel, err := locateRel(r.Root, position.Filename)
	if err != nil {
		rel = position.Filename
	}
	return filepath.ToSlash(rel), position.Line
}

// ledgerNamed reports whether a type is protection.Ledger after alias and
// pointer resolution.
func ledgerNamed(t types.Type) bool {
	return isNamedIn(t, "/internal/plugins/protection", "Ledger")
}

// guardNamed returns the predicate for one named guard type.
func guardNamed(name string) func(types.Type) bool {
	return func(t types.Type) bool {
		return isNamedIn(t, "/internal/plugins/storage/guard", name)
	}
}

// isNamedIn reports whether a type resolves to a named type of one package.
func isNamedIn(t types.Type, packageSuffix, name string) bool {
	if t == nil {
		return false
	}
	t = types.Unalias(t)
	if pointer, ok := t.(*types.Pointer); ok {
		t = types.Unalias(pointer.Elem())
	}
	named, ok := t.(*types.Named)
	if !ok || named.Obj().Pkg() == nil {
		return false
	}
	if named.Obj().Name() != name {
		return false
	}
	return strings.HasSuffix("/"+named.Obj().Pkg().Path(), packageSuffix)
}

// holdsType reports whether a type is, or reaches through a pointer, slice,
// array, map, channel or struct field, a type the predicate accepts. The
// traversal is cycle-safe, so a recursive container terminates.
func holdsType(t types.Type, accepts func(types.Type) bool) bool {
	return holdsTypeIn(t, accepts, nil)
}

// holdsTypeIn reports whether a type reaches a type the predicate accepts from
// the consumer package: field accessibility is the compiler's answer, so an
// exported field promoted through a private anonymous embedding is reachable
// while a private named field and an ambiguous promotion are not.
func holdsTypeIn(t types.Type, accepts func(types.Type) bool, consumer *types.Package) bool {
	return holdsTypeVisited(t, accepts, consumer, map[types.Type]bool{})
}

// holdsTypeVisited is holdsTypeIn with the traversal-local visited set.
func holdsTypeVisited(t types.Type, accepts func(types.Type) bool, consumer *types.Package, seen map[types.Type]bool) bool {
	if t == nil {
		return false
	}
	t = types.Unalias(t)
	if seen[t] {
		return false
	}
	seen[t] = true
	if accepts(t) {
		return true
	}
	switch typed := t.Underlying().(type) {
	case *types.Pointer:
		return holdsTypeVisited(typed.Elem(), accepts, consumer, seen)
	case *types.Slice:
		return holdsTypeVisited(typed.Elem(), accepts, consumer, seen)
	case *types.Array:
		return holdsTypeVisited(typed.Elem(), accepts, consumer, seen)
	case *types.Map:
		return holdsTypeVisited(typed.Key(), accepts, consumer, seen) ||
			holdsTypeVisited(typed.Elem(), accepts, consumer, seen)
	case *types.Chan:
		return holdsTypeVisited(typed.Elem(), accepts, consumer, seen)
	case *types.Struct:
		names := map[string]struct{}{}
		structFieldNames(t, names, map[types.Type]bool{})
		for name := range names {
			object, _, _ := types.LookupFieldOrMethod(t, true, consumer, name)
			if object == nil {
				continue
			}
			field, ok := object.(*types.Var)
			if !ok {
				continue
			}
			if holdsTypeVisited(field.Type(), accepts, consumer, seen) {
				return true
			}
		}
		return false
	default:
		return false
	}
}

// structFieldNames collects every field name a struct type offers, including
// names promoted through anonymous fields, so the compiler lookup can decide
// whether the consumer actually selects it.
func structFieldNames(t types.Type, names map[string]struct{}, seen map[types.Type]bool) {
	if t == nil {
		return
	}
	t = types.Unalias(t)
	if pointer, ok := t.(*types.Pointer); ok {
		t = types.Unalias(pointer.Elem())
	}
	if seen[t] {
		return
	}
	seen[t] = true
	structure, ok := t.Underlying().(*types.Struct)
	if !ok {
		return
	}
	for i := 0; i < structure.NumFields(); i++ {
		field := structure.Field(i)
		names[field.Name()] = struct{}{}
		if field.Anonymous() {
			structFieldNames(field.Type(), names, seen)
		}
	}
}

// isIssuerInterface reports whether a type is an interface whose method set
// contains Authorize, including embedded interfaces.
func isIssuerInterface(t types.Type) bool {
	if t == nil {
		return false
	}
	t = types.Unalias(t)
	if _, ok := t.Underlying().(*types.Interface); !ok {
		return false
	}
	return hasAuthorizeMethod(t)
}

// hasAuthorizeMethod reports whether a type's method set contains Authorize.
func hasAuthorizeMethod(t types.Type) bool {
	if t == nil {
		return false
	}
	methods := types.NewMethodSet(t)
	for i := 0; i < methods.Len(); i++ {
		if methods.At(i).Obj().Name() == "Authorize" {
			return true
		}
	}
	return false
}

// isProtectionFunc reports whether a function belongs to the protection package
// under one name.
func isProtectionFunc(function *types.Func, name string) bool {
	if function == nil || function.Name() != name || function.Pkg() == nil {
		return false
	}
	return strings.HasSuffix("/"+function.Pkg().Path(), "/internal/plugins/protection")
}

// guardAdmitDeclaration refuses a structural guard.Admit: the symbol must not
// exist at all, so no consumer can mint an authority through the guard package.
func guardAdmitDeclaration(r *repo, p *pluginmeta.Package) []string {
	if p.Dir != guardDir {
		return nil
	}
	var findings []string
	for _, f := range p.Files {
		for _, decl := range f.AST.Decls {
			for _, name := range declaredNames(decl) {
				if name != "Admit" {
					continue
				}
				findings = append(findings, fmt.Sprintf(
					"%s:%d: guard.Admit must not exist; the protection ledger issues this run's authority",
					f.Rel, r.line(decl)))
			}
		}
	}
	return findings
}

// declaredNames lists the top-level names one declaration introduces.
func declaredNames(decl ast.Decl) []string {
	if function, ok := decl.(*ast.FuncDecl); ok {
		return []string{function.Name.Name}
	}
	gen := decl.(*ast.GenDecl)
	var out []string
	for _, spec := range gen.Specs {
		switch spec := spec.(type) {
		case *ast.TypeSpec:
			out = append(out, spec.Name.Name)
		case *ast.ValueSpec:
			for _, name := range spec.Names {
				out = append(out, name.Name)
			}
		case *ast.ImportSpec:
		}
	}
	return out
}

// restricted renders one restricted-symbol finding.
func restricted(r *repo, f *pluginmeta.File, node ast.Node, symbol, allowed string) []string {
	return []string{fmt.Sprintf("%s:%d: %s is restricted to %s", f.Rel, r.line(node), symbol, allowed)}
}
