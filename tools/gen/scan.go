package main

import (
	"fmt"
	"slices"
	"strings"

	"github.com/vi-latyshev/go-cluster-backup/tools/internal/pluginmeta"
)

// pluginsPrefix is the module-relative directory every plugin package lives in.
const pluginsPrefix = "internal/plugins/"

// plugin is one compiled plugin discovered from a package's loader Spec.
type plugin struct {
	path     string   // import path
	dir      string   // module-relative slash directory
	name     string   // package name
	id       string   // Spec.ID; a factory derives it from its kind and type
	summary  string   // Spec.Summary
	floor    bool     // Spec.Floor
	requires []string // Spec.Requires
	after    []string // Spec.After
	apply    pluginmeta.ApplyKind
	imports  []string // ids of plugin packages this package imports in production
}

// The Apply shapes the emitter switches on.
const (
	applyNone    = pluginmeta.ApplyNone
	applyPlugin  = pluginmeta.ApplyPlugin
	applyFactory = pluginmeta.ApplyFactory
)

// load builds the plugin view of the module at root: the packages under
// internal/plugins that declare a Spec, their resolved ids and the production
// import edges between them. The metadata rules live in pluginmeta; this is the
// generator's policy on top of them.
func load(root string) ([]*plugin, error) {
	tree, err := pluginmeta.Read(root)
	if err != nil {
		return nil, err
	}
	out := make([]*plugin, 0, len(tree.Packages))
	byPath := make(map[string]*plugin, len(tree.Packages))
	for _, pkg := range tree.Packages {
		if !isPluginDir(pkg.Dir) {
			continue
		}
		if len(pkg.Problems) > 0 {
			return nil, problemError(pkg)
		}
		if pkg.Spec == nil {
			continue
		}
		if pkg.Spec.Summary == "" {
			return nil, fmt.Errorf("%s: Spec.Summary is required", pkg.Path)
		}
		p := &plugin{
			path:     pkg.Path,
			dir:      pkg.Dir,
			name:     pkg.Name,
			id:       pkg.Spec.ID,
			summary:  pkg.Spec.Summary,
			floor:    pkg.Spec.Floor,
			requires: pkg.Spec.Requires,
			after:    pkg.Spec.After,
		}
		if pkg.Apply != nil {
			p.apply = pkg.Apply.Kind
		}
		byPath[pkg.Path] = p
		out = append(out, p)
	}
	// One id belongs to one plugin: a factory derives it from its kind and
	// type, every other provider declares it, and a collision is a build error.
	ids := make(map[string]string, len(out))
	for _, p := range out {
		if other, duplicate := ids[p.id]; duplicate {
			return nil, fmt.Errorf("plugins %s and %s both use id %q", other, p.path, p.id)
		}
		ids[p.id] = p.path
	}
	// The import graph is the production import graph between plugin packages:
	// every production file counts, whatever its build tags say, and one edge
	// per target remains.
	for _, p := range out {
		for _, f := range tree.ByPath[p.path].Files {
			for _, imported := range f.Imports {
				target, ok := byPath[imported]
				if !ok || target == p {
					continue
				}
				p.imports = append(p.imports, target.id)
			}
		}
		slices.Sort(p.imports)
		p.imports = slices.Compact(p.imports)
	}
	slices.SortFunc(out, func(a, b *plugin) int { return strings.Compare(a.path, b.path) })
	return out, nil
}

// problemError renders the first protocol violation of one package the way every
// generation error is rendered: the file when there is one, the import path
// always, then the rule broken.
func problemError(pkg *pluginmeta.Package) error {
	problem := pkg.Problems[0]
	if problem.File != "" {
		return fmt.Errorf("%s: %s: %s", problem.File, pkg.Path, problem.Msg)
	}
	return fmt.Errorf("%s: %s", pkg.Path, problem.Msg)
}

// isPluginDir is the generator's plugin policy on the directory: a package
// inside internal/plugins, not the composition root and not a conformance kit.
// Whether the package declares a Spec decides whether it is a plugin; a
// protocol violation in such a directory refuses generation either way.
func isPluginDir(dir string) bool {
	rest, ok := strings.CutPrefix(dir, pluginsPrefix)
	if !ok || rest == "" {
		return false
	}
	base := rest
	if i := strings.LastIndex(rest, "/"); i >= 0 {
		base = rest[i+1:]
	}
	return base != "all" && !strings.HasSuffix(base, "test")
}
