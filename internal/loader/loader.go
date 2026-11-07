// Package loader is the kernel of the runtime. It registers compiled plugins in
// memory, records the kinds, services, roles and lifecycle hooks each plugin
// contributes, applies the deployment's selection, orders plugins by their
// effective dependencies and runs one role. It names no feature: every plugin
// id, kind, service, role and command belongs to a plugin, and this package
// imports only the standard library.
package loader

import (
	"errors"
	"fmt"
)

// Imports is the generated import graph: a compiled plugin id mapped to the ids
// of the plugin packages it imports. The generator derives it from the source
// graph; Spec.Requires adds runtime-only edges on top, and together they are the
// one effective dependency model selection, ordering and startup share.
type Imports map[string][]string

// Spec describes one compiled plugin. Registration is memory only: no I/O, no
// environment, no goroutines.
type Spec struct {
	ID       string   // stable plugin id, the unit of selection and dependency
	Summary  string   // one line for diagnostics
	Floor    bool     // cannot be disabled; never depends on an optional plugin
	Requires []string // runtime-only plugin ids on top of the import graph; never a binary
	After    []string // soft ordering hints; an unregistered id is skipped
}

// Registry is one compiled set. Create it with New; after registration it is
// read-only apart from ApplySelection, which records the active set.
type Registry struct {
	specs             map[string]*pluginEntry
	order             []string
	imports           Imports
	kinds             map[string]any
	services          map[string]any
	roles             map[string]*roleEntry
	roleOrder         []string
	def               *roleEntry
	hooks             map[string]map[string]Lifecycle
	active            map[string]bool
	current           string
	selectionProblems []string
}

type pluginEntry struct {
	spec Spec
	// library is Add's nil apply: the plugin contributes nothing to the
	// loader, so it is mandatory infrastructure for whoever imports it and
	// cannot be selected away.
	library bool
}

// New returns an empty registry.
func New() *Registry {
	return &Registry{
		specs:    map[string]*pluginEntry{},
		kinds:    map[string]any{},
		services: map[string]any{},
		roles:    map[string]*roleEntry{},
		hooks:    map[string]map[string]Lifecycle{},
	}
}

// Add registers one plugin. apply contributes the plugin's kinds, services,
// roles and lifecycle hooks; nil is a library — a plugin that contributes
// nothing to the loader, such as a client package other plugins import. A
// library is always active and is not selectable: disabling it is refused like
// disabling a floor plugin. An error from apply aborts registration, so a
// half-registered registry is never run.
func Add(r *Registry, s Spec, apply func(*Registry) error) error {
	if r.current != "" {
		return errors.New("loader: plugin registration is not nested")
	}
	if s.ID == "" {
		return errors.New("loader: plugin id is empty")
	}
	if _, dup := r.specs[s.ID]; dup {
		return fmt.Errorf("loader: plugin %q registered twice", s.ID)
	}
	for _, dep := range s.Requires {
		if dep == "" {
			return fmt.Errorf("loader: plugin %q requires an empty id", s.ID)
		}
	}
	r.specs[s.ID] = &pluginEntry{spec: s, library: apply == nil}
	r.order = append(r.order, s.ID)
	if apply == nil {
		return nil
	}
	r.current = s.ID
	err := apply(r)
	r.current = ""
	if err != nil {
		return fmt.Errorf("plugin %q: %w", s.ID, err)
	}
	return nil
}

// SetImports records the generated import graph. The composition root calls it
// once, before or after Add: an edge to an unregistered plugin is reported by
// Health rather than here, because the graph may arrive before the plugins do.
func SetImports(r *Registry, imports Imports) error {
	if r.imports != nil {
		return errors.New("loader: imports are already set")
	}
	r.imports = make(Imports, len(imports))
	for id, deps := range imports {
		if id == "" {
			return errors.New("loader: import graph has an empty plugin id")
		}
		for _, dep := range deps {
			if dep == "" {
				return fmt.Errorf("loader: plugin %q imports an empty plugin id", id)
			}
		}
		r.imports[id] = deps
	}
	return nil
}

// Dependencies is the effective dependency list of one compiled plugin: the
// generated imports united with the runtime-only Requires, in that order,
// without duplicates. An edge to an unregistered plugin is returned as it is;
// Health reports it.
func Dependencies(r *Registry, id string) ([]string, bool) {
	entry, ok := r.specs[id]
	if !ok {
		return nil, false
	}
	deps := make([]string, 0, len(r.imports[id])+len(entry.spec.Requires))
	seen := make(map[string]struct{}, cap(deps))
	add := func(ids []string) {
		for _, dep := range ids {
			if _, dup := seen[dep]; dup {
				continue
			}
			seen[dep] = struct{}{}
			deps = append(deps, dep)
		}
	}
	add(r.imports[id])
	add(entry.spec.Requires)
	return deps, true
}

// Active reports whether a compiled plugin is in the active set. Before a
// selection is applied every compiled plugin counts as active.
func Active(r *Registry, id string) bool { return r.isActive(id) }

func (r *Registry) isActive(id string) bool {
	if _, ok := r.specs[id]; !ok {
		return false
	}
	return r.active == nil || r.active[id]
}

// ActiveIDs lists the effective active set in registration order. It is what
// offline role and instance checks read after ApplySelection: a disabled
// plugin is absent, a library is present whatever the deployment listed.
func ActiveIDs(r *Registry) []string {
	out := make([]string, 0, len(r.order))
	for _, id := range r.order {
		if r.isActive(id) {
			out = append(out, id)
		}
	}
	return out
}

// FloorIDs lists the effective floor set in registration order: every plugin
// whose Spec declares Floor, plus transitively every effective dependency of
// one. Declared-Floor and promoted dependencies are indistinguishable here —
// both are mandatory, and ApplySelection refuses either as a disable. This is
// the closure the generated floor manifest lists.
func FloorIDs(r *Registry) []string {
	set := floorSet(r)
	out := make([]string, 0, len(set))
	for _, id := range r.order {
		if set[id] {
			out = append(out, id)
		}
	}
	return out
}

// floorSet computes the effective floor closure over the effective dependency
// graph. An unregistered dependency is left out: Health reports it, and it is
// not a compiled plugin anyone could disable.
func floorSet(r *Registry) map[string]bool {
	set := make(map[string]bool, len(r.order))
	queue := make([]string, 0, len(r.order))
	for _, id := range r.order {
		if r.specs[id].spec.Floor {
			queue = append(queue, id)
		}
	}
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		if _, compiled := r.specs[id]; !compiled || set[id] {
			continue
		}
		set[id] = true
		deps, _ := Dependencies(r, id)
		queue = append(queue, deps...)
	}
	return set
}

// Plugin is the diagnostic view of one compiled plugin.
type Plugin struct {
	ID      string
	Summary string
	Floor   bool
}

// Plugins lists every compiled plugin in registration order.
func Plugins(r *Registry) []Plugin {
	out := make([]Plugin, 0, len(r.order))
	for _, id := range r.order {
		spec := r.specs[id].spec
		out = append(out, Plugin{ID: spec.ID, Summary: spec.Summary, Floor: spec.Floor})
	}
	return out
}
