package loader

import (
	"context"
	"fmt"
)

// Selector answers which compiled plugins the deployment turns off. It is the
// loader's one inversion: the selection plugin reads configuration this package
// never sees.
type Selector interface {
	Disabled(ctx context.Context) ([]string, error)
}

// Selection is the service every composition root may provide once.
var Selection = NewService[Selector]("loader.selection")

// selectionProvider returns the one compiled binding of Selection and, when the
// bindings are ambiguous, the problem that prevents reading a selection.
func selectionProvider(r *Registry) (Selector, string) {
	box, ok := r.services[Selection.name].(*serviceBox[Selector])
	if !ok {
		return nil, ""
	}
	if len(box.entries) != 1 {
		return nil, fmt.Sprintf("selection: %d providers, want one", len(box.entries))
	}
	return box.entries[0].value, ""
}

// selectionOwner is the plugin that provides Selection, or "" when the binding
// is absent or ambiguous.
func selectionOwner(r *Registry) string {
	box, ok := r.services[Selection.name].(*serviceBox[Selector])
	if !ok || len(box.entries) != 1 {
		return ""
	}
	return box.entries[0].owner
}

// ApplySelection computes the active set from the disabled list and returns the
// problems that made a disable inapplicable: an unknown id, a library, a plugin
// in the effective floor closure — declared Floor or a transitive dependency of
// one — a plugin an active plugin depends on, or the selection provider itself.
// A refused disable stays active — a selection problem never narrows the set
// below what the deployment needs, and an operational role refuses on any
// problem instead of running a partial one.
func ApplySelection(r *Registry, disabled []string) []string {
	r.selectionProblems = nil
	r.active = make(map[string]bool, len(r.specs))
	provider := selectionOwner(r)
	floor := floorSet(r)
	off := make(map[string]bool, len(disabled))
	seen := make(map[string]bool, len(disabled))
	for _, id := range disabled {
		if seen[id] {
			continue
		}
		seen[id] = true
		entry, ok := r.specs[id]
		if !ok {
			r.selectionProblems = append(r.selectionProblems, fmt.Sprintf("selection: unknown plugin %q", id))
			continue
		}
		switch {
		case entry.library:
			r.selectionProblems = append(r.selectionProblems,
				fmt.Sprintf("selection: library plugin %q is always active and cannot be disabled", id))
		case entry.spec.Floor:
			r.selectionProblems = append(r.selectionProblems, fmt.Sprintf("selection: floor plugin %q cannot be disabled", id))
		case floor[id]:
			r.selectionProblems = append(r.selectionProblems,
				fmt.Sprintf("selection: plugin %q is a floor dependency and cannot be disabled", id))
		case id == provider:
			r.selectionProblems = append(r.selectionProblems, fmt.Sprintf("selection: the selection provider %q cannot be disabled", id))
		default:
			off[id] = true
		}
	}
	for changed := true; changed; {
		changed = false
		for _, id := range r.order {
			if off[id] {
				continue
			}
			deps, _ := Dependencies(r, id)
			for _, dep := range deps {
				if !off[dep] {
					continue
				}
				r.selectionProblems = append(r.selectionProblems,
					fmt.Sprintf("selection: plugin %q cannot be disabled while active plugin %q depends on it", dep, id))
				delete(off, dep)
				changed = true
			}
		}
	}
	for _, id := range r.order {
		if !off[id] {
			r.active[id] = true
		}
	}
	return r.selectionProblems
}
