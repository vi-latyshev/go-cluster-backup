package loader

import (
	"fmt"
	"maps"
	"slices"
)

// Health reports every intrinsic problem of the compiled and selected registry:
// the selection it refused, broken effective dependencies, a dependency cycle,
// a missing default role, lifecycle hooks naming no role, and every kind's
// uniqueness and Validate check. A dependency of a floor plugin is not a
// problem: it joins the effective floor closure and is mandatory like its
// dependent. The result is sorted, so two reports of the same registry read
// alike. It performs no I/O and changes nothing.
func Health(r *Registry) []string {
	problems := slices.Clone(r.selectionProblems)
	for _, id := range r.order {
		deps, _ := Dependencies(r, id)
		for _, dep := range deps {
			switch {
			case dep == id:
				problems = append(problems, fmt.Sprintf("plugin %q depends on itself", id))
			case r.specs[dep] == nil:
				problems = append(problems, fmt.Sprintf("plugin %q depends on unregistered plugin %q", id, dep))
			}
		}
	}
	if _, err := loadOrder(r); err != nil {
		problems = append(problems, err.Error())
	}
	if r.def == nil {
		problems = append(problems, "no default role is registered")
	}
	for _, id := range r.order {
		for role := range r.hooks[id] {
			if r.roles[role] == nil {
				problems = append(problems, fmt.Sprintf("plugin %q has a lifecycle hook for unregistered role %q", id, role))
			}
		}
	}
	for _, name := range slices.Sorted(maps.Keys(r.kinds)) {
		problems = append(problems, r.kinds[name].(checked).problems(r.isActive)...)
	}
	for _, name := range slices.Sorted(maps.Keys(r.services)) {
		problems = append(problems, r.services[name].(checked).problems(r.isActive)...)
	}
	slices.Sort(problems)
	return problems
}
