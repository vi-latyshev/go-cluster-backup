package loader

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"time"
)

// Session is one role invocation: the registry, the arguments left after the
// role name and the process streams. Main normalizes a nil stream.
type Session struct {
	Registry *Registry
	Args     []string
	Stdin    io.Reader
	Stdout   io.Writer
	Stderr   io.Writer
}

func (s Session) logf(format string, args ...any) {
	_, _ = fmt.Fprintf(s.Stderr, format+"\n", args...)
}

// RoleSpec registers a process entrypoint. Name is the token that selects the
// role in the first argument; role names and command names are disjoint. Run
// returns the process exit code. Disabled is the outcome when the owning plugin
// is not active: it must refuse — 1 for an operational refusal, 2 for a usage
// error — and never fall back to the default role. Diagnostic marks the roles
// that run despite Health problems so they can report them; every other role
// refuses before Run.
type RoleSpec struct {
	Name       string
	Default    bool
	Diagnostic bool
	Run        func(context.Context, Session) int
	Disabled   func(Session) int
}

type roleEntry struct {
	spec  RoleSpec
	owner string
}

// Role registers one role for the plugin being added.
func Role(r *Registry, s RoleSpec) error {
	if r.current == "" {
		return errors.New("loader: a role is registered outside Add")
	}
	if s.Name == "" {
		return errors.New("loader: role name is empty")
	}
	if s.Run == nil {
		return fmt.Errorf("loader: role %q has no Run", s.Name)
	}
	if s.Disabled == nil {
		return fmt.Errorf("loader: role %q has no Disabled outcome", s.Name)
	}
	if _, dup := r.roles[s.Name]; dup {
		return fmt.Errorf("loader: role %q registered twice", s.Name)
	}
	if s.Default && r.def != nil {
		return fmt.Errorf("loader: role %q is the second default role", s.Name)
	}
	entry := &roleEntry{spec: s, owner: r.current}
	r.roles[s.Name] = entry
	r.roleOrder = append(r.roleOrder, s.Name)
	if s.Default {
		r.def = entry
	}
	return nil
}

// Roles is the compiled role table: each providing plugin id mapped to the
// role names it registers, in registration order. A disabled owner stays
// listed, so an offline check can tell a role no plugin provides from one the
// deployment selected away.
func Roles(r *Registry) map[string][]string {
	out := make(map[string][]string, len(r.roles))
	for _, name := range r.roleOrder {
		owner := r.roles[name].owner
		out[owner] = append(out[owner], name)
	}
	return out
}

// Lifecycle is a plugin's start, quiesce and stop work in one role. Budget
// bounds each phase; zero means no extra budget.
type Lifecycle struct {
	Start   func(context.Context) error
	Quiesce func(context.Context) error
	Stop    func(context.Context) error
	Budget  time.Duration
}

// OnRole binds a lifecycle to one role for the plugin being added. Only the
// hooks of the selected role run, in dependency order.
func OnRole(r *Registry, role string, l Lifecycle) error {
	if r.current == "" {
		return errors.New("loader: a lifecycle is registered outside Add")
	}
	if role == "" {
		return errors.New("loader: lifecycle role is empty")
	}
	if l.Start == nil && l.Quiesce == nil && l.Stop == nil {
		return errors.New("loader: lifecycle has no phase")
	}
	if l.Budget < 0 {
		return fmt.Errorf("loader: lifecycle for role %q has a negative budget", role)
	}
	if r.hooks[r.current] == nil {
		r.hooks[r.current] = map[string]Lifecycle{}
	}
	if _, dup := r.hooks[r.current][role]; dup {
		return fmt.Errorf("loader: lifecycle for role %q registered twice", role)
	}
	r.hooks[r.current][role] = l
	return nil
}

type lifecycleEntry struct {
	owner     string
	lifecycle Lifecycle
}

// Ordering is one participant of a dependency-ordered set: its id, the soft
// After hints — an unknown id is skipped — and the hard Needs requirements — an
// unknown id is refused.
type Ordering struct {
	ID    string
	After []string
	Needs []string
}

// Order returns every id once, dependencies before dependents. Ties keep the
// input order. A duplicate id, a Needs id that is not a participant or a cycle
// is refused; an After id that is not a participant is ignored.
func Order(nodes []Ordering) ([]string, error) {
	index := make(map[string]int, len(nodes))
	for i, node := range nodes {
		if node.ID == "" {
			return nil, errors.New("loader: ordering id is empty")
		}
		if _, dup := index[node.ID]; dup {
			return nil, fmt.Errorf("loader: ordering id %q appears twice", node.ID)
		}
		index[node.ID] = i
	}
	out := make([][]int, len(nodes))
	indegree := make([]int, len(nodes))
	for i, node := range nodes {
		for _, id := range node.Needs {
			dep, ok := index[id]
			if !ok {
				return nil, fmt.Errorf("loader: %q needs unknown id %q", node.ID, id)
			}
			out[dep] = append(out[dep], i)
			indegree[i]++
		}
		for _, id := range node.After {
			if dep, ok := index[id]; ok && dep != i {
				out[dep] = append(out[dep], i)
				indegree[i]++
			}
		}
	}
	order := make([]string, 0, len(nodes))
	done := make([]bool, len(nodes))
	for len(order) < len(nodes) {
		next := -1
		for i := range nodes {
			if !done[i] && indegree[i] == 0 {
				next = i
				break
			}
		}
		if next < 0 {
			return nil, fmt.Errorf("loader: dependency cycle includes %q", nodes[slices.Index(done, false)].ID)
		}
		done[next] = true
		order = append(order, nodes[next].ID)
		for _, dependent := range out[next] {
			indegree[dependent]--
		}
	}
	return order, nil
}

// loadOrder orders every compiled plugin by its effective dependencies and soft
// After hints.
func loadOrder(r *Registry) ([]string, error) {
	nodes := make([]Ordering, 0, len(r.order))
	for _, id := range r.order {
		deps, _ := Dependencies(r, id)
		nodes = append(nodes, Ordering{ID: id, After: r.specs[id].spec.After, Needs: deps})
	}
	return Order(nodes)
}

// hooksFor returns the lifecycle hooks of one role in dependency order, falling
// back to registration order when the graph does not order — a diagnostic role
// still has to run and report the cycle. Only active plugins run hooks: a
// disabled plugin's lifecycle is not the deployment's, and its start or stop
// must not touch resources the deployment turned off.
func (r *Registry) hooksFor(role string) []lifecycleEntry {
	order := r.order
	if sorted, err := loadOrder(r); err == nil {
		order = sorted
	}
	var out []lifecycleEntry
	for _, id := range order {
		if !r.isActive(id) {
			continue
		}
		if lifecycle, ok := r.hooks[id][role]; ok {
			out = append(out, lifecycleEntry{owner: id, lifecycle: lifecycle})
		}
	}
	return out
}
