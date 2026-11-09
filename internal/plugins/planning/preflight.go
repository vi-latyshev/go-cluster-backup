package planning

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"sort"
	"strings"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/discovery"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/execution"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/processor"
)

// Result is one instance's preflight outcome: its kind, its instance name, and
// what is wrong with it — a failed Check, or an external binary its plugin type
// requires that PATH does not have. An object a discovery excluded is a result
// of its own: OK, carrying the decision instead of an error, and nothing of it
// is built.
type Result struct {
	Kind     string
	Instance string
	OK       bool
	Error    string
	// Excluded is non-empty for an object the discovery saw and deliberately
	// does not back up: model.ExclusionIgnored or model.ExclusionCovered, with
	// ExcludedReason saying why.
	Excluded       model.ExclusionKind
	ExcludedReason string
}

// Target names one declared instance a preflight can select.
type Target struct{ Kind, Name string }

// Request selects what a preflight checks. Instances empty means every declared
// instance — the behaviour of a preflight that selects nothing in particular. A
// discovery brings the sources it finds along: they are never nameable, because
// their names exist only inside the rules.
type Request struct{ Instances []Target }

// Instance is one built instance a preflight sees, as assembly projects it for
// the scope the command checks.
type Instance struct {
	Kind string
	Name string
	// Check is the plugin's own Check; nil when it implements none.
	Check instances.Checker
	// Decode is the instance's restore-side check, set for the processors that
	// implement it: what `validate --preflight` asks about key material.
	Decode processor.DecodeChecker
}

// Discoverer is one built discovery the preflight runs.
type Discoverer struct {
	Name     string
	Instance discovery.Discoverer
}

// Binaries lists the external programs one instance's plugin type declares;
// assembly resolves the instance type and reports the missing ones.
type Binaries func(kind, name string) []string

// Options are the preflight's typed collaborators, projected by assembly.
type Options struct {
	// Instances lists the built instances of the scope the command checks, in
	// any order.
	Instances []Instance
	// Discoveries are the built discoveries the scope checks.
	Discoveries []Discoverer
	// Binaries is the declared-binary lookup for one instance.
	Binaries Binaries
	// Build constructs one found source the way the run would.
	Build SourceFactory
	// Check runs a built source's Check; nil skips it.
	Check SourceCheck
	// WantDecode asks every processor's restore-side check too.
	WantDecode bool
	// Discovered runs every discovery and checks the sources it finds.
	Discovered bool
}

// Row is one instance's outcome with the error itself: the error class decides
// how a caller scopes a failure, and a string cannot carry it. A row that
// carries an exclusion has no error: it is a decision, not a fault.
type Row struct {
	Kind     string
	Name     string
	Err      error
	Excluded model.ExclusionKind
	Reason   string
}

// Targets lists every built instance a preflight can select — what the CLI
// validates a selection against and diffs for its "not checked" report. It
// checks nothing and reads nothing (no Check, no binary lookup, no Discover), so
// the list is the same before and after any preflight ran, in a stable order.
func Targets(ctx context.Context, instances []Instance) []Target {
	if ctx.Err() != nil {
		return nil
	}
	out := make([]Target, 0, len(instances))
	for _, inst := range instances {
		out = append(out, Target{Kind: inst.Kind, Name: inst.Name})
	}
	sortTargets(out)
	return out
}

// Results checks every built instance and the sources the discoveries find, and
// returns one result per instance, in a stable order, without stopping at the
// first failure: the caller decides what a failure means. The selection is
// applied before any Check, binary lookup or Discover runs, so an unselected
// instance is not touched at all; an unselected discovery contributes no rows. A
// discovered source is built exactly as a run would build it, so a literal its
// own plugin rejects fails `validate --preflight` rather than the 04:20 run.
func (p Planner) Results(ctx context.Context, req Request, o Options) []Result {
	o.WantDecode, o.Discovered = true, true
	rows := p.Check(ctx, req.Instances, o)
	out := make([]Result, 0, len(rows))
	for _, r := range rows {
		res := Result{Kind: r.Kind, Instance: r.Name, OK: r.Err == nil,
			Excluded: r.Excluded, ExcludedReason: r.Reason}
		if r.Err != nil {
			res.Error = r.Err.Error()
		}
		out = append(out, res)
	}
	return out
}

// Check checks the selected instances and, when Discovered is set, runs every
// selected discovery, builds each found source and checks it. A run walks its
// discoveries itself, so its scoped checks leave Discovered false and do that
// work once — with a nil selection, which is every instance.
//
// selection names the declared instances to check; nil means all of them. A
// name that is not a listed instance is a caller error and gets its own failed
// row, never silence: the preflight reports what it was asked for.
func (p Planner) Check(ctx context.Context, selection []Target, o Options) []Row {
	var selected map[Target]bool
	if len(selection) > 0 {
		selected = make(map[Target]bool, len(selection))
		for _, t := range selection {
			selected[t] = true
		}
	}
	out := make([]Row, 0, len(o.Instances))
	if selected != nil {
		known := make([]string, 0, len(o.Instances))
		for _, inst := range o.Instances {
			known = append(known, inst.Kind+"/"+inst.Name)
		}
		for _, t := range selection {
			if !containsInstance(o.Instances, t) {
				out = append(out, Row{Kind: t.Kind, Name: t.Name,
					Err: fmt.Errorf("unknown instance %s/%s (known: %s)", t.Kind, t.Name, strings.Join(known, ", "))})
			}
		}
	}
	for _, inst := range o.Instances {
		if selected != nil && !selected[Target{inst.Kind, inst.Name}] {
			continue
		}
		row := Row{Kind: inst.Kind, Name: inst.Name}
		if missing := missingBinaries(o.Binaries, inst.Kind, inst.Name); len(missing) > 0 {
			row.Err = fmt.Errorf("required binaries are not on PATH: %s", strings.Join(missing, ", "))
			out = append(out, row)
			continue
		}
		if inst.Check != nil {
			_, err := execution.Attempt(ctx, p.policy, "check "+inst.Kind+" "+inst.Name, func(ctx context.Context, _ int) (struct{}, error) {
				return struct{}{}, execution.SafeCall(func() error { return inst.Check.Check(ctx) })
			})
			row.Err = err
		}
		// A processor's decode side is checked when the caller asks for it, and
		// nowhere else: `validate --preflight` asks whether the key material is at
		// hand, while a run or a stage — which only encode — must not fail because
		// a private key is not mounted in this pod.
		if o.WantDecode && row.Err == nil && inst.Decode != nil {
			_, err := execution.Attempt(ctx, p.policy, "check decode "+inst.Kind+" "+inst.Name, func(ctx context.Context, _ int) (struct{}, error) {
				return struct{}{}, execution.SafeCall(func() error { return inst.Decode.CheckDecode(ctx) })
			})
			row.Err = err
		}
		out = append(out, row)
	}
	if o.Discovered {
		out = p.runDiscoveries(ctx, o, out, selected)
	}
	sortRows(out)
	return out
}

// BinaryErrors is what stops a run or a stage before any plugin work: a binary a
// declared plugin requires and PATH does not have is the deployment, not a
// blip, and no retry fixes it.
func BinaryErrors(instances []Instance, binaries Binaries) []error {
	var errs []error
	for _, inst := range instances {
		if missing := missingBinaries(binaries, inst.Kind, inst.Name); len(missing) > 0 {
			errs = append(errs, fmt.Errorf("%s %s: required binaries are not on PATH: %s",
				inst.Kind, inst.Name, strings.Join(missing, ", ")))
		}
	}
	return errs
}

// containsInstance reports whether a target names one of the built instances;
// the projected list is the authority, so the list has one source.
func containsInstance(instances []Instance, t Target) bool {
	for _, inst := range instances {
		if inst.Kind == t.Kind && inst.Name == t.Name {
			return true
		}
	}
	return false
}

// runDiscoveries fills in what every discovery sees, one row per instance: a
// discovery whose own Check failed keeps its one row, annotated — nothing can be
// built from it, so its Discover is not run and no second row appears; a
// discovery whose Discover fails turns its own row into the failed one instead
// of adding another; and every found source gets its own row, built and checked
// the way the run would. The count of rows is therefore the count of instances,
// declared and discovered.
func (p Planner) runDiscoveries(ctx context.Context, o Options, rows []Row, selected map[Target]bool) []Row {
	index := make(map[Target]int, len(rows))
	for i, row := range rows {
		index[Target{row.Kind, row.Name}] = i
	}
	for _, db := range o.Discoveries {
		if selected != nil && !selected[Target{"discovery", db.Name}] {
			// Unselected: no row, no Discover, no found sources.
			continue
		}
		i, ok := index[Target{"discovery", db.Name}]
		if !ok {
			continue
		}
		if rows[i].Err != nil {
			rows[i].Err = fmt.Errorf("%w; its sources were not checked", rows[i].Err)
			continue
		}
		found, err := p.Discover(ctx, db.Name, db.Instance, AnyRetryable)
		if err != nil {
			rows[i].Err = err
			continue
		}
		for _, f := range found {
			rows = append(rows, p.checkFound(ctx, f, o)...)
		}
	}
	return rows
}

// checkFound builds one found source and runs its Check, closing what it built:
// preflight leaves no instance behind. An excluded object is a row of its own
// with no build at all — the decision is the whole result. An instance whose
// domain holds several artifacts reports one row per unit: the parent's own
// check is the instance's, and each unit is built and checked the way a run
// would build it.
func (p Planner) checkFound(ctx context.Context, f discovery.Found, o Options) []Row {
	if f.Excluded != "" {
		return []Row{{Kind: "source", Name: f.Name, Excluded: f.Excluded, Reason: f.ExcludedReason}}
	}
	row := Row{Kind: "source", Name: f.Name}
	if f.Problem != "" {
		row.Err = errors.New(f.Problem)
		return []Row{row}
	}
	var owned []io.Closer
	defer func() {
		for i := len(owned) - 1; i >= 0; i-- {
			_ = owned[i].Close()
		}
	}()
	own := func(c io.Closer) { owned = append(owned, c) }
	s, err := o.Build(ctx, f.Type, f.Name, f.Config)
	if err != nil {
		row.Err = err
		return []Row{row}
	}
	if c, ok := s.(io.Closer); ok {
		own(c)
	}
	if o.Check != nil {
		if err := o.Check(ctx, s, f.Name); err != nil {
			row.Err = err
			return []Row{row}
		}
	}
	units := p.Expand(ctx, Unit{Instance: f.Name, Type: f.Type, Source: s, Artifact: f.Artifact},
		Expansion{Build: o.Build, Check: o.Check, Own: own})
	rows := make([]Row, 0, len(units))
	for _, u := range units {
		// The parent's own unit is already checked above and carries no
		// problem; a unit reports the failure of building or checking it.
		rows = append(rows, Row{Kind: "source", Name: u.Instance, Err: u.Problem})
	}
	return rows
}

// missingBinaries lists the external binaries one instance's plugin type declares
// and PATH does not have.
func missingBinaries(binaries Binaries, kind, name string) []string {
	if binaries == nil {
		return nil
	}
	var missing []string
	for _, bin := range binaries(kind, name) {
		if _, err := exec.LookPath(bin); err != nil {
			missing = append(missing, bin)
		}
	}
	return missing
}

func sortRows(out []Row) {
	sort.Slice(out, func(i, j int) bool {
		if out[i].Kind != out[j].Kind {
			return out[i].Kind < out[j].Kind
		}
		return out[i].Name < out[j].Name
	})
}

func sortTargets(out []Target) {
	sort.Slice(out, func(i, j int) bool {
		if out[i].Kind != out[j].Kind {
			return out[i].Kind < out[j].Kind
		}
		return out[i].Name < out[j].Name
	})
}
