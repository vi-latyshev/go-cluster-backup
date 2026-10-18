package engine

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"sort"
	"strings"

	"github.com/vi-latyshev/go-cluster-backup/internal/discovery"
	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/processor"
	"github.com/vi-latyshev/go-cluster-backup/internal/source"
)

// PreflightResult is one instance's preflight outcome: its kind, its instance
// name, and what is wrong with it — a failed Check, or an external binary its
// plugin type requires that PATH does not have. An object a discovery excluded
// is a result of its own: OK, carrying the decision instead of an error, and
// nothing of it is built.
type PreflightResult struct {
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

// PreflightTarget names one declared instance a preflight can select.
type PreflightTarget struct{ Kind, Name string }

// PreflightRequest selects what a preflight checks. Instances empty means
// every declared instance — the behaviour of a preflight that selects nothing
// in particular. A discovery brings the sources it finds along: they are never
// nameable, because their names exist only inside the rules.
type PreflightRequest struct{ Instances []PreflightTarget }

// PreflightTargets lists every declared instance a preflight can select — what
// the CLI validates a selection against and diffs for its "not checked"
// report. It checks nothing and reads nothing (no Check, no binary lookup, no
// Discover), so the list is the same before and after any preflight ran, in a
// stable order. A stage's own sources are listed under their own kind, because
// the same name may be declared in both sections — the staged source of the run
// side and the source the stage produces it with.
func (e *Engine) PreflightTargets(ctx context.Context) []PreflightTarget {
	if ctx.Err() != nil {
		return nil
	}
	instances := e.instancesFor(scopeAll)
	out := make([]PreflightTarget, 0, len(instances))
	for _, inst := range instances {
		out = append(out, PreflightTarget{Kind: inst.kind, Name: inst.name})
	}
	return out
}

// checkScope names the part of one configuration a command checks: a run checks
// everything but the stage's own sources — they belong to another container, with
// mounts this pod does not have — a stage checks its own sources and the
// processors their pipelines name, and `validate --preflight` checks all of it.
type checkScope uint8

const (
	scopeAll checkScope = iota
	scopeRun
	scopeStage
)

// PreflightResults checks the selected instances — the declared ones and the
// sources the selected discoveries find — and returns one result per instance,
// in a stable order, without stopping at the first failure: the caller decides
// what a failure means. The selection is applied before any Check, binary
// lookup or Discover runs, so an unselected instance is not touched at all; an
// unselected discovery contributes no rows. A discovered source is built
// exactly as a run would build it, so a literal its own plugin rejects fails
// `validate --preflight` rather than the 04:20 run.
func (e *Engine) PreflightResults(ctx context.Context, req PreflightRequest) []PreflightResult {
	rows := e.preflight(ctx, e.checkPolicy(), scopeAll, true, true, req.Instances)
	out := make([]PreflightResult, 0, len(rows))
	for _, r := range rows {
		res := PreflightResult{Kind: r.kind, Instance: r.name, OK: r.err == nil,
			Excluded: r.excluded, ExcludedReason: r.reason}
		if r.err != nil {
			res.Error = r.err.Error()
		}
		out = append(out, res)
	}
	return out
}

// preflightRow is one instance's outcome with the error itself: the error class
// decides how a caller scopes a failure, and a string cannot carry it. A row
// that carries an exclusion has no error: it is a decision, not a fault.
type preflightRow struct {
	kind     string
	name     string
	err      error
	excluded model.ExclusionKind
	reason   string
}

// checkPolicy is the retry policy a preflight uses: the run's, or the restore's
// on a config without a run section.
func (e *Engine) checkPolicy() retryPolicy {
	if e.cfg.Run == nil {
		return e.restoreRetry
	}
	return e.retry
}

// preflight checks the declared instances and, when discovered is set, runs
// every selected discovery, builds each found source and checks it. A run
// walks its discoveries itself, so its scoped checks pass false and do that
// work once — with a nil selection, which is every instance.
//
// selection names the declared instances to check; nil means all of them. A
// name that is not a declared instance is a caller error and gets its own
// failed row, never silence: the preflight reports what it was asked for.
func (e *Engine) preflight(ctx context.Context, p retryPolicy, scope checkScope, wantDecode, discovered bool, selection []PreflightTarget) []preflightRow {
	instances := e.instancesFor(scope)
	var selected map[instanceKey]bool
	if len(selection) > 0 {
		selected = make(map[instanceKey]bool, len(selection))
		for _, t := range selection {
			selected[instanceKey{t.Kind, t.Name}] = true
		}
	}
	out := make([]preflightRow, 0, len(instances))
	if selected != nil {
		known := make([]string, 0, len(instances))
		for _, inst := range instances {
			known = append(known, inst.kind+"/"+inst.name)
		}
		for _, t := range selection {
			if _, ok := knownInstance(instances, t); !ok {
				out = append(out, preflightRow{kind: t.Kind, name: t.Name,
					err: fmt.Errorf("unknown instance %s/%s (known: %s)", t.Kind, t.Name, strings.Join(known, ", "))})
			}
		}
	}
	for _, inst := range instances {
		if selected != nil && !selected[instanceKey{inst.kind, inst.name}] {
			continue
		}
		row := preflightRow{kind: inst.kind, name: inst.name}
		if missing := e.missingBinaries(inst.kind, inst.name); len(missing) > 0 {
			row.err = fmt.Errorf("required binaries are not on PATH: %s", strings.Join(missing, ", "))
			out = append(out, row)
			continue
		}
		if inst.checker != nil {
			_, err := attempt(ctx, p, "check "+inst.kind+" "+inst.name, func(ctx context.Context, _ int) (struct{}, error) {
				return struct{}{}, safeCall(func() error { return inst.checker.Check(ctx) })
			})
			row.err = err
		}
		// A processor's decode side is checked when the caller asks for it, and
		// nowhere else: `validate --preflight` asks whether the key material is
		// at hand, while a run or a stage — which only encode — must not fail
		// because a private key is not mounted in this pod.
		if wantDecode && row.err == nil && inst.decoder != nil {
			_, err := attempt(ctx, p, "check decode "+inst.kind+" "+inst.name, func(ctx context.Context, _ int) (struct{}, error) {
				return struct{}{}, safeCall(func() error { return inst.decoder.CheckDecode(ctx) })
			})
			row.err = err
		}
		out = append(out, row)
	}
	if discovered {
		out = e.runDiscoveries(ctx, p, out, selected)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].kind != out[j].kind {
			return out[i].kind < out[j].kind
		}
		return out[i].name < out[j].name
	})
	return out
}

// knownInstance reports whether a target names one of the built instances;
// instances() is the authority, so the list has one source.
func knownInstance(instances []builtInstance, t PreflightTarget) (builtInstance, bool) {
	for _, inst := range instances {
		if inst.kind == t.Kind && inst.name == t.Name {
			return inst, true
		}
	}
	return builtInstance{}, false
}

// runDiscoveries fills in what every discovery sees, one row per instance: a
// discovery whose own Check failed keeps its one row, annotated — nothing can
// be built from it, so its Discover is not run and no second row appears; a
// discovery whose Discover fails turns its own row into the failed one instead
// of adding another; and every found source gets its own row, built and
// checked the way the run would. The count of rows is therefore the count of
// instances, declared and discovered.
func (e *Engine) runDiscoveries(ctx context.Context, p retryPolicy, rows []preflightRow, selected map[instanceKey]bool) []preflightRow {
	index := make(map[instanceKey]int, len(rows))
	for i, row := range rows {
		index[instanceKey{row.kind, row.name}] = i
	}
	for _, db := range e.discoveries {
		if selected != nil && !selected[instanceKey{"discovery", db.name}] {
			// Unselected: no row, no Discover, no found sources.
			continue
		}
		i, ok := index[instanceKey{"discovery", db.name}]
		if !ok {
			continue
		}

		if rows[i].err != nil {
			rows[i].err = fmt.Errorf("%w; its sources were not checked", rows[i].err)
			continue
		}
		found, err := e.discoverAll(ctx, p, db.name, db.inst, retryableProblem)
		if err != nil {
			rows[i].err = err
			continue
		}
		for _, f := range found {
			rows = append(rows, e.checkFound(ctx, p, f)...)
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
func (e *Engine) checkFound(ctx context.Context, p retryPolicy, f discovery.Found) []preflightRow {
	if f.Excluded != "" {
		return []preflightRow{{kind: "source", name: f.Name, excluded: f.Excluded, reason: f.ExcludedReason}}
	}
	row := preflightRow{kind: "source", name: f.Name}
	if f.Problem != "" {
		row.err = errors.New(f.Problem)
		return []preflightRow{row}
	}
	scope := &closerScope{}
	defer func() { _ = scope.close() }()
	s, err := plugin.Build(ctx, e.reg, source.Kind, f.Type, f.Config, e.deps("source", f.Type, f.Name))
	if err != nil {
		row.err = err
		return []preflightRow{row}
	}
	scope.track(s)
	if err := e.checkSource(ctx, s, f.Name); err != nil {
		row.err = err
		return []preflightRow{row}
	}
	units := e.expandUnits(ctx, p, expandedUnit{instance: f.Name, srcType: f.Type, src: s, artifact: f.Artifact}, scope)
	rows := make([]preflightRow, 0, len(units))
	for _, u := range units {
		// The parent's own unit is already checked above and carries no
		// problem; a unit reports the failure of building or checking it.
		rows = append(rows, preflightRow{kind: "source", name: u.instance, err: u.problem})
	}
	return rows
}

// binaryErrors is what stops a run or a stage before any plugin work: a binary
// a declared plugin requires and PATH does not have is the deployment, not a
// blip, and no retry fixes it.
func (e *Engine) binaryErrors(scope checkScope) []error {
	var errs []error
	for _, inst := range e.instancesFor(scope) {
		if missing := e.missingBinaries(inst.kind, inst.name); len(missing) > 0 {
			errs = append(errs, fmt.Errorf("%s %s: required binaries are not on PATH: %s",
				inst.kind, inst.name, strings.Join(missing, ", ")))
		}
	}
	return errs
}

// failedChecks is the scoped half of a preflight: an instance whose Check
// failed fails its own work, and the run's other instances are unaffected.
type failedChecks map[instanceKey]error

type instanceKey struct{ kind, name string }

func (c failedChecks) err(kind, name string) error { return c[instanceKey{kind: kind, name: name}] }

// scopedChecks runs every declared instance's Check under the given retry
// policy — a transient check error is retried like the instance's own work —
// and keeps the failures that survive, per instance, error class included.
func (e *Engine) scopedChecks(ctx context.Context, p retryPolicy, scope checkScope) failedChecks {
	out := failedChecks{}
	for _, r := range e.preflight(ctx, p, scope, false, false, nil) {
		if r.err != nil {
			out[instanceKey{kind: r.kind, name: r.name}] = r.err
		}
	}
	return out
}

// builtInstance is one built instance as a preflight sees it.
type builtInstance struct {
	kind    string
	name    string
	checker plugin.Checker // nil when the plugin implements no Check
	// decoder is the instance's restore-side check, set for the processors that
	// implement it: what `validate --preflight` asks about key material.
	decoder processor.DecodeChecker
}

// instancesFor lists the built instances one command checks, in a stable order:
// what a preflight checks and what a command refuses to start without the
// binaries of. The kind of a stage source is stage_source: the staged source of
// the run side is a different instance with the same name.
func (e *Engine) instancesFor(scope checkScope) []builtInstance {
	var out []builtInstance
	add := func(kind, name string, v any) {
		inst := builtInstance{kind: kind, name: name}
		if c, ok := v.(plugin.Checker); ok {
			inst.checker = c
		}
		if d, ok := v.(processor.DecodeChecker); ok {
			inst.decoder = d
		}
		out = append(out, inst)
	}
	if scope == scopeStage {
		// A stage checks its own sources and the processors their pipelines name
		// — nothing else of the file belongs to this container.
		wanted := map[string]bool{}
		for _, sb := range e.stageSources {
			add("stage_source", sb.name, sb.inst)
			for _, ref := range sb.refs {
				wanted[ref] = true
			}
		}
		for _, name := range sortedKeys(e.processors) {
			if wanted[name] {
				add("processor", name, e.processors[name])
			}
		}
		sortInstances(out)
		return out
	}
	for _, name := range sortedKeys(e.processors) {
		if p, ok := e.processors[name]; ok {
			add("processor", name, p)
		}
	}
	for _, sb := range e.storages {
		add("storage", sb.name, sb.inst)
	}
	for _, sb := range e.sources {
		add("source", sb.name, sb.inst)
	}
	for _, db := range e.discoveries {
		add("discovery", db.name, db.inst)
	}
	for _, nb := range e.notifiers {
		add("notifier", nb.name, nb.inst)
	}
	if e.catalog != nil {
		add("catalog", "catalog", e.catalog)
	}
	if scope == scopeAll {
		for _, sb := range e.stageSources {
			add("stage_source", sb.name, sb.inst)
		}
	}
	sortInstances(out)
	return out
}

func sortInstances(out []builtInstance) {
	sort.Slice(out, func(i, j int) bool {
		if out[i].kind != out[j].kind {
			return out[i].kind < out[j].kind
		}
		return out[i].name < out[j].name
	})
}

// instanceType is the plugin type one built instance was declared with.
func (e *Engine) instanceType(kind, name string) string {
	switch kind {
	case "processor":
		return e.cfg.Processors[name].Type
	case "stage_source":
		if e.cfg.Stage != nil {
			return e.cfg.Stage.Sources[name].Type
		}
	case "storage":
		return e.cfg.Storages[name].Type
	case "source":
		return e.cfg.Sources[name].Type
	case "discovery":
		return e.cfg.Discoveries[name].Type
	case "notifier":
		return e.cfg.Notifiers[name].Type
	case "catalog":
		if e.cfg.Catalog != nil {
			return e.cfg.Catalog.Type
		}
	}
	return ""
}

// missingBinaries lists the external binaries one instance's plugin type
// declares and PATH does not have. `doctor` asks the same question of a whole
// distribution at image build time.
func (e *Engine) missingBinaries(kind, name string) []string {
	var missing []string
	for _, bin := range e.reg.Requires(kind, e.instanceType(kind, name)) {
		if _, err := exec.LookPath(bin); err != nil {
			missing = append(missing, bin)
		}
	}
	return missing
}
