package assembly

import (
	"context"
	"errors"
	"fmt"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/lifecycle"
	"io"
	"strings"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/inventory"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/planning"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/workflow/restore"
)

// The restore methods below are the assembly's temporary wiring of the restore
// workflow: they project the assembly's built plugins into the workflow's typed
// options and call it. They disappear once the Floor workflow builds the
// restore workflow directly.

func (e *Assembly) restoreWorkflow() *restore.Workflow {
	storages := make([]restore.Storage, 0, len(e.storages))
	for _, sb := range e.storages {
		storages = append(storages, restore.Storage{
			Name:            sb.name,
			Port:            sb.dest.Port(),
			CallParallelism: sb.cfg.CallParallelism,
			Log:             e.deps("storage", sb.cfg.Type, sb.name).Log,
		})
	}
	types := make(map[string]string, len(e.processors))
	for name := range e.processors {
		types[name] = e.cfg.Processors[name].Type
	}
	o := restore.Options{
		Layout:         e.layout,
		Policy:         e.restoreRetry,
		Storages:       storages,
		Processors:     e.processors,
		ProcessorTypes: types,
		Targets:        e,
		Log:            e.log,
	}
	if e.cfg.Restore != nil {
		o.Configured = true
		o.SpoolDir = e.restoreSpoolDir
		o.Reserve = e.restoreReserve
		o.Order = e.restoreOrder
		o.ApplyOptions = e.cfg.Restore.Apply
	}
	return restore.New(o)
}

// ResolveFetch describes what a fetch would download.
func (e *Assembly) ResolveFetch(ctx context.Context, req restore.FetchRequest) (restore.FetchPlan, error) {
	return e.restoreWorkflow().ResolveFetch(ctx, req)
}

// Fetch downloads one artifact (or one raw object) into dst.
func (e *Assembly) Fetch(ctx context.Context, req restore.FetchRequest, dst io.Writer) (restore.FetchReport, error) {
	return e.restoreWorkflow().Fetch(ctx, req, dst)
}

// Apply writes one fetched and verified artifact to a target.
func (e *Assembly) Apply(ctx context.Context, req restore.ApplyRunRequest) (source.ApplyReport, error) {
	return e.restoreWorkflow().Apply(ctx, req)
}

// Verify walks the requested depth for one artifact or every artifact of a run.
func (e *Assembly) Verify(ctx context.Context, req restore.VerifyRequest) ([]restore.VerifyResult, error) {
	return e.restoreWorkflow().Verify(ctx, req)
}

// StoredRuns lists the runs every storage holds, including runs without a
// manifest.
func (e *Assembly) StoredRuns(ctx context.Context, names []string) ([]inventory.StoredRunView, error) {
	return e.restoreWorkflow().StoredRuns(ctx, names)
}

// StorageOrder names the storages a fetch walks, in walk order.
func (e *Assembly) StorageOrder(names []string) ([]string, error) {
	return e.restoreWorkflow().StorageOrder(names)
}

// ResolveTarget builds the source an apply or a probed verification writes
// into: a static source instance by name, or "discovered" — the same discovery
// rules find the target, by the same artifact name. The returned closer owns
// every instance the resolution built and is closed by the workflow when the
// call returns.
func (e *Assembly) ResolveTarget(ctx context.Context, target, artifact string) (restore.Target, io.Closer, error) {
	scope := lifecycle.New()
	t, typ, err := e.resolveTarget(ctx, target, artifact, scope)
	if err != nil {
		_ = scope.Close(ctx)
		return restore.Target{}, nil, err
	}
	return restore.Target{Type: typ, Source: t}, closeScope{scope: scope, ctx: ctx}, nil
}

// closeScope adapts the assembly's build scope to io.Closer for the workflow.
type closeScope struct {
	scope *lifecycle.Scope
	ctx   context.Context
}

func (c closeScope) Close() error { return c.scope.Close(c.ctx) }

// under reports whether the envelope is the artifact itself or one of its
// parents: a unit artifact is named "<envelope>/<unit>".
func under(envelope, artifact string) bool {
	return envelope == artifact || strings.HasPrefix(artifact, envelope+"/")
}

// resolveTarget builds the source an apply or a probe writes to: a static
// source instance by name, or "discovered" — the same discovery rules find the
// target, by the same artifact name.
func (e *Assembly) resolveTarget(ctx context.Context, target, artifact string, scope *lifecycle.Scope) (source.Source, string, error) {
	if target == "" {
		return nil, "", errors.New("apply: Target is required")
	}
	if target == "discovered" {
		planner := planning.New(e.restoreRetry)
		for _, db := range e.discoveries {
			// The same loop as a run, with the narrower question: only a
			// retryable problem on the target this restore came to resolve
			// buys another pass.
			found, err := planner.Discover(ctx, db.name, db.inst, planning.TargetUnder(artifact))
			if err != nil {
				return nil, "", fmt.Errorf("apply: discovery %s: %w", db.name, err)
			}
			for _, f := range found {
				// A unit artifact is named "<envelope>/<unit>", so an instance
				// whose envelope prefixes the target is the one that may hold
				// it — every other found object is left alone, unbuilt.
				if !under(f.Artifact.Name, artifact) {
					continue
				}
				if f.Excluded != "" {
					return nil, "", fmt.Errorf("apply: the discovered target for %q is excluded: %s", artifact, f.ExcludedReason)
				}
				if f.Problem != "" {
					return nil, "", fmt.Errorf("apply: the discovered target for %q is unavailable: %s", artifact, f.Problem)
				}
				inst, err := instances.BuildActive(ctx, e.loader, e.reg, source.Kind, f.Type, f.Config, e.deps("source", f.Type, f.Name))
				if err != nil {
					return nil, "", err
				}
				own(scope, inst)
				units := planner.Expand(ctx, planning.Unit{
					Instance: f.Name, Type: f.Type, Source: inst, Artifact: f.Artifact,
				}, planning.Expansion{
					Build: func(ctx context.Context, typ, instance string, raw jsontext.Value) (source.Source, error) {
						return instances.BuildActive(ctx, e.loader, e.reg, source.Kind, typ, raw, e.deps("source", typ, instance))
					},
					Check: e.checkSource,
					Own:   func(c io.Closer) { scope.Own(c) },
				})
				var held []string
				for _, u := range units {
					held = append(held, u.Artifact.Name)
					if u.Artifact.Name != artifact {
						continue
					}
					if u.Problem != nil {
						return nil, "", fmt.Errorf("apply: the discovered target for %q is unavailable: %w", artifact, u.Problem)
					}
					return u.Source, u.Type, nil
				}
				if len(held) > 0 {
					// The instance that declares this artifact is alive and its
					// domain holds other artifacts: what the caller named is a
					// unit the server no longer has — a dropped database, or an
					// instance recreated without it. Creating it is the one step
					// a restore cannot do for itself.
					return nil, "", fmt.Errorf(
						"apply: %s holds no artifact %q (it holds: %s); a unit the server no longer has — a dropped database, a recreated instance — must exist empty and be discovered again before it can be restored into",
						f.Name, artifact, strings.Join(held, ", "))
				}
			}
		}
		return nil, "", fmt.Errorf("apply: no discovered source has artifact %q", artifact)
	}
	for _, sb := range e.sources {
		if sb.name == target {
			return sb.inst, sb.typ, nil
		}
	}
	names := make([]string, 0, len(e.sources))
	for _, sb := range e.sources {
		names = append(names, sb.name)
	}
	return nil, "", fmt.Errorf("apply: unknown target %q (known: %v, or \"discovered\")", target, names)
}
