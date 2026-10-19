// Package engine is the use-case layer: it reads the config, builds every plugin
// through the registry, and runs the backup pipeline. It never calls os.Exit,
// holds no globals, and a run is repeatable in one process.
//
// The facade is the engine's public API; its first consumer is the CLI, and a
// future UI or HTTP trigger calls the same methods with the same flat data.
package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strings"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/catalog"
	"github.com/vi-latyshev/go-cluster-backup/internal/discovery"
	"github.com/vi-latyshev/go-cluster-backup/internal/human"
	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/notifier"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/processor"
	"github.com/vi-latyshev/go-cluster-backup/internal/retention"
	"github.com/vi-latyshev/go-cluster-backup/internal/source"
	"github.com/vi-latyshev/go-cluster-backup/internal/storage"
)

type RunRequest struct {
	// Kind is the run's identity: scheduled or manual. It is required — an
	// empty or unknown value is refused before any write — and it is not a
	// configuration profile: both kinds read the same file.
	Kind model.RunKind
	// DryRun resolves discovery, pipelines and the secret invariant, and writes
	// nothing anywhere.
	DryRun bool
	// ConfigPath is the configuration file the caller read, named in the run's
	// opening log line so a log read months later says which file produced it.
	// The engine reads no file and derives nothing from it.
	ConfigPath string
}

// StageRequest has no fields of its own: the stage directory is config
// (stage.dir), the one place it lives, and ConfigPath is what the opening log
// line names.
type StageRequest struct {
	// ConfigPath is the configuration file the caller read, named in the stage's
	// opening log line.
	ConfigPath string
}

type storageBinding struct {
	name string
	inst storage.Storage
	cfg  StorageInstance
	// retention is this storage's policy per run kind: the engine partitions
	// runs by kind and calls each partition's policy with a homogeneous list, so
	// a manual run never takes a scheduled slot. A kind absent here has no
	// policy — a scheduled run refuses the storage, a manual run is refused.
	retention map[model.RunKind]retention.Retention
}

type sourceBinding struct {
	name string
	// checkKind is the kind this binding is checked under: "source" for the run
	// side, "stage_source" for the stage's own — the same name may be declared in
	// both sections, and one's failed check must never fail the other's work.
	checkKind string
	typ       string
	inst      source.Source
	artifact  model.Artifact
	pipeline  string
	refs      []string // processor instance names of the pipeline, in order
	procs     []processor.Processor
}

type discoveryBinding struct {
	name string
	inst discovery.Discoverer
}

type notifierBinding struct {
	name string
	inst notifier.Notifier
}

type Engine struct {
	cfg *Config
	reg *plugin.Registry
	log *slog.Logger

	// merged is the configuration exactly as this engine decoded it: the user's
	// file with the distribution's defaults under it, rule sources included.
	// defaultPointers names the leaves whose value came from a defaults
	// document — what `config effective` marks.
	merged          jsontext.Value
	defaultPointers []string

	storages   []*storageBinding
	processors map[string]processor.Processor
	sources    []sourceBinding
	// stageSources are the sources of the stage section: the stage command
	// builds them, the run never does, and their check set follows (checkScope).
	stageSources []sourceBinding
	discoveries  []discoveryBinding
	notifiers    []notifierBinding
	catalog      catalog.Catalog
	// lease is the catalog's single-writer capability, when the catalog has one
	// (catalog.Leaser). A catalog without it leaves manual runs refused.
	lease catalog.Leaser

	retry retryPolicy
	// restoreRetry / restoreSpoolDir / restoreReserve / restoreOrder come from the
	// restore section; a run config without one falls back to its run retry for
	// read-only paths, and every path that needs a spool demands the section.
	restoreRetry    retryPolicy
	restoreSpoolDir string
	restoreReserve  int64
	restoreOrder    []string

	closers []io.Closer
}

// New merges the distribution's defaults under the configuration file, decodes
// the result (strictly, all errors at once), builds every plugin and checks
// every reference between instances. defs may be nil — then the file is read as
// it stands, which is what a test with a complete config wants. It performs no
// network I/O: that is Preflight's job.
func New(ctx context.Context, raw jsontext.Value, defs *Defaults, reg *plugin.Registry, log *slog.Logger) (*Engine, error) {
	if reg == nil {
		return nil, errors.New("engine: registry is nil")
	}
	if log == nil {
		return nil, errors.New("engine: logger is nil")
	}
	merged, defaultPointers, err := defs.Merge(ctx, reg, log, raw)
	if err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	cfg := &Config{}
	if err := plugin.Decode(merged, cfg); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	e := &Engine{
		cfg:             cfg,
		reg:             reg,
		log:             log,
		merged:          merged,
		defaultPointers: defaultPointers,
		processors:      map[string]processor.Processor{},
	}
	if cfg.Restore != nil {
		e.restoreSpoolDir = cfg.Restore.SpoolDir
		e.restoreReserve = cfg.Restore.SpoolReserveBytes.Int64()
		e.restoreOrder = cfg.Restore.StorageOrder
		e.restoreRetry = retryPolicy{
			attempts:     cfg.Restore.Retry.Attempts,
			initial:      cfg.Restore.Retry.InitialDelay.Std(),
			max:          cfg.Restore.Retry.MaxDelay.Std(),
			jitter:       cfg.Restore.Retry.Jitter,
			unclassified: cfg.Restore.UnclassifiedError,
			log:          log,
		}
	}
	if cfg.Run != nil {
		e.retry = retryPolicy{
			attempts:     cfg.Run.Retry.Attempts,
			initial:      cfg.Run.Retry.InitialDelay.Std(),
			max:          cfg.Run.Retry.MaxDelay.Std(),
			jitter:       cfg.Run.Retry.Jitter,
			unclassified: cfg.Run.UnclassifiedError,
			log:          log,
		}
	}
	if cfg.Restore == nil {
		// Read-only paths on a run config (StoredRuns) reuse the run's policy.
		e.restoreRetry = e.retry
	}
	var errs []error
	for _, name := range sortedKeys(cfg.Processors) {
		inst := cfg.Processors[name]
		p, err := e.build(ctx, processor.Kind, "processor", name, inst)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		e.processors[name] = p
	}
	for _, name := range sortedKeys(cfg.Storages) {
		si := cfg.Storages[name]
		inst, err := e.build(ctx, storage.Kind, "storage", name, Instance{Type: si.Type, Config: si.Config})
		if err != nil {
			errs = append(errs, err)
			continue
		}
		// The retention of a storage is an instance of its own per run kind,
		// inside the storage: it carries no name — the storage's names it in a
		// log line and in a config error — and a storage without a scheduled one
		// fetches but cannot run.
		rets := map[model.RunKind]retention.Retention{}
		for _, kind := range sortedKeys(si.Retention) {
			inst := si.Retention[kind]
			r, err := plugin.Build(ctx, e.reg, retention.Kind, inst.Type, inst.Config,
				e.deps("retention", inst.Type, name))
			if err != nil {
				errs = append(errs, fmt.Errorf("/storages/%s/retention/%s: %w", name, kind, err))
				continue
			}
			e.track(r)
			rets[model.RunKind(kind)] = r
		}
		e.storages = append(e.storages, &storageBinding{name: name, inst: inst, cfg: si, retention: rets})
	}
	for _, name := range sortedKeys(cfg.Sources) {
		sb, err := e.buildSource(ctx, "sources", name, cfg.Sources[name])
		if err != nil {
			errs = append(errs, err)
			continue
		}
		e.sources = append(e.sources, sb)
	}
	if cfg.Stage != nil {
		for _, name := range sortedKeys(cfg.Stage.Sources) {
			sb, err := e.buildSource(ctx, "stage.sources", name, cfg.Stage.Sources[name])
			if err != nil {
				errs = append(errs, err)
				continue
			}
			e.stageSources = append(e.stageSources, sb)
		}
	}
	for _, name := range sortedKeys(cfg.Discoveries) {
		inst := cfg.Discoveries[name]
		d, err := e.build(ctx, discovery.Kind, "discovery", name, inst)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		e.discoveries = append(e.discoveries, discoveryBinding{name: name, inst: d})
	}
	errs = append(errs, e.checkDiscoveryRules(ctx)...)
	for _, name := range sortedKeys(cfg.Notifiers) {
		inst := cfg.Notifiers[name]
		n, err := e.build(ctx, notifier.Kind, "notifier", name, inst)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		e.notifiers = append(e.notifiers, notifierBinding{name: name, inst: n})
	}
	if cfg.Catalog != nil {
		c, err := e.build(ctx, catalog.Kind, "catalog", "catalog", *cfg.Catalog)
		if err != nil {
			errs = append(errs, err)
		} else {
			e.catalog = c
			if l, ok := c.(catalog.Leaser); ok {
				e.lease = l
			}
		}
	}
	if err := errors.Join(errs...); err != nil {
		_ = e.Close()
		return nil, err
	}
	return e, nil
}

func (e *Engine) build[T any](ctx context.Context, kind plugin.Kind[T], kindName, name string, inst Instance) (T, error) {
	var zero T
	t, err := plugin.Build(ctx, e.reg, kind, inst.Type, inst.Config, e.deps(kindName, inst.Type, name))
	if err != nil {
		return zero, fmt.Errorf("/%s/%s: %w", sectionName(kindName), name, err)
	}
	e.track(t)
	return t, nil
}

func (e *Engine) buildSource(ctx context.Context, section, name string, si SourceInstance) (sourceBinding, error) {
	s, err := plugin.Build(ctx, e.reg, source.Kind, si.Type, si.Config, e.deps("source", si.Type, name))
	if err != nil {
		return sourceBinding{}, fmt.Errorf("/%s/%s: %w", section, name, err)
	}
	e.track(s)
	procs, refs, err := e.resolvePipeline(ctx, "/"+section+"/"+name+"/pipeline", si.Pipeline)
	if err != nil {
		return sourceBinding{}, err
	}
	// Two facts about a secret artifact are configuration facts, refused here —
	// offline, by validate — and not at 04:20: a source that carries the chain
	// of the process that produced its bytes applies no pipeline of its own
	// (the chain's own seals are checked when it is read), and every other
	// secret source must route through a pipeline that seals.
	if _, ok := s.(source.RecordedChain); ok {
		if len(si.Pipeline) != 0 {
			return sourceBinding{}, fmt.Errorf(
				"/%s/%s: the source carries the chain of the process that produced its bytes (stage); its pipeline must be empty, not %v",
				section, name, si.Pipeline)
		}
	} else if si.Artifact.Sensitivity == model.SensitivitySecret {
		seals, err := e.pipelineSeals(ctx, procs)
		if err != nil {
			return sourceBinding{}, fmt.Errorf("/%s/%s/pipeline: %w", section, name, err)
		}
		if !seals {
			return sourceBinding{}, fmt.Errorf(
				"/%s/%s: a secret artifact's pipeline %v has no sealing processor; the run would refuse it before the first byte",
				section, name, si.Pipeline)
		}
	}
	checkKind := "source"
	if section == "stage.sources" {
		checkKind = "stage_source"
	}
	return sourceBinding{
		name: name, checkKind: checkKind, typ: si.Type, inst: s, pipeline: pipelineName(refs),
		refs: refs, procs: procs,
		artifact: model.Artifact{Name: si.Artifact.Name, Group: si.Artifact.Group,
			Importance: si.Artifact.Importance, Sensitivity: si.Artifact.Sensitivity},
	}, nil
}

// resolvePipeline turns one pipeline — the processor instance names of a source
// or a rule, in encode order — into the instances themselves. A name that is
// not a declared processor is an error naming every valid one; a name may
// repeat, because two passes of one processor are two steps, not a mistake.
func (e *Engine) resolvePipeline(ctx context.Context, path string, names []string) ([]processor.Processor, []string, error) {
	procs := make([]processor.Processor, 0, len(names))
	refs := make([]string, 0, len(names))
	for i, ref := range names {
		p, ok := e.processors[ref]
		if !ok {
			return nil, nil, fmt.Errorf("%s/%d: unknown processor instance %q (known: %v)", path, i, ref, sortedKeys(e.processors))
		}
		// A processor that cannot encode is a fact about the configuration, not
		// about the night: the pipeline is refused here, before anything runs.
		if checker, ok := p.(processor.EncodeChecker); ok {
			if err := checker.CheckEncode(ctx); err != nil {
				return nil, nil, fmt.Errorf("%s/%d: processor %q: %w", path, i, ref, err)
			}
		}
		procs = append(procs, p)
		refs = append(refs, ref)
	}
	return procs, refs, nil
}

// sectionName is the configuration section one kind's instances live under: the
// kind name pluralised, with the one kind whose plural is not its name plus an
// "s" spelled out.
func sectionName(kind string) string {
	if kind == "discovery" {
		return "discoveries"
	}
	return kind + "s"
}

// pipelineName is what the manifest keeps of a pipeline for a human reader: the
// processor instance names, in order, joined by "+". The artifact's actual chain
// is the manifest's own chain, never this string — a restore decodes with the
// chain, so renaming a pipeline never orphans a run.
func pipelineName(refs []string) string { return strings.Join(refs, "+") }

func (e *Engine) deps(kind, typ, name string) plugin.Deps {
	return plugin.Deps{Instance: name, Log: e.log.With("kind", kind, "type", typ, "instance", name)}
}

// event emits one event of this engine's log. The engine NAMES the event and
// hands over its typed attributes; internal/human renders the line, so which
// values a person sees and which stay behind --log-level debug is decided in
// one place. An emitter never builds a sentence, and never spells an object key
// into one.
func (e *Engine) event(ev human.Event) {
	e.log.Log(context.Background(), ev.Severity, ev.Name, slog.Any("event", ev))
}

// at is event on one instance's own logger, so the line carries that instance's
// prefix: "<kind> <type>/<instance>: ".
func (e *Engine) at(kind, typ, name string, ev human.Event) {
	e.deps(kind, typ, name).Log.Log(context.Background(), ev.Severity, ev.Name, slog.Any("event", ev))
}

// storageEvent and sourceEvent are the two prefixed emitters the run uses most.
func (e *Engine) storageEvent(sb *storageBinding, ev human.Event) {
	e.at("storage", sb.cfg.Type, sb.name, ev)
}

func (e *Engine) sourceEvent(typ, name string, ev human.Event) { e.at("source", typ, name, ev) }

func (e *Engine) track(v any) {
	if c, ok := v.(io.Closer); ok {
		e.closers = append(e.closers, c)
	}
}

// closerScope collects the plugin instances ONE scoped unit of work builds
// outside the config — a run's discovered sources, a facade call's discovered
// target — so that unit closes them when it ends. Only config-built instances
// belong to the Engine's lifetime list; anything built per run or per call
// accumulating there would leak one instance per unit of work.
type closerScope struct{ closers []io.Closer }

func (r *closerScope) track(v any) {
	if c, ok := v.(io.Closer); ok {
		r.closers = append(r.closers, c)
	}
}

// close closes everything the unit built, in reverse build order, joining
// errors, and forgets them: a second close is a no-op.
func (r *closerScope) close() error {
	var errs []error
	for _, c := range slices.Backward(r.closers) {
		errs = append(errs, c.Close())
	}
	r.closers = nil
	return errors.Join(errs...)
}

// Close closes every plugin instance in reverse build order, joining errors.
func (e *Engine) Close() error {
	var errs []error
	for _, c := range slices.Backward(e.closers) {
		errs = append(errs, c.Close())
	}
	return errors.Join(errs...)
}

// Descriptors is the single source of documentation and JSON Schema.
func (e *Engine) Descriptors(_ context.Context) ([]plugin.Descriptor, error) {
	return e.reg.Descriptors(), nil
}

// EffectiveConfig returns the configuration as this engine read it: the user's
// file with the distribution's defaults merged under it, rule sources included,
// and the JSON pointers whose value came from a default. It is what
// `config effective` prints, and what proves the merge happened before any byte
// crossed a plugin boundary.
func (e *Engine) EffectiveConfig() (jsontext.Value, []string) {
	return e.merged, e.defaultPointers
}
