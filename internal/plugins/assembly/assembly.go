// Package assembly is the use-case layer: it reads the config, builds every plugin
// through the registry, and runs the backup pipeline. It never calls os.Exit,
// holds no globals, and a run is repeatable in one process.
//
// The facade is the assembly's public API; its first consumer is the CLI, and a
// future UI or HTTP trigger calls the same methods with the same flat data.
package assembly

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"reflect"
	"slices"
	"strings"
	"time"

	"encoding/json/jsontext"
	json "encoding/json/v2"

	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/catalog"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/configuration"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/discovery"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/execution"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/inventory"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/layout"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/lifecycle"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/must"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/notifier"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/pipeline"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/planning"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/processor"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/protection"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/reporting"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/retention"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage/guard"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/workflow/backup"
)

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
	// dest is the guarded destination: reads go through its Port, writes
	// through a Handle opened under the run's Authority, and deletion exists
	// only as its Remover, held by the protection ledger.
	dest guard.Destination
	cfg  configuration.StorageInstance
	// retention is this storage's policy per run kind: the protection ledger
	// partitions runs by kind and calls each partition's policy with a
	// homogeneous list, so a manual run never takes a scheduled slot. A kind
	// absent here has no policy — a scheduled run refuses the storage, a manual
	// run is refused.
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
	// rateLimit is the source envelope's raw-byte limit, applied before the
	// first processor and never recorded in the chain: 0 is unlimited.
	rateLimit model.ByteSize
}

type discoveryBinding struct {
	name string
	inst discovery.Discoverer
}

type notifierBinding struct {
	name string
	inst notifier.Notifier
}

// Deps is everything one assembly reads besides the configuration document:
// the one factory registry, the compiled loader registry that carries the
// effective plugin selection, and the log.
type Deps struct {
	Factories *instances.Registry
	Loader    *loader.Registry
	Log       *slog.Logger
}

type Assembly struct {
	cfg *configuration.Config
	reg *instances.Registry
	log *slog.Logger
	// loader is the compiled graph and the effective selection: every build is
	// gated on it, never on a second copy of the selection.
	loader *loader.Registry
	// active is the resolver's effective set, read by reference checks.
	active configuration.Active

	// merged is the configuration exactly as this assembly decoded it: the user's
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
	// reporter derives .Run.Previous from the storage survey and the previous
	// Job's status. It is nil when the distribution compiles no reporting
	// settings, which is what a test-only config looks like.
	reporter *reporting.Reporter
	backup   *backup.Workflow
	// layout renders every run id and object key of this configuration, and
	// reads a run's kind back from its id; the survey reads the same layout.
	layout layout.Layout
	// inventory is the storage survey, bound to the layout and the run's
	// execution policy.
	inventory inventory.Inventory
	// lease is the writer-lease protocol: acquire, settle, mark, clear and — as
	// the lifecycle's Release step — CAS-release with the record's uncertainty.
	lease *protection.Lease
	// ledger is the protection ledger: the one delete authority, occupancy and
	// cap admission. Destinations are its only Remover source.
	ledger *protection.Ledger
	// pipe produces every artifact: one policy for retries, the spool reserve,
	// the artifact cap and the progress interval of this configuration.
	pipe *pipeline.Producer

	retry execution.Options
	// restoreRetry / restoreSpoolDir / restoreReserve / restoreOrder come from the
	// restore section; a run config without one falls back to its run retry for
	// read-only paths, and every path that needs a spool demands the section.
	restoreRetry    execution.Options
	restoreSpoolDir string
	restoreReserve  int64
	restoreOrder    []string

	// scope owns every instance this invocation built, in construction order;
	// the role closes it after the operation returned.
	scope *lifecycle.Scope
	// lifecycle is the effective cleanup budgets under plugins.config.lifecycle.
	lifecycle lifecycle.Config
	// warnings are the coded, non-fatal facts the offline configuration checks
	// found — a source type's published credential exception. They are reported
	// by validate and never fail a command.
	warnings []model.Warning
}

// New merges the distribution's defaults under the configuration file, decodes
// the result (strictly, all errors at once), builds every plugin and checks
// every reference between instances. defs may be nil — then the file is read as
// it stands, which is what a test with a complete config wants. It performs no
// network I/O: that is Preflight's job.
func New(ctx context.Context, raw jsontext.Value, defs *configuration.Defaults, d Deps) (*Assembly, error) {
	if d.Factories == nil {
		return nil, errors.New("assembly: registry is nil")
	}
	if d.Loader == nil {
		return nil, errors.New("assembly: the compiled loader registry is required")
	}
	if d.Log == nil {
		return nil, errors.New("assembly: logger is nil")
	}
	log := d.Log
	merged, defaultPointers, mergeErrs := defs.Merge(d.Factories, raw)
	cfg := &configuration.Config{}
	decodeErr := configuration.Decode(merged, cfg)
	if decodeErr != nil {
		// The strict decoder stops at the first malformed section. The checks
		// below are diagnostics only (construction is refused on decodeErr), so
		// every readable section is decoded independently to keep the other
		// sections' references visible.
		cfg = diagnosticConfig(merged)
	}
	// The diagnostic view also keeps each readable reference's original JSON
	// index: a malformed element is invalid, not a user name, and the readable
	// names beside it are diagnosed at the positions they were written.
	refs := diagnosticReferencesOf(merged)
	// A merge or decode problem never short-circuits the diagnostic pass: the
	// partial configuration (json/v2 fills known members before it reports an
	// unknown one) still answers every check that needs no construction.
	var configErrs []error
	if len(mergeErrs) > 0 {
		configErrs = append(configErrs, fmt.Errorf("config: %w", errors.Join(mergeErrs...)))
	}
	if decodeErr != nil {
		configErrs = append(configErrs, fmt.Errorf("config: %w", decodeErr))
	}
	// The selection is resolved and every cross-instance reference checked
	// before the first construction: a deployment that turned off a plugin its
	// configuration still names, or left a required role without a provider,
	// refuses as a whole rather than running with instances missing.
	var listErrs []error
	if decodeErr != nil && refs != nil {
		compiledIDs := make([]string, 0)
		for _, plugin := range loader.Plugins(d.Loader) {
			compiledIDs = append(compiledIDs, plugin.ID)
		}
		bundleNames := make([]string, 0)
		for _, bundle := range loader.All(d.Loader, configuration.Bundles) {
			bundleNames = append(bundleNames, bundle.Name)
		}
		validBundles, validDisabled, selErrs := configuration.CheckSelection(
			refs.bundles, refs.disabled, compiledIDs, bundleNames)
		listErrs = selErrs
		cfg.Plugins = &configuration.Plugins{
			Bundles:  selectionNames(validBundles),
			Disabled: selectionNames(validDisabled),
		}
	}
	active, resolveErrs := configuration.Resolve(cfg, d.Loader)
	selectionErrs := append(configErrs, listErrs...)
	selectionErrs = append(selectionErrs, resolveErrs...)
	selectionErrs = append(selectionErrs, cfg.CheckRoles(active, configuration.RolesFrom(d.Loader))...)
	if len(selectionErrs) > 0 {
		// The aggregated answer also carries the reference problems that are
		// judgeable without construction, and never builds a plugin for
		// diagnostics.
		selectionErrs = append(selectionErrs, pureReferenceErrors(cfg, d.Factories, active, refs)...)
		return nil, errors.Join(selectionErrs...)
	}
	e := &Assembly{
		scope:           lifecycle.New(),
		cfg:             cfg,
		reg:             d.Factories,
		log:             d.Log,
		loader:          d.Loader,
		active:          active,
		merged:          merged,
		defaultPointers: defaultPointers,
		processors:      map[string]processor.Processor{},
	}
	// The layout is projected before any plugin is built: the restore and stage
	// sections render keys through the same one the run does.
	idLayout := ""
	if cfg.Run != nil {
		idLayout = cfg.Run.IDLayout
	}
	e.layout = layout.New(layout.Config{
		IDLayout:       idLayout,
		RunPrefix:      cfg.Layout.RunPrefix,
		ArtifactKey:    cfg.Layout.ArtifactKey,
		ManifestKey:    cfg.Layout.ManifestKey,
		ManualIDSuffix: cfg.Layout.ManualIDSuffix,
	})
	if cfg.Restore != nil {
		e.restoreSpoolDir = cfg.Restore.SpoolDir
		e.restoreReserve = cfg.Restore.SpoolReserveBytes.Int64()
		e.restoreOrder = cfg.Restore.StorageOrder
		e.restoreRetry = execution.Options{
			Attempts:     cfg.Restore.Retry.Attempts,
			Initial:      cfg.Restore.Retry.InitialDelay.Std(),
			Max:          cfg.Restore.Retry.MaxDelay.Std(),
			Jitter:       cfg.Restore.Retry.Jitter,
			Unclassified: cfg.Restore.UnclassifiedError,
			Log:          log,
		}
	}
	if cfg.Run != nil {
		e.retry = execution.Options{
			Attempts:     cfg.Run.Retry.Attempts,
			Initial:      cfg.Run.Retry.InitialDelay.Std(),
			Max:          cfg.Run.Retry.MaxDelay.Std(),
			Jitter:       cfg.Run.Retry.Jitter,
			Unclassified: cfg.Run.UnclassifiedError,
			Log:          log,
		}
	}
	if cfg.Run != nil {
		e.pipe = pipeline.New(pipeline.Options{
			Policy: e.retry, ReserveBytes: cfg.Run.SpoolReserveBytes.Int64(),
			MaxArtifactBytes: cfg.Run.MaxArtifactBytes.Int64(),
			ProgressInterval: cfg.Run.ProgressInterval.Std(), Log: log,
		})
	}
	if cfg.Restore == nil {
		// Read-only paths on a run config (StoredRuns) reuse the run's policy.
		e.restoreRetry = e.retry
	}
	e.inventory = inventory.New(inventory.Options{Layout: e.layout, Policy: e.retry})
	var errs []error
	for _, name := range sortedKeys(cfg.Processors) {
		inst := cfg.Processors[name]
		p, err := build(e, ctx, processor.Kind, "processor", name, inst)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		e.processors[name] = p
	}
	for _, name := range sortedKeys(cfg.Storages) {
		si := cfg.Storages[name]
		// A disabled storage backend is refused by ID before its factory runs;
		// the storage build path is the one that cannot use BuildActive, because
		// its construction input is not the framework's fixed one.
		if _, known := e.reg.ConfigType(storage.Kind.Name(), si.Type); known {
			if id := storage.Kind.ID(si.Type); !loader.Active(e.loader, id) {
				errs = append(errs, fmt.Errorf("/storages/%s: %s: plugin is disabled", name, id))
				continue
			}
		}
		// A storage is always built through its own factory: the construction
		// input carries the instance envelope's call_parallelism, which the
		// framework's fixed factory signature has no place for.
		dest, err := guard.Build(ctx, e.reg, si.Type, si.Config,
			storage.BuildInput{CallParallelism: si.CallParallelism}, e.deps("storage", si.Type, name))
		if err != nil {
			errs = append(errs, fmt.Errorf("/storages/%s: %w", name, err))
			continue
		}
		// The constructed storage is a closeable client of this invocation: the
		// scope owns the raw instance through the destination and closes it in
		// reverse construction order, without ever handing the instance out.
		e.track(dest)
		// The retention of a storage is an instance of its own per run kind,
		// inside the storage: it carries no name — the storage's names it in a
		// log line and in a config error — and a storage without a scheduled one
		// fetches but cannot run.
		rets := map[model.RunKind]retention.Retention{}
		for _, kind := range sortedKeys(si.Retention) {
			inst := si.Retention[kind]
			r, err := instances.BuildActive(ctx, e.loader, e.reg, retention.Kind, inst.Type, inst.Config,
				e.deps("retention", inst.Type, name))
			if err != nil {
				errs = append(errs, fmt.Errorf("/storages/%s/retention/%s: %w", name, kind, err))
				continue
			}
			e.track(r)
			rets[model.RunKind(kind)] = r
		}
		e.storages = append(e.storages, &storageBinding{name: name, dest: dest, cfg: si, retention: rets})
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
		d, err := build(e, ctx, discovery.Kind, "discovery", name, inst)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		e.discoveries = append(e.discoveries, discoveryBinding{name: name, inst: d})
		// A node-scoped discovery narrows every writer to its own node, and
		// protection tells one writer's runs from a peer's only by the stable
		// run.writer_identity. A deployment that opts into the scope without
		// naming its writer cannot account for a peer's runs — retention would
		// credit or delete them — so the configuration is refused here, where
		// both fields are visible, before a run starts.
		if ns, ok := d.(discovery.NodeScoped); ok && ns.NodeScoped() && writerIdentity(cfg) == "" {
			errs = append(errs, fmt.Errorf(
				"/discoveries/%s: node_scope is on and run/writer_identity is empty: state the stable writer id this node's runs carry, and one per peer sharing the prefix",
				name))
		}
	}
	errs = append(errs, configuration.CheckRules(e.reg, e.active, cfg)...)
	errs = append(errs, e.checkRestoreApply()...)
	for _, name := range sortedKeys(cfg.Notifiers) {
		inst := cfg.Notifiers[name]
		n, err := build(e, ctx, notifier.Kind, "notifier", name, inst)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		e.notifiers = append(e.notifiers, notifierBinding{name: name, inst: n})
	}
	if cfg.Catalog != nil {
		c, err := build(e, ctx, catalog.Kind, "catalog", "catalog", *cfg.Catalog)
		if err != nil {
			errs = append(errs, err)
		} else {
			e.catalog = c
		}
	}
	// Every key template is checked once here: a typo or an unsafe key is the
	// configuration's, refused before a run starts or a restore surveys.
	if err := e.layout.Check(); err != nil {
		errs = append(errs, err)
	}
	errs = append(errs, e.buildReporter(ctx))
	errs = append(errs, e.buildLifecycle())
	e.buildProtection()
	e.backup = e.buildBackupWorkflow()
	if err := errors.Join(errs...); err != nil {
		_ = e.Close(ctx)
		return nil, err
	}
	return e, nil
}

// Run executes one backup run through the Floor skeleton: this assembly only
// projects the providers it built into the workflow's typed options.
func (e *Assembly) Run(ctx context.Context, req backup.RunRequest) (model.RunReport, error) {
	return e.backup.Run(ctx, req)
}

// buildBackupWorkflow projects the built providers into the backup workflow's
// typed options: no configuration document and no assembly type crosses the
// boundary.
func (e *Assembly) buildBackupWorkflow() *backup.Workflow {
	var run backup.RunOptions
	if cfg := e.cfg.Run; cfg != nil {
		run = backup.RunOptions{
			Deadline: cfg.Deadline, FinishAllowance: cfg.FinishAllowance, SpoolDir: cfg.SpoolDir,
			MaxArtifactBytes: cfg.MaxArtifactBytes, StorageParallelism: cfg.StorageParallelism,
			MinVerifiedStorages: cfg.MinVerifiedStorages, DegradedExitCode: cfg.DegradedExitCode,
			FailedExitCode: cfg.FailedExitCode, NotifierFailureIsFatal: cfg.NotifierFailureIsFatal,
			WriterIdentity: cfg.WriterIdentity,
		}
	}
	storages := make([]backup.Storage, 0, len(e.storages))
	for _, sb := range e.storages {
		storages = append(storages, backup.Storage{
			Name: sb.name, Type: sb.cfg.Type, Port: sb.dest.Port(), TimeBudget: sb.cfg.TimeBudget,
			MaxTotalBytes: sb.cfg.MaxTotalBytes, CallParallelism: sb.cfg.CallParallelism, Retention: sb.retention,
		})
	}
	sources := make([]backup.Source, 0, len(e.sources))
	for _, sb := range e.sources {
		sources = append(sources, backup.Source{
			Name: sb.name, CheckKind: sb.checkKind, Type: sb.typ, Instance: sb.inst,
			Artifact: sb.artifact, Pipeline: sb.pipeline, Refs: sb.refs, Processors: sb.procs,
			RateLimit: sb.rateLimit,
		})
	}
	discoveries := make([]backup.Discovery, 0, len(e.discoveries))
	for _, db := range e.discoveries {
		discoveries = append(discoveries, backup.Discovery{Name: db.name, Instance: db.inst})
	}
	notifiers := make([]backup.Notifier, 0, len(e.notifiers))
	for _, nb := range e.notifiers {
		notifiers = append(notifiers, backup.Notifier{Name: nb.name, Type: e.notifierType(nb.name), Instance: nb.inst})
	}
	return backup.New(backup.Options{
		Run: run, RunConfigured: e.cfg.Run != nil, CatalogConfigured: e.cfg.Catalog != nil,
		Layout: e.layout, Policy: e.retry, Inventory: e.inventory, Pipeline: e.pipe,
		Lease: e.lease, Reporter: e.reporter,
		Scope: e.scope, Lifecycle: e.lifecycle,
		Sources: sources, ProcessorTypes: e.processorTypes(), Processors: e.processors,
		Storages: storages, Discoveries: discoveries, Notifiers: notifiers,
		Instances: e.instancesFor(scopeRun), Binaries: e.declaredBinaries,
		BuildSource: func(ctx context.Context, typ, instance string, raw jsontext.Value) (source.Source, error) {
			return instances.BuildActive(ctx, e.loader, e.reg, source.Kind, typ, raw, e.deps("source", typ, instance))
		},
		Log: e.log,
	}, e.ledger)
}

// notifierType is the registered type of one notifier instance, for the log's
// prefix.
func (e *Assembly) notifierType(name string) string { return e.cfg.Notifiers[name].Type }

// processorTypes is the configured type of every processor instance.
func (e *Assembly) processorTypes() map[string]string {
	out := make(map[string]string, len(e.cfg.Processors))
	for name, inst := range e.cfg.Processors {
		out[name] = inst.Type
	}
	return out
}

// buildLifecycle reads the cleanup budgets from the unified plugins root. The
// defaults document fills the block; a hand-built test config without one runs
// under zero bounds, which the shutdown path treats as "no grace configured".
func (e *Assembly) buildLifecycle() error {
	root, _ := configuration.ObjectOf(e.merged)
	plugins, _ := configuration.ObjectOf(root["plugins"])
	config, _ := configuration.ObjectOf(plugins["config"])
	block, ok := config["lifecycle"]
	if !ok {
		return nil
	}
	var lc lifecycle.Config
	if err := configuration.Decode(block, &lc); err != nil {
		return fmt.Errorf("/plugins/config/lifecycle: %w", err)
	}
	if err := validateGrace(lc, e.cfg); err != nil {
		return err
	}
	e.lifecycle = lc
	return nil
}

// validateGrace refuses cleanup budgets whose required grace cannot be computed:
// no unbounded fallback is reachable at shutdown.
func validateGrace(lc lifecycle.Config, cfg *configuration.Config) error {
	var release time.Duration
	if cfg != nil && cfg.Run != nil {
		release = cfg.Run.FinishAllowance.Std()
	}
	if _, err := lifecycle.RequiredGrace(lifecycle.GraceInputs{
		Join: lc.Join.Std(), Release: release, AbortTotal: lc.AbortTotal.Std(),
		Close: lc.Close.Std(), Margin: lc.Margin.Std(),
	}); err != nil {
		return fmt.Errorf("/plugins/config/lifecycle: %w", err)
	}
	return nil
}

// buildProtection binds the writer-lease protocol and the protection ledger to
// this configuration: the destinations are the ledger's only Remover source, and
// the stale cutoff is the instant before which no live run can still be writing.
func (e *Assembly) buildProtection() {
	var (
		wait, poll, hardBound, finish         time.Duration
		holderNamespace, holderPod, holderUID string
		writerIdentity                        string
	)
	stale := time.Now().UTC()
	if cfg := e.cfg.Run; cfg != nil {
		wait, poll, hardBound, finish = cfg.LeaseWait.Std(), cfg.LeasePoll.Std(), cfg.LeaseHardBound.Std(), cfg.FinishAllowance.Std()
		holderNamespace, holderPod, holderUID = cfg.HolderNamespaceEnv, cfg.HolderPodEnv, cfg.HolderUIDEnv
		writerIdentity = cfg.WriterIdentity
		stale = stale.Add(-(cfg.Deadline.Std() + cfg.FinishAllowance.Std()))
	}
	e.lease = protection.NewLease(protection.LeaseOptions{
		Catalog: e.catalog, Wait: wait, Poll: poll, HardBound: hardBound, FinishAllowance: finish,
		HolderNamespaceEnv: holderNamespace, HolderPodEnv: holderPod, HolderUIDEnv: holderUID, Log: e.log,
	})
	destinations := make([]guard.Destination, 0, len(e.storages))
	policies := make(map[string]map[model.RunKind]retention.Retention, len(e.storages))
	caps := make(map[string]int64, len(e.storages))
	for _, sb := range e.storages {
		destinations = append(destinations, sb.dest)
		policies[sb.name] = sb.retention
		if cap := sb.cfg.MaxTotalBytes.Int64(); cap > 0 {
			caps[sb.name] = cap
		}
	}
	e.ledger = protection.NewLedger(protection.LedgerOptions{
		Destinations: destinations, Policies: policies, Caps: caps,
		StaleCutoff: stale, Retry: e.retry, WriterIdentity: writerIdentity,
	})
}

// buildReporter reads the reporting plugin's settings from the unified plugins
// root. The distribution's defaults document fills the block; a config without
// one (a test's hand-built document) leaves .Run.Previous empty, which is what
// a run outside a compiled distribution would report.
func (e *Assembly) buildReporter(ctx context.Context) error {
	// The strict decode that ran before this call proved every level here is an
	// object: a non-object file or a missing plugin root never reaches it.
	root, _ := configuration.ObjectOf(e.merged)
	plugins, _ := configuration.ObjectOf(root["plugins"])
	config, _ := configuration.ObjectOf(plugins["config"])
	block, ok := config["reporting"]
	if !ok {
		return nil
	}
	var rc reporting.Config
	if err := configuration.Decode(block, &rc); err != nil {
		return fmt.Errorf("/plugins/config/reporting: %w", err)
	}
	reporter, err := reporting.New(ctx, rc)
	if err != nil {
		return fmt.Errorf("/plugins/config/reporting: %w", err)
	}
	e.reporter = reporter
	return nil
}

// checkScope names the part of one configuration a command checks: a run
// checks everything but the stage's own sources — they belong to another
// container, with mounts this pod does not have — a stage checks its own
// sources and the processors their pipelines name, and a distribution-wide
// check sees all of it.
type checkScope uint8

const (
	scopeAll checkScope = iota
	scopeRun
	scopeStage
)

// instanceKey names one built instance in a scoped check result.
type instanceKey struct{ kind, name string }

// failedChecks is the scoped half of a preflight: an instance whose Check
// failed fails its own work, and the run's other instances are unaffected.
type failedChecks map[instanceKey]error

// PreflightTargets lists every declared instance a preflight can select — what
// the CLI validates a selection against and diffs for its "not checked" report.
// It checks nothing and reads nothing, so the list is the same before and after
// any preflight ran, in a stable order.
func (e *Assembly) PreflightTargets(ctx context.Context) []planning.Target {
	if ctx.Err() != nil {
		return nil
	}
	return planning.Targets(ctx, e.instancesFor(scopeAll))
}

// PreflightResults checks the selected instances — the declared ones and the
// sources the selected discoveries find — and returns one result per instance,
// without stopping at the first failure: the caller decides what a failure
// means. A discovered source is built exactly as a run would build it, and
// closed here, because the preflight owns the scope of its own constructions.
func (e *Assembly) PreflightResults(ctx context.Context, req planning.Request) []planning.Result {
	discoveries := make([]planning.Discoverer, 0, len(e.discoveries))
	for _, db := range e.discoveries {
		discoveries = append(discoveries, planning.Discoverer{Name: db.name, Instance: db.inst})
	}
	results := planning.New(e.checkPolicy()).Results(ctx, req, planning.Options{
		Instances:   e.instancesFor(scopeAll),
		Discoveries: discoveries,
		Binaries:    e.declaredBinaries,
		Credential:  e.credentialCheck,
		Build: func(ctx context.Context, typ, instance string, raw jsontext.Value) (source.Source, error) {
			return instances.BuildActive(ctx, e.loader, e.reg, source.Kind, typ, raw, e.deps("source", typ, instance))
		},
		Check: e.checkSource,
	})
	return results
}

// pipelineSeals reports whether any processor of a pipeline seals a stream,
// which is what the run-time secret invariant asks of it too.
func (e *Assembly) pipelineSeals(ctx context.Context, procs []processor.Processor) (bool, error) {
	for _, p := range procs {
		tr, err := execution.Attempt(ctx, e.checkPolicy(), "describe processor", func(ctx context.Context, _ int) (processor.ProcessorTraits, error) {
			return execution.SafeValue(func() (processor.ProcessorTraits, error) { return p.Describe(ctx) })
		})
		if err != nil {
			return false, err
		}
		if tr.Seals {
			return true, nil
		}
	}
	return false, nil
}

// checkSource runs one discovered source's Check under the run's retry policy;
// a plugin without a Check has nothing to fail.
func (e *Assembly) checkSource(ctx context.Context, s source.Source, name string) error {
	checker, ok := s.(instances.Checker)
	if !ok {
		return nil
	}
	_, err := execution.Attempt(ctx, e.retry, "check source "+name, func(ctx context.Context, _ int) (struct{}, error) {
		return struct{}{}, execution.SafeCall(func() error { return checker.Check(ctx) })
	})
	return err
}

// checkPolicy is the retry policy a preflight uses: the run's, or the restore's
// on a config without a run section.
func (e *Assembly) checkPolicy() execution.Options {
	if e.cfg.Run == nil {
		return e.restoreRetry
	}
	return e.retry
}

// instancesFor lists the built instances one command checks, in a stable order:
// what a preflight checks and what a command refuses to start without the
// binaries of. The kind of a stage source is stage_source: the staged source of
// the run side is a different instance with the same name.
func (e *Assembly) instancesFor(scope checkScope) []planning.Instance {
	var out []planning.Instance
	add := func(kind, name string, v any) {
		inst := planning.Instance{Kind: kind, Name: name}
		if c, ok := v.(instances.Checker); ok {
			inst.Check = c
		}
		if b, ok := v.(instances.CheckBinaries); ok {
			inst.BinariesCheck = b
		}
		if d, ok := v.(processor.DecodeChecker); ok {
			inst.Decode = d
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
		return out
	}
	for _, name := range sortedKeys(e.processors) {
		add("processor", name, e.processors[name])
	}
	for _, sb := range e.storages {
		add("storage", sb.name, sb.dest.Port())
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
	return out
}

// declaredBinaries lists the external programs one instance's plugin type
// declares; planning resolves what PATH is missing.
func (e *Assembly) declaredBinaries(kind, name string) []string {
	return e.reg.Binaries(kind, e.instanceType(kind, name))
}

// instanceType is the plugin type one built instance was declared with.
func (e *Assembly) instanceType(kind, name string) string {
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

// binaryErrors is what stops a run or a stage before any plugin work: a binary
// a declared plugin requires and PATH does not have is the deployment, not a
// blip.
func (e *Assembly) binaryErrors(scope checkScope) []error {
	return planning.BinaryErrors(e.instancesFor(scope), e.declaredBinaries)
}

// scopedChecks runs every declared instance's Check under the given retry
// policy — a transient check error is retried like the instance's own work —
// and keeps the failures that survive, per instance, error class included.
func (e *Assembly) scopedChecks(ctx context.Context, p execution.Options, scope checkScope) failedChecks {
	rows := planning.New(p).Check(ctx, nil, planning.Options{
		Instances: e.instancesFor(scope),
		Binaries:  e.declaredBinaries,
	})
	out := failedChecks{}
	for _, r := range rows {
		if r.Err != nil {
			out[instanceKey{kind: r.Kind, name: r.Name}] = r.Err
		}
	}
	return out
}

func build[T any](e *Assembly, ctx context.Context, kind instances.Kind[T], kindName, name string, inst configuration.Instance) (T, error) {
	var zero T
	t, err := instances.BuildActive(ctx, e.loader, e.reg, kind, inst.Type, inst.Config, e.deps(kindName, inst.Type, name))
	if err != nil {
		return zero, fmt.Errorf("/%s/%s: %w", sectionName(kindName), name, err)
	}
	e.track(t)
	return t, nil
}

func (e *Assembly) buildSource(ctx context.Context, section, name string, si configuration.SourceInstance) (sourceBinding, error) {
	s, err := instances.BuildActive(ctx, e.loader, e.reg, source.Kind, si.Type, si.Config, e.deps("source", si.Type, name))
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
	sb := sourceBinding{
		name: name, checkKind: checkKind, typ: si.Type, inst: s, pipeline: pipelineName(refs),
		refs: refs, procs: procs, rateLimit: si.RateLimit,
		artifact: model.Artifact{Name: si.Artifact.Name, Group: si.Artifact.Group,
			Importance: si.Artifact.Importance, Sensitivity: si.Artifact.Sensitivity},
	}
	// The credential rule is the run's own, applied offline: bytes that carry
	// credentials and no sealing step are refused here, where the operator can
	// fix the file, or warned about under a source type's published exception.
	warning, err := e.checkCredentials(ctx, sb)
	if err != nil {
		return sourceBinding{}, fmt.Errorf("/%s/%s: %w", section, name, err)
	}
	if warning.Code != "" {
		e.warnings = append(e.warnings, warning)
	}
	return sb, nil
}

// checkCredentials decides one declared source's credential bytes from the same
// pure facts the run's Resolve uses: what the source says its bytes are, whether
// the pipeline seals them, and the source type's own verdict. It reads no mount
// and starts no client — Describe's contract is static facts — and a source
// whose static facts cannot be answered here is left to the run, which reports
// the failure against its own artifact.
func (e *Assembly) checkCredentials(ctx context.Context, sb sourceBinding) (model.Warning, error) {
	return e.credentialDecision(ctx, sb.name, sb.inst, sb.artifact, sb.procs)
}

// credentialCheck applies the same decision to one discovered source before the
// planning preflight's operational Check: its pipeline names are this
// configuration's processor instances, resolved the way a run resolves them. A
// name this build does not know is left to the run, which fails that artifact.
func (e *Assembly) credentialCheck(ctx context.Context, src source.Source, artifact model.Artifact, pipeline []string) (model.Warning, error) {
	procs := make([]processor.Processor, 0, len(pipeline))
	for _, name := range pipeline {
		p, ok := e.processors[name]
		if !ok {
			return model.Warning{}, nil
		}
		procs = append(procs, p)
	}
	return e.credentialDecision(ctx, artifact.Name, src, artifact, procs)
}

// credentialDecision is the one credential/sealing decision shared by the
// declared-source check, the discovered-source preflight and the run's Resolve:
// bytes that carry credentials and no sealing step are refused, or warned about
// under a source type's published exception.
func (e *Assembly) credentialDecision(ctx context.Context, name string, src source.Source, artifact model.Artifact, procs []processor.Processor) (model.Warning, error) {
	traits, err := execution.Attempt(ctx, e.checkPolicy(), "describe source "+name, func(ctx context.Context, _ int) (source.SourceTraits, error) {
		return execution.SafeValue(func() (source.SourceTraits, error) { return src.Describe(ctx) })
	})
	if err != nil {
		return model.Warning{}, nil
	}
	if !traits.Credentials {
		return model.Warning{}, nil
	}
	sealed := traits.Sealed
	if _, recorded := src.(source.RecordedChain); !recorded {
		// A recorded chain's seals are the stage record's to say, and the run
		// reads that record itself; this run's own pipeline is the only one an
		// offline check can judge.
		seals, err := e.pipelineSeals(ctx, procs)
		if err != nil {
			return model.Warning{}, nil
		}
		sealed = sealed || seals
	}
	if sealed {
		return model.Warning{}, nil
	}
	verdict := source.CredentialRefuse
	if cp, ok := src.(source.CredentialPolicy); ok {
		v, err := execution.Attempt(ctx, e.checkPolicy(), "credential policy "+name, func(ctx context.Context, _ int) (source.CredentialVerdict, error) {
			return execution.SafeValue(func() (source.CredentialVerdict, error) { return cp.CredentialPolicy(ctx) })
		})
		if err != nil && !errors.Is(err, errors.ErrUnsupported) {
			return model.Warning{}, err
		}
		if err == nil {
			verdict = v
		}
	}
	decision := source.DecideCredentials(artifact, traits, sealed, verdict)
	if decision.Refused != "" {
		return model.Warning{}, errors.New(decision.Refused)
	}
	return decision.Warning, nil
}

// resolvePipeline turns one pipeline — the processor instance names of a source
// or a rule, in encode order — into the instances themselves. A name that is
// not a declared processor is an error naming every valid one; a name may
// repeat, because two passes of one processor are two steps, not a mistake.
func (e *Assembly) resolvePipeline(ctx context.Context, path string, names []string) ([]processor.Processor, []string, error) {
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

func (e *Assembly) deps(kind, typ, name string) instances.Deps {
	return instances.Deps{Instance: name, Log: e.log.With("kind", kind, "type", typ, "instance", name)}
}

// own records one built instance with the invocation scope when closeable.
func own(scope *lifecycle.Scope, v any) {
	if c, ok := v.(io.Closer); ok {
		scope.Own(c)
	}
}

func (e *Assembly) track(v any) {
	if c, ok := v.(io.Closer); ok {
		e.scope.Own(c)
	}
}

// Close closes every instance this invocation built in reverse construction
// order, joining errors. The role calls it after the operation returned; a
// cancelled invocation closed the scope inside its cancel sequence, so a second
// Close is a no-op.
// Close closes every instance this invocation built, bounded by the caller's
// context. An invocation whose join did not complete is poisoned: Close returns
// lifecycle.ErrIncompleteJoin and closes nothing, because its clients may still
// be in use.
func (e *Assembly) Close(ctx context.Context) error { return e.scope.Close(ctx) }

// Descriptors is the single source of documentation and JSON Schema.
func (e *Assembly) Descriptors(_ context.Context) ([]instances.Descriptor, error) {
	return e.reg.Descriptors(), nil
}

// EffectiveConfig returns the configuration as this assembly read it: the user's
// file with the distribution's defaults merged under it, rule sources included,
// and the JSON pointers whose value came from a default. It is what
// `config effective` prints, and what proves the merge happened before any byte
// crossed a plugin boundary.
func (e *Assembly) EffectiveConfig() (jsontext.Value, []string) {
	return e.merged, e.defaultPointers
}

// Warnings returns the coded, non-fatal facts the offline checks found — a
// source type's published credential exception, the same one a run records on
// its artifact. They never fail a command, and a caller prints them beside its
// own answer.
func (e *Assembly) Warnings() []model.Warning {
	return append([]model.Warning(nil), e.warnings...)
}

// checkRestoreApply decodes every restore.apply block offline, exactly as the
// restore will: a key is a source TYPE, and that type's own config value judges
// the block — no instance is built and nothing is read, so a restore-only
// target type with no declared instance is still checked, and a disabled type
// with a restorer is checked too. A key naming no source type and a block for a
// type with no restore are dead configuration and are refused with the block
// named; a block its own decoder refuses is reported under its key.
func (e *Assembly) checkRestoreApply() []error {
	if e.cfg.Restore == nil {
		return nil
	}
	var errs []error
	for _, typ := range sortedKeys(e.cfg.Restore.Apply) {
		path := "/restore/apply/" + typ
		ct, known := e.reg.ConfigType(source.Kind.Name(), typ)
		if !known {
			errs = append(errs, fmt.Errorf("%s: no source type %q (known: %s)",
				path, typ, strings.Join(e.reg.Types(source.Kind.Name()), ", ")))
			continue
		}
		// The config type is the capability's home: a pointer to a zero config
		// finds a value-receiver method, and the checker decodes the same bytes
		// Apply would.
		checker, ok := reflect.New(ct).Interface().(source.ApplyOptionsChecker)
		if !ok {
			errs = append(errs, fmt.Errorf("%s: source type %q has no restore; remove the block", path, typ))
			continue
		}
		for _, label := range e.layout.SampleRunIDs() {
			err := checker.CheckApplyOptions(e.cfg.Restore.Apply[typ], label)
			if errors.Is(err, errors.ErrUnsupported) {
				errs = append(errs, fmt.Errorf("%s: source type %q has no restore; remove the block", path, typ))
				break
			}
			if err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", path, err))
				break
			}
		}
	}
	return errs
}

// writerIdentity is the stable writer id the run section states, "" when the
// configuration carries no run section (a restore-only file).
func writerIdentity(cfg *configuration.Config) string {
	if cfg.Run == nil {
		return ""
	}
	return cfg.Run.WriterIdentity
}

// sortedKeys returns a map's keys in a stable order.
func sortedKeys[V any](m map[string]V) []string { return slices.Sorted(maps.Keys(m)) }

// diagnosticConfig decodes the merged document tolerantly for diagnostics: a
// malformed node fails alone — a top-level section, a collection entry or a
// storage's retention entry — while every readable sibling stays visible to the
// pure checks. It exists only on the strict decode's error path; construction is
// refused by the original error.
func diagnosticConfig(merged jsontext.Value) *configuration.Config {
	cfg := &configuration.Config{}
	obj, ok := configuration.ObjectOf(merged)
	if !ok {
		return cfg
	}
	for _, key := range sortedKeys(obj) {
		switch key {
		case "sources":
			cfg.Sources = diagnosticEntries[configuration.SourceInstance](obj[key])
		case "processors":
			cfg.Processors = diagnosticEntries[configuration.Instance](obj[key])
		case "discoveries":
			cfg.Discoveries = diagnosticEntries[configuration.Instance](obj[key])
		case "notifiers":
			cfg.Notifiers = diagnosticEntries[configuration.Instance](obj[key])
		case "storages":
			cfg.Storages = diagnosticStorages(obj[key])
		case "stage":
			cfg.Stage = diagnosticStage(obj[key])
		default:
			// The section value came out of one parsed object, so re-encoding it
			// into a one-member object cannot fail.
			section := must.Do(json.Marshal(map[string]jsontext.Value{key: obj[key]}))
			_ = json.Unmarshal(section, cfg)
		}
	}
	return cfg
}

// diagnosticEntry decodes one entry member by member: a malformed field hides
// only itself, never the entry's independently readable reference members
// (type, pipeline, retention, config).
func diagnosticEntry[T any](obj map[string]jsontext.Value) T {
	var value T
	for _, key := range sortedKeys(obj) {
		piece := must.Do(json.Marshal(map[string]jsontext.Value{key: obj[key]}))
		_ = json.Unmarshal(piece, &value)
	}
	return value
}

// diagnosticEntries decodes a named collection entry by entry, member by member:
// a bad node before a readable sibling never hides it, and inside an entry a bad
// unrelated field never hides the entry's readable references.
func diagnosticEntries[T any](raw jsontext.Value) map[string]T {
	obj, ok := configuration.ObjectOf(raw)
	if !ok {
		return nil
	}
	out := make(map[string]T, len(obj))
	for _, name := range sortedKeys(obj) {
		entry, ok := configuration.ObjectOf(obj[name])
		if !ok {
			continue
		}
		out[name] = diagnosticEntry[T](entry)
	}
	return out
}

// diagnosticStorages decodes storages entry by entry and member by member. The
// retention map is decoded policy by policy on top, so a malformed budget never
// hides a readable retention reference and a bad policy hides only itself.
func diagnosticStorages(raw jsontext.Value) map[string]configuration.StorageInstance {
	obj, ok := configuration.ObjectOf(raw)
	if !ok {
		return nil
	}
	out := make(map[string]configuration.StorageInstance, len(obj))
	for _, name := range sortedKeys(obj) {
		entry, ok := configuration.ObjectOf(obj[name])
		if !ok {
			continue
		}
		si := diagnosticEntry[configuration.StorageInstance](entry)
		if retention, ok := entry["retention"]; ok {
			si.Retention = diagnosticEntries[*configuration.Instance](retention)
		}
		out[name] = si
	}
	return out
}

// indexedReference is one readable list entry with the JSON index it was
// written at. Validity and position live beside the name, never inside it: a
// malformed element is absent, and a user string equal to any marker stays a
// plain readable reference.
type indexedReference struct {
	index int
	name  string
}

// diagnosticReferences is the diagnostics-only indexed view of every reference
// list: source and stage pipelines, the disabled plugin ids and the bundles.
type diagnosticReferences struct {
	pipelines map[string]map[string][]indexedReference
	disabled  []configuration.SelectionRef
	bundles   []configuration.SelectionRef
}

// diagnosticReferenceList reads one list element by element, keeping each
// readable string with its original JSON index.
func diagnosticReferenceList(raw jsontext.Value) []indexedReference {
	var items []jsontext.Value
	if json.Unmarshal(raw, &items) != nil {
		return nil
	}
	out := make([]indexedReference, 0, len(items))
	for i, item := range items {
		var name string
		if json.Unmarshal(item, &name) == nil {
			out = append(out, indexedReference{index: i, name: name})
		}
	}
	return out
}

// selectionNames projects validated selection references to their names.
func selectionNames(refs []configuration.SelectionRef) []string {
	out := make([]string, 0, len(refs))
	for _, ref := range refs {
		out = append(out, ref.Name)
	}
	return out
}

// diagnosticSelectionList reads one selection list with original JSON indices.
func diagnosticSelectionList(raw jsontext.Value) []configuration.SelectionRef {
	var items []jsontext.Value
	if json.Unmarshal(raw, &items) != nil {
		return nil
	}
	out := make([]configuration.SelectionRef, 0, len(items))
	for i, item := range items {
		var name string
		if json.Unmarshal(item, &name) == nil {
			out = append(out, configuration.SelectionRef{Index: i, Name: name})
		}
	}
	return out
}

// diagnosticPipelines reads every named entry's pipeline with its indices.
func diagnosticPipelines(raw jsontext.Value) map[string][]indexedReference {
	obj, ok := configuration.ObjectOf(raw)
	if !ok {
		return nil
	}
	out := make(map[string][]indexedReference, len(obj))
	for _, name := range sortedKeys(obj) {
		entry, ok := configuration.ObjectOf(obj[name])
		if !ok {
			continue
		}
		if pipeline, ok := entry["pipeline"]; ok {
			out[name] = diagnosticReferenceList(pipeline)
		}
	}
	return out
}

// diagnosticReferencesOf extracts the indexed reference view from the merged
// document; nothing here constructs a plugin.
func diagnosticReferencesOf(merged jsontext.Value) *diagnosticReferences {
	obj, ok := configuration.ObjectOf(merged)
	if !ok {
		return nil
	}
	refs := &diagnosticReferences{pipelines: map[string]map[string][]indexedReference{}}
	for _, section := range []string{"sources", "stage.sources"} {
		if section == "sources" {
			refs.pipelines[section] = diagnosticPipelines(obj["sources"])
			continue
		}
		if stage, ok := configuration.ObjectOf(obj["stage"]); ok {
			refs.pipelines[section] = diagnosticPipelines(stage["sources"])
		}
	}
	if plugins, ok := configuration.ObjectOf(obj["plugins"]); ok {
		if disabled, ok := plugins["disabled"]; ok {
			refs.disabled = diagnosticSelectionList(disabled)
		}
		if bundles, ok := plugins["bundles"]; ok {
			refs.bundles = diagnosticSelectionList(bundles)
		}
	}
	return refs
}

// diagnosticStage decodes a stage section whose sources are read entry by entry;
// its scalar members decode independently so one bad value hides only itself.
func diagnosticStage(raw jsontext.Value) *configuration.StageConfig {
	stage := &configuration.StageConfig{}
	obj, ok := configuration.ObjectOf(raw)
	if !ok {
		return stage
	}
	if sources, ok := obj["sources"]; ok {
		stage.Sources = diagnosticEntries[configuration.SourceInstance](sources)
	}
	for _, key := range sortedKeys(obj) {
		if key == "sources" {
			continue
		}
		section := must.Do(json.Marshal(map[string]jsontext.Value{key: obj[key]}))
		_ = json.Unmarshal(section, stage)
	}
	return stage
}

// pureReferenceErrors reports the configuration's reference problems that need
// no construction: nested rule references, declared instance types and pipeline
// names. It runs only when the selection itself is refused, so diagnostics
// aggregate both classes without building a plugin the selection disabled.
// fallbackRefs answers the indexed pipeline of one section entry, or nil when
// the raw document held no list (the caller then uses the decoded fallback).
func fallbackRefs(refs *diagnosticReferences, section, name string) []indexedReference {
	if refs == nil {
		return nil
	}
	entries, ok := refs.pipelines[section]
	if !ok {
		return nil
	}
	indexed, ok := entries[name]
	if !ok {
		return nil
	}
	return indexed
}

func pureReferenceErrors(cfg *configuration.Config, reg *instances.Registry, active configuration.Active, refs *diagnosticReferences) []error {
	errs := configuration.CheckRules(reg, active, cfg)
	known := func(kind, typ string) bool {
		_, ok := reg.ConfigType(kind, typ)
		return ok
	}
	typErr := func(section, kind, name, typ string) {
		if typ != "" && !known(kind, typ) {
			errs = append(errs, fmt.Errorf("/%s/%s: unknown %s type %q (known: %s)",
				section, name, kind, typ, strings.Join(reg.Types(kind), ", ")))
		}
	}
	pipelineErr := func(section, name string, fallback []string) {
		indexed := fallbackRefs(refs, section, name)
		if indexed == nil {
			for i, ref := range fallback {
				if _, ok := cfg.Processors[ref]; ok {
					continue
				}
				errs = append(errs, fmt.Errorf("/%s/%s/pipeline/%d: unknown processor instance %q (known: %v)",
					section, name, i, ref, sortedKeys(cfg.Processors)))
			}
			return
		}
		for _, ref := range indexed {
			if _, ok := cfg.Processors[ref.name]; ok {
				continue
			}
			errs = append(errs, fmt.Errorf("/%s/%s/pipeline/%d: unknown processor instance %q (known: %v)",
				section, name, ref.index, ref.name, sortedKeys(cfg.Processors)))
		}
	}
	for _, name := range sortedKeys(cfg.Processors) {
		typErr("processors", "processor", name, cfg.Processors[name].Type)
	}
	for _, name := range sortedKeys(cfg.Sources) {
		si := cfg.Sources[name]
		typErr("sources", "source", name, si.Type)
		pipelineErr("sources", name, si.Pipeline)
	}
	if cfg.Stage != nil {
		for _, name := range sortedKeys(cfg.Stage.Sources) {
			si := cfg.Stage.Sources[name]
			typErr("stage.sources", "source", name, si.Type)
			pipelineErr("stage.sources", name, si.Pipeline)
		}
	}
	for _, name := range sortedKeys(cfg.Discoveries) {
		typErr("discoveries", "discovery", name, cfg.Discoveries[name].Type)
	}
	for _, name := range sortedKeys(cfg.Notifiers) {
		typErr("notifiers", "notifier", name, cfg.Notifiers[name].Type)
	}
	for _, name := range sortedKeys(cfg.Storages) {
		si := cfg.Storages[name]
		typErr("storages", "storage", name, si.Type)
		for _, kind := range sortedKeys(si.Retention) {
			if inst := si.Retention[kind]; inst != nil {
				typErr("storages/"+name+"/retention", "retention", kind, inst.Type)
			}
		}
	}
	if cfg.Catalog != nil {
		typErr("catalogs", "catalog", "catalog", cfg.Catalog.Type)
	}
	return errs
}
