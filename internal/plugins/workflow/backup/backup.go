// Package backup is the backup workflow: the Floor driver's fixed skeleton —
// acquire, deferred release, refusal, complete survey and collision checks,
// admitted writes, verified manifests, retention and the common finish. Every
// collaborator arrives as a typed option projected by assembly; the workflow
// holds no configuration document and builds no provider.
package backup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/catalog"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/discovery"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/execution"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/human"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/inventory"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/layout"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/lifecycle"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/notifier"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/pipeline"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/planning"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/processor"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/protection"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/reporting"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/retention"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage/guard"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/transfer"
)

// RunOptions is the projection of the configuration's run section: what the
// workflow's skeleton reads, nothing else.
type RunOptions struct {
	Deadline               model.Duration
	FinishAllowance        model.Duration
	SpoolDir               string
	MaxArtifactBytes       model.ByteSize
	StorageParallelism     int
	MinVerifiedStorages    int
	DegradedExitCode       int
	FailedExitCode         int
	NotifierFailureIsFatal bool
	// WriterIdentity is the stable writer id this deployment opted into, "" when
	// it did not. Every manifest stores it, and the storage sweep aborts a stale
	// upload only when the run it belongs to proves the same writer.
	WriterIdentity string
}

// Storage is one built destination's read port as the workflow receives it:
// reads, capability checks and handle issuance only. Writes go through handles,
// deletion through the protection ledger; neither is reachable from here.
type Storage struct {
	Name            string
	Type            string
	Port            guard.Port
	TimeBudget      model.Duration
	MaxTotalBytes   model.ByteSize
	CallParallelism int
	Retention       map[model.RunKind]retention.Retention
}

// Source is one built source instance with its envelope and pipeline.
type Source struct {
	Name       string
	CheckKind  string
	Type       string
	Instance   source.Source
	Artifact   model.Artifact
	Pipeline   string
	Refs       []string
	Processors []processor.Processor
	// RateLimit is the raw bytes per second this source may produce before its
	// first processor: part of the source's envelope, never a chain step.
	RateLimit model.ByteSize
}

// Discovery is one built discovery the run surveys.
type Discovery struct {
	Name     string
	Instance discovery.Discoverer
}

// Notifier is one built notifier the run reports through.
type Notifier struct {
	Name     string
	Type     string
	Instance notifier.Notifier
}

// Options are the workflow's typed collaborators, projected by assembly.
type Options struct {
	Run RunOptions
	// RunConfigured is true when the configuration carries a run section; the
	// run command refuses one without.
	RunConfigured bool
	// CatalogConfigured is true when the configuration declares a catalog: a
	// manual run needs one that can hold the writer lease.
	CatalogConfigured bool
	Layout            layout.Layout
	Policy            execution.Options
	Inventory         inventory.Inventory
	Pipeline          *pipeline.Producer
	Lease             *protection.Lease
	Reporter          *reporting.Reporter
	// Sources are the run-side sources; StageSources stay out of the run.
	Sources []Source
	// ProcessorTypes is the configured type of every processor instance, for the
	// manifest's recorded chain.
	ProcessorTypes map[string]string
	// Processors are the configured processor instances by name: a discovery
	// rule's pipeline resolves against them.
	Processors  map[string]processor.Processor
	Storages    []Storage
	Discoveries []Discovery
	Notifiers   []Notifier
	// Instances are the built run-scope instances a preflight checks, and
	// Binaries answers their declared external programs.
	Instances []planning.Instance
	Binaries  planning.Binaries
	// BuildSource constructs one discovered source the way the run would.
	BuildSource func(ctx context.Context, typ, instance string, raw jsontext.Value) (source.Source, error)
	// Scope owns every instance this invocation built; it is closed by the role
	// after the operation returned, or inside the cancel sequence on SIGTERM.
	Scope *lifecycle.Scope
	// Lifecycle is the effective cleanup budgets.
	Lifecycle lifecycle.Config
	Log       *slog.Logger
}

// Workflow is the backup use case bound to one configuration's collaborators.
type Workflow struct {
	run               RunOptions
	layout            layout.Layout
	inventory         inventory.Inventory
	pipe              *pipeline.Producer
	lease             *protection.Lease
	ledger            *protection.Ledger
	reporter          *reporting.Reporter
	retry             execution.Options
	processorTypes    map[string]string
	processors        map[string]processor.Processor
	runConfigured     bool
	catalogConfigured bool
	storages          []*storageBinding
	sources           []sourceBinding
	discoveries       []discoveryBinding
	notifiers         []notifierBinding
	instances         []planning.Instance
	binaries          planning.Binaries
	buildSource       func(ctx context.Context, typ, instance string, raw jsontext.Value) (source.Source, error)
	log               *slog.Logger
	notifierTypeMap   map[string]string
	// scope owns every instance this invocation built; the run's discovered
	// sources go through it too.
	scope     *lifecycle.Scope
	lifecycle lifecycle.Config
	// The operation goroutine publishes its cancel-sequence state through mu;
	// the supervisor reads it only under the same lock. opDone closes exactly
	// once, when the whole operation returned — the one completion signal the
	// join waits on, so a synchronous phase cannot look like a completed run.
	mu           sync.Mutex
	opDone       chan struct{}
	opCancel     context.CancelFunc
	claim        *catalog.Lease
	transfersRun map[string]*transfer.Transfer
}

// resetInvocation clears the previous operation's cancel state. It runs before
// the operation goroutine starts.
func (e *Workflow) resetInvocation() {
	e.mu.Lock()
	e.opDone = make(chan struct{})
	e.opCancel, e.claim, e.transfersRun = nil, nil, nil
	e.mu.Unlock()
}

// setOpCancel publishes the run's cancellation switch.
func (e *Workflow) setOpCancel(cancel context.CancelFunc) {
	e.mu.Lock()
	e.opCancel = cancel
	e.mu.Unlock()
}

// clearOpCancel drops the publish after the operation returned.
func (e *Workflow) clearOpCancel() {
	e.mu.Lock()
	e.opCancel = nil
	e.mu.Unlock()
}

// setClaim publishes the held claim for the release step.
func (e *Workflow) setClaim(claim *catalog.Lease) {
	e.mu.Lock()
	e.claim = claim
	e.mu.Unlock()
}

// setTransfers publishes the run's transfers for the own-upload abort.
func (e *Workflow) setTransfers(transfers map[string]*transfer.Transfer) {
	e.mu.Lock()
	e.transfersRun = transfers
	e.mu.Unlock()
}

type storageBinding struct {
	name string
	// port is the destination's read port: survey and verification read through
	// it, writes through a Handle opened under the run's Authority, and deletion
	// exists only as a Remover the protection ledger holds.
	port guard.Port
	cfg  Storage
	// retention is this storage's policy per run kind.
	retention map[model.RunKind]retention.Retention
}

type sourceBinding struct {
	name      string
	checkKind string
	typ       string
	inst      source.Source
	artifact  model.Artifact
	pipeline  string
	refs      []string
	procs     []processor.Processor
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

// instanceKey names one built instance in a scoped check result.
type instanceKey struct{ kind, name string }

// failedChecks is the scoped half of a preflight: an instance whose Check
// failed fails its own work, and the run's other instances are unaffected.
type failedChecks map[instanceKey]error

func (c failedChecks) err(kind, name string) error { return c[instanceKey{kind: kind, name: name}] }

// RunRequest is one backup run's request.
type RunRequest struct {
	// Kind is the run's identity: scheduled or manual. It is required and is not
	// a configuration profile — both kinds read the same file.
	Kind model.RunKind
	// DryRun resolves discovery, pipelines and the secret invariant, and writes
	// nothing anywhere.
	DryRun bool
	// ConfigPath is the configuration file the caller read, named in the run's
	// opening log line.
	ConfigPath string
}

// resolvePipeline turns one pipeline — the processor instance names of a rule,
// in encode order — into the instances themselves.
func (w *Workflow) resolvePipeline(ctx context.Context, path string, names []string) ([]processor.Processor, []string, error) {
	procs := make([]processor.Processor, 0, len(names))
	refs := make([]string, 0, len(names))
	for i, ref := range names {
		p, ok := w.processors[ref]
		if !ok {
			return nil, nil, fmt.Errorf("%s/%d: unknown processor instance %q (known: %v)", path, i, ref, sortedKeys(w.processors))
		}
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

// pipelineName is what the manifest keeps of a pipeline for a human reader.
func pipelineName(refs []string) string { return strings.Join(refs, "+") }

// own records one built instance with the invocation scope when it is closeable;
// a plugin without Close has nothing the scope could release.
func own(scope *lifecycle.Scope, v any) {
	if c, ok := v.(io.Closer); ok {
		scope.Own(c)
	}
}

// CancelRequests closes admission and cancels the producer and request
// contexts. It is idempotent.
func (e *Workflow) cancelRequests() {
	e.mu.Lock()
	cancel := e.opCancel
	e.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// join waits for every request goroutine the run started. It returns only when
// they are all gone, or with the cleanup deadline's error while some may still
// run — and then the lease is never released.
func (e *Workflow) join(ctx context.Context) error {
	e.mu.Lock()
	done := e.opDone
	e.mu.Unlock()
	if done == nil {
		return nil
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// releaseClaim CAS-releases this run's claim together with the record's
// uncertainty exactly as it stands. Only a complete join may call it.
func (e *Workflow) releaseClaim(ctx context.Context) error {
	e.mu.Lock()
	claim := e.claim
	e.mu.Unlock()
	if claim == nil {
		return nil
	}
	return e.lease.Release(ctx)
}

// abortOwn aborts exactly this run's own upload IDs, one shared deadline over
// all transfers; no Put, no Complete, no Delete.
func (e *Workflow) abortOwn(ctx context.Context) error {
	e.mu.Lock()
	transfers := make([]*transfer.Transfer, 0, len(e.transfersRun))
	for _, tr := range e.transfersRun {
		transfers = append(transfers, tr)
	}
	e.mu.Unlock()
	var errs []error
	for _, tr := range transfers {
		if err := tr.AbortOwn(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// shutdown runs the cancel sequence on the one cleanup deadline derived from
// the effective lifecycle budgets. Cancellation is not a run failure: phase
// failures are logged and diagnostic, the caller's own failure stays primary.
func (e *Workflow) shutdown(parent context.Context) error {
	grace, err := lifecycle.RequiredGrace(lifecycle.GraceInputs{
		Join:       e.lifecycle.Join.Std(),
		Release:    e.run.FinishAllowance.Std(),
		AbortTotal: e.lifecycle.AbortTotal.Std(),
		Close:      e.lifecycle.Close.Std(),
		Margin:     e.lifecycle.Margin.Std(),
	})
	if err != nil {
		e.event(human.Failure("lifecycle.grace.invalid", human.Text("reason", err.Error())))
		return err
	}
	cleanup, cancel := lifecycle.CleanupContext(parent, grace)
	defer cancel()
	scope := e.scope
	if scope == nil {
		scope = lifecycle.New()
	}
	err = scope.Cancel(cleanup, lifecycle.Shutdown{
		CancelRequests: e.cancelRequests,
		Join:           e.join,
		Release:        e.releaseClaim,
		Abort:          e.abortOwn,
	})
	if err != nil {
		e.event(human.Warning("lifecycle.cancel.failed", human.Text("reason", err.Error())))
	}
	return err
}

// New binds the workflow to its collaborators. The ledger is an argument, not
// an Options field: the Floor package receives the concrete issuer directly from
// an allowlisted holder, and no aggregate value projects it to another package.
func New(o Options, ledger *protection.Ledger) *Workflow {
	w := &Workflow{
		run: o.Run, layout: o.Layout, inventory: o.Inventory, pipe: o.Pipeline,
		lease: o.Lease, ledger: ledger, reporter: o.Reporter, retry: o.Policy,
		processorTypes: o.ProcessorTypes, processors: o.Processors,
		runConfigured: o.RunConfigured, catalogConfigured: o.CatalogConfigured,
		instances: o.Instances, binaries: o.Binaries,
		buildSource: o.BuildSource, log: o.Log, scope: o.Scope, lifecycle: o.Lifecycle,
	}
	for _, s := range o.Storages {
		w.storages = append(w.storages, &storageBinding{name: s.Name, port: s.Port, cfg: s, retention: s.Retention})
	}
	for _, s := range o.Sources {
		w.sources = append(w.sources, sourceBinding{
			name: s.Name, checkKind: s.CheckKind, typ: s.Type, inst: s.Instance,
			artifact: s.Artifact, pipeline: s.Pipeline, refs: s.Refs, procs: s.Processors,
			rateLimit: s.RateLimit,
		})
	}
	for _, d := range o.Discoveries {
		w.discoveries = append(w.discoveries, discoveryBinding{name: d.Name, inst: d.Instance})
	}
	for _, n := range o.Notifiers {
		w.notifiers = append(w.notifiers, notifierBinding{name: n.Name, inst: n.Instance})
		w.notifierTypes()[n.Name] = n.Type
	}
	return w
}

func (w *Workflow) notifierTypes() map[string]string {
	if w.notifierTypeMap == nil {
		w.notifierTypeMap = map[string]string{}
	}
	return w.notifierTypeMap
}

// event emits one event of this workflow's log.
func (w *Workflow) event(ev human.Event) {
	w.log.Log(context.Background(), ev.Severity, ev.Name, slog.Any("event", ev))
}

// at is event on one instance's own logger, so the line carries its prefix.
func (w *Workflow) at(kind, typ, name string, ev human.Event) {
	w.deps(kind, typ, name).Log.Log(context.Background(), ev.Severity, ev.Name, slog.Any("event", ev))
}

func (w *Workflow) storageEvent(sb *storageBinding, ev human.Event) {
	w.at("storage", sb.cfg.Type, sb.name, ev)
}

func (w *Workflow) deps(kind, typ, name string) instances.Deps {
	return instances.Deps{Instance: name, Log: w.log.With("kind", kind, "type", typ, "instance", name)}
}

// storageNames lists the storages in configuration order.
func (w *Workflow) storageNames() []string {
	out := make([]string, 0, len(w.storages))
	for _, sb := range w.storages {
		out = append(out, sb.name)
	}
	return out
}

// notifierType is the registered type of one notifier instance.
func (w *Workflow) notifierType(name string) string { return w.notifierTypes()[name] }

// binaryErrors is what stops a run before any plugin work: a binary a declared
// plugin requires and PATH does not have is the deployment, not a blip.
func (w *Workflow) binaryErrors() []error {
	return planning.BinaryErrors(w.instances, w.binaries)
}

// scopedChecks runs every declared run-scope instance's Check under the run's
// retry policy and keeps the failures that survive, per instance.
func (w *Workflow) scopedChecks(ctx context.Context) failedChecks {

	rows := planning.New(w.retry).Check(ctx, nil, planning.Options{
		Instances: w.instances,
		Binaries:  w.binaries,
	})
	out := failedChecks{}
	for _, r := range rows {
		if r.Err != nil {
			out[instanceKey{kind: r.Kind, name: r.Name}] = r.Err
		}
	}
	return out
}

// sortedKeys returns a map's keys in a stable order.
func sortedKeys[V any](m map[string]V) []string { return slices.Sorted(maps.Keys(m)) }
