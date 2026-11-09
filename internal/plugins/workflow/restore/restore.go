// Package restore is the restore workflow: it lists what the storages hold,
// resolves and downloads one artifact over guarded read ports, applies it to a
// target and verifies it at three depths. Every storage is a guard.Port — the
// workflow has no method that could write to a destination.
package restore

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/execution"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/human"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/inventory"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/layout"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/pipeline"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/processor"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage/guard"
)

// Storage is one destination the restore paths may read: its guarded read port,
// the parallelism of its manifest reads and the instance-prefixed logger its
// survey events go to.
type Storage struct {
	Name            string
	Port            guard.Port
	CallParallelism int
	Log             *slog.Logger
}

// Options are the workflow's typed collaborators, projected by assembly. A
// workflow never receives the configuration document, a registry or a raw
// storage instance.
type Options struct {
	// Layout renders the manifest keys and reads a run's kind back from its id.
	Layout layout.Layout
	// Policy is the execution owner's retry policy for the restore paths.
	Policy execution.Options
	// Configured is true when the configuration carries a restore section: the
	// spool and the walk order live there, and every path that needs one refuses
	// without it.
	Configured bool
	SpoolDir   string
	Reserve    int64
	// Order is restore.storage_order: the walk when a request names no storage.
	Order []string
	// Storages are every declared destination, in declaration order.
	Storages []Storage
	// Processors are the configured processor instances by name; ProcessorTypes
	// is the configured type of each, which a manifest's recorded chain must
	// agree with.
	Processors     map[string]processor.Processor
	ProcessorTypes map[string]string
	// Targets resolves the source an apply or a probe writes into. It is nil
	// when the configuration declares no target; apply and a probed verification
	// then refuse.
	Targets Targets
	// ApplyOptions are the per-target-type `restore.apply` settings.
	ApplyOptions map[string]jsontext.Value
	// Log receives this workflow's own events.
	Log *slog.Logger
}

// Workflow is the restore use case bound to one configuration's collaborators.
type Workflow struct {
	o     Options
	inv   inventory.Inventory
	names []string
}

// New binds the restore workflow to its collaborators. A storage without a
// logger gets a discarding one: its survey events are diagnostics, never a
// reason to fail the call.
func New(o Options) *Workflow {
	w := &Workflow{o: o, inv: inventory.New(inventory.Options{Layout: o.Layout, Policy: o.Policy})}
	w.names = make([]string, 0, len(o.Storages))
	for i := range w.o.Storages {
		if w.o.Storages[i].Log == nil {
			w.o.Storages[i].Log = slog.New(slog.DiscardHandler)
		}
	}
	for _, s := range w.o.Storages {
		w.names = append(w.names, s.Name)
	}
	return w
}

// event emits one workflow event through the projected logger.
func (w *Workflow) event(ev human.Event) {
	if w.o.Log == nil {
		return
	}
	w.o.Log.Log(context.Background(), ev.Severity, ev.Name, slog.Any("event", ev))
}

// binding is one storage of the walk: the guarded port every restore path reads
// through, and what its survey needs.
type binding struct {
	name            string
	port            guard.Port
	callParallelism int
	log             *slog.Logger
}

func (w *Workflow) storageByName(name string) *binding {
	for i := range w.o.Storages {
		if w.o.Storages[i].Name == name {
			return w.binding(&w.o.Storages[i])
		}
	}
	return nil
}

func (w *Workflow) binding(s *Storage) *binding {
	return &binding{name: s.Name, port: s.Port, callParallelism: s.CallParallelism, log: s.Log}
}

func (w *Workflow) storageNames() []string { return w.names }

// orderStorages resolves the walk of one request: the names it gave, else
// restore.storage_order.
func (w *Workflow) orderStorages(names []string) ([]*binding, error) {
	if len(names) == 0 {
		if !w.o.Configured {
			return nil, errNoRestoreSection("restore")
		}
		names = w.o.Order
	}
	out := make([]*binding, 0, len(names))
	for _, name := range names {
		s := w.storageByName(name)
		if s == nil {
			return nil, fmt.Errorf("unknown storage %q (known: %v)", name, w.storageNames())
		}
		out = append(out, s)
	}
	return out, nil
}

// StorageOrder names the storages a fetch walks, in walk order: the request's
// own list when it gave one, else restore.storage_order — the same derivation
// the walk itself uses, exposed so a caller cannot name a different set.
func (w *Workflow) StorageOrder(names []string) ([]string, error) {
	sbs, err := w.orderStorages(names)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(sbs))
	for _, sb := range sbs {
		out = append(out, sb.name)
	}
	return out, nil
}

// StoredRuns lists the runs every storage holds, including runs without a
// manifest. Storages are surveyed in the caller's order; a storage that cannot
// be listed fails the whole listing, never a partial view.
func (w *Workflow) StoredRuns(ctx context.Context, names []string) ([]inventory.StoredRunView, error) {
	storages, err := w.orderStorages(names)
	if err != nil {
		return nil, err
	}
	prefix, err := w.prefix("stored runs")
	if err != nil {
		return nil, err
	}
	var out []inventory.StoredRunView
	for _, sb := range storages {
		views, err := w.inv.StoredRuns(ctx, prefix, []inventory.Storage{w.surveyStorage(sb)})
		if err != nil {
			return nil, err
		}
		out = append(out, views...)
	}
	return out, nil
}

// prefix is layout.Prefix with the workflow's own refusal when the
// configuration has no layout section.
func (w *Workflow) prefix(what string) (string, error) {
	prefix, err := w.o.Layout.Prefix()
	if err != nil {
		return "", fmt.Errorf("%s: config has no layout section; a run survey needs layout.run_prefix: %w", what, err)
	}
	return prefix, nil
}

func (w *Workflow) surveyStorage(sb *binding) inventory.Storage {
	return inventory.Storage{Name: sb.name, Reader: sb.port, CallParallelism: sb.callParallelism, Log: sb.log}
}

// surveyFunc memoizes one storage survey per call group, so every path of one
// fetch — the run selection and each storage's turn — sees the same listing.
func (w *Workflow) surveyFunc(ctx context.Context, prefix string, surveys map[string]inventory.Survey) func(*binding) (inventory.Survey, bool) {
	return func(sb *binding) (inventory.Survey, bool) {
		if sur, ok := surveys[sb.name]; ok {
			return sur, sur.OK
		}
		sur := w.inv.SurveyStorage(ctx, prefix, w.surveyStorage(sb))
		surveys[sb.name] = sur
		return sur, sur.OK
	}
}

// decodeStages resolves the manifest's chain against the current config: the
// instance must exist here and its type must match what the manifest recorded.
func (w *Workflow) decodeStages(ctx context.Context, chain []model.ProcessorStep) ([]pipeline.Stage, error) {
	stages := make([]pipeline.Stage, 0, len(chain))
	for i := len(chain) - 1; i >= 0; i-- {
		step := chain[i]
		if step.Instance == "" {
			return nil, fmt.Errorf("fetch: the manifest's chain step has no instance name")
		}
		p, ok := w.o.Processors[step.Instance]
		if !ok {
			return nil, fmt.Errorf("fetch: the manifest's chain names processor instance %q, which this config does not have", step.Instance)
		}
		if ct := w.o.ProcessorTypes[step.Instance]; ct != step.Type {
			return nil, fmt.Errorf("fetch: processor instance %q is type %q here, %q in the manifest", step.Instance, ct, step.Type)
		}
		// The restore side's own preflight, before the first byte is fetched: a
		// processor that cannot decode says so here, with its own name, instead of
		// failing the stream halfway through.
		if d, ok := p.(processor.DecodeChecker); ok {
			if err := d.CheckDecode(ctx); err != nil {
				return nil, fmt.Errorf("fetch: processor %q cannot decode: %w", step.Instance, err)
			}
		}
		stages = append(stages, p.Decode)
	}
	return stages, nil
}

// errNoRestoreSection is the refusal of every path that needs the restore
// section's spool or walk order.
func errNoRestoreSection(what string) error {
	return fmt.Errorf("%s: config has no restore section", what)
}

// joinHolds renders the per-storage refusal of a plan.
func joinHolds(holds []StorageHold) string {
	skips := make([]StorageSkip, 0, len(holds))
	for _, h := range holds {
		if !h.Holds {
			skips = append(skips, StorageSkip{Storage: h.Storage, Reason: h.Reason})
		}
	}
	return joinSkips(skips)
}

// joinSkips renders one skip list.
func joinSkips(skips []StorageSkip) string {
	if len(skips) == 0 {
		return "no storage was tried"
	}
	parts := make([]string, 0, len(skips))
	for _, s := range skips {
		parts = append(parts, fmt.Sprintf("%s: %s", s.Storage, s.Reason))
	}
	return strings.Join(parts, "; ")
}

// artifactOK reports whether the manifest records the artifact as ok.
func artifactOK(rep *model.RunReport, name string) bool {
	for _, a := range rep.Artifacts {
		if a.Artifact.Name == name {
			return a.Outcome == model.ArtifactOK
		}
	}
	return false
}

func findArtifact(rep *model.RunReport, name string) *model.ArtifactResult {
	for i := range rep.Artifacts {
		if rep.Artifacts[i].Artifact.Name == name {
			return &rep.Artifacts[i]
		}
	}
	return nil
}
