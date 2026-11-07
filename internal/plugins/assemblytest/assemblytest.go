// Package assemblytest is the shared test-kit of the assembly and workflow
// tests: the observable fake world, its fakes and the configuration builders
// both suites start from. It is a normal package with its own tests, like
// storage/storagetest and catalog/catalogtest.
package assemblytest

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"encoding/json/jsontext"
	json "encoding/json/v2"

	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/catalog"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/catalog/catalogtest"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/configuration"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/discovery"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/must"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/notifier"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/processor"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage/storagetest"
)

// RegisterFunc contributes one distribution's compiled plugins to a kit world:
// the binary's composition root or the test's own registration.
type RegisterFunc func(*loader.Registry, *instances.Registry) error

// World is the test harness: fakes are created by the test, the registry hands
// them back by instance name, and everything the assembly does stays observable.
type World struct {
	Registry     *instances.Registry
	Loader       *loader.Registry
	Sources      map[string]*Source
	ExtraSources map[string]source.Source
	// extraStorages replaces the in-memory fake for one instance name, for the
	// storages that have to honour a context mid-call.
	ExtraStorages map[string]storage.Storage
	Storages      map[string]*storagetest.Fake
	Processors    map[string]*Processor
	Discoveries   map[string]*Discovery
	Notifiers     map[string]*Notifier
	Catalog       *Catalog
	// configs is the config every source instance was built from, by instance
	// name: what proves the run handed a unit's own config to the build.
	Configs map[string]map[string]jsontext.Value
}

func NewWorld(register RegisterFunc) (*World, error) {
	w := &World{
		Registry:      instances.NewRegistry(),
		Sources:       map[string]*Source{},
		ExtraSources:  map[string]source.Source{},
		ExtraStorages: map[string]storage.Storage{},
		Storages:      map[string]*storagetest.Fake{},
		Processors:    map[string]*Processor{},
		Discoveries:   map[string]*Discovery{},
		Notifiers:     map[string]*Notifier{},
		Configs:       map[string]map[string]jsontext.Value{},
	}
	registerFakes(w)
	lr := loader.New()
	// The distribution's compiled plugins come from its one composition root:
	// the same registration line the binary runs, never a per-plugin list a test
	// has to keep in step.
	if register != nil {
		if Err := register(lr, w.Registry); Err != nil {
			return nil, Err
		}
	}
	// The fake factories are compiled plugins of this test distribution: each is
	// selectable (a no-op apply, not a library) so a test can turn one off and
	// prove the build gate refuses it.
	for _, id := range []string{
		source.Kind.ID("fake"), processor.Kind.ID("fake"), storage.Kind.ID("fake"),
		discovery.Kind.ID("fake"), notifier.Kind.ID("fake"), catalog.Kind.ID("fake"),
	} {
		must.Do(0, loader.Add(lr, loader.Spec{ID: id}, func(*loader.Registry) error { return nil }))
	}
	// The roles the configuration sections demand. A distribution that already
	// carries the CLI host registers them itself; the kit supplies its stand-in
	// roles only for a hostless test distribution.
	if !hasOperationalRoles(lr) {
		must.Do(0, AddTestRoles(lr))
	}
	w.Loader = lr
	return w, nil
}

// registerFakes registers the kit's six factory types in one registry. Their
// registration cannot fail in a fresh registry.
func registerFakes(w *World) {
	must.Do(0, instances.Register(w.Registry, source.Kind, instances.FactorySpec[map[string]jsontext.Value, source.Source]{
		Type: "fake", Example: []byte(`{}`),
		New: func(_ context.Context, cfg map[string]jsontext.Value, deps instances.Deps) (source.Source, error) {
			w.Configs[deps.Instance] = cfg
			if s, ok := w.ExtraSources[deps.Instance]; ok {
				return s, nil
			}
			s, ok := w.Sources[deps.Instance]
			if !ok {
				s = &Source{Data: []byte("artifact data for " + deps.Instance)}
				w.Sources[deps.Instance] = s
			}
			return s, nil
		},
	}))
	must.Do(0, instances.Register(w.Registry, processor.Kind, instances.FactorySpec[map[string]jsontext.Value, processor.Processor]{
		Type: "fake", Example: []byte(`{}`),
		New: func(_ context.Context, _ map[string]jsontext.Value, deps instances.Deps) (processor.Processor, error) {
			p, ok := w.Processors[deps.Instance]
			if !ok {
				p = &Processor{}
				w.Processors[deps.Instance] = p
			}
			return p, nil
		},
	}))
	must.Do(0, storagetest.RegisterFake(w.Registry, w.StorageFor))
	must.Do(0, instances.Register(w.Registry, discovery.Kind, instances.FactorySpec[map[string]jsontext.Value, discovery.Discoverer]{
		Type: "fake", Example: []byte(`{}`),
		New: func(_ context.Context, _ map[string]jsontext.Value, deps instances.Deps) (discovery.Discoverer, error) {
			d, ok := w.Discoveries[deps.Instance]
			if !ok {
				d = &Discovery{}
				w.Discoveries[deps.Instance] = d
			}
			return d, nil
		},
	}))
	must.Do(0, instances.Register(w.Registry, notifier.Kind, instances.FactorySpec[map[string]jsontext.Value, notifier.Notifier]{
		Type: "fake", Example: []byte(`{}`),
		New: func(_ context.Context, _ map[string]jsontext.Value, deps instances.Deps) (notifier.Notifier, error) {
			n, ok := w.Notifiers[deps.Instance]
			if !ok {
				n = &Notifier{}
				w.Notifiers[deps.Instance] = n
			}
			return n, nil
		},
	}))
	must.Do(0, instances.Register(w.Registry, catalog.Kind, instances.FactorySpec[map[string]jsontext.Value, catalog.Catalog]{
		Type: "fake", Example: []byte(`{}`),
		New: func(_ context.Context, _ map[string]jsontext.Value, _ instances.Deps) (catalog.Catalog, error) {
			if w.Catalog == nil {
				w.Catalog = &Catalog{}
			}
			return w.Catalog, nil
		},
	}))
}

// hasOperationalRoles reports whether the compiled set already owns the
// backup job role, which the CLI host's Apply registers with all its siblings.
func hasOperationalRoles(lr *loader.Registry) bool {
	for _, roles := range loader.Roles(lr) {
		for _, role := range roles {
			if role == string(configuration.RoleBackup) {
				return true
			}
		}
	}
	return false
}

// AddTestRoles registers the three operational roles on a standalone compiled
// set, as the CLI host registers them in the binary.
func AddTestRoles(lr *loader.Registry) error {
	return loader.Add(lr, loader.Spec{ID: "test-roles"}, func(lr *loader.Registry) error {
		for _, name := range []string{
			string(configuration.RoleBackup), string(configuration.RoleStage), string(configuration.RoleRestore),
		} {
			if Err := loader.Role(lr, loader.RoleSpec{
				Name: name, Run: testRoleRun, Disabled: testRoleDisabled,
			}); Err != nil {
				return Err
			}
		}
		return nil
	})
}

// testRoleRun and testRoleDisabled are the kit's stand-in entrypoints: every
// role runs nothing and refuses when its plugin is not active.
func testRoleRun(context.Context, loader.Session) int { return 0 }

func testRoleDisabled(loader.Session) int { return 1 }

// syncLoader declares every factory the test has registered since NewWorld as
// a compiled plugin of the test distribution, so a run-time registration is
// selectable exactly like the harness's own fakes. It resets the selection to
// all-active; the assembly's Resolve applies the configuration's list next.
func (w *World) SyncLoader() {
	compiled := map[string]bool{}
	for _, p := range loader.Plugins(w.Loader) {
		compiled[p.ID] = true
	}
	for _, kind := range []string{
		source.Kind.Name(), processor.Kind.Name(), storage.Kind.Name(),
		discovery.Kind.Name(), notifier.Kind.Name(), catalog.Kind.Name(),
	} {
		for _, typ := range w.Registry.Types(kind) {
			id := instances.ID(kind, typ)
			if compiled[id] {
				continue
			}
			must.Do(0, loader.Add(w.Loader, loader.Spec{ID: id}, func(*loader.Registry) error { return nil }))
		}
	}
	_ = loader.ApplySelection(w.Loader, nil)
}

func (w *World) StorageFor(name string) storage.Storage {
	if s, ok := w.ExtraStorages[name]; ok {
		return s
	}
	if s, ok := w.Storages[name]; ok {
		return s
	}
	s := storagetest.NewFake()
	w.Storages[name] = s
	return s
}

func (w *World) Storage(name string) (*storagetest.Fake, bool) {
	s, ok := w.Storages[name]
	return s, ok
}

// --- config builders ---------------------------------------------------------

func ConfigRun(spoolDir string) map[string]any {
	return map[string]any{
		"id_layout":                 "20060102T150405Z",
		"spool_dir":                 spoolDir,
		"spool_reserve_bytes":       0,
		"max_artifact_bytes":        int64(1) << 30,
		"deadline":                  "30s",
		"finish_allowance":          "5s",
		"min_verified_storages":     1,
		"degraded_exit_code":        3,
		"failed_exit_code":          7,
		"storage_parallelism":       1,
		"unclassified_error":        "transient",
		"notifier_failure_is_fatal": false,
		"progress_interval":         "15s",
		"lease_wait":                "0s",
		"lease_poll":                "10ms",
		"lease_hard_bound":          "10ms",
		"holder_namespace_env":      "",
		"holder_pod_env":            "",
		"holder_uid_env":            "",
		"retry": map[string]any{
			"attempts": 3, "initial_delay": "1ms", "max_delay": "5ms", "jitter": 0,
		},
	}
}

func ConfigLayout() map[string]any {
	return map[string]any{
		"run_prefix":       "runs/{{ .Run.ID }}",
		"artifact_key":     "{{ .Artifact.Group }}/{{ .Artifact.Name }}{{ .Artifact.Ext }}",
		"manifest_key":     "manifest.json",
		"manual_id_suffix": "-manual",
	}
}

func Instance(typ string) map[string]any {
	return map[string]any{"type": typ, "config": map[string]any{}}
}

func SourceInstance(typ, group, name string, pipeline []string, importance, sensitivity string) map[string]any {
	return map[string]any{
		"type":     typ,
		"artifact": map[string]any{"group": group, "name": name, "importance": importance, "sensitivity": sensitivity},
		"pipeline": pipeline,
		"config":   map[string]any{},
	}
}

// StorageInstance builds one storage whose scheduled retention is the named
// type, keep_last by default: the policy is inline per run kind now, so a test
// that wants another one edits the entry (see WithRetention, WithManualRetention).
func StorageInstance(typ, retentionType string) map[string]any {
	return map[string]any{
		"type": typ,
		"retention": map[string]any{
			"scheduled": map[string]any{"type": retentionType, "config": KeepLastCfg(3)["config"]},
		},
		"max_total_bytes": int64(1) << 40,
		"time_budget":     "10s", "config": map[string]any{},
		"call_parallelism": 1,
	}
}

// WithRetention replaces one storage's scheduled retention policy.
func WithRetention(cfg map[string]any, storage string, retention map[string]any) map[string]any {
	cfg["storages"].(map[string]any)[storage].(map[string]any)["retention"] = map[string]any{"scheduled": retention}
	return cfg
}

// WithManualRetention adds one storage's manual retention policy, enabling
// manual runs there.
func WithManualRetention(cfg map[string]any, storage string, retention map[string]any) map[string]any {
	ret := cfg["storages"].(map[string]any)[storage].(map[string]any)["retention"].(map[string]any)
	ret["manual"] = retention
	return cfg
}

func KeepLastCfg(count int) map[string]any {
	return map[string]any{"type": "keep_last", "config": map[string]any{
		"count": count, "min_keep": 1, "count_incomplete": true,
	}}
}

// BaseConfig is the smallest complete run config: two sources, one processor,
// one pipeline, the given storages and one keep_last retention.
func BaseConfig(t *testing.T, storages map[string]any) map[string]any {
	return map[string]any{
		"run":        ConfigRun(t.TempDir()),
		"layout":     ConfigLayout(),
		"sources":    map[string]any{"vpn": SourceInstance("fake", "vpn", "wireguard", []string{"zstd3"}, "required", "plain")},
		"processors": map[string]any{"zstd3": Instance("fake")},
		"storages":   storages,
	}
}

// --- fakes -------------------------------------------------------------------

type Source struct {
	Data        []byte
	Traits      source.SourceTraits
	DescribeErr error
	DumpErr     error
	DumpPanic   bool
	// CheckOnce makes the first Check fail with this error and later ones
	// succeed; CheckErr makes every Check fail. Both model what preflight now
	// scopes to one instance.
	CheckOnce error
	CheckErr  error
	// DumpDelay makes Dump take that long: production time, which must not be
	// charged to a storage's time budget. DumpOnce Fails the first Dump only.
	DumpDelay time.Duration
	DumpOnce  error
	// Units makes this source a multi-artifact record; without them it answers
	// errors.ErrUnsupported, the "I am one artifact" every other fake keeps
	// saying. UnitsErr Fails the enumeration itself.
	UnitList []source.Unit
	UnitsErr error
	// UnitsCalls counts enumerations: what a test measures execution's retry
	// rule on.
	UnitsCalls int
	// Stats is what DumpStats reports after a Dump; StatsErr Fails the report.
	Stats    model.DumpStats
	StatsErr error
	Checks   int
	Dumps    int
	mu       sync.Mutex
}

// Units is the optional source.Enumerator capability: a domain of several
// artifacts, or the explicit "one artifact" of every other fake.
func (f *Source) Units(ctx context.Context) ([]source.Unit, error) {
	if Err := ctx.Err(); Err != nil {
		return nil, Err
	}
	f.mu.Lock()
	f.UnitsCalls++
	f.mu.Unlock()
	if f.UnitsErr != nil {
		return nil, f.UnitsErr
	}
	if f.UnitList == nil {
		return nil, errors.ErrUnsupported
	}
	return f.UnitList, nil
}

// DumpStats is the optional source.DumpStatsReporter capability.
func (f *Source) DumpStats(ctx context.Context) (model.DumpStats, error) {
	if Err := ctx.Err(); Err != nil {
		return model.DumpStats{}, Err
	}
	if f.StatsErr != nil {
		return model.DumpStats{}, f.StatsErr
	}
	return f.Stats, nil
}

func (f *Source) Describe(ctx context.Context) (source.SourceTraits, error) {
	if Err := ctx.Err(); Err != nil {
		return source.SourceTraits{}, Err
	}
	if f.DescribeErr != nil {
		return source.SourceTraits{}, f.DescribeErr
	}
	tr := f.Traits
	if tr.Ext == "" && !tr.Sealed {
		tr.Ext = ".dat"
	}
	if tr.Origin == "" {
		tr.Origin = "test://fake/source"
	}
	return tr, nil
}

// ChainSource is a source whose bytes another process produced — the staged
// one's contract: it reports the recorded chain through the optional capability
// and claims nothing about sealing itself.
type ChainSource struct {
	Data  []byte
	Ext   string
	Chain []model.ProcessorStep
}

func (c *ChainSource) Describe(ctx context.Context) (source.SourceTraits, error) {
	if Err := ctx.Err(); Err != nil {
		return source.SourceTraits{}, Err
	}
	return source.SourceTraits{Ext: c.Ext, Origin: "test://chain/source"}, nil
}

func (c *ChainSource) Dump(ctx context.Context, dst io.Writer) error {
	if Err := ctx.Err(); Err != nil {
		return Err
	}
	_, Err := dst.Write(c.Data)
	return Err
}

func (c *ChainSource) RecordedChain(ctx context.Context) ([]model.ProcessorStep, error) {
	if Err := ctx.Err(); Err != nil {
		return nil, Err
	}
	return c.Chain, nil
}

func (f *Source) Dump(ctx context.Context, dst io.Writer) error {
	f.mu.Lock()
	f.Dumps++
	f.mu.Unlock()
	if f.DumpPanic {
		panic("test source panic")
	}
	if Err := ctx.Err(); Err != nil {
		return Err
	}
	if f.DumpErr != nil {
		return f.DumpErr
	}
	if f.Dumps == 1 && f.DumpOnce != nil {
		return f.DumpOnce
	}
	if f.DumpDelay > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(f.DumpDelay):
		}
	}
	_, Err := dst.Write(f.Data)
	return Err
}

func (f *Source) Check(ctx context.Context) error {
	if Err := ctx.Err(); Err != nil {
		return Err
	}
	f.mu.Lock()
	f.Checks++
	n := f.Checks
	Err := f.CheckErr
	if n == 1 && f.CheckOnce != nil {
		Err = f.CheckOnce
	}
	f.mu.Unlock()
	return Err
}

type Processor struct {
	Seals     bool
	Ext       string
	EncodeErr error
	PanicNow  bool
	CheckOnce error
	CheckErr  error
	Checks    int
	// DecodeHook runs at the start of every Decode call.
	DecodeHook func()
}

func (p *Processor) Check(context.Context) error {
	p.Checks++
	if p.Checks == 1 && p.CheckOnce != nil {
		return p.CheckOnce
	}
	return p.CheckErr
}

func (p *Processor) Describe(ctx context.Context) (processor.ProcessorTraits, error) {
	if Err := ctx.Err(); Err != nil {
		return processor.ProcessorTraits{}, Err
	}
	Ext := p.Ext
	if Ext == "" {
		Ext = ".zst"
	}
	return processor.ProcessorTraits{Ext: Ext, Seals: p.Seals}, nil
}

func (p *Processor) Encode(ctx context.Context, dst io.Writer, src io.Reader) error {
	if p.PanicNow {
		panic("test processor panic")
	}
	if p.EncodeErr != nil {
		return p.EncodeErr
	}
	_, Err := io.Copy(dst, src)
	_ = ctx
	return Err
}

func (p *Processor) Decode(ctx context.Context, dst io.Writer, src io.Reader) error {
	_ = ctx
	if p.DecodeHook != nil {
		p.DecodeHook()
	}
	if p.PanicNow {
		panic("test processor panic")
	}
	_, Err := io.Copy(dst, src)
	return Err
}

type Discovery struct {
	Found     []discovery.Found
	Err       error
	ErrOnce   error
	Discovers int
	PanicNow  bool
	CheckErr  error
}

func (d *Discovery) Check(context.Context) error { return d.CheckErr }

func (d *Discovery) Discover(ctx context.Context) ([]discovery.Found, error) {
	if d.PanicNow {
		panic("test discovery panic")
	}
	if Err := ctx.Err(); Err != nil {
		return nil, Err
	}
	d.Discovers++
	if d.Discovers == 1 && d.ErrOnce != nil {
		return nil, d.ErrOnce
	}
	if d.Err != nil {
		return nil, d.Err
	}
	return d.Found, nil
}

type Notifier struct {
	mu       sync.Mutex
	Events   []model.Event
	Fails    int    // fail this many calls, then succeed
	FailOn   string // fail every call for this event name, whatever Fails says
	CheckErr error
}

func (n *Notifier) Check(context.Context) error { return n.CheckErr }

func (n *Notifier) Notify(ctx context.Context, ev model.Event) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if Err := ctx.Err(); Err != nil {
		return Err
	}
	if n.FailOn == ev.Name {
		return errors.New("test notifier failure")
	}
	if n.Fails > 0 {
		n.Fails--
		return errors.New("test notifier failure")
	}
	n.Events = append(n.Events, ev)
	return nil
}

// EventNames lists the names of the events this notifier received, in order.
func (n *Notifier) EventNames() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	names := make([]string, 0, len(n.Events))
	for _, ev := range n.Events {
		names = append(names, ev.Name)
	}
	return names
}

// Last answers the newest event this notifier received.
func (n *Notifier) Last() (model.Event, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if len(n.Events) == 0 {
		return model.Event{}, false
	}
	return n.Events[len(n.Events)-1], true
}

// Finished answers the newest finished event this notifier received.
func (n *Notifier) Finished() (model.Event, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for i := len(n.Events) - 1; i >= 0; i-- {
		if n.Events[i].Name == model.EventFinished {
			return n.Events[i], true
		}
	}
	return model.Event{}, false
}

type Catalog struct {
	// catalogtest.Leaser is the in-memory writer-lease record: the one shared
	// implementation the conformance suite and every assembly harness use, so a
	// test never drives the lease protocol by hand.
	catalogtest.Leaser
	CheckErr error
}

func (c *Catalog) Check(context.Context) error { return c.CheckErr }

// helpers ---------------------------------------------------------------------

func Logger() *slog.Logger { return slog.New(slog.DiscardHandler) }

func JSONMarshal(v any) (jsontext.Value, error) { return json.Marshal(v) }

func JSONUnmarshal(b []byte, v any) error { return json.Unmarshal(b, v) }

func ReadDirNames(dir string) ([]string, error) {
	entries, Err := os.ReadDir(dir)
	if Err != nil {
		return nil, Err
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names, nil
}

func MustMarshal(v any) []byte { return must.Do(json.Marshal(v)) }

// checkCount and dumpCount are what a test measures execution's retry rule on.
// Close makes every fake source a closeable instance: the shared kit stands in
// for the plugins whose clients a scope must release.
func (f *Source) Close() error { return nil }

func (f *Source) CheckCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.Checks
}

func (f *Source) DumpCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.Dumps
}
