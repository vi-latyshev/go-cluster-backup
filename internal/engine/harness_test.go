package engine

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"encoding/json/jsontext"
	json "encoding/json/v2"

	"github.com/vi-latyshev/go-cluster-backup/internal/catalog"
	"github.com/vi-latyshev/go-cluster-backup/internal/catalog/catalogtest"
	"github.com/vi-latyshev/go-cluster-backup/internal/discovery"
	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/notifier"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/retention/gfs"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/retention/keeplast"
	"github.com/vi-latyshev/go-cluster-backup/internal/processor"
	"github.com/vi-latyshev/go-cluster-backup/internal/source"
	"github.com/vi-latyshev/go-cluster-backup/internal/storage"
	"github.com/vi-latyshev/go-cluster-backup/internal/storage/storagetest"
)

// world is the test harness: fakes are created by the test, the registry hands
// them back by instance name, and everything the engine does stays observable.
type world struct {
	registry     *plugin.Registry
	sources      map[string]*fakeSource
	extraSources map[string]source.Source
	// extraStorages replaces the in-memory fake for one instance name, for the
	// storages that have to honour a context mid-call.
	extraStorages map[string]storage.Storage
	storages      map[string]*storagetest.Fake
	processors    map[string]*fakeProcessor
	discoveries   map[string]*fakeDiscovery
	notifiers     map[string]*fakeNotifier
	catalog       *fakeCatalog
	// configs is the config every source instance was built from, by instance
	// name: what proves the engine handed a unit's own config to the build.
	configs map[string]map[string]jsontext.Value
}

func newWorld(t *testing.T) *world {
	t.Helper()
	w := &world{
		registry:      plugin.NewRegistry(),
		sources:       map[string]*fakeSource{},
		extraSources:  map[string]source.Source{},
		extraStorages: map[string]storage.Storage{},
		storages:      map[string]*storagetest.Fake{},
		processors:    map[string]*fakeProcessor{},
		discoveries:   map[string]*fakeDiscovery{},
		notifiers:     map[string]*fakeNotifier{},
		configs:       map[string]map[string]jsontext.Value{},
	}
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(plugin.Register(w.registry, source.Kind, plugin.Spec[map[string]jsontext.Value, source.Source]{
		Type: "fake", Example: []byte(`{}`),
		New: func(_ context.Context, cfg map[string]jsontext.Value, deps plugin.Deps) (source.Source, error) {
			w.configs[deps.Instance] = cfg
			if s, ok := w.extraSources[deps.Instance]; ok {
				return s, nil
			}
			s, ok := w.sources[deps.Instance]
			if !ok {
				s = &fakeSource{data: []byte("artifact data for " + deps.Instance)}
				w.sources[deps.Instance] = s
			}
			return s, nil
		},
	}))
	must(plugin.Register(w.registry, processor.Kind, plugin.Spec[map[string]jsontext.Value, processor.Processor]{
		Type: "fake", Example: []byte(`{}`),
		New: func(_ context.Context, _ map[string]jsontext.Value, deps plugin.Deps) (processor.Processor, error) {
			p, ok := w.processors[deps.Instance]
			if !ok {
				p = &fakeProcessor{}
				w.processors[deps.Instance] = p
			}
			return p, nil
		},
	}))
	must(plugin.Register(w.registry, storage.Kind, plugin.Spec[map[string]jsontext.Value, storage.Storage]{
		Type: "fake", Example: []byte(`{}`),
		New: func(_ context.Context, _ map[string]jsontext.Value, deps plugin.Deps) (storage.Storage, error) {
			if s, ok := w.extraStorages[deps.Instance]; ok {
				return s, nil
			}
			s, ok := w.storages[deps.Instance]
			if !ok {
				s = storagetest.NewFake()
				w.storages[deps.Instance] = s
			}
			return s, nil
		},
	}))
	must(plugin.Register(w.registry, discovery.Kind, plugin.Spec[map[string]jsontext.Value, discovery.Discoverer]{
		Type: "fake", Example: []byte(`{}`),
		New: func(_ context.Context, _ map[string]jsontext.Value, deps plugin.Deps) (discovery.Discoverer, error) {
			d, ok := w.discoveries[deps.Instance]
			if !ok {
				d = &fakeDiscovery{}
				w.discoveries[deps.Instance] = d
			}
			return d, nil
		},
	}))
	must(plugin.Register(w.registry, notifier.Kind, plugin.Spec[map[string]jsontext.Value, notifier.Notifier]{
		Type: "fake", Example: []byte(`{}`),
		New: func(_ context.Context, _ map[string]jsontext.Value, deps plugin.Deps) (notifier.Notifier, error) {
			n, ok := w.notifiers[deps.Instance]
			if !ok {
				n = &fakeNotifier{}
				w.notifiers[deps.Instance] = n
			}
			return n, nil
		},
	}))
	must(plugin.Register(w.registry, catalog.Kind, plugin.Spec[map[string]jsontext.Value, catalog.Catalog]{
		Type: "fake", Example: []byte(`{}`),
		New: func(_ context.Context, _ map[string]jsontext.Value, _ plugin.Deps) (catalog.Catalog, error) {
			if w.catalog == nil {
				w.catalog = &fakeCatalog{}
			}
			return w.catalog, nil
		},
	}))
	must(gfs.Register(w.registry))
	must(keeplast.Register(w.registry))
	return w
}

// registerType registers one more plugin type under a kind, accepting any config —
// the step-2 type names of the cluster config decode through these stand-ins.
func (w *world) registerType(t *testing.T, kind, typ string) {
	t.Helper()
	var err error
	switch kind {
	case "source":
		err = plugin.Register(w.registry, source.Kind, plugin.Spec[map[string]jsontext.Value, source.Source]{
			Type: typ, Example: []byte(`{}`),
			New: func(_ context.Context, cfg map[string]jsontext.Value, deps plugin.Deps) (source.Source, error) {
				w.configs[deps.Instance] = cfg
				if s, ok := w.extraSources[deps.Instance]; ok {
					return s, nil
				}
				if s, ok := w.sources[deps.Instance]; ok {
					return s, nil
				}
				s := &fakeSource{data: []byte("x")}
				w.sources[deps.Instance] = s
				return s, nil
			},
		})
	case "processor":
		err = plugin.Register(w.registry, processor.Kind, plugin.Spec[map[string]jsontext.Value, processor.Processor]{
			Type: typ, Example: []byte(`{}`),
			New: func(_ context.Context, _ map[string]jsontext.Value, deps plugin.Deps) (processor.Processor, error) {
				if p, ok := w.processors[deps.Instance]; ok {
					return p, nil
				}
				p := &fakeProcessor{}
				w.processors[deps.Instance] = p
				return p, nil
			},
		})
	case "storage":
		err = plugin.Register(w.registry, storage.Kind, plugin.Spec[map[string]jsontext.Value, storage.Storage]{
			Type: typ, Example: []byte(`{}`),
			New: func(_ context.Context, _ map[string]jsontext.Value, deps plugin.Deps) (storage.Storage, error) {
				if s, ok := w.storages[deps.Instance]; ok {
					return s, nil
				}
				s := storagetest.NewFake()
				w.storages[deps.Instance] = s
				return s, nil
			},
		})
	case "discovery":
		err = plugin.Register(w.registry, discovery.Kind, plugin.Spec[map[string]jsontext.Value, discovery.Discoverer]{
			Type: typ, Example: []byte(`{}`),
			New: func(_ context.Context, _ map[string]jsontext.Value, _ plugin.Deps) (discovery.Discoverer, error) {
				return &fakeDiscovery{}, nil
			},
		})
	case "catalog":
		err = plugin.Register(w.registry, catalog.Kind, plugin.Spec[map[string]jsontext.Value, catalog.Catalog]{
			Type: typ, Example: []byte(`{}`),
			New: func(_ context.Context, _ map[string]jsontext.Value, _ plugin.Deps) (catalog.Catalog, error) {
				if w.catalog == nil {
					w.catalog = &fakeCatalog{}
				}
				return w.catalog, nil
			},
		})
	case "notifier":
		err = plugin.Register(w.registry, notifier.Kind, plugin.Spec[map[string]jsontext.Value, notifier.Notifier]{
			Type: typ, Example: []byte(`{}`),
			New: func(_ context.Context, _ map[string]jsontext.Value, deps plugin.Deps) (notifier.Notifier, error) {
				if n, ok := w.notifiers[deps.Instance]; ok {
					return n, nil
				}
				n := &fakeNotifier{}
				w.notifiers[deps.Instance] = n
				return n, nil
			},
		})
	default:
		t.Fatalf("unknown kind %q", kind)
	}
	if err != nil {
		t.Fatal(err)
	}
}

func (w *world) newEngine(t *testing.T, cfg map[string]any) *Engine {
	t.Helper()
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	e, err := New(context.Background(), raw, nil, w.registry, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e
}

// newEngineErr is newEngine for the configurations that must be refused at
// build time: it hands the error back instead of failing the test on it.
func (w *world) newEngineErr(t *testing.T, cfg map[string]any) (*Engine, error) {
	t.Helper()
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	e, err := New(context.Background(), raw, nil, w.registry, slog.New(slog.DiscardHandler))
	if e != nil {
		t.Cleanup(func() { _ = e.Close() })
	}
	return e, err
}

func (w *world) storage(t *testing.T, name string) *storagetest.Fake {
	t.Helper()
	s, ok := w.storages[name]
	if !ok {
		t.Fatalf("storage %q not built", name)
	}
	return s
}

// --- config builders ---------------------------------------------------------

func configRun(spoolDir string) map[string]any {
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

func configLayout() map[string]any {
	return map[string]any{
		"run_prefix":       "runs/{{ .Run.ID }}",
		"artifact_key":     "{{ .Artifact.Group }}/{{ .Artifact.Name }}{{ .Artifact.Ext }}",
		"manifest_key":     "manifest.json",
		"manual_id_suffix": "-manual",
	}
}

func instance(typ string) map[string]any {
	return map[string]any{"type": typ, "config": map[string]any{}}
}

func sourceInstance(typ, group, name string, pipeline []string, importance, sensitivity string) map[string]any {
	return map[string]any{
		"type":     typ,
		"artifact": map[string]any{"group": group, "name": name, "importance": importance, "sensitivity": sensitivity},
		"pipeline": pipeline,
		"config":   map[string]any{},
	}
}

// storageInstance builds one storage whose scheduled retention is the named
// type, keep_last by default: the policy is inline per run kind now, so a test
// that wants another one edits the entry (see withRetention, withManualRetention).
func storageInstance(typ, retentionType string) map[string]any {
	return map[string]any{
		"type": typ,
		"retention": map[string]any{
			"scheduled": map[string]any{"type": retentionType, "config": keepLastCfg(3)["config"]},
		},
		"max_total_bytes": int64(1) << 40,
		"time_budget":     "10s", "config": map[string]any{},
	}
}

// withRetention replaces one storage's scheduled retention policy.
func withRetention(cfg map[string]any, storage string, retention map[string]any) map[string]any {
	cfg["storages"].(map[string]any)[storage].(map[string]any)["retention"] = map[string]any{"scheduled": retention}
	return cfg
}

// withManualRetention adds one storage's manual retention policy, enabling
// manual runs there.
func withManualRetention(cfg map[string]any, storage string, retention map[string]any) map[string]any {
	ret := cfg["storages"].(map[string]any)[storage].(map[string]any)["retention"].(map[string]any)
	ret["manual"] = retention
	return cfg
}

func keepLastCfg(count int) map[string]any {
	return map[string]any{"type": "keep_last", "config": map[string]any{
		"count": count, "min_keep": 1, "count_incomplete": true,
	}}
}

// baseConfig is the smallest complete run config: two sources, one processor,
// one pipeline, the given storages and one keep_last retention.
func baseConfig(t *testing.T, storages map[string]any) map[string]any {
	t.Helper()
	return map[string]any{
		"version":    1,
		"run":        configRun(t.TempDir()),
		"layout":     configLayout(),
		"sources":    map[string]any{"vpn": sourceInstance("fake", "vpn", "wireguard", []string{"zstd3"}, "required", "plain")},
		"processors": map[string]any{"zstd3": instance("fake")},
		"storages":   storages,
	}
}

// --- fakes -------------------------------------------------------------------

type fakeSource struct {
	data        []byte
	traits      source.SourceTraits
	describeErr error
	dumpErr     error
	dumpPanic   bool
	// checkOnce makes the first Check fail with this error and later ones
	// succeed; checkErr makes every Check fail. Both model what preflight now
	// scopes to one instance.
	checkOnce error
	checkErr  error
	// dumpDelay makes Dump take that long: production time, which must not be
	// charged to a storage's time budget. dumpOnce fails the first Dump only.
	dumpDelay time.Duration
	dumpOnce  error
	// units makes this source a multi-artifact record; without them it answers
	// errors.ErrUnsupported, the "I am one artifact" every other fake keeps
	// saying. unitsErr fails the enumeration itself.
	units    []source.Unit
	unitsErr error
	// unitsCalls counts enumerations: what a test measures the engine's retry
	// rule on.
	unitsCalls int
	// stats is what DumpStats reports after a Dump; statsErr fails the report.
	stats    model.DumpStats
	statsErr error
	checks   int
	dumps    int
	mu       sync.Mutex
}

// Units is the optional source.Enumerator capability: a domain of several
// artifacts, or the explicit "one artifact" of every other fake.
func (f *fakeSource) Units(ctx context.Context) ([]source.Unit, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	f.unitsCalls++
	f.mu.Unlock()
	if f.unitsErr != nil {
		return nil, f.unitsErr
	}
	if f.units == nil {
		return nil, errors.ErrUnsupported
	}
	return f.units, nil
}

// DumpStats is the optional source.DumpStatsReporter capability.
func (f *fakeSource) DumpStats(ctx context.Context) (model.DumpStats, error) {
	if err := ctx.Err(); err != nil {
		return model.DumpStats{}, err
	}
	if f.statsErr != nil {
		return model.DumpStats{}, f.statsErr
	}
	return f.stats, nil
}

func (f *fakeSource) Describe(ctx context.Context) (source.SourceTraits, error) {
	if err := ctx.Err(); err != nil {
		return source.SourceTraits{}, err
	}
	if f.describeErr != nil {
		return source.SourceTraits{}, f.describeErr
	}
	tr := f.traits
	if tr.Ext == "" && !tr.Sealed {
		tr.Ext = ".dat"
	}
	if tr.Origin == "" {
		tr.Origin = "test://fake/source"
	}
	return tr, nil
}

// chainSource is a source whose bytes another process produced — the staged
// one's contract: it reports the recorded chain through the optional capability
// and claims nothing about sealing itself.
type chainSource struct {
	data  []byte
	ext   string
	chain []model.ProcessorStep
}

func (c *chainSource) Describe(ctx context.Context) (source.SourceTraits, error) {
	if err := ctx.Err(); err != nil {
		return source.SourceTraits{}, err
	}
	return source.SourceTraits{Ext: c.ext, Origin: "test://chain/source"}, nil
}

func (c *chainSource) Dump(ctx context.Context, dst io.Writer) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err := dst.Write(c.data)
	return err
}

func (c *chainSource) RecordedChain(ctx context.Context) ([]model.ProcessorStep, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return c.chain, nil
}

func (f *fakeSource) Dump(ctx context.Context, dst io.Writer) error {
	f.mu.Lock()
	f.dumps++
	f.mu.Unlock()
	if f.dumpPanic {
		panic("test source panic")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if f.dumpErr != nil {
		return f.dumpErr
	}
	if f.dumps == 1 && f.dumpOnce != nil {
		return f.dumpOnce
	}
	if f.dumpDelay > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(f.dumpDelay):
		}
	}
	_, err := dst.Write(f.data)
	return err
}

func (f *fakeSource) unitsCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.unitsCalls
}

func (f *fakeSource) dumpCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.dumps
}

func (f *fakeSource) Check(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	f.checks++
	n := f.checks
	err := f.checkErr
	if n == 1 && f.checkOnce != nil {
		err = f.checkOnce
	}
	f.mu.Unlock()
	return err
}

func (f *fakeSource) checkCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.checks
}

type fakeProcessor struct {
	seals     bool
	ext       string
	encodeErr error
	panicNow  bool
	checkOnce error
	checkErr  error
	checks    int
	// decodeHook runs at the start of every Decode call.
	decodeHook func()
}

func (p *fakeProcessor) Check(context.Context) error {
	p.checks++
	if p.checks == 1 && p.checkOnce != nil {
		return p.checkOnce
	}
	return p.checkErr
}

func (p *fakeProcessor) Describe(ctx context.Context) (processor.ProcessorTraits, error) {
	if err := ctx.Err(); err != nil {
		return processor.ProcessorTraits{}, err
	}
	ext := p.ext
	if ext == "" {
		ext = ".zst"
	}
	return processor.ProcessorTraits{Ext: ext, Seals: p.seals}, nil
}

func (p *fakeProcessor) Encode(ctx context.Context, dst io.Writer, src io.Reader) error {
	if p.panicNow {
		panic("test processor panic")
	}
	if p.encodeErr != nil {
		return p.encodeErr
	}
	_, err := io.Copy(dst, src)
	_ = ctx
	return err
}

func (p *fakeProcessor) Decode(ctx context.Context, dst io.Writer, src io.Reader) error {
	_ = ctx
	if p.decodeHook != nil {
		p.decodeHook()
	}
	if p.panicNow {
		panic("test processor panic")
	}
	_, err := io.Copy(dst, src)
	return err
}

type fakeDiscovery struct {
	found     []discovery.Found
	err       error
	errOnce   error
	discovers int
	panicNow  bool
	checkErr  error
	// rules makes this discoverer a discovery.RuleSourceReporter: with it set,
	// construction validates the rules offline.
	rules []discovery.RuleSource
}

func (d *fakeDiscovery) Check(context.Context) error { return d.checkErr }

func (d *fakeDiscovery) Discover(ctx context.Context) ([]discovery.Found, error) {
	if d.panicNow {
		panic("test discovery panic")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	d.discovers++
	if d.discovers == 1 && d.errOnce != nil {
		return nil, d.errOnce
	}
	if d.err != nil {
		return nil, d.err
	}
	return d.found, nil
}

func (d *fakeDiscovery) RuleSources(ctx context.Context) ([]discovery.RuleSource, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return d.rules, nil
}

type fakeNotifier struct {
	mu       sync.Mutex
	events   []model.Event
	fails    int // fail this many calls, then succeed
	checkErr error
}

func (n *fakeNotifier) Check(context.Context) error { return n.checkErr }

func (n *fakeNotifier) Notify(ctx context.Context, ev model.Event) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if n.fails > 0 {
		n.fails--
		return errors.New("test notifier failure")
	}
	n.events = append(n.events, ev)
	return nil
}

func (n *fakeNotifier) eventNames() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	var names []string
	for _, e := range n.events {
		names = append(names, e.Name)
	}
	return names
}

// last returns the newest event the notifier received.
func (n *fakeNotifier) last() (model.Event, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if len(n.events) == 0 {
		return model.Event{}, false
	}
	return n.events[len(n.events)-1], true
}

type fakeCatalog struct {
	// catalogtest.Leaser is the optional catalog.Leaser capability: one shared
	// in-memory claim, so the two harnesses do not each re-implement it.
	catalogtest.Leaser
	mu       sync.Mutex
	runs     map[string]model.RunReport
	checkErr error
}

func (c *fakeCatalog) Check(context.Context) error { return c.checkErr }

func (c *fakeCatalog) Save(ctx context.Context, run model.RunReport) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.runs == nil {
		c.runs = map[string]model.RunReport{}
	}
	c.runs[run.RunID] = run
	return nil
}

func (c *fakeCatalog) Load(ctx context.Context, req catalog.LoadRequest) ([]model.RunReport, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	var runs []model.RunReport
	for _, r := range c.runs {
		runs = append(runs, r)
	}
	sortByStartedDesc(runs)
	if req.Limit > 0 && len(runs) > req.Limit {
		runs = runs[:req.Limit]
	}
	return runs, nil
}

func (c *fakeCatalog) Forget(ctx context.Context, ids []string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, id := range ids {
		delete(c.runs, id)
	}
	return nil
}

func sortByStartedDesc(runs []model.RunReport) {
	for i := 1; i < len(runs); i++ {
		for j := i; j > 0 && runs[j].StartedAt.After(runs[j-1].StartedAt); j-- {
			runs[j], runs[j-1] = runs[j-1], runs[j]
		}
	}
}

// helpers ---------------------------------------------------------------------

func testLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

func jsonMarshal(v any) (jsontext.Value, error) { return json.Marshal(v) }

func jsonUnmarshal(b []byte, v any) error { return json.Unmarshal(b, v) }

func readDirNames(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names, nil
}

func mustMarshal(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func lastPutKey(t *testing.T, s *storagetest.Fake) string {
	t.Helper()
	last := ""
	for _, ev := range s.Events() {
		if ev.Op == storagetest.OpPut {
			last = ev.Key
		}
	}
	return last
}

func indexOfEvent(events []storagetest.Event, op storagetest.Op, key string) int {
	for i, ev := range events {
		if ev.Op == op && ev.Key == key {
			return i
		}
	}
	return -1
}

// stallingStorage is the in-memory storage with one slow call: a manifest write
// that holds its context and gives up when the context does — what a real
// storage does when a request stalls past its timeout.
type stallingStorage struct {
	*storagetest.Fake
	manifestDelay time.Duration
}

func (s *stallingStorage) Put(ctx context.Context, req storage.PutRequest, body io.Reader) (model.ObjectInfo, error) {
	if s.manifestDelay > 0 && strings.HasSuffix(req.Key, "manifest.json") {
		select {
		case <-ctx.Done():
			return model.ObjectInfo{}, ctx.Err()
		case <-time.After(s.manifestDelay):
		}
	}
	return s.Fake.Put(ctx, req, body)
}
