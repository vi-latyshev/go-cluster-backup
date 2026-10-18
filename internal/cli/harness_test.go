package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/catalog"
	"github.com/vi-latyshev/go-cluster-backup/internal/catalog/catalogtest"
	"github.com/vi-latyshev/go-cluster-backup/internal/defaults"
	"github.com/vi-latyshev/go-cluster-backup/internal/discovery"
	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/notifier"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/source"
	"github.com/vi-latyshev/go-cluster-backup/internal/storage"
	"github.com/vi-latyshev/go-cluster-backup/internal/storage/storagetest"
)

// testRunID is what run.id_layout "fixedrun" renders: a Go time layout without
// any reference-time token formats to itself, which pins every key a test
// asserts on.
const testRunID = "fixedrun"

// world is the CLI test harness. Everything the cluster would supply is a fake
// the test owns — a source whose bytes the config names, a storage whose every
// operation the test can fail — and the generic plugins are the real ones.
type world struct {
	storages map[string]*storagetest.Fake
	checks   map[string]error // per source instance: its Check fails with this
	found    map[string][]discovery.Found
	sources  map[string]*syntheticSource // per source instance: its Check counter
	// catalogErr and notifierErr arm a failing catalog Save and a failing
	// Notify, so a test can prove a failure is still visible at a high
	// --log-level.
	catalogErr  error
	notifierErr error
	// catalog is the memory catalog the engine builds, so a test can arrange a
	// writer claim and its holder's liveness.
	catalog *memoryCatalog
}

func newWorld() *world {
	return &world{
		storages: map[string]*storagetest.Fake{},
		checks:   map[string]error{},
		found:    map[string][]discovery.Found{},
		sources:  map[string]*syntheticSource{},
		// ONE catalog across every call of a test: the engine is built per
		// command, and a claim a test arranges before the second call must still
		// be there when the second call runs.
		catalog: &memoryCatalog{Leaser: *catalogtest.NewLeaser()},
	}
}

// register is the registry the command layer runs against: every first-party
// plugin plus the two test types.
func (w *world) register(r *plugin.Registry) error {
	if err := Generic(r); err != nil {
		return err
	}
	if err := plugin.Register(r, storage.Kind, plugin.Spec[map[string]jsontext.Value, storage.Storage]{
		Type:      "faulty",
		Summary:   "a storage whose every operation the test can fail",
		Stability: plugin.Experimental,
		Example:   []byte(`{}`),
		New: func(_ context.Context, _ map[string]jsontext.Value, deps plugin.Deps) (storage.Storage, error) {
			return w.storage(deps.Instance), nil
		},
	}); err != nil {
		return err
	}
	if err := plugin.Register(r, discovery.Kind, plugin.Spec[map[string]jsontext.Value, discovery.Discoverer]{
		Type:      "fakediscovery",
		Summary:   "a discovery that returns the Found list the test set",
		Stability: plugin.Experimental,
		Example:   []byte(`{}`),
		New: func(_ context.Context, _ map[string]jsontext.Value, deps plugin.Deps) (discovery.Discoverer, error) {
			return &fakeDiscovery{found: w.found[deps.Instance]}, nil
		},
	}); err != nil {
		return err
	}
	if err := plugin.Register(r, catalog.Kind, plugin.Spec[map[string]jsontext.Value, catalog.Catalog]{
		Type:      "memory",
		Summary:   "an in-memory run history the test owns",
		Stability: plugin.Experimental,
		Example:   []byte(`{}`),
		New: func(_ context.Context, _ map[string]jsontext.Value, _ plugin.Deps) (catalog.Catalog, error) {
			w.catalog.err = w.catalogErr
			return w.catalog, nil
		},
	}); err != nil {
		return err
	}
	if err := plugin.Register(r, notifier.Kind, plugin.Spec[map[string]jsontext.Value, notifier.Notifier]{
		Type:      "memory",
		Summary:   "a notifier that records every event it was handed",
		Stability: plugin.Experimental,
		Example:   []byte(`{}`),
		New: func(_ context.Context, _ map[string]jsontext.Value, _ plugin.Deps) (notifier.Notifier, error) {
			return &memoryNotifier{err: w.notifierErr}, nil
		},
	}); err != nil {
		return err
	}
	return plugin.Register(r, source.Kind, plugin.Spec[syntheticConfig, source.Source]{
		Type:      "synthetic",
		Summary:   "a source that dumps the bytes, claims the origin and restores into the directory its config names",
		Stability: plugin.Experimental,
		Example:   []byte(`{"payload": "example", "dir": "", "origin": "test:example"}`),
		New: func(_ context.Context, cfg syntheticConfig, deps plugin.Deps) (source.Source, error) {
			s := &syntheticSource{cfg: cfg, failCheck: w.checks[deps.Instance]}
			w.sources[deps.Instance] = s
			return s, nil
		},
	})
}

func (w *world) storage(name string) *storagetest.Fake {
	if w.storages[name] == nil {
		w.storages[name] = storagetest.NewFake()
	}
	return w.storages[name]
}

// main runs one command through the real command layer and captures its output.
func (w *world) main(ctx context.Context, args ...string) (int, string, string) {
	var stdout, stderr bytes.Buffer
	code := Main(ctx, Options{Program: "backup", Register: w.register, Defaults: [][]byte{defaults.Defaults, []byte(defaultsYAML)}}, args, &stdout, &stderr)
	return code, stdout.String(), stderr.String()
}

// down fails every operation of one storage, unclassified: the engine's retry
// policy decides what an unclassified error means, exactly as it would for a
// real outage.
func down(op storagetest.Op, key string, call int) error {
	return fmt.Errorf("test storage is down (%s %q call %d)", op, key, call)
}

// memoryCatalog is a run history the test owns: enough of catalog.Catalog for
// the engine's save and previous-run paths, plus the catalog.Leaser capability
// so a manual run can take the writer lease.
type memoryCatalog struct {
	// catalogtest.Leaser is the optional catalog.Leaser capability: one shared
	// in-memory claim, so the two harnesses do not each re-implement it.
	catalogtest.Leaser
	mu   sync.Mutex
	runs []model.RunReport
	err  error
}

func (c *memoryCatalog) Save(_ context.Context, rep model.RunReport) error {
	if c.err != nil {
		return c.err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, r := range c.runs {
		if r.RunID == rep.RunID {
			c.runs[i] = rep
			return nil
		}
	}
	c.runs = append(c.runs, rep)
	return nil
}

func (c *memoryCatalog) Load(_ context.Context, req catalog.LoadRequest) ([]model.RunReport, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]model.RunReport, 0, len(c.runs))
	for i := len(c.runs) - 1; i >= 0; i-- {
		out = append(out, c.runs[i])
		if req.Limit > 0 && len(out) == req.Limit {
			break
		}
	}
	return out, nil
}

func (c *memoryCatalog) Forget(_ context.Context, runIDs []string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	drop := map[string]bool{}
	for _, id := range runIDs {
		drop[id] = true
	}
	kept := c.runs[:0]
	for _, r := range c.runs {
		if !drop[r.RunID] {
			kept = append(kept, r)
		}
	}
	c.runs = kept
	return nil
}

// memoryNotifier records every event it is handed and never fails.
type memoryNotifier struct {
	mu     sync.Mutex
	events []model.Event
	err    error
}

func (n *memoryNotifier) Notify(_ context.Context, ev model.Event) error {
	if n.err != nil {
		return n.err
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.events = append(n.events, ev)
	return nil
}

// fakeDiscovery returns the Found list the test set for its instance.
type fakeDiscovery struct {
	found []discovery.Found
}

func (d *fakeDiscovery) Discover(ctx context.Context) ([]discovery.Found, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return d.found, nil
}

// syntheticConfig is the test source's config: the bytes it dumps, the
// non-secret origin it claims, and where its restorer writes. Every field is
// the test's to state — the module's defaults document knows no such plugin.
type syntheticConfig struct {
	Payload string `json:"payload" required:"true" doc:"bytes the source dumps"`
	Dir     string `json:"dir" required:"true" doc:"directory the restorer writes into; empty means this instance cannot apply"`
	Origin  string `json:"origin" doc:"non-secret origin identity of this instance"`
}

type syntheticSource struct {
	cfg       syntheticConfig
	failCheck error
	mu        sync.Mutex
	checks    int
}

func (s *syntheticSource) Describe(ctx context.Context) (source.SourceTraits, error) {
	if err := ctx.Err(); err != nil {
		return source.SourceTraits{}, err
	}
	return source.SourceTraits{Ext: ".dat", Origin: s.cfg.Origin}, nil
}

func (s *syntheticSource) Dump(ctx context.Context, dst io.Writer) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err := io.WriteString(dst, s.cfg.Payload)
	return err
}

func (s *syntheticSource) Check(ctx context.Context) error {
	s.mu.Lock()
	s.checks++
	s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.failCheck
}

// checkCount reports how many times Check ran: a scoped preflight proves it
// never touched an excluded instance.
func (s *syntheticSource) checkCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.checks
}

// Apply is the source's restorer half: it writes the decoded bytes into the
// configured directory, named by the mode.
func (s *syntheticSource) Apply(ctx context.Context, req source.ApplyRequest, src io.Reader) (source.ApplyReport, error) {
	if err := ctx.Err(); err != nil {
		return source.ApplyReport{}, err
	}
	if s.cfg.Dir == "" {
		return source.ApplyReport{}, errors.New("synthetic: dir is empty, this instance cannot apply")
	}
	name := "side-by-side-" + req.Label
	if req.Mode == source.ApplyInPlace {
		name = "in-place"
	}
	data, err := io.ReadAll(src)
	if err != nil {
		return source.ApplyReport{}, err
	}
	path := filepath.Join(s.cfg.Dir, name)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return source.ApplyReport{}, err
	}
	return source.ApplyReport{Target: path, Bytes: int64(len(data))}, nil
}

func (s *syntheticSource) Probe(ctx context.Context, src io.Reader) (source.ProbeReport, error) {
	if err := ctx.Err(); err != nil {
		return source.ProbeReport{}, err
	}
	n, err := io.Copy(io.Discard, src)
	return source.ProbeReport{Entries: 1, Bytes: n}, err
}

// configYAML renders the run, layout and restore head every test config shares.
// dir is the test's temporary directory: spool, restore spool and both storage
// roots live under it. The restore order is [offsite, exchange], so a fetch test
// can take the second storage down and prove the walk moves on.
func configYAML(dir, body string) string {
	return fmt.Sprintf(`version: 1
run:
  id_layout: fixedrun
  spool_dir: "%[1]s/spool"
  spool_reserve_bytes: 0
  max_artifact_bytes: 1073741824
  deadline: 1m
  finish_allowance: 5s
  min_verified_storages: 1
  degraded_exit_code: 3
  failed_exit_code: 7
  storage_parallelism: 1
  unclassified_error: transient
  notifier_failure_is_fatal: false
  progress_interval: 15s
  lease_hard_bound: 8400s
  retry: {attempts: 1, initial_delay: 1ms, max_delay: 1ms, jitter: 0}
layout:
  run_prefix: "runs/{{ .Run.ID }}"
  artifact_key: "{{ .Artifact.Group }}/{{ .Artifact.Name }}{{ .Artifact.Ext }}"
  manifest_key: manifest.json
  manual_id_suffix: "-manual"
restore:
  spool_dir: "%[1]s/restore"
  spool_reserve_bytes: 0
  storage_order: [offsite, exchange]
  unclassified_error: transient
  retry: {attempts: 1, initial_delay: 1ms, max_delay: 1ms, jitter: 0}
%s`, dir, body)
}

// exchangeYAML is the real local storage of every test config; root is a
// directory the test inspects afterwards.
func exchangeYAML(root string) string {
	return fmt.Sprintf("  exchange:\n    type: local\n    retention: %s\n    max_total_bytes: %s\n    time_budget: 10s\n    config: {root: %q, page_size: 100}\n", keepRetention, keepCap, root)
}

// faultyYAML is the second storage: the one a scenario can take down.
var faultyYAML = `  offsite:
    type: faulty
    retention: ` + keepRetention + `
    max_total_bytes: ` + keepCap + `
    time_budget: 10s
    config: {}
`

// "" is the one retention policy both storages share.
// keepRetention is the scheduled retention policy every storage of a test config
// carries: the policy belongs to its storage, one per run kind, so the storages
// spread it into their own entry. The storage's own cap lives on the envelope.
const keepRetention = `{scheduled: {type: keep_last, config: {count: 3, min_keep: 1, count_incomplete: true}}}`

// keepCap is the configured byte cap every test storage carries.
const keepCap = `1048576`

// zstdYAML is a real processor with a small window: the pipeline every plain
// artifact test goes through.
const zstdYAML = `processors:
  squeeze:
    type: zstd
    config: {level: fastest, concurrency: 1, window_bytes: 1024, decode_window_bytes: 1024}
`

// plainSourceYAML is one plain synthetic source whose artifact is group "g",
// name "n" — two different strings, so a test can tell the name from the key.
func plainSourceYAML(payload string) string {
	return fmt.Sprintf(`sources:
  thing:
    type: synthetic
    artifact: {group: g, name: n, importance: required, sensitivity: plain}
    pipeline: [squeeze]
    config: {payload: %s, dir: "", origin: "test:thing"}
`, payload)
}

func mustWrite(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func mustRead(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// artifactKey returns the one object of a run that is not its manifest, so a
// test can count the reads of the object itself.
func artifactKey(t *testing.T, f *storagetest.Fake) string {
	t.Helper()
	var keys []string
	for _, k := range f.Keys() {
		if !strings.HasSuffix(k, "/manifest.json") {
			keys = append(keys, k)
		}
	}
	if len(keys) != 1 {
		t.Fatalf("the storage holds %v, want exactly one artifact", f.Keys())
	}
	return keys[0]
}

// uniqueRunIDs rewrites one config file's id_layout to a nanosecond layout, so a
// test that runs the engine more than once does not hit the run-id collision the
// engine refuses: two runs of one fixed id are the same run, and would overwrite
// each other's objects.
func uniqueRunIDs(t *testing.T, path string) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw = bytes.Replace(raw, []byte("id_layout: fixedrun"), []byte("id_layout: 20060102T150405.000000000"), 1)
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
}
