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

	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/all/components"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/catalog"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/catalog/catalogtest"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/configuration/defaults"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/discovery"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/notifier"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage/storagetest"
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
	// catalogErr and notifierErr arm a failing lease acquisition and a failing
	// Notify, so a test can prove a failure is still visible at a high
	// --log-level.
	catalogErr  error
	notifierErr error
	// catalog is the in-memory writer-lease record the assembly builds, so a test
	// can arrange a writer claim and its holder's liveness.
	catalog *catalogtest.Leaser
	// credentialBytes and credentialVerdicts script the synthetic source's
	// credential facts per instance: whether its bytes carry credentials and the
	// published verdict its policy answers.
	credentialBytes    map[string]bool
	credentialVerdicts map[string]source.CredentialVerdict
}

func newWorld() *world {
	return &world{
		storages: map[string]*storagetest.Fake{},
		checks:   map[string]error{},
		found:    map[string][]discovery.Found{},
		sources:  map[string]*syntheticSource{},
		// ONE lease record across every call of a test: the assembly is built per
		// command, and a claim a test arranges before the second call must still
		// be there when the second call runs.
		catalog:            catalogtest.NewLeaser(),
		credentialBytes:    map[string]bool{},
		credentialVerdicts: map[string]source.CredentialVerdict{},
	}
}

// register adds the compiled distribution — the generated composition root —
// and the test doubles below to one factory registry.
func (w *world) register(r *instances.Registry) error {
	lr := loader.New()
	if err := components.Compose(lr, r); err != nil {
		return err
	}
	return w.registerDoubles(lr, r)
}

// registerDoubles contributes the test doubles the way a real backend does: one
// loader plugin carrying its descriptor and its factory, so selection, describe
// and schema resolve the harness types exactly like compiled ones.
func (w *world) registerDoubles(lr *loader.Registry, r *instances.Registry) error {
	if err := loader.Add(lr, loader.Spec{ID: "storage/faulty", Summary: "a storage whose every operation the test can fail"},
		func(lr *loader.Registry) error {
			return storage.AddFactory(lr, r, storage.FactorySpec[map[string]jsontext.Value]{
				Type:      "faulty",
				Summary:   "a storage whose every operation the test can fail",
				Stability: instances.Experimental,
				Example:   []byte(`{}`),
				New: func(_ context.Context, _ map[string]jsontext.Value, deps instances.Deps, _ storage.BuildInput) (storage.Storage, error) {
					return w.storage(deps.Instance), nil
				},
			})
		}); err != nil {
		return err
	}
	if err := loader.Add(lr, loader.Spec{ID: "source/synthetic", Summary: "a source the test drives"},
		func(lr *loader.Registry) error {
			return instances.AddFactory(lr, r, source.Kind, instances.FactorySpec[syntheticConfig, source.Source]{
				Type:      "synthetic",
				Summary:   "a source that dumps the bytes, claims the origin and restores into the directory its config names",
				Stability: instances.Experimental,
				Example:   []byte(`{"payload": "example", "dir": "", "origin": "test:example"}`),
				New: func(_ context.Context, cfg syntheticConfig, deps instances.Deps) (source.Source, error) {
					s := &syntheticSource{cfg: cfg, failCheck: w.checks[deps.Instance],
						credentials: w.credentialBytes[deps.Instance], verdict: w.credentialVerdicts[deps.Instance]}
					w.sources[deps.Instance] = s
					return s, nil
				},
			})
		}); err != nil {
		return err
	}
	if err := loader.Add(lr, loader.Spec{ID: "discovery/fakediscovery", Summary: "a discovery the test drives"},
		func(lr *loader.Registry) error {
			return instances.AddFactory(lr, r, discovery.Kind, instances.FactorySpec[map[string]jsontext.Value, discovery.Discoverer]{
				Type:      "fakediscovery",
				Summary:   "a discovery that returns the Found list the test set",
				Stability: instances.Experimental,
				Example:   []byte(`{}`),
				New: func(_ context.Context, _ map[string]jsontext.Value, deps instances.Deps) (discovery.Discoverer, error) {
					return &fakeDiscovery{found: w.found[deps.Instance]}, nil
				},
			})
		}); err != nil {
		return err
	}
	if err := loader.Add(lr, loader.Spec{ID: "catalog/memory", Summary: "an in-memory writer lease the test owns"},
		func(lr *loader.Registry) error {
			return instances.AddFactory(lr, r, catalog.Kind, instances.FactorySpec[map[string]jsontext.Value, catalog.Catalog]{
				Type:      "memory",
				Summary:   "an in-memory writer lease the test owns",
				Stability: instances.Experimental,
				Example:   []byte(`{}`),
				New: func(_ context.Context, _ map[string]jsontext.Value, _ instances.Deps) (catalog.Catalog, error) {
					if w.catalogErr != nil {
						return &failingLease{Leaser: w.catalog, err: w.catalogErr}, nil
					}
					return w.catalog, nil
				},
			})
		}); err != nil {
		return err
	}
	return loader.Add(lr, loader.Spec{ID: "notifier/memory", Summary: "a notifier that records every event"},
		func(lr *loader.Registry) error {
			return instances.AddFactory(lr, r, notifier.Kind, instances.FactorySpec[map[string]jsontext.Value, notifier.Notifier]{
				Type:      "memory",
				Summary:   "a notifier that records every event it was handed",
				Stability: instances.Experimental,
				Example:   []byte(`{}`),
				New: func(_ context.Context, _ map[string]jsontext.Value, _ instances.Deps) (notifier.Notifier, error) {
					return &memoryNotifier{err: w.notifierErr}, nil
				},
			})
		})
}

func (w *world) storage(name string) *storagetest.Fake {
	if w.storages[name] == nil {
		w.storages[name] = storagetest.NewFake()
	}
	return w.storages[name]
}

// command is one command's compiled graph and factory registry together: the
// distribution, the test doubles and the host roles, the same shape production
// builds. Registration of this fixed set cannot fail; a failure is the harness
// itself being wrong, not a command outcome.
func (w *world) command() (*loader.Registry, *instances.Registry) {
	lr := loader.New()
	r := instances.NewRegistry()
	if err := components.Compose(lr, r); err != nil {
		panic(err)
	}
	if err := w.registerDoubles(lr, r); err != nil {
		panic(err)
	}
	if err := AddHost(lr, Options{Program: "backup"}); err != nil {
		panic(err)
	}
	return lr, r
}

// factories is the command's factory registry alone, for tests that only need
// the types.
func (w *world) factories() *instances.Registry {
	_, r := w.command()
	return r
}

// loader is the command's compiled plugin graph alone, for tests that only need
// the graph.
func (w *world) loader() *loader.Registry {
	lr, _ := w.command()
	return lr
}

// main runs one command through the real command layer and captures its output.
func (w *world) main(ctx context.Context, args ...string) (int, string, string) {
	lr, factories := w.command()
	var stdout, stderr bytes.Buffer
	code := Main(ctx, Options{Program: "backup", Loader: lr, Factories: factories, Defaults: [][]byte{defaults.Defaults, []byte(defaultsYAML)}}, args, &stdout, &stderr)
	return code, stdout.String(), stderr.String()
}

// down fails every operation of one storage, unclassified: execution's retry
// policy decides what an unclassified error means, exactly as it would for a
// real outage.
func down(op storagetest.Op, key string, call int) error {
	return fmt.Errorf("test storage is down (%s %q call %d)", op, key, call)
}

// failingLease is the lease record with every acquisition failing: the failure
// path a test turns on to prove a lease error stays visible at a high log level.
type failingLease struct {
	*catalogtest.Leaser
	err error
}

func (c *failingLease) Acquire(context.Context, catalog.LeaseRequest) (catalog.Lease, error) {
	return catalog.Lease{}, c.err
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
	// credentials and verdict script the source's credential facts, the same
	// ones a real source type reports.
	credentials bool
	verdict     source.CredentialVerdict
	mu          sync.Mutex
	checks      int
}

func (s *syntheticSource) Describe(ctx context.Context) (source.SourceTraits, error) {
	if err := ctx.Err(); err != nil {
		return source.SourceTraits{}, err
	}
	return source.SourceTraits{Ext: ".dat", Origin: s.cfg.Origin, Credentials: s.credentials}, nil
}

// CredentialPolicy is the optional published verdict of this source type.
func (s *syntheticSource) CredentialPolicy(ctx context.Context) (source.CredentialVerdict, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return s.verdict, nil
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
	return fmt.Sprintf(`run:
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
  writer_identity: ""
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
// test that runs the assembly more than once does not hit the run-id collision the
// backup workflow refuses: two runs of one fixed id are the same run, and would overwrite
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
