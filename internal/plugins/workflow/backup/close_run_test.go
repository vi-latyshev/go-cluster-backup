package backup_test

import (
	"context"
	"errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/catalog"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/processor"
	"io"
	"strings"
	"sync"
	"testing"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/discovery"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/workflow/backup"
)

func TestRunClosesTheSourcesItDiscovered(t *testing.T) {
	w := newWorld(t)
	discovered := &closingSource{}
	w.ExtraSources["discovered-db"] = discovered
	cfg := baseConfig(t, twoStorages())
	// Two runs in one test must not share a run id: the backup workflow refuses a
	// collision, and a nanosecond layout is what makes them distinct here.
	cfg["run"].(map[string]any)["id_layout"] = "20060102T150405.000000000"
	cfg["discoveries"] = map[string]any{"workloads": instance("fake")}
	e := serviceCheck(t, w, cfg)
	w.Discoveries["workloads"].Found = []discovery.Found{{
		Name:   "discovered-db",
		Type:   "fake",
		Config: jsontext.Value(`{}`),
		Artifact: model.Artifact{Group: "databases", Name: "discovered-db",
			Importance: model.ImportanceBestEffort, Sensitivity: model.SensitivityPlain},
		Pipeline: []string{"zstd3"},
	}}

	for i := range 2 {
		rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
		if err != nil {
			t.Fatalf("run %d: %v", i+1, err)
		}
		ok := artifactByName(rep, "discovered-db")
		if ok.Outcome != model.ArtifactOK {
			t.Fatalf("run %d: the discovered artifact did not run: %+v", i+1, ok)
		}
	}
	// The run's discovered sources belong to the invocation's one scope: the
	// operation does not close them, the role's Close does. Each run builds the
	// instance the fake factory answers with, so the two runs own two entries —
	// the fixture returns the same pointer — and one invocation Close runs both.
	if got := discovered.closeCount(); got != 0 {
		t.Fatalf("the operation closed a run-scoped source %d times", got)
	}
	if err := e.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := discovered.closeCount(); got != 2 {
		t.Fatalf("the invocation Close ran %d times, want one per run-built instance", got)
	}
	if err := e.Close(context.Background()); err != nil {
		t.Fatalf("a second Close: %v", err)
	}
	if got := discovered.closeCount(); got != 2 {
		t.Fatalf("a second Close closed the source again (%d)", got)
	}
}

// TestRunClosesADiscoveredSourceWhoseCheckFails: a discovered source is built
// and belongs to the run even when its Check fails — the run closes it exactly
// once, and the assembly.Assembly's lifetime list never sees it.
func TestRunClosesADiscoveredSourceWhoseCheckFails(t *testing.T) {
	w := newWorld(t)
	broken := &closingSource{}
	broken.CheckErr = errors.New("api refused")
	w.ExtraSources["broken-db"] = broken
	cfg := baseConfig(t, twoStorages())
	cfg["discoveries"] = map[string]any{"workloads": instance("fake")}
	e := serviceCheck(t, w, cfg)
	w.Discoveries["workloads"].Found = []discovery.Found{foundSource("broken-db", "broken-db", model.ImportanceBestEffort)}

	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if got := artifactByName(rep, "broken-db"); got.Outcome != model.ArtifactFailed || !strings.Contains(got.Error, "api refused") {
		t.Fatalf("the failed artifact: %+v", got)
	}
	if got := broken.closeCount(); got != 0 {
		t.Fatalf("the operation closed a failed-check source %d times", got)
	}
	if err := e.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := broken.closeCount(); got != 1 {
		t.Fatalf("the invocation Close ran %d times for a failed-check source, want exactly 1", got)
	}
}

// failingCloseSource is a discovered source whose Close reports a failure: the
// run's scoped close has to surface it without failing the run itself.
type failingCloseSource struct {
	fakeSource
	closeErr error
	mu       sync.Mutex
	closes   int
}

func (f *failingCloseSource) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closes++
	return f.closeErr
}

// errDescribeProcessor cannot describe itself: the artifact's chain cannot be
// built, so the artifact fails before any byte.
type errDescribeProcessor struct{}

func (p *errDescribeProcessor) Describe(context.Context) (processor.ProcessorTraits, error) {
	return processor.ProcessorTraits{}, errors.New("processor describe failed")
}

func (p *errDescribeProcessor) Encode(ctx context.Context, dst io.Writer, src io.Reader) error {
	_, err := io.Copy(dst, src)
	return err
}

func (p *errDescribeProcessor) Decode(ctx context.Context, dst io.Writer, src io.Reader) error {
	_, err := io.Copy(dst, src)
	return err
}

// registerProcessorType registers one processor type whose New returns the given
// processor, so a test can hand the backup workflow a processor of its own shape.
func registerProcessorType(t *testing.T, w *world, typ string, p processor.Processor) {
	t.Helper()
	if err := instances.Register(w.Registry, processor.Kind, instances.FactorySpec[map[string]jsontext.Value, processor.Processor]{
		Type: typ, Example: []byte(`{}`),
		New: func(context.Context, map[string]jsontext.Value, instances.Deps) (processor.Processor, error) {
			return p, nil
		},
	}); err != nil {
		t.Fatal(err)
	}
}

// registerCatalogType registers one catalog type whose New returns c.
func registerCatalogType(t *testing.T, w *world, typ string, c catalog.Catalog) {
	t.Helper()
	if err := instances.Register(w.Registry, catalog.Kind, instances.FactorySpec[map[string]jsontext.Value, catalog.Catalog]{
		Type: typ, Example: []byte(`{}`),
		New: func(context.Context, map[string]jsontext.Value, instances.Deps) (catalog.Catalog, error) {
			return c, nil
		},
	}); err != nil {
		t.Fatal(err)
	}
}

// TestProduceFailsWhenTheSpoolDirectoryVanishes: a spool directory that is gone
// or replaced by a file fails the artifact that needed it, permanently and by
// name — the run reports it instead of writing somewhere else or retrying a
// condition that cannot change. The storage's Fault hook does the replacing on
// the run's first survey, the window between the spool's creation and the first
// artifact.
