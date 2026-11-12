package assembly

// The assembly tests' side of the shared assemblytest kit: the fake world, its
// fakes and the configuration builders live in internal/plugins/assemblytest; this
// file only binds them to this package's construction entry point.

import (
	"context"
	"log/slog"
	"testing"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/assemblytest"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/retention/gfs"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/retention/keeplast"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage/storagetest"
)

type (
	fakeSource       = assemblytest.Source
	fakeProcessor    = assemblytest.Processor
	fakeDiscovery    = assemblytest.Discovery
	fakeNotifier     = assemblytest.Notifier
	fakeCatalog      = assemblytest.Catalog
	chainSource      = assemblytest.ChainSource
	credentialSource = assemblytest.CredentialSource
)

// world is the kit world plus this package's construction entry points.
type world struct{ *assemblytest.World }

// newWorld builds the kit with the distribution's retention factories, which is
// the compiled set these tests need.
func newWorld(t *testing.T) *world {
	t.Helper()
	kit, err := assemblytest.NewWorld(func(lr *loader.Registry, reg *instances.Registry) error {
		if err := loader.Add(lr, gfs.Spec, func(lr *loader.Registry) error { return gfs.Apply(lr, reg) }); err != nil {
			return err
		}
		return loader.Add(lr, keeplast.Spec, func(lr *loader.Registry) error { return keeplast.Apply(lr, reg) })
	})
	if err != nil {
		t.Fatal(err)
	}
	return &world{kit}
}

func (w *world) newAssembly(t *testing.T, cfg map[string]any) *Assembly {
	t.Helper()
	w.syncLoader(t)
	raw, err := jsonMarshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	e, err := New(context.Background(), raw, nil, Deps{Factories: w.Registry, Loader: w.Loader, Log: testLogger()})
	if err != nil {
		t.Fatalf("assembly.New: %v", err)
	}
	t.Cleanup(func() { _ = e.Close(context.Background()) })
	return e
}

func (w *world) newAssemblyErr(t *testing.T, cfg map[string]any) (*Assembly, error) {
	t.Helper()
	w.syncLoader(t)
	raw, err := jsonMarshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	e, err := New(context.Background(), raw, nil, Deps{Factories: w.Registry, Loader: w.Loader, Log: testLogger()})
	if e != nil {
		t.Cleanup(func() { _ = e.Close(context.Background()) })
	}
	return e, err
}

func (w *world) storage(t *testing.T, name string) *storagetest.Fake {
	t.Helper()
	s, ok := w.World.Storage(name)
	if !ok {
		t.Fatalf("storage %q not built", name)
	}
	return s
}

func (w *world) syncLoader(t *testing.T) {
	t.Helper()
	w.World.SyncLoader()
}

// The shared pure helpers under the names the assembly tests already use.
func testLogger() *slog.Logger                  { return assemblytest.Logger() }
func jsonMarshal(v any) (jsontext.Value, error) { return assemblytest.JSONMarshal(v) }
func jsonUnmarshal(b []byte, v any) error       { return assemblytest.JSONUnmarshal(b, v) }
func mustMarshal(t *testing.T, v any) []byte    { return assemblytest.MustMarshal(v) }

func readDirNames(dir string) ([]string, error) { return assemblytest.ReadDirNames(dir) }

func configRun(spoolDir string) map[string]any { return assemblytest.ConfigRun(spoolDir) }

func configLayout() map[string]any { return assemblytest.ConfigLayout() }

func instance(typ string) map[string]any { return assemblytest.Instance(typ) }

func sourceInstance(typ, group, name string, pipeline []string, importance, sensitivity string) map[string]any {
	return assemblytest.SourceInstance(typ, group, name, pipeline, importance, sensitivity)
}

func storageInstance(typ, retentionType string) map[string]any {
	return assemblytest.StorageInstance(typ, retentionType)
}

func keepLastCfg(count int) map[string]any { return assemblytest.KeepLastCfg(count) }

func baseConfig(t *testing.T, storages map[string]any) map[string]any {
	t.Helper()
	return assemblytest.BaseConfig(t, storages)
}

func withRetention(cfg map[string]any, name string, retention map[string]any) map[string]any {
	return assemblytest.WithRetention(cfg, name, retention)
}

func withManualRetention(cfg map[string]any, name string, retention map[string]any) map[string]any {
	return assemblytest.WithManualRetention(cfg, name, retention)
}
