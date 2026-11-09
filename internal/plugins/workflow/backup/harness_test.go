package backup_test

// The workflow tests' side of the shared assemblytest kit: the fake world, its
// fakes and the configuration builders live in internal/plugins/assemblytest; this
// file only binds them to the distribution and to this package's fixtures.

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/all"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/assembly"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/assemblytest"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage/storagetest"
)

type (
	fakeSource    = assemblytest.Source
	fakeProcessor = assemblytest.Processor
	fakeDiscovery = assemblytest.Discovery
	fakeNotifier  = assemblytest.Notifier
	chainSource   = assemblytest.ChainSource
)

// world is the kit world plus this package's construction entry points.
type world struct{ *assemblytest.World }

// newWorld builds the kit with the binary's whole composition root, which is
// the compiled set these tests need.
func newWorld(t *testing.T) *world {
	t.Helper()
	kit, err := assemblytest.NewWorld(all.Compose)
	if err != nil {
		t.Fatal(err)
	}
	return &world{kit}
}

func (w *world) newAssembly(t *testing.T, cfg map[string]any) *assembly.Assembly {
	t.Helper()
	w.syncLoader(t)
	raw, err := jsonMarshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	e, err := assembly.New(context.Background(), raw, nil,
		assembly.Deps{Factories: w.Registry, Loader: w.Loader, Log: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatalf("assembly.New: %v", err)
	}
	t.Cleanup(func() { _ = e.Close(context.Background()) })
	return e
}

func (w *world) newAssemblyErr(t *testing.T, cfg map[string]any) (*assembly.Assembly, error) {
	t.Helper()
	w.syncLoader(t)
	raw, err := jsonMarshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	e, err := assembly.New(context.Background(), raw, nil,
		assembly.Deps{Factories: w.Registry, Loader: w.Loader, Log: slog.New(slog.DiscardHandler)})
	if e != nil {
		t.Cleanup(func() { _ = e.Close(context.Background()) })
	}
	return e, err
}

func (w *world) storage(t *testing.T, name string) *storagetest.Fake {
	t.Helper()
	s, ok := w.Storage(name)
	if !ok {
		t.Fatalf("storage %q not built", name)
	}
	return s
}

func (w *world) syncLoader(t *testing.T) {
	t.Helper()
	w.SyncLoader()
}

// The shared pure helpers under the names this package's tests already use.
func testLogger() *slog.Logger                  { return assemblytest.Logger() }
func jsonMarshal(v any) (jsontext.Value, error) { return assemblytest.JSONMarshal(v) }
func jsonUnmarshal(b []byte, v any) error       { return assemblytest.JSONUnmarshal(b, v) }
func mustMarshal(t *testing.T, v any) []byte    { return assemblytest.MustMarshal(v) }

func readDirNames(dir string) ([]string, error) { return assemblytest.ReadDirNames(dir) }

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
