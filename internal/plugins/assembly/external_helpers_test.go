package assembly_test

import (
	"context"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/assembly"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/workflow/backup"
	"testing"
	"time"
)

func testTime() time.Time { return time.Now().UTC() }

func withRestore(t *testing.T, cfg map[string]any, order ...string) map[string]any {
	t.Helper()
	cfg["restore"] = map[string]any{
		"spool_dir":           t.TempDir(),
		"spool_reserve_bytes": 0,
		"storage_order":       order,
		"unclassified_error":  "transient",
		"retry":               map[string]any{"attempts": 3, "initial_delay": "1ms", "max_delay": "5ms", "jitter": 0},
	}
	return cfg
}

// twoStorages is one primary and one offsite fake storage.
func twoStorages() map[string]any {
	return map[string]any{
		"primary": assembly.StorageInstance("fake", "keep_last"),
		"offsite": assembly.StorageInstance("fake", "keep_last"),
	}
}

// runOnce performs a delivered run so the storages hold a real manifest.
func runOnce(t *testing.T, w *assembly.World, cfg map[string]any) *assembly.Assembly {
	t.Helper()
	e := w.NewAssembly(t, cfg)
	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeDelivered {
		t.Fatalf("seed run outcome = %q: %+v", rep.Outcome, rep.Storages)
	}
	return e
}
