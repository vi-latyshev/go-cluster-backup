package cli

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/model"
)

// manualStorageYAML is the pair of storages a manual run needs: each accepts
// both run kinds, with the configured cap on its envelope.
func manualStorageYAML(root string) string {
	return fmt.Sprintf(`storages:
  exchange:
    type: local
    retention:
      scheduled: {type: keep_last, config: {count: 3, min_keep: 1, count_incomplete: true}}
      manual: {type: keep_last, config: {count: 5, min_keep: 1, count_incomplete: true}}
    max_total_bytes: 1048576
    time_budget: 10s
    config: {root: %q, page_size: 100}
  offsite:
    type: faulty
    retention:
      scheduled: {type: keep_last, config: {count: 3, min_keep: 1, count_incomplete: true}}
      manual: {type: keep_last, config: {count: 5, min_keep: 1, count_incomplete: true}}
    max_total_bytes: 1048576
    time_budget: 10s
    config: {}
`, root)
}

// TestRunRequiresKind: the run's identity has no default — a run without --kind
// is a usage error naming the flag, before anything is read.
func TestRunRequiresKind(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	root := filepath.Join(dir, "exchange")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	cfg := mustWrite(t, dir, "config.yaml",
		configYAML(dir, plainSourceYAML("hi")+zstdYAML+"catalog:\n  type: memory\n  config: {}\n"+manualStorageYAML(root)))
	code, _, stderr := w.main(context.Background(), "run", "--config", cfg)
	if code != exitUsage {
		t.Fatalf("exit %d, want %d: %s", code, exitUsage, stderr)
	}
	if !strings.Contains(stderr, "--kind") {
		t.Fatalf("the refusal does not name --kind: %s", stderr)
	}
	code, _, stderr = w.main(context.Background(), "run", "--config", cfg, "--kind", "nightly")
	if code != exitUsage {
		t.Fatalf("an unknown kind: exit %d, want %d: %s", code, exitUsage, stderr)
	}
}

// TestManualRunRoundTripShowsKind: a manual run writes under a suffixed id and
// `restore list` names it as manual, so an operator finds it the way a restore
// needs to.
func TestManualRunRoundTripShowsKind(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	root := filepath.Join(dir, "exchange")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	cfg := mustWrite(t, dir, "config.yaml",
		configYAML(dir, plainSourceYAML("hi")+zstdYAML+"catalog:\n  type: memory\n  config: {}\n"+manualStorageYAML(root)))

	code, stdout, stderr := w.main(context.Background(), "run", "--config", cfg, "--kind", "manual")
	if code != 0 {
		t.Fatalf("manual run: exit %d\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}
	if !strings.Contains(stdout, "manual run fixedrun-manual") {
		t.Fatalf("the summary does not name the manual run:\n%s", stdout)
	}

	code, stdout, stderr = w.main(context.Background(), "restore", "list", "--config", cfg)
	if code != 0 {
		t.Fatalf("restore list: exit %d\n%s", code, stderr)
	}
	if !strings.Contains(stdout, "run fixedrun-manual storage=exchange kind=manual manifest=yes") {
		t.Fatalf("restore list does not show the manual run and its kind:\n%s", stdout)
	}
}

// TestDeadHolderRecoveredWithDefaults (M-default-wait): with the service's own
// defaults — no lease_wait stated in the file — a scheduled run recovers from a
// holder that is PROVEN terminated, at once.
func TestDeadHolderRecoveredWithDefaults(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	root := filepath.Join(dir, "exchange")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	cfg := mustWrite(t, dir, "config.yaml",
		configYAML(dir, plainSourceYAML("hi")+zstdYAML+"catalog:\n  type: memory\n  config: {}\n"+manualStorageYAML(root)))
	uniqueRunIDs(t, cfg) // this test runs the engine twice
	code, _, stderr := w.main(context.Background(), "run", "--config", cfg, "--kind", "scheduled")
	if code != 0 {
		t.Fatalf("a first run failed: %d\n%s", code, stderr)
	}
	// The next night finds a claim whose holder is gone.
	w.catalog.Hold("killed-night", model.RunKindScheduled, "infrastructure", "backup-old", "uid-old", time.Now())
	w.catalog.SetTerminated(true, true) // the kubelet recorded the terminal phase
	acquiresBefore := w.catalog.AcquireCount()
	code, stdout, stderr := w.main(context.Background(), "run", "--config", cfg, "--kind", "scheduled")
	if code != 0 {
		t.Fatalf("the night did not recover from a dead holder with effective defaults: %d\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}
	// The claim the test arranged was seen and taken over; a run through a free
	// lease would not have observed it at all.
	if w.catalog.AcquireCount() < acquiresBefore+2 {
		t.Fatalf("the held claim was not observed and taken over: %d acquires", w.catalog.AcquireCount()-acquiresBefore)
	}

}
