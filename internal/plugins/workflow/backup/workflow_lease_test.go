package backup_test

// The workflow-level lease proofs: what assembly.Assembly.Run does with a claim's state.
// The lease protocol's own internals are the protection plugin's tests; these
// drive the run, so a change in the run's skeleton is what they catch.

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/workflow/backup"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/catalog"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/catalog/catalogtest"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
)

// TestManualRefusesTheKubeletProof: a manual run never takes over a claim, even
// one whose holder's pod the kubelet recorded as terminated — a takeover is a
// scheduled run's decision.
func TestManualRefusesTheKubeletProof(t *testing.T) {
	w := newWorld(t)
	e := w.newAssembly(t, leaseConfig(t, "40ms"))
	w.Catalog.Hold("dead-run", model.RunKindScheduled, "infrastructure", "backup-abc", "uid-1", time.Now())
	w.Catalog.SetTerminated(true, true)
	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindManual})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Lease == nil || !rep.Lease.Refused {
		t.Fatalf("a manual run took over on the kubelet proof: %+v", rep.Lease)
	}
	if _, ok := w.Catalog.Held(); !ok {
		t.Fatal("the claim disappeared")
	}
}

// TestLeaseCapabilityAnsweredUnsupported: the lease capability is discovered by
// call. A catalog that answers errors.ErrUnsupported is no lease at all: a
// scheduled run proceeds lease-free, a manual run refuses to start.
func TestLeaseCapabilityAnsweredUnsupported(t *testing.T) {
	w := newWorld(t)
	err := instances.Register(w.Registry, catalog.Kind, instances.FactorySpec[map[string]jsontext.Value, catalog.Catalog]{
		Type: "nolease", Example: []byte(`{}`),
		New: func(_ context.Context, _ map[string]jsontext.Value, _ instances.Deps) (catalog.Catalog, error) {
			return &unsupportedLeaseCatalog{}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	cfg := manualConfig(t, map[string]any{"alpha": manualStorage(1 << 40)})
	cfg["catalog"] = instance("nolease")
	e := w.newAssembly(t, cfg)

	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatalf("a scheduled run must proceed without the capability: %v", err)
	}
	if rep.Outcome != model.OutcomeDelivered {
		t.Fatalf("scheduled outcome = %q, want delivered", rep.Outcome)
	}
	if _, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindManual}); err == nil {
		t.Fatal("a manual run started without a lease capability")
	}
}

// unsupportedLeaseCatalog is a catalog whose backend cannot hold a claim and
// answers errors.ErrUnsupported: a capability discovered by call, not by type.
type unsupportedLeaseCatalog struct{ catalogtest.Leaser }

func (c *unsupportedLeaseCatalog) Acquire(context.Context, catalog.LeaseRequest) (catalog.Lease, error) {
	return catalog.Lease{}, fmt.Errorf("this catalog holds no lease: %w", errors.ErrUnsupported)
}

// leaseConfig is a manual-capable config with a leased wait short enough to
// witness.
func leaseConfig(t *testing.T, wait string) map[string]any {
	t.Helper()
	cfg := manualConfig(t, map[string]any{"alpha": manualStorage(1 << 40)})
	run := cfg["run"].(map[string]any)
	run["lease_wait"] = wait
	run["lease_poll"] = "10ms"
	run["lease_hard_bound"] = "10ms"
	return cfg
}
