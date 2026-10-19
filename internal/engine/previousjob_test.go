package engine

import (
	"context"
	"errors"
	"testing"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/catalog"
	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
)

// inspectCatalog is a catalog with the optional PreviousRunInspector capability:
// what the engine must read for the previous run's failure, and never act on.
type inspectCatalog struct {
	fakeCatalog
	failure *model.PreviousFailure
	err     error
	calls   int
	ns, pod string
}

func (c *inspectCatalog) PreviousJobFailure(_ context.Context, namespace, pod string) (*model.PreviousFailure, error) {
	c.calls++
	c.ns, c.pod = namespace, pod
	return c.failure, c.err
}

// inspectWorld registers the inspecting catalog and points the run's identity
// environment variables at a pod.
func inspectWorld(t *testing.T, insp *inspectCatalog) (*world, *Engine) {
	t.Helper()
	w := newWorld(t)
	if err := plugin.Register(w.registry, catalog.Kind, plugin.Spec[map[string]jsontext.Value, catalog.Catalog]{
		Type: "inspect", Example: []byte(`{}`),
		New: func(_ context.Context, _ map[string]jsontext.Value, _ plugin.Deps) (catalog.Catalog, error) {
			return insp, nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	cfg := manualConfig(t, map[string]any{"alpha": manualStorage(1 << 40)})
	cfg["catalog"] = instance("inspect")
	run := cfg["run"].(map[string]any)
	run["holder_namespace_env"], run["holder_pod_env"] = "TEST_NS", "TEST_POD_NAME"
	t.Setenv("TEST_NS", "infrastructure")
	t.Setenv("TEST_POD_NAME", "backup-now")
	return w, w.newEngine(t, cfg)
}

// TestPreviousJobFailureIsReported: the engine reads the previous Job's failure
// through the catalog's capability, with the run's own pod identity, and carries
// it in the report.
func TestPreviousJobFailureIsReported(t *testing.T) {
	insp := &inspectCatalog{failure: &model.PreviousFailure{Job: "backup-prev-job", Reason: "BackoffLimitExceeded"}}
	_, e := inspectWorld(t, insp)
	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if insp.ns != "infrastructure" || insp.pod != "backup-now" {
		t.Fatalf("the inspector was not handed the run's own identity: %q/%q", insp.ns, insp.pod)
	}
	if rep.Previous.Failure == nil || rep.Previous.Failure.Job != "backup-prev-job" {
		t.Fatalf("the previous Job's failure is not in the report: %+v", rep.Previous)
	}
	if rep.Outcome != model.OutcomeDelivered {
		t.Fatalf("the previous run's failure changed this run's outcome: %q", rep.Outcome)
	}
}

// TestPreviousJobUnreadableDegradesButNeverFails: a missing grant is visible as
// "previous run status unavailable" and the run still delivers.
func TestPreviousJobUnreadableDegradesButNeverFails(t *testing.T) {
	insp := &inspectCatalog{err: errors.New("jobs is forbidden")}
	_, e := inspectWorld(t, insp)
	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Previous.Unavailable == "" {
		t.Fatal("an unreadable previous Job was not surfaced")
	}
	if rep.Outcome != model.OutcomeDelivered {
		t.Fatalf("an unreadable previous Job changed the run's outcome: %q", rep.Outcome)
	}
}
