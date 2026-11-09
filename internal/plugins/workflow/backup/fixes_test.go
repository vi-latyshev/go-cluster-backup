package backup_test

// Iteration-1 fixes at the workflow level: C1 (a live synchronous phase is not
// a complete operation) and M8 (refused finished events still carry the
// previous-run record).

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/catalog"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/workflow/backup"
)

func TestC1_SynchronousPhaseIsNeverACompleteJoin(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newWorld(t)
		parts := addMultipart(w, "alpha", 4)
		parts.sweepHold = make(chan struct{})
		parts.sweepEntered = make(chan struct{}, 1)
		cfg := serverConfig(t, map[string]any{"alpha": storageInstance("fake", "keep_last")})
		cfg["catalog"] = instance("fake")
		e := serviceCheck(t, w, cfg)

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			_, err := e.Run(ctx, backup.RunRequest{Kind: model.RunKindScheduled})
			done <- err
		}()
		select {
		case <-parts.sweepEntered:
		case <-time.After(5 * time.Second):
			t.Fatal("the run never reached the synchronous sweep")
		}
		cancel()
		// The operation is still inside its synchronous sweep: the claim must stay
		// held. A completed counter would release it here.
		time.Sleep(50 * time.Millisecond)
		if _, err := w.Catalog.Acquire(context.Background(), catalog.LeaseRequest{Holder: "successor", Kind: model.RunKindScheduled}); err == nil {
			t.Fatal("the claim was released while the operation was still running")
		}
		close(parts.sweepHold)
		if err := <-done; err != nil {
			t.Fatalf("cancelled run returned %v", err)
		}
		if _, err := w.Catalog.Acquire(context.Background(), catalog.LeaseRequest{Holder: "successor", Kind: model.RunKindScheduled}); err != nil {
			t.Fatalf("the finished operation never released the claim: %v", err)
		}
	})
}

func TestM8_RefusedFinishedEventCarriesPreviousRecord(t *testing.T) {
	// A pod identity makes the reporting block attempt the previous-Job lookup;
	// the API host is unset, so the failure is what the record must carry.
	t.Setenv("GCB_PREV_NAMESPACE", "backup")
	t.Setenv("GCB_PREV_POD", "backup-other")
	w := newWorld(t)
	n := &fakeNotifier{}
	w.Notifiers["ops"] = n
	cfg := serverConfig(t, map[string]any{"alpha": manualStorage(1 << 40)})
	cfg["catalog"] = instance("fake")
	cfg["notifiers"] = map[string]any{"ops": instance("fake")}
	cfg["plugins"] = map[string]any{"bundles": []any{}, "disabled": []any{}, "config": map[string]any{
		"lifecycle": map[string]any{"join": "1m", "abort_total": "2m", "close": "30s", "margin": "30s"},
		"reporting": map[string]any{
			"api_host_env": "GCB_PREV_HOST", "api_port_env": "GCB_PREV_PORT",
			"token_file": "/var/run/secrets/token", "ca_file": "/var/run/secrets/ca.crt",
			"request_timeout": "30s", "namespace_env": "GCB_PREV_NAMESPACE", "pod_env": "GCB_PREV_POD",
		},
	}}
	e := w.newAssembly(t, cfg)
	w.Catalog.Hold("other-run", model.RunKindScheduled, "infrastructure", "backup-other", "uid-other", time.Now())
	w.Catalog.SetTerminated(true, true)

	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindManual})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Lease == nil || !rep.Lease.Refused {
		t.Fatalf("the manual run was not refused on the live claim: %+v", rep.Lease)
	}
	var finished *model.Event
	for i := range n.Events {
		if n.Events[i].Name == model.EventFinished {
			finished = &n.Events[i]
		}
	}
	if finished == nil {
		t.Fatal("the refused run sent no finished event")
	}
	if finished.Run.Previous.Unavailable == "" {
		t.Fatalf("the refused finished event carries no previous record: %+v", finished.Run.Previous)
	}
	if keys := w.storage(t, "alpha").Keys(); len(keys) != 0 {
		t.Fatalf("a refused run surveyed storage: %v", keys)
	}
}
