package backup_test

// The SIGTERM sequence at the workflow level: one cleanup deadline, join before
// release, own-upload aborts after release, and no release without a join.

import (
	"context"
	"errors"
	"io"

	"encoding/json/jsontext"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/catalog"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/notifier"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage/storagetest"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/workflow/backup"
)

// withLifecycle gives the config the cleanup budgets the chart's grace is
// derived from, and a finishing allowance short enough to stay virtual.
func withLifecycle(cfg map[string]any) map[string]any {
	cfg["plugins"] = map[string]any{"bundles": []any{}, "disabled": []any{}, "config": map[string]any{
		"lifecycle": map[string]any{"join": "1ms", "abort_total": "1ms", "close": "1ms", "margin": "1ms"},
	}}
	cfg["run"].(map[string]any)["finish_allowance"] = "1ms"
	return cfg
}

// heldPut is a whole-object storage whose Put waits for cancellation: the run's
// request goroutine is still in flight when SIGTERM arrives.
type heldPut struct {
	*storagetest.Fake
	entered chan struct{}
}

func (h *heldPut) Put(ctx context.Context, req storage.PutRequest, body io.Reader) (model.ObjectInfo, error) {
	select {
	case h.entered <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return model.ObjectInfo{}, ctx.Err()
}

func registerHeldPut(t *testing.T, w *world, typ string) *heldPut {
	t.Helper()
	h := &heldPut{Fake: storagetest.NewFake(), entered: make(chan struct{}, 1)}
	if err := storage.Register(w.Registry, storage.FactorySpec[map[string]jsontext.Value]{
		Type: typ, Example: []byte(`{}`),
		New: func(context.Context, map[string]jsontext.Value, instances.Deps, storage.BuildInput) (storage.Storage, error) {
			return h, nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	return h
}

func serverConfig(t *testing.T, storages map[string]any) map[string]any {
	cfg := baseConfig(t, storages)
	cfg["sources"] = map[string]any{"vpn": sourceInstance("fake", "vpn", "wireguard", nil, "required", "plain")}
	return withLifecycle(cfg)
}

// TestSigtermJoinsBeforeReleasingAndAbortsAfter: the run's own request goroutine
// has returned before the claim is released; the successor can take the claim
// while this run's abort is still blocked.
func TestSigtermJoinsBeforeReleasingAndAbortsAfter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newWorld(t)
		parts := addMultipart(w, "alpha", 4)
		parts.partErr = errors.New("part refused")
		parts.abortErr = errors.New("abort refused")
		parts.abortGate = make(chan struct{})
		parts.abortEntered = make(chan struct{}, 1)
		held := registerHeldPut(t, w, "held")
		w.Sources["vpn"] = &fakeSource{Data: []byte("0123456789")}
		cfg := serverConfig(t, map[string]any{"alpha": storageInstance("fake", "keep_last"), "beta": storageInstance("held", "keep_last")})
		cfg["catalog"] = instance("fake")
		e := serviceCheck(t, w, cfg)

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			_, err := e.Run(ctx, backup.RunRequest{Kind: model.RunKindScheduled})
			done <- err
		}()
		select {
		case <-held.entered:
		case <-time.After(5 * time.Second):
			t.Fatal("the run never reached the held request")
		}
		cancel()
		select {
		case <-parts.abortEntered:
		case <-time.After(5 * time.Second):
			t.Fatal("the cancel sequence never reached the own-upload abort")
		}
		// Release happened before the blocked abort: the successor takes the claim.
		if _, err := w.Catalog.Acquire(context.Background(), catalog.LeaseRequest{Holder: "successor", Kind: model.RunKindScheduled}); err != nil {
			t.Fatalf("the successor could not acquire while the abort was blocked: %v", err)
		}
		close(parts.abortGate)
		if err := <-done; err != nil {
			t.Fatalf("cancelled run returned %v", err)
		}
	})
}

// TestSigtermAbortIsBoundedByTheSharedDeadline: an abort that outlasts the one
// cleanup deadline ends the sequence; the claim is already released.
func TestSigtermAbortIsBoundedByTheSharedDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newWorld(t)
		parts := addMultipart(w, "alpha", 4)
		parts.partErr = errors.New("part refused")
		parts.abortErr = errors.New("abort refused")
		parts.abortGate = make(chan struct{})
		parts.abortEntered = make(chan struct{}, 1)
		held := registerHeldPut(t, w, "held")
		w.Sources["vpn"] = &fakeSource{Data: []byte("0123456789")}
		cfg := serverConfig(t, map[string]any{"alpha": storageInstance("fake", "keep_last"), "beta": storageInstance("held", "keep_last")})
		cfg["catalog"] = instance("fake")
		e := serviceCheck(t, w, cfg)

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			_, err := e.Run(ctx, backup.RunRequest{Kind: model.RunKindScheduled})
			done <- err
		}()
		select {
		case <-held.entered:
		case <-time.After(5 * time.Second):
			t.Fatal("the run never reached the held request")
		}
		start := time.Now()
		cancel()
		select {
		case <-parts.abortEntered:
		case <-time.After(5 * time.Second):
			t.Fatal("the cancel sequence never reached the own-upload abort")
		}
		// The shared cleanup deadline ends the blocked abort; no gate release.
		if err := <-done; err != nil {
			t.Fatalf("a bounded abort turned cancellation into a run failure: %v", err)
		}
		if elapsed := time.Since(start); elapsed < time.Second {
			t.Fatalf("the abort ended after %s, before the shared deadline", elapsed)
		}
		if _, err := w.Catalog.Acquire(context.Background(), catalog.LeaseRequest{Holder: "successor", Kind: model.RunKindScheduled}); err != nil {
			t.Fatalf("the claim was not released before the bounded abort: %v", err)
		}
	})
}

// TestIncompleteJoinNeverReleases: a request goroutine that ignores cancellation
// outlives the join deadline, so the claim stays held and conservative.
func TestIncompleteJoinNeverReleases(t *testing.T) {
	w := newWorld(t)
	parts := addMultipart(w, "alpha", 4)
	parts.beginStuck = make(chan struct{})
	w.Sources["vpn"] = &fakeSource{Data: []byte("0123456789")}
	cfg := serverConfig(t, map[string]any{"alpha": storageInstance("fake", "keep_last")})
	cfg["catalog"] = instance("fake")
	e := serviceCheck(t, w, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := e.Run(ctx, backup.RunRequest{Kind: model.RunKindScheduled})
		done <- err
	}()
	for begins := 0; begins == 0; {
		begins, _, _, _ = parts.stats()
		if begins > 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("an incomplete join returned no error")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the cancel sequence did not stop at the join")
	}
	if _, held := w.Catalog.Held(); !held {
		t.Fatal("a claim was released although the join never completed")
	}
	close(parts.beginStuck)
	time.Sleep(20 * time.Millisecond)
}

// lateCtx reports cancellation only after the operation finished: the
// supervisor's completed-run arm still runs the cancel sequence, so a run that
// finished just as SIGTERM arrived releases, aborts and closes.
type lateCtx struct {
	context.Context
	late atomic.Bool
}

func (c *lateCtx) Done() <-chan struct{} { return nil }

func (c *lateCtx) Err() error {
	if c.late.Load() {
		return context.Canceled
	}
	return nil
}

// lateNotifier flips the late cancellation when the finished event lands.
type lateNotifier struct{ late *atomic.Bool }

func (n lateNotifier) Check(context.Context) error { return nil }

func (n lateNotifier) Notify(_ context.Context, ev model.Event) error {
	if ev.Name == model.EventFinished {
		n.late.Store(true)
	}
	return nil
}

func TestFinishedAsSigtermArrivesStillRunsTheSequence(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newWorld(t)
		ctx := &lateCtx{Context: context.Background()}
		if err := instances.Register(w.Registry, notifier.Kind, instances.FactorySpec[map[string]jsontext.Value, notifier.Notifier]{
			Type: "late", Example: []byte(`{}`),
			New: func(context.Context, map[string]jsontext.Value, instances.Deps) (notifier.Notifier, error) {
				return lateNotifier{late: &ctx.late}, nil
			},
		}); err != nil {
			t.Fatal(err)
		}
		cfg := serverConfig(t, map[string]any{"alpha": storageInstance("fake", "keep_last")})
		cfg["catalog"] = instance("fake")
		cfg["notifiers"] = map[string]any{"ops": instance("late")}
		e := w.newAssembly(t, cfg)

		rep, err := e.Run(ctx, backup.RunRequest{Kind: model.RunKindScheduled})
		if err != nil {
			t.Fatal(err)
		}
		if rep.Outcome != model.OutcomeDelivered {
			t.Fatalf("outcome %q: %+v", rep.Outcome, rep.Storages)
		}
		if !ctx.late.Load() {
			t.Fatal("the finished notifier never ran")
		}
	})
}
