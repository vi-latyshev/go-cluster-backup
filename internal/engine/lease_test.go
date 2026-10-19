package engine

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/catalog"
	"github.com/vi-latyshev/go-cluster-backup/internal/catalog/catalogtest"
	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
)

// leaseConfig is a manual-capable config with a short wait, poll and hard bound,
// so a test exercises the refusal and takeover paths without sitting out the
// deployment's fifteen-minute wait and 8400-second bound.
func leaseConfig(t *testing.T, wait string) map[string]any {
	t.Helper()
	cfg := manualConfig(t, map[string]any{"alpha": manualStorage(1 << 40)})
	run := cfg["run"].(map[string]any)
	run["lease_wait"] = wait
	run["lease_poll"] = "10ms"
	run["lease_hard_bound"] = "10ms"
	return cfg
}

// TestScheduledTakesOverOnTheKubeletProof: the kubelet's own terminal phase of
// the holder's exact pod is proof at once — the claim is young, the hard bound
// is not needed.
func TestScheduledTakesOverOnTheKubeletProof(t *testing.T) {
	w := newWorld(t)
	e := w.newEngine(t, leaseConfig(t, "20ms"))
	w.catalog.Hold("dead-run", model.RunKindScheduled, "infrastructure", "backup-abc", "uid-1", time.Now())
	w.catalog.SetTerminated(true, true)
	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatalf("the scheduled run did not take over a proven-terminated holder: %v", err)
	}
	if rep.Outcome == model.OutcomeFailed {
		t.Fatalf("the scheduled run failed: %+v", rep.Storages)
	}
	if w.catalog.AcquireCount() < 2 {
		t.Fatalf("no takeover attempt was made: %d acquires", w.catalog.AcquireCount())
	}
}

// TestAbsentPodTakesOverOnlyPastTheHardBound: an absent pod (or a different UID)
// proves nothing until the claim is older than the longest a writer can live; a
// young one counts as alive for both kinds.
func TestAbsentPodTakesOverOnlyPastTheHardBound(t *testing.T) {
	for _, kind := range []model.RunKind{model.RunKindManual, model.RunKindScheduled} {
		t.Run(string(kind), func(t *testing.T) {
			w := newWorld(t)
			cfg := leaseConfig(t, "40ms")
			cfg["run"].(map[string]any)["lease_hard_bound"] = "1h"
			e := w.newEngine(t, cfg)
			// Young: the object vanished but the process may still be running.
			w.catalog.Hold("dead-run", model.RunKindManual, "infrastructure", "backup-abc", "uid-1", time.Now())
			w.catalog.SetTerminated(true, false)
			rep, err := e.Run(context.Background(), RunRequest{Kind: kind})
			if err != nil {
				t.Fatal(err)
			}
			if kind == model.RunKindManual {
				if rep.Lease == nil || !rep.Lease.Refused {
					t.Fatalf("a manual run did not refuse a young absent holder: %+v", rep.Lease)
				}
			} else if rep.Outcome != model.OutcomeFailed || rep.Lease == nil || rep.Lease.Error == "" {
				t.Fatalf("a scheduled run did not fail on a young absent holder: outcome=%q lease=%+v", rep.Outcome, rep.Lease)
			}
			// Old enough: the hard bound passed, so the process cannot be alive.
			w.catalog.SetTerminated(true, false)
			w.catalog.Hold("dead-run", model.RunKindManual, "infrastructure", "backup-abc", "uid-1", time.Now().Add(-2*time.Hour))
			rep, err = e.Run(context.Background(), RunRequest{Kind: kind})
			if err != nil {
				t.Fatalf("the run did not take over a claim past the hard bound: %v", err)
			}
			if rep.Outcome == model.OutcomeFailed {
				t.Fatalf("the run failed after a hard-bound takeover: %+v", rep.Storages)
			}
		})
	}
}

// TestManualRefusesTheKubeletProof: only a scheduled run acts on the terminal
// phase; a manual run waits for the object to be conclusively gone (the hard
// bound) or the claim to be free.
func TestManualRefusesTheKubeletProof(t *testing.T) {
	w := newWorld(t)
	e := w.newEngine(t, leaseConfig(t, "40ms"))
	w.catalog.Hold("dead-run", model.RunKindScheduled, "infrastructure", "backup-abc", "uid-1", time.Now())
	w.catalog.SetTerminated(true, true)
	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindManual})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Lease == nil || !rep.Lease.Refused {
		t.Fatalf("a manual run took over on the kubelet proof: %+v", rep.Lease)
	}
	if _, ok := w.catalog.Held(); !ok {
		t.Fatal("the claim disappeared")
	}
}

// runScheduledFailed runs a scheduled run that cannot take the claim and
// requires the common finish: no error (the report carries the outcome), a
// failed outcome, and the lease reason in the report.
func runScheduledFailed(t *testing.T, e *Engine) model.RunReport {
	t.Helper()
	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatalf("a scheduled run that could not take the claim returned an error instead of a report: %v", err)
	}
	if rep.Outcome != model.OutcomeFailed || rep.Lease == nil || rep.Lease.Error == "" {
		t.Fatalf("a scheduled run that could not take the claim did not fail with the lease reason: outcome=%q lease=%+v", rep.Outcome, rep.Lease)
	}
	return rep
}

// TestLiveSilentHolderIsNeverTakenOver: a holder that is alive but says nothing —
// paused, blocked, in a long request — keeps the claim. A manual run refuses at
// once; a scheduled run waits its bound and fails loudly.
func TestLiveSilentHolderIsNeverTakenOver(t *testing.T) {
	w := newWorld(t)
	e := w.newEngine(t, leaseConfig(t, "50ms"))
	w.catalog.Hold("live-run", model.RunKindManual, "infrastructure", "backup-abc", "uid-1", time.Now().Add(-time.Hour))
	w.catalog.SetTerminated(false, false)

	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindManual})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeFailed || rep.Lease == nil || !rep.Lease.Refused {
		t.Fatalf("a manual run did not refuse a live claim: %+v %+v", rep.Outcome, rep.Lease)
	}
	runScheduledFailed(t, e)
	if _, ok := w.catalog.Held(); !ok {
		t.Fatal("the live holder's claim disappeared")
	}
}

// TestLivenessErrorCountsAsAlive: a liveness check that fails proves nothing, so
// nobody takes the claim over — even when the claim is old.
func TestLivenessErrorCountsAsAlive(t *testing.T) {
	w := newWorld(t)
	e := w.newEngine(t, leaseConfig(t, "30ms"))
	w.catalog.Hold("live-run", model.RunKindManual, "infrastructure", "backup-abc", "uid-1", time.Now().Add(-time.Hour))
	w.catalog.SetHolderErr(errors.New("the pod read failed"))

	runScheduledFailed(t, e)
	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindManual})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Lease == nil || !rep.Lease.Refused {
		t.Fatalf("a manual run did not refuse when liveness failed: %+v", rep.Lease)
	}
	if _, ok := w.catalog.Held(); !ok {
		t.Fatal("the claim disappeared after an unprovable liveness check")
	}
}

// TestUnsupportedLivenessCountsAsAlive: a leaser that answers ErrUnsupported has
// no proof either, and behaves exactly like one without the capability.
func TestUnsupportedLivenessCountsAsAlive(t *testing.T) {
	w := newWorld(t)
	e := w.newEngine(t, leaseConfig(t, "30ms"))
	w.catalog.Hold("live-run", model.RunKindManual, "infrastructure", "backup-abc", "uid-1", time.Now().Add(-time.Hour))
	w.catalog.HolderUnsupported = true

	runScheduledFailed(t, e)
}

// TestHolderIdentityComesFromTheEnvironment: the deployment supplies the pod
// namespace, name and UID through downward-API environment variables the run
// config names, and the claim records them.
func TestHolderIdentityComesFromTheEnvironment(t *testing.T) {
	w := newWorld(t)
	cfg := manualConfig(t, map[string]any{"alpha": manualStorage(1 << 40)})
	run := cfg["run"].(map[string]any)
	run["holder_namespace_env"], run["holder_pod_env"], run["holder_uid_env"] = "TEST_NS", "TEST_POD_NAME", "TEST_POD_UID"
	t.Setenv("TEST_NS", "infrastructure")
	t.Setenv("TEST_POD_NAME", "backup-xyz")
	t.Setenv("TEST_POD_UID", "uid-xyz")
	e := w.newEngine(t, cfg)
	lease, _, _, err := e.acquireWriterLease(context.Background(), model.RunKindManual, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	if lease.HolderNamespace != "infrastructure" || lease.HolderPod != "backup-xyz" || lease.HolderUID != "uid-xyz" {
		t.Fatalf("the claim does not record the process identity: %+v", lease)
	}
}

// TestReleaseWithForeignTokenIsANoOp: Release only ever drops the claim it
// carries the token for.
func TestReleaseWithForeignTokenIsANoOp(t *testing.T) {
	w := newWorld(t)
	w.newEngine(t, manualConfig(t, map[string]any{"alpha": manualStorage(1 << 40)}))
	first, err := w.catalog.Acquire(context.Background(), catalog.LeaseRequest{Holder: "a", Kind: model.RunKindManual})
	if err != nil {
		t.Fatal(err)
	}
	foreign := first
	foreign.Token = "somebody-else"
	if err := w.catalog.Release(context.Background(), foreign); err != nil {
		t.Fatal(err)
	}
	if _, ok := w.catalog.Held(); !ok {
		t.Fatal("a foreign-token release dropped the claim")
	}
	if err := w.catalog.Release(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	if _, ok := w.catalog.Held(); ok {
		t.Fatal("the holder's own release did not drop the claim")
	}
}

// TestLeaseCapabilityAnsweredUnsupported: a catalog whose Acquire answers
// ErrUnsupported behaves exactly like one without the interface — a scheduled
// run proceeds, a manual run is refused.
func TestLeaseCapabilityAnsweredUnsupported(t *testing.T) {
	w := newWorld(t)
	err := plugin.Register(w.registry, catalog.Kind, plugin.Spec[map[string]jsontext.Value, catalog.Catalog]{
		Type: "nolease", Example: []byte(`{}`),
		New: func(_ context.Context, _ map[string]jsontext.Value, _ plugin.Deps) (catalog.Catalog, error) {
			return &unsupportedLeaseCatalog{}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	cfg := manualConfig(t, map[string]any{"alpha": manualStorage(1 << 40)})
	cfg["catalog"] = instance("nolease")
	e := w.newEngine(t, cfg)

	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatalf("a scheduled run must proceed without the capability: %v", err)
	}
	if rep.Outcome != model.OutcomeDelivered {
		t.Fatalf("scheduled outcome = %q, want delivered", rep.Outcome)
	}
	if _, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindManual}); err == nil {
		t.Fatal("a manual run started without a lease capability")
	}
}

// unsupportedLeaseCatalog implements the Leaser interface and answers
// errors.ErrUnsupported: a capability discovered by call, not by type.
type unsupportedLeaseCatalog struct{ fakeCatalog }

func (c *unsupportedLeaseCatalog) Acquire(context.Context, catalog.LeaseRequest) (catalog.Lease, error) {
	return catalog.Lease{}, fmt.Errorf("this catalog holds no lease: %w", errors.ErrUnsupported)
}

// scriptedLeaser is the shared in-memory leaser with scriptable Acquire and
// Release failures.
type scriptedLeaser struct {
	catalogtest.Leaser
	acquireErr func(catalog.LeaseRequest) error
	releaseErr error
	releases   int
}

func (l *scriptedLeaser) Acquire(ctx context.Context, req catalog.LeaseRequest) (catalog.Lease, error) {
	if l.acquireErr != nil {
		if err := l.acquireErr(req); err != nil {
			return catalog.Lease{}, err
		}
	}
	return l.Leaser.Acquire(ctx, req)
}

func (l *scriptedLeaser) Release(ctx context.Context, lease catalog.Lease) error {
	l.releases++
	if l.releaseErr != nil {
		return l.releaseErr
	}
	return l.Leaser.Release(ctx, lease)
}

// plainLeaser implements catalog.Leaser only: it has no liveness capability,
// so nobody can prove its holder terminated.
type plainLeaser struct{}

func (l *plainLeaser) Acquire(context.Context, catalog.LeaseRequest) (catalog.Lease, error) {
	return catalog.Lease{}, nil
}

func (l *plainLeaser) Release(context.Context, catalog.Lease) error { return nil }

// TestLeaseTakeoverAcquireFails: a takeover whose own Acquire fails returns
// that error — the proof let the engine try, it did not promise success.
func TestLeaseTakeoverAcquireFails(t *testing.T) {
	w := newWorld(t)
	e := w.newEngine(t, leaseConfig(t, "20ms"))
	sl := &scriptedLeaser{}
	e.lease = sl
	sl.Hold("dead-run", model.RunKindScheduled, "infrastructure", "backup-abc", "uid-1", time.Now())
	sl.SetTerminated(true, true)
	sl.acquireErr = func(req catalog.LeaseRequest) error {
		if req.TakeoverToken != "" {
			return plugin.Permanent(errors.New("takeover refused"))
		}
		return nil
	}
	_, _, _, err := e.acquireWriterLease(context.Background(), model.RunKindScheduled, "run-1")
	if err == nil || !strings.Contains(err.Error(), "takeover refused") {
		t.Fatalf("acquireWriterLease = %v, want the takeover refusal", err)
	}
}

// TestLeaseDefinitiveUndatedTakeover: a proven-terminated holder whose claim
// carries no acquisition time is still taken over, with no quarantine to record.
func TestLeaseDefinitiveUndatedTakeover(t *testing.T) {
	w := newWorld(t)
	e := w.newEngine(t, leaseConfig(t, "20ms"))
	sl := &scriptedLeaser{}
	e.lease = sl
	sl.Hold("dead-run", model.RunKindScheduled, "infrastructure", "backup-abc", "uid-1", time.Time{})
	sl.SetTerminated(true, true)
	lease, _, quarantine, err := e.acquireWriterLease(context.Background(), model.RunKindScheduled, "run-1")
	if err != nil {
		t.Fatalf("the scheduled run did not take over an undated terminated holder: %v", err)
	}
	if lease == nil || lease.Holder != "run-1" {
		t.Fatalf("lease = %+v, want the taken claim", lease)
	}
	if !quarantine.IsZero() {
		t.Fatalf("quarantine = %v, want zero for an undated holder", quarantine)
	}
}

// TestHardBoundPassedUndated: a claim with no acquisition time is never a
// hard-bound proof.
func TestHardBoundPassedUndated(t *testing.T) {
	w := newWorld(t)
	e := w.newEngine(t, leaseConfig(t, "0s"))
	if e.hardBoundPassed(catalog.Lease{}) {
		t.Fatal("an undated claim passed the hard bound")
	}
}

// TestHolderTerminatedWithoutLiveness: a leaser with no liveness capability
// proves nothing, so its holder always counts as alive.
func TestHolderTerminatedWithoutLiveness(t *testing.T) {
	w := newWorld(t)
	e := w.newEngine(t, leaseConfig(t, "0s"))
	e.lease = &plainLeaser{}
	terminated, definitive, provable := e.holderTerminated(context.Background(), catalog.Lease{})
	if terminated || definitive || provable {
		t.Fatalf("holderTerminated without a capability = (%v, %v, %v), want all false", terminated, definitive, provable)
	}
}

// TestReleaseWriterLeaseReportsAFailure: a release that fails is logged and
// returned, never mistaken for a dropped claim.
func TestReleaseWriterLeaseReportsAFailure(t *testing.T) {
	w := newWorld(t)
	e := w.newEngine(t, leaseConfig(t, "0s"))
	sl := &scriptedLeaser{releaseErr: errors.New("release failed")}
	e.lease = sl
	e.releaseWriterLease(context.Background(), catalog.Lease{Token: "t"}, "run-1")
	if sl.releases != 1 {
		t.Fatalf("Release ran %d times, want once", sl.releases)
	}
}
