package protection

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/catalog"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/catalog/catalogtest"
	pluginerrors "github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/inventory"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
)

// discardLog is the test logger: the lease's events are asserted by the tests
// that care about them, the rest only must not panic.
func discardLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// testLease is a lease policy with short waits so refusal and takeover paths
// need no deployment-scale timing.
func testLease(cat catalog.Catalog, mutate func(*LeaseOptions)) *Lease {
	o := LeaseOptions{
		Catalog:         cat,
		Wait:            40 * time.Millisecond,
		Poll:            time.Millisecond,
		HardBound:       10 * time.Millisecond,
		FinishAllowance: time.Second,
		Log:             discardLog(),
	}
	if mutate != nil {
		mutate(&o)
	}
	return NewLease(o)
}

// scripted is the shared in-memory catalog with scriptable failures and call
// counters.
type scripted struct {
	catalogtest.Leaser
	acquireErr func(catalog.LeaseRequest) error
	releaseErr error
	releases   int
	releaseCtx context.Context
	markErr    error
	marks      int
	clearErr   error
	clears     int
	observeErr error
	observes   int
}

func (s *scripted) Acquire(ctx context.Context, req catalog.LeaseRequest) (catalog.Lease, error) {
	if s.acquireErr != nil {
		if err := s.acquireErr(req); err != nil {
			return catalog.Lease{}, err
		}
	}
	return s.Leaser.Acquire(ctx, req)
}

func (s *scripted) Release(ctx context.Context, lease catalog.Lease) error {
	s.releases++
	s.releaseCtx = ctx
	if s.releaseErr != nil {
		return s.releaseErr
	}
	return s.Leaser.Release(ctx, lease)
}

func (s *scripted) MarkUncertainty(ctx context.Context, lease catalog.Lease) error {
	s.marks++
	if s.markErr != nil {
		return s.markErr
	}
	return s.Leaser.MarkUncertainty(ctx, lease)
}

func (s *scripted) ClearUncertainty(ctx context.Context, lease catalog.Lease) error {
	s.clears++
	if s.clearErr != nil {
		return s.clearErr
	}
	return s.Leaser.ClearUncertainty(ctx, lease)
}

func (s *scripted) ObserveUncertainty(ctx context.Context, lease catalog.Lease, digest string) error {
	s.observes++
	if s.observeErr != nil {
		return s.observeErr
	}
	return s.Leaser.ObserveUncertainty(ctx, lease, digest)
}

// plainCatalog holds a claim but has neither the liveness nor the observation
// capability.
type plainCatalog struct{ inner *catalogtest.Leaser }

func (p plainCatalog) Acquire(ctx context.Context, req catalog.LeaseRequest) (catalog.Lease, error) {
	return p.inner.Acquire(ctx, req)
}

func (p plainCatalog) MarkUncertainty(ctx context.Context, lease catalog.Lease) error {
	return p.inner.MarkUncertainty(ctx, lease)
}

func (p plainCatalog) ClearUncertainty(ctx context.Context, lease catalog.Lease) error {
	return p.inner.ClearUncertainty(ctx, lease)
}

func (p plainCatalog) Release(ctx context.Context, lease catalog.Lease) error {
	return p.inner.Release(ctx, lease)
}

// unsupportedCatalog cannot hold a claim at all.
type unsupportedCatalog struct{ inner *catalogtest.Leaser }

func (u unsupportedCatalog) Acquire(ctx context.Context, req catalog.LeaseRequest) (catalog.Lease, error) {
	return catalog.Lease{}, fmt.Errorf("this catalog holds no lease: %w", errors.ErrUnsupported)
}

func (u unsupportedCatalog) MarkUncertainty(context.Context, catalog.Lease) error { return nil }

func (u unsupportedCatalog) ClearUncertainty(context.Context, catalog.Lease) error { return nil }

func (u unsupportedCatalog) Release(context.Context, catalog.Lease) error { return nil }

// TestAcquireAndIdentity: a free claim is taken for either kind and records the
// process identity the deployment names.
func TestAcquireAndIdentity(t *testing.T) {
	for _, kind := range []model.RunKind{model.RunKindManual, model.RunKindScheduled} {
		t.Run(string(kind), func(t *testing.T) {
			t.Setenv("TEST_NS", "infrastructure")
			t.Setenv("TEST_POD", "backup-abc")
			t.Setenv("TEST_UID", "uid-1")
			l := testLease(&scripted{}, func(o *LeaseOptions) {
				o.HolderNamespaceEnv, o.HolderPodEnv, o.HolderUIDEnv = "TEST_NS", "TEST_POD", "TEST_UID"
			})
			claim, _, err := l.Acquire(context.Background(), kind, "run-1")
			if err != nil {
				t.Fatal(err)
			}
			if claim == nil || claim.Holder != "run-1" || claim.Kind != kind {
				t.Fatalf("claim = %+v", claim)
			}
			if claim.HolderNamespace != "infrastructure" || claim.HolderPod != "backup-abc" || claim.HolderUID != "uid-1" {
				t.Fatalf("the claim does not record the process identity: %+v", claim)
			}
			held, ok := l.Claim()
			if !ok || held.Token != claim.Token {
				t.Fatalf("Claim = %+v, %v", held, ok)
			}
		})
	}
}

// TestAcquireWithoutCatalog: a manual run needs a durable claim; a scheduled run
// writes as it always did.
func TestAcquireWithoutCatalog(t *testing.T) {
	l := testLease(nil, nil)
	if _, _, err := l.Acquire(context.Background(), model.RunKindManual, "run-1"); err == nil {
		t.Fatal("a manual run acquired a claim without a catalog")
	}
	if _, _, err := l.Acquire(context.Background(), model.RunKindScheduled, "run-1"); err != nil {
		t.Fatalf("a scheduled run without a catalog = %v", err)
	}
}

// TestAcquireUnsupported: a catalog that answers ErrUnsupported behaves like one
// that cannot hold a claim.
func TestAcquireUnsupported(t *testing.T) {
	cat := unsupportedCatalog{inner: catalogtest.NewLeaser()}
	if _, _, err := testLease(cat, nil).Acquire(context.Background(), model.RunKindManual, "run-1"); err == nil {
		t.Fatal("a manual run acquired a claim from a catalog that cannot hold one")
	}
	if claim, _, err := testLease(cat, nil).Acquire(context.Background(), model.RunKindScheduled, "run-1"); err != nil || claim != nil {
		t.Fatalf("scheduled acquire = %+v, %v", claim, err)
	}
}

// TestTakeoverProofs: a definitive terminal phase lets a scheduled run take over
// at once; an absent pod needs the hard bound; a manual run waits for release.
func TestTakeoverProofs(t *testing.T) {
	dead := func(cat *scripted) {
		cat.Hold("dead-run", model.RunKindScheduled, "infrastructure", "backup-abc", "uid-1", time.Now())
	}
	t.Run("definitive scheduled", func(t *testing.T) {
		cat := &scripted{}
		dead(cat)
		cat.SetTerminated(true, true)
		claim, _, err := testLease(cat, nil).Acquire(context.Background(), model.RunKindScheduled, "run-1")
		if err != nil {
			t.Fatal(err)
		}
		if claim.Uncertainty == nil || !slices.Contains(claim.Uncertainty.RunIDs, "dead-run") {
			t.Fatalf("the takeover did not arm uncertainty: %+v", claim.Uncertainty)
		}
	})
	t.Run("definitive manual refuses", func(t *testing.T) {
		cat := &scripted{}
		dead(cat)
		cat.SetTerminated(true, true)
		_, held, err := testLease(cat, nil).Acquire(context.Background(), model.RunKindManual, "run-1")
		if !errors.Is(err, catalog.ErrLeaseHeld) || held.Holder != "dead-run" {
			t.Fatalf("manual acquire = %+v, %v", held, err)
		}
	})
	t.Run("absent young refuses", func(t *testing.T) {
		cat := &scripted{}
		dead(cat)
		cat.SetTerminated(true, false)
		_, _, err := testLease(cat, func(o *LeaseOptions) { o.HardBound = time.Hour }).
			Acquire(context.Background(), model.RunKindScheduled, "run-1")
		if err == nil {
			t.Fatal("a young absent pod was taken over")
		}
	})
	t.Run("absent past bound takes over", func(t *testing.T) {
		cat := &scripted{}
		cat.Hold("dead-run", model.RunKindManual, "infrastructure", "backup-abc", "uid-1", time.Now().Add(-2*time.Hour))
		cat.SetTerminated(true, false)
		if _, _, err := testLease(cat, nil).Acquire(context.Background(), model.RunKindManual, "run-1"); err != nil {
			t.Fatalf("a claim past the hard bound was not taken over: %v", err)
		}
	})
	t.Run("undated claim never passes the bound", func(t *testing.T) {
		l := testLease(&scripted{}, nil)
		if l.hardBoundPassed(catalog.Lease{}) {
			t.Fatal("an undated claim passed the hard bound")
		}
	})
}

// TestLiveHolderIsNeverTakenOver: a running pod, a failed read, an unsupported
// answer and a catalog without the capability all count as alive.
func TestLiveHolderIsNeverTakenOver(t *testing.T) {
	tests := map[string]func(*scripted){
		"running":     func(c *scripted) { c.SetTerminated(false, false) },
		"read failed": func(c *scripted) { c.SetHolderErr(errors.New("the pod read failed")) },
		"unsupported": func(c *scripted) { c.HolderUnsupported = true },
	}
	for name, arrange := range tests {
		t.Run(name, func(t *testing.T) {
			cat := &scripted{}
			cat.Hold("live-run", model.RunKindManual, "infrastructure", "backup-abc", "uid-1", time.Now().Add(-time.Hour))
			arrange(cat)
			_, _, err := testLease(cat, nil).Acquire(context.Background(), model.RunKindScheduled, "run-1")
			if err == nil {
				t.Fatal("a live holder was taken over")
			}
			if _, ok := cat.Held(); !ok {
				t.Fatal("the live holder's claim disappeared")
			}
		})
	}
	t.Run("no capability", func(t *testing.T) {
		inner := catalogtest.NewLeaser()
		inner.Hold("live-run", model.RunKindManual, "infrastructure", "backup-abc", "uid-1", time.Now().Add(-time.Hour))
		terminated, definitive, provable := testLease(plainCatalog{inner: inner}, nil).
			holderTerminated(context.Background(), catalog.Lease{Holder: "live-run"})
		if terminated || definitive || provable {
			t.Fatalf("holderTerminated = (%v, %v, %v)", terminated, definitive, provable)
		}
	})
}

// TestAcquireWaitsThenFails: a scheduled run waits its bounded time for a live
// holder and returns the refusal.
func TestAcquireWaitsThenFails(t *testing.T) {
	cat := &scripted{}
	cat.Hold("live-run", model.RunKindManual, "infrastructure", "backup-abc", "uid-1", time.Now())
	cat.SetTerminated(false, false)
	_, held, err := testLease(cat, func(o *LeaseOptions) { o.Wait = 5 * time.Millisecond; o.Poll = time.Millisecond }).
		Acquire(context.Background(), model.RunKindScheduled, "run-1")
	if err == nil || held.Holder != "live-run" {
		t.Fatalf("acquire = %+v, %v, want the bounded refusal", held, err)
	}
}

// TestAcquireStopsOnCancellation: a cancelled wait returns the context error
// instead of spinning.
func TestAcquireStopsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cat := &scripted{}
	cat.Hold("live-run", model.RunKindManual, "infrastructure", "backup-abc", "uid-1", time.Now())
	_, _, err := testLease(cat, func(o *LeaseOptions) { o.Wait = time.Hour }).
		Acquire(ctx, model.RunKindScheduled, "run-1")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled acquire = %v, want context.Canceled", err)
	}
}

// TestTakeoverAcquireFails: the proof let the run try, it did not promise
// success.
func TestTakeoverAcquireFails(t *testing.T) {
	cat := &scripted{acquireErr: func(req catalog.LeaseRequest) error {
		if req.TakeoverToken != "" {
			return pluginerrors.Permanent(errors.New("takeover refused"))
		}
		return nil
	}}
	cat.Hold("dead-run", model.RunKindScheduled, "infrastructure", "backup-abc", "uid-1", time.Now())
	cat.SetTerminated(true, true)
	_, _, err := testLease(cat, nil).Acquire(context.Background(), model.RunKindScheduled, "run-1")
	if err == nil || !strings.Contains(err.Error(), "takeover refused") {
		t.Fatalf("acquire = %v, want the takeover refusal", err)
	}
}

// TestMarkAndClear: the mark names this run and is refused on an inherited claim;
// a failed mark stops the write path.
func TestMarkAndClear(t *testing.T) {
	ctx := context.Background()
	cat := &scripted{}
	l := testLease(cat, nil)
	if err := l.MarkUncertainty(ctx); err == nil {
		t.Fatal("a lease without a claim marked one")
	}
	if err := l.ClearUncertainty(ctx); err == nil {
		t.Fatal("a lease without a claim cleared one")
	}
	if _, _, err := l.Acquire(ctx, model.RunKindScheduled, "run-1"); err != nil {
		t.Fatal(err)
	}
	if err := l.MarkUncertainty(ctx); err != nil {
		t.Fatal(err)
	}
	if unc := cat.Uncertainty(); unc == nil || !slices.Contains(unc.RunIDs, "run-1") {
		t.Fatalf("the mark = %+v", unc)
	}
	if err := l.ClearUncertainty(ctx); err != nil {
		t.Fatal(err)
	}
	if cat.Uncertainty() != nil {
		t.Fatal("the settled mark survived the clear")
	}

	inherited := &scripted{}
	inherited.SetUncertainty(&catalog.Uncertainty{RunIDs: []string{"dead"}, Since: time.Now().UTC()})
	li := testLease(inherited, nil)
	if _, _, err := li.Acquire(ctx, model.RunKindScheduled, "run-1"); err != nil {
		t.Fatal(err)
	}
	// C7: a successor marks its OWN writes on the inherited record; the mark
	// appends this run's id and never drops the predecessor's.
	if err := li.MarkUncertainty(ctx); err != nil {
		t.Fatalf("a successor could not mark its own writes: %v", err)
	}
	if got := inherited.Uncertainty(); got == nil || len(got.RunIDs) != 2 ||
		got.RunIDs[0] != "dead" || got.RunIDs[1] != "run-1" {
		t.Fatalf("the pending set = %+v, want [dead run-1]", got)
	}
	if ids := li.PendingRunIDs(); len(ids) != 2 || ids[0] != "dead" || ids[1] != "run-1" {
		t.Fatalf("PendingRunIDs = %v, want [dead run-1]", ids)
	}
	if err := li.ClearUncertainty(ctx); err != nil {
		t.Fatal(err)
	}
	if ids := li.PendingRunIDs(); ids != nil {
		t.Fatalf("a cleared claim still reports pending ids: %v", ids)
	}
	if ids := testLease(&scripted{}, nil).PendingRunIDs(); ids != nil {
		t.Fatalf("a free lease reports pending ids: %v", ids)
	}

	failing := &scripted{markErr: errors.New("the catalog is unreachable")}
	lf := testLease(failing, nil)
	if _, _, err := lf.Acquire(ctx, model.RunKindScheduled, "run-2"); err != nil {
		t.Fatal(err)
	}
	if err := lf.MarkUncertainty(ctx); err == nil || !strings.Contains(err.Error(), "unreachable") {
		t.Fatalf("failed mark = %v", err)
	}
}

// TestClearFailureIsConservative: a failed clear keeps the record uncertain.
func TestClearFailureIsConservative(t *testing.T) {
	cat := &scripted{clearErr: errors.New("the catalog is unreachable")}
	l := testLease(cat, nil)
	if _, _, err := l.Acquire(context.Background(), model.RunKindScheduled, "run-1"); err != nil {
		t.Fatal(err)
	}
	if err := l.MarkUncertainty(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := l.ClearUncertainty(context.Background()); err == nil {
		t.Fatal("a failed clear reported success")
	}
	if cat.Uncertainty() == nil {
		t.Fatal("a failed clear dropped the mark")
	}
}

// TestSettleInheritedUncertainty: first and different observations are recorded,
// two equal ones clear, and failed clears or observations stay conservative.
func TestSettleInheritedUncertainty(t *testing.T) {
	ctx := context.Background()
	storages := []string{"alpha"}
	surveys := func(key string) map[string]inventory.Survey {
		return map[string]inventory.Survey{"alpha": {Storage: "alpha", OK: true, Runs: map[string]inventory.Run{
			"dead": {Objects: []model.ObjectInfo{{Key: key, Size: 1}}},
		}}}
	}
	cat := &scripted{}
	cat.SetUncertainty(&catalog.Uncertainty{RunIDs: []string{"dead"}, Since: time.Now().UTC()})
	l := testLease(cat, nil)
	claim, _, err := l.Acquire(ctx, model.RunKindScheduled, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	if claim.Uncertainty == nil {
		t.Fatal("the acquired claim did not inherit the uncertainty")
	}
	if settled, err := l.SettleInheritedUncertainty(ctx, model.RunKindManual, "run-1", storages, surveys("k1")); settled || err != nil {
		t.Fatalf("a manual run observed = (%v, %v)", settled, err)
	}
	// A lease without a claim observes nothing.
	if settled, err := testLease(cat, nil).SettleInheritedUncertainty(ctx, model.RunKindScheduled, "run-1", storages, surveys("k1")); settled || err != nil {
		t.Fatalf("a free lease observed = (%v, %v)", settled, err)
	}

	first, err := l.SettleInheritedUncertainty(ctx, model.RunKindScheduled, "run-1", storages, surveys("k1"))
	if err != nil || first {
		t.Fatalf("first observation = (%v, %v)", first, err)
	}
	recorded := snapshotUncertainty(cat.Uncertainty())
	if recorded == nil || !slices.Contains(recorded.RunIDs, "dead") || recorded.Digest == "" {
		t.Fatalf("the first observation = %+v", recorded)
	}
	if cat.observes != 1 {
		t.Fatalf("ObserveUncertainty ran %d times", cat.observes)
	}

	l.claim.Uncertainty = snapshotUncertainty(recorded)
	if settled, err := l.SettleInheritedUncertainty(ctx, model.RunKindScheduled, "run-2", storages, surveys("k2")); settled || err != nil {
		t.Fatalf("a different observation = (%v, %v)", settled, err)
	}
	replaced := snapshotUncertainty(cat.Uncertainty())
	if replaced == nil || replaced.Digest == recorded.Digest {
		t.Fatalf("the different observation was not recorded: before=%q after=%+v", recorded.Digest, replaced)
	}

	l.claim.Uncertainty = snapshotUncertainty(replaced)
	if settled, err := l.SettleInheritedUncertainty(ctx, model.RunKindScheduled, "run-3", storages, surveys("k2")); !settled || err != nil {
		t.Fatalf("two equal observations = (%v, %v)", settled, err)
	}
	if cat.Uncertainty() != nil {
		t.Fatal("the settlement proof did not clear the record")
	}

	failingClear := &scripted{clearErr: errors.New("unreachable")}
	failingClear.SetUncertainty(&catalog.Uncertainty{RunIDs: []string{"dead"}, Digest: replaced.Digest})
	lf := testLease(failingClear, nil)
	if _, _, err := lf.Acquire(ctx, model.RunKindScheduled, "run-4"); err != nil {
		t.Fatal(err)
	}
	if settled, err := lf.SettleInheritedUncertainty(ctx, model.RunKindScheduled, "run-4", storages, surveys("k2")); settled || err != nil {
		t.Fatalf("a failed clear = (%v, %v)", settled, err)
	}
	if failingClear.Uncertainty() == nil {
		t.Fatal("a failed clear dropped the uncertainty")
	}

	failingObserve := &scripted{observeErr: errors.New("unreachable")}
	lo := testLease(failingObserve, nil)
	claimU := catalogtest.NewLeaser()
	claimU.SetUncertainty(&catalog.Uncertainty{RunIDs: []string{"dead"}})
	taken, err := claimU.Acquire(ctx, catalog.LeaseRequest{Holder: "run-5", Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	lo.claim, lo.held = taken, true
	if settled, err := lo.SettleInheritedUncertainty(ctx, model.RunKindScheduled, "run-5", storages, surveys("k1")); settled || err == nil {
		t.Fatalf("a failed observation = (%v, %v)", settled, err)
	}
}

// snapshotUncertainty copies a record value so a later observation cannot mutate
// the value a test compares against.
func snapshotUncertainty(u *catalog.Uncertainty) *catalog.Uncertainty {
	if u == nil {
		return nil
	}
	clone := *u
	return &clone
}

// TestLateCommitKeepsInheritedUncertainty: a commit between two observations
// changes the digest, so the uncertainty stands with the new observation.
func TestLateCommitKeepsInheritedUncertainty(t *testing.T) {
	ctx := context.Background()
	storages := []string{"alpha"}
	surveys := func(late bool) map[string]inventory.Survey {
		objects := []model.ObjectInfo{{Key: "dead/a", Size: 1}}
		if late {
			objects = append(objects, model.ObjectInfo{Key: "dead/late", Size: 7})
		}
		return map[string]inventory.Survey{"alpha": {Storage: "alpha", OK: true, Runs: map[string]inventory.Run{
			"dead": {Objects: objects},
		}}}
	}
	cat := &scripted{}
	cat.SetUncertainty(&catalog.Uncertainty{RunIDs: []string{"dead"}})
	l := testLease(cat, nil)
	if _, _, err := l.Acquire(ctx, model.RunKindScheduled, "run-1"); err != nil {
		t.Fatal(err)
	}
	if settled, err := l.SettleInheritedUncertainty(ctx, model.RunKindScheduled, "run-1", storages, surveys(false)); settled || err != nil {
		t.Fatalf("first = (%v, %v)", settled, err)
	}
	recorded := snapshotUncertainty(cat.Uncertainty())
	l.claim.Uncertainty = snapshotUncertainty(recorded)
	if settled, err := l.SettleInheritedUncertainty(ctx, model.RunKindScheduled, "run-2", storages, surveys(true)); settled || err != nil {
		t.Fatalf("late commit = (%v, %v)", settled, err)
	}
	after := cat.Uncertainty()
	if after == nil || after.Digest == recorded.Digest || after.Digest == "" {
		t.Fatalf("the late commit's inventory was not recorded: %+v", after)
	}
}

// TestFailedSurveyBreaksTheObservationChain: a failed survey invalidates the
// stored observation; only two consecutive full surveys settle.
func TestFailedSurveyBreaksTheObservationChain(t *testing.T) {
	ctx := context.Background()
	storages := []string{"alpha"}
	full := map[string]inventory.Survey{"alpha": {Storage: "alpha", OK: true, Runs: map[string]inventory.Run{
		"dead": {Objects: []model.ObjectInfo{{Key: "dead/a", Size: 1}}},
	}}}
	failed := map[string]inventory.Survey{"alpha": {Storage: "alpha", OK: false}}
	cat := &scripted{}
	cat.SetUncertainty(&catalog.Uncertainty{RunIDs: []string{"dead"}})
	l := testLease(cat, nil)
	if _, _, err := l.Acquire(ctx, model.RunKindScheduled, "run-1"); err != nil {
		t.Fatal(err)
	}
	if settled, err := l.SettleInheritedUncertainty(ctx, model.RunKindScheduled, "run-1", storages, full); settled || err != nil {
		t.Fatalf("first = (%v, %v)", settled, err)
	}
	l.claim.Uncertainty = snapshotUncertainty(cat.Uncertainty())
	if settled, err := l.SettleInheritedUncertainty(ctx, model.RunKindScheduled, "run-2", storages, failed); settled || err != nil {
		t.Fatalf("failed survey = (%v, %v)", settled, err)
	}
	if unc := cat.Uncertainty(); unc == nil || unc.Digest != "" {
		t.Fatalf("a failed survey did not invalidate the observation: %+v", unc)
	}
	l.claim.Uncertainty = snapshotUncertainty(cat.Uncertainty())
	if settled, err := l.SettleInheritedUncertainty(ctx, model.RunKindScheduled, "run-3", storages, full); settled || err != nil {
		t.Fatalf("after failure = (%v, %v)", settled, err)
	}
	l.claim.Uncertainty = snapshotUncertainty(cat.Uncertainty())
	if settled, err := l.SettleInheritedUncertainty(ctx, model.RunKindScheduled, "run-4", storages, full); !settled || err != nil {
		t.Fatalf("two equal = (%v, %v)", settled, err)
	}
	if cat.Uncertainty() != nil {
		t.Fatal("the settlement proof did not clear the record")
	}
}

// TestSettlementNeedsTheObservationCapability: a catalog without the capability
// answers unsupported and the uncertainty stays.
func TestSettlementNeedsTheObservationCapability(t *testing.T) {
	inner := catalogtest.NewLeaser()
	inner.SetUncertainty(&catalog.Uncertainty{RunIDs: []string{"dead"}})
	cat := plainCatalog{inner: inner}
	l := testLease(cat, nil)
	if _, _, err := l.Acquire(context.Background(), model.RunKindScheduled, "run-1"); err != nil {
		t.Fatal(err)
	}
	surveys := map[string]inventory.Survey{"alpha": {Storage: "alpha", OK: true}}
	settled, err := l.SettleInheritedUncertainty(context.Background(), model.RunKindScheduled, "run-1", []string{"alpha"}, surveys)
	if settled || !errors.Is(err, errors.ErrUnsupported) {
		t.Fatalf("settle without observation = (%v, %v)", settled, err)
	}
	if inner.Uncertainty() == nil {
		t.Fatal("the uncertainty was dropped without an observation")
	}
}

// TestStaleHolderCannotClear: a holder whose claim was taken over cannot clear
// the successor's record.
func TestStaleHolderCannotClear(t *testing.T) {
	ctx := context.Background()
	cat := catalogtest.NewLeaser()
	first, err := cat.Acquire(ctx, catalog.LeaseRequest{Holder: "A", Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if err := cat.MarkUncertainty(ctx, first); err != nil {
		t.Fatal(err)
	}
	if _, err := cat.Acquire(ctx, catalog.LeaseRequest{Holder: "B", Kind: model.RunKindScheduled, TakeoverToken: first.Token}); err != nil {
		t.Fatal(err)
	}
	l := testLease(plainCatalog{inner: cat}, nil)
	l.claim, l.held = first, true
	if err := l.ClearUncertainty(ctx); err != nil {
		t.Fatal(err)
	}
	if cat.Uncertainty() == nil {
		t.Fatal("a stale holder cleared the successor's uncertainty")
	}
}

// TestReleaseBudgetAndFailure: release runs under the finishing allowance on the
// caller's cleanup deadline, drops the claim, and reports a failure without
// claiming success; a lease that holds nothing releases as a no-op.
func TestReleaseBudgetAndFailure(t *testing.T) {
	ctx := context.Background()
	cat := &scripted{}
	l := testLease(cat, func(o *LeaseOptions) { o.FinishAllowance = 25 * time.Millisecond })
	if err := l.Release(ctx); err != nil {
		t.Fatalf("release without a claim = %v", err)
	}
	if _, _, err := l.Acquire(ctx, model.RunKindScheduled, "run-1"); err != nil {
		t.Fatal(err)
	}
	if err := l.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if cat.releases != 1 {
		t.Fatalf("Release ran %d times", cat.releases)
	}
	if _, has := cat.releaseCtx.Deadline(); !has {
		t.Fatal("release ran without its finishing-allowance budget")
	}
	if _, ok := l.Claim(); ok {
		t.Fatal("a released lease still reports its claim")
	}

	failing := &scripted{releaseErr: errors.New("release failed")}
	lf := testLease(failing, nil)
	if _, _, err := lf.Acquire(ctx, model.RunKindScheduled, "run-2"); err != nil {
		t.Fatal(err)
	}
	if err := lf.Release(ctx); err == nil {
		t.Fatal("a failed release reported success")
	}
}

// TestM6_ClearResetsLocalUncertainty: after the durable clear succeeds the local
// claim is known too, so the run that settled an inherited record can arm its
// own mark for the writes it is now allowed to make.
func TestM6_ClearResetsLocalUncertainty(t *testing.T) {
	ctx := context.Background()
	storages := []string{"alpha"}
	surveys := map[string]inventory.Survey{"alpha": {Storage: "alpha", OK: true, Runs: map[string]inventory.Run{
		"dead": {Objects: []model.ObjectInfo{{Key: "dead/a", Size: 1}}},
	}}}
	cat := &scripted{}
	cat.SetUncertainty(&catalog.Uncertainty{RunIDs: []string{"dead"}})
	l := testLease(cat, nil)
	if _, _, err := l.Acquire(ctx, model.RunKindScheduled, "run-1"); err != nil {
		t.Fatal(err)
	}
	if settled, err := l.SettleInheritedUncertainty(ctx, model.RunKindScheduled, "run-1", storages, surveys); settled || err != nil {
		t.Fatalf("first observation = (%v, %v)", settled, err)
	}
	l.claim.Uncertainty = snapshotUncertainty(cat.Uncertainty())
	if settled, err := l.SettleInheritedUncertainty(ctx, model.RunKindScheduled, "run-2", storages, surveys); !settled || err != nil {
		t.Fatalf("settlement = (%v, %v)", settled, err)
	}
	claim, _ := l.Claim()
	if claim.Uncertainty != nil {
		t.Fatalf("a settled claim still carries the stale uncertainty: %+v", claim.Uncertainty)
	}
	if err := l.MarkUncertainty(ctx); err != nil {
		t.Fatalf("the settled run cannot mark its own writes: %v", err)
	}
	if unc := cat.Uncertainty(); unc == nil || !slices.Contains(unc.RunIDs, "run-1") {
		t.Fatalf("the run's own mark = %+v", unc)
	}
}

// TestC7_DigestCoversEveryPendingRun: the settlement inventory hashes every
// pending holder's prefix, so a successor's own writes change the next digest
// and cannot hide behind the inherited run's observation.
func TestC7_DigestCoversEveryPendingRun(t *testing.T) {
	storages := []string{"alpha", "beta"}
	surveys := func(objects map[string][]model.ObjectInfo) map[string]inventory.Survey {
		out := map[string]inventory.Survey{}
		for _, name := range storages {
			runs := map[string]inventory.Run{}
			for id, objs := range objects {
				runs[id] = inventory.Run{Objects: objs}
			}
			out[name] = inventory.Survey{Storage: name, OK: true, Runs: runs}
		}
		return out
	}
	base := surveys(map[string][]model.ObjectInfo{
		"dead":  {{Key: "dead/a", Size: 1}},
		"run-2": {{Key: "run-2/a", Size: 2}},
	})
	onlyDead := []string{"dead"}
	both := []string{"dead", "run-2"}
	deadOnly, observed := uncertaintyDigest(onlyDead, storages, base)
	if !observed {
		t.Fatal("a full survey was not observed")
	}
	everything, _ := uncertaintyDigest(both, storages, base)
	if everything == deadOnly {
		t.Fatal("the successor's prefix does not enter the digest")
	}
	// The successor's own late write changes the digest of the same pending set.
	changed := surveys(map[string][]model.ObjectInfo{
		"dead":  {{Key: "dead/a", Size: 1}},
		"run-2": {{Key: "run-2/a", Size: 2}, {Key: "run-2/late", Size: 7}},
	})
	if moved, _ := uncertaintyDigest(both, storages, changed); moved == everything {
		t.Fatal("a late commit of the successor's run did not change the digest")
	}
	failed := surveys(nil)
	failed["alpha"] = inventory.Survey{Storage: "alpha", OK: false}
	if _, observed := uncertaintyDigest(both, storages, failed); observed {
		t.Fatal("a failed survey was read as an observation")
	}
}

// TestM7_InterruptedObservationNeverSettles: BeginObservation writes a pending
// marker before the survey; a run that crashes between Begin and Settle leaves
// it, and the successor records a first observation instead of comparing
// against the stale digest.
func TestM7_InterruptedObservationNeverSettles(t *testing.T) {
	ctx := context.Background()
	storages := []string{"alpha"}
	surveys := map[string]inventory.Survey{"alpha": {Storage: "alpha", OK: true, Runs: map[string]inventory.Run{
		"dead": {Objects: []model.ObjectInfo{{Key: "dead/a", Size: 1}}},
	}}}
	cat := &scripted{}
	cat.SetUncertainty(&catalog.Uncertainty{RunIDs: []string{"dead"}})

	first := testLease(cat, nil)
	if _, _, err := first.Acquire(ctx, model.RunKindScheduled, "run-1"); err != nil {
		t.Fatal(err)
	}
	if err := first.BeginObservation(ctx); err != nil {
		t.Fatal(err)
	}
	if got := cat.Uncertainty(); got == nil || !strings.HasPrefix(got.Digest, "pending:") {
		t.Fatalf("BeginObservation left no pending marker: %+v", got)
	}
	// The process dies here: no Settle. The successor takes the claim.
	if err := first.Release(ctx); err != nil {
		t.Fatal(err)
	}

	second := testLease(cat, nil)
	if _, _, err := second.Acquire(ctx, model.RunKindScheduled, "run-2"); err != nil {
		t.Fatal(err)
	}
	if err := second.BeginObservation(ctx); err != nil {
		t.Fatal(err)
	}
	if settled, err := second.SettleInheritedUncertainty(ctx, model.RunKindScheduled, "run-2", storages, surveys); settled || err != nil {
		t.Fatalf("an interrupted observation settled = (%v, %v)", settled, err)
	}
	if got := cat.Uncertainty(); got == nil || strings.HasPrefix(got.Digest, "pending:") || got.Digest == "" {
		t.Fatalf("the first completed observation was not recorded: %+v", got)
	}
}

// TestM7_FailedInvalidationCannotClearAStaleDigest: when a failed survey cannot
// durably invalidate the earlier observation, the pending marker written before
// that survey still forbids the comparison, so a later equal full survey cannot
// clear the record on an observation separated by an unknown gap.
func TestM7_FailedInvalidationCannotClearAStaleDigest(t *testing.T) {
	ctx := context.Background()
	storages := []string{"alpha"}
	surveys := map[string]inventory.Survey{"alpha": {Storage: "alpha", OK: true, Runs: map[string]inventory.Run{
		"dead": {Objects: []model.ObjectInfo{{Key: "dead/a", Size: 1}}},
	}}}
	failed := map[string]inventory.Survey{"alpha": {Storage: "alpha", OK: false}}
	cat := &scripted{}
	cat.SetUncertainty(&catalog.Uncertainty{RunIDs: []string{"dead"}})

	// Run 1 completes a full observation D.
	run1 := testLease(cat, nil)
	if _, _, err := run1.Acquire(ctx, model.RunKindScheduled, "run-1"); err != nil {
		t.Fatal(err)
	}
	if err := run1.BeginObservation(ctx); err != nil {
		t.Fatal(err)
	}
	if settled, err := run1.SettleInheritedUncertainty(ctx, model.RunKindScheduled, "run-1", storages, surveys); settled || err != nil {
		t.Fatalf("first observation = (%v, %v)", settled, err)
	}
	if err := run1.Release(ctx); err != nil {
		t.Fatal(err)
	}

	// Run 2 fails its survey AND its durable invalidation.
	run2 := testLease(cat, nil)
	if _, _, err := run2.Acquire(ctx, model.RunKindScheduled, "run-2"); err != nil {
		t.Fatal(err)
	}
	if err := run2.BeginObservation(ctx); err != nil {
		t.Fatal(err)
	}
	cat.observeErr = errors.New("the catalog is unreachable")
	if _, err := run2.SettleInheritedUncertainty(ctx, model.RunKindScheduled, "run-2", storages, failed); err == nil {
		t.Fatal("a failed invalidation reported success")
	}
	cat.observeErr = nil
	if err := run2.Release(ctx); err != nil {
		t.Fatal(err)
	}

	// Run 3 sees the same inventory, but the record still carries run 2's
	// pending marker: it records an observation and must not clear.
	run3 := testLease(cat, nil)
	if _, _, err := run3.Acquire(ctx, model.RunKindScheduled, "run-3"); err != nil {
		t.Fatal(err)
	}
	if err := run3.BeginObservation(ctx); err != nil {
		t.Fatal(err)
	}
	if settled, err := run3.SettleInheritedUncertainty(ctx, model.RunKindScheduled, "run-3", storages, surveys); settled || err != nil {
		t.Fatalf("a survey separated by a failed observation cleared = (%v, %v)", settled, err)
	}
	if err := run3.Release(ctx); err != nil {
		t.Fatal(err)
	}

	// Run 4 follows a completed observation, so two consecutive ones settle.
	run4 := testLease(cat, nil)
	if _, _, err := run4.Acquire(ctx, model.RunKindScheduled, "run-4"); err != nil {
		t.Fatal(err)
	}
	if err := run4.BeginObservation(ctx); err != nil {
		t.Fatal(err)
	}
	if settled, err := run4.SettleInheritedUncertainty(ctx, model.RunKindScheduled, "run-4", storages, surveys); !settled || err != nil {
		t.Fatalf("two consecutive completed observations = (%v, %v)", settled, err)
	}
}

// TestM7_BeginWithoutObserverRefuses: a catalog that cannot record the attempt
// answers unsupported, so the workflow never treats the survey as evidence.
func TestM7_BeginWithoutObserverRefuses(t *testing.T) {
	inner := catalogtest.NewLeaser()
	inner.SetUncertainty(&catalog.Uncertainty{RunIDs: []string{"dead"}})
	l := testLease(plainCatalog{inner: inner}, nil)
	if _, _, err := l.Acquire(context.Background(), model.RunKindScheduled, "run-1"); err != nil {
		t.Fatal(err)
	}
	if err := l.BeginObservation(context.Background()); !errors.Is(err, errors.ErrUnsupported) {
		t.Fatalf("BeginObservation without observer = %v, want ErrUnsupported", err)
	}
	// A claim with no uncertainty has nothing to begin.
	free := testLease(&scripted{}, nil)
	if err := free.BeginObservation(context.Background()); err != nil {
		t.Fatalf("a free lease began an observation: %v", err)
	}
	// A durable failure to write the attempt marker is surfaced: the workflow
	// then never treats the survey as settlement evidence.
	failing := &scripted{observeErr: errors.New("the catalog is unreachable")}
	failing.SetUncertainty(&catalog.Uncertainty{RunIDs: []string{"dead"}})
	lf := testLease(failing, nil)
	if _, _, err := lf.Acquire(context.Background(), model.RunKindScheduled, "run-2"); err != nil {
		t.Fatal(err)
	}
	if err := lf.BeginObservation(context.Background()); err == nil {
		t.Fatal("a failed attempt marker reported success")
	}
}

// TestR2_PendingRemovalBoundIsTheCatalogsWriteTimeout: the finish window reserves
// the catalog's own upper bound for one record write, and nothing without the
// capability.
func TestR2_PendingRemovalBoundIsTheCatalogsWriteTimeout(t *testing.T) {
	bounded := &scripted{}
	bounded.WriteBound = 30 * time.Second
	if got := testLease(bounded, nil).PendingRemovalBound(); got != 30*time.Second {
		t.Fatalf("PendingRemovalBound() = %s, want the catalog's 30s write bound", got)
	}
	plain := plainCatalog{inner: catalogtest.NewLeaser()}
	if _, ok := any(plain).(catalog.WriteBounded); ok {
		t.Fatal("the plain catalog reports a write bound")
	}
	if got := testLease(plain, nil).PendingRemovalBound(); got != 0 {
		t.Fatalf("PendingRemovalBound() = %s, want 0 without the capability", got)
	}
	// A free lease over a bounded catalog still answers the bound: the reserve is
	// the catalog's property, not the claim's.
	if got := testLease(bounded, nil).PendingRemovalBound(); got != 30*time.Second {
		t.Fatalf("PendingRemovalBound() without a claim = %s, want 30s", got)
	}
}

// TestR2_M14_RemoveOwnPendingKeepsInheritedPrefixes: a settled successor durably
// drops only its own id; the inherited pending prefix and the recorded digest
// survive.
func TestR2_M14_RemoveOwnPendingKeepsInheritedPrefixes(t *testing.T) {
	ctx := context.Background()
	cat := &scripted{}
	cat.SetUncertainty(&catalog.Uncertainty{RunIDs: []string{"dead"}, Digest: "d1", Since: time.Now().UTC()})
	l := testLease(cat, nil)
	if _, _, err := l.Acquire(ctx, model.RunKindScheduled, "run-1"); err != nil {
		t.Fatal(err)
	}
	if err := l.MarkUncertainty(ctx); err != nil {
		t.Fatal(err)
	}
	if err := l.RemoveOwnPending(ctx); err != nil {
		t.Fatal(err)
	}
	got := cat.Uncertainty()
	if got == nil || len(got.RunIDs) != 1 || got.RunIDs[0] != "dead" {
		t.Fatalf("the pending set = %+v, want [dead]", got)
	}
	if got.Digest != "d1" {
		t.Fatalf("the recorded digest = %q, want d1 kept for the inherited prefix", got.Digest)
	}
	// Removing again is a no-op: the own id is gone.
	before := cat.observes
	if err := l.RemoveOwnPending(ctx); err != nil {
		t.Fatalf("repeat removal = %v", err)
	}
	if cat.observes != before {
		t.Fatal("a no-op removal wrote to the catalog")
	}
	// A free lease removes nothing.
	if err := testLease(cat, nil).RemoveOwnPending(ctx); err != nil {
		t.Fatalf("a free lease = %v", err)
	}
}

// TestR2_M14_RemoveOwnPendingClearsTheRecord: when only this run is pending, a
// settled run clears the whole record.
func TestR2_M14_RemoveOwnPendingClearsTheRecord(t *testing.T) {
	ctx := context.Background()
	cat := &scripted{}
	l := testLease(cat, nil)
	if _, _, err := l.Acquire(ctx, model.RunKindScheduled, "run-1"); err != nil {
		t.Fatal(err)
	}
	if err := l.MarkUncertainty(ctx); err != nil {
		t.Fatal(err)
	}
	if err := l.RemoveOwnPending(ctx); err != nil {
		t.Fatal(err)
	}
	if cat.Uncertainty() != nil {
		t.Fatalf("the record survived a settled own-only run: %+v", cat.Uncertainty())
	}
	if _, held := l.Claim(); !held {
		t.Fatal("removing the pending id dropped the claim")
	}
}

// TestR2_M14_RemoveOwnPendingFailureKeepsTheId: a failed durable update keeps
// the own id; the record stays conservative.
func TestR2_M14_RemoveOwnPendingFailureKeepsTheId(t *testing.T) {
	ctx := context.Background()
	cat := &scripted{markErr: errors.New("catalog unavailable")}
	cat.SetUncertainty(&catalog.Uncertainty{RunIDs: []string{"dead"}})
	l := testLease(cat, nil)
	if _, _, err := l.Acquire(ctx, model.RunKindScheduled, "run-1"); err != nil {
		t.Fatal(err)
	}
	l.claim.Uncertainty.RunIDs = append(l.claim.Uncertainty.RunIDs, "run-1")
	if err := l.RemoveOwnPending(ctx); err == nil {
		t.Fatal("a failed removal reported success")
	}
	if ids := l.PendingRunIDs(); len(ids) != 2 {
		t.Fatalf("the local pending set = %v, want both ids after a failed removal", ids)
	}
}

// TestR2_M7_BrokenMarkerInvalidationGuards: when the record cannot be
// invalidated (no observer, or the write fails), Release still runs and the
// record stays conservative instead of panicking or clearing.
func TestR2_M7_BrokenMarkerInvalidationGuards(t *testing.T) {
	ctx := context.Background()
	inner := catalogtest.NewLeaser()
	inner.SetUncertainty(&catalog.Uncertainty{RunIDs: []string{"dead"}, Digest: "d1"})
	withoutObserver := testLease(plainCatalog{inner: inner}, nil)
	if _, _, err := withoutObserver.Acquire(ctx, model.RunKindScheduled, "run-1"); err != nil {
		t.Fatal(err)
	}
	withoutObserver.observationBroken = true
	if err := withoutObserver.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if unc := inner.Uncertainty(); unc == nil || unc.Digest != "d1" {
		t.Fatalf("a record without an observer lost its observation: %+v", unc)
	}

	cat := &scripted{observeErr: errors.New("the catalog is unreachable")}
	cat.SetUncertainty(&catalog.Uncertainty{RunIDs: []string{"dead"}, Digest: "d1"})
	failing := testLease(cat, nil)
	if _, _, err := failing.Acquire(ctx, model.RunKindScheduled, "run-2"); err != nil {
		t.Fatal(err)
	}
	failing.observationBroken = true
	if err := failing.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if unc := cat.Uncertainty(); unc == nil || unc.Digest != "d1" {
		t.Fatalf("a failed invalidation changed the record: %+v", unc)
	}
}
