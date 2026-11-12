package catalogtest

import (
	"context"
	"errors"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/catalog"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
)

var leaseBase = time.Date(2026, 10, 14, 3, 0, 0, 0, time.UTC)

func cancelledContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

// TestLeaserRefusesAHeldClaimAndTakesOverOnItsExactToken: the fake obeys the
// same rule the real catalog does — a held claim refuses and names its holder,
// and only the exact takeover token hands it over, armed with uncertainty for
// the holder it replaced.
func TestLeaserRefusesAHeldClaimAndTakesOverOnItsExactToken(t *testing.T) {
	l := NewLeaser()
	ctx := context.Background()

	if _, err := l.Acquire(cancelledContext(t), catalog.LeaseRequest{Holder: "run-a"}); err == nil {
		t.Fatal("a cancelled context did not stop the acquisition")
	}

	first, err := l.Acquire(ctx, catalog.LeaseRequest{
		Holder: "run-a", Kind: model.RunKindManual,
		HolderNamespace: "infrastructure", HolderPod: "backup-a", HolderUID: "uid-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	if first.Token == "" || first.Holder != "run-a" || first.Kind != model.RunKindManual {
		t.Fatalf("the claim does not record its holder: %+v", first)
	}
	if first.HolderPod != "backup-a" || first.HolderUID != "uid-a" || first.HolderNamespace != "infrastructure" {
		t.Fatalf("the claim does not record the process identity: %+v", first)
	}
	if first.AcquiredAt.IsZero() {
		t.Fatal("the claim records no acquisition time")
	}
	if first.Uncertainty != nil {
		t.Fatalf("a fresh record carries uncertainty %+v", first.Uncertainty)
	}
	if l.AcquireCount() != 1 {
		t.Fatalf("acquires = %d, want 1", l.AcquireCount())
	}

	held, err := l.Acquire(ctx, catalog.LeaseRequest{Holder: "run-b", Kind: model.RunKindScheduled})
	if !errors.Is(err, catalog.ErrLeaseHeld) {
		t.Fatalf("a held claim was handed over: %v", err)
	}
	if held.Holder != "run-a" || held.Token != first.Token {
		t.Fatalf("the refusal does not name the holder: %+v", held)
	}
	if _, err := l.Acquire(ctx, catalog.LeaseRequest{Holder: "run-b", TakeoverToken: "not-the-token"}); !errors.Is(err, catalog.ErrLeaseHeld) {
		t.Fatalf("a foreign takeover token was accepted: %v", err)
	}

	taken, err := l.Acquire(ctx, catalog.LeaseRequest{
		Holder: "run-b", Kind: model.RunKindScheduled, TakeoverToken: first.Token,
	})
	if err != nil {
		t.Fatalf("a proven-dead claim was not taken over: %v", err)
	}
	if taken.Holder != "run-b" || taken.Token == first.Token {
		t.Fatalf("takeover returned %+v", taken)
	}
	if taken.Uncertainty == nil || !slices.Equal(taken.Uncertainty.RunIDs, []string{"run-a"}) {
		t.Fatalf("a takeover armed no uncertainty for the replaced holder: %+v", taken.Uncertainty)
	}
	if current, ok := l.Held(); !ok || current.Token != taken.Token {
		t.Fatalf("Held = %+v, %v; want the new claim", current, ok)
	}
}

// TestLeaserBoundsOneRecordWrite: the fake reports its configured write bound
// and holds a mark or a clear for WriteHold, released by the caller's
// cancellation. A hold has no socket behind it, so the waits run on virtual
// time, and synctest.Wait proves the clear entered the hold before it is
// cancelled.
func TestLeaserBoundsOneRecordWrite(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := NewLeaser()
		l.WriteBound = 30 * time.Second
		if got := l.WriteTimeout(); got != 30*time.Second {
			t.Fatalf("WriteTimeout() = %s, want the configured 30s", got)
		}
		ctx := context.Background()
		first, err := l.Acquire(ctx, catalog.LeaseRequest{Holder: "run-a"})
		if err != nil {
			t.Fatal(err)
		}
		pending := first
		pending.Uncertainty = &catalog.Uncertainty{RunIDs: []string{"run-a"}, Since: leaseBase}
		l.WriteHold = 20 * time.Millisecond
		start := time.Now()
		if err := l.MarkUncertainty(ctx, pending); err != nil {
			t.Fatal(err)
		}
		if took := time.Since(start); took < 20*time.Millisecond {
			t.Fatalf("MarkUncertainty returned in %s, want it held for WriteHold", took)
		}

		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		if err := l.ClearUncertainty(cancelled, first); err == nil {
			t.Fatal("a cancelled context did not stop the held clear")
		}
		if unc := l.Uncertainty(); unc == nil {
			t.Fatal("the cancelled clear removed the uncertainty")
		}

		// A cancellation while the hold is in flight releases it.
		l.WriteHold = time.Hour
		inflight, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- l.ClearUncertainty(inflight, first) }()
		synctest.Wait() // the clear is blocked in the hold
		stop()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("the in-flight clear = %v, want the cancellation", err)
		}
		if unc := l.Uncertainty(); unc == nil {
			t.Fatal("the cancelled in-flight clear removed the uncertainty")
		}
	})
}

func TestLeaserKeepsTheRecordsUncertainty(t *testing.T) {
	l := NewLeaser()
	ctx := context.Background()

	want := &catalog.Uncertainty{RunIDs: []string{"dead-run"}, Since: leaseBase}
	l.SetUncertainty(want)
	if got := l.Uncertainty(); got != want {
		t.Fatalf("Uncertainty = %+v, want the armed record", got)
	}
	first, err := l.Acquire(ctx, catalog.LeaseRequest{Holder: "run-a"})
	if err != nil {
		t.Fatal(err)
	}
	if first.Uncertainty != want {
		t.Fatalf("the acquisition did not inherit the uncertainty: %+v", first.Uncertainty)
	}
	extended := first
	extended.Uncertainty = &catalog.Uncertainty{RunIDs: []string{"dead-run", "run-a"}, Since: leaseBase}
	if err := l.MarkUncertainty(ctx, extended); err != nil {
		t.Fatal(err)
	}
	if got := l.Uncertainty(); got == nil || !slices.Equal(got.RunIDs, []string{"dead-run", "run-a"}) {
		t.Fatalf("a mark did not record the set as handed: %+v", got)
	}

	second, err := l.Acquire(ctx, catalog.LeaseRequest{Holder: "run-b", TakeoverToken: first.Token})
	if err != nil {
		t.Fatal(err)
	}
	if err := l.MarkUncertainty(cancelledContext(t), second); err == nil {
		t.Fatal("a cancelled context did not stop the mark")
	}
	staleMark := catalog.Lease{Token: "stale", Uncertainty: &catalog.Uncertainty{RunIDs: []string{"stale-run"}}}
	if err := l.MarkUncertainty(ctx, staleMark); err != nil {
		t.Fatalf("a stale mark = %v", err)
	}
	if got := l.Uncertainty(); got == nil || !slices.Equal(got.RunIDs, []string{"dead-run", "run-a"}) {
		t.Fatalf("a stale mark changed the record: %+v", got)
	}
	if err := l.ClearUncertainty(cancelledContext(t), second); err == nil {
		t.Fatal("a cancelled context did not stop the clear")
	}
	if err := l.ClearUncertainty(ctx, catalog.Lease{Token: "stale"}); err != nil {
		t.Fatalf("a stale clear = %v", err)
	}
	if got := l.Uncertainty(); got == nil || len(got.RunIDs) != 2 {
		t.Fatalf("a stale clear changed the record: %+v", got)
	}
	if err := l.ClearUncertainty(ctx, second); err != nil {
		t.Fatal(err)
	}
	if got := l.Uncertainty(); got != nil {
		t.Fatalf("the holder's clear left uncertainty %+v", got)
	}

	// A released record keeps the uncertainty its holder armed, and the free
	// record's own mark and clear are no-ops.
	second.Uncertainty = &catalog.Uncertainty{RunIDs: []string{"run-b"}, Since: leaseBase}
	if err := l.MarkUncertainty(ctx, second); err != nil {
		t.Fatal(err)
	}
	if err := l.Release(cancelledContext(t), second); err == nil {
		t.Fatal("a cancelled context did not stop the release")
	}
	if err := l.Release(ctx, second); err != nil {
		t.Fatal(err)
	}
	if got := l.Uncertainty(); got == nil || !slices.Equal(got.RunIDs, []string{"run-b"}) {
		t.Fatalf("Release dropped the holder's uncertainty: %+v", got)
	}
	if _, ok := l.Held(); ok {
		t.Fatal("a released claim is still held")
	}
	if err := l.MarkUncertainty(ctx, second); err != nil {
		t.Fatalf("a mark on a free record = %v", err)
	}
	if err := l.ClearUncertainty(ctx, second); err != nil {
		t.Fatalf("a clear on a free record = %v", err)
	}
	if got := l.Uncertainty(); got == nil {
		t.Fatal("a call on a free record dropped the uncertainty")
	}
}

// TestLeaserObservesTheSurveyDigest: the fake records a full survey's digest
// under the holder's token only — a cancelled call, a stale caller and a free
// record change nothing, no observation creates an uncertainty by itself, and a
// later observation replaces the earlier one without touching RunIDs or Since.
func TestLeaserObservesTheSurveyDigest(t *testing.T) {
	l := NewLeaser()
	ctx := context.Background()

	if err := l.ObserveUncertainty(cancelledContext(t), catalog.Lease{Token: "any"}, "digest"); err == nil {
		t.Fatal("a cancelled context did not stop the observation")
	}
	held, err := l.Acquire(ctx, catalog.LeaseRequest{Holder: "run-a"})
	if err != nil {
		t.Fatal(err)
	}
	if err := l.ObserveUncertainty(ctx, held, "premature"); err != nil {
		t.Fatalf("an observation without an uncertainty = %v", err)
	}
	if got := l.Uncertainty(); got != nil {
		t.Fatalf("an observation created an uncertainty: %+v", got)
	}
	pending := held
	pending.Uncertainty = &catalog.Uncertainty{RunIDs: []string{"run-a"}, Since: leaseBase}
	if err := l.MarkUncertainty(ctx, pending); err != nil {
		t.Fatal(err)
	}
	if err := l.ObserveUncertainty(ctx, catalog.Lease{Token: "stale-token"}, "stale-digest"); err != nil {
		t.Fatalf("a stale observation = %v", err)
	}
	if got := l.Uncertainty(); got == nil || got.Digest != "" {
		t.Fatalf("a stale observation recorded a digest: %+v", got)
	}
	if err := l.ObserveUncertainty(ctx, held, "digest-1"); err != nil {
		t.Fatal(err)
	}
	recorded := l.Uncertainty()
	if recorded == nil || recorded.Digest != "digest-1" || !slices.Equal(recorded.RunIDs, []string{"run-a"}) {
		t.Fatalf("the observation recorded %+v", recorded)
	}
	if err := l.ObserveUncertainty(ctx, held, "digest-2"); err != nil {
		t.Fatal(err)
	}
	if got := l.Uncertainty(); got.Digest != "digest-2" || !got.Since.Equal(recorded.Since) || !slices.Equal(got.RunIDs, []string{"run-a"}) {
		t.Fatalf("the second observation recorded %+v", got)
	}
	if err := l.Release(ctx, held); err != nil {
		t.Fatal(err)
	}
	if err := l.ObserveUncertainty(ctx, held, "late-digest"); err != nil {
		t.Fatalf("an observation on a free record = %v", err)
	}
	if got := l.Uncertainty(); got == nil || got.Digest != "digest-2" {
		t.Fatalf("a free record's observation changed it: %+v", got)
	}
}

// TestLeaserReleaseOnlyDropsTheClaimItCarriesTheTokenFor: a release by a run
// that no longer holds the claim is a no-op, and releasing an empty leaser is
// not an error.
func TestLeaserReleaseOnlyDropsTheClaimItCarriesTheTokenFor(t *testing.T) {
	l := NewLeaser()
	ctx := context.Background()

	if err := l.Release(ctx, catalog.Lease{Token: "nothing-held"}); err != nil {
		t.Fatalf("releasing an empty leaser = %v", err)
	}
	if l.Uncertainty() != nil {
		t.Fatal("an empty leaser carries uncertainty")
	}

	held, err := l.Acquire(ctx, catalog.LeaseRequest{Holder: "run-a"})
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Release(ctx, catalog.Lease{Token: "stale-token"}); err != nil {
		t.Fatalf("a stale release = %v", err)
	}
	if _, ok := l.Held(); !ok {
		t.Fatal("a stale release dropped the live claim")
	}
	if err := l.Release(ctx, held); err != nil {
		t.Fatalf("the holder's own release = %v", err)
	}
	if _, ok := l.Held(); ok {
		t.Fatal("the holder's own release did not drop the claim")
	}
}

// TestLeaserHolderStateAnswersTheConfiguredVerdict: the fake reports alive by
// default, the configured verdict otherwise, and an error or an unsupported
// leaser is what the protection lease must read as "alive".
func TestLeaserHolderStateAnswersTheConfiguredVerdict(t *testing.T) {
	l := NewLeaser()
	ctx := context.Background()
	lease := catalog.Lease{Holder: "run-a", HolderNamespace: "infrastructure", HolderPod: "backup-a", HolderUID: "uid-a"}

	terminated, definitive, err := l.HolderState(ctx, lease)
	if err != nil || terminated || definitive {
		t.Fatalf("HolderState = (%v, %v, %v), want the holder alive", terminated, definitive, err)
	}

	l.SetTerminated(true, true)
	if terminated, definitive, err = l.HolderState(ctx, lease); err != nil || !terminated || !definitive {
		t.Fatalf("HolderState = (%v, %v, %v), want the terminal phase", terminated, definitive, err)
	}

	l.SetHolderErr(errors.New("the pod read failed"))
	if terminated, definitive, err = l.HolderState(ctx, lease); err == nil || terminated || definitive {
		t.Fatalf("HolderState = (%v, %v, %v), want a failed read", terminated, definitive, err)
	}
	l.SetHolderErr(nil)

	l.HolderUnsupported = true
	if _, _, err = l.HolderState(ctx, lease); !errors.Is(err, errors.ErrUnsupported) {
		t.Fatalf("HolderState = %v, want ErrUnsupported", err)
	}
	l.HolderUnsupported = false

	if _, _, err = l.HolderState(cancelledContext(t), lease); err == nil {
		t.Fatal("a cancelled context did not stop the liveness check")
	}
}

// TestLeaserHoldArrangesAClaimToRespect: a held claim refuses without a token,
// the counters read back under the lock, and a takeover of it inherits whatever
// uncertainty the record already carries.
func TestLeaserHoldArrangesAClaimToRespect(t *testing.T) {
	l := NewLeaser()
	ctx := context.Background()

	l.SetUncertainty(&catalog.Uncertainty{RunIDs: []string{"earlier"}, Since: leaseBase})
	l.Hold("dead-run", model.RunKindScheduled, "infrastructure", "backup-a", "uid-a", leaseBase)
	held, ok := l.Held()
	if !ok {
		t.Fatal("Hold left no claim")
	}
	if held.Holder != "dead-run" || held.Kind != model.RunKindScheduled || held.HolderUID != "uid-a" || !held.AcquiredAt.Equal(leaseBase) {
		t.Fatalf("Hold arranged %+v", held)
	}
	if l.AcquireCount() != 0 {
		t.Fatalf("acquires = %d, want 0 before any acquisition", l.AcquireCount())
	}
	if _, err := l.Acquire(ctx, catalog.LeaseRequest{Holder: "run-b"}); !errors.Is(err, catalog.ErrLeaseHeld) {
		t.Fatalf("a held claim was handed over: %v", err)
	}
	if l.AcquireCount() != 1 {
		t.Fatalf("acquires = %d, want 1", l.AcquireCount())
	}
	taken, err := l.Acquire(ctx, catalog.LeaseRequest{Holder: "run-b", TakeoverToken: held.Token})
	if err != nil {
		t.Fatal(err)
	}
	if got := taken.Uncertainty; got == nil || !slices.Equal(got.RunIDs, []string{"earlier"}) {
		t.Fatalf("the armed uncertainty was not inherited: %+v", got)
	}
}
