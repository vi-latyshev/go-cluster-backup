package catalogtest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/catalog"
	"github.com/vi-latyshev/go-cluster-backup/internal/model"
)

var leaseBase = time.Date(2026, 10, 14, 3, 0, 0, 0, time.UTC)

// TestLeaserRefusesAHeldClaimAndTakesOverOnItsExactToken: the fake obeys the
// same rule the real capability does — a held claim refuses and names its
// holder, and only the exact takeover token hands it over.
func TestLeaserRefusesAHeldClaimAndTakesOverOnItsExactToken(t *testing.T) {
	l := NewLeaser()
	ctx := context.Background()

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := l.Acquire(cancelled, catalog.LeaseRequest{Holder: "run-a"}); err == nil {
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
	if current, ok := l.Held(); !ok || current.Token != taken.Token {
		t.Fatalf("Held = %+v, %v; want the new claim", current, ok)
	}
}

// TestLeaserQuarantineSurvivesTheRecord: a takeover's quarantine deadline is
// kept in the record across a release, and every later acquisition inherits the
// maximum of the request, the record and the claim in hand.
func TestLeaserQuarantineSurvivesTheRecord(t *testing.T) {
	l := NewLeaser()
	ctx := context.Background()
	early := leaseBase
	late := leaseBase.Add(time.Hour)

	first, err := l.Acquire(ctx, catalog.LeaseRequest{Holder: "run-a", QuarantineUntil: late})
	if err != nil {
		t.Fatal(err)
	}
	if !first.QuarantineUntil.Equal(late) {
		t.Fatalf("the claim records quarantine %s, want %s", first.QuarantineUntil, late)
	}
	if err := l.Release(ctx, first); err != nil {
		t.Fatal(err)
	}
	if until := l.QuarantineUntil(); !until.Equal(late) {
		t.Fatalf("Release dropped the quarantine: %s", until)
	}
	if _, ok := l.Held(); ok {
		t.Fatal("a released claim is still held")
	}

	// A released record still carries its deadline into the next acquisition.
	second, err := l.Acquire(ctx, catalog.LeaseRequest{Holder: "run-b", QuarantineUntil: early})
	if err != nil {
		t.Fatal(err)
	}
	if !second.QuarantineUntil.Equal(late) {
		t.Fatalf("the released quarantine was not inherited: %s", second.QuarantineUntil)
	}

	// A takeover by an earlier run inherits the deadline of the claim it took.
	l.SetQuarantine(early)
	third, err := l.Acquire(ctx, catalog.LeaseRequest{
		Holder: "run-c", TakeoverToken: second.Token, QuarantineUntil: early,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !third.QuarantineUntil.Equal(late) {
		t.Fatalf("the taken claim's quarantine was not inherited: %s", third.QuarantineUntil)
	}

	// A quarantine armed on the leaser outlives the request that meets it.
	l.SetQuarantine(late)
	fourth, err := l.Acquire(ctx, catalog.LeaseRequest{
		Holder: "run-d", TakeoverToken: third.Token, QuarantineUntil: early,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !fourth.QuarantineUntil.Equal(late) {
		t.Fatalf("the armed quarantine was not inherited: %s", fourth.QuarantineUntil)
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
	if until := l.QuarantineUntil(); !until.IsZero() {
		t.Fatalf("an empty leaser carries quarantine %s", until)
	}

	held, err := l.Acquire(ctx, catalog.LeaseRequest{Holder: "run-a", QuarantineUntil: leaseBase})
	if err != nil {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := l.Release(cancelled, held); err == nil {
		t.Fatal("a cancelled context did not stop the release")
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
	if until := l.QuarantineUntil(); !until.Equal(leaseBase) {
		t.Fatalf("Release dropped the quarantine: %s", until)
	}
}

// TestLeaserHolderStateAnswersTheConfiguredVerdict: the fake reports alive by
// default, the configured verdict otherwise, and an error or an unsupported
// leaser is what the engine must read as "alive".
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

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, err = l.HolderState(cancelled, lease); err == nil {
		t.Fatal("a cancelled context did not stop the liveness check")
	}
}

// TestLeaserHoldArrangesAClaimToRespect: a held claim refuses without a token,
// and the counters read back under the lock.
func TestLeaserHoldArrangesAClaimToRespect(t *testing.T) {
	l := NewLeaser()
	ctx := context.Background()

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
	if until := l.QuarantineUntil(); !until.IsZero() {
		t.Fatalf("an unarmed record carries quarantine %s", until)
	}
	l.SetQuarantine(leaseBase)
	taken, err := l.Acquire(ctx, catalog.LeaseRequest{Holder: "run-b", TakeoverToken: held.Token})
	if err != nil {
		t.Fatal(err)
	}
	if !taken.QuarantineUntil.Equal(leaseBase) {
		t.Fatalf("the armed quarantine was not inherited: %s", taken.QuarantineUntil)
	}
}
