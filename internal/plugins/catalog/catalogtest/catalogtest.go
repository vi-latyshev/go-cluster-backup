// Package catalogtest is the conformance suite every catalog implementation runs:
//
//	catalogtest.Run(t, func(t *testing.T) catalog.Catalog { return newTestCatalog(t) })
//
// newFn must return a fresh, empty catalog for every call. The suite covers the
// writer-lease protocol only — acquisition, takeover, release and the durable
// uncertainty that outlives a holder: its pending-run set, which keeps every
// holder whose asynchronous writes may still settle, and its survey digest. A
// catalog that implements catalog.UncertaintyObserver is also held to the
// digest-observation rules; one that does not is still conforming, because the
// capability is optional.
package catalogtest

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/catalog"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances/plugintest"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
)

type Check struct {
	Name string
	Run  func(t *testing.T, c catalog.Catalog) error
}

var Checks = []Check{
	{"acquire takes a free claim and refuses a held one", checkAcquire},
	{"a takeover needs the exact token", checkTakeover},
	{"release drops only its own claim", checkRelease},
	{"uncertainty survives a release and only its holder clears it", checkUncertainty},
	{"the pending set carries every holder until a settlement clears it", checkPendingSet},
	{"a takeover arms uncertainty the old holder cannot touch", checkStaleHolder},
	{"the observer records a survey digest under the holder's token", checkObserveDigest},
	{"returns on cancellation", checkCancel},
}

func Run(t *testing.T, newFn func(t *testing.T) catalog.Catalog) {
	t.Helper()
	for _, c := range Checks {
		t.Run(c.Name, func(t *testing.T) {
			if err := c.Run(t, newFn(t)); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func checkAcquire(t *testing.T, c catalog.Catalog) error {
	ctx := context.Background()
	first, err := c.Acquire(ctx, catalog.LeaseRequest{
		Holder: "run-a", Kind: model.RunKindManual,
		HolderNamespace: "infrastructure", HolderPod: "backup-a", HolderUID: "uid-a",
	})
	if err != nil {
		return fmt.Errorf("Acquire: %w", err)
	}
	if first.Token == "" || first.Holder != "run-a" || first.Kind != model.RunKindManual {
		return fmt.Errorf("the claim does not record its holder: %+v", first)
	}
	if first.HolderPod != "backup-a" || first.HolderUID != "uid-a" || first.HolderNamespace != "infrastructure" {
		return fmt.Errorf("the claim does not record the process identity: %+v", first)
	}
	if first.AcquiredAt.IsZero() {
		return fmt.Errorf("the claim records no acquisition time")
	}
	held, err := c.Acquire(ctx, catalog.LeaseRequest{Holder: "run-b", Kind: model.RunKindScheduled})
	if !errors.Is(err, catalog.ErrLeaseHeld) {
		return fmt.Errorf("a held claim was handed over: %w", err)
	}
	if held.Holder != "run-a" || held.Token != first.Token {
		return fmt.Errorf("the refusal does not name the holder: %+v", held)
	}
	return nil
}

func checkTakeover(t *testing.T, c catalog.Catalog) error {
	ctx := context.Background()
	first, err := c.Acquire(ctx, catalog.LeaseRequest{Holder: "run-a", Kind: model.RunKindScheduled})
	if err != nil {
		return fmt.Errorf("Acquire: %w", err)
	}
	if _, err := c.Acquire(ctx, catalog.LeaseRequest{Holder: "run-b", TakeoverToken: "not-the-token"}); !errors.Is(err, catalog.ErrLeaseHeld) {
		return fmt.Errorf("a foreign takeover token was accepted: %w", err)
	}
	taken, err := c.Acquire(ctx, catalog.LeaseRequest{
		Holder: "run-b", Kind: model.RunKindScheduled, TakeoverToken: first.Token,
	})
	if err != nil {
		return fmt.Errorf("a proven-dead claim was not taken over: %w", err)
	}
	if taken.Holder != "run-b" || taken.Token == first.Token {
		return fmt.Errorf("takeover returned %+v", taken)
	}
	return nil
}

func checkRelease(t *testing.T, c catalog.Catalog) error {
	ctx := context.Background()
	held, err := c.Acquire(ctx, catalog.LeaseRequest{Holder: "run-a"})
	if err != nil {
		return fmt.Errorf("Acquire: %w", err)
	}
	if err := c.Release(ctx, catalog.Lease{Token: "stale-token"}); err != nil {
		return fmt.Errorf("a stale release = %w", err)
	}
	if _, err := c.Acquire(ctx, catalog.LeaseRequest{Holder: "run-b"}); !errors.Is(err, catalog.ErrLeaseHeld) {
		return fmt.Errorf("a stale release dropped the live claim: %w", err)
	}
	if err := c.Release(ctx, held); err != nil {
		return fmt.Errorf("the holder's own release = %w", err)
	}
	if _, err := c.Acquire(ctx, catalog.LeaseRequest{Holder: "run-b"}); err != nil {
		return fmt.Errorf("the released claim was not free: %w", err)
	}
	return nil
}

func checkUncertainty(t *testing.T, c catalog.Catalog) error {
	ctx := context.Background()
	first, err := c.Acquire(ctx, catalog.LeaseRequest{Holder: "run-a"})
	if err != nil {
		return fmt.Errorf("Acquire: %w", err)
	}
	pending := first
	pending.Uncertainty = &catalog.Uncertainty{RunIDs: []string{"run-a"}, Since: time.Now().UTC()}
	if err := c.MarkUncertainty(ctx, pending); err != nil {
		return fmt.Errorf("MarkUncertainty: %w", err)
	}
	if err := c.MarkUncertainty(ctx, pending); err != nil {
		return fmt.Errorf("the second MarkUncertainty: %w", err)
	}
	if err := c.Release(ctx, first); err != nil {
		return fmt.Errorf("Release: %w", err)
	}
	second, err := c.Acquire(ctx, catalog.LeaseRequest{Holder: "run-b"})
	if err != nil {
		return fmt.Errorf("Acquire after a release: %w", err)
	}
	if second.Uncertainty == nil || len(second.Uncertainty.RunIDs) != 1 || second.Uncertainty.RunIDs[0] != "run-a" {
		return fmt.Errorf("the released record's uncertainty was not inherited: %+v", second.Uncertainty)
	}
	if err := c.ClearUncertainty(ctx, second); err != nil {
		return fmt.Errorf("ClearUncertainty: %w", err)
	}
	if err := c.Release(ctx, second); err != nil {
		return fmt.Errorf("Release: %w", err)
	}
	third, err := c.Acquire(ctx, catalog.LeaseRequest{Holder: "run-c"})
	if err != nil {
		return fmt.Errorf("Acquire after a settled release: %w", err)
	}
	if third.Uncertainty != nil {
		return fmt.Errorf("a settled record still carries uncertainty %+v", third.Uncertainty)
	}
	return nil
}

// checkPendingSet: the durable pending-run set keeps every holder whose
// asynchronous writes may still settle. A takeover inherits the predecessor's
// id, the successor hands back a set extended with its own id, a stale holder
// cannot rewrite it, and only a settlement clears the whole set.
func checkPendingSet(t *testing.T, c catalog.Catalog) error {
	ctx := context.Background()
	first, err := c.Acquire(ctx, catalog.LeaseRequest{Holder: "run-a"})
	if err != nil {
		return fmt.Errorf("Acquire: %w", err)
	}
	pendingA := first
	pendingA.Uncertainty = &catalog.Uncertainty{RunIDs: []string{"run-a"}, Since: time.Now().UTC()}
	if err := c.MarkUncertainty(ctx, pendingA); err != nil {
		return fmt.Errorf("MarkUncertainty: %w", err)
	}
	second, err := c.Acquire(ctx, catalog.LeaseRequest{Holder: "run-b", TakeoverToken: first.Token})
	if err != nil {
		return fmt.Errorf("takeover: %w", err)
	}
	if second.Uncertainty == nil || len(second.Uncertainty.RunIDs) != 1 || second.Uncertainty.RunIDs[0] != "run-a" {
		return fmt.Errorf("the takeover did not keep the pending set: %+v", second.Uncertainty)
	}
	pendingAB := second
	pendingAB.Uncertainty = &catalog.Uncertainty{RunIDs: []string{"run-a", "run-b"}, Since: second.Uncertainty.Since}
	if err := c.MarkUncertainty(ctx, pendingAB); err != nil {
		return fmt.Errorf("the successor's MarkUncertainty: %w", err)
	}
	stale := first
	stale.Uncertainty = &catalog.Uncertainty{RunIDs: []string{"run-a"}, Since: time.Now().UTC()}
	if err := c.MarkUncertainty(ctx, stale); err != nil {
		return fmt.Errorf("a stale MarkUncertainty = %w", err)
	}
	if err := c.Release(ctx, second); err != nil {
		return fmt.Errorf("Release: %w", err)
	}
	third, err := c.Acquire(ctx, catalog.LeaseRequest{Holder: "run-c"})
	if err != nil {
		return fmt.Errorf("Acquire after a release: %w", err)
	}
	if third.Uncertainty == nil || len(third.Uncertainty.RunIDs) != 2 ||
		third.Uncertainty.RunIDs[0] != "run-a" || third.Uncertainty.RunIDs[1] != "run-b" {
		return fmt.Errorf("the released record's pending set = %+v, want every holder", third.Uncertainty)
	}
	if err := c.ClearUncertainty(ctx, third); err != nil {
		return fmt.Errorf("ClearUncertainty: %w", err)
	}
	if err := c.Release(ctx, third); err != nil {
		return fmt.Errorf("Release: %w", err)
	}
	settled, err := c.Acquire(ctx, catalog.LeaseRequest{Holder: "run-d"})
	if err != nil {
		return fmt.Errorf("Acquire after a settled release: %w", err)
	}
	if settled.Uncertainty != nil {
		return fmt.Errorf("a cleared record still carries %+v", settled.Uncertainty)
	}
	return nil
}

func checkStaleHolder(t *testing.T, c catalog.Catalog) error {
	ctx := context.Background()
	first, err := c.Acquire(ctx, catalog.LeaseRequest{Holder: "run-a"})
	if err != nil {
		return fmt.Errorf("Acquire: %w", err)
	}
	taken, err := c.Acquire(ctx, catalog.LeaseRequest{Holder: "run-b", TakeoverToken: first.Token})
	if err != nil {
		return fmt.Errorf("takeover: %w", err)
	}
	if taken.Uncertainty == nil || len(taken.Uncertainty.RunIDs) != 1 || taken.Uncertainty.RunIDs[0] != "run-a" {
		return fmt.Errorf("a takeover armed no uncertainty for the holder it replaced: %+v", taken.Uncertainty)
	}
	stale := first
	stale.Uncertainty = &catalog.Uncertainty{RunIDs: []string{"run-a"}, Since: time.Now().UTC()}
	if err := c.MarkUncertainty(ctx, stale); err != nil {
		return fmt.Errorf("a stale MarkUncertainty = %w", err)
	}
	if err := c.ClearUncertainty(ctx, first); err != nil {
		return fmt.Errorf("a stale ClearUncertainty = %w", err)
	}
	if err := c.Release(ctx, first); err != nil {
		return fmt.Errorf("a stale Release = %w", err)
	}
	if _, err := c.Acquire(ctx, catalog.LeaseRequest{Holder: "run-c"}); !errors.Is(err, catalog.ErrLeaseHeld) {
		return fmt.Errorf("a stale release dropped the successor's claim: %w", err)
	}
	if err := c.Release(ctx, taken); err != nil {
		return fmt.Errorf("the successor's release = %w", err)
	}
	next, err := c.Acquire(ctx, catalog.LeaseRequest{Holder: "run-d"})
	if err != nil {
		return fmt.Errorf("Acquire after the successor released: %w", err)
	}
	if next.Uncertainty == nil || len(next.Uncertainty.RunIDs) != 1 || next.Uncertainty.RunIDs[0] != "run-a" {
		return fmt.Errorf("the old holder's calls changed the successor's uncertainty: %+v", next.Uncertainty)
	}
	return nil
}

func checkCancel(t *testing.T, c catalog.Catalog) error {
	lease := catalog.Lease{Token: "any"}
	type op struct {
		name string
		fn   func(ctx context.Context) error
	}
	ops := []op{
		{"Acquire", func(ctx context.Context) error {
			_, err := c.Acquire(ctx, catalog.LeaseRequest{Holder: "run-a"})
			return err
		}},
		{"MarkUncertainty", func(ctx context.Context) error { return c.MarkUncertainty(ctx, lease) }},
		{"ClearUncertainty", func(ctx context.Context) error { return c.ClearUncertainty(ctx, lease) }},
		{"Release", func(ctx context.Context) error { return c.Release(ctx, lease) }},
	}
	if observer, ok := c.(catalog.UncertaintyObserver); ok {
		ops = append(ops, op{"ObserveUncertainty", func(ctx context.Context) error {
			return observer.ObserveUncertainty(ctx, lease, "digest")
		}})
	}
	for _, o := range ops {
		if err := plugintest.MustReturnWithError(t, 5*time.Second, o.name, o.fn); err != nil {
			return err
		}
	}
	return nil
}

// checkObserveDigest: a catalog with the observation capability records a full
// survey's digest in the record's uncertainty, compare-and-swapped on the
// claim's token — a stale caller changes nothing, a release keeps the digest, a
// takeover inherits it and a clear removes it with the uncertainty.
func checkObserveDigest(t *testing.T, c catalog.Catalog) error {
	observer, ok := c.(catalog.UncertaintyObserver)
	if !ok {
		// Observation is optional: without it the uncertainty simply never
		// settles, which is conservative, not a conformance failure.
		return nil
	}
	ctx := context.Background()
	first, err := c.Acquire(ctx, catalog.LeaseRequest{Holder: "run-a"})
	if err != nil {
		return fmt.Errorf("Acquire: %w", err)
	}
	if err := observer.ObserveUncertainty(ctx, first, "premature"); err != nil {
		return fmt.Errorf("ObserveUncertainty without an uncertainty = %w", err)
	}
	pending := first
	pending.Uncertainty = &catalog.Uncertainty{RunIDs: []string{"run-a"}, Since: time.Now().UTC()}
	if err := c.MarkUncertainty(ctx, pending); err != nil {
		return fmt.Errorf("MarkUncertainty: %w", err)
	}
	if err := observer.ObserveUncertainty(ctx, first, "digest-1"); err != nil {
		return fmt.Errorf("ObserveUncertainty: %w", err)
	}
	if err := observer.ObserveUncertainty(ctx, catalog.Lease{Token: "stale-token"}, "stale-digest"); err != nil {
		return fmt.Errorf("a stale ObserveUncertainty = %w", err)
	}
	second, err := c.Acquire(ctx, catalog.LeaseRequest{Holder: "run-b", TakeoverToken: first.Token})
	if err != nil {
		return fmt.Errorf("takeover: %w", err)
	}
	if second.Uncertainty == nil || len(second.Uncertainty.RunIDs) != 1 || second.Uncertainty.RunIDs[0] != "run-a" || second.Uncertainty.Digest != "digest-1" {
		return fmt.Errorf("the takeover did not inherit the observed digest: %+v", second.Uncertainty)
	}
	if err := observer.ObserveUncertainty(ctx, second, "digest-2"); err != nil {
		return fmt.Errorf("the successor's ObserveUncertainty = %w", err)
	}
	if err := c.Release(ctx, second); err != nil {
		return fmt.Errorf("Release: %w", err)
	}
	third, err := c.Acquire(ctx, catalog.LeaseRequest{Holder: "run-c"})
	if err != nil {
		return fmt.Errorf("Acquire after a release: %w", err)
	}
	if third.Uncertainty == nil || third.Uncertainty.Digest != "digest-2" {
		return fmt.Errorf("the released record did not keep the last observation: %+v", third.Uncertainty)
	}
	if err := c.ClearUncertainty(ctx, third); err != nil {
		return fmt.Errorf("ClearUncertainty: %w", err)
	}
	if err := c.Release(ctx, third); err != nil {
		return fmt.Errorf("Release: %w", err)
	}
	settled, err := c.Acquire(ctx, catalog.LeaseRequest{Holder: "run-d"})
	if err != nil {
		return fmt.Errorf("Acquire after a settled release: %w", err)
	}
	if settled.Uncertainty != nil {
		return fmt.Errorf("a cleared record still carries a digest: %+v", settled.Uncertainty)
	}
	return nil
}
