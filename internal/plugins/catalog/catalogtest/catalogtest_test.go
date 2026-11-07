package catalogtest

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/catalog"
)

// openDoor hands a held claim to anyone: the refusal check must catch it.
type openDoor struct{ *Leaser }

func (o openDoor) Acquire(ctx context.Context, req catalog.LeaseRequest) (catalog.Lease, error) {
	o.mu.Lock()
	o.record.Holder = ""
	o.mu.Unlock()
	return o.Leaser.Acquire(ctx, req)
}

// blindRefusal refuses a held claim without naming the holder.
type blindRefusal struct{ *Leaser }

func (b blindRefusal) Acquire(ctx context.Context, req catalog.LeaseRequest) (catalog.Lease, error) {
	held, err := b.Leaser.Acquire(ctx, req)
	if errors.Is(err, catalog.ErrLeaseHeld) {
		return catalog.Lease{}, err
	}
	return held, err
}

// tokenless takes the claim without a token, so no later call can prove it is
// the holder.
type tokenless struct{ *Leaser }

func (t tokenless) Acquire(ctx context.Context, req catalog.LeaseRequest) (catalog.Lease, error) {
	held, err := t.Leaser.Acquire(ctx, req)
	held.Token = ""
	return held, err
}

// anonymized takes the claim without the process identity a liveness check
// needs.
type anonymized struct{ *Leaser }

func (a anonymized) Acquire(ctx context.Context, req catalog.LeaseRequest) (catalog.Lease, error) {
	held, err := a.Leaser.Acquire(ctx, req)
	held.HolderNamespace, held.HolderPod, held.HolderUID = "", "", ""
	return held, err
}

// undated takes the claim without an acquisition time.
type undated struct{ *Leaser }

func (u undated) Acquire(ctx context.Context, req catalog.LeaseRequest) (catalog.Lease, error) {
	held, err := u.Leaser.Acquire(ctx, req)
	held.AcquiredAt = time.Time{}
	return held, err
}

// noTakeover refuses every takeover, however exact its token.
type noTakeover struct{ *Leaser }

func (n noTakeover) Acquire(ctx context.Context, req catalog.LeaseRequest) (catalog.Lease, error) {
	if req.TakeoverToken != "" {
		n.mu.Lock()
		held := n.record
		n.mu.Unlock()
		return held, fmt.Errorf("never hands over: %w", catalog.ErrLeaseHeld)
	}
	return n.Leaser.Acquire(ctx, req)
}

// sameToken takes a claim over but keeps the old token, so a stale release
// would still match.
type sameToken struct{ *Leaser }

func (s sameToken) Acquire(ctx context.Context, req catalog.LeaseRequest) (catalog.Lease, error) {
	held, err := s.Leaser.Acquire(ctx, req)
	if err != nil || req.TakeoverToken == "" {
		return held, err
	}
	held.Token = req.TakeoverToken
	return held, nil
}

// noArm takes a claim over without recording that the replaced holder may still
// have provider operations in flight.
type noArm struct{ *Leaser }

func (n noArm) Acquire(ctx context.Context, req catalog.LeaseRequest) (catalog.Lease, error) {
	held, err := n.Leaser.Acquire(ctx, req)
	if err != nil || req.TakeoverToken == "" {
		return held, err
	}
	n.mu.Lock()
	n.record.Uncertainty = nil
	n.mu.Unlock()
	held.Uncertainty = nil
	return held, nil
}

// failingAcquire breaks every acquisition from the given call on.
type failingAcquire struct {
	*Leaser
	failFrom int
	calls    int
}

func (f *failingAcquire) Acquire(ctx context.Context, req catalog.LeaseRequest) (catalog.Lease, error) {
	f.calls++
	if f.calls >= f.failFrom {
		return catalog.Lease{}, errors.New("acquire failed")
	}
	return f.Leaser.Acquire(ctx, req)
}

// failingRelease breaks the release with the given one-based call number.
type failingRelease struct {
	*Leaser
	failOn int
	calls  int
}

func (f *failingRelease) Release(ctx context.Context, lease catalog.Lease) error {
	f.calls++
	if f.calls == f.failOn {
		return errors.New("release failed")
	}
	return f.Leaser.Release(ctx, lease)
}

// dropAny releases any claim, whoever holds it.
type dropAny struct{ *Leaser }

func (d dropAny) Release(ctx context.Context, lease catalog.Lease) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.record = catalog.Lease{Uncertainty: d.record.Uncertainty}
	return nil
}

// stuckRelease never drops the claim.
type stuckRelease struct{ *Leaser }

func (s stuckRelease) Release(context.Context, catalog.Lease) error { return nil }

// clearingRelease drops the claim together with the record's uncertainty.
type clearingRelease struct{ *Leaser }

func (c clearingRelease) Release(ctx context.Context, lease catalog.Lease) error {
	c.mu.Lock()
	c.record.Uncertainty = nil
	c.mu.Unlock()
	return c.Leaser.Release(ctx, lease)
}

// failingMark breaks the uncertainty mark with the given one-based call number.
type failingMark struct {
	*Leaser
	failOn int
	calls  int
}

func (f *failingMark) MarkUncertainty(ctx context.Context, lease catalog.Lease) error {
	f.calls++
	if f.calls == f.failOn {
		return errors.New("mark failed")
	}
	return f.Leaser.MarkUncertainty(ctx, lease)
}

// forgetfulMark reports success and records nothing.
type forgetfulMark struct{ *Leaser }

func (f forgetfulMark) MarkUncertainty(context.Context, catalog.Lease) error { return nil }

// holderOnlyMark records only the caller's own run id, dropping the inherited
// prefixes whose writes may still settle.
type holderOnlyMark struct{ *Leaser }

func (h holderOnlyMark) MarkUncertainty(ctx context.Context, lease catalog.Lease) error {
	if lease.Uncertainty == nil {
		return nil
	}
	lease.Uncertainty = &catalog.Uncertainty{RunIDs: []string{lease.Holder}, Since: lease.Uncertainty.Since}
	return h.Leaser.MarkUncertainty(ctx, lease)
}

// anyTokenMark records the handed set for whoever asks, so a stale holder
// rewrites a successor's pending set.
type anyTokenMark struct{ *Leaser }

func (a anyTokenMark) MarkUncertainty(ctx context.Context, lease catalog.Lease) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if lease.Uncertainty == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	next := *lease.Uncertainty
	next.RunIDs = append([]string(nil), lease.Uncertainty.RunIDs...)
	a.record.Uncertainty = &next
	return nil
}

// failingClear breaks the uncertainty clear.
type failingClear struct{ *Leaser }

func (f failingClear) ClearUncertainty(context.Context, catalog.Lease) error {
	return errors.New("clear failed")
}

// stickyClear reports success and clears nothing.
type stickyClear struct{ *Leaser }

func (s stickyClear) ClearUncertainty(context.Context, catalog.Lease) error { return nil }

// clearAny clears the record's uncertainty for any caller, holder or not.
type clearAny struct{ *Leaser }

func (c clearAny) ClearUncertainty(ctx context.Context, lease catalog.Lease) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.record.Uncertainty = nil
	return nil
}

// ctxIgnoring lies about success on a cancelled context.
type ctxIgnoring struct{ *Leaser }

func (ctxIgnoring) Acquire(context.Context, catalog.LeaseRequest) (catalog.Lease, error) {
	return catalog.Lease{}, nil
}

func (ctxIgnoring) MarkUncertainty(context.Context, catalog.Lease) error { return nil }

func (ctxIgnoring) ClearUncertainty(context.Context, catalog.Lease) error { return nil }

func (ctxIgnoring) Release(context.Context, catalog.Lease) error { return nil }

// ctxIgnoringObserver respects the context everywhere but the digest
// observation.
type ctxIgnoringObserver struct{ *Leaser }

func (ctxIgnoringObserver) ObserveUncertainty(context.Context, catalog.Lease, string) error {
	return nil
}

// failingObserve breaks the digest observation with the given one-based call
// number.
type failingObserve struct {
	*Leaser
	failOn int
	calls  int
}

func (f *failingObserve) ObserveUncertainty(ctx context.Context, lease catalog.Lease, digest string) error {
	f.calls++
	if f.calls == f.failOn {
		return errors.New("observe failed")
	}
	return f.Leaser.ObserveUncertainty(ctx, lease, digest)
}

// forgetfulObserve reports success and records no digest.
type forgetfulObserve struct{ *Leaser }

func (forgetfulObserve) ObserveUncertainty(context.Context, catalog.Lease, string) error { return nil }

// eagerObserve records the digest whoever asks, so a stale holder overwrites a
// successor's observation.
type eagerObserve struct{ *Leaser }

func (e eagerObserve) ObserveUncertainty(_ context.Context, _ catalog.Lease, digest string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.record.Uncertainty != nil {
		e.record.Uncertainty.Digest = digest
	}
	return nil
}

// digestDroppingRelease drops the claim together with the record's digest, so
// the proof a successor inherited never compares.
type digestDroppingRelease struct{ *Leaser }

func (d digestDroppingRelease) Release(ctx context.Context, lease catalog.Lease) error {
	d.mu.Lock()
	if d.record.Uncertainty != nil {
		d.record.Uncertainty.Digest = ""
	}
	d.mu.Unlock()
	return d.Leaser.Release(ctx, lease)
}

// withoutObserver hides the optional capabilities behind the bare lease seam.
type withoutObserver struct{ catalog.Catalog }

func TestLeaserPassesTheSuite(t *testing.T) {
	Run(t, func(*testing.T) catalog.Catalog { return NewLeaser() })
	t.Run("without the optional capabilities", func(t *testing.T) {
		Run(t, func(*testing.T) catalog.Catalog { return withoutObserver{NewLeaser()} })
	})
}

// TestC7_ThePendingSetKeepsEveryHolder: the durable record names every holder
// whose asynchronous writes may still settle — a successor inherits the
// predecessor's id, hands back a set extended with its own id, a stale holder
// cannot rewrite it, and only a settlement clears the whole set.
func TestC7_ThePendingSetKeepsEveryHolder(t *testing.T) {
	l := NewLeaser()
	ctx := context.Background()

	first, err := l.Acquire(ctx, catalog.LeaseRequest{Holder: "run-a"})
	if err != nil {
		t.Fatal(err)
	}
	pendingA := first
	pendingA.Uncertainty = &catalog.Uncertainty{RunIDs: []string{"run-a"}, Since: leaseBase}
	if err := l.MarkUncertainty(ctx, pendingA); err != nil {
		t.Fatal(err)
	}
	taken, err := l.Acquire(ctx, catalog.LeaseRequest{Holder: "run-b", TakeoverToken: first.Token})
	if err != nil {
		t.Fatal(err)
	}
	if taken.Uncertainty == nil || !slices.Equal(taken.Uncertainty.RunIDs, []string{"run-a"}) {
		t.Fatalf("the takeover inherited %+v", taken.Uncertainty)
	}
	pendingAB := taken
	pendingAB.Uncertainty = &catalog.Uncertainty{RunIDs: []string{"run-a", "run-b"}, Since: taken.Uncertainty.Since}
	if err := l.MarkUncertainty(ctx, pendingAB); err != nil {
		t.Fatal(err)
	}
	stale := first
	stale.Uncertainty = &catalog.Uncertainty{RunIDs: []string{"run-a"}, Since: leaseBase}
	if err := l.MarkUncertainty(ctx, stale); err != nil {
		t.Fatalf("a stale mark = %v", err)
	}
	if got := l.Uncertainty(); got == nil || !slices.Equal(got.RunIDs, []string{"run-a", "run-b"}) {
		t.Fatalf("a stale holder rewrote the pending set: %+v", got)
	}
	if err := l.Release(ctx, taken); err != nil {
		t.Fatal(err)
	}
	next, err := l.Acquire(ctx, catalog.LeaseRequest{Holder: "run-c"})
	if err != nil {
		t.Fatal(err)
	}
	if next.Uncertainty == nil || !slices.Equal(next.Uncertainty.RunIDs, []string{"run-a", "run-b"}) {
		t.Fatalf("the released pending set = %+v, want every holder", next.Uncertainty)
	}
	if err := l.ClearUncertainty(ctx, next); err != nil {
		t.Fatal(err)
	}
	if got := l.Uncertainty(); got != nil {
		t.Fatalf("the cleared record carries %+v", got)
	}
}

// TestChecksCatchBrokenCatalogs: the lease rules a check states are enforced —
// a catalog that hands a claim over, ignores a token or drops the record's
// uncertainty is reported, not passed.
func TestChecksCatchBrokenCatalogs(t *testing.T) {
	cases := []struct {
		check string
		cat   catalog.Catalog
	}{
		{"acquire takes a free claim and refuses a held one", openDoor{NewLeaser()}},
		{"a takeover needs the exact token", openDoor{NewLeaser()}},
		{"a takeover needs the exact token", noTakeover{NewLeaser()}},
		{"a takeover needs the exact token", sameToken{NewLeaser()}},
		{"release drops only its own claim", dropAny{NewLeaser()}},
		{"release drops only its own claim", stuckRelease{NewLeaser()}},
		{"uncertainty survives a release and only its holder clears it", forgetfulMark{NewLeaser()}},
		{"uncertainty survives a release and only its holder clears it", clearingRelease{NewLeaser()}},
		{"uncertainty survives a release and only its holder clears it", stickyClear{NewLeaser()}},
		{"the pending set carries every holder until a settlement clears it", noArm{NewLeaser()}},
		{"the pending set carries every holder until a settlement clears it", anyTokenMark{NewLeaser()}},
		{"a takeover arms uncertainty the old holder cannot touch", noArm{NewLeaser()}},
		{"a takeover arms uncertainty the old holder cannot touch", dropAny{NewLeaser()}},
		{"a takeover arms uncertainty the old holder cannot touch", clearAny{NewLeaser()}},
		{"the observer records a survey digest under the holder's token", eagerObserve{NewLeaser()}},
		{"the observer records a survey digest under the holder's token", digestDroppingRelease{NewLeaser()}},
		{"returns on cancellation", ctxIgnoring{NewLeaser()}},
		{"returns on cancellation", ctxIgnoringObserver{NewLeaser()}},
	}
	for _, tc := range cases {
		t.Run(tc.check, func(t *testing.T) {
			for _, c := range Checks {
				if c.Name != tc.check {
					continue
				}
				if err := c.Run(t, tc.cat); err == nil {
					t.Fatalf("check %q passed a broken catalog", c.Name)
				}
				return
			}
			t.Fatalf("check %q not found", tc.check)
		})
	}
}

// TestChecksReportEveryBrokenOperation: every error branch a check carries is
// reached by a catalog that breaks in exactly that way, so a broken catalog is
// reported rather than passing silently.
func TestChecksReportEveryBrokenOperation(t *testing.T) {
	cases := []struct {
		check string
		cat   catalog.Catalog
	}{
		{"acquire takes a free claim and refuses a held one", &failingAcquire{Leaser: NewLeaser(), failFrom: 1}},
		{"acquire takes a free claim and refuses a held one", tokenless{NewLeaser()}},
		{"acquire takes a free claim and refuses a held one", anonymized{NewLeaser()}},
		{"acquire takes a free claim and refuses a held one", undated{NewLeaser()}},
		{"acquire takes a free claim and refuses a held one", blindRefusal{NewLeaser()}},
		{"a takeover needs the exact token", &failingAcquire{Leaser: NewLeaser(), failFrom: 1}},
		{"release drops only its own claim", &failingAcquire{Leaser: NewLeaser(), failFrom: 1}},
		{"release drops only its own claim", &failingAcquire{Leaser: NewLeaser(), failFrom: 3}},
		{"release drops only its own claim", &failingRelease{Leaser: NewLeaser(), failOn: 1}},
		{"release drops only its own claim", &failingRelease{Leaser: NewLeaser(), failOn: 2}},
		{"uncertainty survives a release and only its holder clears it", &failingAcquire{Leaser: NewLeaser(), failFrom: 1}},
		{"uncertainty survives a release and only its holder clears it", &failingAcquire{Leaser: NewLeaser(), failFrom: 2}},
		{"uncertainty survives a release and only its holder clears it", &failingAcquire{Leaser: NewLeaser(), failFrom: 3}},
		{"uncertainty survives a release and only its holder clears it", &failingMark{Leaser: NewLeaser(), failOn: 1}},
		{"uncertainty survives a release and only its holder clears it", &failingMark{Leaser: NewLeaser(), failOn: 2}},
		{"uncertainty survives a release and only its holder clears it", &failingRelease{Leaser: NewLeaser(), failOn: 1}},
		{"uncertainty survives a release and only its holder clears it", &failingRelease{Leaser: NewLeaser(), failOn: 2}},
		{"uncertainty survives a release and only its holder clears it", failingClear{NewLeaser()}},
		{"the pending set carries every holder until a settlement clears it", &failingAcquire{Leaser: NewLeaser(), failFrom: 1}},
		{"the pending set carries every holder until a settlement clears it", &failingMark{Leaser: NewLeaser(), failOn: 1}},
		{"the pending set carries every holder until a settlement clears it", &failingAcquire{Leaser: NewLeaser(), failFrom: 2}},
		{"the pending set carries every holder until a settlement clears it", noArm{NewLeaser()}},
		{"the pending set carries every holder until a settlement clears it", &failingMark{Leaser: NewLeaser(), failOn: 2}},
		{"the pending set carries every holder until a settlement clears it", &failingMark{Leaser: NewLeaser(), failOn: 3}},
		{"the pending set carries every holder until a settlement clears it", &failingRelease{Leaser: NewLeaser(), failOn: 1}},
		{"the pending set carries every holder until a settlement clears it", &failingAcquire{Leaser: NewLeaser(), failFrom: 3}},
		{"the pending set carries every holder until a settlement clears it", holderOnlyMark{NewLeaser()}},
		{"the pending set carries every holder until a settlement clears it", failingClear{NewLeaser()}},
		{"the pending set carries every holder until a settlement clears it", &failingRelease{Leaser: NewLeaser(), failOn: 2}},
		{"the pending set carries every holder until a settlement clears it", &failingAcquire{Leaser: NewLeaser(), failFrom: 4}},
		{"the pending set carries every holder until a settlement clears it", stickyClear{NewLeaser()}},
		{"a takeover arms uncertainty the old holder cannot touch", &failingAcquire{Leaser: NewLeaser(), failFrom: 1}},
		{"a takeover arms uncertainty the old holder cannot touch", &failingAcquire{Leaser: NewLeaser(), failFrom: 2}},
		{"a takeover arms uncertainty the old holder cannot touch", &failingAcquire{Leaser: NewLeaser(), failFrom: 4}},
		{"a takeover arms uncertainty the old holder cannot touch", &failingMark{Leaser: NewLeaser(), failOn: 1}},
		{"a takeover arms uncertainty the old holder cannot touch", failingClear{NewLeaser()}},
		{"a takeover arms uncertainty the old holder cannot touch", &failingRelease{Leaser: NewLeaser(), failOn: 1}},
		{"a takeover arms uncertainty the old holder cannot touch", &failingRelease{Leaser: NewLeaser(), failOn: 2}},
		{"the observer records a survey digest under the holder's token", &failingAcquire{Leaser: NewLeaser(), failFrom: 1}},
		{"the observer records a survey digest under the holder's token", &failingObserve{Leaser: NewLeaser(), failOn: 1}},
		{"the observer records a survey digest under the holder's token", &failingMark{Leaser: NewLeaser(), failOn: 1}},
		{"the observer records a survey digest under the holder's token", &failingObserve{Leaser: NewLeaser(), failOn: 2}},
		{"the observer records a survey digest under the holder's token", &failingObserve{Leaser: NewLeaser(), failOn: 3}},
		{"the observer records a survey digest under the holder's token", &failingAcquire{Leaser: NewLeaser(), failFrom: 2}},
		{"the observer records a survey digest under the holder's token", forgetfulObserve{NewLeaser()}},
		{"the observer records a survey digest under the holder's token", &failingObserve{Leaser: NewLeaser(), failOn: 4}},
		{"the observer records a survey digest under the holder's token", &failingRelease{Leaser: NewLeaser(), failOn: 1}},
		{"the observer records a survey digest under the holder's token", &failingAcquire{Leaser: NewLeaser(), failFrom: 3}},
		{"the observer records a survey digest under the holder's token", digestDroppingRelease{NewLeaser()}},
		{"the observer records a survey digest under the holder's token", failingClear{NewLeaser()}},
		{"the observer records a survey digest under the holder's token", &failingRelease{Leaser: NewLeaser(), failOn: 2}},
		{"the observer records a survey digest under the holder's token", &failingAcquire{Leaser: NewLeaser(), failFrom: 4}},
		{"the observer records a survey digest under the holder's token", stickyClear{NewLeaser()}},
		{"returns on cancellation", ctxIgnoringObserver{NewLeaser()}},
	}
	for _, tc := range cases {
		t.Run(tc.check, func(t *testing.T) {
			for _, c := range Checks {
				if c.Name != tc.check {
					continue
				}
				if err := c.Run(t, tc.cat); err == nil {
					t.Fatalf("check %q passed a broken catalog", c.Name)
				}
				return
			}
			t.Fatalf("check %q not found", tc.check)
		})
	}
}

// brokenEnv makes the child of TestRunFailsTheSuiteOnABrokenCatalog run the
// suite instead of spawning another child.
const brokenEnv = "CATALOGTEST_BROKEN"

// TestRunFailsTheSuiteOnABrokenCatalog: Run is what every catalog's test calls,
// and a check that reports a problem must fail the test — a conformance suite
// that swallowed one would pass a catalog that does not conform. The failing
// test is the child process's, the way a conformance suite's failure is never
// this package's own.
func TestRunFailsTheSuiteOnABrokenCatalog(t *testing.T) {
	if os.Getenv(brokenEnv) == "1" {
		Run(t, func(*testing.T) catalog.Catalog { return openDoor{NewLeaser()} })
		return
	}
	args := []string{"-test.run=TestRunFailsTheSuiteOnABrokenCatalog"}
	// The child is this same coverage-instrumented binary, and go test gave this
	// one a directory to report what it ran into: without it the child's own run
	// would be measured nowhere.
	if dir := os.Getenv("GOCOVERDIR"); dir != "" {
		args = append(args, "-test.gocoverdir="+dir)
	}
	cmd := exec.Command(os.Args[0], args...)
	cmd.Env = append(os.Environ(), brokenEnv+"=1")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("the conformance suite passed a catalog that hands a held claim over:\n%s", out)
	}
	var exit *exec.ExitError
	if !errors.As(err, &exit) {
		t.Fatalf("the child did not run to its own end: %v", err)
	}
	if !strings.Contains(string(out), "a held claim was handed over") {
		t.Fatalf("the child's failure does not carry the check's message:\n%s", out)
	}
}
