package protection

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/catalog"
	pluginerrors "github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/execution"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/human"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/inventory"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
)

// LeaseOptions are the typed lease policy the assembly projects from the run
// section: how long a scheduled run waits, its poll interval, the deployment's
// bound on a writer's life, the finishing allowance the release budget comes
// from, and the downward-API variables naming this process.
type LeaseOptions struct {
	Catalog            catalog.Catalog
	Wait               time.Duration
	Poll               time.Duration
	HardBound          time.Duration
	FinishAllowance    time.Duration
	HolderNamespaceEnv string
	HolderPodEnv       string
	HolderUIDEnv       string
	Log                *slog.Logger
}

// Lease is the one writer claim of one run. Acquire fills it; Mark, Clear,
// Settle and Release act on it by its token; a second Acquire starts a new claim
// and forgets the first, which the caller must have released.
type Lease struct {
	o     LeaseOptions
	claim catalog.Lease
	held  bool
	// completed and clean capture the settlement state AT ACQUISITION: the last
	// completed observation's digest, and whether no attempt was in flight when
	// this run took the claim. BeginObservation writes its pending marker before
	// the survey, so the comparison must read these captured values, not the
	// record as it stands after Begin.
	completed string
	clean     bool
	// observationBroken records that the durable write-ahead marker could not
	// be written for this run: Release invalidates the recorded observation
	// before the CAS release, so a run that refused before its survey cannot be
	// bridged by two equal surveys around it.
	observationBroken bool
}

// NewLease binds the claim to its policy.
func NewLease(o LeaseOptions) *Lease { return &Lease{o: o} }

// Acquire takes the cross-process writer lease before any storage write. A
// manual run refuses at once while the holder cannot be shown gone and is
// refused outright when no catalog can hold a claim; a scheduled run waits up to
// Wait and then returns a plain error, so the night fails loudly rather than
// racing.
//
// The takeover rules use only facts the cluster records itself:
//
//   - the holder's exact pod found, by its recorded UID, in phase Succeeded or
//     Failed: the kubelet recorded that the process ended, and a SCHEDULED run
//     takes over at once.
//   - an absent pod or a different UID proves nothing on its own: a forced
//     deletion removes the object while the process may still run. It allows a
//     takeover of either kind only once the claim's acquisition time is older
//     than HardBound — the longest any writer can live under the Job's
//     activeDeadlineSeconds.
//
// A running or pending pod, a partial identity, a failed read and a catalog
// without the liveness capability all count as alive: nobody takes a claim over
// on a guess. A scheduled run then waits its bounded time and fails loudly every
// cycle, which is visible and fixable by the deployment. With a working catalog
// no held claim blocks scheduled runs forever: the holder either terminated
// definitively or outlived the hard bound, and past it the claim is taken. The
// takeover arms the record's uncertainty; it never declares occupancy known, and
// no clock ever clears that uncertainty.
//
// The taken claim carries the record's uncertainty: a non-nil Uncertainty means
// the replaced holder may still have provider-side writes in flight, and this
// run treats every storage's occupancy as unknown.
func (l *Lease) Acquire(ctx context.Context, kind model.RunKind, runID string) (*catalog.Lease, catalog.Lease, error) {
	if l.o.Catalog == nil {
		return nil, catalog.Lease{}, leaseUnsupported(kind)
	}
	req := catalog.LeaseRequest{Holder: runID, Kind: kind}
	req.HolderNamespace, req.HolderPod, req.HolderUID = l.holderIdentity()
	deadline := time.Now().Add(l.o.Wait)
	for {
		claim, err := l.o.Catalog.Acquire(ctx, req)
		if err == nil {
			l.claim, l.held = claim, true
			l.observationBroken = false
			l.rememberSettlement()
			l.event(human.New("lease.acquired", human.Text("kind", string(kind)), human.Text("run", runID)))
			return &l.claim, catalog.Lease{}, nil
		}
		if errors.Is(err, errors.ErrUnsupported) {
			return nil, catalog.Lease{}, leaseUnsupported(kind)
		}
		if errors.Is(err, catalog.ErrLeaseHeld) {
			if l.mayTakeOver(ctx, kind, claim) {
				req.TakeoverToken = claim.Token
				taken, terr := l.o.Catalog.Acquire(ctx, req)
				req.TakeoverToken = ""
				if terr == nil {
					l.claim, l.held = taken, true
					l.observationBroken = false
					l.rememberSettlement()
					l.event(human.New("lease.acquired", human.Text("kind", string(kind)), human.Text("run", runID),
						human.Text("previous", claim.Holder)))
					return &l.claim, catalog.Lease{}, nil
				}
				err = terr
			} else if kind == model.RunKindManual {
				return nil, claim, err
			}
		}
		class, _ := pluginerrors.ClassOf(err)
		if class == pluginerrors.ClassPermanent || time.Now().After(deadline) {
			return nil, claim, err
		}
		l.event(human.Warning("lease.waiting", human.Text("kind", string(kind)), human.Text("reason", err.Error())))
		select {
		case <-ctx.Done():
			return nil, catalog.Lease{}, ctx.Err()
		case <-time.After(l.o.Poll):
		}
	}
}

// mayTakeOver decides whether one held claim may be replaced. The rules never
// let a clock speak about occupancy — only about whether the holder's PROCESS
// can still be alive, under the deployment's declared bound on a writer's life:
//
//   - a definitive terminal phase (the kubelet recorded Succeeded/Failed for the
//     holder's exact pod) is proof at once, and only a scheduled run acts on it;
//   - an absent or changed pod proves nothing by itself, but past HardBound the
//     process cannot be alive any more, so either kind may take over. That is a
//     bound on the writer's lifetime under activeDeadlineSeconds, validated by
//     the chart — not an estimate of when a provider settles a write.
//
// A missing, unsupported or failed liveness check counts as alive: nobody takes
// a claim over on a guess.
func (l *Lease) mayTakeOver(ctx context.Context, kind model.RunKind, claim catalog.Lease) bool {
	terminated, definitive, provable := l.holderTerminated(ctx, claim)
	if !provable || !terminated {
		return false
	}
	if definitive {
		return kind == model.RunKindScheduled
	}
	return l.hardBoundPassed(claim)
}

// hardBoundPassed reports whether the claim's acquisition is older than the
// longest any writer can live.
func (l *Lease) hardBoundPassed(claim catalog.Lease) bool {
	if claim.AcquiredAt.IsZero() {
		return false // undated: never a proof
	}
	return time.Since(claim.AcquiredAt) >= l.o.HardBound
}

// holderTerminated asks the catalog's optional liveness capability. provable is
// false when the capability is absent, answers errors.ErrUnsupported, or the
// check failed — and an unprovable holder always counts as alive.
func (l *Lease) holderTerminated(ctx context.Context, claim catalog.Lease) (terminated, definitive, provable bool) {
	liveness, ok := l.o.Catalog.(catalog.Liveness)
	if !ok {
		return false, false, false
	}
	type verdict struct{ terminated, definitive bool }
	v, err := execution.SafeValue(func() (verdict, error) {
		t, d, err := liveness.HolderState(ctx, claim)
		return verdict{t, d}, err
	})
	if err != nil {
		if !errors.Is(err, errors.ErrUnsupported) {
			l.event(human.Warning("lease.liveness.failed",
				human.Text("run", claim.Holder), human.Text("reason", "the holder counts as alive: "+err.Error())))
		}
		return false, false, false
	}
	return v.terminated, v.definitive, true
}

// holderIdentity reads the deployment's opaque process identity — in Kubernetes
// the pod namespace, name and UID from the downward API. An unset or empty
// variable leaves the identity incomplete, and a catalog that cannot find the
// holder counts it as alive.
func (l *Lease) holderIdentity() (namespace, pod, uid string) {
	if name := l.o.HolderNamespaceEnv; name != "" {
		namespace = os.Getenv(name)
	}
	if name := l.o.HolderPodEnv; name != "" {
		pod = os.Getenv(name)
	}
	if name := l.o.HolderUIDEnv; name != "" {
		uid = os.Getenv(name)
	}
	return namespace, pod, uid
}

// leaseUnsupported is what a run does when no catalog can hold a claim: a manual
// run is refused, a scheduled one writes as it always did.
func leaseUnsupported(kind model.RunKind) error {
	if kind == model.RunKindManual {
		return errors.New("no catalog can hold the writer lease")
	}
	return nil
}

// rememberSettlement captures what the acquired record says about the last
// completed observation.
func (l *Lease) rememberSettlement() {
	l.completed, l.clean = "", true
	if l.claim.Uncertainty == nil {
		return
	}
	l.completed = completedDigest(l.claim.Uncertainty.Digest)
	l.clean = !strings.HasPrefix(l.claim.Uncertainty.Digest, pendingPrefix)
}

// Claim returns the claim this lease currently holds.
func (l *Lease) Claim() (catalog.Lease, bool) { return l.claim, l.held }

// MarkUncertainty durably adds this holder's run id to the record's pending set
// before its first potentially asynchronous write — an accepted multipart
// commit, an HTTP 202. The record is durable and shared: a crash from here on
// leaves a successor an unknown occupancy, never a known one. An inherited
// record gains this run's id; it is never dropped, so the settlement digest
// covers every pending prefix. A failed mark stops the write path, because a
// record that could not be made conservative is not a licence to write.
func (l *Lease) MarkUncertainty(ctx context.Context) error {
	if !l.held {
		return errors.New("marking a claim this run does not hold")
	}
	next := catalog.Uncertainty{Since: time.Now().UTC()}
	if l.claim.Uncertainty != nil {
		next = *l.claim.Uncertainty
	}
	if !slices.Contains(next.RunIDs, l.claim.Holder) {
		next.RunIDs = append(slices.Clone(next.RunIDs), l.claim.Holder)
	}
	pending := l.claim
	pending.Uncertainty = &next
	if err := l.o.Catalog.MarkUncertainty(ctx, pending); err != nil {
		return fmt.Errorf("marking that provider writes may settle after this run: %w", err)
	}
	l.claim = pending
	l.event(human.New("lease.uncertain", human.Text("run", l.claim.Holder),
		human.Text("reason", "provider writes may settle after this run exits; occupancy stays unknown until a terminal proof and a fresh survey")))
	return nil
}

// PendingRunIDs returns the record's pending holder ids: every run whose
// accepted writes may still settle. The protection ledger protects every one of
// them. Nil when the claim carries no uncertainty.
func (l *Lease) PendingRunIDs() []string {
	if !l.held || l.claim.Uncertainty == nil {
		return nil
	}
	return slices.Clone(l.claim.Uncertainty.RunIDs)
}

// ClearUncertainty removes the record's uncertainty once the run's own writes
// have a terminal proof. It is only ever called while the claim is the caller's:
// a stale holder changes nothing. A failed clear keeps the record conservative
// and is a warning; unknown occupancy costs availability, not correctness. The
// local claim is updated only after the durable clear succeeded.
func (l *Lease) ClearUncertainty(ctx context.Context) error {
	if !l.held {
		return errors.New("clearing a claim this run does not hold")
	}
	if err := l.o.Catalog.ClearUncertainty(ctx, l.claim); err != nil {
		l.event(human.Warning("lease.uncertain.failed",
			human.Text("run", l.claim.Holder), human.Text("reason", "the record stays uncertain: "+err.Error())))
		return fmt.Errorf("clearing the record's uncertainty: %w", err)
	}
	l.claim.Uncertainty = nil
	l.event(human.New("lease.uncertain.cleared", human.Text("run", l.claim.Holder)))
	return nil
}

// pendingPrefix marks an observation that has begun but not completed. The value
// after it is the last COMPLETED observation, kept so the next full survey can
// still be compared; the prefix itself says an attempt is outstanding and
// forbids that comparison.
const pendingPrefix = "pending:"

// BeginObservation durably records, BEFORE the settlement survey starts, that an
// observation is in flight. A crash, a failed survey or a failed later write
// leaves the pending marker in the record, so a successor never compares against
// an observation whose attempt never completed. It is a no-op for a run whose
// claim carries no uncertainty, and for a manual run, which never observes.
func (l *Lease) BeginObservation(ctx context.Context) error {
	if !l.held || l.claim.Uncertainty == nil {
		return nil
	}
	observer, ok := l.o.Catalog.(catalog.UncertaintyObserver)
	if !ok {
		return fmt.Errorf("recording the observation attempt: %w", errors.ErrUnsupported)
	}
	next := pendingToken(l.claim.Uncertainty.Digest)
	if err := observer.ObserveUncertainty(ctx, l.claim, next); err != nil {
		// The marker is not durable: the attempt is broken, and Release makes one
		// invalidating observation so the chain cannot bridge this run.
		l.observationBroken = true
		return fmt.Errorf("recording the observation attempt: %w", err)
	}
	l.claim.Uncertainty.Digest = next
	return nil
}

// pendingToken converts one recorded digest into the in-flight form, keeping the
// last completed value after the prefix. An already pending value is kept as it
// is: the last completed observation is the one already stored.
func pendingToken(digest string) string {
	if strings.HasPrefix(digest, pendingPrefix) {
		return digest
	}
	return pendingPrefix + digest
}

// completedDigest is the last completed observation a record carries.
func completedDigest(digest string) string { return strings.TrimPrefix(digest, pendingPrefix) }

// SettleInheritedUncertainty applies the clock-free settlement proof to one
// scheduled run's full survey. The record's uncertainty names every run whose
// late commit no clock can disprove; the digest of their inventories — keys and
// sizes under every prefix, in record order, per storage — changes the moment a
// commit lands. Two consecutive COMPLETED observations of the same digest prove
// nothing landed in between, and the record is cleared; a pending marker forbids
// the comparison, so an interrupted attempt always starts the chain again. A
// survey that did not finish is recorded as the empty digest, invalidating the
// earlier observation.
//
// It reports whether occupancy is now known. A clear that fails leaves the
// record uncertain — conservative, and the run continues unknown. Only a
// scheduled run observes: a manual run under unknown occupancy writes nothing,
// and its survey is not evidence anyone trusts.
func (l *Lease) SettleInheritedUncertainty(ctx context.Context, kind model.RunKind, runID string,
	storages []string, surveys map[string]inventory.Survey) (bool, error) {
	if !l.held {
		return false, nil
	}
	if kind != model.RunKindScheduled || l.claim.Uncertainty == nil {
		return false, nil
	}
	digest, observed := uncertaintyDigest(l.claim.Uncertainty.RunIDs, storages, surveys)
	if !observed {
		if err := l.observeWriterUncertainty(ctx, ""); err != nil {
			return false, err
		}
		return false, nil
	}
	if l.clean && l.completed != "" && l.completed == digest {
		if err := l.ClearUncertainty(ctx); err != nil {
			return false, nil
		}
		return true, nil
	}
	if err := l.observeWriterUncertainty(ctx, digest); err != nil {
		return false, err
	}
	return false, nil
}

// uncertaintyDigest is the deterministic inventory of every pending run's
// prefix across every storage: each storage's name, then each run's object keys
// and sizes in key order — the record's run order, so every holder's writes
// enter the next proof. A failed survey answers observed=false — a partial view
// is not an observation, and comparing it with a full one would be a guess.
func uncertaintyDigest(runIDs []string, storages []string, surveys map[string]inventory.Survey) (digest string, observed bool) {
	h := sha256.New()
	for _, name := range storages {
		s, ok := surveys[name]
		if !ok || !s.OK {
			return "", false
		}
		_, _ = fmt.Fprintf(h, "storage\x00%s\x00", name)
		for _, runID := range runIDs {
			_, _ = fmt.Fprintf(h, "run\x00%s\x00", runID)
			r, ok := s.Runs[runID]
			if !ok {
				continue // the run holds nothing here: an empty inventory
			}
			objects := slices.Clone(r.Objects)
			slices.SortFunc(objects, func(a, b model.ObjectInfo) int { return strings.Compare(a.Key, b.Key) })
			for _, o := range objects {
				_, _ = fmt.Fprintf(h, "%s\x00%d\n", o.Key, o.Size)
			}
		}
	}
	return hex.EncodeToString(h.Sum(nil)), true
}

// observeWriterUncertainty records one full survey's digest in the claim's
// record through the catalog's optional observation capability, and mirrors it
// locally once the durable write succeeded. An empty digest invalidates an
// earlier observation, so only consecutive completed surveys are ever compared.
// A catalog without the capability answers as unsupported: the run keeps
// occupancy unknown and names the missing capability, so the deployment can fix
// it.
func (l *Lease) observeWriterUncertainty(ctx context.Context, digest string) error {
	observer, ok := l.o.Catalog.(catalog.UncertaintyObserver)
	if !ok {
		return fmt.Errorf("recording the uncertainty survey: %w", errors.ErrUnsupported)
	}
	if err := observer.ObserveUncertainty(ctx, l.claim, digest); err != nil {
		return fmt.Errorf("recording the uncertainty survey: %w", err)
	}
	if l.claim.Uncertainty != nil {
		l.claim.Uncertainty.Digest = digest
	}
	// Mirror the completed observation locally: the next settlement call on
	// this claim compares against exactly what the durable record now says.
	l.clean, l.completed = true, digest
	l.event(human.New("lease.uncertain.observed",
		human.Text("run", l.claim.Holder), human.Text("digest", digest)))
	return nil
}

// Release drops the claim under the ONE cleanup deadline the lifecycle owns.
//
// It is the lifecycle Shutdown.Release step: the caller runs it only after every
// request goroutine has joined, and it CAS-releases this run's claim together
// with its durable uncertainty — the catalog keeps that uncertainty exactly as
// it stands, so a run that may have left a late commit behind never hands a
// successor a known-looking occupancy. The child budget is FinishAllowance, so
// release cannot consume the abort's share of the same cleanup deadline; the
// deadline itself is never extended here.
func (l *Lease) Release(ctx context.Context) error {
	if !l.held {
		return nil
	}
	l.invalidateBrokenObservation(ctx)
	rel, cancel := context.WithTimeout(ctx, l.o.FinishAllowance)
	defer cancel()
	err := l.o.Catalog.Release(rel, l.claim)
	l.held = false
	if err != nil {
		l.event(human.Warning("lease.release.failed",
			human.Text("run", l.claim.Holder), human.Text("reason", err.Error())))
		return err
	}
	l.event(human.New("lease.released", human.Text("run", l.claim.Holder)))
	return nil
}

// invalidateBrokenObservation makes ONE observation attempt with the empty
// digest when this run's write-ahead marker could not be written: the record's
// last completed observation becomes unusable, so a later full survey starts a
// new chain instead of clearing a digest that spans a failed attempt. A failed
// invalidation leaves the record conservative and is logged; there is no retry.
func (l *Lease) invalidateBrokenObservation(ctx context.Context) {
	if !l.observationBroken || l.claim.Uncertainty == nil {
		return
	}
	observer, ok := l.o.Catalog.(catalog.UncertaintyObserver)
	if !ok {
		return
	}
	if err := observer.ObserveUncertainty(ctx, l.claim, ""); err != nil {
		l.event(human.Warning("lease.uncertain.failed",
			human.Text("run", l.claim.Holder), human.Text("reason", "the failed observation attempt stays recorded: "+err.Error())))
		return
	}
	l.claim.Uncertainty.Digest = ""
	l.clean, l.completed = true, ""
}

// PendingRemovalBound is the upper bound of one durable record write — the
// catalog's own WriteTimeout — or 0 when the catalog cannot bound one. The
// workflow reserves it out of the finish window before the removal of this
// run's pending id.
func (l *Lease) PendingRemovalBound() time.Duration {
	bounded, ok := l.o.Catalog.(catalog.WriteBounded)
	if !ok {
		return 0
	}
	return bounded.WriteTimeout()
}

// RemoveOwnPending durably removes this holder's own id from the record's
// pending set after its writes have a terminal proof. Inherited ids stay, and
// the record's digest and Since are kept because the digest covers only the
// remaining prefixes. A set left empty clears the record as ClearUncertainty
// does. No-op when the claim is not held or the holder is not in the set; the
// local claim mirrors the durable result only after success.
func (l *Lease) RemoveOwnPending(ctx context.Context) error {
	if !l.held || l.claim.Uncertainty == nil {
		return nil
	}
	ids := make([]string, 0, len(l.claim.Uncertainty.RunIDs))
	found := false
	for _, id := range l.claim.Uncertainty.RunIDs {
		if id == l.claim.Holder {
			found = true
			continue
		}
		ids = append(ids, id)
	}
	if !found {
		return nil
	}
	if len(ids) == 0 {
		return l.ClearUncertainty(ctx)
	}
	next := *l.claim.Uncertainty
	next.RunIDs = ids
	pending := l.claim
	pending.Uncertainty = &next
	if err := l.o.Catalog.MarkUncertainty(ctx, pending); err != nil {
		return fmt.Errorf("removing this run from the record's pending set: %w", err)
	}
	l.claim = pending
	return nil
}

func (l *Lease) event(ev human.Event) {
	l.o.Log.Log(context.Background(), ev.Severity, ev.Name, slog.Any("event", ev))
}
