package engine

import (
	"context"
	"errors"
	"os"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/catalog"
	"github.com/vi-latyshev/go-cluster-backup/internal/human"
	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
)

// acquireWriterLease takes the cross-process writer lease before any storage
// write. A manual run refuses at once while the holder cannot be shown gone and
// is refused outright when the catalog cannot hold a lease at all; a scheduled
// run waits up to run.lease_wait and then returns a plain error, so the night
// fails loudly rather than racing.
//
// The takeover rules use only facts that hold on this deployment (one node,
// restartPolicy Never, activeDeadlineSeconds):
//
//   - definitive proof — the holder pod found with its recorded UID in phase
//     Succeeded or Failed — lets a SCHEDULED run take over at once, because the
//     kubelet itself recorded that the process ended.
//   - an absent pod or a different UID proves nothing: a forced deletion removes
//     the object while the process may still run. It allows a takeover of either
//     kind only once the claim's acquisition time is older than
//     run.lease_hard_bound — the longest any writer can live under the Job's
//     activeDeadlineSeconds.
//
// A running or pending pod, a partial identity, an absent liveness capability,
// errors.ErrUnsupported and a failed read all count as alive. The third return
// value is the instant until which the predecessor's already-accepted provider
// operations may still land, so the successor's occupancy is unknown until then
// (zero means "known").
func (e *Engine) acquireWriterLease(ctx context.Context, kind model.RunKind, runID string) (*catalog.Lease, catalog.Lease, time.Time, error) {
	if e.lease == nil {
		return nil, catalog.Lease{}, time.Time{}, e.leaseUnsupported(kind)
	}
	req := catalog.LeaseRequest{Holder: runID, Kind: kind}
	req.HolderNamespace, req.HolderPod, req.HolderUID = e.holderIdentity()
	deadline := time.Now().Add(e.cfg.Run.LeaseWait.Std())
	for {
		lease, err := e.lease.Acquire(ctx, req)
		if err == nil {
			e.event(human.New("lease.acquired", human.Text("kind", string(kind)), human.Text("run", runID)))
			return &lease, catalog.Lease{}, lease.QuarantineUntil, nil
		}
		if errors.Is(err, errors.ErrUnsupported) {
			return nil, catalog.Lease{}, time.Time{}, e.leaseUnsupported(kind)
		}
		if errors.Is(err, catalog.ErrLeaseHeld) {
			takeover, quarantine := e.mayTakeOver(ctx, kind, lease)
			if takeover {
				req.TakeoverToken = lease.Token
				req.QuarantineUntil = quarantine
				taken, terr := e.lease.Acquire(ctx, req)
				req.TakeoverToken = ""
				req.QuarantineUntil = time.Time{}
				if terr == nil {
					e.event(human.New("lease.acquired", human.Text("kind", string(kind)), human.Text("run", runID),
						human.Text("previous", lease.Holder)))
					// The quarantine the catalog holds is the maximum of this
					// takeover's and every earlier one: it survives a Release, so a
					// successor's early exit cannot hand a known-looking occupancy to
					// a run inside the dead holder's bound.
					return &taken, catalog.Lease{}, taken.QuarantineUntil, nil
				}
				err = terr
			} else if kind == model.RunKindManual {
				return nil, lease, time.Time{}, err
			}
		}
		class, _ := plugin.ClassOf(err)
		if class == plugin.ClassPermanent || time.Now().After(deadline) {
			return nil, lease, time.Time{}, err
		}
		e.event(human.Warning("lease.waiting", human.Text("kind", string(kind)), human.Text("reason", err.Error())))
		select {
		case <-ctx.Done():
			return nil, catalog.Lease{}, time.Time{}, ctx.Err()
		case <-time.After(e.cfg.Run.LeasePoll.Std()):
		}
	}
}

// mayTakeOver applies the two proofs to one held claim, and returns the instant
// until which the dead holder's provider-side operations may still land.
func (e *Engine) mayTakeOver(ctx context.Context, kind model.RunKind, lease catalog.Lease) (bool, time.Time) {
	terminated, definitive, provable := e.holderTerminated(ctx, lease)
	if !provable || !terminated {
		return false, time.Time{}
	}
	if definitive {
		// Only a scheduled run acts on the kubelet's terminal phase; a manual run
		// waits for the object to be conclusively gone (or the claim to be free).
		if kind != model.RunKindScheduled {
			return false, time.Time{}
		}
		// The process ended, but a request it had already handed to the provider
		// may still commit (Yandex answers 202 before the commit). Until its hard
		// bound passes, the successor's occupancy is unknown.
		if !lease.AcquiredAt.IsZero() {
			return true, lease.AcquiredAt.Add(e.cfg.Run.LeaseHardBound.Std())
		}
		return true, time.Time{}
	}
	// Absent or a different UID: only the hard bound makes that a proof.
	if e.hardBoundPassed(lease) {
		return true, time.Time{}
	}
	return false, time.Time{}
}

// hardBoundPassed reports whether the claim's acquisition is older than the
// longest any writer can live.
func (e *Engine) hardBoundPassed(lease catalog.Lease) bool {
	if lease.AcquiredAt.IsZero() {
		return false // undated: never a proof
	}
	return time.Since(lease.AcquiredAt) >= e.cfg.Run.LeaseHardBound.Std()
}

// holderTerminated asks the leaser's optional liveness capability. provable is
// false when the capability is absent, answers errors.ErrUnsupported, or the
// check failed — and an unprovable holder always counts as alive.
func (e *Engine) holderTerminated(ctx context.Context, lease catalog.Lease) (terminated, definitive, provable bool) {
	liveness, ok := e.lease.(catalog.Liveness)
	if !ok {
		return false, false, false
	}
	type verdict struct{ terminated, definitive bool }
	v, err := safeValue(func() (verdict, error) {
		t, d, err := liveness.HolderState(ctx, lease)
		return verdict{t, d}, err
	})
	if err != nil {
		if !errors.Is(err, errors.ErrUnsupported) {
			e.event(human.Warning("lease.liveness.failed",
				human.Text("run", lease.Holder), human.Text("reason", "the holder counts as alive: "+err.Error())))
		}
		return false, false, false
	}
	return v.terminated, v.definitive, true
}

// holderIdentity reads the deployment's opaque process identity — in Kubernetes
// the pod namespace, name and UID from the downward API. An unset or empty
// variable leaves the identity incomplete, and a leaser that cannot find the
// holder counts it as alive.
func (e *Engine) holderIdentity() (namespace, pod, uid string) {
	if name := e.cfg.Run.HolderNamespaceEnv; name != "" {
		namespace = os.Getenv(name)
	}
	if name := e.cfg.Run.HolderPodEnv; name != "" {
		pod = os.Getenv(name)
	}
	if name := e.cfg.Run.HolderUIDEnv; name != "" {
		uid = os.Getenv(name)
	}
	return namespace, pod, uid
}

// leaseUnsupported is what a run does when the catalog cannot hold a claim: a
// manual run is refused, a scheduled one writes as it always did.
func (e *Engine) leaseUnsupported(kind model.RunKind) error {
	if kind == model.RunKindManual {
		return errors.New("no catalog can hold the writer lease")
	}
	return nil
}

// releaseWriterLease drops the claim at the end of the run, on a context that
// outlives a cancelled run: a pod being terminated still releases the lease so
// the next run is not made to wait. Release is a no-op when the claim is no
// longer ours.
func (e *Engine) releaseWriterLease(ctx context.Context, lease catalog.Lease, runID string) {
	relCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), e.cfg.Run.FinishAllowance.Std())
	defer cancel()
	if err := e.lease.Release(relCtx, lease); err != nil && !errors.Is(err, errors.ErrUnsupported) {
		e.event(human.Warning("lease.release.failed", human.Text("run", runID), human.Text("reason", err.Error())))
		return
	}
	e.event(human.New("lease.released", human.Text("run", runID)))
}
