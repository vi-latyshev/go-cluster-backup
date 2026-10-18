// Package catalog is the seam for run state that does not live on a destination —
// the history that answers "a run was missed". The interface obeys the seam rules
// documented in package plugin.
package catalog

import (
	"context"
	"errors"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
)

// LoadRequest asks for the newest runs; Limit <= 0 means no limit. A returned
// slice is ordered newest first by StartedAt.
type LoadRequest struct{ Limit int }

type Catalog interface {
	// Save upserts by RunID. A report with Outcome "running" marks a run start;
	// the same RunID saved again with a final outcome completes it.
	Save(ctx context.Context, run model.RunReport) error
	Load(ctx context.Context, req LoadRequest) ([]model.RunReport, error)
	// Forget removes entries; ids that are not present are not an error.
	Forget(ctx context.Context, runIDs []string) error
}

var Kind = plugin.NewKind[Catalog]("catalog")

// ErrLeaseHeld is the marker a Leaser returns when a live lease is already held.
// It is a serialisable sentinel discovered by errors.Is, never a private type:
// a manual run refuses on it at once, a scheduled one waits its bounded time and
// then fails loudly.
var ErrLeaseHeld = errors.New("the writer lease is held by another run")

// Lease is the single-writer claim one run holds over a catalog's storages while
// it runs. Two runs writing the same storage roots would overwrite each other's
// objects and let one's retention delete the other's in-flight ones, and the
// deployment's own concurrencyPolicy does not cover a manually created Job.
//
// The storages have no fencing tokens, so a claim is never handed over on the
// strength of a clock: a held claim is taken over only when the Leaser can PROVE
// its holder has terminated (catalog.Liveness). The claim therefore records who
// the holder is — its run, its kind, and its process identity, supplied by the
// deployment as opaque strings — and Token makes a release by a process that no
// longer holds the claim a no-op.
type Lease struct {
	Holder string        `json:"holder"`
	Kind   model.RunKind `json:"kind"`
	// Token is unique per acquisition: Release only ever drops the claim it
	// carries this token for.
	Token string `json:"token"`
	// HolderNamespace, HolderPod and HolderUID identify the holder's process —
	// the pod's namespace, name and UID in Kubernetes — so a leaser with the
	// Liveness capability can find it. Opaque to the engine; an incomplete
	// identity means the leaser cannot tell, which counts as alive.
	HolderNamespace string    `json:"holder_namespace,omitempty"`
	HolderPod       string    `json:"holder_pod,omitempty"`
	HolderUID       string    `json:"holder_uid,omitempty"`
	AcquiredAt      time.Time `json:"acquired_at"`
	// QuarantineUntil is when the storage occupancy becomes trustworthy again
	// after a takeover whose predecessor may still have provider operations in
	// flight. It is part of the RECORD, not of a holder: Release keeps it and a
	// later Acquire inherits the maximum, so a successor's early exit cannot
	// hand a known-looking occupancy to a run inside the dead holder's bound.
	QuarantineUntil time.Time `json:"quarantine_until,omitempty"`
}

// LeaseRequest asks for the lease. Holder, Kind, HolderPod and HolderUID are the
// caller's own identity. TakeoverToken, when set, asks to take over exactly that
// claim: the engine sets it only after the Leaser proved the holder terminated,
// and a claim whose token has changed in the meantime is refused.
type LeaseRequest struct {
	Holder          string
	Kind            model.RunKind
	HolderNamespace string
	HolderPod       string
	HolderUID       string
	TakeoverToken   string
	// QuarantineUntil is the instant this takeover makes occupancy unknown
	// until; the record keeps the maximum of it and whatever an earlier takeover
	// recorded.
	QuarantineUntil time.Time
}

// Leaser is the optional catalog capability that serialises runs: a capability
// is discovered by call (errors.Is(err, errors.ErrUnsupported)), never by
// widening the Catalog interface. A catalog without it leaves manual runs
// refused — the engine will not write a manual run where a scheduled one could
// race it.
type Leaser interface {
	// Acquire takes the lease in ONE attempt: it returns the taken Lease when
	// the claim is free, and ErrLeaseHeld when a claim is held — with the held
	// claim returned alongside the error, so a caller can name the holder. An
	// existing claim is taken over only when the request carries its exact
	// TakeoverToken, which the caller sets after proving the holder terminated.
	Acquire(ctx context.Context, req LeaseRequest) (Lease, error)
	// Release drops the claim if its token is still the holder's; a claim
	// already gone or taken over is not an error.
	Release(ctx context.Context, lease Lease) error
}

// Liveness is the optional capability a Leaser implements when it can report,
// from a fact the cluster itself records, whether a holder terminated. There is
// no clock in the question.
//
// terminated and definitive are reported together because the two kinds of proof
// are not equal. definitive is true only when the kubelet recorded the terminal
// phase (Succeeded or Failed) of the holder's EXACT pod — a fact that cannot be
// produced by a forced deletion. definitive false means the pod object is absent
// or belongs to a different UID, which force-deletion can produce while the
// process still runs: that proof is only good past the hard bound the deployment
// sets from its Job deadline.
//
// A running or pending pod, a partial identity and any failed read are all
// "not terminated" (with an error for the read failure): nobody takes a claim
// over on a guess. A Leaser without this capability, and one that answers
// errors.ErrUnsupported, are treated the same way.
type Liveness interface {
	HolderState(ctx context.Context, lease Lease) (terminated, definitive bool, err error)
}

// PreviousRunInspector is the optional catalog capability that reports why the
// previous Job of the caller's own CronJob failed before this tool could report
// — the failures that leave no catalog record. A capability is discovered by
// call, never by widening the Catalog interface.
//
// namespace and pod are the caller's own process identity, from the deployment's
// downward API (run.holder_namespace_env / run.holder_pod_env); an empty pod
// means the caller is not running as a Kubernetes pod and there is nothing to
// inspect. The returned failure is nil when the previous Job succeeded or there
// is none; an error is a read the caller could not make and is surfaced, never
// fatal.
type PreviousRunInspector interface {
	PreviousJobFailure(ctx context.Context, namespace, pod string) (*model.PreviousFailure, error)
}
