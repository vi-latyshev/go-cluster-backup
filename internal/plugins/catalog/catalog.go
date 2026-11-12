// Package catalog is the seam for the cross-process writer lease one run holds
// over a deployment's destination storages. It records nothing else: what
// happened to earlier runs is read from the storages themselves, and why a
// previous Job failed before this tool ran is read from the cluster, never from
// a catalog. The interface obeys the seam rules documented in package instances.
package catalog

import (
	"context"
	"errors"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
)

// Catalog is the writer lease. Two runs writing the same storage roots would
// overwrite each other's objects and let one's retention delete the other's
// in-flight ones, and the deployment's own concurrencyPolicy does not cover a
// manually created Job.
//
// The storages have no fencing tokens, so a claim is never handed over on the
// strength of a clock: a held claim is taken over only when the caller can PROVE
// its holder has terminated (Liveness) and asks for exactly that claim. The
// claim records who the holder is — its run, its kind, and its process identity,
// supplied by the deployment as opaque strings — and Token makes a call by a
// process that no longer holds the claim a no-op.
//
// Every method makes one attempt: retries belong to execution, which owns the
// run's budget. A failure to reach the durable record is surfaced, never turned
// into "the claim is free" or "occupancy is known".
type Catalog interface {
	// Acquire takes the lease in one attempt: it returns the taken Lease when
	// the claim is free, and ErrLeaseHeld when a claim is held — with the held
	// claim returned alongside the error, so a caller can name the holder. An
	// existing claim is taken over only when the request carries its exact
	// TakeoverToken. A takeover arms the record's pending set for the holder it
	// replaces; a free or released record hands its uncertainty on unchanged.
	Acquire(ctx context.Context, req LeaseRequest) (Lease, error)
	// MarkUncertainty persists, atomically with the claim's compare-and-swap, the
	// uncertainty the caller carries in lease: before a holder's first
	// potentially asynchronous write — an accepted multipart upload, a 202
	// commit — the caller hands the record its new pending-run set. The record
	// takes the value exactly as handed: keeping every inherited run id and
	// adding this holder's is the caller's decision. A lease handed without an
	// uncertainty changes nothing — marking never clears a record — and a claim
	// already gone or taken over changes nothing either.
	MarkUncertainty(ctx context.Context, lease Lease) error
	// ClearUncertainty removes the record's uncertainty, atomically with the
	// claim's compare-and-swap and only while the claim is still the caller's,
	// after a terminal proof and a fresh survey established that occupancy is
	// known. There is no clock in the decision. A stale holder changes nothing.
	ClearUncertainty(ctx context.Context, lease Lease) error
	// Release drops the claim, atomically with the record's uncertainty exactly
	// as it stands, digest included: an uncertainty this run armed survives, a
	// successor's is never cleared, and a claim already gone or taken over is
	// not an error.
	Release(ctx context.Context, lease Lease) error
}

var Kind = instances.NewKind[Catalog]("catalog")

// WriteBounded is the optional capability a Catalog implements when it can
// bound one record write — MarkUncertainty or ClearUncertainty, read and patch
// together. A run that armed the record's uncertainty keeps that bound as the
// finish window's reserve, so the removal of its own pending id survives a
// receiver that spends the rest of the window. A catalog without the capability
// answers no bound, and the reserve is zero. The Catalog interface is not
// widened.
type WriteBounded interface {
	WriteTimeout() time.Duration
}

// ErrLeaseHeld is the marker a Catalog returns when a live lease is already
// held. It is a serialisable sentinel discovered by errors.Is, never a private
// type: a manual run refuses on it at once, a scheduled one waits its bounded
// time and then fails loudly.
var ErrLeaseHeld = errors.New("the writer lease is held by another run")

// Uncertainty is the record's statement that runs may have provider-side
// writes that settle after they exit, so storage occupancy is unknown until a
// terminal proof and a fresh survey say otherwise. It belongs to the RECORD,
// never to a holder: Release keeps it, Acquire inherits it, a takeover arms it,
// and only the claim's own holder can extend or clear it. No clock expires it —
// a guessed settlement bound would turn a late commit into a false "known".
type Uncertainty struct {
	// RunIDs names every holder whose accepted writes may still settle. The
	// current holder's own id is added before its first asynchronous write; an
	// inherited prefix is never dropped until a settlement clears the record.
	RunIDs []string `json:"run_ids"`
	// Since is when the uncertainty was recorded, for the operator reading the
	// record; it is not an expiry.
	Since time.Time `json:"since"`
	// Digest is the last full-survey inventory of every RunIDs prefix: object
	// keys and sizes, per storage. Two consecutive full surveys that produce the
	// same digest prove no late provider commit landed between them. Empty means
	// no valid observation; no clock expires the uncertainty.
	Digest string `json:"digest,omitempty"`
}

// Lease is the single-writer claim one run holds while it writes.
type Lease struct {
	Holder string        `json:"holder"`
	Kind   model.RunKind `json:"kind"`
	// Token is unique per acquisition: Release, MarkUncertainty and
	// ClearUncertainty only ever act on the claim they carry this token for.
	Token string `json:"token"`
	// HolderNamespace, HolderPod and HolderUID identify the holder's process —
	// the pod's namespace, name and UID in Kubernetes — so a leaser with the
	// Liveness capability can find it. Opaque here; an incomplete identity means
	// the leaser cannot tell, which counts as alive.
	HolderNamespace string    `json:"holder_namespace,omitempty"`
	HolderPod       string    `json:"holder_pod,omitempty"`
	HolderUID       string    `json:"holder_uid,omitempty"`
	AcquiredAt      time.Time `json:"acquired_at"`
	// Uncertainty is the record's uncertainty this claim was acquired under:
	// non-nil means occupancy is unknown and no run may treat its storage view
	// as known.
	Uncertainty *Uncertainty `json:"uncertainty,omitempty"`
}

// LeaseRequest asks for the lease. Holder, Kind, HolderPod and HolderUID are the
// caller's own identity. TakeoverToken, when set, asks to take over exactly that
// claim: the run sets it only after the optional Liveness capability's
// HolderState proved the holder terminated, and a claim whose token has changed
// in the meantime is refused.
type LeaseRequest struct {
	Holder          string
	Kind            model.RunKind
	HolderNamespace string
	HolderPod       string
	HolderUID       string
	TakeoverToken   string
}

// Liveness is the optional capability a Catalog implements when it can report,
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
// over on a guess. A Catalog without this capability, and one that answers
// errors.ErrUnsupported, are treated the same way.
type Liveness interface {
	HolderState(ctx context.Context, lease Lease) (terminated, definitive bool, err error)
}

// UncertaintyObserver is the optional capability a Catalog implements when it
// can record a full survey's inventory digest in the claim's record.
//
// The observation is compare-and-swapped on the claim's token exactly as
// MarkUncertainty and ClearUncertainty are: a caller whose claim is gone or was
// taken over changes nothing. It sets the digest of the uncertainty the record
// already carries, keeping its RunID and Since; a record with no uncertainty is
// left without one, and a record whose uncertainty was cleared is not given a
// new one. One attempt per call, like every other catalog method.
//
// A Catalog that cannot observe answers by not implementing this interface: the
// record's uncertainty then never settles on its own, which costs availability
// and never guesses settlement.
type UncertaintyObserver interface {
	ObserveUncertainty(ctx context.Context, lease Lease, digest string) error
}
