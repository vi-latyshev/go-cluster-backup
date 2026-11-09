package catalogtest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/catalog"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
)

// Leaser is an in-memory writer-lease record with the same semantics the real
// catalog has: a held claim refuses, a takeover needs its exact token, and the
// record's uncertainty — its pending-run set and digest — survives a release
// and is inherited by every later acquisition. It implements catalog.Liveness
// and catalog.UncertaintyObserver with the verdicts a test configures, so the
// conformance suite and the harnesses share one implementation and no test
// drives the lease path by hand.
type Leaser struct {
	mu     sync.Mutex
	record catalog.Lease
	// Acquires counts the acquisitions, so a harness can prove a run retried.
	Acquires int
	// Terminated and Definitive are what HolderState answers for the current
	// holder (default: alive). Definitive models the kubelet's terminal phase; it
	// matters only to a scheduled takeover.
	Terminated bool
	Definitive bool
	// HolderErr, when set, makes the check fail — which the protection lease must read as
	// "alive".
	HolderErr error
	// HolderUnsupported makes HolderState answer errors.ErrUnsupported: a leaser
	// that cannot prove anything at all.
	HolderUnsupported bool
}

// NewLeaser returns a record with no holder, counting its first acquisition as
// one.
func NewLeaser() *Leaser { return &Leaser{} }

func (l *Leaser) Acquire(ctx context.Context, req catalog.LeaseRequest) (catalog.Lease, error) {
	if err := ctx.Err(); err != nil {
		return catalog.Lease{}, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.Acquires++
	held := l.record
	free := held.Holder == ""
	if !free && (req.TakeoverToken == "" || held.Token != req.TakeoverToken) {
		return held, fmt.Errorf("held by run %s (%s): %w", held.Holder, held.Kind, catalog.ErrLeaseHeld)
	}
	uncertainty := held.Uncertainty
	if !free && uncertainty == nil {
		// A takeover replaces a holder that may still have provider operations in
		// flight: occupancy is unknown until a terminal proof and a fresh survey.
		uncertainty = &catalog.Uncertainty{RunIDs: []string{held.Holder}, Since: time.Now().UTC()}
	}
	l.record = catalog.Lease{Holder: req.Holder, Kind: req.Kind,
		Token: fmt.Sprintf("%s/%d", req.Holder, l.Acquires), HolderNamespace: req.HolderNamespace,
		HolderPod: req.HolderPod, HolderUID: req.HolderUID, AcquiredAt: time.Now().UTC(),
		Uncertainty: uncertainty}
	l.Terminated, l.Definitive = false, false
	return l.record, nil
}

func (l *Leaser) MarkUncertainty(ctx context.Context, lease catalog.Lease) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.record.Holder == "" || l.record.Token != lease.Token || lease.Uncertainty == nil {
		return nil
	}
	// The caller decides the pending set; the record takes it as handed.
	next := *lease.Uncertainty
	next.RunIDs = append([]string(nil), lease.Uncertainty.RunIDs...)
	l.record.Uncertainty = &next
	return nil
}

func (l *Leaser) ClearUncertainty(ctx context.Context, lease catalog.Lease) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.record.Holder == "" || l.record.Token != lease.Token {
		return nil
	}
	l.record.Uncertainty = nil
	return nil
}

func (l *Leaser) ObserveUncertainty(ctx context.Context, lease catalog.Lease, digest string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.record.Holder == "" || l.record.Token != lease.Token || l.record.Uncertainty == nil {
		return nil
	}
	l.record.Uncertainty.Digest = digest
	return nil
}

func (l *Leaser) Release(ctx context.Context, lease catalog.Lease) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.record.Holder == "" || l.record.Token != lease.Token {
		return nil
	}
	// The claim is dropped, the uncertainty is not: it belongs to the record.
	l.record = catalog.Lease{Uncertainty: l.record.Uncertainty}
	return nil
}

// HolderState is the optional catalog.Liveness capability: the verdict a test
// set, or an error the protection lease must treat as "alive".
func (l *Leaser) HolderState(ctx context.Context, lease catalog.Lease) (terminated, definitive bool, err error) {
	if err := ctx.Err(); err != nil {
		return false, false, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.HolderUnsupported {
		return false, false, fmt.Errorf("this leaser cannot tell: %w", errors.ErrUnsupported)
	}
	if l.HolderErr != nil {
		return false, false, l.HolderErr
	}
	return l.Terminated, l.Definitive, nil
}

// Hold arranges a claim a run must respect, with the given process identity and
// acquisition age.
func (l *Leaser) Hold(holder string, kind model.RunKind, namespace, pod, uid string, acquired time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.record.Holder = holder
	l.record.Kind = kind
	l.record.Token = "held-by-" + holder
	l.record.HolderNamespace = namespace
	l.record.HolderPod = pod
	l.record.HolderUID = uid
	l.record.AcquiredAt = acquired
}

// SetUncertainty arranges an uncertainty the record carries into every
// acquisition, the way a takeover by an earlier run would leave it.
func (l *Leaser) SetUncertainty(uncertainty *catalog.Uncertainty) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.record.Uncertainty = uncertainty
}

// Uncertainty reads the record's uncertainty, whether or not a claim is held.
func (l *Leaser) Uncertainty() *catalog.Uncertainty {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.record.Uncertainty
}

// SetTerminated configures the liveness verdict for the current holder.
func (l *Leaser) SetTerminated(terminated, definitive bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.Terminated, l.Definitive = terminated, definitive
}

// SetHolderErr makes the next liveness check fail.
func (l *Leaser) SetHolderErr(err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.HolderErr = err
}

// Held returns the claim the fake currently holds, and whether there is one.
func (l *Leaser) Held() (catalog.Lease, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.record.Holder == "" {
		return catalog.Lease{}, false
	}
	return l.record, true
}

// AcquireCount reads the counter under the lock.
func (l *Leaser) AcquireCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.Acquires
}
