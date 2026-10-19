package catalogtest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/catalog"
	"github.com/vi-latyshev/go-cluster-backup/internal/model"
)

// Leaser is an in-memory writer-lease claim with the same semantics the real
// catalog.Leaser capability has: a held claim refuses, and is taken over only by
// a request carrying its exact TakeoverToken. It also implements
// catalog.Liveness, with the verdict a test configures, so the two harnesses
// share one implementation and no test drives the lease path by hand.
type Leaser struct {
	mu       sync.Mutex
	lease    *catalog.Lease
	Acquires int
	// Terminated and Definitive are what HolderState answers for the current
	// holder (default: alive). Definitive models the kubelet's terminal phase; it
	// matters only to a scheduled takeover.
	Terminated bool
	Definitive bool
	// HolderErr, when set, makes the check fail — which the engine must read as
	// "alive".
	HolderErr error
	// HolderUnsupported makes HolderState answer errors.ErrUnsupported: a leaser
	// that cannot prove anything at all.
	HolderUnsupported bool
	// quarantine is the deadline the record carries into every acquisition,
	// whether or not a claim is held.
	quarantine time.Time
}

// NewLeaser returns a leaser whose holder counts as alive until a test says
// otherwise.
func NewLeaser() *Leaser { return &Leaser{} }

func (l *Leaser) Acquire(ctx context.Context, req catalog.LeaseRequest) (catalog.Lease, error) {
	if err := ctx.Err(); err != nil {
		return catalog.Lease{}, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.Acquires++
	free := l.lease == nil || l.lease.Holder == ""
	if !free && (req.TakeoverToken == "" || l.lease.Token != req.TakeoverToken) {
		return *l.lease, fmt.Errorf("held by run %s (%s): %w", l.lease.Holder, l.lease.Kind, catalog.ErrLeaseHeld)
	}
	quarantine := req.QuarantineUntil
	if l.quarantine.After(quarantine) {
		quarantine = l.quarantine
	}
	if l.lease != nil && l.lease.QuarantineUntil.After(quarantine) {
		quarantine = l.lease.QuarantineUntil
	}
	l.quarantine = quarantine
	taken := catalog.Lease{Holder: req.Holder, Kind: req.Kind,
		Token: fmt.Sprintf("%s/%d", req.Holder, l.Acquires), HolderNamespace: req.HolderNamespace,
		HolderPod: req.HolderPod, HolderUID: req.HolderUID, AcquiredAt: time.Now().UTC(),
		QuarantineUntil: quarantine}
	l.lease = &taken
	l.Terminated, l.Definitive = false, false
	return taken, nil
}

func (l *Leaser) Release(ctx context.Context, lease catalog.Lease) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.lease != nil && l.lease.Token == lease.Token {
		// The claim is dropped, the quarantine is not: it belongs to the record.
		l.lease = &catalog.Lease{QuarantineUntil: l.lease.QuarantineUntil}
	}
	return nil
}

// HolderState is the optional catalog.Liveness capability: the verdict a test
// set, or an error the engine must treat as "alive".
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
	l.lease = &catalog.Lease{Holder: holder, Kind: kind, Token: "held-by-" + holder,
		HolderNamespace: namespace, HolderPod: pod, HolderUID: uid, AcquiredAt: acquired}
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
	if l.lease == nil || l.lease.Holder == "" {
		return catalog.Lease{}, false
	}
	return *l.lease, true
}

// SetQuarantine arranges a quarantine deadline the next acquisition inherits,
// the way a takeover by an earlier run would leave it in the record.
func (l *Leaser) SetQuarantine(until time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.quarantine = until
}

// QuarantineUntil reads the quarantine deadline the record keeps across
// releases.
func (l *Leaser) QuarantineUntil() time.Time {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.lease == nil {
		return time.Time{}
	}
	return l.lease.QuarantineUntil
}

// AcquireCount reads the counter under the lock.
func (l *Leaser) AcquireCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.Acquires
}
