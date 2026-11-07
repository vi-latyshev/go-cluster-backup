package protection

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/execution"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/human"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/inventory"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/retention"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage/guard"
)

// LedgerOptions are the typed collaborators the assembly projects: the storage
// destinations (the only source of a Remover and of each instance's scoped
// logger), each storage's cap and retention policies, the execution policy
// deletion calls use, and the stale cutoff.
type LedgerOptions struct {
	Destinations []guard.Destination
	// Policies is each storage's retention policy per run kind. A kind absent
	// here has no policy.
	Policies map[string]map[model.RunKind]retention.Retention
	// Caps is max_total_bytes per storage; 0 or absent means no cap.
	Caps map[string]int64
	// StaleCutoff is the instant before which no live run can still be writing:
	// its deadline plus its finish allowance.
	StaleCutoff time.Time
	// Retry is the execution policy every deletion call runs under.
	Retry execution.Options
}

// Accounting is the capacity-only view of the protection ledger: the bookings a
// transfer makes for the objects it uploads. The interface is SEALED by its
// unexported marker method, which only a protection-owned type can implement:
// the concrete *Ledger (and any foreign type) cannot stand in for it, so a type
// assertion to *Ledger or to an issuer interface always fails. It carries no
// authority — no Authorize, no Ready/BindLease/Start, no survey, no retention
// and no deletion.
type Accounting interface {
	accountingView()
	Admit(storage, key string, size int64, manual bool) bool
	Shrink(storage, key string, size int64)
	Commit(storage, key string, size int64)
	Unreserve(storage, key string)
}

// accounting is the sealed wrapper a ledger hands out. It borrows the ledger;
// it has no issuer method and its dynamic type is this unexported wrapper, never
// the ledger itself.
type accounting struct{ l *Ledger }

func (accounting) accountingView() {}

func (a accounting) Admit(storage, key string, size int64, manual bool) bool {
	return a.l.Admit(storage, key, size, manual)
}

func (a accounting) Shrink(storage, key string, size int64) { a.l.Shrink(storage, key, size) }

func (a accounting) Commit(storage, key string, size int64) { a.l.Commit(storage, key, size) }

func (a accounting) Unreserve(storage, key string) { a.l.Unreserve(storage, key) }

// Accounting returns the sealed capacity view of this ledger for a non-Floor
// collaborator such as a transfer. The view names no authority method and its
// dynamic value is not a *Ledger.
func (l *Ledger) Accounting() Accounting { return accounting{l: l} }

// Ledger is one run's protection ledger: the occupancy of every storage, the
// admission its writes pass, and the ONE delete authority. It is the only holder
// of Destination.Remover() values; no port or handle can delete.
type Ledger struct {
	o        LedgerOptions
	removers map[string]guard.Remover
	logs     map[string]*slog.Logger
	order    []string
	caps     map[string]*capState
	state    *retentionState
	// started is set by Start, the completed full survey.
	started bool
	// lease is the run's claim, bound by the Floor right after Acquire.
	lease *Lease
}

var _ Accounting = accounting{}

// NewLedger binds the ledger to its collaborators.
func NewLedger(o LedgerOptions) *Ledger {
	l := &Ledger{o: o, removers: map[string]guard.Remover{}, logs: map[string]*slog.Logger{}, caps: map[string]*capState{}}
	for _, d := range o.Destinations {
		l.order = append(l.order, d.Name())
		l.removers[d.Name()] = d.Remover()
		l.logs[d.Name()] = d.Log()
		l.caps[d.Name()] = newCapState(o.Caps[d.Name()], 0, false)
	}
	return l
}

// BindLease binds this ledger to the run's held claim. The Floor calls it
// immediately after Acquire succeeds, before the survey. A ledger without a
// bound, held claim is not Ready.
func (l *Ledger) BindLease(lease *Lease) {
	if l == nil {
		return
	}
	l.lease = lease
}

// Ready reports whether the ledger holds the verified state an authority
// requires: at least one constructed destination and the completed full survey.
// A lease-free invocation (no catalog can hold a claim, so the Floor binds
// nothing) is ready for its scheduled run; once a lease is bound, its claim must
// still be held, because a released or zero claim is not authority. A zero or
// publicly constructed ledger is never ready.
func (l *Ledger) Ready() bool {
	if l == nil || !l.started || len(l.order) == 0 {
		return false
	}
	if l.lease == nil {
		return true // lease-free invocation: no catalog can hold a claim
	}
	_, held := l.lease.Claim()
	return held
}

// Authorize issues one run's write authority. The Floor calls it only after the
// writer lease is acquired, the run id is free and admission passed; closing the
// authority revokes every handle issued under it. A zero, publicly constructed
// or not-yet-verified ledger mints an EXPIRED authority instead: every Open and
// Handle call through it refuses, so authority exists only from verified
// protection state.
func (l *Ledger) Authorize(deadline time.Time) *guard.Authority {
	if !l.Ready() {
		return guard.NewAuthority(time.Time{})
	}
	return guard.NewAuthority(deadline)
}

// Storages lists the destination names in assembly order.
func (l *Ledger) Storages() []string { return append([]string(nil), l.order...) }

// Start applies the completed survey: each storage's occupancy is its listing
// bytes and a survey that did not finish leaves the storage unknown. The
// retention state is built from the same surveys.
func (l *Ledger) Start(surveys map[string]inventory.Survey) {
	for name, cap := range l.caps {
		s, ok := surveys[name]
		if !ok {
			cap.markUnknown()
			continue
		}
		cap.occupancy = surveyBytes(s)
		cap.known = s.OK
	}
	l.state = newRetentionState(surveys)
	l.started = true
}

// Unknown records that one storage's occupancy cannot be established.
func (l *Ledger) Unknown(name string) { l.caps[name].markUnknown() }

// MarkUncertain applies the run's unknown-occupancy policy in one call: the
// named run's prefix becomes the inventory the settlement proof compares, so no
// sweep may delete it, and every storage's occupancy becomes unknown — bytes an
// earlier writer's accepted commit may still add are uncounted, so cap eviction
// is closed until a terminal proof and a fresh survey settle the record. A
// caller cannot forget half of the policy.
func (l *Ledger) MarkUncertain(runID string) {
	l.state.markUncertain(runID)
	for _, name := range l.order {
		l.Unknown(name)
	}
}

// Admit asks whether one object's write may start. A manual write past the cap,
// or on an unknown occupancy, is refused; a scheduled write always proceeds and
// the cap phase frees room later.
func (l *Ledger) Admit(name, key string, size int64, manual bool) bool {
	return l.caps[name].reserve(key, size, manual)
}

// Commit books a written object: its reservation becomes occupancy.
func (l *Ledger) Commit(name, key string, size int64) { l.caps[name].commit(key, size) }

// Shrink lowers one object's reservation to the exact produced size.
func (l *Ledger) Shrink(name, key string, size int64) { l.caps[name].shrink(key, size) }

// Unreserve drops the booking of an object that was never written.
func (l *Ledger) Unreserve(name, key string) { l.caps[name].unreserve(key) }

// Used is one storage's occupancy as this run knows it.
func (l *Ledger) Used(name string) int64 { return l.caps[name].used() }

// Limits reports one storage's cap and the bytes held, for a refusal report.
func (l *Ledger) Limits(name string) (limit, used int64) { return l.caps[name].limits() }

// capState accounts one storage's occupancy against its configured cap and
// admits a manual run's writes. Occupancy is the survey's own listing bytes —
// every run of every kind, manifests included — plus what this run has written
// or is writing: an object reserved before its write is not reserved twice on a
// retry, and an unknown occupancy is never free.
type capState struct {
	cap       int64
	occupancy int64
	known     bool
	reserved  map[string]int64
	mu        sync.Mutex
}

func newCapState(cap, surveyBytes int64, known bool) *capState {
	return &capState{cap: cap, occupancy: surveyBytes, known: known, reserved: map[string]int64{}}
}

func (c *capState) reserve(key string, n int64, refuse bool) bool {
	if c.cap <= 0 {
		return true
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.reserved[key]; ok {
		return true // a retry of an object already reserved
	}
	var pending int64
	for _, v := range c.reserved {
		pending += v
	}
	if refuse && (!c.known || c.occupancy+pending+n > c.cap) {
		return false
	}
	c.reserved[key] = n
	return true
}

func (c *capState) shrink(key string, n int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if cur, ok := c.reserved[key]; ok && n >= 0 && n < cur {
		c.reserved[key] = n
	}
}

func (c *capState) unreserve(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.reserved, key)
}

func (c *capState) markUnknown() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.known = false
}

func (c *capState) commit(key string, n int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.reserved, key)
	c.occupancy += n
}

func (c *capState) releaseBytes(n int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.occupancy -= n
	if c.occupancy < 0 {
		c.occupancy = 0
	}
}

func (c *capState) used() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	var pending int64
	for _, v := range c.reserved {
		pending += v
	}
	return c.occupancy + pending
}

func (c *capState) limits() (limit, used int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var pending int64
	for _, v := range c.reserved {
		pending += v
	}
	return c.cap, c.occupancy + pending
}

// occupancyKnown reports whether the ledger is a trustworthy total: false when a
// survey or multipart inventory failed, or when the lease records that an
// earlier writer's accepted writes may still land.
func (c *capState) occupancyKnown() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.known
}

// surveyBytes is the real listing bytes of one storage under the run prefix:
// every run of every kind, manifests and partially delivered objects included.
func surveyBytes(sur inventory.Survey) int64 {
	if !sur.OK {
		return 0
	}
	var total int64
	for _, r := range sur.Runs {
		for _, o := range r.Objects {
			total += o.Size
		}
	}
	return total
}

// event writes one storage's line through that instance's own scoped logger:
// the kind/type/instance prefix is the one the assembly gave its Deps, never a
// second naming scheme built here.
func (l *Ledger) event(storage string, ev human.Event) {
	log := l.logs[storage]
	if log == nil {
		log = slog.Default()
	}
	log.Log(context.Background(), ev.Severity, ev.Name, slog.Any("event", ev))
}
