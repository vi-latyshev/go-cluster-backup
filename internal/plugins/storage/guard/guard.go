// Package guard is the mutation boundary of the storage seam. A workflow, an
// uploader or a restore path receives a read-only Port or a write Handle, never
// a storage instance: the raw instance stays behind internal/plugins/storage.
//
// A Handle exists only when the run's Authority admitted it — the run acquired
// its writer lease, found its id free and passed admission — and every call
// re-checks that authority: a zero, closed or expired handle refuses. Deletion
// has no method here at all: it belongs to the protection ledger, which alone
// holds a Remover.
package guard

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"time"

	pluginerrors "github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage"
	raw "github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage/internal"
)

// The three refusals are permanent: a caller without authority cannot retry its
// way into one, and a destination that was never built cannot become built.
var (
	// ErrNoAuthority refuses a zero, closed or expired handle or authority.
	ErrNoAuthority = errors.New("storage guard: no write authority")
	// ErrNoReadPort refuses a zero read port.
	ErrNoReadPort = errors.New("storage guard: no read port")
	// ErrNoDestination refuses a destination that was never built.
	ErrNoDestination = errors.New("storage guard: destination was not built")
)

// Port is the read-only port of one destination: what a survey, a restore or a
// verification may do. It carries no method that could mutate anything, and a
// zero Port refuses instead of dereferencing nothing.
type Port struct {
	name  string
	inner storage.Reader
	log   *slog.Logger
}

// Read returns the read-only port of one destination. The assembly builds
// ports from Destination.Port; this constructor exists for the places that hold
// a Reader already, such as a conformance kit.
func Read(name string, r storage.Reader) Port {
	return Port{name: name, inner: r}
}

// Name is the instance name the port was issued for.
func (p Port) Name() string { return p.name }

// Log is the instance's scoped logger, carrying the kind/type/instance
// attributes the assembly gave its Deps.
func (p Port) Log() *slog.Logger { return p.log }

// Describe reports what the destination declares about itself.
func (p Port) Describe(ctx context.Context) (storage.StorageTraits, error) {
	if p.inner == nil {
		return storage.StorageTraits{}, pluginerrors.Permanent(ErrNoReadPort)
	}
	return p.inner.Describe(ctx)
}

// Stat reports one object.
func (p Port) Stat(ctx context.Context, key string) (model.ObjectInfo, error) {
	if p.inner == nil {
		return model.ObjectInfo{}, pluginerrors.Permanent(ErrNoReadPort)
	}
	return p.inner.Stat(ctx, key)
}

// List pages a listing.
func (p Port) List(ctx context.Context, req storage.ListRequest) (storage.ListPage, error) {
	if p.inner == nil {
		return storage.ListPage{}, pluginerrors.Permanent(ErrNoReadPort)
	}
	return p.inner.List(ctx, req)
}

// Get reads one object into dst.
func (p Port) Get(ctx context.Context, key string, dst io.Writer) error {
	if p.inner == nil {
		return pluginerrors.Permanent(ErrNoReadPort)
	}
	return p.inner.Get(ctx, key, dst)
}

// Authority is one run's write authority. The run's owner creates it after the
// writer lease is acquired, the run id is found free and admission passed, and
// it expires at the run's write deadline. It is the only source of write
// handles; closing it revokes every handle it ever issued.
type Authority struct {
	deadline time.Time
	now      func() time.Time
	closed   atomic.Bool
}

// NewAuthority creates the write authority of one run. It is the protection
// ledger's constructor: the assembly's workflow calls Ledger.Authorize, never
// this directly, and tools/rules restricts the symbol to
// internal/plugins/protection. A zero or past deadline is admitted as expired:
// every handle it would issue refuses, which is the conservative answer to a
// caller that could not name its write window.
func NewAuthority(deadline time.Time) *Authority {
	return &Authority{deadline: deadline, now: time.Now}
}

// Close revokes the authority and every handle issued under it.
func (a *Authority) Close() {
	if a != nil {
		a.closed.Store(true)
	}
}

// Expired reports whether a handle issued now would refuse.
func (a *Authority) Expired() bool { return a.check() != nil }

func (a *Authority) check() error {
	if a == nil || a.now == nil || a.closed.Load() || !a.now().Before(a.deadline) {
		return pluginerrors.Permanent(ErrNoAuthority)
	}
	return nil
}

// Destination is one constructed storage instance behind the raw boundary. The
// assembly holds destinations; a workflow receives their Port or a Handle, never
// the destination itself.
type Destination struct {
	name string
	raw  raw.Raw
	// log is the instance's own scoped logger, the one its construction Deps
	// carried: every line the safety owner emits for this destination goes
	// through it, so the kind, type and instance prefix stays the same as every
	// other storage line.
	log *slog.Logger
}

// Build constructs one destination through the storage factory and keeps the
// instance behind the raw boundary. It is the only construction path the
// assembly uses; the instance's Deps logger travels with it.
func Build(ctx context.Context, r *instances.Registry, typ string, cfg jsontext.Value,
	in storage.BuildInput, d instances.Deps) (Destination, error) {
	s, err := storage.Build(ctx, r, typ, cfg, in, d)
	if err != nil {
		return Destination{}, err
	}
	return Destination{name: d.Instance, raw: raw.Raw{Storage: s}, log: d.Log}, nil
}

// Name is the instance name the destination was built for.
func (d Destination) Name() string { return d.name }

// Log is the instance's scoped logger, carrying the kind/type/instance
// attributes the assembly gave its Deps.
func (d Destination) Log() *slog.Logger { return d.log }

// Port returns the read-only port of the destination. It is the only surface a
// workflow receives: reads, metadata and Open under a live Authority.
func (d Destination) Port() Port {
	return Port{name: d.name, inner: d.raw.Storage, log: d.log}
}

// Close releases the constructed storage when its plugin implements io.Closer;
// a storage without Close is a no-op, and a zero destination is a no-op. The
// invocation's Scope calls it exactly once, in reverse construction order,
// after the operation returned (or after the join on the cancel path). The raw
// instance itself never leaves this package.
func (d Destination) Close() error {
	if d.raw.Storage == nil {
		return nil
	}
	if c, ok := d.raw.Storage.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

// Remover returns the delete primitive of this destination. Only the protection
// ledger holds one: a workflow never sees a Destination, and neither Port nor
// Handle has a delete method.
func (d Destination) Remover() Remover { return Remover{inner: d.raw.Storage} }

// Open issues one write handle of this port under one authority. A port whose
// reader is not a constructed storage, or a zero, closed or expired authority,
// refuses. It is the only mutation entry a workflow has.
func (p Port) Open(a *Authority) (Handle, error) {
	s, ok := p.inner.(storage.Storage)
	if !ok || s == nil {
		return Handle{}, pluginerrors.Permanent(ErrNoDestination)
	}
	if err := a.check(); err != nil {
		return Handle{}, err
	}
	return Handle{state: &handleState{name: p.name, inner: s, auth: a}}, nil
}

// WritesAsynchronously reports whether the destination can hand a provider a
// write that settles after it returns — the multipart capability, in practice.
// It is a metadata probe, not an authority: the safety owner reads it to decide
// whether the run's record must be marked uncertain before the first write.
func (p Port) WritesAsynchronously() bool {
	if s, ok := p.inner.(storage.Storage); ok {
		return storage.WritesAsynchronously(s)
	}
	if w, ok := p.inner.(storage.AsynchronousWriter); ok {
		return w.WritesAsynchronously()
	}
	_, ok := p.inner.(storage.MultipartUploader)
	return ok
}

// Check runs the destination's optional preflight check — credentials,
// reachability, an expected mount — when the backend implements
// instances.Checker. A backend without the capability has nothing to check and
// reports nil; a zero port refuses. It needs no write authority: the check
// reads, never mutates. Port itself satisfies instances.Checker, so the
// assembly can hand it to preflight as the storage's checker.
func (p Port) Check(ctx context.Context) error {
	if p.inner == nil {
		return pluginerrors.Permanent(ErrNoReadPort)
	}
	checker, ok := p.inner.(instances.Checker)
	if !ok {
		return nil
	}
	return checker.Check(ctx)
}

// Handle is one destination's write handle. It is issued only by
// Destination.Open under a live Authority, and it carries no exported state: a
// zero Handle refuses every call, and copies share the authority's revocation.
type Handle struct {
	state *handleState
}

type handleState struct {
	name  string
	inner storage.Storage
	auth  *Authority
}

// Name is the instance name the handle was issued for.
func (h Handle) Name() string {
	if h.state == nil {
		return ""
	}
	return h.state.name
}

// Port returns the destination's read-only port. Reading is not a mutation, so
// it stays available after the authority expires.
func (h Handle) Port() Port {
	if h.state == nil {
		return Port{}
	}
	return Port{name: h.state.name, inner: h.state.inner}
}

func (h Handle) check() error {
	if h.state == nil {
		return pluginerrors.Permanent(ErrNoAuthority)
	}
	return h.state.auth.check()
}

// Put stores one object under the handle's authority.
func (h Handle) Put(ctx context.Context, req storage.PutRequest, body io.Reader) (model.ObjectInfo, error) {
	if err := h.check(); err != nil {
		return model.ObjectInfo{}, err
	}
	return h.state.inner.Put(ctx, req, body)
}

// Uploader reports the guarded multipart capability of the destination. The
// second result is false when the destination uploads whole objects only.
func (h Handle) Uploader() (Uploader, bool) {
	if h.state == nil {
		return Uploader{}, false
	}
	mu, ok := h.state.inner.(storage.MultipartUploader)
	if !ok {
		return Uploader{}, false
	}
	return Uploader{state: &uploaderState{handle: h.state, inner: mu}}, true
}

// Uploader is the guarded multipart capability of one handle: every call that
// can create or discover data checks the authority, exactly like Put.
type Uploader struct {
	state *uploaderState
}

type uploaderState struct {
	handle *handleState
	inner  storage.MultipartUploader
}

func (u Uploader) check() error {
	if u.state == nil {
		return pluginerrors.Permanent(ErrNoAuthority)
	}
	return u.state.handle.auth.check()
}

// BeginMultipart opens one upload under the authority.
func (u Uploader) BeginMultipart(ctx context.Context, req storage.PutRequest) (storage.MultipartSession, error) {
	if err := u.check(); err != nil {
		return storage.MultipartSession{}, err
	}
	return u.state.inner.BeginMultipart(ctx, req)
}

// PutPart sends one part under the authority.
func (u Uploader) PutPart(ctx context.Context, session storage.MultipartSession, part storage.Part) (storage.PartResult, error) {
	if err := u.check(); err != nil {
		return storage.PartResult{}, err
	}
	return u.state.inner.PutPart(ctx, session, part)
}

// CompleteMultipart commits one upload under the authority.
func (u Uploader) CompleteMultipart(ctx context.Context, session storage.MultipartSession, parts []storage.PartResult) error {
	if err := u.check(); err != nil {
		return err
	}
	return u.state.inner.CompleteMultipart(ctx, session, parts)
}

// AbortMultipart discards one upload. It deliberately does not check the
// authority: aborting destroys an upload this caller already holds and can
// never create data, and cleanup must still run after cancellation, expiry or
// Close. A zero Uploader refuses; enumeration (StaleMultipart, Inventory) does
// check the authority, so a stale handle cannot discover a successor's uploads.
func (u Uploader) AbortMultipart(ctx context.Context, session storage.MultipartSession) error {
	if u.state == nil {
		return pluginerrors.Permanent(ErrNoAuthority)
	}
	return u.state.inner.AbortMultipart(ctx, session)
}

// StaleMultipart lists the incomplete uploads older than the cutoff under the
// authority. It drives destructive cleanup, so it never runs without one.
func (u Uploader) StaleMultipart(ctx context.Context, olderThan time.Time) ([]storage.StaleUpload, error) {
	if err := u.check(); err != nil {
		return nil, err
	}
	return u.state.inner.StaleMultipart(ctx, olderThan)
}

// Inventory is the guarded full-inventory capability of one handle: every
// incomplete upload under the destination's own root, of any age. It is
// independent of the multipart upload capability, because a destination can
// report foreign uploads without being able to upload parts itself.
type Inventory struct {
	state *inventoryState
}

type inventoryState struct {
	handle *handleState
	inner  storage.MultipartInventory
}

// Inventory reports the guarded inventory capability of the destination. The
// second result is false when the destination cannot list incomplete uploads.
func (h Handle) Inventory() (Inventory, bool) {
	if h.state == nil {
		return Inventory{}, false
	}
	inv, ok := h.state.inner.(storage.MultipartInventory)
	if !ok {
		return Inventory{}, false
	}
	return Inventory{state: &inventoryState{handle: h.state, inner: inv}}, true
}

// List lists every incomplete upload under the authority. A zero Inventory, and
// one whose authority is closed or expired, refuses: listing drives destructive
// cleanup, so it never runs without a live grant.
func (i Inventory) List(ctx context.Context) ([]storage.StaleUpload, error) {
	if i.state == nil {
		return nil, pluginerrors.Permanent(ErrNoAuthority)
	}
	if err := i.state.handle.auth.check(); err != nil {
		return nil, err
	}
	return i.state.inner.Inventory(ctx)
}

// Remover is the delete primitive of one destination. It exists for the
// protection ledger alone: the ledger decides what may be deleted and is the
// only owner of a Remover. A zero Remover refuses.
type Remover struct {
	inner storage.Storage
}

// Delete removes one object. Deletion is idempotent only in the storage seam's
// sense: a definitely absent key is success.
func (r Remover) Delete(ctx context.Context, key string) error {
	if r.inner == nil {
		return pluginerrors.Permanent(ErrNoDestination)
	}
	return r.inner.Delete(ctx, key)
}
