package transfer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/execution"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/human"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/pipeline"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/protection"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/spool"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage/guard"
)

// Options are one destination's typed transfer collaborators in one run: the
// retry policy and budget clock of its calls, its already-scoped logger, the
// protection ledger its overlapping upload books against, and the configured
// artifact bound and manual admission.
type Options struct {
	Retry execution.Options
	Clock *execution.Clock
	Log   *slog.Logger
	// Accounting is the sealed capacity-only view: the bookings this transfer
	// makes for the objects it uploads. It carries no authority.
	Accounting       protection.Accounting
	Storage          string
	MaxArtifactBytes int64
	Manual           bool
	CallParallelism  int
}

// Transfer drives one destination's multipart uploads in one run. The handle it
// is given is the only write authority; the Transfer keeps the upload IDs it
// opened, so a cancelled invocation can abort exactly its own under one shared
// deadline.
type Transfer struct {
	o   Options
	mu  sync.Mutex
	own map[string]ownUpload
}

type ownUpload struct {
	uploader guard.Uploader
	session  storage.MultipartSession
}

// New binds a transfer to one destination's options. The call parallelism is the
// storage's own ceiling and is always at least one part.
func New(o Options) *Transfer {
	if o.CallParallelism < 1 {
		o.CallParallelism = 1
	}
	return &Transfer{o: o, own: map[string]ownUpload{}}
}

// Request identifies one object and, for the finished path, the spool file that
// holds its bytes.
type Request struct {
	Key    string
	Label  string
	File   string
	Size   int64
	SHA256 string
}

// Outcome reports what one transfer decided and did. Handled false leaves the
// whole object to the caller — a destination without the capability, an object
// that fits one request, a refused overlap, or a producer that never produced.
// Started says the upload's opening event was already emitted.
type Outcome struct {
	Handled  bool
	Started  bool
	Attempts int
	Parts    int
	// Bytes is the size of the object this transfer committed to the service;
	// zero when it handled nothing.
	Bytes int64
}

// Upload sends one finished object through the guarded multipart capability. It
// reports Handled false when the destination uploads whole objects or answers
// errors.ErrUnsupported for this size, so the caller keeps its Put.
func (t *Transfer) Upload(ctx context.Context, h guard.Handle, sp *spool.Spool, req Request) (Outcome, error) {
	u, ok := h.Uploader()
	if !ok || req.Size <= 0 {
		return Outcome{}, nil
	}
	session, attempts, err := t.begin(ctx, u, storage.PutRequest{Key: req.Key, Size: req.Size, SHA256: req.SHA256}, req.Label)
	if errors.Is(err, errors.ErrUnsupported) {
		return Outcome{}, nil
	}
	out := Outcome{Handled: true, Attempts: attempts}
	if err != nil {
		return out, err
	}
	parts, partAttempts, err := t.uploadParts(ctx, u, sp, &finishedSource{name: req.File, size: req.Size}, session, req.Label)
	out.Attempts += partAttempts
	if err != nil {
		return out, errors.Join(err, t.abort(ctx, u, session, req.Key))
	}
	out.Parts = len(parts)
	out.Bytes = req.Size
	t.multipartEvent(req.Label, req.Size, session, len(parts))
	attempts, err = t.finish(ctx, h, u, session, parts, req.Key, req.Label, req.Size)
	out.Attempts += attempts
	return out, err
}

// Follow drives one destination through an artifact that is still being
// produced. It books the run's artifact bound in the ledger, asks the plugin for
// an upload as the file grows, and sends every part whose bytes are final. When
// the producer finishes it sends the tail, commits, verifies and books the exact
// size.
//
// A producer that fails, a refused overlap and a destination that only answers
// errors.ErrUnsupported leave Handled false: the caller uploads the finished
// file with its own whole-object path.
func (t *Transfer) Follow(ctx context.Context, h guard.Handle, sp *spool.Spool, g *pipeline.Growth, req Request) (Outcome, error) {
	u, ok := h.Uploader()
	if !ok {
		return Outcome{}, nil
	}
	if t.o.Accounting != nil &&
		!t.o.Accounting.Admit(t.o.Storage, req.Key, t.o.MaxArtifactBytes, t.o.Manual) {
		return Outcome{}, nil
	}
	src := &followSource{g: g}
	started := false
	var out Outcome
	var session storage.MultipartSession
	opened := false
	probed := int64(-1)
	var final int64
	for !opened {
		n, done, err := src.next(ctx, probed)
		if err != nil {
			return t.abandon(ctx, u, opened, session, req, started, out.Attempts)
		}
		if done {
			final = n
			break
		}
		if n <= 0 {
			// Nothing is final yet: there is no size to ask the plugin about.
			probed = n
			continue
		}
		if !started {
			t.event(human.New("storage.upload.start", human.Text("artifact", req.Label)))
			started = true
		}
		var attempts int
		session, opened, attempts, err = t.probeBegin(ctx, u, req.Key, n, req.Label)
		out.Attempts += attempts
		if err != nil {
			return t.failFollow(ctx, u, session, opened, req, err, started, out.Attempts)
		}
		probed = n
	}
	if !opened {
		// The object fits one request, or the producer finished before a session
		// could open: the finished file decides, as it always has.
		if t.o.Accounting != nil {
			t.o.Accounting.Shrink(t.o.Storage, req.Key, final)
		}
		return Outcome{Started: started, Attempts: out.Attempts}, nil
	}
	parts, partAttempts, err := t.uploadParts(ctx, u, sp, src, session, req.Label)
	out.Attempts += partAttempts
	out.Handled, out.Started = true, true
	if err != nil {
		// The upload recorded where its cause came from. Only the producer's OWN
		// failure falls back to the finished file; a part's failure and the run's
		// cancellation are reported as the cause the upload chose — never replaced
		// by the context's state at this later moment.
		origin, cause := terminalOf(err)
		if origin == originSource {
			return t.abandon(ctx, u, true, session, req, started, out.Attempts)
		}
		return t.failFollow(ctx, u, session, true, req, cause, true, out.Attempts)
	}
	_, finalSize, _, _, _ := g.Snapshot()
	out.Bytes = finalSize
	if t.o.Accounting != nil {
		t.o.Accounting.Shrink(t.o.Storage, req.Key, finalSize)
	}
	t.multipartEvent(req.Label, finalSize, session, len(parts))
	attempts, err := t.finish(ctx, h, u, session, parts, req.Key, req.Label, finalSize)
	out.Attempts += attempts
	if err != nil {
		return out, err
	}
	if t.o.Accounting != nil {
		t.o.Accounting.Commit(t.o.Storage, req.Key, finalSize)
	}
	return out, nil
}

// abandon ends an overlapping upload because the producer failed — never the
// destination's own fault. The unfinished upload is aborted once and its booking
// released when the abort confirmed it, and the caller keeps the whole-object
// fallback.
func (t *Transfer) abandon(ctx context.Context, u guard.Uploader, opened bool, session storage.MultipartSession,
	req Request, started bool, attempts int) (Outcome, error) {
	if opened {
		if err := t.abort(ctx, u, session, req.Key); err != nil {
			// The parts may still exist and are billed: the booking stays, and
			// this destination never reuses the key to upload the retry's file.
			return Outcome{Handled: true, Started: started, Attempts: attempts},
				fmt.Errorf("aborting the failed attempt: %w", err)
		}
	}
	if t.o.Accounting != nil {
		t.o.Accounting.Unreserve(t.o.Storage, req.Key)
	}
	return Outcome{Started: started, Attempts: attempts}, nil
}

// failFollow ends an overlapping upload with the destination's own failure: the
// unfinished upload is aborted once — the failure that got here already returned
// its in-flight part — and the booking is released when the abort confirmed
// nothing is left.
func (t *Transfer) failFollow(ctx context.Context, u guard.Uploader, session storage.MultipartSession, opened bool,
	req Request, cause error, started bool, attempts int) (Outcome, error) {
	if opened {
		if err := t.abort(ctx, u, session, req.Key); err == nil && t.o.Accounting != nil {
			t.o.Accounting.Unreserve(t.o.Storage, req.Key)
		}
	} else if t.o.Accounting != nil {
		t.o.Accounting.Unreserve(t.o.Storage, req.Key)
	}
	return Outcome{Handled: true, Started: started, Attempts: attempts}, cause
}

// Sweep takes the destination's full multipart inventory and aborts the uploads
// a killed run left behind: a pod lost outright mid-upload leaves parts that are
// billed, that no retention and no listing ever sees, and that only the next run
// can come back for. It never fails the run: an upload that cannot be swept
// stays billed.
//
// The inventory is every upload of any age, undated included; any upload it
// finds — or a failed inventory — leaves bytes no listing shows, so the
// destination's occupancy is UNKNOWN. A destination without the inventory
// capability cannot create such parts and stays known. The stale listing, cut at
// now minus the longest a live run can last, only decides which uploads are
// aborted, so an upload a run still might be writing is never taken; a
// destination without the upload capability has nothing to abort. It reports the
// attempts made and whether the occupancy is unknown.
func (t *Transfer) Sweep(ctx context.Context, h guard.Handle, cutoff time.Time) (attempts int, unknown bool) {
	if inv, ok := h.Inventory(); ok {
		var inventory []storage.StaleUpload
		_, err := execution.Call(t.o.Clock, ctx, func(ctx context.Context) (struct{}, error) {
			return execution.Attempt(ctx, t.o.Retry, "inventory multipart "+t.o.Storage, func(ctx context.Context, _ int) (struct{}, error) {
				attempts++
				var ierr error
				inventory, ierr = execution.SafeValue(func() ([]storage.StaleUpload, error) { return inv.List(ctx) })
				return struct{}{}, ierr
			})
		})
		switch {
		case err != nil:
			t.event(human.Failure("multipart.inventory.failed",
				human.Text("reason", "the foreign parts stay uncounted, occupancy is unknown: "+err.Error())))
			unknown = true
		case len(inventory) > 0:
			unknown = true
		}
	}
	u, ok := h.Uploader()
	if !ok {
		return attempts, unknown
	}
	var stale []storage.StaleUpload
	_, err := execution.Call(t.o.Clock, ctx, func(ctx context.Context) (struct{}, error) {
		return execution.Attempt(ctx, t.o.Retry, "list stale multipart "+t.o.Storage, func(ctx context.Context, _ int) (struct{}, error) {
			attempts++
			var ierr error
			stale, ierr = execution.SafeValue(func() ([]storage.StaleUpload, error) { return u.StaleMultipart(ctx, cutoff) })
			return struct{}{}, ierr
		})
	})
	if err != nil {
		t.event(human.Failure("multipart.stale-list.failed",
			human.Text("reason", "uploads a killed run left stay billed: "+err.Error())))
		return attempts, true
	}
	for _, up := range stale {
		session := storage.MultipartSession{Key: up.Key, UploadID: up.UploadID}
		_, err := execution.Attempt(ctx, t.o.Retry, "abort stale multipart "+up.Key, func(ctx context.Context, _ int) (struct{}, error) {
			attempts++
			return struct{}{}, execution.SafeCall(func() error { return u.AbortMultipart(ctx, session) })
		})
		if err != nil {
			unknown = true
			t.event(human.Failure("multipart.stale-abort.failed",
				human.Text("reason", "the parts stay billed: "+err.Error()), human.ObjectKey("key", up.Key)))
			continue
		}
		t.event(human.New("multipart.stale-aborted",
			human.Text("run", "a killed run left this upload behind"),
			human.Text("initiated", up.Initiated.Format(time.RFC3339)), human.ObjectKey("key", up.Key)))
	}
	return attempts, unknown
}

// AbortOwn aborts exactly the upload IDs this transfer opened and never
// completed, on the context it is given: the invocation's cleanup owner hands it
// the one detached, bounded CleanupContext shared by every shutdown step, so the
// abort still runs after the run's own context is gone. The transfer never
// detaches or extends that context itself, and the service call runs on it
// unchanged. It creates nothing and deletes nothing: an abort can only destroy
// an upload this run already holds. A failed abort is joined; the parts stay
// billed.
func (t *Transfer) AbortOwn(ctx context.Context) error {
	t.mu.Lock()
	own := make([]ownUpload, 0, len(t.own))
	for _, o := range t.own {
		own = append(own, o)
	}
	t.mu.Unlock()
	var errs []error
	for _, o := range own {
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}
		if err := t.abort(ctx, o.uploader, o.session, o.session.Key); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// track remembers one open upload for the shutdown abort.
func (t *Transfer) track(u guard.Uploader, session storage.MultipartSession) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.own[session.UploadID] = ownUpload{uploader: u, session: session}
}

// forget drops one upload the service no longer holds: it was committed or
// aborted successfully.
func (t *Transfer) forget(session storage.MultipartSession) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.own, session.UploadID)
}

// probeBegin asks the destination whether the bytes produced so far already need
// parts. errors.ErrUnsupported means "not yet" and is not a failure.
func (t *Transfer) probeBegin(ctx context.Context, u guard.Uploader, key string, size int64, label string) (storage.MultipartSession, bool, int, error) {
	session, attempts, err := t.begin(ctx, u, storage.PutRequest{Key: key, Size: size}, label)
	if errors.Is(err, errors.ErrUnsupported) {
		return storage.MultipartSession{}, false, attempts, nil
	}
	if err != nil {
		return storage.MultipartSession{}, false, attempts, err
	}
	return session, true, attempts, nil
}

// begin opens one upload. errors.ErrUnsupported is the plugin's answer "this
// object is one Put" — a decision, not a fault, so it books no attempt.
func (t *Transfer) begin(ctx context.Context, u guard.Uploader, req storage.PutRequest, label string) (storage.MultipartSession, int, error) {
	var session storage.MultipartSession
	var attempts int
	_, err := execution.Call(t.o.Clock, ctx, func(ctx context.Context) (struct{}, error) {
		return execution.Attempt(ctx, t.o.Retry, "begin multipart "+label, func(ctx context.Context, _ int) (struct{}, error) {
			s, err := execution.SafeValue(func() (storage.MultipartSession, error) { return u.BeginMultipart(ctx, req) })
			if errors.Is(err, errors.ErrUnsupported) {
				return struct{}{}, pluginerrors.Permanent(err)
			}
			attempts++
			if err != nil {
				return struct{}{}, err
			}
			if s.PartSize <= 0 {
				return struct{}{}, pluginerrors.Permanent(fmt.Errorf("multipart %s: the plugin answered a part size of %d", req.Key, s.PartSize))
			}
			session = s
			return struct{}{}, nil
		})
	})
	if err != nil {
		return storage.MultipartSession{}, attempts, err
	}
	t.track(u, session)
	return session, attempts, nil
}

// finish commits a complete part list and proves the object is there. A commit
// whose answer was lost leaves the upload gone and the object there: the service
// answers not_found for the upload the transfer already completed, and the Stat
// decides, exactly as it does after a completion that claimed success — the
// object is the only proof either way. Every failure hands the unfinished upload
// to the abort.
func (t *Transfer) finish(ctx context.Context, h guard.Handle, u guard.Uploader, session storage.MultipartSession,
	parts []storage.PartResult, key, label string, size int64) (int, error) {
	attempts, err := t.complete(ctx, u, session, parts)
	if err != nil {
		if class, _ := pluginerrors.ClassOf(err); class == pluginerrors.ClassNotFound {
			a, verr := t.verify(ctx, h, key, label, size)
			attempts += a
			if verr == nil {
				return attempts, nil
			}
		}
		return attempts, errors.Join(err, t.abort(ctx, u, session, key))
	}
	a, err := t.verify(ctx, h, key, label, size)
	attempts += a
	if err != nil {
		return attempts, errors.Join(err, t.abort(ctx, u, session, key))
	}
	return attempts, nil
}

// multipartEvent reports one upload's shape after its parts were accepted.
func (t *Transfer) multipartEvent(label string, size int64, session storage.MultipartSession, parts int) {
	t.event(human.New("storage.multipart",
		human.Text("artifact", label),
		human.Count("parts", int64(parts)),
		human.Count("total", (size+session.PartSize-1)/session.PartSize),
		human.Bytes("part_size", session.PartSize)))
}

// verify proves a completed upload is there: the object at the size that was
// sent, under the retry policy every other Stat gets.
func (t *Transfer) verify(ctx context.Context, h guard.Handle, key, label string, size int64) (int, error) {
	attempts := 0
	_, err := execution.Call(t.o.Clock, ctx, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, execution.AttemptErr(ctx, t.o.Retry, "verify "+label, func(ctx context.Context, _ int) error {
			attempts++
			return t.verifyOnce(ctx, h, label, key, size)
		})
	})
	return attempts, err
}

// verifyOnce is one Stat: the object at the expected size, or a transient
// failure. An object that is not visible right after its commit is a failed
// commit of this call, never the not_found of an absent key.
func (t *Transfer) verifyOnce(ctx context.Context, h guard.Handle, label, key string, size int64) error {
	stt, err := execution.SafeValue(func() (model.ObjectInfo, error) { return h.Port().Stat(ctx, key) })
	if err != nil {
		if class, _ := pluginerrors.ClassOf(err); class == pluginerrors.ClassNotFound {
			return pluginerrors.Transient(fmt.Errorf("object %s is not visible after the upload (not committed)", label))
		}
		return err
	}
	if stt.Size != size {
		return pluginerrors.Transient(fmt.Errorf("size after upload is %d, want %d: the upload is not committed", stt.Size, size))
	}
	return nil
}

// complete commits the upload. Retrying it is safe: the parts are named by
// number and ETag, so a commit that already happened is answered as an upload
// that is gone, and the verification after it decides what actually landed.
func (t *Transfer) complete(ctx context.Context, u guard.Uploader, session storage.MultipartSession, parts []storage.PartResult) (int, error) {
	attempts := 0
	_, err := execution.Call(t.o.Clock, ctx, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, execution.AttemptErr(ctx, t.o.Retry, "complete multipart "+session.Key, func(ctx context.Context, _ int) error {
			attempts++
			return execution.SafeCall(func() error { return u.CompleteMultipart(ctx, session, parts) })
		})
	})
	if err != nil {
		return attempts, err
	}
	t.forget(session)
	return attempts, nil
}

// abort discards an unfinished upload on the context it is given — the upload's
// own context while the run still writes, or the cleanup owner's detached,
// bounded CleanupContext once the invocation shuts down. It never detaches or
// extends that context itself: bounding all of this run's aborts together belongs
// to the one cleanup deadline, and the service call runs on the context
// unchanged. It deliberately does not need a live write authority, because an
// abort destroys an upload this run already holds and can never create data. A
// failed abort is logged and returned: the parts stay billed, and nothing hides
// the original error.
func (t *Transfer) abort(ctx context.Context, u guard.Uploader, session storage.MultipartSession, key string) error {
	_, err := execution.Call(t.o.Clock, ctx, func(context.Context) (struct{}, error) {
		return struct{}{}, execution.SafeCall(func() error { return u.AbortMultipart(ctx, session) })
	})
	if err != nil {
		t.event(human.Failure("multipart.abort.failed",
			human.Text("reason", "the parts stay billed: "+err.Error()), human.ObjectKey("key", key)))
		return err
	}
	t.forget(session)
	return nil
}

// partSource is one artifact's bytes as the parts loop sees them: the bytes that
// are final, whether the object is whole, and the spool file they live in. A
// source that waits for a producer is the overlap; one that reports its size at
// once is the after-produce upload.
type partSource interface {
	// next reports the bytes final so far and whether the object is whole after
	// the caller consumed after bytes. It blocks only when there is nothing new
	// to offer; an error means the object will never be whole.
	next(ctx context.Context, after int64) (n int64, done bool, err error)
	// file names the spool file the parts are read from.
	file() string
	// terminal reports a channel that closes on the next change of the source's
	// state, and the producer's own failure the moment its growth recorded it.
	// Both come from one snapshot, so a failure between a check and a wait is
	// never lost; a finished source has neither.
	terminal() (changed <-chan struct{}, err error)
}

// finishedSource is an object that is already whole: the after-produce path.
type finishedSource struct {
	name string
	size int64
}

func (s *finishedSource) next(context.Context, int64) (int64, bool, error) {
	return s.size, true, nil
}

func (s *finishedSource) file() string { return s.name }

// terminal: the finished source has no producer to fail and never changes.
func (s *finishedSource) terminal() (<-chan struct{}, error) { return nil, nil }

// followSource is the first produce attempt while it grows. It remembers the
// change it last saw, so a change that landed between two calls is never lost:
// a call with bytes beyond the caller's mark returns at once, and one that finds
// nothing new waits for the next change.
type followSource struct {
	g    *pipeline.Growth
	name string
	seen <-chan struct{}
}

func (s *followSource) next(ctx context.Context, after int64) (int64, bool, error) {
	for {
		file, written, done, changed, err := s.g.Snapshot()
		if done || err != nil || written > after || changed != s.seen {
			s.name, s.seen = file, changed
			return written, done, err
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return 0, false, context.Cause(ctx)
		}
	}
}

func (s *followSource) file() string { return s.name }

// terminal reports the producer's failure the moment its growth recorded it —
// before the retry removes the failed attempt's spool file — and the channel
// that wakes the slot wait on the next change.
func (s *followSource) terminal() (<-chan struct{}, error) {
	_, _, _, changed, err := s.g.Snapshot()
	return changed, err
}

// producerFailure is the source's own failure, wrapped where the parts loop
// chose it as the upload's cause: the caller that sees it may fall back to the
// finished file, and nothing else may. A part's refusal and the run's
// cancellation are not producer failures, whatever the producer did a moment
// later.
type producerFailure struct{ err error }

func (p *producerFailure) Error() string { return p.err.Error() }
func (p *producerFailure) Unwrap() error { return p.err }

// terminalOrigin is where the cause of one upload's end came from: a part's own
// failure, the producer's failure, or the run's cancellation, by that priority.
type terminalOrigin uint8

const (
	originPart terminalOrigin = iota + 1
	originSource
	originParent
)

// uploadError is the ONE cause an upload chose, with the origin recorded when
// the event happened. Its callers act on the origin and report the error; they
// never re-derive either from the context or the growth afterwards.
type uploadError struct {
	origin terminalOrigin
	err    error
}

func (u *uploadError) Error() string { return u.err.Error() }
func (u *uploadError) Unwrap() error { return u.err }

// terminalOf reads the origin and cause uploadParts recorded. Any other error is
// reported as the upload's own part failure, so a caller still fails the
// destination instead of guessing a fallback.
func terminalOf(err error) (terminalOrigin, error) {
	var ue *uploadError
	if errors.As(err, &ue) {
		return ue.origin, ue.err
	}
	return originPart, err
}

func isContextError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// uploadTerminal records the facts that can end one multipart upload, each when
// it happens. The cause is chosen here, once, from the recorded facts by the
// plan's priority: an independent part failure, then the producer's failure,
// then the run's cancellation. Recording a cause and stopping the upload are ONE
// step — the recorder holds the cancel function — so no caller can record a
// terminal fact and forget to end the upload. Nothing here reads the context or
// the growth to re-classify an event that already happened.
type uploadTerminal struct {
	mu     sync.Mutex
	stop   context.CancelCauseFunc
	part   error
	source *producerFailure
	parent error
}

// recordPart records one part's failure and stops the upload when it is the
// cause. An error is a consequence, not a cause, only when it is the upload's
// own recorded cause coming back, a context error this upload's stop or the
// run's end imposed on the part, or a spool file that vanished after the
// producer's failure was already recorded. Any other part failure, including a
// vanished spool file with no producer failure behind it, is the cause.
func (t *uploadTerminal) recordPart(parent context.Context, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.part != nil {
		return
	}
	if t.source != nil && errors.Is(err, t.source) {
		return
	}
	if isContextError(err) && (t.source != nil || parent.Err() != nil) {
		return
	}
	if errors.Is(err, errVanishedPart) && t.source != nil {
		return
	}
	t.part = err
	t.stop(err)
}

// recordSource records the producer's failure and stops the upload at once; a
// part failure already recorded keeps the priority, so the source is recorded
// but the upload is already ending through that part.
func (t *uploadTerminal) recordSource(err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.source != nil {
		return
	}
	t.source = &producerFailure{err: err}
	if t.part == nil {
		t.stop(t.source)
	}
}

// recordSourceFrom records the producer's terminal state when the source
// reports one: the single exit calls it so a failure no part or dispatcher call
// happened to carry is still in the facts before the cause is chosen. A context
// error the run's own cancellation produced is not a producer failure.
func (t *uploadTerminal) recordSourceFrom(parent context.Context, src partSource) {
	_, sf := src.terminal()
	if sf == nil {
		return
	}
	t.recordSourceFact(parent, sf)
}

// recordSourceFact records one producer failure, unless it is the context error
// the run's own cancellation produced: that is the run's fact, not the
// producer's.
func (t *uploadTerminal) recordSourceFact(parent context.Context, err error) {
	if isContextError(err) && parent.Err() != nil {
		return
	}
	t.recordSource(err)
}

// recordNext records the error the parts source answered: the upload's own
// cause coming back is not a new fact, a context error while the run is
// cancelled is the run's cancellation, and anything else is the producer's
// failure — recorded, and stopped, even when the run has also been cancelled.
func (t *uploadTerminal) recordNext(parent context.Context, err error) {
	t.mu.Lock()
	own := t.part != nil || t.source != nil
	t.mu.Unlock()
	if own {
		return
	}
	if isContextError(err) && parent.Err() != nil {
		t.recordParent(err)
		return
	}
	t.recordSource(err)
}

// recordVanished records the producer's failure a vanished spool file points at,
// when the source reports one, before the file's part error is judged: the
// producer's failure is a terminal cause, and recording it stops the upload.
func (t *uploadTerminal) recordVanished(parent context.Context, src partSource) {
	t.recordSourceFrom(parent, src)
}

// recordParent records the run's cancellation, which never outranks a cause the
// upload recorded itself.
func (t *uploadTerminal) recordParent(err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.parent == nil {
		t.parent = err
	}
}

// cause is the ONE place the upload's cause is chosen.
func (t *uploadTerminal) cause() *uploadError {
	t.mu.Lock()
	defer t.mu.Unlock()
	switch {
	case t.part != nil:
		return &uploadError{origin: originPart, err: t.part}
	case t.source != nil:
		return &uploadError{origin: originSource, err: t.source}
	case t.parent != nil:
		return &uploadError{origin: originParent, err: t.parent}
	}
	return nil
}

// errVanishedPart marks a part whose spool file the producer's own retry removed
// under it: only an already recorded producer failure may explain such a
// vanished file, and a part that finds it without one is a terminal failure of
// its own.
var errVanishedPart = errors.New("the spool file the part reads is gone")

// uploadParts sends the object's parts from the source it is handed — a file
// that is already whole, or one the producer is still writing — with at most
// the destination's call_parallelism in flight. A part is sent only once its
// bytes are final, and every part is its own destination call and its own retry
// budget, so a failed part is opened again from the spool file, never the parts
// already accepted.
//
// Every event that can end the upload is recorded in an uploadTerminal when it
// happens, and the cause is chosen there — a part's own failure first, then the
// producer's failure (wrapped so the caller can fall back to the finished file),
// then the run's cancellation. The upload has ONE exit: the chosen cause
// cancels its own context, every in-flight part returns, and only then the
// caller may abort. A part that ended because a neighbour failed never hides the
// cause. The parts are returned by number, whatever order they finished in.
func (t *Transfer) uploadParts(ctx context.Context, u guard.Uploader, sp *spool.Spool, src partSource,
	session storage.MultipartSession, label string) ([]storage.PartResult, int, error) {
	pctx, stop := context.WithCancelCause(ctx)
	defer stop(nil)
	slots := make(chan struct{}, t.o.CallParallelism)
	var wg sync.WaitGroup
	term := &uploadTerminal{stop: stop}
	results := map[int32]storage.PartResult{}
	var resMu sync.Mutex
	var attempts atomic.Int64

	number := int32(1)
	var offset int64
	seen := int64(-1)
dispatch:
	for {
		n, done, err := src.next(pctx, seen)
		if err != nil {
			term.recordNext(ctx, err)
			break
		}
		seen = n
		file := src.file()
		for offset < n && (done || offset+session.PartSize < n) {
			size := min(session.PartSize, n-offset)
		slotsWait:
			for {
				// The producer can end while every slot is busy: its terminal
				// state is read with the notification channel in one snapshot,
				// recorded — which stops the upload — and the dispatcher leaves
				// at once, never waiting for a slot or the storage budget.
				changed, sf := src.terminal()
				if sf != nil {
					term.recordSourceFact(ctx, sf)
					break dispatch
				}
				select {
				case slots <- struct{}{}:
				case <-pctx.Done():
					if ctx.Err() != nil {
						term.recordParent(ctx.Err())
					}
					break dispatch
				case <-changed:
					// The growth changed: loop and read its terminal state again.
					continue
				}
				break slotsWait
			}
			num, off := number, offset
			wg.Add(1)
			go func() {
				defer wg.Done()
				res, used, perr := t.sendPart(pctx, u, sp, file, session, num, off, size, label)
				attempts.Add(int64(used))
				if perr != nil {
					// A part that found its spool file gone asks the source
					// whether the producer failed before this moment: that fact
					// is recorded as it is, and only then can the vanished file
					// be read as the failed attempt's cleanup.
					if errors.Is(perr, errVanishedPart) {
						term.recordVanished(ctx, src)
					}
					term.recordPart(ctx, perr)
				} else {
					resMu.Lock()
					results[num] = res
					resMu.Unlock()
				}
				<-slots
			}()
			offset += size
			number++
		}
		if done && offset >= n {
			break
		}
	}
	wg.Wait()
	// The single exit records the producer's terminal state, so a failure no
	// part or dispatcher call happened to carry is still in the facts before the
	// cause is chosen; an already recorded part failure keeps the priority.
	if ctx.Err() != nil {
		term.recordParent(ctx.Err())
	}
	term.recordSourceFrom(ctx, src)
	if cause := term.cause(); cause != nil {
		return nil, int(attempts.Load()), cause
	}
	parts := make([]storage.PartResult, 0, len(results))
	for n := int32(1); n < number; n++ {
		parts = append(parts, results[n])
	}
	return parts, int(attempts.Load()), nil
}

// sendPart is one part's own destination call: the retry loop runs inside the
// budget, so a slow part and its backoff are charged to the storage, and the
// bytes are read from the file that produced them (a replay of an earlier
// attempt included). A cancelled upload opens no file and books no attempt: the
// context is checked here, immediately before the call, because a slot and the
// cancellation can be ready together.
func (t *Transfer) sendPart(ctx context.Context, u guard.Uploader, sp *spool.Spool, file string,
	session storage.MultipartSession, number int32, offset, size int64, label string) (storage.PartResult, int, error) {
	if cause := context.Cause(ctx); cause != nil {
		return storage.PartResult{}, 0, cause
	}
	attempts := 0
	res, err := execution.Call(t.o.Clock, ctx, func(ctx context.Context) (storage.PartResult, error) {
		return execution.Attempt(ctx, t.o.Retry, fmt.Sprintf("part %d of %s", number, label),
			func(ctx context.Context, _ int) (storage.PartResult, error) {
				attempts++
				f, err := sp.Open(file)
				if err != nil {
					if errors.Is(err, os.ErrNotExist) {
						return storage.PartResult{}, pluginerrors.Permanent(fmt.Errorf("%w: %w", errVanishedPart, err))
					}
					return storage.PartResult{}, pluginerrors.Permanent(err)
				}
				defer func() { _ = f.Close() }()
				// A section of the spool file, not a buffer: the destination reads
				// exactly this part's bytes and holds none of the rest.
				part := storage.Part{Number: number, Size: size, Body: io.NewSectionReader(f, offset, size)}
				return execution.SafeValue(func() (storage.PartResult, error) { return u.PutPart(ctx, session, part) })
			})
	})
	if err != nil {
		return storage.PartResult{}, attempts, err
	}
	if res.Number != number {
		return storage.PartResult{}, attempts, pluginerrors.Permanent(fmt.Errorf(
			"multipart %s: the plugin answered part %d for part %d", session.Key, res.Number, number))
	}
	return res, attempts, nil
}

// event emits one event of this destination's own upload story on its scoped
// logger.
func (t *Transfer) event(ev human.Event) {
	if t.o.Log == nil {
		return
	}
	t.o.Log.Log(context.Background(), ev.Severity, ev.Name, slog.Any("event", ev))
}
