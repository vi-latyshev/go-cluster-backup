package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"time"
)

// CleanupContext is the ONE deadline every cancel step shares. It is detached
// from the cancelled parent — a cleanup step must still be attempted after the
// run's context is gone — and bounded by total. The deadline bounds how long we
// wait; it never claims to stop an uncooperative goroutine.
func CleanupContext(parent context.Context, total time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(parent), total)
}

// ErrIncompleteJoin refuses every later close of an invocation whose join did
// not complete: its clients may still be in use, so they are never closed.
var ErrIncompleteJoin = errors.New("lifecycle: invocation join did not complete")

// Scope owns the instances one invocation built, in construction order. The role
// closes it after the operation returned, so a client is never closed under code
// that still uses it.
type Scope struct {
	closers    []io.Closer
	closed     bool
	joinFailed bool
}

// New returns an empty scope.
func New() *Scope { return &Scope{} }

// Own tracks one instance. Its Close runs in reverse construction order.
func (s *Scope) Own(c io.Closer) {
	if c == nil {
		return
	}
	s.closers = append(s.closers, c)
}

// Close closes every owned instance in reverse construction order, joining every
// failure. Each close's WAIT is bounded by the shared deadline: a closer already
// running past it is left behind, the deadline failure is returned, and no later
// instance starts — a timeout bounds the wait, it never promises to stop the
// goroutine. A second Close is a no-op: the invocation that built the instances
// owns their close exactly once.
func (s *Scope) Close(ctx context.Context) error {
	if s.closed {
		return nil
	}
	if s.joinFailed {
		return ErrIncompleteJoin
	}
	s.closed = true
	var errs []error
	for _, c := range slices.Backward(s.closers) {
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}
		if err := closeWithin(ctx, c); err != nil {
			errs = append(errs, err)
			if ctx.Err() != nil {
				break
			}
		}
	}
	s.closers = nil
	return errors.Join(errs...)
}

// closeWithin waits for one closer no longer than the shared deadline. The close
// runs in its own goroutine, so a deadline bounds the wait even when the closer
// itself blocks; an uncooperative closer is left behind, never claimed stopped.
func closeWithin(ctx context.Context, c io.Closer) error {
	done := make(chan error, 1)
	go func() { done <- c.Close() }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// JoinFailed reports whether an earlier Cancel stopped at the join phase: the
// invocation is poisoned, and every later Close refuses instead of closing
// instances whose users may still be running.
func (s *Scope) JoinFailed() bool { return s.joinFailed }

// Closed reports whether the scope has closed its instances.
func (s *Scope) Closed() bool { return s.closed }

// Phase names one step of the cancel sequence in diagnostics.
type Phase string

const (
	PhaseJoin    Phase = "join"
	PhaseRelease Phase = "release"
	PhaseAbort   Phase = "abort"
	PhaseClose   Phase = "close"
)

// Shutdown is the cancel sequence in order: close admission and cancel producers
// and request contexts, join every request goroutine, CAS-release this run's
// claim with its durable uncertainty, abort exactly this run's own upload IDs,
// then close the scope. Every callback runs under the caller's one cleanup
// deadline from CleanupContext.
type Shutdown struct {
	// CancelRequests closes admission and cancels producers and request contexts.
	// It must be idempotent.
	CancelRequests func()
	// Join waits for every request goroutine. It must return only once they have
	// all returned; while it reports an error the lease is never released.
	Join func(context.Context) error
	// Release CAS-releases this run's claim together with its durable uncertainty.
	Release func(context.Context) error
	// Abort aborts exactly this run's own upload IDs: no Put, no Complete, no
	// Delete, one shared deadline over all of them.
	Abort func(context.Context) error
}

// validate refuses a sequence with a missing step before anything runs.
func (sh Shutdown) validate() error {
	switch {
	case sh.CancelRequests == nil:
		return errors.New("lifecycle: shutdown has no admission step")
	case sh.Join == nil:
		return errors.New("lifecycle: shutdown has no join")
	case sh.Release == nil:
		return errors.New("lifecycle: shutdown has no release")
	case sh.Abort == nil:
		return errors.New("lifecycle: shutdown has no abort")
	}
	return nil
}

// PhaseError names the cancel step that failed; the primary failure stays the
// caller's, and every phase failure is joined, never hidden.
type PhaseError struct {
	Phase Phase
	Err   error
}

func (e *PhaseError) Error() string { return fmt.Sprintf("lifecycle: %s: %v", e.Phase, e.Err) }

func (e *PhaseError) Unwrap() error { return e.Err }

// Cancel runs the cancel sequence inside ctx — the one cleanup deadline — and
// closes the scope last. An incomplete join stops the sequence: the lease is not
// released and the clients are not closed while their users may still run, so
// the claim stays held and conservative. Release and abort failures are
// diagnostic; they never stop the remaining steps.
func (s *Scope) Cancel(ctx context.Context, sh Shutdown) error {
	if err := sh.validate(); err != nil {
		return err
	}
	sh.CancelRequests()
	if err := sh.Join(ctx); err != nil {
		s.joinFailed = true
		return &PhaseError{Phase: PhaseJoin, Err: err}
	}
	var errs []error
	if err := sh.Release(ctx); err != nil {
		errs = append(errs, &PhaseError{Phase: PhaseRelease, Err: err})
	}
	if err := sh.Abort(ctx); err != nil {
		errs = append(errs, &PhaseError{Phase: PhaseAbort, Err: err})
	}
	if err := s.Close(ctx); err != nil {
		errs = append(errs, &PhaseError{Phase: PhaseClose, Err: err})
	}
	return errors.Join(errs...)
}
