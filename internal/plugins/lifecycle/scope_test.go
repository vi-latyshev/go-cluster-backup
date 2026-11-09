package lifecycle

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// recordingCloser appends its name to one shared order on Close.
type recordingCloser struct {
	name  string
	err   error
	order *[]string
}

func (c recordingCloser) Close() error {
	*c.order = append(*c.order, c.name)
	return c.err
}

// TestScopeClosesInReverseConstructionOrder: the invocation that built the
// instances closes them last-built first, joins every failure and closes once.
func TestScopeClosesInReverseConstructionOrder(t *testing.T) {
	var order []string
	firstErr := errors.New("first instance refused to close")
	secondErr := errors.New("second instance refused to close")
	s := New()
	s.Own(recordingCloser{name: "first", err: firstErr, order: &order})
	s.Own(recordingCloser{name: "second", err: secondErr, order: &order})
	s.Own(nil) // a nil instance has nothing to close
	if err := s.Close(context.Background()); !errors.Is(err, firstErr) || !errors.Is(err, secondErr) {
		t.Fatalf("Close did not join both failures: %v", err)
	}
	if want := []string{"second", "first"}; strings.Join(order, ",") != strings.Join(want, ",") {
		t.Fatalf("close order = %v, want %v", order, want)
	}
	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("the second Close = %v, want a no-op", err)
	}
	if len(order) != 2 {
		t.Fatalf("a second Close closed an instance again: %v", order)
	}
}

// TestScopeStopsAtTheCleanupDeadline: a deadline already spent closes nothing and
// reports itself, so a shutdown never starts a close it cannot wait for.
func TestScopeStopsAtTheCleanupDeadline(t *testing.T) {
	var order []string
	s := New()
	s.Own(recordingCloser{name: "first", order: &order})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.Close(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Close on an expired deadline = %v, want the deadline failure", err)
	}
	if len(order) != 0 {
		t.Fatalf("an instance was closed past the deadline: %v", order)
	}
}

// runShutdown records every step and answers the configured failures.
func runShutdown(joined *[]string, joinErr, releaseErr, abortErr error) Shutdown {
	step := func(name string, err error) func(context.Context) error {
		return func(context.Context) error {
			*joined = append(*joined, name)
			return err
		}
	}
	return Shutdown{
		CancelRequests: func() { *joined = append(*joined, "admission") },
		Join:           step("join", joinErr),
		Release:        step("release", releaseErr),
		Abort:          step("abort", abortErr),
	}
}

// TestCancelRunsTheSequenceInOrder: admission, join, release, abort, then the
// scope close — the order the owner approved for SIGTERM.
func TestCancelRunsTheSequenceInOrder(t *testing.T) {
	var steps []string
	s := New()
	s.Own(recordingCloser{name: "client", order: &steps})
	if err := s.Cancel(context.Background(), runShutdown(&steps, nil, nil, nil)); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	want := "admission,join,release,abort,client"
	if got := strings.Join(steps, ","); got != want {
		t.Fatalf("cancel sequence = %s, want %s", got, want)
	}
}

// TestCancelStopsBeforeReleaseWhenJoinIsIncomplete: a join that does not complete
// forbids the release — the claim stays held and conservative — and the clients
// stay open because their users may still run.
func TestCancelStopsBeforeReleaseWhenJoinIsIncomplete(t *testing.T) {
	var steps []string
	joinErr := errors.New("a request goroutine is still running")
	s := New()
	s.Own(recordingCloser{name: "client", order: &steps})
	err := s.Cancel(context.Background(), runShutdown(&steps, joinErr, nil, nil))
	if !errors.Is(err, joinErr) {
		t.Fatalf("Cancel = %v, want the join failure", err)
	}
	var phase *PhaseError
	if !errors.As(err, &phase) || phase.Phase != PhaseJoin {
		t.Fatalf("the failure does not name the join phase: %v", err)
	}
	if got := strings.Join(steps, ","); got != "admission,join" {
		t.Fatalf("an incomplete join still ran cleanup: %s", got)
	}
}

// TestM4_IncompleteJoinPoisonsTheScope: on a PhaseJoin error the clients stay
// open, and every later Close refuses with ErrIncompleteJoin instead of closing
// instances whose users may still be running.
func TestM4_IncompleteJoinPoisonsTheScope(t *testing.T) {
	var steps []string
	joinErr := errors.New("a request goroutine is still running")
	s := New()
	s.Own(recordingCloser{name: "client", order: &steps})
	if err := s.Cancel(context.Background(), runShutdown(&steps, joinErr, nil, nil)); !errors.Is(err, joinErr) {
		t.Fatalf("Cancel = %v, want the join failure", err)
	}
	if !s.JoinFailed() {
		t.Fatal("the scope does not record the incomplete join")
	}
	if s.Closed() {
		t.Fatal("the scope closed after an incomplete join")
	}
	for range 2 {
		if err := s.Close(context.Background()); !errors.Is(err, ErrIncompleteJoin) {
			t.Fatalf("Close after an incomplete join = %v, want ErrIncompleteJoin", err)
		}
	}
	if len(steps) != 2 || steps[1] != "join" {
		t.Fatalf("a client was closed after an incomplete join: %v", steps)
	}
}

// TestM4_CompletedCancelClosesTheScopeOnce: a complete sequence owns the close,
// and a second Close stays a no-op.
func TestM4_CompletedCancelClosesTheScopeOnce(t *testing.T) {
	var steps []string
	s := New()
	s.Own(recordingCloser{name: "client", order: &steps})
	if err := s.Cancel(context.Background(), runShutdown(&steps, nil, nil, nil)); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if s.JoinFailed() {
		t.Fatal("a complete sequence reports an incomplete join")
	}
	if !s.Closed() {
		t.Fatal("the scope did not close its instances")
	}
	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("a second Close = %v, want a no-op", err)
	}
	if got := strings.Join(steps, ","); got != "admission,join,release,abort,client" {
		t.Fatalf("cancel sequence = %s", got)
	}
}

// TestCancelJoinsEveryPhaseFailure: release, abort and close failures are all
// reported and none stops the remaining steps; the primary failure stays the
// caller's.
func TestCancelJoinsEveryPhaseFailure(t *testing.T) {
	var steps []string
	releaseErr := errors.New("the CAS write failed")
	abortErr := errors.New("the endpoint refused the abort")
	closeErr := errors.New("the client close failed")
	s := New()
	s.Own(recordingCloser{name: "client", err: closeErr, order: &steps})
	err := s.Cancel(context.Background(), runShutdown(&steps, nil, releaseErr, abortErr))
	for _, want := range []error{releaseErr, abortErr, closeErr} {
		if !errors.Is(err, want) {
			t.Errorf("Cancel does not carry %v: %v", want, err)
		}
	}
	for _, phase := range []Phase{PhaseRelease, PhaseAbort, PhaseClose} {
		if !strings.Contains(err.Error(), "lifecycle: "+string(phase)+": ") {
			t.Errorf("Cancel does not name phase %q: %v", phase, err)
		}
	}
	if got := strings.Join(steps, ","); got != "admission,join,release,abort,client" {
		t.Fatalf("a failed phase stopped the sequence: %s", got)
	}
}

// TestCancelRefusesAnIncompleteSequence: a shutdown missing a step is refused
// before admission closes, so a wiring mistake never half-runs a cancellation.
func TestCancelRefusesAnIncompleteSequence(t *testing.T) {
	cases := []struct {
		name string
		drop func(*Shutdown)
		want string
	}{
		{"admission", func(sh *Shutdown) { sh.CancelRequests = nil }, "admission step"},
		{"join", func(sh *Shutdown) { sh.Join = nil }, "join"},
		{"release", func(sh *Shutdown) { sh.Release = nil }, "release"},
		{"abort", func(sh *Shutdown) { sh.Abort = nil }, "abort"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var steps []string
			sh := runShutdown(&steps, nil, nil, nil)
			tc.drop(&sh)
			err := New().Cancel(context.Background(), sh)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Cancel = %v, want a refusal naming %q", err, tc.want)
			}
			if len(steps) != 0 {
				t.Fatalf("an incomplete sequence ran %v", steps)
			}
		})
	}
}

// TestCleanupContextOutlivesTheCancelledParent: the cleanup deadline is detached
// from the run context — a cancelled run still gets its cleanup — and bounded by
// the one total.
func TestCleanupContextOutlivesTheCancelledParent(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	cancelParent()
	ctx, cancel := CleanupContext(parent, time.Minute)
	defer cancel()
	if err := ctx.Err(); err != nil {
		t.Fatalf("the cleanup context inherited the cancelled parent: %v", err)
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("the cleanup context has no deadline")
	}
	if left := time.Until(deadline); left > time.Minute || left < 55*time.Second {
		t.Fatalf("the cleanup deadline is %s away, want the configured minute", left)
	}
	cancel()
	if err := ctx.Err(); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel did not end the cleanup context: %v", err)
	}
}
