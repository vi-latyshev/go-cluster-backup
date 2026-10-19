package plugintest

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
)

// recorder stands in for *testing.T in the helpers' own tests: a fake
// testing.TB cannot exist (the interface has an unexported method), and a live
// T would end the test at the first Fatalf instead of letting the test assert
// that the Fatalf happened.
type recorder struct {
	fatals []string
	errors []string
}

func (r *recorder) Helper() {}

func (r *recorder) Fatalf(format string, args ...any) {
	r.fatals = append(r.fatals, fmt.Sprintf(format, args...))
}

func (r *recorder) Errorf(format string, args ...any) {
	r.errors = append(r.errors, fmt.Sprintf(format, args...))
}

// TestCheckSpecReportsEveryIssue: the exported check's failure path writes one
// line per issue, naming the plugin and the check. Removing the loop leaves the
// recorder empty; dropping the fields leaves the line unnamed.
func TestCheckSpecReportsEveryIssue(t *testing.T) {
	type cfg struct {
		Label string `json:"label"` // no doc: the "fields documented" check fails
	}
	r := plugin.NewRegistry()
	err := plugin.Register(r, plugin.NewKind[error]("test"), plugin.Spec[cfg, error]{
		Type: "undocumented", Example: []byte(`{"label":"x"}`),
		New: func(context.Context, cfg, plugin.Deps) (error, error) { return nil, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	rec := &recorder{}
	checkSpec(rec, r)
	if len(rec.errors) == 0 {
		t.Fatal("a broken plugin produced no issue line")
	}
	for _, line := range rec.errors {
		if !strings.Contains(line, "test/undocumented") || !strings.Contains(line, "fields documented") {
			t.Fatalf("issue line does not name the plugin and the check: %q", line)
		}
	}
	if len(rec.fatals) != 0 {
		t.Fatalf("reporting an issue failed the test: %v", rec.fatals)
	}
}

// TestMustReturnWithErrorAcceptsAnError: an operation that observes the
// cancelled context and reports it passes, and the context it received really
// was cancelled. A helper that forgot to cancel the context, or that treated
// the honest error as a failure, fails here.
func TestMustReturnWithErrorAcceptsAnError(t *testing.T) {
	r := &recorder{}
	err := mustReturnWithError(r, 5*time.Second, "Dump", func(ctx context.Context) error {
		if ctx.Err() == nil {
			t.Fatal("the helper did not pass a cancelled context")
		}
		return ctx.Err()
	})
	if err != nil {
		t.Fatalf("an operation that returned the context error was rejected: %v", err)
	}
	if len(r.fatals) != 0 {
		t.Fatalf("a passing operation reported failures: %v", r.fatals)
	}
}

// TestMustReturnWithErrorRejectsSuccess: an operation that answers a cancelled
// context with nil is the defect the helper exists to catch. Dropping the nil
// check — the whole point of the helper — fails this test.
func TestMustReturnWithErrorRejectsSuccess(t *testing.T) {
	r := &recorder{}
	err := mustReturnWithError(r, 5*time.Second, "Dump", func(context.Context) error { return nil })
	if err == nil {
		t.Fatal("an operation that returned success on a cancelled context passed")
	}
	if !strings.Contains(err.Error(), "Dump") || !strings.Contains(err.Error(), "returned success") {
		t.Fatalf("error = %v, want it to name the operation and the defect", err)
	}
	if len(r.fatals) != 0 {
		t.Fatalf("a prompt return reported a timeout: %v", r.fatals)
	}
}

// TestMustReturnWithinFailsALateFunction: an operation that ignores the
// cancelled context is cut off at the deadline — the call returns then, not
// when the operation finally wakes — and the test fails naming the operation.
// Removing the timeout case makes the call wait for the full sleep (the
// elapsed assertion), removing the Fatalf leaves the recorder empty.
func TestMustReturnWithinFailsALateFunction(t *testing.T) {
	r := &recorder{}
	start := time.Now()
	err := mustReturnWithin(r, 10*time.Millisecond, "Notify", func(context.Context) error {
		time.Sleep(500 * time.Millisecond)
		return nil
	})
	elapsed := time.Since(start)
	if elapsed > 400*time.Millisecond {
		t.Fatalf("the helper waited %s for an operation that ignored its context", elapsed)
	}
	if len(r.fatals) != 1 {
		t.Fatalf("fatals = %v, want exactly one timeout failure", r.fatals)
	}
	for _, want := range []string{"Notify", "did not return"} {
		if !strings.Contains(r.fatals[0], want) {
			t.Fatalf("the timeout failure does not name %q: %q", want, r.fatals[0])
		}
	}
	_ = err // the timeout return value is the live-T branch that never runs with a recorder
}

// TestExportedHelpersThroughARealT: the wrappers the conformance kits call
// behave as the cores they delegate to.
func TestExportedHelpersThroughARealT(t *testing.T) {
	if err := MustReturnWithError(t, time.Second, "Decide", func(ctx context.Context) error {
		return ctx.Err()
	}); err != nil {
		t.Fatalf("MustReturnWithError = %v", err)
	}
	err := MustReturnWithError(t, time.Second, "Decide", func(context.Context) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "Decide") {
		t.Fatalf("MustReturnWithError on success = %v", err)
	}
	if err := MustReturnWithin(t, time.Second, "Load", func(ctx context.Context) error {
		return context.DeadlineExceeded
	}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("MustReturnWithin = %v, want the operation's own error", err)
	}
}

// TestErrWriterFailsWithinTwoWrites pins the contract the plugin error-path
// tests rely on: a byte-wise writer survives the first calls and fails on the
// one that reaches the limit, and one oversized chunk fails immediately — a
// change to the limit or the arithmetic is a behaviour change, not a detail.
func TestErrWriterFailsWithinTwoWrites(t *testing.T) {
	w := &ErrWriter{}
	for i := 1; i < errWriterLimit; i++ {
		if n, err := w.Write(make([]byte, 1)); err != nil || n != 1 {
			t.Fatalf("write %d: %d, %v", i, n, err)
		}
	}
	if n, err := w.Write(make([]byte, 1)); err == nil || n != 1 {
		t.Fatalf("the write that reaches the limit: %d, %v", n, err)
	}
	if _, err := w.Write(make([]byte, 1)); err == nil {
		t.Fatal("a write past the limit was accepted")
	}

	big := &ErrWriter{}
	n, err := big.Write(make([]byte, 8))
	if err == nil || n != errWriterLimit {
		t.Fatalf("one 8-byte write: %d, %v, want %d bytes and a failure", n, err, errWriterLimit)
	}
}
