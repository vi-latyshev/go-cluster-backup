package must

import (
	"errors"
	"testing"
)

// TestDoReturnsTheValue: the ordinary path is the value the call produced, with
// its type intact.
func TestDoReturnsTheValue(t *testing.T) {
	if got := Do(7, nil); got != 7 {
		t.Errorf("Do(7, nil) = %d, want 7", got)
	}
	if got := Do("v", error(nil)); got != "v" {
		t.Errorf("Do(\"v\", nil) = %q, want %q", got, "v")
	}
}

// TestDoPanicsOnAnError: an error reaching Do is a bug in the caller's claim,
// and the panic carries the error itself so a stack trace names it.
func TestDoPanicsOnAnError(t *testing.T) {
	want := errors.New("the impossible happened")
	defer func() {
		got := recover()
		if got == nil {
			t.Fatal("Do returned instead of panicking")
		}
		err, ok := got.(error)
		if !ok {
			t.Fatalf("Do panicked with %v, want the error", got)
		}
		if !errors.Is(err, want) {
			t.Errorf("Do panicked with %v, want %v", err, want)
		}
	}()
	_ = Do(0, want)
	t.Fatal("Do returned instead of panicking")
}
