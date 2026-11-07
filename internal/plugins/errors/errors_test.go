package pluginerrors

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

// TestClassMarkers: each constructor reports its class, the marked error prints
// and unwraps to the cause, and no delay hint comes with a plain marker.
func TestClassMarkers(t *testing.T) {
	base := errors.New("boom")
	for _, tc := range []struct {
		name string
		err  error
		want Class
	}{
		{"permanent", Permanent(base), ClassPermanent},
		{"transient", Transient(base), ClassTransient},
		{"not found", NotFound(base), ClassNotFound},
	} {
		if got, after := ClassOf(tc.err); got != tc.want || after != 0 {
			t.Errorf("%s: ClassOf = %q %v, want %q 0", tc.name, got, after, tc.want)
		}
		if got := tc.err.Error(); got != "boom" {
			t.Errorf("%s: Error() = %q, want the cause text", tc.name, got)
		}
		if !errors.Is(tc.err, base) {
			t.Errorf("%s: class marker does not unwrap to its cause", tc.name)
		}
	}
}

// TestRetryAfter: the delay hint rides with the transient class and is reported
// verbatim.
func TestRetryAfter(t *testing.T) {
	base := errors.New("slow down")
	class, after := ClassOf(RetryAfter(base, 30*time.Second))
	if class != ClassTransient || after != 30*time.Second {
		t.Fatalf("ClassOf = %q %v, want transient 30s", class, after)
	}
	if !errors.Is(RetryAfter(base, time.Second), base) {
		t.Fatal("RetryAfter does not unwrap to its cause")
	}
}

// TestClassOfUnclassified: an ordinary error carries no class, and ClassOf on
// nil reports the same instead of panicking.
func TestClassOfUnclassified(t *testing.T) {
	if class, after := ClassOf(errors.New("plain")); class != "" || after != 0 {
		t.Fatalf("unclassified: ClassOf = %q %v", class, after)
	}
	if class, after := ClassOf(nil); class != "" || after != 0 {
		t.Fatalf("nil: ClassOf = %q %v", class, after)
	}
}

// TestClassOfThroughAWrap: a marker found through fmt.Errorf("%w") is the class
// of the whole error.
func TestClassOfThroughAWrap(t *testing.T) {
	base := errors.New("boom")
	for _, tc := range []struct {
		name string
		err  error
		want Class
	}{
		{"permanent", fmt.Errorf("wrap: %w", Permanent(base)), ClassPermanent},
		{"transient", fmt.Errorf("wrap: %w", Transient(base)), ClassTransient},
		{"not found", fmt.Errorf("wrap: %w", NotFound(base)), ClassNotFound},
	} {
		if got, _ := ClassOf(tc.err); got != tc.want {
			t.Errorf("%s: ClassOf = %q, want %q", tc.name, got, tc.want)
		}
	}
}

// TestClassOfOutermost: the outermost marker wins, so an inner hint cannot
// override the class an outer layer decided.
func TestClassOfOutermost(t *testing.T) {
	base := errors.New("boom")
	inner := RetryAfter(base, 30*time.Second)
	if class, after := ClassOf(Permanent(inner)); class != ClassPermanent || after != 0 {
		t.Fatalf("outer permanent: ClassOf = %q %v, want permanent 0", class, after)
	}
	if class, after := ClassOf(Transient(Permanent(base))); class != ClassTransient || after != 0 {
		t.Fatalf("outer transient: ClassOf = %q %v, want transient 0", class, after)
	}
}
