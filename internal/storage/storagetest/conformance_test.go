package storagetest

// The location guard's own contract: the shape comes from one base string, the
// generator produces it, and the resolver refuses everything else. The storage
// plugins' live tests pin the refusals for their own bases; this covers the
// helper itself, including the programmer-error panic.

import (
	"errors"
	"testing"
)

func TestConformanceLocationResolvesOnlyItsOwnShape(t *testing.T) {
	loc := NewConformanceLocation("conformance-", 12)
	generated := loc.Generate(t)
	if got, err := loc.Resolve(generated); err != nil || got != generated {
		t.Fatalf("Resolve(%q) = %q, %v; want the generated value accepted", generated, got, err)
	}
	for _, raw := range []string{"conformance-0123456789ab/", "conformance-0123456789AB", "conformance-0123456789a", "elsewhere"} {
		if _, err := loc.Resolve(raw); err == nil {
			t.Errorf("Resolve(%q) accepted a value outside the shape", raw)
		}
	}
	if _, err := loc.Resolve(""); !errors.Is(err, ErrLocationUnset) {
		t.Errorf("the empty value: err %v, want ErrLocationUnset", err)
	}
}

func TestNewConformanceLocationRejectsAnUngeneratableShape(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("an odd hex-digit count was accepted; it cannot be generated")
		}
	}()
	NewConformanceLocation("conformance-", 11)
}
