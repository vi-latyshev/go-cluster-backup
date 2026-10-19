package storagetest

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"testing"
)

// ErrLocationUnset marks an absent environment value: a live entry point skips
// on it, and the empty value is never treated as a target.
var ErrLocationUnset = errors.New("the conformance location is not set")

// ConformanceLocation is the only place a live storage harness may write to or
// delete: a fresh location of one shape — a base string followed by a run of
// lowercase hex digits. A live cleanup entry point points its environment at
// such a location and resolves it through Resolve, so every storage plugin
// shares one definition of what may be touched, and the guard can never drift
// from the generator that produced the location.
type ConformanceLocation struct {
	base      string
	hexDigits int
	shape     *regexp.Regexp
}

// NewConformanceLocation fixes the shape: base followed by hexDigits lowercase
// hex digits, with hexDigits positive and even. It panics on a shape it could
// not generate — a programming error in a test helper.
func NewConformanceLocation(base string, hexDigits int) *ConformanceLocation {
	if hexDigits <= 0 || hexDigits%2 != 0 {
		panic(fmt.Sprintf("storagetest: hexDigits must be a positive even number, got %d", hexDigits))
	}
	return &ConformanceLocation{
		base:      base,
		hexDigits: hexDigits,
		shape: regexp.MustCompile(
			"^" + regexp.QuoteMeta(base) + "[0-9a-f]{" + strconv.Itoa(hexDigits) + "}$"),
	}
}

// Generate returns a fresh location of this shape, and fails the test if the
// generator and the guard ever disagree.
func (l *ConformanceLocation) Generate(t *testing.T) string {
	t.Helper()
	return l.generate(t)
}

// generate is the generator behind Generate, taking the narrow handle its one
// failure path needs so the kit's own tests can drive it.
func (l *ConformanceLocation) generate(t fatalReporter) string {
	b := make([]byte, l.hexDigits/2)
	// crypto/rand.Read has no failure mode left to report: it fills b entirely
	// or crashes the program, and the default Reader is documented never to
	// return an error.
	_, _ = rand.Read(b)
	loc := l.base + hex.EncodeToString(b)
	// The guard is the one place the generator and the resolver are held
	// together: an edit that changes the shape without changing this
	// generator — a new field, another alphabet — fails here, before a live
	// harness could be pointed at a prefix the resolver would have refused.
	if !l.shape.MatchString(loc) {
		t.Fatalf("storagetest: the generator produced %q, which is not a conformance location", loc)
	}
	return loc
}

// Resolve validates the target a live cleanup entry point was given, before
// any request leaves the process: the empty value is ErrLocationUnset (the
// caller skips), anything that is not this shape is refused, and only the
// exact shape is returned unchanged.
func (l *ConformanceLocation) Resolve(raw string) (string, error) {
	if raw == "" {
		return "", ErrLocationUnset
	}
	if !l.shape.MatchString(raw) {
		return "", fmt.Errorf("refusing %q: cleanup may only touch %s", raw, l.shape)
	}
	return raw, nil
}
