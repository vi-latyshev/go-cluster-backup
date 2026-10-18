package plugin

import (
	"errors"
	"time"
)

// Class is a serialisable error-class marker: it survives JSON and a future
// out-of-process bridge, unlike a private Go error type.
type Class string

const (
	ClassPermanent Class = "permanent"
	ClassTransient Class = "transient"
	// ClassNotFound means "definitely absent" — distinct from "do not know",
	// which is ClassTransient.
	ClassNotFound Class = "not_found"
)

type classified struct {
	err   error
	class Class
	after time.Duration
}

func (c *classified) Error() string { return c.err.Error() }
func (c *classified) Unwrap() error { return c.err }

// Permanent marks an error not worth retrying: config, auth, quota, not found.
func Permanent(err error) error { return &classified{err: err, class: ClassPermanent} }

// Transient marks an error worth retrying with backoff.
func Transient(err error) error { return &classified{err: err, class: ClassTransient} }

// NotFound marks an error that means "definitely absent". A call that cannot
// tell absence from any other failure must NOT return NotFound.
func NotFound(err error) error { return &classified{err: err, class: ClassNotFound} }

// RetryAfter is Transient with a delay hint the engine honours verbatim.
func RetryAfter(err error, d time.Duration) error {
	return &classified{err: err, class: ClassTransient, after: d}
}

// ClassOf reports the outermost class marker and its delay hint; "" (with a zero
// delay) means the error is unclassified, and the engine's config decides what
// an unclassified error means.
func ClassOf(err error) (Class, time.Duration) {
	if c, ok := errors.AsType[*classified](err); ok {
		return c.class, c.after
	}
	return "", 0
}
