package storage

import (
	"context"
	"io"
	"time"
)

// MultipartSession is one multipart upload in progress: the service's handle for
// it and the part size the engine slices the object into. The engine computes
// ceil(size / PartSize) parts and sends them in order; the last one may be
// shorter.
type MultipartSession struct {
	Key      string
	UploadID string
	PartSize int64
}

// Part is one slice of the object. Number is 1-based and ascending; Size is the
// number of bytes Body holds; Body reads exactly Size bytes — the engine hands
// over a section of its spool file, never a buffer of the part.
type Part struct {
	Number int32
	Size   int64
	Body   io.Reader
}

// PartResult is what the service answered for one part: the number it was sent
// for and the handle CompleteMultipartUpload needs back for it.
type PartResult struct {
	Number int32
	ETag   string
}

// StaleUpload is one multipart upload a killed process left behind: what
// AbortMultipart needs to discard it, and when the service says it was started.
type StaleUpload struct {
	Key       string
	UploadID  string
	Initiated time.Time
}

// MultipartUploader is an optional capability of a Storage whose service
// uploads one object as parts: the engine drives the upload call by call, so a
// part that failed is sent again without re-sending the parts already accepted.
//
// The capability is discovered by call, exactly like the storage kind. A storage
// that does not implement it, and a storage that answers errors.ErrUnsupported
// to BeginMultipart because this object fits one Put, are uploaded whole.
//
// Every method is ONE attempt — retries, backoff and budgets belong to the
// engine, which drives this interface, never the plugin. An error is classified
// exactly like the kind's: plugin.Permanent, plugin.Transient (with a
// RetryAfter hint where the service gave one), plugin.NotFound only when the
// service says the upload or the object is definitely gone. Aborting an
// unfinished upload is the caller's job, and it is what keeps unfinished parts
// from being billed.
type MultipartUploader interface {
	// BeginMultipart opens one upload. A size the storage refuses outright is a
	// permanent error; a size that needs no parts is errors.ErrUnsupported,
	// answered without touching the service, and the engine then uses Put. The
	// returned PartSize is the size the caller must slice the object into.
	BeginMultipart(ctx context.Context, req PutRequest) (MultipartSession, error)
	// PutPart sends one part. Sending a number again replaces the part the
	// service already holds under it — which is what a retry of one part is.
	PutPart(ctx context.Context, session MultipartSession, part Part) (PartResult, error)
	// CompleteMultipart commits the upload and returns only when the service
	// accepted the commit; a part list that does not match what the service
	// holds is an error. A commit that did not happen must be an error even when
	// the answer was HTTP 200 — the service reports some failures that way.
	CompleteMultipart(ctx context.Context, session MultipartSession, parts []PartResult) error
	// AbortMultipart discards the upload and every part of it: an upload already
	// gone is nothing left to clean, not an error. The engine calls it when the
	// upload has failed, including when the context the upload ran under is
	// already dead — the implementation runs the call on a context of its own,
	// bounded by a request timeout of its own, because one control request must
	// never eat the time the run's ending needs.
	AbortMultipart(ctx context.Context, session MultipartSession) error
	// StaleMultipart lists the incomplete uploads this storage holds under its
	// own keys that were initiated before the cutoff — what a process killed
	// outright leaves behind, billed and invisible to every listing and retention.
	// The keys are relative to the storage's own root, exactly like every other
	// key; nothing outside that root is ever listed, and an upload the service
	// does not date is never called stale. The service answers one page per
	// call and the implementation follows the pages; the engine aborts each
	// returned upload through AbortMultipart, so one attempt per call stays the
	// rule.
	StaleMultipart(ctx context.Context, olderThan time.Time) ([]StaleUpload, error)
}

// MultipartInventory is the optional capability of a storage to report EVERY
// in-progress multipart upload under its own root, of any age — the full
// inventory, separate from StaleMultipart's abort selection. An unfinished
// upload of an earlier writer bills bytes no object listing shows, so any entry
// here makes the storage's occupancy unknown until it is aborted; a storage
// without this capability, or one whose inventory fails, is treated the same
// way by the engine's manual admission.
type MultipartInventory interface {
	// Inventory lists every incomplete upload under the storage's own root, one
	// page per call, following the pages. An upload the service does not date is
	// included: its size is unknown either way.
	Inventory(ctx context.Context) ([]StaleUpload, error)
}
