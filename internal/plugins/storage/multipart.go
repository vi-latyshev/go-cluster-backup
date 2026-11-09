package storage

import (
	"context"
	"io"
	"time"
)

// MultipartSession is one multipart upload in progress: the service's handle for
// it and the part size transfer slices the object into. Transfer computes
// ceil(size / PartSize) parts; it may send several of them at once, in any
// order, and hands the accepted list back by number. The last part may be
// shorter.
type MultipartSession struct {
	Key      string
	UploadID string
	PartSize int64
}

// Part is one slice of the object. Number is 1-based and unique within a
// session; Size is the number of bytes Body holds; Body reads exactly Size
// bytes — transfer hands over a section of its spool file, never a buffer of
// the part.
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
// uploads one object as parts: transfer drives the upload call by call, so a
// part that failed is sent again without re-sending the parts already accepted.
//
// Transfer holds up to the storage's call_parallelism parts in flight at
// once: PutPart may be called concurrently, with distinct numbers, in one
// session, the calls may finish in any order, and CompleteMultipart receives the
// accepted parts by number. A plugin that accepts parts only in ascending order
// does not satisfy the capability; the conformance run (storagetest.RunMultipart)
// is what enforces the concurrent contract, and s3 runs it.
//
// The capability is discovered by call, exactly like the storage kind. A storage
// that does not implement it, and a storage that answers errors.ErrUnsupported
// to BeginMultipart because this object fits one Put, are uploaded whole.
//
// Every method is ONE attempt — retries, backoff and budgets belong to
// execution, which drives this interface, never the plugin. An error is
// classified exactly like the kind's: pluginerrors.Permanent, pluginerrors.Transient (with
// a RetryAfter hint where the service gave one), pluginerrors.NotFound only
// when the service says the upload or the object is definitely gone. Aborting an
// unfinished upload is the caller's job, and it is what keeps unfinished parts
// from being billed.
type MultipartUploader interface {
	// BeginMultipart opens one upload. A size the storage refuses outright is a
	// permanent error; a size that needs no parts is errors.ErrUnsupported,
	// answered without touching the service, and transfer then uses Put. The
	// returned PartSize is the size the caller must slice the object into.
	//
	// While the run is still producing the object, Size is the bytes produced
	// so far — a lower bound — and SHA256 is empty; transfer asks again as the
	// object grows, so a size that needs no parts must be answered
	// errors.ErrUnsupported without touching the service. Parts may be sent
	// while the object grows; CompleteMultipart is called only once it is whole.
	BeginMultipart(ctx context.Context, req PutRequest) (MultipartSession, error)
	// PutPart sends one part. Sending a number again replaces the part the
	// service already holds under it — which is what a retry of one part is. It
	// may be called concurrently with distinct numbers in one session; the
	// transfer holds up to call_parallelism of them in flight and the service may
	// accept them in any order.
	PutPart(ctx context.Context, session MultipartSession, part Part) (PartResult, error)
	// CompleteMultipart commits the upload and returns only when the service
	// accepted the commit; a part list that does not match what the service
	// holds is an error. The list carries the accepted parts by number, whatever
	// order they finished in. A commit that did not happen must be an error even
	// when the answer was HTTP 200 — the service reports some failures that way.
	CompleteMultipart(ctx context.Context, session MultipartSession, parts []PartResult) error
	// AbortMultipart discards the upload and every part of it: an upload already
	// gone is nothing left to clean, not an error. It runs on the caller's
	// context only — the cleanup owner hands it a detached, bounded context when
	// the run's own context is already dead, so the implementation neither
	// detaches the call nor owns an abort budget of its own.
	AbortMultipart(ctx context.Context, session MultipartSession) error
	// StaleMultipart lists the incomplete uploads this storage holds under its
	// own keys that were initiated before the cutoff — what a process killed
	// outright leaves behind, billed and invisible to every listing and retention.
	// The keys are relative to the storage's own root, exactly like every other
	// key; nothing outside that root is ever listed, and an upload the service
	// does not date is never called stale. The service answers one page per
	// call and the implementation follows the pages; transfer aborts each
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
// way by the protection ledger's manual admission.
type MultipartInventory interface {
	// Inventory lists every incomplete upload under the storage's own root, one
	// page per call, following the pages. An upload the service does not date is
	// included: its size is unknown either way.
	Inventory(ctx context.Context) ([]StaleUpload, error)
}
