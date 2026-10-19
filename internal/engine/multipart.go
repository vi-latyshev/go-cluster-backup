package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/human"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/spool"
	"github.com/vi-latyshev/go-cluster-backup/internal/storage"
)

// multipartUpload drives one object through a storage's multipart capability:
// begin, one call per part, complete, each under the configured retry policy —
// so a part that failed is sent again and the parts the service already accepted
// are not. It reports whether it handled the object: a storage without the
// capability, and one that answers errors.ErrUnsupported because the object fits
// a single request, leave the whole-object Put to the caller.
//
// Every failure path hands the unfinished upload to the storage's AbortMultipart:
// parts of an upload nobody finishes are billed and no retention ever sees them.
func (e *Engine) multipartUpload(ctx context.Context, sb *storageBinding, st *storageRunState,
	sp *spool.Spool, pa producedArtifact, key string) (bool, error) {
	mu, ok := sb.inst.(storage.MultipartUploader)
	if !ok || pa.bytes <= 0 {
		return false, nil
	}
	req := storage.PutRequest{Key: key, Size: pa.bytes, SHA256: pa.sha256}
	session, err := e.beginMultipart(ctx, st, mu, req, artifactName(pa))
	if errors.Is(err, errors.ErrUnsupported) {
		return false, nil
	}
	if err != nil {
		return true, err
	}
	parts, err := e.uploadParts(ctx, st, mu, sp, pa, session)
	if err != nil {
		return true, errors.Join(err, e.abortMultipart(ctx, mu, session, key))
	}
	e.storageEvent(sb, human.New("storage.multipart",
		human.Text("artifact", human.ArtifactLabel(pa.artifact.Group, pa.artifact.Name)),
		human.Count("parts", int64(len(parts))),
		human.Count("total", (pa.bytes+session.PartSize-1)/session.PartSize),
		human.Bytes("part_size", session.PartSize)))
	if err := e.completeMultipart(ctx, st, mu, session, parts); err != nil {
		// A commit whose answer was lost leaves the upload gone and the object
		// there: the service answers not_found for the upload the engine already
		// completed. The Stat decides, exactly as it does after a completion that
		// claimed success — the object is the only proof either way.
		if class, _ := plugin.ClassOf(err); class == plugin.ClassNotFound {
			if verr := e.statUploaded(ctx, st, sb, key, artifactName(pa), pa.bytes); verr == nil {
				return true, nil
			}
		}
		return true, errors.Join(err, e.abortMultipart(ctx, mu, session, key))
	}
	// The completion is not the proof: the object must be there at the size that
	// was sent, exactly as for a whole-object Put. A 200 that committed nothing
	// fails here, and the abort on top of it is what discards its parts.
	if err := e.statUploaded(ctx, st, sb, key, artifactName(pa), pa.bytes); err != nil {
		return true, errors.Join(err, e.abortMultipart(ctx, mu, session, key))
	}
	return true, nil
}

// artifactName is one produced artifact's log label, for a retry line that must
// not carry its key.
func artifactName(pa producedArtifact) string {
	return human.ArtifactLabel(pa.artifact.Group, pa.artifact.Name)
}

// statUploaded proves a completed upload is there: the object at the size that
// was sent, under the retry policy every other Stat gets.
func (e *Engine) statUploaded(ctx context.Context, st *storageRunState, sb *storageBinding, key, label string, size int64) error {
	_, err := attempt(ctx, e.retry, "verify "+label, func(ctx context.Context, _ int) (struct{}, error) {
		st.addAttempts(1)
		return struct{}{}, e.verifyObject(ctx, sb, key, label, size)
	})
	return err
}

// beginMultipart opens one upload. errors.ErrUnsupported is the plugin's answer
// "this object is one Put" — a decision, not a fault, so it costs neither the
// retry budget nor an attempt in the report.
func (e *Engine) beginMultipart(ctx context.Context, st *storageRunState, mu storage.MultipartUploader,
	req storage.PutRequest, label string) (storage.MultipartSession, error) {
	return attempt(ctx, e.retry, "begin multipart "+label, func(ctx context.Context, _ int) (storage.MultipartSession, error) {
		session, err := safeValue(func() (storage.MultipartSession, error) { return mu.BeginMultipart(ctx, req) })
		if errors.Is(err, errors.ErrUnsupported) {
			return storage.MultipartSession{}, plugin.Permanent(err)
		}
		st.addAttempts(1)
		if err != nil {
			return storage.MultipartSession{}, err
		}
		if session.PartSize <= 0 {
			return storage.MultipartSession{}, plugin.Permanent(
				fmt.Errorf("multipart %s: the plugin answered a part size of %d", req.Key, session.PartSize))
		}
		return session, nil
	})
}

// uploadParts sends the object's parts in order. Each part is its own call and
// its own retry budget: a failed part is opened again from the spool file —
// never the parts already accepted — and the attempt is counted in the report
// like any other.
func (e *Engine) uploadParts(ctx context.Context, st *storageRunState, mu storage.MultipartUploader,
	sp *spool.Spool, pa producedArtifact, session storage.MultipartSession) ([]storage.PartResult, error) {
	total := (pa.bytes + session.PartSize - 1) / session.PartSize
	results := make([]storage.PartResult, 0, total)
	for number, offset := int32(1), int64(0); offset < pa.bytes; number++ {
		size := min(session.PartSize, pa.bytes-offset)
		res, err := attempt(ctx, e.retry, fmt.Sprintf("part %d of %s", number, artifactName(pa)),
			func(ctx context.Context, _ int) (storage.PartResult, error) {
				st.addAttempts(1)
				f, err := sp.Open(pa.file)
				if err != nil {
					return storage.PartResult{}, plugin.Permanent(err)
				}
				defer func() { _ = f.Close() }()
				// A section of the spool file, not a buffer: the storage reads
				// exactly this part's bytes and holds none of the rest.
				part := storage.Part{Number: number, Size: size, Body: io.NewSectionReader(f, offset, size)}
				return safeValue(func() (storage.PartResult, error) { return mu.PutPart(ctx, session, part) })
			})
		if err != nil {
			return nil, err
		}
		if res.Number != number {
			return nil, plugin.Permanent(fmt.Errorf(
				"multipart %s: the plugin answered part %d for part %d", session.Key, res.Number, number))
		}
		results = append(results, res)
		offset += size
	}
	return results, nil
}

// completeMultipart commits the upload. Retrying it is safe: the parts are named
// by number and ETag, so a commit that already happened is answered as an upload
// that is gone, and the verification after it decides what actually landed.
func (e *Engine) completeMultipart(ctx context.Context, st *storageRunState, mu storage.MultipartUploader,
	session storage.MultipartSession, parts []storage.PartResult) error {
	_, err := attempt(ctx, e.retry, "complete multipart "+session.Key, func(ctx context.Context, _ int) (struct{}, error) {
		st.addAttempts(1)
		return struct{}{}, safeCall(func() error { return mu.CompleteMultipart(ctx, session, parts) })
	})
	return err
}

// abortMultipart discards an unfinished upload. It runs on the context the
// upload ran under, dead or not — the storage calls the service on a context of
// its own, because the moment an abort is needed is exactly the moment that
// context is gone. A failed abort is logged and joined into the failure: the
// parts stay billed, and nothing hides the original error.
func (e *Engine) abortMultipart(ctx context.Context, mu storage.MultipartUploader,
	session storage.MultipartSession, key string) error {
	err := safeCall(func() error { return mu.AbortMultipart(ctx, session) })
	if err != nil {
		e.event(human.Failure("multipart.abort.failed",
			human.Text("reason", "the parts stay billed: "+err.Error()), human.ObjectKey("key", key)))
	}
	return err
}

// sweepStaleUploads aborts the multipart uploads a killed run left behind: a pod
// lost outright mid-upload leaves parts that are billed, that no retention and no
// listing ever sees, and that only the next run can come back for. It runs once
// per run per storage that uploads in parts, at the run's start, under the
// storage's own budget, and never fails the run: an upload that cannot be swept
// stays billed, and the night's copy is what matters.
//
// The cutoff is now minus the longest a live run can last — its deadline plus its
// finish allowance — so an upload a run still might be writing can never be
// taken: a manual run overlapping the nightly one is exactly that case. The
// uploads are aborted one call per upload, each under the engine's own retry
// policy, and every one of them is written to the run log.
// It reports whether the storage's occupancy is now UNKNOWN: a listing or an
// abort that failed leaves bytes nobody can count, and a manual run treats
// unknown as used.
func (e *Engine) sweepStaleUploads(ctx context.Context, sb *storageBinding, st *storageRunState, cutoff time.Time) (unknown bool) {
	mu, ok := sb.inst.(storage.MultipartUploader)
	if !ok {
		return false
	}
	st.mu.Lock()
	fatal := st.fatal != nil
	st.mu.Unlock()
	if fatal {
		return false
	}
	var stale []storage.StaleUpload
	_, err := storageCall(&st.clock, ctx, func(ctx context.Context) (struct{}, error) {
		return attempt(ctx, e.retry, "list stale multipart "+sb.name, func(ctx context.Context, _ int) (struct{}, error) {
			st.addAttempts(1)
			var err error
			stale, err = safeValue(func() ([]storage.StaleUpload, error) { return mu.StaleMultipart(ctx, cutoff) })
			return struct{}{}, err
		})
	})
	if err != nil {
		e.storageEvent(sb, human.Failure("multipart.stale-list.failed",
			human.Text("reason", "uploads a killed run left stay billed: "+err.Error())))
		return true
	}
	for _, u := range stale {
		session := storage.MultipartSession{Key: u.Key, UploadID: u.UploadID}
		_, err := attempt(ctx, e.retry, "abort stale multipart "+u.Key, func(ctx context.Context, _ int) (struct{}, error) {
			st.addAttempts(1)
			return struct{}{}, safeCall(func() error { return mu.AbortMultipart(ctx, session) })
		})
		if err != nil {
			unknown = true
			e.storageEvent(sb, human.Failure("multipart.stale-abort.failed",
				human.Text("reason", "the parts stay billed: "+err.Error()), human.ObjectKey("key", u.Key)))
			continue
		}
		e.storageEvent(sb, human.New("multipart.stale-aborted",
			human.Text("run", "a killed run left this upload behind"),
			human.Text("initiated", u.Initiated.Format(time.RFC3339)), human.ObjectKey("key", u.Key)))
	}
	return unknown
}
