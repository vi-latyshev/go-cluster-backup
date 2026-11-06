package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/human"
	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/spool"
	"github.com/vi-latyshev/go-cluster-backup/internal/storage"
)

// partSource is one artifact's bytes as the parts loop sees them: the bytes that
// are final, whether the object is whole, and the spool file they live in. A
// source that waits for a producer is the overlap; one that reports its size at
// once is the after-produce upload.
type partSource interface {
	// next reports the bytes final so far and whether the object is whole after
	// the caller consumed after bytes. It blocks only when there is nothing new
	// to offer; an error means the object will never be whole.
	next(ctx context.Context, after int64) (n int64, done bool, err error)
	// file names the spool file the parts are read from.
	file() string
	// terminal reports a channel that closes on the next change of the source's
	// state, and the producer's own failure the moment its growth recorded it.
	// Both come from one snapshot, so a failure between a check and a wait is
	// never lost; a finished source has neither.
	terminal() (changed <-chan struct{}, err error)
}

// finishedSource is an object that is already whole: the after-produce path.
type finishedSource struct {
	name string
	size int64
}

func (s *finishedSource) next(context.Context, int64) (int64, bool, error) {
	return s.size, true, nil
}

func (s *finishedSource) file() string { return s.name }

// terminal: the finished source has no producer to fail and never changes.
func (s *finishedSource) terminal() (<-chan struct{}, error) { return nil, nil }

// followSource is the first produce attempt while it grows. It remembers the
// change it last saw, so a change that landed between two calls is never lost:
// a call with bytes beyond the caller's mark returns at once, and one that finds
// nothing new waits for the next change.
type followSource struct {
	g    *growth
	name string
	seen <-chan struct{}
}

func (s *followSource) next(ctx context.Context, after int64) (int64, bool, error) {
	for {
		file, written, done, changed, err := s.g.snapshot()
		if done || err != nil || written > after || changed != s.seen {
			s.name, s.seen = file, changed
			return written, done, err
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return 0, false, context.Cause(ctx)
		}
	}
}

func (s *followSource) file() string { return s.name }

// terminal reports the producer's failure the moment its growth recorded it —
// before the retry removes the failed attempt's spool file — and the channel
// that wakes the slot wait on the next change.
func (s *followSource) terminal() (<-chan struct{}, error) {
	_, _, _, changed, err := s.g.snapshot()
	return changed, err
}

// multipartUpload drives one finished object through a storage's multipart
// capability: begin, one call per part, complete, each under the configured
// retry policy — so a part that failed is sent again and the parts the service
// already accepted are not. It reports whether it handled the object: a storage
// without the capability, and one that answers errors.ErrUnsupported because
// the object fits a single request, leave the whole-object Put to the caller.
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
	parts, err := e.uploadParts(ctx, st, mu, sp, &finishedSource{name: pa.file, size: pa.bytes},
		session, artifactName(pa), sb.cfg.CallParallelism)
	if err != nil {
		return true, errors.Join(err, e.abortMultipart(ctx, st, mu, session, key))
	}
	e.multipartEvent(sb, artifactName(pa), pa.bytes, session, len(parts))
	return true, e.finishMultipart(ctx, st, sb, mu, session, parts, key, artifactName(pa), pa.bytes)
}

// finishMultipart commits a complete part list and proves the object is there.
// A commit whose answer was lost leaves the upload gone and the object there:
// the service answers not_found for the upload the engine already completed, and
// the Stat decides, exactly as it does after a completion that claimed success —
// the object is the only proof either way. Every failure hands the unfinished
// upload to AbortMultipart.
func (e *Engine) finishMultipart(ctx context.Context, st *storageRunState, sb *storageBinding,
	mu storage.MultipartUploader, session storage.MultipartSession, parts []storage.PartResult,
	key, label string, size int64) error {
	if err := e.completeMultipart(ctx, st, mu, session, parts); err != nil {
		if class, _ := plugin.ClassOf(err); class == plugin.ClassNotFound {
			if verr := e.statUploaded(ctx, st, sb, key, label, size); verr == nil {
				return nil
			}
		}
		return errors.Join(err, e.abortMultipart(ctx, st, mu, session, key))
	}
	if err := e.statUploaded(ctx, st, sb, key, label, size); err != nil {
		return errors.Join(err, e.abortMultipart(ctx, st, mu, session, key))
	}
	return nil
}

// followUpload drives one multipart-capable storage through an artifact that is
// still being produced. It reserves the run's artifact bound in the storage's
// ledger, asks the plugin for an upload as the file grows — an answer of
// errors.ErrUnsupported means the object still fits one request, and costs the
// service nothing — and sends every part whose bytes are final. When the
// producer finishes it sends the tail, commits, verifies and books the exact
// size.
//
// A producer that fails, and a storage whose bound does not fit a manual run's
// admission, fall back to the after-produce upload: the same calls on the
// finished file, exactly the path a storage without the capability takes.
func (e *Engine) followUpload(ctx context.Context, sb *storageBinding, st *storageRunState,
	sp *spool.Spool, g *growth, key, label string, kind model.RunKind, caps map[string]*capState,
	ready <-chan struct{}, produced *produceOutcome) bool {
	st.mu.Lock()
	fatal := st.fatal
	st.mu.Unlock()
	if fatal != nil {
		return false
	}
	mu := sb.inst.(storage.MultipartUploader)
	cap := caps[sb.name]
	if cap != nil {
		if !cap.reserve(key, e.cfg.Run.MaxArtifactBytes.Int64(), kind == model.RunKindManual) {
			return e.uploadAfterProduce(ctx, sb, st, sp, key, label, kind, caps, ready, produced, false)
		}
	}
	src := &followSource{g: g}
	started := false
	var session storage.MultipartSession
	opened := false
	var final int64
	probed := int64(-1)
	for !opened {
		n, done, err := src.next(ctx, probed)
		if err != nil {
			return e.abandonFollow(ctx, sb, st, sp, mu, cap, opened, session,
				key, label, kind, caps, ready, produced, started)
		}
		if done {
			final = n
			break
		}
		if !started {
			e.storageEvent(sb, human.New("storage.upload.start", human.Text("artifact", label)))
			started = true
		}
		session, opened, err = e.probeBegin(ctx, st, mu, key, n, label)
		if err != nil {
			return e.failFollow(ctx, sb, st, cap, opened, mu, session, key, label, err)
		}
		probed = n
	}
	if !opened {
		// The object fit one request, or the producer finished before a session
		// could open: the finished file decides, as it always has.
		if cap != nil {
			cap.shrink(key, final)
		}
		return e.uploadAfterProduce(ctx, sb, st, sp, key, label, kind, caps, ready, produced, started)
	}
	parts, err := e.uploadParts(ctx, st, mu, sp, src, session, label, sb.cfg.CallParallelism)
	if err != nil {
		// The upload recorded where its cause came from. Only the producer's OWN
		// failure falls back to the finished file; a part's failure and the run's
		// cancellation are reported as the cause the upload chose — never replaced
		// by the context's state at this later moment.
		origin, cause := terminalOf(err)
		if origin == originSource {
			return e.abandonFollow(ctx, sb, st, sp, mu, cap, true, session,
				key, label, kind, caps, ready, produced, started)
		}
		return e.failFollow(ctx, sb, st, cap, true, mu, session, key, label, cause)
	}
	<-ready
	pa := produced.pa
	if cap != nil {
		cap.shrink(key, pa.bytes)
	}
	e.multipartEvent(sb, label, pa.bytes, session, len(parts))
	if err := e.finishMultipart(ctx, st, sb, mu, session, parts, key, label, pa.bytes); err != nil {
		e.failUpload(ctx, sb, st, label, key, err)
		return false
	}
	st.mu.Lock()
	st.tried = true
	st.mu.Unlock()
	if cap != nil {
		cap.commit(key, pa.bytes)
	}
	st.markObject(pa.bytes)
	e.storageEvent(sb, human.New("storage.upload.done",
		human.Text("artifact", label), human.Bytes("size", pa.bytes)))
	return true
}

// probeBegin asks the storage whether the bytes produced so far already need
// parts. errors.ErrUnsupported means "not yet" and is not a failure; any other
// error fails the storage's copy.
func (e *Engine) probeBegin(ctx context.Context, st *storageRunState, mu storage.MultipartUploader,
	key string, size int64, label string) (storage.MultipartSession, bool, error) {
	session, err := e.beginMultipart(ctx, st, mu, storage.PutRequest{Key: key, Size: size}, label)
	if errors.Is(err, errors.ErrUnsupported) {
		return storage.MultipartSession{}, false, nil
	}
	if err != nil {
		return storage.MultipartSession{}, false, err
	}
	return session, true, nil
}

// failFollow ends an overlapping upload with the storage's own failure: the
// unfinished upload is aborted once — the failure that got here already
// returned its in-flight part — and the booking is released when the abort
// confirmed nothing is left.
func (e *Engine) failFollow(ctx context.Context, sb *storageBinding, st *storageRunState, cap *capState,
	opened bool, mu storage.MultipartUploader, session storage.MultipartSession,
	key, label string, cause error) bool {
	if opened {
		if err := e.abortMultipart(ctx, st, mu, session, key); err == nil && cap != nil {
			cap.unreserve(key)
		}
	} else if cap != nil {
		cap.unreserve(key)
	}
	e.failUpload(ctx, sb, st, label, key, cause)
	return false
}

// abandonFollow ends an overlapping upload because the producer failed or the
// run was cancelled — never the storage's own fault. The unfinished upload is
// aborted once and its booking released when the abort confirmed it; the storage
// is not marked tried, so the report keeps saying the artifact never reached it.
// A producer that recovered on a later attempt gets the finished-file upload.
func (e *Engine) abandonFollow(ctx context.Context, sb *storageBinding, st *storageRunState, sp *spool.Spool,
	mu storage.MultipartUploader, cap *capState, opened bool, session storage.MultipartSession,
	key, label string, kind model.RunKind, caps map[string]*capState,
	ready <-chan struct{}, produced *produceOutcome, started bool) bool {
	if opened {
		if err := e.abortMultipart(ctx, st, mu, session, key); err != nil {
			// The parts may still exist and are billed: the booking stays, and
			// this storage never reuses the key to upload the retry's file. A
			// manual run counts the leftover bytes until the run ends.
			e.failUpload(ctx, sb, st, label, key, fmt.Errorf("aborting the failed attempt: %w", err))
			return false
		}
	}
	if cap != nil {
		cap.unreserve(key)
	}
	<-ready
	return e.uploadAfterProduce(ctx, sb, st, sp, key, label, kind, caps, ready, produced, started)
}

// uploadAfterProduce is the storage's turn once the producer is done: the
// finished file goes through the same upload a storage without the capability
// uses, exactly as it did before overlapping existed.
func (e *Engine) uploadAfterProduce(ctx context.Context, sb *storageBinding, st *storageRunState,
	sp *spool.Spool, key, label string, kind model.RunKind, caps map[string]*capState,
	ready <-chan struct{}, produced *produceOutcome, started bool) bool {
	<-ready
	if produced.err != nil {
		return false
	}
	if !started {
		e.storageEvent(sb, human.New("storage.upload.start", human.Text("artifact", label)))
	}
	if !e.uploadObject(ctx, sb, st, produced.pa, sp, key, kind, caps) {
		return false
	}
	e.storageEvent(sb, human.New("storage.upload.done",
		human.Text("artifact", label), human.Bytes("size", produced.pa.bytes)))
	return true
}

// multipartEvent reports one upload's shape after its parts were accepted.
func (e *Engine) multipartEvent(sb *storageBinding, label string, size int64, session storage.MultipartSession, parts int) {
	e.storageEvent(sb, human.New("storage.multipart",
		human.Text("artifact", label),
		human.Count("parts", int64(parts)),
		human.Count("total", (size+session.PartSize-1)/session.PartSize),
		human.Bytes("part_size", session.PartSize)))
}

// artifactName is one produced artifact's log label, for a retry line that must
// not carry its key.
func artifactName(pa producedArtifact) string {
	return human.ArtifactLabel(pa.artifact.Group, pa.artifact.Name)
}

// statUploaded proves a completed upload is there: the object at the size that
// was sent, under the retry policy every other Stat gets.
func (e *Engine) statUploaded(ctx context.Context, st *storageRunState, sb *storageBinding, key, label string, size int64) error {
	return storageCallErr(&st.clock, ctx, func(ctx context.Context) error {
		return attemptErr(ctx, e.retry, "verify "+label, func(ctx context.Context, _ int) error {
			st.addAttempts(1)
			return e.verifyObject(ctx, sb, key, label, size)
		})
	})
}

// beginMultipart opens one upload. errors.ErrUnsupported is the plugin's answer
// "this object is one Put" — a decision, not a fault, so it costs neither the
// retry budget nor an attempt in the report.
func (e *Engine) beginMultipart(ctx context.Context, st *storageRunState, mu storage.MultipartUploader,
	req storage.PutRequest, label string) (storage.MultipartSession, error) {
	return storageCall(&st.clock, ctx, func(ctx context.Context) (storage.MultipartSession, error) {
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
	})
}

// producerFailure is the source's own failure, wrapped where the parts loop
// chose it as the upload's cause: the caller that sees it may fall back to the
// finished file, and nothing else may. A part's refusal and the run's
// cancellation are not producer failures, whatever the producer did a moment
// later.
type producerFailure struct{ err error }

func (p *producerFailure) Error() string { return p.err.Error() }
func (p *producerFailure) Unwrap() error { return p.err }

// terminalOrigin is where the cause of one upload's end came from: a part's own
// failure, the producer's failure, or the run's cancellation, by that priority.
type terminalOrigin uint8

const (
	originPart terminalOrigin = iota + 1
	originSource
	originParent
)

// uploadError is the ONE cause an upload chose, with the origin recorded when
// the event happened. Its callers act on the origin and report the error; they
// never re-derive either from the context or the growth afterwards.
type uploadError struct {
	origin terminalOrigin
	err    error
}

func (u *uploadError) Error() string { return u.err.Error() }
func (u *uploadError) Unwrap() error { return u.err }

// terminalOf reads the origin and cause uploadParts recorded. Any other error is
// reported as the upload's own part failure, so a caller still fails the
// storage instead of guessing a fallback.
func terminalOf(err error) (terminalOrigin, error) {
	var ue *uploadError
	if errors.As(err, &ue) {
		return ue.origin, ue.err
	}
	return originPart, err
}

func isContextError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// uploadTerminal records the facts that can end one multipart upload, each when
// it happens. The cause is chosen here, once, from the recorded facts by the
// plan's priority: an independent part failure, then the producer's failure,
// then the run's cancellation. Recording a cause and stopping the upload are ONE
// step — the recorder holds the cancel function — so no caller can record a
// terminal fact and forget to end the upload. Nothing here reads the context or
// the growth to re-classify an event that already happened.
type uploadTerminal struct {
	mu     sync.Mutex
	stop   context.CancelCauseFunc
	part   error
	source *producerFailure
	parent error
}

// recordPart records one part's failure and stops the upload when it is the
// cause. An error is a consequence, not a cause, only when it is the upload's
// own recorded cause coming back, a context error this upload's stop or the
// run's end imposed on the part, or a spool file that vanished after the
// producer's failure was already recorded. Any other part failure, including a
// vanished spool file with no producer failure behind it, is the cause.
func (t *uploadTerminal) recordPart(parent context.Context, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.part != nil {
		return
	}
	if t.source != nil && errors.Is(err, t.source) {
		return
	}
	if isContextError(err) && (t.source != nil || parent.Err() != nil) {
		return
	}
	if errors.Is(err, errVanishedPart) && t.source != nil {
		return
	}
	t.part = err
	t.stop(err)
}

// recordSource records the producer's failure and stops the upload at once; a
// part failure already recorded keeps the priority, so the source is recorded
// but the upload is already ending through that part.
func (t *uploadTerminal) recordSource(err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.source != nil {
		return
	}
	t.source = &producerFailure{err: err}
	if t.part == nil {
		t.stop(t.source)
	}
}

// recordSourceFrom records the producer's terminal state when the source
// reports one: the single exit calls it so a failure no part or dispatcher call
// happened to carry is still in the facts before the cause is chosen. A context
// error the run's own cancellation produced is not a producer failure.
func (t *uploadTerminal) recordSourceFrom(parent context.Context, src partSource) {
	_, sf := src.terminal()
	if sf == nil {
		return
	}
	t.recordSourceFact(parent, sf)
}

// recordSourceFact records one producer failure, unless it is the context error
// the run's own cancellation produced: that is the run's fact, not the
// producer's.
func (t *uploadTerminal) recordSourceFact(parent context.Context, err error) {
	if isContextError(err) && parent.Err() != nil {
		return
	}
	t.recordSource(err)
}

// recordNext records the error the parts source answered: the upload's own
// cause coming back is not a new fact, a context error while the run is
// cancelled is the run's cancellation, and anything else is the producer's
// failure — recorded, and stopped, even when the run has also been cancelled.
func (t *uploadTerminal) recordNext(parent context.Context, err error) {
	t.mu.Lock()
	own := t.part != nil || t.source != nil
	t.mu.Unlock()
	if own {
		return
	}
	if isContextError(err) && parent.Err() != nil {
		t.recordParent(err)
		return
	}
	t.recordSource(err)
}

// recordVanished records the producer's failure a vanished spool file points at,
// when the source reports one, before the file's part error is judged: the
// producer's failure is a terminal cause, and recording it stops the upload.
func (t *uploadTerminal) recordVanished(parent context.Context, src partSource) {
	t.recordSourceFrom(parent, src)
}

// recordParent records the run's cancellation, which never outranks a cause the
// upload recorded itself.
func (t *uploadTerminal) recordParent(err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.parent == nil {
		t.parent = err
	}
}

// cause is the ONE place the upload's cause is chosen.
func (t *uploadTerminal) cause() *uploadError {
	t.mu.Lock()
	defer t.mu.Unlock()
	switch {
	case t.part != nil:
		return &uploadError{origin: originPart, err: t.part}
	case t.source != nil:
		return &uploadError{origin: originSource, err: t.source}
	case t.parent != nil:
		return &uploadError{origin: originParent, err: t.parent}
	}
	return nil
}

// errVanishedPart marks a part whose spool file the producer's own retry removed
// under it: only an already recorded producer failure may explain such a
// vanished file, and a part that finds it without one is a terminal failure of
// its own.
var errVanishedPart = errors.New("the spool file the part reads is gone")

// uploadParts sends the object's parts from the source it is handed — a file
// that is already whole, or one the producer is still writing — with at most
// `parallelism` of them in flight: the ceiling the storage's call_parallelism
// states. A part is sent only once its bytes are final, and every part is its
// own storage call and its own retry budget, so a failed part is opened again
// from the spool file, never the parts already accepted.
//
// Every event that can end the upload is recorded in an uploadTerminal when it
// happens, and the cause is chosen there — a part's own failure first, then the
// producer's failure (wrapped so the caller can fall back to the finished file),
// then the run's cancellation. The upload has ONE exit: the chosen cause
// cancels its own context, every in-flight part returns, and only then the
// caller may abort. A part that ended because a neighbour failed never hides the
// cause. The parts are returned by number, whatever order they finished in.
func (e *Engine) uploadParts(ctx context.Context, st *storageRunState, mu storage.MultipartUploader,
	sp *spool.Spool, src partSource, session storage.MultipartSession, label string, parallelism int) ([]storage.PartResult, error) {
	pctx, stop := context.WithCancelCause(ctx)
	defer stop(nil)
	slots := make(chan struct{}, parallelism)
	var wg sync.WaitGroup
	term := &uploadTerminal{stop: stop}
	results := map[int32]storage.PartResult{}
	var resMu sync.Mutex

	number := int32(1)
	var offset int64
	seen := int64(-1)
dispatch:
	for {
		n, done, err := src.next(pctx, seen)
		if err != nil {
			term.recordNext(ctx, err)
			break
		}
		seen = n
		file := src.file()
		for offset < n && (done || offset+session.PartSize < n) {
			size := min(session.PartSize, n-offset)
		slotsWait:
			for {
				// The producer can end while every slot is busy: its terminal
				// state is read with the notification channel in one snapshot,
				// recorded — which stops the upload — and the dispatcher leaves
				// at once, never waiting for a slot or the storage budget.
				changed, sf := src.terminal()
				if sf != nil {
					term.recordSourceFact(ctx, sf)
					break dispatch
				}
				select {
				case slots <- struct{}{}:
				case <-pctx.Done():
					if ctx.Err() != nil {
						term.recordParent(ctx.Err())
					}
					break dispatch
				case <-changed:
					// The growth changed: loop and read its terminal state again.
					continue
				}
				break slotsWait
			}
			num, off := number, offset
			wg.Add(1)
			go func() {
				defer wg.Done()
				res, perr := e.sendPart(pctx, st, mu, sp, file, session, num, off, size, label)
				if perr != nil {
					// A part that found its spool file gone asks the source
					// whether the producer failed before this moment: that fact
					// is recorded as it is, and only then can the vanished file
					// be read as the failed attempt's cleanup.
					if errors.Is(perr, errVanishedPart) {
						term.recordVanished(ctx, src)
					}
					term.recordPart(ctx, perr)
				} else {
					resMu.Lock()
					results[num] = res
					resMu.Unlock()
				}
				<-slots
			}()
			offset += size
			number++
		}
		if done && offset >= n {
			break
		}
	}
	wg.Wait()
	// The single exit records the producer's terminal state, so a failure no
	// part or dispatcher call happened to carry is still in the facts before the
	// cause is chosen; an already recorded part failure keeps the priority.
	if ctx.Err() != nil {
		term.recordParent(ctx.Err())
	}
	term.recordSourceFrom(ctx, src)
	if cause := term.cause(); cause != nil {
		return nil, cause
	}
	parts := make([]storage.PartResult, 0, len(results))
	for n := int32(1); n < number; n++ {
		parts = append(parts, results[n])
	}
	return parts, nil
}

// sendPart is one part's own storage call: the retry loop runs inside the
// budget, so a slow part and its backoff are charged to the storage, and the
// bytes are read from the file that produced them (a replay of an earlier
// attempt included). A cancelled upload opens no file and books no attempt: the
// context is checked here, immediately before the call, because a slot and the
// cancellation can be ready together.
func (e *Engine) sendPart(ctx context.Context, st *storageRunState, mu storage.MultipartUploader,
	sp *spool.Spool, file string, session storage.MultipartSession, number int32, offset, size int64, label string) (storage.PartResult, error) {
	if cause := context.Cause(ctx); cause != nil {
		return storage.PartResult{}, cause
	}
	return storageCall(&st.clock, ctx, func(ctx context.Context) (storage.PartResult, error) {
		res, err := attempt(ctx, e.retry, fmt.Sprintf("part %d of %s", number, label),
			func(ctx context.Context, _ int) (storage.PartResult, error) {
				st.addAttempts(1)
				f, err := sp.Open(file)
				if err != nil {
					if errors.Is(err, os.ErrNotExist) {
						return storage.PartResult{}, plugin.Permanent(fmt.Errorf("%w: %w", errVanishedPart, err))
					}
					return storage.PartResult{}, plugin.Permanent(err)
				}
				defer func() { _ = f.Close() }()
				// A section of the spool file, not a buffer: the storage reads
				// exactly this part's bytes and holds none of the rest.
				part := storage.Part{Number: number, Size: size, Body: io.NewSectionReader(f, offset, size)}
				return safeValue(func() (storage.PartResult, error) { return mu.PutPart(ctx, session, part) })
			})
		if err != nil {
			return storage.PartResult{}, err
		}
		if res.Number != number {
			return storage.PartResult{}, plugin.Permanent(fmt.Errorf(
				"multipart %s: the plugin answered part %d for part %d", session.Key, res.Number, number))
		}
		return res, nil
	})
}

// completeMultipart commits the upload. Retrying it is safe: the parts are named
// by number and ETag, so a commit that already happened is answered as an upload
// that is gone, and the verification after it decides what actually landed.
func (e *Engine) completeMultipart(ctx context.Context, st *storageRunState, mu storage.MultipartUploader,
	session storage.MultipartSession, parts []storage.PartResult) error {
	return storageCallErr(&st.clock, ctx, func(ctx context.Context) error {
		return attemptErr(ctx, e.retry, "complete multipart "+session.Key, func(ctx context.Context, _ int) error {
			st.addAttempts(1)
			return safeCall(func() error { return mu.CompleteMultipart(ctx, session, parts) })
		})
	})
}

// abortMultipart discards an unfinished upload. It runs on the context the
// upload ran under, dead or not — the storage calls the service on a context of
// its own, because the moment an abort is needed is exactly the moment that
// context is gone. A failed abort is logged and joined into the failure: the
// parts stay billed, and nothing hides the original error.
func (e *Engine) abortMultipart(ctx context.Context, st *storageRunState, mu storage.MultipartUploader,
	session storage.MultipartSession, key string) error {
	err := storageCallErr(&st.clock, ctx, func(context.Context) error {
		return safeCall(func() error { return mu.AbortMultipart(ctx, session) })
	})
	if err != nil {
		e.event(human.Failure("multipart.abort.failed",
			human.Text("reason", "the parts stay billed: "+err.Error()), human.ObjectKey("key", key)))
	}
	return err
}

// storageCallErr is storageCall for a call with no value: the error is the whole
// result.
func storageCallErr(clock *storageClock, parent context.Context, fn func(ctx context.Context) error) error {
	_, err := storageCall(clock, parent, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, fn(ctx)
	})
	return err
}

// attemptErr is attempt for a call with no value.
func attemptErr(ctx context.Context, p retryPolicy, what string, fn func(ctx context.Context, attempt int) error) error {
	_, err := attempt(ctx, p, what, func(ctx context.Context, attempt int) (struct{}, error) {
		return struct{}{}, fn(ctx, attempt)
	})
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
