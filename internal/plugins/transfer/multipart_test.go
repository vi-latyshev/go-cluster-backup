package transfer

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	pluginerrors "github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/execution"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/inventory"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/protection"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/spool"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage/guard"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage/storagetest"
)

// fakeMultipart is a storagetest.Fake with the multipart capability: parts are
// counted per number — which is how the transfer's per-part retry is measured —
// and assembled by Complete into the in-memory fake, so the transfer's own Stat
// and the test's Bytes see exactly what was committed.
type fakeMultipart struct {
	*storagetest.Fake
	partSize int64

	// partErr fails one part call, with the number it names and how many calls
	// that part has already taken. partDelay runs inside every part call, for a
	// test that spends a storage's time budget or cancels the run mid-upload.
	partErr   func(number int32, call int) error
	partDelay func(number int32, ctx context.Context) error
	// beginHook and partHook run at the start of those calls: what a test uses to
	// learn an upload opened, or a part arrived, while the producer is gated.
	beginHook func(req storage.PutRequest)
	partHook  func(number int32)

	// completeDrop makes Complete answer success while committing nothing: the
	// service's own answer is never the proof, the Stat after it is.
	completeDrop bool
	// completeLost makes Complete answer the way a retry after a commit whose
	// answer was lost does: the upload is gone, the object is there, the answer
	// is not_found. completeGone is the same answer with nothing committed.
	completeLost bool
	completeGone bool

	// stale are the uploads the sweep sees — what a killed run leaves behind,
	// filtered by the cutoff exactly as a service lists them; staleErr fails the
	// listing. A stale entry is not an open upload of this fake: only the sweep's
	// abort of it is counted.
	stale    []storage.StaleUpload
	staleErr error
	// inventory is every incomplete upload of any age, undated included; the
	// inventoryErr hook fails one call by number. inventoryCalls counts them.
	inventory      []storage.StaleUpload
	inventoryErr   func(call int) error
	inventoryCalls int

	mu          sync.Mutex
	begins      int
	aborts      int
	completes   int
	staleLists  int
	abortedKeys []string
	calls       map[int32]int
	uploads     map[string]*fakeMPUpload
	next        int

	// The parallel witness: parts in flight at once, how many were in flight at
	// every Abort, the calls in order, and the numbers Complete was handed.
	inFlight      int
	maxInFlight   int
	abortInFlight []int
	journal       []string
	completeOrder []int32
	abortHook     func()
	// sessions counts the uploads actually opened: the probes answered
	// errors.ErrUnsupported are Begin calls too, and are not sessions.
	sessions int
}

type fakeMPUpload struct {
	key   string
	parts map[int32][]byte
}

func newFakeMultipart(partSize int64) *fakeMultipart {
	return &fakeMultipart{
		Fake: storagetest.NewFake(), partSize: partSize,
		calls: map[int32]int{}, uploads: map[string]*fakeMPUpload{},
	}
}

func (f *fakeMultipart) BeginMultipart(ctx context.Context, req storage.PutRequest) (storage.MultipartSession, error) {
	if err := ctx.Err(); err != nil {
		return storage.MultipartSession{}, err
	}
	if f.beginHook != nil {
		f.beginHook(req)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.begins++
	f.journal = append(f.journal, "begin")
	if req.Size <= f.partSize {
		return storage.MultipartSession{}, fmt.Errorf("object %s fits one part: %w", req.Key, errors.ErrUnsupported)
	}
	f.next++
	id := fmt.Sprintf("upload-%d", f.next)
	f.uploads[id] = &fakeMPUpload{key: req.Key, parts: map[int32][]byte{}}
	f.sessions++
	return storage.MultipartSession{Key: req.Key, UploadID: id, PartSize: f.partSize}, nil
}

func (f *fakeMultipart) PutPart(ctx context.Context, session storage.MultipartSession, part storage.Part) (storage.PartResult, error) {
	f.mu.Lock()
	f.inFlight++
	if f.inFlight > f.maxInFlight {
		f.maxInFlight = f.inFlight
	}
	f.journal = append(f.journal, fmt.Sprintf("part %d", part.Number))
	f.mu.Unlock()
	defer func() {
		f.mu.Lock()
		f.inFlight--
		f.mu.Unlock()
	}()
	if f.partHook != nil {
		f.partHook(part.Number)
	}
	if f.partDelay != nil {
		if err := f.partDelay(part.Number, ctx); err != nil {
			return storage.PartResult{}, err
		}
	}
	if err := ctx.Err(); err != nil {
		return storage.PartResult{}, err
	}
	f.mu.Lock()
	f.calls[part.Number]++
	call := f.calls[part.Number]
	partErr := f.partErr
	f.mu.Unlock()
	if partErr != nil {
		if err := partErr(part.Number, call); err != nil {
			return storage.PartResult{}, err
		}
	}
	data, err := io.ReadAll(io.LimitReader(part.Body, part.Size+1))
	if err != nil {
		return storage.PartResult{}, err
	}
	if int64(len(data)) != part.Size {
		return storage.PartResult{}, fmt.Errorf("part %d carried %d bytes, want %d", part.Number, len(data), part.Size)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	upload, ok := f.uploads[session.UploadID]
	if !ok {
		return storage.PartResult{}, pluginerrors.NotFound(errors.New("no such upload"))
	}
	upload.parts[part.Number] = data
	return storage.PartResult{Number: part.Number, ETag: fmt.Sprintf("etag-%d", part.Number)}, nil
}

func (f *fakeMultipart) CompleteMultipart(ctx context.Context, session storage.MultipartSession, parts []storage.PartResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	f.completes++
	f.journal = append(f.journal, "complete")
	f.completeOrder = f.completeOrder[:0]
	for _, p := range parts {
		f.completeOrder = append(f.completeOrder, p.Number)
	}
	upload, ok := f.uploads[session.UploadID]
	if !ok {
		f.mu.Unlock()
		return pluginerrors.NotFound(errors.New("no such upload"))
	}
	var body []byte
	for _, p := range parts {
		data, ok := upload.parts[p.Number]
		if !ok {
			f.mu.Unlock()
			return fmt.Errorf("part %d is not on the upload", p.Number)
		}
		body = append(body, data...)
	}
	delete(f.uploads, session.UploadID)
	f.mu.Unlock()
	if f.completeGone {
		return pluginerrors.NotFound(errors.New("no such upload"))
	}
	if !f.completeDrop {
		f.PutObject(session.Key, body, time.Now())
	}
	if f.completeLost {
		return pluginerrors.NotFound(errors.New("no such upload"))
	}
	return nil
}

func (f *fakeMultipart) AbortMultipart(ctx context.Context, session storage.MultipartSession) error {
	if f.abortHook != nil {
		f.abortHook()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.aborts++
	f.abortedKeys = append(f.abortedKeys, session.Key)
	f.abortInFlight = append(f.abortInFlight, f.inFlight)
	f.journal = append(f.journal, "abort")
	if _, ok := f.uploads[session.UploadID]; !ok {
		return nil
	}
	delete(f.uploads, session.UploadID)
	return nil
}

func (f *fakeMultipart) StaleMultipart(ctx context.Context, olderThan time.Time) ([]storage.StaleUpload, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.staleLists++
	if f.staleErr != nil {
		return nil, f.staleErr
	}
	var out []storage.StaleUpload
	for _, u := range f.stale {
		if u.Initiated.Before(olderThan) {
			out = append(out, u)
		}
	}
	return out, nil
}

func (f *fakeMultipart) Inventory(ctx context.Context) ([]storage.StaleUpload, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.inventoryCalls++
	if f.inventoryErr != nil {
		if err := f.inventoryErr(f.inventoryCalls); err != nil {
			return nil, err
		}
	}
	return append([]storage.StaleUpload(nil), f.inventory...), nil
}

func (f *fakeMultipart) stats() (begins, aborts, completes int, calls map[int32]int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	counts := map[int32]int{}
	for number, n := range f.calls {
		counts[number] = n
	}
	return f.begins, f.aborts, f.completes, counts
}

func (f *fakeMultipart) flightStats() (maxInFlight int, abortInFlight []int, journal []string, completeOrder []int32) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.maxInFlight, append([]int(nil), f.abortInFlight...),
		append([]string(nil), f.journal...), append([]int32(nil), f.completeOrder...)
}

func (f *fakeMultipart) sweepStats() (lists int, aborted []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.staleLists, append([]string(nil), f.abortedKeys...)
}

func (f *fakeMultipart) sessionCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sessions
}

// multipartFake is the multipart fake with scriptable failures on the three calls
// the transfer's failure paths run through.
type multipartFake struct {
	*fakeMultipart
	beginErr      error
	beginPartSize *int64
	abortErr      error
	partNum       func(int32) int32
}

func (z *multipartFake) BeginMultipart(ctx context.Context, req storage.PutRequest) (storage.MultipartSession, error) {
	if z.beginHook != nil {
		z.beginHook(req)
	}
	if z.beginErr != nil {
		return storage.MultipartSession{}, z.beginErr
	}
	if z.beginPartSize != nil {
		return storage.MultipartSession{Key: req.Key, UploadID: "zz", PartSize: *z.beginPartSize}, nil
	}
	return z.fakeMultipart.BeginMultipart(ctx, req)
}

func (z *multipartFake) PutPart(ctx context.Context, session storage.MultipartSession, part storage.Part) (storage.PartResult, error) {
	res, err := z.fakeMultipart.PutPart(ctx, session, part)
	if err != nil {
		return res, err
	}
	if z.partNum != nil {
		res.Number = z.partNum(part.Number)
	}
	return res, nil
}

func (z *multipartFake) AbortMultipart(ctx context.Context, session storage.MultipartSession) error {
	if z.abortErr != nil {
		return z.abortErr
	}
	return z.fakeMultipart.AbortMultipart(ctx, session)
}

// testLog discards events: the transfer's log lines are human text, not the
// subject of these tests.
func testLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// destination builds one guarded destination around a fake storage instance.
func destination(t *testing.T, name string, s storage.Storage) guard.Destination {
	t.Helper()
	reg := instances.NewRegistry()
	err := storage.Register(reg, storage.FactorySpec[map[string]jsontext.Value]{
		Type: "fake", Example: []byte(`{}`),
		New: func(context.Context, map[string]jsontext.Value, instances.Deps, storage.BuildInput) (storage.Storage, error) {
			return s, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	dest, err := guard.Build(context.Background(), reg, "fake", jsontext.Value(`{}`),
		storage.BuildInput{CallParallelism: 1}, instances.Deps{Instance: name, Log: testLog()})
	if err != nil {
		t.Fatal(err)
	}
	return dest
}

// handle opens the write handle of one destination under the authority the
// protection ledger issues — the only production issuance path — from verified
// state: a constructed destination and a completed survey. The test's write
// window is live for an hour.
func handle(t *testing.T, d guard.Destination) guard.Handle {
	t.Helper()
	ledger := protection.NewLedger(protection.LedgerOptions{Destinations: []guard.Destination{d}})
	ledger.Start(map[string]inventory.Survey{d.Name(): {Storage: d.Name(), OK: true}})
	h, err := d.Port().Open(ledger.Authorize(time.Now().Add(time.Hour)))
	if err != nil {
		t.Fatal(err)
	}
	return h
}

// testRetry is the transfer's retry policy in tests: three attempts, tiny waits.
func testRetry() execution.Options {
	return execution.Options{Attempts: 3, Initial: time.Millisecond, Max: 4 * time.Millisecond, Unclassified: pluginerrors.ClassTransient}
}

// newTransfer binds a transfer with the test defaults a case does not override.
func newTransfer(o Options) *Transfer {
	if o.Log == nil {
		o.Log = testLog()
	}
	if o.Clock == nil {
		o.Clock = execution.NewClock(time.Minute)
	}
	if o.Retry.Attempts == 0 {
		o.Retry = testRetry()
	}
	if o.Storage == "" {
		o.Storage = "store"
	}
	return New(o)
}

func newSpool(t *testing.T) *spool.Spool {
	t.Helper()
	sp, err := spool.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return sp
}

// spoolFile writes one whole artifact into the spool and returns its name.
func spoolFile(t *testing.T, sp *spool.Spool, data []byte) string {
	t.Helper()
	w, err := sp.Create("artifact")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return w.Name()
}

// upload runs one finished-file transfer and returns its outcome.
func upload(t *testing.T, tr *Transfer, h guard.Handle, sp *spool.Spool, data []byte) (Outcome, error) {
	t.Helper()
	file := spoolFile(t, sp, data)
	return tr.Upload(context.Background(), h, sp, Request{
		Key: "runs/r/artifact.bin", Label: "g/n", File: file, Size: int64(len(data)),
	})
}

// TestUploadResendsOnlyTheFailedPart: a part that failed once is sent again, and
// the parts the service already accepted are not — the whole point of driving
// one part per call.
func TestUploadResendsOnlyTheFailedPart(t *testing.T) {
	parts := newFakeMultipart(4)
	parts.partErr = func(number int32, call int) error {
		if number == 2 && call == 1 {
			return pluginerrors.Transient(errors.New("injected 500 on part 2"))
		}
		return nil
	}
	h := handle(t, destination(t, "a_parts", parts))
	data := []byte("0123456789")
	out, err := upload(t, newTransfer(Options{}), h, newSpool(t), data)
	if err != nil {
		t.Fatal(err)
	}
	if !out.Handled || out.Parts != 3 {
		t.Fatalf("outcome %+v, want a handled three-part upload", out)
	}
	begins, aborts, completes, calls := parts.stats()
	if begins < 1 || completes != 1 || aborts != 0 {
		t.Fatalf("begins=%d completes=%d aborts=%d, want at least one begin, one complete, no abort", begins, completes, aborts)
	}
	if calls[1] != 1 || calls[2] != 2 || calls[3] != 1 {
		t.Fatalf("part calls %v, want part 2 sent twice and only part 2", calls)
	}
	if got := parts.sessionCount(); got != 1 {
		t.Fatalf("sessions = %d, want the one upload the retry of part 2 reused", got)
	}
	if got, ok := parts.Bytes("runs/r/artifact.bin"); !ok || !bytes.Equal(got, data) {
		t.Fatalf("the committed object is %q", got)
	}
}

// TestUploadPartBudgetSpentAborts: a part that never succeeds spends the retry
// budget, the upload is aborted exactly once — no orphaned parts — and the
// failure names the part.
func TestUploadPartBudgetSpentAborts(t *testing.T) {
	parts := newFakeMultipart(4)
	parts.partErr = func(number int32, _ int) error {
		if number == 2 {
			return pluginerrors.Transient(errors.New("injected 500 on every part 2"))
		}
		return nil
	}
	h := handle(t, destination(t, "a_parts", parts))
	out, err := upload(t, newTransfer(Options{}), h, newSpool(t), []byte("0123456789"))
	if err == nil || !strings.Contains(err.Error(), "part 2") {
		t.Fatalf("upload = %v, want the part's own failure", err)
	}
	if !out.Handled {
		t.Fatalf("outcome %+v, want the upload handled", out)
	}
	_, aborts, completes, calls := parts.stats()
	if aborts != 1 || completes != 0 {
		t.Fatalf("aborts=%d completes=%d, want exactly one abort and no complete", aborts, completes)
	}
	if calls[2] != 3 || calls[3] != 0 {
		t.Fatalf("part calls %v, want part 2 three times and part 3 never", calls)
	}
	if _, ok := parts.Bytes("runs/r/artifact.bin"); ok {
		t.Fatalf("the failed upload left an object: %v", parts.Keys())
	}
}

// TestUploadAbortOnCancellation: the run ends mid-upload, and the unfinished
// upload is still handed to the abort — the moment an abort is needed is the
// moment the context it runs under is already dead.
func TestUploadAbortOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	parts := newFakeMultipart(4)
	parts.partDelay = func(_ int32, ctx context.Context) error {
		cancel()
		<-ctx.Done()
		return ctx.Err()
	}
	h := handle(t, destination(t, "a_parts", parts))
	sp := newSpool(t)
	file := spoolFile(t, sp, []byte("0123456789"))
	_, err := newTransfer(Options{}).Upload(ctx, h, sp, Request{Key: "k", Label: "g/n", File: file, Size: 10})
	if err == nil {
		t.Fatal("a cancelled upload reported success")
	}
	if _, aborts, _, _ := parts.stats(); aborts != 1 {
		t.Fatalf("aborts = %d, want exactly 1 after a cancelled upload", aborts)
	}
}

// TestUploadSmallObjectKeepsTheSinglePut: the capability answers
// errors.ErrUnsupported without a request, so the object is left to one Put.
func TestUploadSmallObjectKeepsTheSinglePut(t *testing.T) {
	parts := newFakeMultipart(1 << 20)
	h := handle(t, destination(t, "a_parts", parts))
	out, err := upload(t, newTransfer(Options{}), h, newSpool(t), []byte("tiny"))
	if err != nil {
		t.Fatal(err)
	}
	if out.Handled {
		t.Fatalf("outcome %+v, want the whole-object path", out)
	}
	begins, aborts, completes, calls := parts.stats()
	if begins != 1 || aborts != 0 || completes != 0 || len(calls) != 0 {
		t.Fatalf("an object that fits one request used the multipart path: begins=%d aborts=%d completes=%d calls=%v",
			begins, aborts, completes, calls)
	}
}

// TestUploadStorageBudgetEndsMidUpload: a storage whose time budget runs out in
// the middle of an upload aborts it.
func TestUploadStorageBudgetEndsMidUpload(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		parts := newFakeMultipart(4)
		parts.partDelay = func(_ int32, ctx context.Context) error {
			select {
			case <-time.After(40 * time.Millisecond):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		h := handle(t, destination(t, "a_parts", parts))
		tr := newTransfer(Options{Clock: execution.NewClock(60 * time.Millisecond)})
		_, err := upload(t, tr, h, newSpool(t), []byte("0123456789"))
		if err == nil {
			t.Fatal("an exhausted budget reported success")
		}
		_, aborts, completes, _ := parts.stats()
		if aborts != 1 || completes != 0 {
			t.Fatalf("aborts=%d completes=%d, want one abort and no complete", aborts, completes)
		}
	})
}

// TestUploadCompletionThatCommitsNothingFails: a Complete that answers success
// and stores nothing is caught by the Stat after it.
func TestUploadCompletionThatCommitsNothingFails(t *testing.T) {
	parts := newFakeMultipart(4)
	parts.completeDrop = true
	h := handle(t, destination(t, "a_parts", parts))
	_, err := upload(t, newTransfer(Options{}), h, newSpool(t), []byte("0123456789"))
	if err == nil {
		t.Fatal("a completion that committed nothing verified")
	}
	_, aborts, completes, _ := parts.stats()
	if completes != 1 || aborts != 1 {
		t.Fatalf("completes=%d aborts=%d, want one complete and one abort", completes, aborts)
	}
	if _, ok := parts.Bytes("runs/r/artifact.bin"); ok {
		t.Fatalf("the dropped completion left an object: %v", parts.Keys())
	}
}

// TestUploadCommitWithLostAnswerIsVerified: the service committed the upload and
// the answer to the commit was lost; the Stat decides, and the copy is good.
func TestUploadCommitWithLostAnswerIsVerified(t *testing.T) {
	parts := newFakeMultipart(4)
	parts.completeLost = true
	h := handle(t, destination(t, "a_parts", parts))
	data := []byte("0123456789")
	if _, err := upload(t, newTransfer(Options{}), h, newSpool(t), data); err != nil {
		t.Fatal(err)
	}
	_, aborts, completes, _ := parts.stats()
	if completes != 1 || aborts != 0 {
		t.Fatalf("completes=%d aborts=%d, want one complete and no abort", completes, aborts)
	}
	if got, ok := parts.Bytes("runs/r/artifact.bin"); !ok || !bytes.Equal(got, data) {
		t.Fatalf("the committed object is %q, present=%v", got, ok)
	}
}

// TestUploadCommitGoneWithoutObjectFails: the same not_found answer with nothing
// committed is a failure — the rescue path is a proof, never a benefit of the
// doubt.
func TestUploadCommitGoneWithoutObjectFails(t *testing.T) {
	parts := newFakeMultipart(4)
	parts.completeGone = true
	h := handle(t, destination(t, "a_parts", parts))
	if _, err := upload(t, newTransfer(Options{}), h, newSpool(t), []byte("0123456789")); err == nil {
		t.Fatal("a completion with no object verified")
	}
	if _, aborts, _, _ := parts.stats(); aborts != 1 {
		t.Fatalf("aborts = %d, want exactly 1", aborts)
	}
}

// TestUploadBeginError: a Begin that fails with a real error fails the copy and
// aborts nothing — no upload exists yet.
func TestUploadBeginError(t *testing.T) {
	z := &multipartFake{fakeMultipart: newFakeMultipart(4), beginErr: pluginerrors.Permanent(errors.New("begin refused"))}
	h := handle(t, destination(t, "a_parts", z))
	_, err := upload(t, newTransfer(Options{}), h, newSpool(t), []byte("0123456789"))
	if err == nil || !strings.Contains(err.Error(), "begin refused") {
		t.Fatalf("upload = %v, want the begin failure", err)
	}
	if _, aborts, _, _ := z.stats(); aborts != 0 {
		t.Fatalf("aborts = %d, want none: no upload existed", aborts)
	}
}

// TestUploadZeroPartSize: a plugin that answers a part size of zero has broken
// its contract; the upload is refused as permanent.
func TestUploadZeroPartSize(t *testing.T) {
	zero := int64(0)
	z := &multipartFake{fakeMultipart: newFakeMultipart(4), beginPartSize: &zero}
	h := handle(t, destination(t, "a_parts", z))
	_, err := upload(t, newTransfer(Options{}), h, newSpool(t), []byte("0123456789"))
	if err == nil || !strings.Contains(err.Error(), "part size of 0") {
		t.Fatalf("upload = %v, want the zero-part-size refusal", err)
	}
}

// TestUploadSpoolOpenFails: a part whose spool file cannot be opened is a
// permanent failure of the upload — no retry recovers the bytes.
func TestUploadSpoolOpenFails(t *testing.T) {
	parts := newFakeMultipart(4)
	h := handle(t, destination(t, "a_parts", parts))
	_, err := newTransfer(Options{}).Upload(context.Background(), h, newSpool(t),
		Request{Key: "k", Label: "g/n", File: "missing.bin", Size: 10})
	if err == nil {
		t.Fatal("an upload opened a spool file that does not exist")
	}
	if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassPermanent {
		t.Fatalf("class = %q, want permanent", class)
	}
}

// TestUploadWrongPartNumber: a plugin that answers the wrong part number has
// broken its contract; the upload fails rather than committing a scrambled
// object.
func TestUploadWrongPartNumber(t *testing.T) {
	z := &multipartFake{fakeMultipart: newFakeMultipart(4), partNum: func(n int32) int32 { return n + 1 }}
	h := handle(t, destination(t, "a_parts", z))
	_, err := upload(t, newTransfer(Options{}), h, newSpool(t), []byte("0123456789"))
	if err == nil || !strings.Contains(err.Error(), "answered part") {
		t.Fatalf("upload = %v, want the wrong-part-number refusal", err)
	}
}

// TestAbortFailureIsReturned: a failed abort is reported and returned, so the
// original failure is joined with it rather than hidden.
func TestAbortFailureIsReturned(t *testing.T) {
	z := &multipartFake{fakeMultipart: newFakeMultipart(4), abortErr: errors.New("abort failed")}
	h := handle(t, destination(t, "a_parts", z))
	u, ok := h.Uploader()
	if !ok {
		t.Fatal("the fake has no multipart capability")
	}
	err := newTransfer(Options{}).abort(context.Background(), u,
		storage.MultipartSession{Key: "k", UploadID: "u"}, "k")
	if err == nil || !strings.Contains(err.Error(), "abort failed") {
		t.Fatalf("abort = %v, want the abort failure", err)
	}
}

// TestSweepAbortsOnlyStaleUploads: the upload an older-than-a-run killed process
// left is aborted; an upload this run's own wall clock could still be writing is
// left alone.
func TestSweepAbortsOnlyStaleUploads(t *testing.T) {
	parts := newFakeMultipart(4)
	parts.stale = []storage.StaleUpload{
		{Key: "runs/oldrun/databases/ns/db.dump.zst", UploadID: "upload-old", Initiated: time.Now().Add(-3 * time.Hour)},
		{Key: "runs/live/databases/ns/db.dump.zst", UploadID: "upload-fresh", Initiated: time.Now()},
	}
	h := handle(t, destination(t, "a_parts", parts))
	attempts, unknown := newTransfer(Options{}).Sweep(context.Background(), h, time.Now().Add(-time.Hour))
	if attempts != 3 || unknown {
		t.Fatalf("sweep = %d attempts, unknown=%v; want the inventory, the listing and one abort, and a known occupancy", attempts, unknown)
	}
	lists, aborted := parts.sweepStats()
	if lists != 1 {
		t.Fatalf("the sweep listed %d times, want once", lists)
	}
	if len(aborted) != 1 || aborted[0] != "runs/oldrun/databases/ns/db.dump.zst" {
		t.Fatalf("aborted %v, want only the upload the killed run left behind", aborted)
	}
}

// TestSweepListFailureIsUnknown: a sweep that cannot even list leaves bytes
// nobody can count and aborts nothing.
func TestSweepListFailureIsUnknown(t *testing.T) {
	parts := newFakeMultipart(4)
	parts.staleErr = errors.New("injected listing failure")
	h := handle(t, destination(t, "a_parts", parts))
	_, unknown := newTransfer(Options{}).Sweep(context.Background(), h, time.Now())
	if !unknown {
		t.Fatal("a failed listing did not report unknown occupancy")
	}
	if _, aborted := parts.sweepStats(); len(aborted) != 0 {
		t.Fatalf("a failed listing aborted %v", aborted)
	}
}

// TestSweepAbortFailureIsUnknown: a stale upload the sweep cannot abort leaves
// bytes nobody can count, so the sweep reports unknown.
func TestSweepAbortFailureIsUnknown(t *testing.T) {
	z := &multipartFake{fakeMultipart: newFakeMultipart(4), abortErr: pluginerrors.Transient(errors.New("abort boom"))}
	z.stale = []storage.StaleUpload{
		{Key: "runs/old/x.bin", UploadID: "u1", Initiated: time.Now().Add(-time.Hour)},
	}
	h := handle(t, destination(t, "a_parts", z))
	if _, unknown := newTransfer(Options{}).Sweep(context.Background(), h, time.Now()); !unknown {
		t.Fatal("a sweep whose abort failed did not report unknown occupancy")
	}
}

// TestSweepWithoutCapability: a whole-object destination has nothing to sweep.
func TestSweepWithoutCapability(t *testing.T) {
	plain := storagetest.NewFake()
	h := handle(t, destination(t, "plain", plain))
	attempts, unknown := newTransfer(Options{}).Sweep(context.Background(), h, time.Now())
	if attempts != 0 || unknown {
		t.Fatalf("sweep = %d attempts, unknown=%v, want a no-op", attempts, unknown)
	}
	if got := plain.CallCount(storagetest.OpList, ""); got != 0 {
		t.Fatalf("a plain destination was listed %d times", got)
	}
}

// TestAbortOwnAbortsOnlyItsOwnUploads: the shutdown abort reaches the upload this
// transfer opened and nothing else.
func TestAbortOwnAbortsOnlyItsOwnUploads(t *testing.T) {
	parts := newFakeMultipart(4)
	h := handle(t, destination(t, "a_parts", parts))
	tr := newTransfer(Options{})
	u, _ := h.Uploader()
	session, _, err := tr.begin(context.Background(), u, storage.PutRequest{Key: "k", Size: 10}, "g/n")
	if err != nil {
		t.Fatal(err)
	}
	foreign := storage.MultipartSession{Key: "other", UploadID: "foreign"}
	if _, err := u.BeginMultipart(context.Background(), storage.PutRequest{Key: "other", Size: 10}); err != nil {
		t.Fatal(err)
	}
	if err := tr.AbortOwn(context.Background()); err != nil {
		t.Fatal(err)
	}
	parts.mu.Lock()
	abortedKeys := append([]string(nil), parts.abortedKeys...)
	parts.mu.Unlock()
	if len(abortedKeys) != 1 || abortedKeys[0] != session.Key {
		t.Fatalf("aborted %v, want only this transfer's upload", abortedKeys)
	}
	if err := u.AbortMultipart(context.Background(), foreign); err != nil {
		t.Fatal(err)
	}
}

// TestAbortOwnStopsAtTheCleanupDeadline: every abort step shares one deadline; a
// spent deadline reports itself and stops.
func TestAbortOwnStopsAtTheCleanupDeadline(t *testing.T) {
	parts := newFakeMultipart(4)
	h := handle(t, destination(t, "a_parts", parts))
	tr := newTransfer(Options{})
	u, _ := h.Uploader()
	if _, _, err := tr.begin(context.Background(), u, storage.PutRequest{Key: "k", Size: 10}, "g/n"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := tr.AbortOwn(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("AbortOwn = %v, want the spent deadline", err)
	}
	if _, aborts, _, _ := parts.stats(); aborts != 0 {
		t.Fatalf("aborts = %d, want none after the deadline", aborts)
	}
}

// TestUploadPartsParallelWithinTheLimit: with parallelism 2, two of four parts
// are in flight at once and each part is sent exactly once.
func TestUploadPartsParallelWithinTheLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		parts := newFakeMultipart(10)
		parts.partDelay = func(_ int32, ctx context.Context) error {
			select {
			case <-time.After(10 * time.Millisecond):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		h := handle(t, destination(t, "a_parts", parts))
		body := bytes.Repeat([]byte("0123456789"), 4)
		if _, err := upload(t, newTransfer(Options{CallParallelism: 2}), h, newSpool(t), body); err != nil {
			t.Fatal(err)
		}
		_, aborts, completes, calls := parts.stats()
		if aborts != 0 || completes != 1 {
			t.Fatalf("aborts=%d completes=%d, want a clean commit", aborts, completes)
		}
		for number := int32(1); number <= 4; number++ {
			if calls[number] != 1 {
				t.Fatalf("part calls %v, want every part sent exactly once", calls)
			}
		}
		if maxInFlight, _, _, _ := parts.flightStats(); maxInFlight != 2 {
			t.Fatalf("the upload held %d parts in flight, want the configured 2", maxInFlight)
		}
		if got, ok := parts.Bytes("runs/r/artifact.bin"); !ok || !bytes.Equal(got, body) {
			t.Fatalf("the committed object is %q", got)
		}
	})
}

// TestUploadCommitsPartsInNumberOrder: parts may finish in any order — here the
// first is the slowest — and Complete is still handed the list by number.
func TestUploadCommitsPartsInNumberOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		parts := newFakeMultipart(10)
		parts.partDelay = func(number int32, ctx context.Context) error {
			select {
			case <-time.After(time.Duration(5-number) * 10 * time.Millisecond):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		h := handle(t, destination(t, "a_parts", parts))
		body := bytes.Repeat([]byte("0123456789"), 4)
		if _, err := upload(t, newTransfer(Options{CallParallelism: 4}), h, newSpool(t), body); err != nil {
			t.Fatal(err)
		}
		_, _, _, order := parts.flightStats()
		if len(order) != 4 || order[0] != 1 || order[1] != 2 || order[2] != 3 || order[3] != 4 {
			t.Fatalf("Complete was handed %v, want the parts by number", order)
		}
	})
}

// TestUploadPartsShareOneBudgetInterval: two parts run at once for forty
// milliseconds, then the third runs for forty: the storage spends eighty
// milliseconds of wall time, not the hundred and twenty of their sum.
func TestUploadPartsShareOneBudgetInterval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		parts := newFakeMultipart(10)
		parts.partDelay = func(_ int32, ctx context.Context) error {
			select {
			case <-time.After(40 * time.Millisecond):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		h := handle(t, destination(t, "a_parts", parts))
		clock := execution.NewClock(100 * time.Millisecond)
		body := []byte("012345678901234567890123456789")
		if _, err := upload(t, newTransfer(Options{Clock: clock, CallParallelism: 2}), h, newSpool(t), body); err != nil {
			t.Fatal(err)
		}
		if got := clock.Total(); got != 80*time.Millisecond {
			t.Fatalf("the storage duration = %s, want the 80ms union of the two overlapping parts and the third", got)
		}
	})
}

// TestUploadPermanentPartFailureStopsNewParts: a permanent failure of part 2
// while part 1 is in flight sends no further part, aborts once after the
// in-flight part returned, and reports the part's own refusal.
func TestUploadPermanentPartFailureStopsNewParts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		parts := newFakeMultipart(4)
		parts.partDelay = func(number int32, ctx context.Context) error {
			if number == 1 {
				<-ctx.Done()
				return ctx.Err()
			}
			return nil
		}
		parts.partErr = func(number int32, _ int) error {
			if number == 2 {
				return pluginerrors.Permanent(errors.New("part 2 refused"))
			}
			return nil
		}
		h := handle(t, destination(t, "a_parts", parts))
		_, err := upload(t, newTransfer(Options{CallParallelism: 2}), h, newSpool(t), []byte("0123456789abcd"))
		if err == nil || !strings.Contains(err.Error(), "part 2 refused") {
			t.Fatalf("upload = %v, want the part's refusal", err)
		}
		if strings.Contains(err.Error(), "context canceled") {
			t.Fatalf("upload = %v: the neighbour's cancellation hid the cause", err)
		}
		_, aborts, completes, calls := parts.stats()
		if aborts != 1 || completes != 0 || calls[3] != 0 {
			t.Fatalf("aborts=%d completes=%d calls=%v, want one abort, no complete and no part 3", aborts, completes, calls)
		}
		if _, abortInFlight, _, _ := parts.flightStats(); len(abortInFlight) != 1 || abortInFlight[0] != 0 {
			t.Fatalf("the abort ran with %v parts in flight, want none", abortInFlight)
		}
	})
}

// TestUploadPartsCancelledOpensNoPart: an upload whose context is already
// cancelled sends nothing.
func TestUploadPartsCancelledOpensNoPart(t *testing.T) {
	sp := newSpool(t)
	file := spoolFile(t, sp, []byte("0123456789"))
	parts := newFakeMultipart(4)
	h := handle(t, destination(t, "a_parts", parts))
	u, _ := h.Uploader()
	session, err := parts.BeginMultipart(context.Background(), storage.PutRequest{Key: "k", Size: 10})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err = newTransfer(Options{}).uploadParts(ctx, u, sp, &finishedSource{name: file, size: 10}, session, "g/n")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("uploadParts = %v, want the cancelled context", err)
	}
	if _, _, journal, _ := parts.flightStats(); len(journal) != 1 || journal[0] != "begin" {
		t.Fatalf("a cancelled upload sent a part: %v", journal)
	}
}

// TestSendPartRefusesACancelledContext: the context is checked immediately
// before the call, so a cancelled upload opens no spool file and reaches no
// plugin.
func TestSendPartRefusesACancelledContext(t *testing.T) {
	sp := newSpool(t)
	parts := newFakeMultipart(4)
	h := handle(t, destination(t, "a_parts", parts))
	u, _ := h.Uploader()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, attempts, err := newTransfer(Options{}).sendPart(ctx, u, sp, "unused", storage.MultipartSession{Key: "k", UploadID: "u", PartSize: 4}, 1, 0, 4, "g/n")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("sendPart = %v, want the cancelled context", err)
	}
	if attempts != 0 {
		t.Fatalf("a cancelled part booked %d attempts", attempts)
	}
	if _, _, journal, _ := parts.flightStats(); len(journal) != 0 {
		t.Fatalf("a cancelled part reached the destination: %v", journal)
	}
}

// TestUploadPartsSpoolFailureStopsUpload: a part that finds its spool file gone
// with no producer failure behind it ends the upload at once with the ENOENT as
// the cause.
func TestUploadPartsSpoolFailureStopsUpload(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sp := newSpool(t)
		file := spoolFile(t, sp, []byte("01234"))
		if err := sp.Remove(file); err != nil {
			t.Fatal(err)
		}
		parts := newFakeMultipart(4)
		h := handle(t, destination(t, "a_parts", parts))
		u, _ := h.Uploader()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		start := time.Now()
		_, _, err := newTransfer(Options{}).uploadParts(ctx, u, sp, &finishedSource{name: file, size: 5},
			storage.MultipartSession{Key: "k", UploadID: "u", PartSize: 4}, "g/n")
		if elapsed := time.Since(start); elapsed != 0 || !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("spool failure waited %s: got=%v want immediate ENOENT", elapsed, err)
		}
		origin, _ := terminalOf(err)
		if origin != originPart {
			t.Fatalf("cause origin = %d, want the part's own failure", origin)
		}
	})
}

// TestTerminalCauseCrossProduct: every ordered pair of the events that can end an
// upload is recorded when it happens, the ONE decision point picks a part
// failure first, then the producer's failure, then the cancellation, and
// recording a cause stops the upload at that same site.
func TestTerminalCauseCrossProduct(t *testing.T) {
	type event int
	const (
		eventPart event = iota + 1
		eventSource
		eventSpool
		eventParent
	)
	names := map[event]string{eventPart: "part", eventSource: "source", eventSpool: "spool", eventParent: "parent"}
	partFailure := pluginerrors.Permanent(errors.New("part refused"))
	sourceFailure := errors.New("producer failed")
	vanished := pluginerrors.Permanent(fmt.Errorf("%w: %w", errVanishedPart, os.ErrNotExist))

	var orderings [][]event
	for _, e := range []event{eventPart, eventSource, eventSpool, eventParent} {
		orderings = append(orderings, []event{e})
	}
	for _, a := range []event{eventPart, eventSource, eventSpool, eventParent} {
		for _, b := range []event{eventPart, eventSource, eventSpool, eventParent} {
			if a != b {
				orderings = append(orderings, []event{a, b})
			}
		}
	}
	for _, ordering := range orderings {
		name := make([]string, 0, len(ordering))
		for _, e := range ordering {
			name = append(name, names[e])
		}
		t.Run(strings.Join(name, "->"), func(t *testing.T) {
			parentCtx, cancelParent := context.WithCancel(context.Background())
			defer cancelParent()
			pctx, stop := context.WithCancelCause(context.Background())
			defer stop(nil)
			term := &uploadTerminal{stop: stop}
			seenPart, seenSource := false, false
			var wantOrigin terminalOrigin
			var wantErr error
			for _, e := range ordering {
				switch e {
				case eventPart:
					term.recordPart(parentCtx, partFailure)
					if !seenPart {
						seenPart, wantOrigin, wantErr = true, originPart, partFailure
					}
				case eventSpool:
					term.recordPart(parentCtx, vanished)
					if !seenPart && !seenSource {
						seenPart, wantOrigin, wantErr = true, originPart, vanished
					}
				case eventSource:
					term.recordNext(parentCtx, sourceFailure)
					if !seenPart && !seenSource {
						seenSource, wantOrigin, wantErr = true, originSource, sourceFailure
					}
				case eventParent:
					cancelParent()
					term.recordParent(parentCtx.Err())
					if !seenPart && !seenSource {
						wantOrigin, wantErr = originParent, parentCtx.Err()
					}
				}
			}
			cause := term.cause()
			if cause == nil {
				t.Fatalf("no cause chosen for %v", ordering)
			}
			if cause.origin != wantOrigin || !errors.Is(cause.err, wantErr) {
				t.Fatalf("cause = %+v, want origin %v and %v", cause, wantOrigin, wantErr)
			}
			gotStop := context.Cause(pctx)
			if seenPart || seenSource {
				if gotStop == nil {
					t.Fatal("recording an own cause did not stop the upload")
				}
			} else if gotStop != nil {
				t.Fatalf("the run's cancellation stopped the upload with %v", gotStop)
			}
		})
	}
}

// failedSource is a source whose producer already failed: what a part finds when
// it asks whether a vanished spool file is the failed attempt's cleanup.
type failedSource struct{ err error }

func (s *failedSource) next(context.Context, int64) (int64, bool, error) { return 0, false, s.err }
func (s *failedSource) file() string                                     { return "f" }
func (s *failedSource) terminal() (<-chan struct{}, error)               { return nil, s.err }

// TestPartErrorDependency: a part's context error is a consequence only when
// this upload stopped the call or the run ended it; a context error with no such
// fact behind it is the part's own failure, and so is any vanished file without
// a recorded producer failure.
func TestPartErrorDependency(t *testing.T) {
	newTerm := func() (*uploadTerminal, context.Context) {
		pctx, stop := context.WithCancelCause(context.Background())
		return &uploadTerminal{stop: stop}, pctx
	}
	t.Run("context error alone is the part's own", func(t *testing.T) {
		term, pctx := newTerm()
		term.recordPart(context.Background(), context.Canceled)
		if !errors.Is(context.Cause(pctx), context.Canceled) {
			t.Fatal("the part's own context error did not stop the upload")
		}
		if got := term.cause(); got == nil || got.origin != originPart {
			t.Fatalf("cause = %v, want the part's own failure", got)
		}
	})
	t.Run("context error after a source failure is dependent", func(t *testing.T) {
		term, pctx := newTerm()
		term.recordNext(context.Background(), errors.New("producer failed"))
		stopped := context.Cause(pctx)
		term.recordPart(context.Background(), context.DeadlineExceeded)
		if got := context.Cause(pctx); !errors.Is(got, stopped) {
			t.Fatal("a dependent part error replaced the recorded cause")
		}
		if got := term.cause(); got == nil || got.origin != originSource {
			t.Fatalf("cause = %v, want the producer's failure", got)
		}
	})
	t.Run("context error after the run ended is dependent", func(t *testing.T) {
		term, pctx := newTerm()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		term.recordParent(ctx.Err())
		term.recordPart(ctx, context.Canceled)
		if got := context.Cause(pctx); got != nil {
			t.Fatalf("a part error cancelled the upload under the run's cancellation: %v", got)
		}
		if got := term.cause(); got == nil || got.origin != originParent {
			t.Fatalf("cause = %v, want the run's cancellation", got)
		}
	})
	t.Run("a vanished file after a source failure is dependent", func(t *testing.T) {
		producerErr := errors.New("producer failed")
		term, pctx := newTerm()
		term.recordVanished(context.Background(), &failedSource{err: producerErr})
		stoppedCause := context.Cause(pctx)
		if stoppedCause == nil {
			t.Fatal("recording the producer's failure did not stop the upload")
		}
		term.recordPart(context.Background(), pluginerrors.Permanent(fmt.Errorf("%w: %w", errVanishedPart, os.ErrNotExist)))
		if got := context.Cause(pctx); !errors.Is(got, stoppedCause) {
			t.Fatal("the cleanup error replaced the producer's failure")
		}
		if got := term.cause(); got == nil || got.origin != originSource || !errors.Is(got.err, producerErr) {
			t.Fatalf("cause = %v, want the producer's failure", got)
		}
	})
	t.Run("a vanished file without a producer failure is the part's own", func(t *testing.T) {
		term, pctx := newTerm()
		term.recordVanished(context.Background(), &failedSource{})
		if got := context.Cause(pctx); got != nil {
			t.Fatalf("a healthy source stopped the upload: %v", got)
		}
		term.recordPart(context.Background(), pluginerrors.Permanent(fmt.Errorf("%w: %w", errVanishedPart, os.ErrNotExist)))
		if got := context.Cause(pctx); got == nil {
			t.Fatal("a vanished file with no producer failure did not stop the upload")
		}
		if got := term.cause(); got == nil || got.origin != originPart {
			t.Fatalf("cause = %v, want the part's own failure", got)
		}
	})
	t.Run("a recorded part failure keeps priority over a later source failure", func(t *testing.T) {
		partFailure := pluginerrors.Permanent(errors.New("part refused"))
		term, pctx := newTerm()
		term.recordPart(context.Background(), partFailure)
		stopped := context.Cause(pctx)
		term.recordSource(errors.New("producer failed"))
		if got := term.cause(); got == nil || got.origin != originPart || !errors.Is(got.err, partFailure) {
			t.Fatalf("cause = %v, want the recorded part failure", got)
		}
		if got := context.Cause(pctx); !errors.Is(got, stopped) {
			t.Fatal("recording the later source failure replaced the upload's cause")
		}
	})
	t.Run("the upload's own cause coming back from a part is dependent", func(t *testing.T) {
		term, pctx := newTerm()
		term.recordNext(context.Background(), errors.New("producer failed"))
		stopped := context.Cause(pctx)
		term.recordPart(context.Background(), stopped)
		if got := term.cause(); got == nil || got.origin != originSource {
			t.Fatalf("cause = %v, want the producer's failure", got)
		}
		if got := context.Cause(pctx); !errors.Is(got, stopped) {
			t.Fatal("the upload's own cause was recorded as a part failure")
		}
	})
}

// TestTerminalOfAForeignError: an error that did not come from uploadParts is
// reported as that upload's own part failure.
func TestTerminalOfAForeignError(t *testing.T) {
	foreign := errors.New("boom")
	origin, cause := terminalOf(foreign)
	if origin != originPart || !errors.Is(cause, foreign) {
		t.Fatalf("terminalOf = (%v, %v), want the error itself as a part failure", origin, cause)
	}
}

// TestUploadPartsSourceFailureDuringSlotWait: the producer failed and then the
// run was cancelled while the dispatcher waited for a slot — the producer's
// failure is still the cause, recorded at the single exit even though no part or
// source call reported it.
func TestUploadPartsSourceFailureDuringSlotWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sp := newSpool(t)
		file := spoolFile(t, sp, []byte("0123456789abcd"))
		started := make(chan struct{}, 3)
		release := make(chan struct{})
		parts := newFakeMultipart(4)
		parts.partDelay = func(_ int32, ctx context.Context) error {
			started <- struct{}{}
			<-release
			return ctx.Err()
		}
		h := handle(t, destination(t, "a_parts", parts))
		u, _ := h.Uploader()
		session, err := parts.BeginMultipart(context.Background(), storage.PutRequest{Key: "k", Size: 14})
		if err != nil {
			t.Fatal(err)
		}
		src := newGatedSource(file, 14)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan error, 1)
		go func() {
			_, _, perr := newTransfer(Options{CallParallelism: 2}).uploadParts(ctx, u, sp, src, session, "g/n")
			done <- perr
		}()
		<-started
		<-started
		synctest.Wait()
		failure := errors.New("producer failed before parent cancellation")
		src.fail(failure)
		cancel()
		synctest.Wait()
		close(release)
		if err := <-done; !errors.Is(err, failure) {
			t.Fatalf("source failure lost during slot wait: got=%v want=%v", err, failure)
		}
	})
}

// gatedSource reports a fixed size, then blocks until the test records the
// producer's failure; its terminal channel is what wakes a saturated dispatcher.
type gatedSource struct {
	name string
	size int64
	mu   sync.Mutex
	err  error
	done chan struct{}
}

func newGatedSource(file string, size int64) *gatedSource {
	return &gatedSource{name: file, size: size, done: make(chan struct{})}
}

func (s *gatedSource) fail(err error) {
	s.mu.Lock()
	if s.err == nil {
		s.err = err
		close(s.done)
	}
	s.mu.Unlock()
}

func (s *gatedSource) next(ctx context.Context, after int64) (int64, bool, error) {
	if after < s.size {
		return s.size, false, nil
	}
	select {
	case <-s.done:
		s.mu.Lock()
		defer s.mu.Unlock()
		return 0, false, s.err
	case <-ctx.Done():
		return 0, false, context.Cause(ctx)
	}
}

func (s *gatedSource) file() string { return s.name }

func (s *gatedSource) terminal() (<-chan struct{}, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.done, s.err
}

// TestUploadZeroSizeKeepsTheSinglePut: a zero-byte object has no parts; the
// finished-file path never reaches the destination.
func TestUploadZeroSizeKeepsTheSinglePut(t *testing.T) {
	parts := newFakeMultipart(4)
	h := handle(t, destination(t, "a_parts", parts))
	sp := newSpool(t)
	out, err := newTransfer(Options{}).Upload(context.Background(), h, sp,
		Request{Key: "k", Label: "g/n", File: "unused", Size: 0})
	if err != nil || out.Handled {
		t.Fatalf("upload = %+v, %v; want the whole-object path", out, err)
	}
	if begins, _, _, _ := parts.stats(); begins != 0 {
		t.Fatalf("begins = %d, want none for a zero-byte object", begins)
	}
}

// TestEventsWithoutALogger: a transfer with no logger still uploads; its events
// are simply not written.
func TestEventsWithoutALogger(t *testing.T) {
	parts := newFakeMultipart(4)
	h := handle(t, destination(t, "a_parts", parts))
	tr := New(Options{Clock: execution.NewClock(time.Minute), Retry: testRetry()})
	if _, err := upload(t, tr, h, newSpool(t), []byte("0123456789")); err != nil {
		t.Fatal(err)
	}
}

// TestAbortOwnJoinsAFailedAbort: the shutdown abort reports an abort the service
// refused and leaves the upload tracked for the next attempt.
func TestAbortOwnJoinsAFailedAbort(t *testing.T) {
	z := &multipartFake{fakeMultipart: newFakeMultipart(4), abortErr: errors.New("abort failed")}
	h := handle(t, destination(t, "a_parts", z))
	tr := newTransfer(Options{})
	u, _ := h.Uploader()
	if _, _, err := tr.begin(context.Background(), u, storage.PutRequest{Key: "k", Size: 10}, "g/n"); err != nil {
		t.Fatal(err)
	}
	if err := tr.AbortOwn(context.Background()); err == nil || !strings.Contains(err.Error(), "abort failed") {
		t.Fatalf("AbortOwn = %v, want the abort failure", err)
	}
	tr.mu.Lock()
	left := len(tr.own)
	tr.mu.Unlock()
	if left != 1 {
		t.Fatalf("tracked uploads = %d, want the failed abort kept for the next attempt", left)
	}
}

// wrongSizeStorage commits normally but reports a wrong size on Stat: the
// verification after the commit must reject it.
type wrongSizeStorage struct {
	*fakeMultipart
	size int64
}

func (w *wrongSizeStorage) Stat(ctx context.Context, key string) (model.ObjectInfo, error) {
	return model.ObjectInfo{Key: key, Size: w.size}, nil
}

// TestVerifyRejectsAWrongSize: a commit whose object is there at another size is
// not the copy the transfer sent.
func TestVerifyRejectsAWrongSize(t *testing.T) {
	w := &wrongSizeStorage{fakeMultipart: newFakeMultipart(4), size: 99}
	h := handle(t, destination(t, "a_parts", w))
	_, err := upload(t, newTransfer(Options{}), h, newSpool(t), []byte("0123456789"))
	if err == nil || !strings.Contains(err.Error(), "size after upload is 99") {
		t.Fatalf("upload = %v, want the size mismatch", err)
	}
	if _, aborts, _, _ := w.stats(); aborts != 1 {
		t.Fatalf("aborts = %d, want the failed verification aborted", aborts)
	}
}

// TestVerifyReportsAStatFailure: a Stat that fails with its own error is
// reported as itself, not turned into "not committed".
func TestVerifyReportsAStatFailure(t *testing.T) {
	parts := newFakeMultipart(4)
	parts.Fault = func(op storagetest.Op, _ string, _ int) error {
		if op == storagetest.OpStat {
			return pluginerrors.Transient(errors.New("stat boom"))
		}
		return nil
	}
	h := handle(t, destination(t, "a_parts", parts))
	_, err := upload(t, newTransfer(Options{}), h, newSpool(t), []byte("0123456789"))
	if err == nil || !strings.Contains(err.Error(), "stat boom") {
		t.Fatalf("upload = %v, want the Stat failure", err)
	}
}

// TestUploadSpoolOpenFailureIsTerminal: a spool-open failure that is not ENOENT
// is a terminal part failure too.
func TestUploadSpoolOpenFailureIsTerminal(t *testing.T) {
	parts := newFakeMultipart(4)
	h := handle(t, destination(t, "a_parts", parts))
	long := strings.Repeat("a", 300)
	_, err := newTransfer(Options{}).Upload(context.Background(), h, newSpool(t),
		Request{Key: "k", Label: "g/n", File: long, Size: 10})
	if err == nil {
		t.Fatal("a spool open failure reported success")
	}
	if errors.Is(err, errVanishedPart) {
		t.Fatalf("upload = %v, want a non-ENOENT open failure", err)
	}
	if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassPermanent {
		t.Fatalf("class = %q, want permanent", class)
	}
}

// TestProducerFailureKeepsItsMessage: the chosen producer failure answers with
// its own error and unwraps to it, which is what tells the caller to fall back.
func TestProducerFailureKeepsItsMessage(t *testing.T) {
	failure := errors.New("producer failed")
	term := &uploadTerminal{stop: func(error) {}}
	term.recordNext(context.Background(), failure)
	cause := term.cause()
	if cause == nil || cause.Error() != failure.Error() || !errors.Is(cause, failure) {
		t.Fatalf("cause = %v, want the producer's own error and identity", cause)
	}
}

// ctxFailSource answers the run's cancellation and reports the same context
// error as its terminal state: the run's fact, never the producer's.
type ctxFailSource struct{ name string }

func (s *ctxFailSource) next(ctx context.Context, _ int64) (int64, bool, error) {
	<-ctx.Done()
	return 0, false, context.Cause(ctx)
}

func (s *ctxFailSource) file() string { return s.name }

func (s *ctxFailSource) terminal() (<-chan struct{}, error) { return nil, context.Canceled }

// TestSourceContextErrorUnderParentCancellationIsTheParentCause: a producer
// whose terminal error is the run's own cancellation is recorded as the run's
// fact, once, and never as a producer failure.
func TestSourceContextErrorUnderParentCancellationIsTheParentCause(t *testing.T) {
	sp := newSpool(t)
	file := spoolFile(t, sp, []byte("0123456789"))
	parts := newFakeMultipart(4)
	h := handle(t, destination(t, "a_parts", parts))
	u, _ := h.Uploader()
	session, err := parts.BeginMultipart(context.Background(), storage.PutRequest{Key: "k", Size: 10})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err = newTransfer(Options{}).uploadParts(ctx, u, sp, &ctxFailSource{name: file}, session, "g/n")
	origin, _ := terminalOf(err)
	if origin != originParent {
		t.Fatalf("origin = %d, want the run's cancellation, err=%v", origin, err)
	}
}

// TestSweepForeignInventoryMakesOccupancyUnknown: uploads of any age, undated
// ones included, bill bytes no run listing shows; a non-empty inventory leaves
// the occupancy unknown even when nothing is old enough to abort.
func TestSweepForeignInventoryMakesOccupancyUnknown(t *testing.T) {
	parts := newFakeMultipart(4)
	parts.inventory = []storage.StaleUpload{
		{Key: "runs/live/x.bin", UploadID: "young", Initiated: time.Now()},
		{Key: "runs/undated/x.bin", UploadID: "undated"},
	}
	h := handle(t, destination(t, "a_parts", parts))
	attempts, unknown := newTransfer(Options{}).Sweep(context.Background(), h, time.Now().Add(-time.Hour))
	if !unknown {
		t.Fatal("a non-empty inventory did not report unknown occupancy")
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d, want the inventory and the stale listing", attempts)
	}
	if _, aborted := parts.sweepStats(); len(aborted) != 0 {
		t.Fatalf("aborted %v, want nothing: no upload is old enough", aborted)
	}
	if parts.inventoryCalls != 1 {
		t.Fatalf("inventory calls = %d, want one", parts.inventoryCalls)
	}
}

// TestSweepInventoryRetriesTransientFailures: the inventory runs under the
// transfer's retry policy; transient failures are spent and the last attempt
// decides.
func TestSweepInventoryRetriesTransientFailures(t *testing.T) {
	parts := newFakeMultipart(4)
	parts.inventoryErr = func(call int) error {
		if call <= 2 {
			return pluginerrors.Transient(errors.New("injected inventory 500"))
		}
		return nil
	}
	h := handle(t, destination(t, "a_parts", parts))
	attempts, unknown := newTransfer(Options{}).Sweep(context.Background(), h, time.Now())
	if unknown {
		t.Fatal("a transient inventory failure that recovered marked the occupancy unknown")
	}
	if parts.inventoryCalls != 3 || attempts != 4 {
		t.Fatalf("inventory calls=%d attempts=%d, want three calls and the stale listing", parts.inventoryCalls, attempts)
	}
}

// TestSweepInventoryExhaustionLeavesOccupancyUnknown: an inventory that never
// succeeds leaves the destination's bytes uncounted.
func TestSweepInventoryExhaustionLeavesOccupancyUnknown(t *testing.T) {
	parts := newFakeMultipart(4)
	parts.inventoryErr = func(int) error { return pluginerrors.Transient(errors.New("injected inventory 500")) }
	h := handle(t, destination(t, "a_parts", parts))
	attempts, unknown := newTransfer(Options{}).Sweep(context.Background(), h, time.Now())
	if !unknown {
		t.Fatal("an exhausted inventory did not report unknown occupancy")
	}
	if attempts != 4 || parts.inventoryCalls != 3 {
		t.Fatalf("attempts=%d inventory calls=%d, want the exhausted inventory and the stale listing", attempts, parts.inventoryCalls)
	}
}

// staleOnly has the stale reaping capability but no inventory: the guard answers
// errors.ErrUnsupported, which is an occupancy this run cannot count.
type staleOnly struct {
	*storagetest.Fake
	aborts int
}

func (s *staleOnly) BeginMultipart(context.Context, storage.PutRequest) (storage.MultipartSession, error) {
	return storage.MultipartSession{}, errors.ErrUnsupported
}

func (s *staleOnly) PutPart(context.Context, storage.MultipartSession, storage.Part) (storage.PartResult, error) {
	return storage.PartResult{}, errors.ErrUnsupported
}

func (s *staleOnly) CompleteMultipart(context.Context, storage.MultipartSession, []storage.PartResult) error {
	return errors.ErrUnsupported
}

func (s *staleOnly) AbortMultipart(context.Context, storage.MultipartSession) error {
	s.aborts++
	return nil
}

func (s *staleOnly) StaleMultipart(context.Context, time.Time) ([]storage.StaleUpload, error) {
	return []storage.StaleUpload{{Key: "runs/old/x.bin", UploadID: "u1", Initiated: time.Now().Add(-time.Hour)}}, nil
}

// TestSweepWithoutInventoryCapability: a destination that cannot list foreign
// uploads cannot bill invisible parts either, so it stays known; its stale
// uploads are still aborted.
func TestSweepWithoutInventoryCapability(t *testing.T) {
	s := &staleOnly{Fake: storagetest.NewFake()}
	h := handle(t, destination(t, "a_parts", s))
	tr := newTransfer(Options{Retry: execution.Options{Attempts: 1, Unclassified: pluginerrors.ClassPermanent}})
	attempts, unknown := tr.Sweep(context.Background(), h, time.Now())
	if unknown {
		t.Fatal("a destination without inventory reported its occupancy unknown")
	}
	if attempts != 2 || s.aborts != 1 {
		t.Fatalf("attempts=%d aborts=%d, want the stale listing and one abort", attempts, s.aborts)
	}
}

// TestR3_C2_AccountingCannotMint is the reviewer's C2 probe: the sealed
// accounting view a transfer receives must not let it recover an authority and
// write around the Floor's admission. The view's dynamic value is not the ledger
// and exposes no Authorize.
func TestR3_C2_AccountingCannotMint(t *testing.T) {
	d := destination(t, "primary", storagetest.NewFake())
	ledger := protection.NewLedger(protection.LedgerOptions{Destinations: []guard.Destination{d}})
	ledger.Start(map[string]inventory.Survey{"primary": {OK: true}})
	tr := newTransfer(Options{Accounting: ledger.Accounting()})
	issuer, ok := any(tr.o.Accounting).(interface {
		Authorize(time.Time) *guard.Authority
	})
	if !ok {
		return
	}
	h, err := d.Port().Open(issuer.Authorize(time.Now().Add(time.Hour)))
	if err != nil {
		return
	}
	_, err = h.Put(context.Background(), storage.PutRequest{Key: "unadmitted", Size: 1}, strings.NewReader("x"))
	if err == nil {
		t.Fatal("Accounting leaked a usable issuer: unadmitted write succeeded")
	}
}

// inventoryOnly can list every incomplete upload but cannot upload parts: the
// inventory is the only evidence of bytes a listing cannot see.
type inventoryOnly struct {
	*storagetest.Fake
	uploads []storage.StaleUpload
}

func (s *inventoryOnly) Inventory(context.Context) ([]storage.StaleUpload, error) {
	return s.uploads, nil
}

// TestSweepInventoryOnlyDestination: a destination without the upload capability
// still reports unknown occupancy when its inventory holds a foreign upload, and
// stays known when the inventory is empty.
func TestSweepInventoryOnlyDestination(t *testing.T) {
	for name, tc := range map[string]struct {
		uploads  []storage.StaleUpload
		unknown  bool
		attempts int
	}{
		"foreign upload": {uploads: []storage.StaleUpload{{Key: "runs/x/big.bin", UploadID: "u1"}}, unknown: true, attempts: 1},
		"empty":          {unknown: false, attempts: 1},
	} {
		t.Run(name, func(t *testing.T) {
			s := &inventoryOnly{Fake: storagetest.NewFake(), uploads: tc.uploads}
			h := handle(t, destination(t, "alpha", s))
			attempts, unknown := newTransfer(Options{}).Sweep(context.Background(), h, time.Now())
			if unknown != tc.unknown || attempts != tc.attempts {
				t.Fatalf("sweep = %d attempts, unknown=%v; want %d and %v", attempts, unknown, tc.attempts, tc.unknown)
			}
		})
	}
}
