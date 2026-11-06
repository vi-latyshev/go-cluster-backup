package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/spool"
	"github.com/vi-latyshev/go-cluster-backup/internal/storage"
	"github.com/vi-latyshev/go-cluster-backup/internal/storage/storagetest"
)

// fakeMultipart is a storagetest.Fake with the multipart capability: parts are
// counted per number — which is how the engine's per-part retry is measured —
// and assembled by Complete into the in-memory fake, so the engine's own Stat and
// the test's Bytes see exactly what was committed.
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
		return storage.PartResult{}, plugin.NotFound(errors.New("no such upload"))
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
		return plugin.NotFound(errors.New("no such upload"))
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
		return plugin.NotFound(errors.New("no such upload"))
	}
	if !f.completeDrop {
		f.PutObject(session.Key, body, time.Now())
	}
	if f.completeLost {
		return plugin.NotFound(errors.New("no such upload"))
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

func (f *fakeMultipart) stats() (begins, aborts, completes int, calls map[int32]int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	counts := map[int32]int{}
	for number, n := range f.calls {
		counts[number] = n
	}
	return f.begins, f.aborts, f.completes, counts
}

// flightStats is the parallel witness: the most parts in flight at once, the
// in-flight count witnessed at every Abort, the calls in order, and the part
// numbers Complete was handed, in the order it was handed them.
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

// sessionCount is how many uploads the fake actually opened.
func (f *fakeMultipart) sessionCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sessions
}

// twoWayStorages is one multipart storage beside one plain one: what proves a
// failure of the multipart copy never costs the other storage's copy.
func twoWayStorages() map[string]any {
	return map[string]any{
		"a_parts": storageInstance("fake", "keep_last"),
		"b_plain": storageInstance("fake", "keep_last"),
	}
}

// TestMultipartResendsOnlyTheFailedPart: a part that failed once is sent again,
// and the parts the service already accepted are not — the whole point of driving
// one part per call. Mutant: an engine that restarts the object at part 1 makes
// part 1 count 2.
func TestMultipartResendsOnlyTheFailedPart(t *testing.T) {
	w := newWorld(t)
	parts := newFakeMultipart(4)
	parts.partErr = func(number int32, call int) error {
		if number == 2 && call == 1 {
			return plugin.Transient(errors.New("injected 500 on part 2"))
		}
		return nil
	}
	w.extraStorages["a_parts"] = parts
	w.storages["a_parts"] = parts.Fake
	cfg := baseConfig(t, twoWayStorages())
	w.sources["vpn"] = &fakeSource{data: []byte("0123456789")} // three parts: 4, 4, 2

	rep, err := w.newEngine(t, cfg).Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Artifacts[0].Outcome != model.ArtifactOK {
		t.Fatalf("the artifact = %+v", rep.Artifacts[0])
	}
	begins, aborts, completes, calls := parts.stats()
	if begins < 1 || completes != 1 || aborts != 0 {
		t.Fatalf("begins=%d completes=%d aborts=%d, want at least one begin, one complete, no abort", begins, completes, aborts)
	}
	if calls[1] != 1 || calls[2] != 2 || calls[3] != 1 {
		t.Fatalf("part calls %v, want part 2 sent twice and only part 2", calls)
	}
	if got, ok := parts.Bytes(rep.Artifacts[0].Key); !ok || !bytes.Equal(got, []byte("0123456789")) {
		t.Fatalf("the committed object is %q", got)
	}
	if r := storageResult(rep, "a_parts"); r.Outcome != model.StorageVerified {
		t.Fatalf("a_parts = %+v", r)
	}
}

// TestMultipartPartBudgetSpentAbortsAndFailsOneStorage: a part that never
// succeeds spends the retry budget, the upload is aborted exactly once — no
// orphaned parts — and only that storage's copy of the artifact fails. Mutant:
// removing the abort leaves parts billed; removing the per-storage scoping
// (failing the run) loses the other storage's copy.
func TestMultipartPartBudgetSpentAbortsAndFailsOneStorage(t *testing.T) {
	w := newWorld(t)
	parts := newFakeMultipart(4)
	parts.partErr = func(number int32, _ int) error {
		if number == 2 {
			return plugin.Transient(errors.New("injected 500 on every part 2"))
		}
		return nil
	}
	w.extraStorages["a_parts"] = parts
	w.storages["a_parts"] = parts.Fake
	cfg := baseConfig(t, twoWayStorages())
	w.sources["vpn"] = &fakeSource{data: []byte("0123456789")}

	rep, err := w.newEngine(t, cfg).Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Artifacts[0].Outcome != model.ArtifactOK {
		t.Fatalf("the artifact = %+v, want the plain storage's copy to hold", rep.Artifacts[0])
	}
	_, aborts, completes, calls := parts.stats()
	if aborts != 1 {
		t.Fatalf("aborts = %d, want exactly 1", aborts)
	}
	if completes != 0 {
		t.Fatalf("completes = %d, want none: the upload never finished", completes)
	}
	// The world's retry policy allows three attempts: every one of them went to
	// the failing part, and the parts after it were never sent.
	if calls[2] != 3 || calls[3] != 0 {
		t.Fatalf("part calls %v, want part 2 three times and part 3 never", calls)
	}
	if r := storageResult(rep, "a_parts"); r.Outcome != model.StorageFailed || !strings.Contains(r.Error, "part 2") {
		t.Fatalf("a_parts = %+v, want a failure naming the part", r)
	}
	if r := storageResult(rep, "b_plain"); r.Outcome != model.StorageVerified {
		t.Fatalf("b_plain = %+v, want the other storage's copy intact", r)
	}
	if rep.Outcome != model.OutcomeDegraded {
		t.Fatalf("outcome %q, want degraded", rep.Outcome)
	}
	key := rep.Artifacts[0].Key
	if _, ok := parts.Bytes(key); ok {
		t.Fatalf("the failed upload left an object: %v", parts.Keys())
	}
	if _, ok := w.storage(t, "b_plain").Bytes(key); !ok {
		t.Fatalf("the plain storage has no copy: %v", w.storage(t, "b_plain").Keys())
	}
	if _, ok := w.storage(t, "b_plain").Bytes("runs/" + rep.RunID + "/manifest.json"); !ok {
		t.Fatalf("the manifest is missing on the storage that held the copy: %v", w.storage(t, "b_plain").Keys())
	}
}

// TestMultipartAbortOnCancellation: the run ends mid-upload, and the unfinished
// upload is still handed to the abort — the moment an abort is needed is the
// moment the context it runs under is already dead.
func TestMultipartAbortOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := newWorld(t)
	parts := newFakeMultipart(4)
	parts.partDelay = func(_ int32, ctx context.Context) error {
		cancel()
		<-ctx.Done()
		return ctx.Err()
	}
	w.extraStorages["a_parts"] = parts
	w.storages["a_parts"] = parts.Fake
	cfg := baseConfig(t, map[string]any{"a_parts": storageInstance("fake", "keep_last")})
	w.sources["vpn"] = &fakeSource{data: []byte("0123456789")}

	rep, err := w.newEngine(t, cfg).Run(ctx, RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	_, aborts, _, _ := parts.stats()
	if aborts != 1 {
		t.Fatalf("aborts = %d, want exactly 1 after a cancelled upload", aborts)
	}
	if rep.Artifacts[0].Outcome != model.ArtifactFailed {
		t.Fatalf("the artifact = %+v, want a failure", rep.Artifacts[0])
	}
	if r := storageResult(rep, "a_parts"); r.Outcome != model.StorageFailed {
		t.Fatalf("a_parts = %+v", r)
	}
}

// TestSmallObjectKeepsTheSinglePut: the capability is asked once and answers
// errors.ErrUnsupported without a request, so the object goes out as one Put —
// the difference the engine must keep for every object that fits a request, and
// for every storage without the capability. Mutant: using the multipart path
// anyway makes the fake's part count non-zero.
func TestSmallObjectKeepsTheSinglePut(t *testing.T) {
	w := newWorld(t)
	parts := newFakeMultipart(1 << 20)
	w.extraStorages["a_parts"] = parts
	w.storages["a_parts"] = parts.Fake
	cfg := baseConfig(t, map[string]any{"a_parts": storageInstance("fake", "keep_last")})
	w.sources["vpn"] = &fakeSource{data: []byte("tiny")}

	rep, err := w.newEngine(t, cfg).Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Artifacts[0].Outcome != model.ArtifactOK {
		t.Fatalf("the artifact = %+v", rep.Artifacts[0])
	}
	begins, aborts, completes, calls := parts.stats()
	if begins < 1 {
		t.Fatalf("begins = %d, want the capability asked while the object grew", begins)
	}
	if aborts != 0 || completes != 0 || len(calls) != 0 {
		t.Fatalf("an object that fits one request used the multipart path: aborts=%d completes=%d calls=%v",
			aborts, completes, calls)
	}
	key := rep.Artifacts[0].Key
	if got := parts.CallCount(storagetest.OpPut, key); got != 1 {
		t.Fatalf("Put calls for %s: %d, want exactly 1", key, got)
	}
	if got := parts.CallCount(storagetest.OpStat, key); got != 1 {
		t.Fatalf("Stat calls for %s: %d, want exactly 1", key, got)
	}
}

// TestStorageBudgetEndsMidUpload: a storage whose time budget runs out in the
// middle of an upload aborts it and fails only its own copy — reported the way
// every other budget failure is, and never at the cost of the other storage.
func TestStorageBudgetEndsMidUpload(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newWorld(t)
		parts := newFakeMultipart(4)
		parts.partDelay = func(_ int32, ctx context.Context) error {
			select {
			case <-time.After(40 * time.Millisecond):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		w.extraStorages["a_parts"] = parts
		w.storages["a_parts"] = parts.Fake
		cfg := baseConfig(t, twoWayStorages())
		w.sources["vpn"] = &fakeSource{data: []byte("0123456789")}
		// The budget is shorter than the three parts need, and longer than one.
		cfg["storages"].(map[string]any)["a_parts"].(map[string]any)["time_budget"] = "60ms"

		rep, err := w.newEngine(t, cfg).Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
		if err != nil {
			t.Fatal(err)
		}
		_, aborts, completes, _ := parts.stats()
		if aborts != 1 {
			t.Fatalf("aborts = %d, want 1: the budget ended mid-upload", aborts)
		}
		if completes != 0 {
			t.Fatalf("completes = %d, want none", completes)
		}
		r := storageResult(rep, "a_parts")
		if r.Outcome != model.StorageFailed {
			t.Fatalf("a_parts = %+v", r)
		}
		if !strings.Contains(r.Error, "part") && !strings.Contains(r.Error, "deadline") {
			t.Fatalf("the failure does not name what ended: %q", r.Error)
		}
		if other := storageResult(rep, "b_plain"); other.Outcome != model.StorageVerified {
			t.Fatalf("b_plain = %+v, want the other storage's copy intact", other)
		}
		if rep.Outcome != model.OutcomeDegraded {
			t.Fatalf("outcome %q, want degraded", rep.Outcome)
		}
	})
}

// TestMultipartCompletionThatCommitsNothingFails: a Complete that answers success
// and stores nothing is caught by the same Stat check as the whole-object path —
// the service's own answer is never the proof. Mutant: dropping the verification
// after Complete reports the copy verified, and the abort with it, while no
// object exists.
func TestMultipartCompletionThatCommitsNothingFails(t *testing.T) {
	w := newWorld(t)
	parts := newFakeMultipart(4)
	parts.completeDrop = true
	w.extraStorages["a_parts"] = parts
	w.storages["a_parts"] = parts.Fake
	cfg := baseConfig(t, twoWayStorages())
	w.sources["vpn"] = &fakeSource{data: []byte("0123456789")}

	rep, err := w.newEngine(t, cfg).Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Artifacts[0].Outcome != model.ArtifactOK {
		t.Fatalf("the artifact = %+v, want the plain storage's copy to hold", rep.Artifacts[0])
	}
	_, aborts, completes, _ := parts.stats()
	if completes != 1 {
		t.Fatalf("completes = %d, want 1", completes)
	}
	if aborts != 1 {
		t.Fatalf("aborts = %d, want exactly 1: the completion held nothing", aborts)
	}
	if r := storageResult(rep, "a_parts"); r.Outcome != model.StorageFailed {
		t.Fatalf("a_parts = %+v, want a failure: the object is not there", r)
	}
	if r := storageResult(rep, "b_plain"); r.Outcome != model.StorageVerified {
		t.Fatalf("b_plain = %+v, want the other storage's copy intact", r)
	}
	if _, ok := parts.Bytes(rep.Artifacts[0].Key); ok {
		t.Fatalf("the dropped completion left an object: %v", parts.Keys())
	}
}

// TestMultipartCommitWithLostAnswerIsVerified: the service committed the upload
// and the answer to the commit was lost, so the retry — or, here, the answer
// itself — is not_found while the object is there. The copy is verified, not
// failed. Mutant: failing on any complete error marks a committed copy failed.
func TestMultipartCommitWithLostAnswerIsVerified(t *testing.T) {
	w := newWorld(t)
	parts := newFakeMultipart(4)
	parts.completeLost = true
	w.extraStorages["a_parts"] = parts
	w.storages["a_parts"] = parts.Fake
	cfg := baseConfig(t, twoWayStorages())
	w.sources["vpn"] = &fakeSource{data: []byte("0123456789")}

	rep, err := w.newEngine(t, cfg).Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Artifacts[0].Outcome != model.ArtifactOK {
		t.Fatalf("the artifact = %+v", rep.Artifacts[0])
	}
	_, aborts, completes, _ := parts.stats()
	if completes != 1 {
		t.Fatalf("completes = %d, want 1", completes)
	}
	if aborts != 0 {
		t.Fatalf("aborts = %d, want none: the upload was already gone", aborts)
	}
	if r := storageResult(rep, "a_parts"); r.Outcome != model.StorageVerified {
		t.Fatalf("a_parts = %+v, want the committed object to verify the copy", r)
	}
	if got, ok := parts.Bytes(rep.Artifacts[0].Key); !ok || !bytes.Equal(got, []byte("0123456789")) {
		t.Fatalf("the committed object is %q, present=%v", got, ok)
	}
}

// TestMultipartCommitGoneWithoutObjectFails: the same not_found answer with
// nothing committed is a failure for that storage alone — the rescue path is a
// proof, never a benefit of the doubt. Mutant: trusting the not_found answer
// alone without the Stat verifies a copy with no object behind it.
func TestMultipartCommitGoneWithoutObjectFails(t *testing.T) {
	w := newWorld(t)
	parts := newFakeMultipart(4)
	parts.completeGone = true
	w.extraStorages["a_parts"] = parts
	w.storages["a_parts"] = parts.Fake
	cfg := baseConfig(t, twoWayStorages())
	w.sources["vpn"] = &fakeSource{data: []byte("0123456789")}

	rep, err := w.newEngine(t, cfg).Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Artifacts[0].Outcome != model.ArtifactOK {
		t.Fatalf("the artifact = %+v, want the plain storage's copy to hold", rep.Artifacts[0])
	}
	_, aborts, _, _ := parts.stats()
	if aborts != 1 {
		t.Fatalf("aborts = %d, want exactly 1", aborts)
	}
	if r := storageResult(rep, "a_parts"); r.Outcome != model.StorageFailed {
		t.Fatalf("a_parts = %+v, want a failure: no object is there", r)
	}
	if r := storageResult(rep, "b_plain"); r.Outcome != model.StorageVerified {
		t.Fatalf("b_plain = %+v, want the other storage's copy intact", r)
	}
	if _, ok := parts.Bytes(rep.Artifacts[0].Key); ok {
		t.Fatalf("an object appeared from a completion that committed nothing: %v", parts.Keys())
	}
}

// TestStaleUploadsOfAKilledRunAreSwept: the sweep runs once for the storage, the
// upload an older-than-a-run killed process left is aborted, and an upload this
// run's own wall clock could still be writing (initiated after the cutoff) is
// left alone. Mutant: passing a cutoff of "now" sweeps the fresh upload too; not
// sweeping at all aborts nothing.
func TestStaleUploadsOfAKilledRunAreSwept(t *testing.T) {
	w := newWorld(t)
	parts := newFakeMultipart(4)
	parts.stale = []storage.StaleUpload{
		{Key: "runs/oldrun/databases/ns/db.dump.zst", UploadID: "upload-old", Initiated: time.Now().Add(-3 * time.Hour)},
		{Key: "runs/live/databases/ns/db.dump.zst", UploadID: "upload-fresh", Initiated: time.Now()},
	}
	w.extraStorages["a_parts"] = parts
	w.storages["a_parts"] = parts.Fake
	cfg := baseConfig(t, twoWayStorages())
	w.sources["vpn"] = &fakeSource{data: []byte("0123456789")}

	rep, err := w.newEngine(t, cfg).Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Artifacts[0].Outcome != model.ArtifactOK {
		t.Fatalf("the artifact = %+v", rep.Artifacts[0])
	}
	if r := storageResult(rep, "a_parts"); r.Outcome != model.StorageVerified {
		t.Fatalf("a_parts = %+v", r)
	}
	lists, aborted := parts.sweepStats()
	if lists != 1 {
		t.Fatalf("the sweep listed %d times, want once per run", lists)
	}
	if len(aborted) != 1 || aborted[0] != "runs/oldrun/databases/ns/db.dump.zst" {
		t.Fatalf("aborted %v, want only the upload the killed run left behind", aborted)
	}
}

// TestStaleSweepFailureIsNotFatal: a sweep that cannot even list costs the run
// nothing — the uploads stay billed, the run keeps its copies, and no storage
// failure is recorded. Mutant: failing the storage on the sweep's error turns a
// delivered run into a degraded one.
func TestStaleSweepFailureIsNotFatal(t *testing.T) {
	w := newWorld(t)
	parts := newFakeMultipart(4)
	parts.staleErr = errors.New("injected listing failure")
	w.extraStorages["a_parts"] = parts
	w.storages["a_parts"] = parts.Fake
	cfg := baseConfig(t, twoWayStorages())
	w.sources["vpn"] = &fakeSource{data: []byte("0123456789")}

	rep, err := w.newEngine(t, cfg).Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Artifacts[0].Outcome != model.ArtifactOK || rep.Outcome != model.OutcomeDelivered {
		t.Fatalf("artifact %+v outcome %q, want delivered", rep.Artifacts[0], rep.Outcome)
	}
	if r := storageResult(rep, "a_parts"); r.Outcome != model.StorageVerified {
		t.Fatalf("a_parts = %+v, want the copy to hold whatever the sweep said", r)
	}
	if _, aborted := parts.sweepStats(); len(aborted) != 0 {
		t.Fatalf("a failed listing aborted %v", aborted)
	}
}

// multipartFake is the multipart fake with scriptable failures on the three calls
// the engine's failure paths run through.
type multipartFake struct {
	*fakeMultipart
	beginErr      error
	beginPartSize *int64
	abortErr      error
	partNum       func(int32) int32
}

func (z *multipartFake) BeginMultipart(ctx context.Context, req storage.PutRequest) (storage.MultipartSession, error) {
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

// multipartRun builds a two-storage world where a_parts is the given multipart
// storage, and runs one scheduled run.
func multipartRun(t *testing.T, z *multipartFake) (*world, model.RunReport) {
	t.Helper()
	w := newWorld(t)
	w.extraStorages["a_parts"] = z
	w.storages["a_parts"] = z.Fake
	cfg := baseConfig(t, twoWayStorages())
	w.sources["vpn"] = &fakeSource{data: []byte("0123456789")}
	rep, err := w.newEngine(t, cfg).Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	return w, rep
}

// TestMultipartBeginErrorFailsOneStorage: a BeginMultipart that fails with a
// real error (not the "one Put" answer) fails that storage's copy and aborts
// nothing — no upload exists yet.
func TestMultipartBeginErrorFailsOneStorage(t *testing.T) {
	z := &multipartFake{fakeMultipart: newFakeMultipart(4), beginErr: plugin.Permanent(errors.New("begin refused"))}
	_, rep := multipartRun(t, z)
	if rep.Artifacts[0].Outcome != model.ArtifactOK {
		t.Fatalf("the artifact = %+v, want the plain storage's copy to hold", rep.Artifacts[0])
	}
	if r := storageResult(rep, "a_parts"); r.Outcome != model.StorageFailed || !strings.Contains(r.Error, "begin refused") {
		t.Fatalf("a_parts = %+v, want the begin failure", r)
	}
	if r := storageResult(rep, "b_plain"); r.Outcome != model.StorageVerified {
		t.Fatalf("b_plain = %+v, want the other storage's copy intact", r)
	}
}

// TestMultipartZeroPartSize: a plugin that answers a part size of zero has
// broken its contract; the upload is refused as permanent.
func TestMultipartZeroPartSize(t *testing.T) {
	zero := int64(0)
	z := &multipartFake{fakeMultipart: newFakeMultipart(4), beginPartSize: &zero}
	_, rep := multipartRun(t, z)
	if r := storageResult(rep, "a_parts"); r.Outcome != model.StorageFailed || !strings.Contains(r.Error, "part size of 0") {
		t.Fatalf("a_parts = %+v, want the zero-part-size refusal", r)
	}
}

// TestUploadPartsSpoolOpenFails: a part whose spool file cannot be opened is
// a permanent failure of the upload — the bytes are gone, no retry recovers them.
func TestUploadPartsSpoolOpenFails(t *testing.T) {
	w := newWorld(t)
	e := w.newEngine(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}))
	sp, err := spool.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	mu := newFakeMultipart(4)
	st := newStorageRunState(e.storages[0])
	pa := producedArtifact{artifact: model.Artifact{Group: "g", Name: "n"}, file: "missing.bin", bytes: 10}
	_, err = e.uploadParts(context.Background(), st, mu, sp,
		&finishedSource{name: pa.file, size: pa.bytes},
		storage.MultipartSession{Key: "k", UploadID: "u", PartSize: 4}, artifactName(pa), 1)
	if err == nil {
		t.Fatal("uploadParts opened a spool file that does not exist")
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassPermanent {
		t.Fatalf("class = %q, want permanent", class)
	}
}

// TestUploadPartsWrongPartNumber: a plugin that answers the wrong part number
// has broken its contract; the upload fails rather than committing a scrambled
// object.
func TestUploadPartsWrongPartNumber(t *testing.T) {
	w := newWorld(t)
	e := w.newEngine(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}))
	dir := t.TempDir()
	sp, err := spool.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	wr, err := sp.Create("f.bin")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wr.Write([]byte("0123456789")); err != nil {
		t.Fatal(err)
	}
	if err := wr.Close(); err != nil {
		t.Fatal(err)
	}
	z := &multipartFake{fakeMultipart: newFakeMultipart(4), partNum: func(n int32) int32 { return n + 1 }}
	session, err := z.fakeMultipart.BeginMultipart(context.Background(), storage.PutRequest{Key: "k", Size: 10})
	if err != nil {
		t.Fatal(err)
	}
	st := newStorageRunState(e.storages[0])
	pa := producedArtifact{artifact: model.Artifact{Group: "g", Name: "n"}, file: wr.Name(), bytes: 10}
	_, err = e.uploadParts(context.Background(), st, z, sp,
		&finishedSource{name: pa.file, size: pa.bytes}, session, artifactName(pa), 1)
	if err == nil || !strings.Contains(err.Error(), "answered part") {
		t.Fatalf("uploadParts = %v, want the wrong-part-number refusal", err)
	}
}

// TestAbortMultipartFailure: a failed abort is reported and returned, so the
// original failure is joined with it rather than hidden.
func TestAbortMultipartFailure(t *testing.T) {
	w := newWorld(t)
	e := w.newEngine(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}))
	z := &multipartFake{fakeMultipart: newFakeMultipart(4), abortErr: errors.New("abort failed")}
	err := e.abortMultipart(context.Background(), newStorageRunState(e.storages[0]), z,
		storage.MultipartSession{Key: "k", UploadID: "u"}, "k")
	if err == nil || !strings.Contains(err.Error(), "abort failed") {
		t.Fatalf("abortMultipart = %v, want the abort failure", err)
	}
}

// TestSweepSkipsAFatalStorage: a storage already marked fatal is not listed
// for stale uploads at all.
func TestSweepSkipsAFatalStorage(t *testing.T) {
	w := newWorld(t)
	z := &multipartFake{fakeMultipart: newFakeMultipart(4)}
	w.extraStorages["a_parts"] = z
	cfg := baseConfig(t, map[string]any{"a_parts": storageInstance("fake", "keep_last")})
	e := w.newEngine(t, cfg)
	sb := e.storages[0]
	st := newStorageRunState(sb)
	st.refuseCap(errors.New("fatal"), 1, 2)
	if got := e.sweepStaleUploads(context.Background(), sb, st, time.Now()); got {
		t.Fatal("a fatal storage's sweep reported unknown occupancy")
	}
	if lists, _ := z.sweepStats(); lists != 0 {
		t.Fatalf("a fatal storage was listed %d times", lists)
	}
}

// TestSweepAbortFailureMakesOccupancyUnknown: a stale upload the sweep cannot
// abort leaves bytes nobody can count, so the sweep reports unknown.
func TestSweepAbortFailureMakesOccupancyUnknown(t *testing.T) {
	w := newWorld(t)
	z := &multipartFake{fakeMultipart: newFakeMultipart(4), abortErr: plugin.Transient(errors.New("abort boom"))}
	z.stale = []storage.StaleUpload{
		{Key: "runs/old/x.bin", UploadID: "u1", Initiated: time.Now().Add(-time.Hour)},
	}
	w.extraStorages["a_parts"] = z
	cfg := baseConfig(t, map[string]any{"a_parts": storageInstance("fake", "keep_last")})
	e := w.newEngine(t, cfg)
	sb := e.storages[0]
	st := newStorageRunState(sb)
	if got := e.sweepStaleUploads(context.Background(), sb, st, time.Now()); !got {
		t.Fatal("a sweep whose abort failed did not report unknown occupancy")
	}
}

// --- retention.go ------------------------------------------------------------

// --- parallel parts -----------------------------------------------------------

// TestMultipartPartsRunParallelWithinTheLimit (A10, P1): with call_parallelism
// 2, two of four parts are in flight at once, each part is sent exactly once,
// and the committed object is the bytes the source wrote.
func TestMultipartPartsRunParallelWithinTheLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newWorld(t)
		parts := newFakeMultipart(10)
		parts.partDelay = func(_ int32, ctx context.Context) error {
			select {
			case <-time.After(10 * time.Millisecond):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		w.extraStorages["a_parts"] = parts
		w.storages["a_parts"] = parts.Fake
		cfg := baseConfig(t, map[string]any{"a_parts": storageInstance("fake", "keep_last")})
		cfg["storages"].(map[string]any)["a_parts"].(map[string]any)["call_parallelism"] = 2
		body := []byte("0123456789012345678901234567890123456789")
		w.sources["vpn"] = &fakeSource{data: body}

		rep, err := w.newEngine(t, cfg).Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
		if err != nil {
			t.Fatal(err)
		}
		if rep.Artifacts[0].Outcome != model.ArtifactOK {
			t.Fatalf("the artifact = %+v", rep.Artifacts[0])
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
		if got, ok := parts.Bytes(rep.Artifacts[0].Key); !ok || !bytes.Equal(got, body) {
			t.Fatalf("the committed object is %q", got)
		}
	})
}

// TestMultipartCommitsPartsInNumberOrder (A11, P2): parts may finish in any
// order — here the first is the slowest — and Complete is still handed the
// list by number, so the object is byte-for-byte the source's.
func TestMultipartCommitsPartsInNumberOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newWorld(t)
		parts := newFakeMultipart(10)
		parts.partDelay = func(number int32, ctx context.Context) error {
			select {
			case <-time.After(time.Duration(5-number) * 10 * time.Millisecond):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		w.extraStorages["a_parts"] = parts
		w.storages["a_parts"] = parts.Fake
		cfg := baseConfig(t, map[string]any{"a_parts": storageInstance("fake", "keep_last")})
		cfg["storages"].(map[string]any)["a_parts"].(map[string]any)["call_parallelism"] = 4
		body := []byte("0123456789012345678901234567890123456789")
		w.sources["vpn"] = &fakeSource{data: body}

		rep, err := w.newEngine(t, cfg).Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
		if err != nil {
			t.Fatal(err)
		}
		if rep.Artifacts[0].Outcome != model.ArtifactOK {
			t.Fatalf("the artifact = %+v", rep.Artifacts[0])
		}
		_, _, completes, _ := parts.stats()
		if completes != 1 {
			t.Fatalf("completes = %d, want 1", completes)
		}
		_, _, _, order := parts.flightStats()
		if len(order) != 4 || order[0] != 1 || order[1] != 2 || order[2] != 3 || order[3] != 4 {
			t.Fatalf("Complete was handed %v, want the parts by number", order)
		}
		if got, ok := parts.Bytes(rep.Artifacts[0].Key); !ok || !bytes.Equal(got, body) {
			t.Fatalf("the committed object is %q", got)
		}
	})
}

// TestMultipartPartsShareOneBudgetInterval (A15, C1): two parts run at once for
// forty milliseconds, then the third runs for forty: the storage spends eighty
// milliseconds of wall time, not the hundred and twenty of their sum, so a
// hundred-millisecond budget still verifies the copy.
func TestMultipartPartsShareOneBudgetInterval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newWorld(t)
		parts := newFakeMultipart(10)
		parts.partDelay = func(_ int32, ctx context.Context) error {
			select {
			case <-time.After(40 * time.Millisecond):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		w.extraStorages["a_parts"] = parts
		w.storages["a_parts"] = parts.Fake
		cfg := baseConfig(t, map[string]any{"a_parts": storageInstance("fake", "keep_last")})
		st := cfg["storages"].(map[string]any)["a_parts"].(map[string]any)
		st["call_parallelism"] = 2
		st["time_budget"] = "100ms"
		w.sources["vpn"] = &fakeSource{data: []byte("012345678901234567890123456789")} // three 10-byte parts

		rep, err := w.newEngine(t, cfg).Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
		if err != nil {
			t.Fatal(err)
		}
		if rep.Artifacts[0].Outcome != model.ArtifactOK {
			t.Fatalf("the artifact = %+v", rep.Artifacts[0])
		}
		r := storageResult(rep, "a_parts")
		if r.Outcome != model.StorageVerified {
			t.Fatalf("a_parts = %+v, want the copy verified inside the budget", r)
		}
		if got := r.Duration.Std(); got != 80*time.Millisecond {
			t.Fatalf("the storage duration = %s, want the 80ms union of the two overlapping parts and the third", got)
		}
	})
}

// TestMultipartPermanentPartFailureStopsNewParts (F8, P2): a permanent failure
// of part 2 while part 1 is in flight sends no further part, aborts the upload
// exactly once after the in-flight part returned, and reports the part's own
// refusal — never the cancellation of its neighbour.
func TestMultipartPermanentPartFailureStopsNewParts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newWorld(t)
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
				return plugin.Permanent(errors.New("part 2 refused"))
			}
			return nil
		}
		w.extraStorages["a_parts"] = parts
		w.storages["a_parts"] = parts.Fake
		cfg := baseConfig(t, twoWayStorages())
		cfg["storages"].(map[string]any)["a_parts"].(map[string]any)["call_parallelism"] = 2
		w.sources["vpn"] = &fakeSource{data: []byte("0123456789abcd")}

		rep, err := w.newEngine(t, cfg).Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
		if err != nil {
			t.Fatal(err)
		}
		if rep.Artifacts[0].Outcome != model.ArtifactOK {
			t.Fatalf("the artifact = %+v, want the plain storage's copy", rep.Artifacts[0])
		}
		_, aborts, completes, calls := parts.stats()
		if aborts != 1 || completes != 0 {
			t.Fatalf("aborts=%d completes=%d, want one abort and no complete", aborts, completes)
		}
		if calls[3] != 0 {
			t.Fatalf("part calls %v: a part was sent after the permanent failure", calls)
		}
		_, abortInFlight, _, _ := parts.flightStats()
		if len(abortInFlight) != 1 || abortInFlight[0] != 0 {
			t.Fatalf("the abort ran with %v parts in flight, want none", abortInFlight)
		}
		r := storageResult(rep, "a_parts")
		if !strings.Contains(r.Error, "part 2 refused") {
			t.Fatalf("a_parts error = %q, want the part's refusal", r.Error)
		}
		if strings.Contains(r.Error, "context canceled") {
			t.Fatalf("a_parts error = %q: the neighbour's cancellation hid the cause", r.Error)
		}
		if r := storageResult(rep, "b_plain"); r.Outcome != model.StorageVerified {
			t.Fatalf("b_plain = %+v, want the other storage's copy", r)
		}
	})
}

// TestPermanentPartFailureOutranksALateProducerFailure (M1): the cause the
// upload chose is the one the storage reports — a producer that failed a moment
// later is not a reason to abandon a permanent part refusal and open a second
// session. The storage is failed with the part's own error, one session was
// opened, and the fallback never ran.
func TestPermanentPartFailureOutranksALateProducerFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newWorld(t)
		parts := addMultipart(w, "a_parts", 4)
		src := &benchSource{
			chunks: func(int) [][]byte { return [][]byte{[]byte("0123456789abcd")} },
			err: func(attempt int) error {
				if attempt == 1 {
					return errors.New("producer interrupted")
				}
				return nil
			},
			gateAfter: 1, release: make(chan struct{}),
		}
		w.extraSources["vpn"] = src
		var release sync.Once
		parts.partDelay = func(number int32, ctx context.Context) error {
			if number == 1 {
				<-ctx.Done()
				release.Do(func() { close(src.release) })
				time.Sleep(2 * time.Millisecond)
				return ctx.Err()
			}
			return nil
		}
		parts.partErr = func(number int32, _ int) error {
			if number == 2 {
				return plugin.Permanent(errors.New("part 2 refused"))
			}
			return nil
		}
		cfg := overlapConfig(t, twoWayStorages())
		cfg["storages"].(map[string]any)["a_parts"].(map[string]any)["call_parallelism"] = 2

		rep, err := w.newEngine(t, cfg).Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
		if err != nil {
			t.Fatal(err)
		}
		got := storageResult(rep, "a_parts")
		if got.Outcome != model.StorageFailed || !strings.Contains(got.Error, "part 2 refused") || parts.sessionCount() != 1 {
			t.Fatalf("permanent part failure lost: outcome=%s error=%q sessions=%d",
				got.Outcome, got.Error, parts.sessionCount())
		}
	})
}

// TestIndependentPartErrorOutranksTheSourceFailure (M1): a part whose own error
// is a refusal — not the cancellation the upload imposed on it — outranks the
// producer's failure even when the producer failed first and the context was
// already cancelled when the part returned.
func TestIndependentPartErrorOutranksTheSourceFailure(t *testing.T) {
	sp, err := spool.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	file := covSpoolFile(t, sp, []byte("01234"))
	g := newGrowth()
	g.begin(file)
	g.advance(5)
	partFailure := plugin.Permanent(errors.New("part refused"))
	sourceFailure := errors.New("producer failed")
	parts := newFakeMultipart(4)
	parts.partDelay = func(_ int32, ctx context.Context) error {
		g.fail(sourceFailure)
		<-ctx.Done()
		return partFailure
	}
	session, err := parts.BeginMultipart(context.Background(), storage.PutRequest{Key: "k", Size: 5})
	if err != nil {
		t.Fatal(err)
	}
	e := &Engine{retry: retryPolicy{attempts: 1}}
	st := &storageRunState{clock: storageClock{budget: time.Minute}}
	_, err = e.uploadParts(context.Background(), st, parts, sp, &followSource{g: g}, session, "g/n", 2)
	if !errors.Is(err, partFailure) {
		t.Fatalf("independent part failure lost: got=%v want=%v", err, partFailure)
	}

	// The producer's own failure is returned as itself: the caller can read its
	// message and unwrap it, which is what tells the fallback from the rest.
	src := newFakeMultipart(4)
	session, err = src.BeginMultipart(context.Background(), storage.PutRequest{Key: "k", Size: 5})
	if err != nil {
		t.Fatal(err)
	}
	_, err = e.uploadParts(context.Background(), st, src, sp, &followSource{g: g}, session, "g/n", 1)
	if err == nil || err.Error() != sourceFailure.Error() || !errors.Is(err, sourceFailure) {
		t.Fatalf("producer failure = %v, want the producer's own error, message and identity", err)
	}
}

// TestCancelledUploadOpensNoPart (m1): an upload whose context is already
// cancelled sends nothing — the slot is taken and released without a plugin
// call, so no file is opened and no attempt is booked.
func TestCancelledUploadOpensNoPart(t *testing.T) {
	sp, err := spool.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	file := covSpoolFile(t, sp, []byte("0123456789"))
	parts := newFakeMultipart(4)
	session, err := parts.BeginMultipart(context.Background(), storage.PutRequest{Key: "k", Size: 10})
	if err != nil {
		t.Fatal(err)
	}
	e := &Engine{retry: retryPolicy{attempts: 1}}
	st := &storageRunState{clock: storageClock{budget: time.Minute}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = e.uploadParts(ctx, st, parts, sp, &finishedSource{name: file, size: 10}, session, "g/n", 2)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("uploadParts = %v, want the cancelled context", err)
	}
	if _, _, journal, _ := parts.flightStats(); len(journal) != 1 || journal[0] != "begin" {
		t.Fatalf("a cancelled upload sent a part: %v", journal)
	}
}

// TestTerminalCauseAcrossTerminationOrderAndParallelism (M1, P2): whatever ends
// the upload — a part's own refusal, the producer's failure, the run's
// cancellation — and whatever the ceiling, the chosen cause survives to the
// storage's report, the fatal state follows it, the producer's failure is the
// only one that falls back to the finished file, and Abort runs after every
// part returned.
func TestTerminalCauseAcrossTerminationOrderAndParallelism(t *testing.T) {
	for _, order := range []string{"part", "source", "parent", "partparent"} {
		for _, parallel := range []int{1, 2, 4} {
			t.Run(fmt.Sprintf("%s/P%d", order, parallel), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					w := newWorld(t)
					parts := addMultipart(w, "a_parts", 4)
					plain := storagetest.NewFake()
					w.extraStorages["b_plain"] = plain
					w.storages["b_plain"] = plain
					cfg := overlapConfig(t, twoWayStorages())
					cfg["storages"].(map[string]any)["a_parts"].(map[string]any)["call_parallelism"] = parallel
					e := w.newEngine(t, cfg)

					ctx, cancel := context.WithCancel(context.Background())
					t.Cleanup(cancel)
					var once sync.Once
					src := &benchSource{
						chunks:    func(int) [][]byte { return [][]byte{[]byte("0123456789abcd")} },
						gateAfter: 1, release: make(chan struct{}),
					}
					switch order {
					case "part":
						parts.partErr = func(number int32, _ int) error {
							if number == 1 {
								once.Do(func() { close(src.release) })
								return plugin.Permanent(errors.New("part 1 refused"))
							}
							return nil
						}
						parts.partDelay = func(number int32, c context.Context) error {
							if number > 1 {
								<-c.Done()
								return c.Err()
							}
							return nil
						}
					case "source":
						failed := make(chan struct{})
						var failedOnce sync.Once
						src.err = func(attempt int) error {
							if attempt != 1 {
								return nil
							}
							failedOnce.Do(func() { close(failed) })
							return errors.New("producer interrupted")
						}
						parts.partHook = func(number int32) {
							if number == 1 {
								once.Do(func() { close(src.release) })
							}
						}
						// The parts of the failed attempt let go once the producer
						// has failed, so the source's own cause is the one seen;
						// the fallback session uploads normally.
						parts.partDelay = func(number int32, c context.Context) error {
							if parts.sessionCount() > 1 {
								return nil
							}
							select {
							case <-c.Done():
								return c.Err()
							case <-failed:
								return nil
							}
						}
					case "parent":
						parts.partHook = func(number int32) {
							if number == 1 {
								once.Do(cancel)
							}
						}
						parts.partDelay = func(number int32, c context.Context) error {
							<-c.Done()
							return c.Err()
						}
					case "partparent":
						// The part's own refusal and the run's cancellation happen
						// together: the refusal was recorded first and stays the
						// cause.
						parts.partErr = func(number int32, _ int) error {
							if number == 1 {
								once.Do(cancel)
								return plugin.Permanent(errors.New("part 1 refused"))
							}
							return nil
						}
						parts.partDelay = func(number int32, c context.Context) error {
							if number > 1 {
								<-c.Done()
								return c.Err()
							}
							return nil
						}
					}

					sp, err := spool.New(t.TempDir())
					if err != nil {
						t.Fatal(err)
					}
					results := map[string]*storageRunState{}
					caps := map[string]*capState{}
					for _, sb := range e.storages {
						results[sb.name] = newStorageRunState(sb)
						caps[sb.name] = newCapState(1<<40, 0, true)
					}
					plan := artifactPlan{instance: "vpn", srcType: "fake", src: src,
						artifact: model.Artifact{Group: "g", Name: "n", Importance: model.ImportanceRequired, Sensitivity: model.SensitivityPlain}}
					ar, verified := e.processArtifact(ctx, sp, plan, "r", model.RunKindScheduled, e.storages, results, caps)

					st := results["a_parts"]
					switch order {
					case "part":
						if !strings.Contains(st.uploadErr.Error(), "part 1 refused") {
							t.Fatalf("storage error = %v, want the part's own refusal", st.uploadErr)
						}
						if st.fatal == nil || !strings.Contains(st.fatal.Error(), "part 1 refused") {
							t.Fatalf("fatal = %v, want the permanent part failure", st.fatal)
						}
						if got := parts.sessionCount(); got != 1 {
							t.Fatalf("sessions = %d, want no fallback after a part refusal", got)
						}
						if ar.Outcome != model.ArtifactOK || !verified["b_plain"] {
							t.Fatalf("artifact %+v verified=%v, want the plain storage's copy", ar, verified)
						}
					case "source":
						if st.uploadErr != nil {
							t.Fatalf("storage error = %v, want the producer failure to fall back", st.uploadErr)
						}
						if st.fatal != nil {
							t.Fatalf("fatal = %v, want the producer's own failure to stay non-fatal", st.fatal)
						}
						if got := parts.sessionCount(); got != 2 {
							t.Fatalf("sessions = %d, want the failed attempt and the fallback", got)
						}
						if ar.Outcome != model.ArtifactOK || !verified["a_parts"] {
							t.Fatalf("artifact %+v verified=%v, want the fallback to verify a_parts", ar, verified)
						}
					case "parent":
						if st.uploadErr == nil || !strings.Contains(st.uploadErr.Error(), "context canceled") {
							t.Fatalf("storage error = %v, want the run's cancellation", st.uploadErr)
						}
						if st.fatal != nil {
							t.Fatalf("fatal = %v, want the cancellation to stay non-fatal", st.fatal)
						}
						if got := parts.sessionCount(); got != 1 {
							t.Fatalf("sessions = %d, want no fallback for a cancelled run", got)
						}
					case "partparent":
						if st.uploadErr == nil || !strings.Contains(st.uploadErr.Error(), "part 1 refused") {
							t.Fatalf("storage error = %v, want the part's own refusal", st.uploadErr)
						}
						if st.fatal != nil {
							t.Fatalf("fatal = %v, want the cancelled run to keep the storage non-fatal", st.fatal)
						}
						if got := parts.sessionCount(); got != 1 {
							t.Fatalf("sessions = %d, want no fallback after a part refusal", got)
						}
					}
					_, aborts, _, _ := parts.stats()
					if aborts != 1 {
						t.Fatalf("aborts = %d, want the one unfinished upload aborted", aborts)
					}
					_, abortInFlight, journal, _ := parts.flightStats()
					if len(abortInFlight) != 1 || abortInFlight[0] != 0 {
						t.Fatalf("the abort ran with %v parts in flight, want none", abortInFlight)
					}
					if after := partsBetweenTheAbortAndTheNextBegin(journal); len(after) != 0 {
						t.Fatalf("parts sent between the abort and the next session: %v", after)
					}
				})
			})
		}
	}
}

// TestSendPartRefusesACancelledContext (m1): the context is checked immediately
// before the call — the slot and the cancellation can be ready together — so a
// cancelled upload opens no spool file, books no attempt and reaches no plugin.
func TestSendPartRefusesACancelledContext(t *testing.T) {
	sp, err := spool.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	file := covSpoolFile(t, sp, []byte("0123456789"))
	parts := newFakeMultipart(4)
	session, err := parts.BeginMultipart(context.Background(), storage.PutRequest{Key: "k", Size: 10})
	if err != nil {
		t.Fatal(err)
	}
	e := &Engine{retry: retryPolicy{attempts: 1}}
	st := &storageRunState{clock: storageClock{budget: time.Minute}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = e.sendPart(ctx, st, parts, sp, file, session, 1, 0, 4, "g/n")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("sendPart = %v, want the cancelled context", err)
	}
	if _, _, journal, _ := parts.flightStats(); len(journal) != 1 {
		t.Fatalf("a cancelled part reached the storage: %v", journal)
	}
	if got := st.result.Attempts; got != 0 {
		t.Fatalf("a cancelled part booked %d attempts", got)
	}
}

// --- terminal cause: recorded facts, not late context state ------------------

// TestSourceFailureOutranksTheParentCancellation (M1): the source failed while
// the run was also cancelled — the producer's failure is still the cause, even
// though the parent's context was already dead when the error was recorded.
func TestSourceFailureOutranksTheParentCancellation(t *testing.T) {
	failure := errors.New("producer failed")
	g := newGrowth()
	g.fail(failure)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := (&Engine{}).uploadParts(ctx, &storageRunState{}, nil, nil,
		&followSource{g: g}, storage.MultipartSession{PartSize: 4}, "g/n", 2)
	if !errors.Is(err, failure) {
		t.Fatalf("got=%v want=%v", err, failure)
	}
}

// TestRunParentKeepsIndependentPartFailure (M1): a permanent part refusal that
// coincides with the run's cancellation stays the reported cause — the caller
// never replaces it with the context's own error.
func TestRunParentKeepsIndependentPartFailure(t *testing.T) {
	w := newWorld(t)
	parts := addMultipart(w, "a_parts", 4)
	w.extraSources["vpn"] = &benchSource{
		chunks:    func(int) [][]byte { return [][]byte{[]byte("01234")} },
		gateAfter: 1, release: make(chan struct{}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	parts.partErr = func(int32, int) error {
		failure := plugin.Permanent(errors.New("part refused"))
		cancel()
		return failure
	}
	cfg := overlapConfig(t, map[string]any{"a_parts": storageInstance("fake", "keep_last")})
	rep, err := w.newEngine(t, cfg).Run(ctx, RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	got := storageResult(rep, "a_parts")
	if !strings.Contains(got.Error, "part refused") {
		t.Fatalf("independent part failure lost: outcome=%s error=%q", got.Outcome, got.Error)
	}
}

// TestSpoolFailureStopsUpload (M2): a part that finds its spool file gone with
// no producer failure behind it ends the upload at once — the parts are
// cancelled, the producer is not waited for, and the ENOENT is the cause.
func TestSpoolFailureStopsUpload(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sp, err := spool.New(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		file := covSpoolFile(t, sp, []byte("01234"))
		g := newGrowth()
		g.begin(file)
		g.advance(5)
		if err := sp.Remove(file); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		e := &Engine{retry: retryPolicy{attempts: 1}}
		st := &storageRunState{clock: storageClock{budget: time.Minute}}
		start := time.Now()
		_, err = e.uploadParts(ctx, st, newFakeMultipart(4), sp, &followSource{g: g},
			storage.MultipartSession{Key: "k", UploadID: "u", PartSize: 4}, "g/n", 2)
		if elapsed := time.Since(start); elapsed != 0 || !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("spool failure waited %s: got=%v want immediate ENOENT", elapsed, err)
		}
	})
}

// TestSpoolOpenFailureIsTerminal: any other spool-open failure is a terminal
// part failure too — only a vanished file behind a recorded producer failure is
// the failed attempt's cleanup.
func TestSpoolOpenFailureIsTerminal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sp, err := spool.New(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		long := strings.Repeat("a", 300)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		e := &Engine{retry: retryPolicy{attempts: 1}}
		st := &storageRunState{clock: storageClock{budget: time.Minute}}
		start := time.Now()
		_, err = e.uploadParts(ctx, st, newFakeMultipart(4), sp,
			&finishedSource{name: long, size: 5},
			storage.MultipartSession{Key: "k", UploadID: "u", PartSize: 4}, "g/n", 2)
		if elapsed := time.Since(start); elapsed != 0 || err == nil {
			t.Fatalf("spool open failure waited %s: got=%v, want an immediate terminal failure", elapsed, err)
		}
		var ue *uploadError
		if !errors.As(err, &ue) || ue.origin != originPart {
			t.Fatalf("cause = %v, want the part's own failure", err)
		}
	})
}

// TestTerminalCauseCrossProduct (M1, M2, P2): every ordered pair of the events
// that can end an upload — an independent part refusal, the producer's failure,
// a vanished spool file, the run's cancellation — is recorded when it happens,
// the ONE decision point picks a part failure first, then the producer's
// failure, then the cancellation, and recording a cause stops the upload at that
// same site. The real cancellation is what the proof observes.
func TestTerminalCauseCrossProduct(t *testing.T) {
	type event int
	const (
		eventPart event = iota + 1
		eventSource
		eventSpool
		eventParent
	)
	names := map[event]string{eventPart: "part", eventSource: "source", eventSpool: "spool", eventParent: "parent"}
	partFailure := plugin.Permanent(errors.New("part refused"))
	sourceFailure := errors.New("producer failed")
	vanished := plugin.Permanent(fmt.Errorf("%w: %w", errVanishedPart, os.ErrNotExist))

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

			// The facts as the recording rules see them: a part is always
			// independent, a spool file is one only before a source failure, a
			// source is new only before any own cause, and the cancellation never
			// outranks either.
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
			// The stop is part of the recording: the first own cause cancelled
			// the upload's context at the site that recorded it, and the run's
			// cancellation alone never did.
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

// failedGrowth is a growth whose producer already failed: what a part finds when
// it asks whether a vanished spool file is the failed attempt's cleanup.
func failedGrowth(err error) *growth {
	g := newGrowth()
	g.fail(err)
	return g
}

// TestPartErrorDependency: a part's context error is a consequence only when
// this upload stopped the call or the run ended it; a context error with no
// such fact behind it is the part's own failure, and so is any vanished file
// without a recorded producer failure — a vanished file behind the producer's
// failure is its cleanup and stops the upload through that failure.
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
		g := failedGrowth(producerErr)
		term, pctx := newTerm()
		term.recordVanished(context.Background(), &followSource{g: g})
		stoppedCause := context.Cause(pctx)
		if stoppedCause == nil {
			t.Fatal("recording the producer's failure did not stop the upload")
		}
		term.recordPart(context.Background(), plugin.Permanent(fmt.Errorf("%w: %w", errVanishedPart, os.ErrNotExist)))
		if got := context.Cause(pctx); !errors.Is(got, stoppedCause) {
			t.Fatal("the cleanup error replaced the producer's failure")
		}
		if got := term.cause(); got == nil || got.origin != originSource || !errors.Is(got.err, producerErr) {
			t.Fatalf("cause = %v, want the producer's failure", got)
		}
	})
	t.Run("a vanished file without a producer failure is the part's own", func(t *testing.T) {
		g := newGrowth()
		g.begin("file")
		g.advance(4)
		term, pctx := newTerm()
		term.recordVanished(context.Background(), &followSource{g: g})
		if got := context.Cause(pctx); got != nil {
			t.Fatalf("a healthy source stopped the upload: %v", got)
		}
		term.recordPart(context.Background(), plugin.Permanent(fmt.Errorf("%w: %w", errVanishedPart, os.ErrNotExist)))
		if got := context.Cause(pctx); got == nil {
			t.Fatal("a vanished file with no producer failure did not stop the upload")
		}
		if got := term.cause(); got == nil || got.origin != originPart {
			t.Fatalf("cause = %v, want the part's own failure", got)
		}
	})
	t.Run("a recorded part failure keeps priority over a later source failure", func(t *testing.T) {
		partFailure := plugin.Permanent(errors.New("part refused"))
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
// reported as that upload's own part failure, so the caller still fails the
// storage rather than guessing a fallback.
func TestTerminalOfAForeignError(t *testing.T) {
	foreign := errors.New("boom")
	origin, cause := terminalOf(foreign)
	if origin != originPart || !errors.Is(cause, foreign) {
		t.Fatalf("terminalOf = (%v, %v), want the error itself as a part failure", origin, cause)
	}
}

// TestSourceFailureDuringSlotWait (M1): the producer failed and then the run was
// cancelled while the dispatcher waited for a slot — the producer's failure is
// still the cause, recorded at the single exit even though no part or source
// call reported it.
func TestSourceFailureDuringSlotWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sp, err := spool.New(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		file := covSpoolFile(t, sp, []byte("0123456789abcd"))
		g := newGrowth()
		g.begin(file)
		g.advance(14)
		started := make(chan struct{}, 3)
		release := make(chan struct{})
		parts := newFakeMultipart(4)
		parts.partDelay = func(_ int32, ctx context.Context) error {
			started <- struct{}{}
			<-release
			return ctx.Err()
		}
		session, err := parts.BeginMultipart(context.Background(), storage.PutRequest{Key: "k", Size: 14})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		e := &Engine{retry: retryPolicy{attempts: 1}}
		st := &storageRunState{clock: storageClock{budget: time.Minute}}
		done := make(chan error, 1)
		go func() {
			_, err := e.uploadParts(ctx, st, parts, sp, &followSource{g: g}, session, "g/n", 2)
			done <- err
		}()
		<-started
		<-started
		failure := errors.New("producer failed before parent cancellation")
		g.fail(failure)
		cancel()
		synctest.Wait()
		close(release)
		if err := <-done; !errors.Is(err, failure) {
			t.Fatalf("source failure lost during slot wait: got=%v want=%v", err, failure)
		}
	})
}

// TestSimultaneousSourceAndPartFailuresKeepThePart (source+part, P2): the
// producer's failure and one part's independent refusal happen together. The
// part was recorded as a cause and keeps the priority; the source failure is a
// fact, not a replacement, and the upload stops at the part's site.
func TestSimultaneousSourceAndPartFailuresKeepThePart(t *testing.T) {
	for _, parallel := range []int{1, 2, 4} {
		t.Run(fmt.Sprintf("P%d", parallel), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				size := 4*parallel + 8
				sp, g := sweepSpool(t, size)
				failNow := make(chan struct{})
				started := make(chan struct{}, parallel)
				parts := newFakeMultipart(4)
				parts.partDelay = func(number int32, ctx context.Context) error {
					started <- struct{}{}
					if number == 1 {
						return nil
					}
					<-ctx.Done()
					return ctx.Err()
				}
				sourceFailure := errors.New("producer failed")
				parts.partErr = func(number int32, _ int) error {
					if number != 1 {
						return nil
					}
					<-failNow
					g.fail(sourceFailure)
					return plugin.Permanent(errors.New("part refused"))
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				done := make(chan struct{})
				var err error
				var elapsed time.Duration
				go func() {
					defer close(done)
					elapsed, err = runSweep(t, parallel, ctx, parts, sp, g)
				}()
				for range parallel {
					<-started
				}
				synctest.Wait()
				close(failNow)
				<-done
				origin, _ := terminalOf(err)
				if origin != originPart || !strings.Contains(err.Error(), "part refused") || elapsed != 0 {
					t.Fatalf("elapsed=%s origin=%d err=%v, want the part's refusal at once", elapsed, origin, err)
				}
			})
		})
	}
}

// sweepSpool is one upload's spool and growth holding size final bytes.
func sweepSpool(t *testing.T, size int) (*spool.Spool, *growth) {
	t.Helper()
	sp, err := spool.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	file := covSpoolFile(t, sp, bytes.Repeat([]byte("x"), size))
	g := newGrowth()
	g.begin(file)
	g.advance(int64(size))
	return sp, g
}

// TestTerminalStateSweep (M1, P2): every terminal fact — a part's refusal, the
// producer's failure, the run's cancellation, and the pairs the plan orders —
// is recorded in the dispatcher state it happens in (waiting in next, waiting
// for a slot, in join), for P=1/2/4, and the reported cause is the plan's
// priority: part > source > parent. A source failure while all slots are busy
// wakes the wait, stops the parts and returns at once.
func TestTerminalStateSweep(t *testing.T) {
	type row struct {
		name string
		run  func(t *testing.T, parallel int, ctx context.Context, cancel context.CancelFunc) (terminalOrigin, time.Duration, error)
	}
	rows := []row{
		{"part/next", func(t *testing.T, parallel int, ctx context.Context, _ context.CancelFunc) (terminalOrigin, time.Duration, error) {
			sp, g := sweepSpool(t, 4*parallel+4)
			failNow := make(chan struct{})
			started := make(chan struct{}, parallel)
			parts := newFakeMultipart(4)
			parts.partDelay = func(number int32, ctx context.Context) error {
				started <- struct{}{}
				if number == 1 {
					return nil
				}
				<-ctx.Done()
				return ctx.Err()
			}
			parts.partErr = func(number int32, _ int) error {
				if number == 1 {
					<-failNow
					return plugin.Permanent(errors.New("part refused"))
				}
				return nil
			}
			return sweepTrigger(t, parallel, parallel, ctx, parts, sp, g, started, func() { close(failNow) })
		}},
		{"part/slot", func(t *testing.T, parallel int, ctx context.Context, _ context.CancelFunc) (terminalOrigin, time.Duration, error) {
			sp, g := sweepSpool(t, 4*parallel+8)
			failNow := make(chan struct{})
			started := make(chan struct{}, parallel)
			parts := newFakeMultipart(4)
			parts.partDelay = func(number int32, ctx context.Context) error {
				started <- struct{}{}
				if number == 1 {
					return nil
				}
				<-ctx.Done()
				return ctx.Err()
			}
			parts.partErr = func(number int32, _ int) error {
				if number == 1 {
					<-failNow
					return plugin.Permanent(errors.New("part refused"))
				}
				return nil
			}
			return sweepTrigger(t, parallel, parallel, ctx, parts, sp, g, started, func() { close(failNow) })
		}},
		{"part-after-parent/join", func(t *testing.T, parallel int, ctx context.Context, cancel context.CancelFunc) (terminalOrigin, time.Duration, error) {
			sp, g := sweepSpool(t, 4*parallel+8)
			release := make(chan struct{})
			settled := make(chan struct{}, parallel)
			parts := newFakeMultipart(4)
			// Every part is held on the test's rendezvous, never on the context:
			// the cancellation cannot preempt a part the test still waits to see
			// start. With a single slot there is no second part to refuse, so the
			// run's cancellation alone ends the upload.
			parts.partDelay = func(_ int32, ctx context.Context) error {
				settled <- struct{}{}
				<-release
				if parallel > 1 {
					// Every held part carries its own refusal into the join; the
					// first one recorded is the cause, whichever scheduler order
					// produced it.
					return plugin.Permanent(errors.New("late part refusal"))
				}
				return ctx.Err()
			}
			wait := min(parallel, 2)
			return sweepRendezvous(t, wait, parallel, ctx, parts, sp, g, settled, func() {
				cancel()
				// The dispatcher is now blocked in join; the parts are still held.
				synctest.Wait()
				close(release)
			})
		}},
		{"source/next", func(t *testing.T, parallel int, ctx context.Context, _ context.CancelFunc) (terminalOrigin, time.Duration, error) {
			sp, g := sweepSpool(t, 4*parallel+4)
			started := make(chan struct{}, parallel)
			parts := newFakeMultipart(4)
			parts.partDelay = func(_ int32, ctx context.Context) error {
				started <- struct{}{}
				<-ctx.Done()
				return ctx.Err()
			}
			return sweepTrigger(t, parallel, parallel, ctx, parts, sp, g, started, func() {
				g.fail(errors.New("producer failed"))
			})
		}},
		{"source/slot", func(t *testing.T, parallel int, ctx context.Context, _ context.CancelFunc) (terminalOrigin, time.Duration, error) {
			sp, g := sweepSpool(t, 4*parallel+8)
			started := make(chan struct{}, parallel)
			parts := newFakeMultipart(4)
			parts.partDelay = func(_ int32, ctx context.Context) error {
				started <- struct{}{}
				<-ctx.Done()
				return ctx.Err()
			}
			return sweepTrigger(t, parallel, parallel, ctx, parts, sp, g, started, func() {
				// A bare change first (the producer writes more): the wait wakes,
				// finds no terminal state and waits again on the fresh channel.
				g.advance(1)
				synctest.Wait()
				g.fail(errors.New("producer failed"))
			})
		}},
		{"source-after-parent/join", func(t *testing.T, parallel int, ctx context.Context, cancel context.CancelFunc) (terminalOrigin, time.Duration, error) {
			sp, g := sweepSpool(t, 4*parallel+8)
			started := make(chan struct{}, parallel)
			release := make(chan struct{})
			var once sync.Once
			parts := newFakeMultipart(4)
			parts.partHook = func(number int32) {
				if number == 1 {
					once.Do(cancel)
				}
			}
			parts.partDelay = func(_ int32, ctx context.Context) error {
				started <- struct{}{}
				<-release
				return ctx.Err()
			}
			return sweepTrigger(t, 1, parallel, ctx, parts, sp, g, started, func() {
				g.fail(errors.New("producer failed"))
				close(release)
			})
		}},
		{"parent/next", func(t *testing.T, parallel int, ctx context.Context, cancel context.CancelFunc) (terminalOrigin, time.Duration, error) {
			sp, g := sweepSpool(t, 4*parallel+4)
			started := make(chan struct{}, parallel)
			parts := newFakeMultipart(4)
			parts.partDelay = func(_ int32, ctx context.Context) error {
				started <- struct{}{}
				<-ctx.Done()
				return ctx.Err()
			}
			return sweepTrigger(t, parallel, parallel, ctx, parts, sp, g, started, cancel)
		}},
		{"parent/slot", func(t *testing.T, parallel int, ctx context.Context, cancel context.CancelFunc) (terminalOrigin, time.Duration, error) {
			sp, g := sweepSpool(t, 4*parallel+8)
			started := make(chan struct{}, parallel)
			parts := newFakeMultipart(4)
			parts.partDelay = func(_ int32, ctx context.Context) error {
				started <- struct{}{}
				<-ctx.Done()
				return ctx.Err()
			}
			return sweepTrigger(t, parallel, parallel, ctx, parts, sp, g, started, cancel)
		}},
	}
	for _, row := range rows {
		for _, parallel := range []int{1, 2, 4} {
			t.Run(fmt.Sprintf("%s/P%d", row.name, parallel), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					origin, elapsed, err := row.run(t, parallel, ctx, cancel)
					want := originPart
					switch {
					case strings.HasPrefix(row.name, "source"):
						want = originSource
					case strings.HasPrefix(row.name, "parent"):
						want = originParent
					case row.name == "part-after-parent/join" && parallel == 1:
						want = originParent
					}
					if origin != want {
						t.Fatalf("origin = %d, want %d (err %v)", origin, want, err)
					}
					if elapsed != 0 {
						t.Fatalf("the upload returned after %s, want the terminal fact to end it at once", elapsed)
					}
				})
			})
		}
	}
}

// sweepTrigger runs one upload, waits for every slot to be busy or for the
// dispatcher to reach the row's state, applies the fact and reports the cause.
func sweepTrigger(t *testing.T, wait, parallel int, ctx context.Context, parts *fakeMultipart, sp *spool.Spool, g *growth,
	started <-chan struct{}, fact func()) (terminalOrigin, time.Duration, error) {
	t.Helper()
	done := make(chan struct{})
	var err error
	var elapsed time.Duration
	go func() {
		defer close(done)
		elapsed, err = runSweep(t, parallel, ctx, parts, sp, g)
	}()
	for range wait {
		<-started
	}
	synctest.Wait()
	fact()
	synctest.Wait()
	<-done
	origin, _ := terminalOf(err)
	return origin, elapsed, err
}

// sweepRendezvous is sweepTrigger for a row whose parts report on their own
// channel before the test releases them by hand: the fact runs only after the
// required starts were seen and the dispatcher settled.
func sweepRendezvous(t *testing.T, wait, parallel int, ctx context.Context, parts *fakeMultipart, sp *spool.Spool, g *growth,
	settled <-chan struct{}, fact func()) (terminalOrigin, time.Duration, error) {
	t.Helper()
	done := make(chan struct{})
	var err error
	var elapsed time.Duration
	go func() {
		defer close(done)
		elapsed, err = runSweep(t, parallel, ctx, parts, sp, g)
	}()
	for range wait {
		<-settled
	}
	fact()
	synctest.Wait()
	<-done
	origin, _ := terminalOf(err)
	return origin, elapsed, err
}

// runSweep drives one uploadParts over a session the fake really holds and
// reports its cause and wall time.
func runSweep(t *testing.T, parallel int, ctx context.Context, parts *fakeMultipart,
	sp *spool.Spool, g *growth) (time.Duration, error) {
	t.Helper()
	session, err := parts.BeginMultipart(ctx, storage.PutRequest{Key: "k", Size: 1 << 30})
	if err != nil {
		t.Fatal(err)
	}
	e := &Engine{retry: retryPolicy{attempts: 1}}
	st := &storageRunState{clock: storageClock{budget: time.Second}}
	start := time.Now()
	_, err = e.uploadParts(ctx, st, parts, sp, &followSource{g: g}, session, "g/n", parallel)
	return time.Since(start), err
}

// TestSourceFailsWithSaturatedDispatcher (M1): every slot is busy when the
// producer fails. The slot wait wakes on the growth's notification, records the
// producer's failure and stops the parts at once — the dispatcher does not wait
// for a slot to free or for the storage budget to expire.
func TestSourceFailsWithSaturatedDispatcher(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sp, err := spool.New(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		file := covSpoolFile(t, sp, []byte("0123456789abcd"))
		g := newGrowth()
		g.begin(file)
		g.advance(14)
		started := make(chan struct{}, 2)
		parts := newFakeMultipart(4)
		parts.partDelay = func(_ int32, ctx context.Context) error {
			started <- struct{}{}
			<-ctx.Done()
			return ctx.Err()
		}
		e := &Engine{retry: retryPolicy{attempts: 1}}
		st := &storageRunState{clock: storageClock{budget: time.Second}}
		done := make(chan error, 1)
		go func() {
			_, err := e.uploadParts(context.Background(), st, parts, sp, &followSource{g: g},
				storage.MultipartSession{Key: "k", UploadID: "u", PartSize: 4}, "g/n", 2)
			done <- err
		}()
		<-started
		<-started
		synctest.Wait()
		failure := errors.New("producer failed while all slots were occupied")
		start := time.Now()
		g.fail(failure)
		if err := sp.Remove(file); err != nil {
			t.Fatal(err)
		}
		got := <-done
		origin, _ := terminalOf(got)
		if elapsed := time.Since(start); elapsed != 0 || origin != originSource || !errors.Is(got, failure) {
			t.Fatalf("elapsed=%s origin=%d got=%v; want immediate source failure", elapsed, origin, got)
		}
	})
}

// TestRecoveredProducerRetainsItsStorageCopy (M1): the producer fails while
// every part slot is busy and recovers on its retry. The wake on the growth
// notification lets the upload end through the producer's failure at once, so
// the fallback uploads the recovered file: the storage is verified, two
// sessions were opened, and the abort ran after the join.
func TestRecoveredProducerRetainsItsStorageCopy(t *testing.T) {
	for _, parallel := range []int{1, 2, 4} {
		t.Run(fmt.Sprintf("P%d", parallel), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				size := 4*parallel + 8
				w := newWorld(t)
				parts := addMultipart(w, "a_parts", 4)
				w.extraStorages["b_plain"] = storagetest.NewFake()
				cfg := overlapConfig(t, twoWayStorages())
				cfg["storages"].(map[string]any)["a_parts"].(map[string]any)["call_parallelism"] = parallel
				st := cfg["storages"].(map[string]any)["a_parts"].(map[string]any)
				st["time_budget"] = "1s"

				started := make(chan struct{}, parallel)
				release := make(chan struct{})
				var once sync.Once
				parts.partDelay = func(_ int32, ctx context.Context) error {
					if parts.sessionCount() > 1 {
						return nil // the fallback uploads the recovered file normally
					}
					if len(started) < parallel {
						started <- struct{}{}
					}
					<-ctx.Done()
					return ctx.Err()
				}
				src := &benchSource{
					chunks: func(int) [][]byte { return [][]byte{bytes.Repeat([]byte("x"), size)} },
					err: func(attempt int) error {
						if attempt == 1 {
							return errors.New("producer interrupted")
						}
						return nil
					},
					gateAfter: 1, release: release,
				}
				w.extraSources["vpn"] = src
				go func() {
					for range parallel {
						<-started
					}
					once.Do(func() { close(release) })
				}()
				start := time.Now()
				rep, err := w.newEngine(t, cfg).Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
				if err != nil {
					t.Fatal(err)
				}
				if elapsed := time.Since(start); elapsed >= time.Second {
					t.Fatalf("the upload ended after %s, want the producer's failure to stop it at once", elapsed)
				}
				if got := storageResult(rep, "a_parts"); got.Outcome != model.StorageVerified {
					t.Fatalf("a_parts = %+v, want the recovered file uploaded", got)
				}
				if got := parts.sessionCount(); got != 2 {
					t.Fatalf("sessions = %d, want the failed attempt and the fallback", got)
				}
				_, aborts, _, _ := parts.stats()
				if aborts != 1 {
					t.Fatalf("aborts = %d, want the one unfinished upload aborted", aborts)
				}
				if _, abortInFlight, _, _ := parts.flightStats(); len(abortInFlight) != 1 || abortInFlight[0] != 0 {
					t.Fatalf("the abort ran with %v parts in flight, want none", abortInFlight)
				}
			})
		})
	}
}

// TestOverlapFinishFailureFailsTheStorage: an overlapping upload whose Complete
// commits nothing is caught by the Stat after it, aborted once, and reported as
// that storage's failure — the follow path's finish, not the whole-object Put.
func TestOverlapFinishFailureFailsTheStorage(t *testing.T) {
	w := newWorld(t)
	parts := addMultipart(w, "a_parts", 4)
	parts.completeDrop = true
	src := &benchSource{
		chunks:    func(int) [][]byte { return [][]byte{[]byte("0123456789")} },
		gateAfter: 1, release: make(chan struct{}),
	}
	w.extraSources["vpn"] = src
	parts.partHook = func(number int32) {
		if number == 1 {
			select {
			case <-src.release:
			default:
				close(src.release)
			}
		}
	}
	cfg := overlapConfig(t, twoWayStorages())
	cfg["storages"].(map[string]any)["a_parts"].(map[string]any)["call_parallelism"] = 2
	rep, err := w.newEngine(t, cfg).Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Artifacts[0].Outcome != model.ArtifactOK {
		t.Fatalf("the artifact = %+v, want the plain storage's copy", rep.Artifacts[0])
	}
	if r := storageResult(rep, "a_parts"); r.Outcome != model.StorageFailed {
		t.Fatalf("a_parts = %+v, want the failed copy", r)
	}
	_, aborts, _, _ := parts.stats()
	if aborts != 1 {
		t.Fatalf("aborts = %d, want the dropped completion aborted once", aborts)
	}
	if _, ok := parts.Bytes(rep.Artifacts[0].Key); ok {
		t.Fatalf("the dropped completion left an object: %v", parts.Keys())
	}
}
