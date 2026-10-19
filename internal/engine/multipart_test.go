package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
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
	partDelay func(ctx context.Context) error

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
	f.mu.Lock()
	defer f.mu.Unlock()
	f.begins++
	if req.Size <= f.partSize {
		return storage.MultipartSession{}, fmt.Errorf("object %s fits one part: %w", req.Key, errors.ErrUnsupported)
	}
	f.next++
	id := fmt.Sprintf("upload-%d", f.next)
	f.uploads[id] = &fakeMPUpload{key: req.Key, parts: map[int32][]byte{}}
	return storage.MultipartSession{Key: req.Key, UploadID: id, PartSize: f.partSize}, nil
}

func (f *fakeMultipart) PutPart(ctx context.Context, session storage.MultipartSession, part storage.Part) (storage.PartResult, error) {
	if f.partDelay != nil {
		if err := f.partDelay(ctx); err != nil {
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
	f.mu.Lock()
	defer f.mu.Unlock()
	f.aborts++
	f.abortedKeys = append(f.abortedKeys, session.Key)
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

func (f *fakeMultipart) sweepStats() (lists int, aborted []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.staleLists, append([]string(nil), f.abortedKeys...)
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
	if begins != 1 || completes != 1 || aborts != 0 {
		t.Fatalf("begins=%d completes=%d aborts=%d, want one begin, one complete, no abort", begins, completes, aborts)
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
	parts.partDelay = func(ctx context.Context) error {
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
	if begins != 1 {
		t.Fatalf("begins = %d, want the one probe that answered unsupported", begins)
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
	w := newWorld(t)
	parts := newFakeMultipart(4)
	parts.partDelay = func(ctx context.Context) error {
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
	_, err = e.uploadParts(context.Background(), st, mu, sp, pa,
		storage.MultipartSession{Key: "k", UploadID: "u", PartSize: 4})
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
	pa := producedArtifact{artifact: model.Artifact{Group: "g", Name: "n"}, file: "f.bin", bytes: 10}
	_, err = e.uploadParts(context.Background(), st, z, sp, pa, session)
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
	err := e.abortMultipart(context.Background(), z, storage.MultipartSession{Key: "k", UploadID: "u"}, "k")
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
