package s3_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/storage"
	"github.com/vi-latyshev/go-cluster-backup/internal/storage/storagetest"
)

const testPartBytes = 5 << 20

// multipartStorage builds the plugin with a part size of testPartBytes against
// the fake endpoint.
func multipartStorage(t *testing.T) (storage.Storage, *fakeS3Server) {
	t.Helper()
	return newStorage(t, func(raw map[string]any) { raw["part_bytes"] = testPartBytes })
}

func capabilityOf(t *testing.T, s storage.Storage) storage.MultipartUploader {
	t.Helper()
	mu, ok := s.(storage.MultipartUploader)
	if !ok {
		t.Fatal("the s3 plugin does not implement storage.MultipartUploader")
	}
	return mu
}

func inventoryOf(t *testing.T, s storage.Storage) storage.MultipartInventory {
	t.Helper()
	inv, ok := s.(storage.MultipartInventory)
	if !ok {
		t.Fatal("the s3 plugin does not implement storage.MultipartInventory")
	}
	return inv
}

func beginUpload(t *testing.T, mu storage.MultipartUploader, key string, size int64) storage.MultipartSession {
	t.Helper()
	session, err := mu.BeginMultipart(context.Background(), storage.PutRequest{Key: key, Size: size})
	if err != nil {
		t.Fatalf("BeginMultipart: %v", err)
	}
	if session.UploadID == "" || session.PartSize != testPartBytes {
		t.Fatalf("session %+v, want an upload id and part size %d", session, int64(testPartBytes))
	}
	return session
}

func partBytes(number int32, body []byte) storage.Part {
	return storage.Part{Number: number, Size: int64(len(body)), Body: bytes.NewReader(body)}
}

// TestMultipartConformance runs the capability's conformance checks against the
// fake endpoint.
func TestMultipartConformance(t *testing.T) {
	f := newFakeS3(t, testBucket)
	storagetest.RunMultipart(t, testPartBytes, func(t *testing.T) storage.Storage {
		return newStorageFrom(t, baseConfig(t, f, func(raw map[string]any) { raw["part_bytes"] = testPartBytes }))
	})
}

// TestMultipartSmallObjectIsThePluginsUnsupported: an object that fits one part
// is answered with errors.ErrUnsupported and no request at all — the engine then
// stores it with a single Put.
func TestMultipartSmallObjectIsThePluginsUnsupported(t *testing.T) {
	s, f := multipartStorage(t)
	mu := capabilityOf(t, s)
	_, err := mu.BeginMultipart(context.Background(), storage.PutRequest{Key: "small.bin", Size: testPartBytes})
	if !errors.Is(err, errors.ErrUnsupported) {
		t.Fatalf("an object of exactly one part: %v", err)
	}
	if f.Count("create") != 0 {
		t.Errorf("the unsupported answer cost %d create requests, want none", f.Count("create"))
	}
}

// TestMultipartOneRequestPerCall: the engine drives the upload, so every call is
// exactly one request — and a failing part is NOT retried inside the plugin, the
// way the SDK's disabled retryer promises.
func TestMultipartOneRequestPerCall(t *testing.T) {
	ctx := context.Background()
	s, f := multipartStorage(t)
	mu := capabilityOf(t, s)
	body := newPattern(t, int(testPartBytes)+7)
	session := beginUpload(t, mu, "one-per-call.bin", int64(len(body)))
	if got := f.Count("create"); got != 1 {
		t.Fatalf("create requests after BeginMultipart: %d, want 1", got)
	}

	// A part that fails: one request, a transient error, no abort from the plugin
	// (cleaning up is the engine's decision, made once).
	f.failNext["part"] = 500
	if _, err := mu.PutPart(ctx, session, partBytes(1, body[:testPartBytes])); err == nil {
		t.Fatal("an injected 500 on a part was accepted")
	} else if class, _ := plugin.ClassOf(err); class != plugin.ClassTransient {
		t.Fatalf("class %q, want transient: %v", class, err)
	}
	if got := f.Count("part"); got != 1 {
		t.Fatalf("part requests for a failed part: %d, want exactly 1", got)
	}
	if got := f.Count("abort"); got != 0 {
		t.Fatalf("the plugin aborted on its own: %d abort requests", got)
	}

	// The engine's retry sends the same number again: the part lands, and the
	// remaining part with it, in two more requests.
	first, err := mu.PutPart(ctx, session, partBytes(1, body[:testPartBytes]))
	if err != nil {
		t.Fatalf("re-sent part: %v", err)
	}
	second, err := mu.PutPart(ctx, session, partBytes(2, body[testPartBytes:]))
	if err != nil {
		t.Fatalf("second part: %v", err)
	}
	if err := mu.CompleteMultipart(ctx, session, []storage.PartResult{first, second}); err != nil {
		t.Fatalf("CompleteMultipart: %v", err)
	}
	for op, want := range map[string]int{"part": 3, "complete": 1, "abort": 0} {
		if got := f.Count(op); got != want {
			t.Errorf("%s requests: got %d, want %d", op, got, want)
		}
	}
	if got := f.OpenUploads(); len(got) != 0 {
		t.Errorf("uploads left open: %v", got)
	}
	stored, ok := f.objectBody("root/one-per-call.bin")
	if !ok || !bytes.Equal(stored, body) {
		t.Errorf("stored object differs: %d bytes, ok=%v", len(stored), ok)
	}
}

// TestMultipartBeginFailureIsOneRequest: a create that fails is transient and is
// not retried inside the plugin.
func TestMultipartBeginFailureIsOneRequest(t *testing.T) {
	s, f := multipartStorage(t)
	mu := capabilityOf(t, s)
	f.failAlways["create"] = 500
	_, err := mu.BeginMultipart(context.Background(), storage.PutRequest{Key: "big.bin", Size: 2 * testPartBytes})
	if err == nil || classOf(err) != plugin.ClassTransient {
		t.Fatalf("create failure: got %v", err)
	}
	if got := f.Count("create"); got != 1 {
		t.Errorf("create requests: got %d, want exactly 1", got)
	}
}

// TestMultipartCompleteWithoutCommitIsAnError: the service answers 200 and
// commits nothing, so nothing but an error may come back — the engine verifies
// the object afterwards, and a 200 that committed nothing must never read as a
// stored object.
func TestMultipartCompleteWithoutCommitIsAnError(t *testing.T) {
	ctx := context.Background()
	s, f := multipartStorage(t)
	mu := capabilityOf(t, s)
	f.completeDrop = true
	body := newPattern(t, int(testPartBytes)+7)
	session := beginUpload(t, mu, "dropped.bin", int64(len(body)))
	first, err := mu.PutPart(ctx, session, partBytes(1, body[:testPartBytes]))
	if err != nil {
		t.Fatal(err)
	}
	second, err := mu.PutPart(ctx, session, partBytes(2, body[testPartBytes:]))
	if err != nil {
		t.Fatal(err)
	}
	err = mu.CompleteMultipart(ctx, session, []storage.PartResult{first, second})
	if err == nil {
		t.Fatal("a 200 without a completion ETag was accepted as a commit")
	}
	if class := classOf(err); class != plugin.ClassTransient {
		t.Errorf("class %q, want transient: %v", class, err)
	}
	if _, err := s.Stat(ctx, "dropped.bin"); classOf(err) != plugin.ClassNotFound {
		t.Errorf("the dropped upload became visible: %v", err)
	}
	if err := mu.AbortMultipart(ctx, session); err != nil {
		t.Fatalf("abort after a dropped completion: %v", err)
	}
	if got := f.OpenUploads(); len(got) != 0 {
		t.Errorf("uploads left open: %v", got)
	}
}

// TestMultipartAbortRunsOnItsOwnContext: the engine aborts when the upload's own
// context is already dead, so the call must go out anyway — one attempt, and an
// upload already gone is nothing left to clean rather than an error.
func TestMultipartAbortRunsOnItsOwnContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	s, f := multipartStorage(t)
	mu := capabilityOf(t, s)
	body := newPattern(t, int(testPartBytes)+7)
	session := beginUpload(t, mu, "aborted.bin", int64(len(body)))
	if _, err := mu.PutPart(ctx, session, partBytes(1, body[:testPartBytes])); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := mu.AbortMultipart(ctx, session); err != nil {
		t.Fatalf("abort with a cancelled context: %v", err)
	}
	if got := f.Count("abort"); got != 1 {
		t.Fatalf("abort requests: got %d, want 1", got)
	}
	if got := f.OpenUploads(); len(got) != 0 {
		t.Fatalf("uploads left open: %v", got)
	}
	if err := mu.AbortMultipart(ctx, session); err != nil {
		t.Fatalf("second abort of an upload already gone: %v", err)
	}
	if got := f.Count("abort"); got != 2 {
		t.Errorf("abort requests after the second abort: got %d, want 2 (one attempt each)", got)
	}
}

// TestCancelMidPartReturnsPromptly: a part in flight ends with the context, and
// the plugin does not sleep or retry on the way out.
func TestCancelMidPartReturnsPromptly(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, f := multipartStorage(t)
	mu := capabilityOf(t, s)
	body := newPattern(t, int(testPartBytes)+7)
	session := beginUpload(t, mu, "cancelled.bin", int64(len(body)))
	f.hang["part"] = true
	done := make(chan error, 1)
	go func() {
		_, err := mu.PutPart(ctx, session, partBytes(1, body[:testPartBytes]))
		done <- err
	}()
	waitFor(t, func() bool { return f.Count("part") >= 1 }, 5*time.Second)
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("a part with a cancelled context returned success")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("PutPart did not return after its context was cancelled")
	}
}

// TestMultipartPartsComeFromTheFile: the engine hands over a section of its spool
// file — a seekable reader over a bounded window — and that is all the plugin
// needs: no buffer of the part is allocated here.
func TestMultipartPartsComeFromTheFile(t *testing.T) {
	ctx := context.Background()
	s, _ := multipartStorage(t)
	mu := capabilityOf(t, s)
	body := newPattern(t, int(testPartBytes)+7)
	f, err := os.CreateTemp(t.TempDir(), "spool-*")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	if _, err := f.Write(body); err != nil {
		t.Fatal(err)
	}
	session := beginUpload(t, mu, "from-file.bin", int64(len(body)))
	var results []storage.PartResult
	for offset, number := int64(0), int32(1); offset < int64(len(body)); number++ {
		size := min(int64(testPartBytes), int64(len(body))-offset)
		res, err := mu.PutPart(ctx, session, storage.Part{
			Number: number, Size: size, Body: io.NewSectionReader(f, offset, size),
		})
		if err != nil {
			t.Fatalf("PutPart(%d) from a file section: %v", number, err)
		}
		results = append(results, res)
		offset += size
	}
	if err := mu.CompleteMultipart(ctx, session, results); err != nil {
		t.Fatalf("CompleteMultipart: %v", err)
	}
	var got bytes.Buffer
	if err := s.Get(ctx, "from-file.bin", &got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Bytes(), body) {
		t.Errorf("read back %d bytes, want %d equal ones", got.Len(), len(body))
	}
}

// TestMultipartRefusesABadPartWithoutARequest: a part number or size outside what
// the capability promises is the caller's bug, and costs no request.
func TestMultipartRefusesABadPartWithoutARequest(t *testing.T) {
	ctx := context.Background()
	s, f := multipartStorage(t)
	mu := capabilityOf(t, s)
	session := beginUpload(t, mu, "bad.bin", 2*testPartBytes)
	if _, err := mu.PutPart(ctx, session, partBytes(0, []byte("x"))); classOf(err) != plugin.ClassPermanent {
		t.Fatalf("part number 0: %v", err)
	}
	if _, err := mu.PutPart(ctx, session, storage.Part{Number: 1, Size: 0, Body: strings.NewReader("")}); classOf(err) != plugin.ClassPermanent {
		t.Fatalf("an empty part: %v", err)
	}
	if _, err := mu.PutPart(ctx, session, partBytes(1, make([]byte, testPartBytes+1))); classOf(err) != plugin.ClassPermanent {
		t.Fatalf("a part above part_bytes: %v", err)
	}
	if err := mu.CompleteMultipart(ctx, session, nil); classOf(err) != plugin.ClassPermanent {
		t.Fatalf("a completion with no parts: %v", err)
	}
	if got := f.Count("part"); got != 0 {
		t.Errorf("a refused part cost %d requests, want none", got)
	}
}

// TestPutRefusesAnObjectAboveOneRequest: the whole-object path never becomes a
// multipart upload of its own — an object above the endpoint's single-request
// limit is the engine's multipart capability to upload.
func TestPutRefusesAnObjectAboveOneRequest(t *testing.T) {
	ctx := context.Background()
	s, f := multipartStorage(t)
	_, err := s.Put(ctx, storage.PutRequest{Key: "huge.bin", Size: 5<<30 + 1}, bytes.NewReader(nil))
	if err == nil || classOf(err) != plugin.ClassPermanent {
		t.Fatalf("an object above one request: %v", err)
	}
	if !strings.Contains(err.Error(), "multipart") {
		t.Errorf("the refusal does not name the multipart path: %v", err)
	}
	if f.Count("put") != 0 || f.Count("create") != 0 {
		t.Errorf("a refused object reached the service: %+v", f.counts)
	}
}

// TestBeginRefusesMorePartsThanTheServiceAccepts: at part_bytes per part, an
// object needing more than the service's 10000 parts could never commit — the
// refusal is permanent and costs no request, instead of 160 GiB of parts that end
// in a failed commit. Mutant: without the check the refused object reaches the
// service. The boundary is the service's: exactly 10000 parts is accepted.
func TestBeginRefusesMorePartsThanTheServiceAccepts(t *testing.T) {
	ctx := context.Background()
	s, f := multipartStorage(t)
	mu := capabilityOf(t, s)
	over := int64(10000)*testPartBytes + 1
	_, err := mu.BeginMultipart(ctx, storage.PutRequest{Key: "too-many-parts.bin", Size: over})
	if classOf(err) != plugin.ClassPermanent {
		t.Fatalf("an object needing more than 10000 parts: %v", err)
	}
	if !strings.Contains(err.Error(), "10000 parts") {
		t.Errorf("the refusal does not name the part limit: %v", err)
	}
	if got := f.Count("create"); got != 0 {
		t.Errorf("the refusal cost %d create requests, want none", got)
	}
	session, err := mu.BeginMultipart(ctx, storage.PutRequest{Key: "at-limit.bin", Size: int64(10000) * testPartBytes})
	if err != nil {
		t.Fatalf("an object of exactly 10000 parts: %v", err)
	}
	if got := f.Count("create"); got != 1 {
		t.Errorf("create requests: got %d, want exactly 1", got)
	}
	if err := mu.AbortMultipart(ctx, session); err != nil {
		t.Fatalf("abort: %v", err)
	}
}

// TestAbortIsBoundedByItsOwnShortBudget: the abort runs after the upload's own
// context died, on a context of its own — and that budget is request_timeout
// divided down, because an abort fires exactly when the run's ending needs its
// seconds. Mutant: giving the abort the whole request_timeout lets a stalling
// endpoint hold the run for it.
func TestAbortIsBoundedByItsOwnShortBudget(t *testing.T) {
	ctx := context.Background()
	s, f := newStorage(t, func(raw map[string]any) {
		raw["part_bytes"] = testPartBytes
		raw["request_timeout"] = "10s"
	})
	mu := capabilityOf(t, s)
	body := newPattern(t, int(testPartBytes)+7)
	session := beginUpload(t, mu, "stalling-abort.bin", int64(len(body)))
	if _, err := mu.PutPart(ctx, session, partBytes(1, body[:testPartBytes])); err != nil {
		t.Fatal(err)
	}
	f.hang["abort"] = true
	start := time.Now()
	done := make(chan error, 1)
	go func() { done <- mu.AbortMultipart(ctx, session) }()
	// The one attempt must have reached the endpoint before the bound is
	// judged: on a loaded machine the request can arrive after the abort's own
	// budget expired, and the count is what proves the attempt was made.
	waitFor(t, func() bool { return f.Count("abort") >= 1 }, 30*time.Second)
	err := <-done
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("an abort against a stalling endpoint returned success")
	}
	if elapsed > 4*time.Second {
		t.Fatalf("the abort took %s; with a 10s request timeout it must come back far sooner", elapsed)
	}
	if got := f.Count("abort"); got != 1 {
		t.Errorf("abort requests: got %d, want exactly 1", got)
	}
}

// TestStaleMultipartListsOnlyItsOwnRootAndCutoff: the sweep lists the open
// uploads of this storage's own root that started before the cutoff. An upload
// another deployment left in the same bucket is neither listed nor touched, and
// the service is asked for this root's prefix — the confinement is asserted on
// the request, not on the answer alone. Mutant: dropping the prefix from the
// request lists the foreign upload.
func TestStaleMultipartListsOnlyItsOwnRootAndCutoff(t *testing.T) {
	ctx := context.Background()
	s, f := multipartStorage(t)
	mu := capabilityOf(t, s)
	fresh := beginUpload(t, mu, "stale-fresh.bin", 2*testPartBytes)
	old := beginUpload(t, mu, "stale-old.bin", 2*testPartBytes)
	f.backdateUpload(old.UploadID, time.Now().Add(-3*time.Hour))
	f.seedUpload("elsewhere/somebody-elses.bin")

	stale, err := mu.StaleMultipart(ctx, time.Now().Add(-time.Hour))
	if err != nil {
		t.Fatalf("StaleMultipart: %v", err)
	}
	if len(stale) != 1 || stale[0].Key != "stale-old.bin" || stale[0].UploadID != old.UploadID {
		t.Fatalf("stale = %+v, want only the upload started hours ago", stale)
	}
	if stale[0].Initiated.IsZero() {
		t.Error("the listed upload carries no initiation time")
	}
	if f.sawUploadPrefix != "root/" {
		t.Errorf("the service was asked for prefix %q, want this storage's own %q", f.sawUploadPrefix, "root/")
	}
	if err := mu.AbortMultipart(ctx, storage.MultipartSession{Key: stale[0].Key, UploadID: stale[0].UploadID}); err != nil {
		t.Fatalf("aborting the listed upload: %v", err)
	}
	open := f.OpenUploads()
	if len(open) != 2 {
		t.Fatalf("open uploads after the sweep of one: %v, want the fresh and the foreign one", open)
	}
	for _, id := range open {
		if id == old.UploadID {
			t.Fatalf("the swept upload is still open: %v", open)
		}
	}
	if fresh.UploadID == "" {
		t.Error("the fresh upload lost its session")
	}
}

// TestStaleMultipartPages: the service answers one page per call, and the
// listing must carry every page's uploads, not just the first. Mutant: reading
// only the first page of a count larger than one page.
func TestStaleMultipartPages(t *testing.T) {
	ctx := context.Background()
	s, f := multipartStorage(t)
	mu := capabilityOf(t, s)
	first := beginUpload(t, mu, "stale-a.bin", 2*testPartBytes)
	beginUpload(t, mu, "stale-b.bin", 2*testPartBytes)
	f.uploadsPageSize = 1

	stale, err := mu.StaleMultipart(ctx, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("StaleMultipart: %v", err)
	}
	if len(stale) != 2 {
		t.Fatalf("stale = %+v, want both uploads across two pages", stale)
	}
	if stale[0].Key != "stale-a.bin" || stale[1].Key != "stale-b.bin" {
		t.Fatalf("stale = %+v, want them in key order", stale)
	}
	if got := f.Count("listuploads"); got != 2 {
		t.Errorf("listuploads requests: got %d, want 2 (one page each)", got)
	}
	if first.UploadID == "" {
		t.Error("the first upload lost its session")
	}
}

// TestStaleMultipartSkipsUndatedUploads: an upload the service does not date is
// never called stale — a missing Initiated decodes as the zero time, which is
// "before every cutoff" and would take an upload a run may be writing right now.
// Mutant: without the skip the undated upload is listed and, by the engine,
// aborted.
func TestStaleMultipartSkipsUndatedUploads(t *testing.T) {
	ctx := context.Background()
	s, f := multipartStorage(t)
	mu := capabilityOf(t, s)
	undated := beginUpload(t, mu, "undated.bin", 2*testPartBytes)
	dated := beginUpload(t, mu, "dated.bin", 2*testPartBytes)
	f.backdateUpload(dated.UploadID, time.Now().Add(-3*time.Hour))
	f.dropInitiated = map[string]bool{undated.UploadID: true}

	stale, err := mu.StaleMultipart(ctx, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("StaleMultipart: %v", err)
	}
	if len(stale) != 1 || stale[0].Key != "dated.bin" {
		t.Fatalf("stale = %+v, want only the dated upload", stale)
	}
}

// TestStaleMultipartStopsOnNonAdvancingMarkers: markers that do not move mean
// the pages will never end; the sweep stops with an error instead of spending
// the storage's budget on the same page forever. Mutant: without the guard the
// call never returns and the test's own deadline cuts it.
func TestStaleMultipartStopsOnNonAdvancingMarkers(t *testing.T) {
	ctx := context.Background()
	s, f := multipartStorage(t)
	mu := capabilityOf(t, s)
	beginUpload(t, mu, "stuck.bin", 2*testPartBytes)
	f.stuckUploads = true

	_, err := mu.StaleMultipart(ctx, time.Now().Add(time.Hour))
	if err == nil {
		t.Fatal("a listing whose markers never advance was accepted")
	}
	if !strings.Contains(err.Error(), "markers did not advance") {
		t.Fatalf("the refusal does not name the cause: %v", err)
	}
	if got := f.Count("listuploads"); got != 1 {
		t.Errorf("listuploads requests: got %d, want 1 — the page that could not advance", got)
	}
}

func TestMultipartCanceledContexts(t *testing.T) {
	s, _ := multipartStorage(t)
	mu := capabilityOf(t, s)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := mu.BeginMultipart(ctx, storage.PutRequest{Key: "k", Size: 2 * testPartBytes}); err == nil {
		t.Error("BeginMultipart with a cancelled context")
	}
	if _, err := mu.PutPart(ctx, storage.MultipartSession{Key: "k", UploadID: "u"}, partBytes(1, []byte("x"))); err == nil {
		t.Error("PutPart with a cancelled context")
	}
	if err := mu.CompleteMultipart(ctx, storage.MultipartSession{Key: "k", UploadID: "u"},
		[]storage.PartResult{{Number: 1, ETag: "e"}}); err == nil {
		t.Error("CompleteMultipart with a cancelled context")
	}
	if _, err := mu.StaleMultipart(ctx, time.Now()); err == nil {
		t.Error("StaleMultipart with a cancelled context")
	}
	if _, err := inventoryOf(t, s).Inventory(ctx); err == nil {
		t.Error("Inventory with a cancelled context")
	}
}

// TestMultipartOperationsReportAnUnreadableSecret: every multipart call builds
// its client per attempt, so an unreadable credential file fails that call.
func TestMultipartOperationsReportAnUnreadableSecret(t *testing.T) {
	s, _ := newStorage(t, func(raw map[string]any) {
		raw["part_bytes"] = testPartBytes
		raw["secret_access_key"] = map[string]any{"file": filepath.Join(t.TempDir(), "absent")}
	})
	mu := capabilityOf(t, s)
	ctx := context.Background()
	if _, err := mu.BeginMultipart(ctx, storage.PutRequest{Key: "k", Size: 2 * testPartBytes}); err == nil {
		t.Error("BeginMultipart with an unreadable secret")
	}
	if _, err := mu.PutPart(ctx, storage.MultipartSession{Key: "k", UploadID: "u"}, partBytes(1, []byte("x"))); err == nil {
		t.Error("PutPart with an unreadable secret")
	}
	if err := mu.CompleteMultipart(ctx, storage.MultipartSession{Key: "k", UploadID: "u"},
		[]storage.PartResult{{Number: 1, ETag: "e"}}); err == nil {
		t.Error("CompleteMultipart with an unreadable secret")
	}
	if err := mu.AbortMultipart(ctx, storage.MultipartSession{Key: "k", UploadID: "u"}); err == nil {
		t.Error("AbortMultipart with an unreadable secret")
	}
	if _, err := mu.StaleMultipart(ctx, time.Now()); err == nil {
		t.Error("StaleMultipart with an unreadable secret")
	}
	if _, err := inventoryOf(t, s).Inventory(ctx); err == nil {
		t.Error("Inventory with an unreadable secret")
	}
}

// TestBeginRefusesAnObjectAboveTheEndpointLimit: the endpoint's own maximum is
// refused before a request, like the single-request path.
func TestBeginRefusesAnObjectAboveTheEndpointLimit(t *testing.T) {
	s, f := multipartStorage(t)
	mu := capabilityOf(t, s)
	_, err := mu.BeginMultipart(context.Background(), storage.PutRequest{Key: "huge.bin", Size: (5 << 40) + 1})
	if err == nil || classOf(err) != plugin.ClassPermanent {
		t.Fatalf("an object above the endpoint limit: %v", err)
	}
	if got := f.Count("create"); got != 0 {
		t.Errorf("the refusal cost %d create requests, want none", got)
	}
}

func TestBeginWithoutAnUploadIDIsTransient(t *testing.T) {
	s, f := multipartStorage(t)
	mu := capabilityOf(t, s)
	f.createNoID = true
	_, err := mu.BeginMultipart(context.Background(), storage.PutRequest{Key: "big.bin", Size: 2 * testPartBytes})
	if err == nil || classOf(err) != plugin.ClassTransient {
		t.Fatalf("a create answer without an upload id: %v", err)
	}
	if got := f.Count("create"); got != 1 {
		t.Errorf("create requests: got %d, want exactly 1", got)
	}
}

func TestPutPartWithoutAnETagIsTransient(t *testing.T) {
	ctx := context.Background()
	s, f := multipartStorage(t)
	mu := capabilityOf(t, s)
	f.partNoETag = true
	session := beginUpload(t, mu, "noetag-part.bin", 2*testPartBytes)
	_, err := mu.PutPart(ctx, session, partBytes(1, newPattern(t, int(testPartBytes))))
	if err == nil || classOf(err) != plugin.ClassTransient {
		t.Fatalf("a part answered without an ETag: %v", err)
	}
}

func TestAbortWithoutASessionIDIsPermanent(t *testing.T) {
	s, f := multipartStorage(t)
	mu := capabilityOf(t, s)
	err := mu.AbortMultipart(context.Background(), storage.MultipartSession{Key: "k"})
	if err == nil || classOf(err) != plugin.ClassPermanent {
		t.Fatalf("an abort without an upload id: %v", err)
	}
	if got := f.Count("abort"); got != 0 {
		t.Errorf("the refusal cost %d abort requests, want none", got)
	}
}

// TestInventoryListsEveryOpenUpload: the full inventory is separate from the
// sweep's cutoff selection — an upload of any age under this storage's root is
// reported, because its parts make the storage's occupancy unknown.
func TestInventoryListsEveryOpenUpload(t *testing.T) {
	ctx := context.Background()
	s, f := multipartStorage(t)
	mu := capabilityOf(t, s)
	inv, ok := s.(storage.MultipartInventory)
	if !ok {
		t.Fatal("the s3 plugin does not implement storage.MultipartInventory")
	}
	old := beginUpload(t, mu, "inventory-old.bin", 2*testPartBytes)
	fresh := beginUpload(t, mu, "inventory-fresh.bin", 2*testPartBytes)
	f.backdateUpload(old.UploadID, time.Now().Add(-3*time.Hour))
	got, err := inv.Inventory(ctx)
	if err != nil {
		t.Fatalf("Inventory: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("inventory = %+v, want both uploads of any age", got)
	}
	if fresh.UploadID == "" {
		t.Error("the fresh upload lost its session")
	}
}

func TestStaleMultipartFailureIsTransient(t *testing.T) {
	s, f := multipartStorage(t)
	mu := capabilityOf(t, s)
	f.failAlways["listuploads"] = 500
	if _, err := mu.StaleMultipart(context.Background(), time.Now()); err == nil || classOf(err) != plugin.ClassTransient {
		t.Fatalf("a failed upload listing: %v", err)
	}
}

func TestInventorySkipsUploadsOutsideTheRoot(t *testing.T) {
	s, f := multipartStorage(t)
	mu := capabilityOf(t, s)
	beginUpload(t, mu, "own.bin", 2*testPartBytes)
	f.uploadsForeign = true
	stale, err := mu.StaleMultipart(context.Background(), time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("StaleMultipart: %v", err)
	}
	for _, u := range stale {
		if strings.Contains(u.Key, "foreign") {
			t.Errorf("an upload outside the storage root was listed: %+v", u)
		}
	}
}
