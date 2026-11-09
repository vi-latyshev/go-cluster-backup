package yandexdisk_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"encoding/json/jsontext"
	json "encoding/json/v2"

	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage/storagetest"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage/yandexdisk"
)

func testRegistry(t testing.TB) *instances.Registry {
	t.Helper()
	lr := loader.New()
	r := instances.NewRegistry()
	if err := loader.Add(lr, yandexdisk.Spec, func(lr *loader.Registry) error {
		return yandexdisk.Apply(lr, r)
	}); err != nil {
		t.Fatal(err)
	}
	return r
}

func writeSecret(t testing.TB, value string) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "secret-*")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(value); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(f.Name(), 0o600); err != nil {
		t.Fatal(err)
	}
	return f.Name()
}

func baseConfig(t testing.TB, f *fakeDiskServer) map[string]any {
	t.Helper()
	return map[string]any{
		"api_url":              f.srv.URL + "/v1/disk",
		"token":                map[string]any{"file": writeSecret(t, "test-token")},
		"root":                 "app:/cluster-backup",
		"permanent_delete":     true,
		"prune_keep_depth":     1,
		"request_timeout":      "5s",
		"commit_poll_interval": "5ms",
		"commit_poll_timeout":  "150ms",
	}
}

func newStorageFrom(t testing.TB, raw map[string]any) storage.Storage {
	t.Helper()
	return newStorageAt(t, raw, 1)
}

func newStorageWithLog(t testing.TB, raw map[string]any, log *slog.Logger) storage.Storage {
	t.Helper()
	return newStorageAt(t, raw, 1, log)
}

// newStorageAt builds one storage through the storage factory with the given
// call_parallelism: the only construction path, which also hands the plugin its
// construction input.
func newStorageAt(t testing.TB, raw map[string]any, parallelism int, log ...*slog.Logger) storage.Storage {
	t.Helper()
	body, err := json.Marshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.DiscardHandler)
	if len(log) > 0 {
		logger = log[0]
	}
	s, err := storage.Build(context.Background(), testRegistry(t), "yandex_disk", body,
		storage.BuildInput{CallParallelism: parallelism}, instances.Deps{Instance: "yandex", Log: logger})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func newStorage(t testing.TB, mutate func(raw map[string]any)) (storage.Storage, *fakeDiskServer) {
	t.Helper()
	f := newFakeDisk(t)
	raw := baseConfig(t, f)
	if mutate != nil {
		mutate(raw)
	}
	return newStorageFrom(t, raw), f
}

func newStorageWithParallelism(t testing.TB, parallelism int) (storage.Storage, *fakeDiskServer) {
	t.Helper()
	f := newFakeDisk(t)
	return newStorageAt(t, baseConfig(t, f), parallelism), f
}

func classOf(err error) pluginerrors.Class {
	class, _ := pluginerrors.ClassOf(err)
	return class
}

func waitFor(t *testing.T, cond func() bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition did not become true in time")
}

// TestGeneralConformance runs the general plugin contract plus the
// storage-owned construction check: the kit builds every example through
// storage.Build with its construction input.
func TestGeneralConformance(t *testing.T) {
	storagetest.CheckSpec(t, testRegistry(t))
}

func TestStorageConformance(t *testing.T) {
	storagetest.Run(t, func(t *testing.T) storage.Storage {
		s, _ := newStorage(t, func(raw map[string]any) { raw["request_timeout"] = "100ms" })
		return s
	}, storagetest.Options{SlowWrite: 300 * time.Millisecond})
}

func TestDescribeTraits(t *testing.T) {
	ctx := context.Background()
	s, _ := newStorage(t, nil)
	tr, err := s.Describe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(tr.Checksums) != 0 {
		t.Errorf("checksums %v: none is verified against the live service, so none may be declared", tr.Checksums)
	}
}

// TestPutToleratesUnverifiedMetadataMD5 pins the deliberate leniency: the
// metadata md5 is reported by Stat but never compared, so a service whose md5
// field means something else cannot fail an upload that landed.
func TestPutToleratesUnverifiedMetadataMD5(t *testing.T) {
	ctx := context.Background()
	s, f := newStorage(t, nil)
	f.badMD5 = true
	body := []byte("landed anyway")
	if _, err := s.Put(ctx, storage.PutRequest{Key: "x.bin", Size: int64(len(body))}, bytes.NewReader(body)); err != nil {
		t.Fatalf("a differing metadata md5 failed the upload: %v", err)
	}
}

// TestPutRequests pins the exact request sequence of one attempt: three folder
// creations for a nested key, one href, one data PUT (with no chunked body),
// and one metadata answer that commits it. Any retry inside the plugin would
// break these counts.
func TestPutRequests(t *testing.T) {
	ctx := context.Background()
	s, f := newStorage(t, nil)
	body := []byte("one attempt")
	if _, err := s.Put(ctx, storage.PutRequest{Key: "a/b/c.bin", Size: int64(len(body))}, bytes.NewReader(body)); err != nil {
		t.Fatal(err)
	}
	for kind, want := range map[string]int{"mkdir": 3, "uploadhref": 1, "putdata": 1, "stat": 1} {
		if got := f.Count(kind); got != want {
			t.Errorf("%s requests: got %d, want %d", kind, got, want)
		}
	}
	if f.sawChunkedBody {
		t.Error("a request body arrived chunked; the declared size must be sent as Content-Length")
	}
	if !f.folders["app:/cluster-backup/a/b"] {
		t.Errorf("intermediate folders were not created: %v", f.folders)
	}
}

func TestFolderCreationFailureIsOneAttempt(t *testing.T) {
	ctx := context.Background()
	s, f := newStorage(t, nil)
	f.failAlways["mkdir"] = http.StatusInternalServerError
	_, err := s.Put(ctx, storage.PutRequest{Key: "x.bin", Size: 1}, strings.NewReader("x"))
	if err == nil {
		t.Fatal("a 500 on folder creation was accepted")
	}
	if class := classOf(err); class != pluginerrors.ClassTransient {
		t.Errorf("class %q, want transient: %v", class, err)
	}
	if got := f.Count("mkdir"); got != 1 {
		t.Errorf("mkdir requests: got %d, want exactly 1 (no retry inside the plugin)", got)
	}
}

func TestHrefIssueFailureIsOneAttempt(t *testing.T) {
	ctx := context.Background()
	s, f := newStorage(t, nil)
	f.failAlways["uploadhref"] = http.StatusInternalServerError
	_, err := s.Put(ctx, storage.PutRequest{Key: "runs/x.bin", Size: 1}, strings.NewReader("x"))
	if err == nil || classOf(err) != pluginerrors.ClassTransient {
		t.Fatalf("href failure: got %v", err)
	}
	if got := f.Count("uploadhref"); got != 1 {
		t.Errorf("href requests: got %d, want exactly 1", got)
	}
	if got := f.Count("mkdir"); got != 2 {
		t.Errorf("mkdir requests: got %d, want 2", got)
	}
	if got := f.Count("putdata"); got != 0 {
		t.Errorf("data PUTs: got %d, want 0", got)
	}
}

func TestRateLimitedHrefPassesRetryAfter(t *testing.T) {
	ctx := context.Background()
	s, f := newStorage(t, nil)
	f.failAlways["uploadhref"] = http.StatusTooManyRequests
	f.retryAfter["uploadhref"] = 7
	_, err := s.Put(ctx, storage.PutRequest{Key: "x.bin", Size: 1}, strings.NewReader("x"))
	if err == nil {
		t.Fatal("a 429 was accepted")
	}
	class, after := pluginerrors.ClassOf(err)
	if class != pluginerrors.ClassTransient || after != 7*time.Second {
		t.Errorf("got class %q after %s, want transient after 7s: %v", class, after, err)
	}
}

func TestPutFetchesFreshHrefPerCall(t *testing.T) {
	ctx := context.Background()
	s, f := newStorage(t, nil)
	body := []byte("fresh")
	for i := range 2 {
		if _, err := s.Put(ctx, storage.PutRequest{Key: "same.bin", Size: int64(len(body))}, bytes.NewReader(body)); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	tokens := f.hrefTokensUsed()
	if len(tokens) != 2 || tokens[0] == tokens[1] {
		t.Fatalf("href tokens used: %v, want two distinct ones — each attempt needs a fresh href", tokens)
	}
	if got := f.Count("uploadhref"); got != 2 {
		t.Errorf("href requests: got %d, want 2", got)
	}
}

// TestFakeRejectsReusedHref fails if the fake stopped enforcing the single-use
// TestReusedHrefIsAcceptedWithoutReplacing pins the measured service behaviour
// the fake models: a second PUT to a consumed href answers 201 and the
// committed object keeps the first upload's bytes. The plugin still fetches a
// fresh href per Put (TestPutFetchesFreshHrefPerCall), because "accepted, not
// replaced" is exactly the trap this shape sets.
func TestReusedHrefIsAcceptedWithoutReplacing(t *testing.T) {
	s, f := newStorage(t, nil)
	body := []byte("self-check")
	if _, err := s.Put(context.Background(), storage.PutRequest{Key: "x.bin", Size: int64(len(body))}, bytes.NewReader(body)); err != nil {
		t.Fatal(err)
	}
	token := f.hrefTokensUsed()[0]
	resp, err := http.NewRequest(http.MethodPut, f.srv.URL+"/up/"+token, strings.NewReader("again"))
	if err != nil {
		t.Fatal(err)
	}
	answer, err := http.DefaultClient.Do(resp)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = answer.Body.Close() }()
	if answer.StatusCode != http.StatusCreated {
		t.Fatalf("second PUT to a consumed href: HTTP %d, want 201 (measured live)", answer.StatusCode)
	}
	if got, ok := f.fileBody("app:/cluster-backup/x.bin"); !ok || !bytes.Equal(got, body) {
		t.Fatalf("the reused href replaced the object: %q ok=%v — it must not", got, ok)
	}
}

func TestAcceptedUploadCommitsAfterPolls(t *testing.T) {
	ctx := context.Background()
	s, f := newStorage(t, nil)
	f.uploadMode = "202"
	f.commitAfter = 2
	body := []byte("arrives late")
	info, err := s.Put(ctx, storage.PutRequest{Key: "late.bin", Size: int64(len(body)), SHA256: ""}, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if info.Size != int64(len(body)) {
		t.Errorf("info %+v", info)
	}
	if got := f.Count("stat"); got != 2 {
		t.Errorf("visibility polls: got %d, want 2 (the save must wait for the committed size)", got)
	}
	if got, ok := f.fileBody("app:/cluster-backup/late.bin"); !ok || !bytes.Equal(got, body) {
		t.Errorf("committed body %q ok=%v", got, ok)
	}
}

func TestAcceptedUploadNeverCommitted(t *testing.T) {
	ctx := context.Background()
	s, f := newStorage(t, nil)
	f.uploadMode = "never"
	_, err := s.Put(ctx, storage.PutRequest{Key: "lost.bin", Size: 4}, strings.NewReader("lost"))
	if err == nil {
		t.Fatal("an upload that never became visible was accepted")
	}
	if class := classOf(err); class != pluginerrors.ClassTransient {
		t.Errorf("class %q, want transient: %v", class, err)
	}
	if got := f.Count("stat"); got < 2 {
		t.Errorf("visibility polls: got %d, want at least 2 before the timeout", got)
	}
	if got := f.Count("putdata"); got != 1 {
		t.Errorf("data PUTs: got %d, want exactly 1", got)
	}
}

// TestC7_AcceptedUploadIsAnAsynchronousWrite is the C7 site proof: the Disk
// accepts an upload with 201/202 before it commits the bytes, so Put can return
// while the write may still settle on the provider. The storage must declare
// the asynchronous-write capability the safety owner consults before the first
// write — the multipart-only classifier missed exactly this shape — and the
// test drives that accepted-then-settled path through Put: the 202 is taken at
// send, the visibility poll inside Put gives up, and the very same upload
// becomes readable afterwards.
func TestC7_AcceptedUploadIsAnAsynchronousWrite(t *testing.T) {
	ctx := context.Background()
	s, f := newStorage(t, nil)
	if !storage.WritesAsynchronously(s) {
		t.Fatal("yandex_disk accepts 201/202 uploads and returns before the Disk commits them; " +
			"the storage must implement storage.AsynchronousWriter")
	}
	f.uploadMode = "202"
	body := []byte("accepted now, committed later")
	_, err := s.Put(ctx, storage.PutRequest{Key: "async.bin", Size: int64(len(body))}, bytes.NewReader(body))
	if err == nil || classOf(err) != pluginerrors.ClassTransient {
		t.Fatalf("a 202 whose commit never became visible inside the poll window: got %v", err)
	}
	if got := f.Count("putdata"); got != 1 {
		t.Fatalf("data PUTs: got %d, want exactly 1 (the plugin must not re-send an accepted body)", got)
	}
	if !f.commitPending("app:/cluster-backup/async.bin") {
		t.Fatal("the fake lost the accepted upload; nothing settles after Put")
	}
	info, err := s.Stat(ctx, "async.bin")
	if err != nil {
		t.Fatalf("an upload the Disk settled after Put returned is not readable: %v", err)
	}
	if info.Size != int64(len(body)) {
		t.Errorf("settled size %d, want %d", info.Size, len(body))
	}
}

func TestInsufficientStorageIsPermanent(t *testing.T) {
	ctx := context.Background()
	s, f := newStorage(t, nil)
	f.uploadMode = "507"
	_, err := s.Put(ctx, storage.PutRequest{Key: "full.bin", Size: 4}, strings.NewReader("full"))
	if err == nil || classOf(err) != pluginerrors.ClassPermanent {
		t.Fatalf("507: got %v", err)
	}
	if got := f.Count("putdata"); got != 1 {
		t.Errorf("data PUTs: got %d, want exactly 1", got)
	}
}

func TestUnauthorizedIsPermanent(t *testing.T) {
	ctx := context.Background()
	s, f := newStorage(t, nil)
	f.uploadMode = "401"
	_, err := s.Put(ctx, storage.PutRequest{Key: "x.bin", Size: 1}, strings.NewReader("x"))
	if err == nil || classOf(err) != pluginerrors.ClassPermanent {
		t.Fatalf("401: got %v", err)
	}
}

func TestTruncatedBodyIsRejected(t *testing.T) {
	ctx := context.Background()
	s, _ := newStorage(t, nil)
	_, err := s.Put(ctx, storage.PutRequest{Key: "short.bin", Size: 100}, strings.NewReader("too short"))
	if err == nil {
		t.Fatal("a body shorter than the declared size was accepted")
	}
}

// TestCancelMidUploadReturnsPromptly cancels while the data PUT is in flight —
// not before it starts, which would prove nothing but the first ctx check.
func TestCancelMidUploadReturnsPromptly(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, f := newStorage(t, nil)
	f.hangPutData = true
	done := make(chan error, 1)
	go func() {
		_, err := s.Put(ctx, storage.PutRequest{Key: "x.bin", Size: 1}, strings.NewReader("x"))
		done <- err
	}()
	waitFor(t, func() bool { return f.Count("putdata") >= 1 }, 5*time.Second)
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Put with a cancelled context returned success")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Put did not return after its context was cancelled")
	}
}

// TestListReturnsTheWholeTree: one List call is one full walk. Every object
// under the prefix appears exactly once, the call names no continuation, and
// the walk makes one service request per folder page.
func TestListReturnsTheWholeTree(t *testing.T) {
	ctx := context.Background()
	s, f := newStorage(t, nil)
	for _, key := range []string{"list/a/0", "list/a/1", "list/a/2", "list/b/x"} {
		f.putObject("app:/cluster-backup/"+key, []byte(key))
	}
	f.pageLimit = 2
	before := f.Count("list")
	page, err := s.List(ctx, storage.ListRequest{Prefix: "list/"})
	if err != nil {
		t.Fatal(err)
	}
	if page.NextToken != "" {
		t.Errorf("a completed walk named a continuation token %q", page.NextToken)
	}
	seen := map[string]int{}
	for _, o := range page.Objects {
		seen[o.Key]++
	}
	if len(seen) != 4 {
		t.Errorf("listing returned %v", seen)
	}
	for _, key := range []string{"list/a/0", "list/a/1", "list/a/2", "list/b/x"} {
		if seen[key] != 1 {
			t.Errorf("key %s seen %d times, want 1", key, seen[key])
		}
	}
	// list/ itself, the two pages of a/, and b/: one request per service page.
	if got := f.Count("list") - before; got != 4 {
		t.Errorf("listing made %d service requests, want 4 (one per service page)", got)
	}
}

// listTreeShape is one positive corpus: objects to seed and the keys a listing
// of prefix must return exactly once.
type listTreeShape struct {
	name    string
	objects []string
	prefix  string
	page    func(f *fakeDiskServer)
}

func listTreeShapes() []listTreeShape {
	return []listTreeShape{
		{name: "empty root", prefix: ""},
		{name: "flat", prefix: "", objects: []string{"a", "b", "c"}},
		{name: "nested", prefix: "", objects: []string{"a/b/c/d.bin", "a/b/e.bin", "a/f.bin"}},
		{name: "many siblings", prefix: "",
			objects: []string{"d1/x", "d2/x", "d3/x", "d4/x", "d5/x"}},
		{name: "paginated folder", prefix: "p/",
			objects: []string{"p/0", "p/1", "p/2", "p/3", "p/4"},
			page:    func(f *fakeDiskServer) { f.pageLimit = 2 }},
		{name: "unknown total", prefix: "p/",
			objects: []string{"p/0", "p/1", "p/2"},
			page:    func(f *fakeDiskServer) { f.listNoTotal = true; f.pageLimit = 1 }},
		{name: "prefix below a folder", prefix: "a/b/",
			objects: []string{"a/b/c", "a/b/d", "a/e", "x/y"}},
	}
}

// TestListReturnsEveryObjectExactlyOnce holds the walk to every positive tree
// and page shape: the same keys once each, a finite walk, and no continuation.
func TestListReturnsEveryObjectExactlyOnce(t *testing.T) {
	for _, tc := range listTreeShapes() {
		t.Run(tc.name, func(t *testing.T) {
			s, f := newStorage(t, nil)
			for _, key := range tc.objects {
				f.putObject("app:/cluster-backup/"+key, []byte(key))
			}
			if tc.page != nil {
				tc.page(f)
			}
			page, err := s.List(context.Background(), storage.ListRequest{Prefix: tc.prefix})
			if err != nil {
				t.Fatal(err)
			}
			if page.NextToken != "" {
				t.Errorf("next token %q after a completed walk", page.NextToken)
			}
			seen := map[string]int{}
			for _, o := range page.Objects {
				seen[o.Key]++
			}
			for _, key := range tc.objects {
				if !strings.HasPrefix(key, tc.prefix) {
					if seen[key] != 0 {
						t.Errorf("object %s is outside the prefix %q", key, tc.prefix)
					}
					continue
				}
				if seen[key] != 1 {
					t.Errorf("key %s seen %d times, want 1", key, seen[key])
				}
			}
			if want := countUnderPrefix(tc.objects, tc.prefix); len(seen) != want {
				t.Errorf("listing returned %d keys, want %d: %v", len(seen), want, seen)
			}
		})
	}
}

func countUnderPrefix(objects []string, prefix string) int {
	n := 0
	for _, key := range objects {
		if strings.HasPrefix(key, prefix) {
			n++
		}
	}
	return n
}

// TestListRunsBranchesInParallel proves call_parallelism reaches the walk: two
// subfolders are held open at once, so the fake serves two listing requests
// simultaneously. Nothing sleeps and no gate is released before both entered,
// so the same corpus at parallelism 1 could never pass this test.
func TestListRunsBranchesInParallel(t *testing.T) {
	s, f := newStorageWithParallelism(t, 2)
	for _, key := range []string{"d1/x", "d2/y"} {
		f.putObject("app:/cluster-backup/"+key, []byte(key))
	}
	g1 := f.holdList("app:/cluster-backup/d1")
	g2 := f.holdList("app:/cluster-backup/d2")
	done := make(chan struct{})
	var page storage.ListPage
	var err error
	go func() {
		defer close(done)
		page, err = s.List(context.Background(), storage.ListRequest{})
	}()
	for _, gate := range []*fakeListGate{g1, g2} {
		select {
		case <-gate.entered:
		case <-time.After(5 * time.Second):
			t.Fatal("one branch never reached the service: the walk did not run both at once")
		}
	}
	if got := f.PeakConcurrentList(); got < 2 {
		t.Errorf("peak concurrent listing requests %d, want at least 2", got)
	}
	close(g1.release)
	close(g2.release)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the listing did not finish after both branches were released")
	}
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, o := range page.Objects {
		seen[o.Key] = true
	}
	if len(seen) != 2 || !seen["d1/x"] || !seen["d2/y"] {
		t.Errorf("parallel listing returned %+v", page.Objects)
	}
}

// TestListStaysSequentialAtParallelismOne is the other end of the same knob:
// one worker means the fake never serves two listing requests at once.
func TestListStaysSequentialAtParallelismOne(t *testing.T) {
	s, f := newStorage(t, nil)
	for _, key := range []string{"d1/x", "d2/y", "d3/z"} {
		f.putObject("app:/cluster-backup/"+key, []byte(key))
	}
	if _, err := s.List(context.Background(), storage.ListRequest{}); err != nil {
		t.Fatal(err)
	}
	if got := f.PeakConcurrentList(); got != 1 {
		t.Errorf("peak concurrent listing requests %d, want exactly 1", got)
	}
}

// TestListWithoutTotalPagesUntilTheEmptyPage: an answer that does not state
// the total cannot prove a folder is fully listed, so the walk pages on until
// an empty page instead of silently dropping everything after the first.
func TestListWithoutTotalPagesUntilTheEmptyPage(t *testing.T) {
	ctx := context.Background()
	s, f := newStorage(t, nil)
	for i := range 3 {
		key := fmt.Sprintf("page/object-%d", i)
		body := []byte(key)
		if _, err := s.Put(ctx, storage.PutRequest{Key: key, Size: int64(len(body))}, bytes.NewReader(body)); err != nil {
			t.Fatal(err)
		}
	}
	f.listNoTotal = true
	f.pageLimit = 1
	before := f.Count("list")
	page, err := s.List(ctx, storage.ListRequest{Prefix: "page/"})
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, o := range page.Objects {
		seen[o.Key] = true
	}
	if len(seen) != 3 {
		t.Errorf("listing returned %v, want all three objects", seen)
	}
	// Three pages with one object each plus the empty page that ends the walk.
	if got := f.Count("list") - before; got != 4 {
		t.Errorf("listing made %d service requests, want 4 (three objects and the empty ending page)", got)
	}
}

func TestListMissingFolderIsEmpty(t *testing.T) {
	s, _ := newStorage(t, nil)
	page, err := s.List(context.Background(), storage.ListRequest{Prefix: "nothing/here/"})
	if err != nil {
		t.Fatalf("listing a folder that does not exist: %v", err)
	}
	if len(page.Objects) != 0 || page.NextToken != "" {
		t.Errorf("page %+v", page)
	}
	page, err = s.List(context.Background(), storage.ListRequest{})
	if err != nil {
		t.Fatalf("listing an empty root: %v", err)
	}
	if len(page.Objects) != 0 {
		t.Errorf("empty root: %+v", page)
	}
}

// TestStatFolderIsDefinitelyAbsent: a key occupied by a folder is an object
// that is definitely not there — never a transient answer.
func TestStatFolderIsDefinitelyAbsent(t *testing.T) {
	s, f := newStorage(t, nil)
	f.putObject("app:/cluster-backup/folder/file.bin", []byte("x"))
	_, err := s.Stat(context.Background(), "folder")
	if class := classOf(err); class != pluginerrors.ClassNotFound {
		t.Fatalf("class %q, want not_found: %v", class, err)
	}
}

// TestStatReadsBothTimestampForms: the API returns the resource timestamp both
// with a zone and, on some endpoints, with a space instead of "T".
func TestStatReadsBothTimestampForms(t *testing.T) {
	ctx := context.Background()
	s, f := newStorage(t, nil)
	f.putObject("app:/cluster-backup/stamp/a.bin", []byte("x"))
	f.setModified("app:/cluster-backup/stamp/a.bin", "2026-10-12 04:29:54")
	info, err := s.Stat(ctx, "stamp/a.bin")
	if err != nil {
		t.Fatal(err)
	}
	if info.ModTime.IsZero() {
		t.Fatal("a space-separated timestamp was not parsed")
	}
	if got := info.ModTime.UTC().Format("2006-01-02T15:04:05"); got != "2026-10-12T04:29:54" {
		t.Errorf("ModTime %s", got)
	}
}

func TestGetPropagatesWriterErrors(t *testing.T) {
	ctx := context.Background()
	s, _ := newStorage(t, nil)
	body := []byte("x")
	if _, err := s.Put(ctx, storage.PutRequest{Key: "w.bin", Size: int64(len(body))}, bytes.NewReader(body)); err != nil {
		t.Fatal(err)
	}
	if err := s.Get(ctx, "w.bin", failWriter{}); err == nil {
		t.Fatal("a failing destination was ignored")
	}
}

type failWriter struct{}

func (failWriter) Write([]byte) (int, error) { return 0, errors.New("no space left") }

func TestListTokenCannotEscapeRoot(t *testing.T) {
	s, _ := newStorage(t, nil)
	_, err := s.List(context.Background(), storage.ListRequest{Prefix: "runs/",
		Token: `{"dir":"app:/Backups","offset":0}`})
	if err == nil || classOf(err) != pluginerrors.ClassPermanent {
		t.Fatalf("a token pointing outside the root: got %v", err)
	}
	if !strings.Contains(err.Error(), "outside the root") {
		t.Errorf("the refusal does not name the cause: %v", err)
	}
}

// TestListUnreadableFolderAnswerFailsTheWalk pins the service semantics this
// build requires: a folder listing answer always carries _embedded.items (an
// empty folder carries an empty list), so a 200 without it proves nothing.
// Reading it as "no children" would silently drop the subtree from the survey —
// a failed walk must return one transient cause and no objects instead.
func TestListUnreadableFolderAnswerFailsTheWalk(t *testing.T) {
	cases := map[string]func(f *fakeDiskServer){
		"items and total absent":      func(f *fakeDiskServer) { f.listNoItems = true; f.listNoTotal = true },
		"items absent, total present": func(f *fakeDiskServer) { f.listNoItems = true },
	}
	for name, shape := range cases {
		t.Run(name, func(t *testing.T) {
			s, f := newStorage(t, nil)
			f.putObject("app:/cluster-backup/runs/x", []byte("x"))
			shape(f)
			page, err := s.List(context.Background(), storage.ListRequest{Prefix: "runs/"})
			if err == nil {
				t.Fatalf("a listing answer without items was accepted as an empty folder: %+v", page)
			}
			if class := classOf(err); class != pluginerrors.ClassTransient {
				t.Errorf("class %q, want transient: %v", class, err)
			}
			if len(page.Objects) != 0 {
				t.Errorf("a failed walk returned objects: %+v", page.Objects)
			}
		})
	}
}

// TestListCursorDotSegmentsAreRefused: a token is untrusted input. The API
// resolves "." and ".." segments of a path parameter to a different resource,
// so a cursor that merely looks confined ("app:/cluster-backup/../Backups")
// would make the plugin read outside the configured root. The guard refuses the
// segment before any request leaves, exactly as it refuses an outside root.
func TestListCursorDotSegmentsAreRefused(t *testing.T) {
	ctx := context.Background()
	tokens := map[string]string{
		"parent segment":  `{"dir":"app:/cluster-backup/../Backups","offset":0}`,
		"current segment": `{"dir":"app:/cluster-backup/./runs","offset":0}`,
		"inner parent":    `{"dir":"app:/cluster-backup/runs/..","offset":0}`,
		"empty segment":   `{"dir":"app:/cluster-backup//runs","offset":0}`,
	}
	for name, token := range tokens {
		t.Run(name, func(t *testing.T) {
			s, f := newStorage(t, nil)
			_, err := s.List(ctx, storage.ListRequest{Token: token})
			if err == nil || classOf(err) != pluginerrors.ClassPermanent {
				t.Fatalf("a cursor with a dot-segment look-alike: got %v", err)
			}
			if got := f.Count("list"); got != 0 {
				t.Errorf("the walk made %d listing requests for a refused cursor, want 0", got)
			}
		})
	}
}

// TestListRefusesDotSegmentFolderFromTheAnswer covers the other direction: a
// folder name reaches the walk through a listing answer, not only through the
// caller's token. The walk must refuse the cursor before it requests the path
// the service would resolve outside the root.
func TestListRefusesDotSegmentFolderFromTheAnswer(t *testing.T) {
	s, f := newStorage(t, nil)
	f.putObject("app:/cluster-backup/../Backups/x", []byte("old archive"))
	_, err := s.List(context.Background(), storage.ListRequest{})
	if err == nil || classOf(err) != pluginerrors.ClassPermanent {
		t.Fatalf("a listed folder named %q: got %v", "..", err)
	}
	for _, p := range f.ListedPaths() {
		if strings.Contains(p, "..") {
			t.Errorf("the walk listed %q: a dot-segment path resolves outside the root", p)
		}
	}
}

// TestOldArchivesStayInvisible is the owner's ruling as a test: objects under
// the old app:/Backups prefix must not appear in a listing and cannot be
// reached through any key — and the folder cleanup a delete runs must stay
// inside the configured root.
func TestOldArchivesStayInvisible(t *testing.T) {
	ctx := context.Background()
	s, f := newStorage(t, nil)
	f.putObject("app:/Backups/2026/09/backup-2026-09-14.tar.zst", []byte("old archive"))
	page, err := s.List(ctx, storage.ListRequest{})
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range page.Objects {
		if strings.Contains(o.Key, "Backups") {
			t.Errorf("listing returned %s", o.Key)
		}
	}
	if _, err := s.Stat(ctx, "Backups/2026/09/backup-2026-09-14.tar.zst"); err == nil {
		t.Error("Stat reached outside the root")
	}
	if err := s.Delete(ctx, "../../Backups/2026/09/backup-2026-09-14.tar.zst"); err == nil {
		t.Error("Delete accepted an escaping key")
	}
	if got, ok := f.fileBody("app:/Backups/2026/09/backup-2026-09-14.tar.zst"); !ok || len(got) == 0 {
		t.Error("the old archive was touched")
	}
	// A delete inside the root runs the folder cleanup: no listing request it
	// makes may name a path outside the root, and the old archives' folders
	// must survive it.
	if _, err := s.Put(ctx, storage.PutRequest{Key: "runs/2026-10-12/x.bin", Size: 1}, strings.NewReader("x")); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, "runs/2026-10-12/x.bin"); err != nil {
		t.Fatal(err)
	}
	if !f.folders["app:/Backups"] || !f.folders["app:/Backups/2026/09"] {
		t.Error("the cleanup removed folders outside the root")
	}
	if _, ok := f.fileBody("app:/Backups/2026/09/backup-2026-09-14.tar.zst"); !ok {
		t.Error("the old archive was removed by the cleanup")
	}
	for _, p := range f.ListedPaths() {
		if !strings.HasPrefix(p, "app:/cluster-backup") {
			t.Errorf("a listing request named %q, outside the root", p)
		}
	}
}

func TestGetRoundTrip(t *testing.T) {
	ctx := context.Background()
	s, f := newStorage(t, nil)
	body := []byte("read me back")
	if _, err := s.Put(ctx, storage.PutRequest{Key: "get/x.bin", Size: int64(len(body))}, bytes.NewReader(body)); err != nil {
		t.Fatal(err)
	}
	var got bytes.Buffer
	if err := s.Get(ctx, "get/x.bin", &got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Bytes(), body) {
		t.Errorf("got %q", got.String())
	}
	_, err := s.Stat(ctx, "get/absent.bin")
	if class := classOf(err); err == nil || class != pluginerrors.ClassNotFound {
		t.Errorf("Stat of an absent key: class %q err %v", class, err)
	}
	if err := s.Get(ctx, "get/absent.bin", io.Discard); classOf(err) != pluginerrors.ClassNotFound {
		t.Errorf("Get of an absent key: %v", err)
	}
	f.failNext["download"] = http.StatusNotFound
	err = s.Get(ctx, "get/x.bin", io.Discard)
	if class := classOf(err); class != pluginerrors.ClassNotFound {
		t.Errorf("Get whose download href is gone: class %q err %v", class, err)
	}
}

// TestDeleteIsIdempotent: the first Delete removes the object and the folders
// it emptied down to the keep depth; the second finds the object absent and
// must not walk anything.
func TestDeleteIsIdempotent(t *testing.T) {
	ctx := context.Background()
	s, f := newStorage(t, nil)
	body := []byte("bye")
	if _, err := s.Put(ctx, storage.PutRequest{Key: "del/a/x.bin", Size: int64(len(body))}, bytes.NewReader(body)); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, "del/a/x.bin"); err != nil {
		t.Fatal(err)
	}
	if f.folders["app:/cluster-backup/del/a"] {
		t.Error("the folder the delete emptied was left behind")
	}
	if !f.folders["app:/cluster-backup/del"] {
		t.Error("the folder at the keep depth was removed")
	}
	before := f.Count("delete")
	if err := s.Delete(ctx, "del/a/x.bin"); err != nil {
		t.Errorf("second Delete of an absent object must be success: %v", err)
	}
	if got := f.Count("delete") - before; got != 1 {
		t.Errorf("the second Delete made %d delete requests, want exactly 1: an absent key triggers no cleanup", got)
	}
}

// TestDeletedFolderIsRemoved: a delete that empties a folder removes it and
// every ancestor it empties down to (never including) the keep depth.
func TestDeletedFolderIsRemoved(t *testing.T) {
	ctx := context.Background()
	s, f := newStorage(t, nil)
	body := []byte("bye")
	if _, err := s.Put(ctx, storage.PutRequest{Key: "runs/a/b/c.bin", Size: int64(len(body))}, bytes.NewReader(body)); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, "runs/a/b/c.bin"); err != nil {
		t.Fatal(err)
	}
	for _, dir := range []string{"app:/cluster-backup/runs/a/b", "app:/cluster-backup/runs/a"} {
		if f.folders[dir] {
			t.Errorf("folder %s was left behind", dir)
		}
	}
	if !f.folders["app:/cluster-backup/runs"] {
		t.Error("the folder at the keep depth was removed")
	}
	if !f.folders["app:/cluster-backup"] {
		t.Error("the configured root was removed")
	}
}

// TestDeepArtifactKeyRoundTrip: the production layout puts an artifact five and
// six segments below the root (runs/<run-id>/databases/<namespace>/<instance>/
// <database>…), so Put must create that whole chain and the cleanup must remove
// exactly the folders the delete emptied, stopping at the runs/ grouping at the
// keep depth. The fake answers 409 on the upload href when the parent folder is
// absent — as the real service does — so a Put that skipped one folder fails
// here rather than silently storing nothing.
func TestDeepArtifactKeyRoundTrip(t *testing.T) {
	ctx := context.Background()
	s, f := newStorage(t, nil)
	keys := []string{
		"runs/2026-10-14T03-00-00Z/databases/example/db-0/postgres.dump.zst",
		"runs/2026-10-14T03-00-00Z/databases/example/db-0/pgdata/full/postgres.dump.zst",
	}
	for _, key := range keys {
		body := []byte("payload of " + key)
		info, err := s.Put(ctx, storage.PutRequest{Key: key, Size: int64(len(body))}, bytes.NewReader(body))
		if err != nil {
			t.Fatalf("Put %s: %v", key, err)
		}
		if info.Size != int64(len(body)) {
			t.Errorf("Put %s reported %+v", key, info)
		}
		st, err := s.Stat(ctx, key)
		if err != nil {
			t.Fatalf("Stat %s: %v", key, err)
		}
		if st.Size != int64(len(body)) {
			t.Errorf("Stat %s reported %+v", key, st)
		}
		var got bytes.Buffer
		if err := s.Get(ctx, key, &got); err != nil {
			t.Fatalf("Get %s: %v", key, err)
		}
		if !bytes.Equal(got.Bytes(), body) {
			t.Errorf("Get %s returned %q, want %q", key, got.String(), body)
		}
	}
	page, err := s.List(ctx, storage.ListRequest{Prefix: "runs/"})
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, o := range page.Objects {
		seen[o.Key] = true
	}
	for _, key := range keys {
		if !seen[key] {
			t.Errorf("List returned %+v, missing %s", page.Objects, key)
		}
	}
	for _, key := range keys {
		if err := s.Delete(ctx, key); err != nil {
			t.Fatalf("Delete %s: %v", key, err)
		}
	}
	for dir := range f.folders {
		if dir != "app:" && dir != "app:/cluster-backup" && dir != "app:/cluster-backup/runs" {
			t.Errorf("folder %s was left behind by the deep-key cleanup", dir)
		}
	}
	for _, dir := range []string{"app:/cluster-backup", "app:/cluster-backup/runs"} {
		if !f.folders[dir] {
			t.Errorf("folder %s was removed", dir)
		}
	}
}

// TestPruneStopsAtKeepDepth is the blast-radius bound: a genuinely empty folder
// at the configured keep depth survives, so the grouping folder of a
// deployment's keys cannot go even if an emptiness verdict is ever wrong; with
// prune_keep_depth 0 the same folder is pruned, which pins what the field means.
func TestPruneStopsAtKeepDepth(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		keep   int
		pruned bool
	}{{keep: 1, pruned: false}, {keep: 0, pruned: true}} {
		s, f := newStorage(t, func(raw map[string]any) { raw["prune_keep_depth"] = tc.keep })
		body := []byte("bye")
		if _, err := s.Put(ctx, storage.PutRequest{Key: "runs/x.bin", Size: int64(len(body))}, bytes.NewReader(body)); err != nil {
			t.Fatal(err)
		}
		if err := s.Delete(ctx, "runs/x.bin"); err != nil {
			t.Fatal(err)
		}
		gone := !f.folders["app:/cluster-backup/runs"]
		if gone != tc.pruned {
			t.Errorf("prune_keep_depth %d: the emptied folder pruned=%v, want %v", tc.keep, gone, tc.pruned)
		}
		if !f.folders["app:/cluster-backup"] {
			t.Errorf("prune_keep_depth %d: the configured root was removed", tc.keep)
		}
	}
}

// TestFolderWithSiblingsIsKept is the emptiness proof as a test: a folder that
// still holds another object must never be deleted, whatever the answer's
// bookkeeping says. The fake removes a folder together with its subtree (as the
// real service does), so a plugin that believed "empty" loses the sibling and
// fails here.
func TestFolderWithSiblingsIsKept(t *testing.T) {
	ctx := context.Background()
	s, f := newStorage(t, nil)
	for _, key := range []string{"runs/old/x.bin", "runs/old/y.bin"} {
		body := []byte(key)
		if _, err := s.Put(ctx, storage.PutRequest{Key: key, Size: int64(len(body))}, bytes.NewReader(body)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Delete(ctx, "runs/old/x.bin"); err != nil {
		t.Fatal(err)
	}
	if !f.folders["app:/cluster-backup/runs/old"] {
		t.Error("a folder still holding an object was removed")
	}
	if _, err := s.Stat(ctx, "runs/old/y.bin"); err != nil {
		t.Fatalf("the sibling object was removed with the folder: %v", err)
	}
	if err := s.Delete(ctx, "runs/old/y.bin"); err != nil {
		t.Fatal(err)
	}
	if f.folders["app:/cluster-backup/runs/old"] {
		t.Error("the folder emptied by the second delete was left behind")
	}
	if !f.folders["app:/cluster-backup/runs"] {
		t.Error("the folder at the keep depth was removed")
	}
}

// TestListingShapesThatCannotProveEmptyLeaveTheFolder: an answer a wrong-shaped
// service could send must never authorise a delete — a missing total, a missing
// item list, a zero total over a non-empty list, or a failed call all mean
// "cannot tell", and the service deletes a non-empty folder with its contents.
func TestListingShapesThatCannotProveEmptyLeaveTheFolder(t *testing.T) {
	cases := map[string]func(f *fakeDiskServer){
		"total absent":      func(f *fakeDiskServer) { f.listNoTotal = true },
		"items absent":      func(f *fakeDiskServer) { f.listNoItems = true },
		"total 0 but items": func(f *fakeDiskServer) { f.listContradicts = true },
		"listing fails":     func(f *fakeDiskServer) { f.failAlways["list"] = http.StatusInternalServerError },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			f := newFakeDisk(t)
			var logs bytes.Buffer
			s := newStorageWithLog(t, baseConfig(t, f), slog.New(slog.NewTextHandler(&logs, nil)))
			// A sibling stays in the folder, so a delete authorised on a shape
			// that cannot prove emptiness would take it too — on the fake as on
			// the real service.
			for _, key := range []string{"del/a/x.bin", "del/a/y.bin"} {
				body := []byte(key)
				if _, err := s.Put(ctx, storage.PutRequest{Key: key, Size: int64(len(body))}, bytes.NewReader(body)); err != nil {
					t.Fatal(err)
				}
			}
			mutate(f)
			if err := s.Delete(ctx, "del/a/x.bin"); err != nil {
				t.Fatalf("a cleanup guarded by an unprovable answer failed the delete: %v", err)
			}
			if !f.folders["app:/cluster-backup/del/a"] {
				t.Error("a folder was removed on an answer that could not prove it empty")
			}
			if !strings.Contains(logs.String(), "app:/cluster-backup/del/a") {
				t.Errorf("the folder left behind was not reported: %s", logs.String())
			}
			if got := f.Count("delete"); got != 1 {
				t.Errorf("delete requests: got %d, want 1 (the object only: no folder delete may follow)", got)
			}
		})
	}
}

// TestMultiNightSweepKeepsListingBounded: five nights of writes and retention
// deletes leave every run folder gone while the grouping folder stays (the keep
// depth), so a later listing walks a constant number of folders. Without the
// folder cleanup each night adds a folder to walk, one list request each,
// forever.
func TestMultiNightSweepKeepsListingBounded(t *testing.T) {
	ctx := context.Background()
	s, f := newStorage(t, nil)
	for night := range 5 {
		dir := fmt.Sprintf("runs/2026-10-%02d", night+1)
		for part := range 3 {
			key := fmt.Sprintf("%s/part-%d.bin", dir, part)
			body := []byte(key)
			if _, err := s.Put(ctx, storage.PutRequest{Key: key, Size: int64(len(body))}, bytes.NewReader(body)); err != nil {
				t.Fatalf("night %d, Put %s: %v", night, key, err)
			}
		}
		for part := range 3 {
			key := fmt.Sprintf("%s/part-%d.bin", dir, part)
			if err := s.Delete(ctx, key); err != nil {
				t.Fatalf("night %d, Delete %s: %v", night, key, err)
			}
		}
	}
	for dir := range f.folders {
		if strings.HasPrefix(dir, "app:/cluster-backup/runs/") {
			t.Errorf("folder %s was left behind by the sweep", dir)
		}
	}
	if !f.folders["app:/cluster-backup/runs"] {
		t.Error("the grouping folder at the keep depth was removed")
	}
	before := f.Count("list")
	page, err := s.List(ctx, storage.ListRequest{})
	if err != nil {
		t.Fatal(err)
	}
	// The root and the (empty) grouping folder: a constant cost that does not
	// grow with the number of nights.
	if got := f.Count("list") - before; got != 2 {
		t.Errorf("listing after five nights made %d list requests, want 2 (root and the empty grouping folder)", got)
	}
	if len(page.Objects) != 0 {
		t.Errorf("listing returned %+v, want nothing", page.Objects)
	}
}

// TestFolderCleanupOfAnAlreadyGoneFolder: a folder that another sweep removed
// first is not an error — the walk continues to the next ancestor.
func TestFolderCleanupOfAnAlreadyGoneFolder(t *testing.T) {
	ctx := context.Background()
	s, f := newStorage(t, nil)
	f.putObject("app:/cluster-backup/del/a/x.bin", []byte("x"))
	delete(f.folders, "app:/cluster-backup/del/a")
	if err := s.Delete(ctx, "del/a/x.bin"); err != nil {
		t.Fatalf("cleanup of a folder that is already gone: %v", err)
	}
	if !f.folders["app:/cluster-backup/del"] || !f.folders["app:/cluster-backup"] {
		t.Error("an ancestor at the keep depth or the root was removed")
	}
}

// TestFolderCleanupFailureKeepsTheDeleteSuccessful: the file delete already
// succeeded, so a folder that cannot be removed is reported through the log and
// left, never turned into a failed delete.
func TestFolderCleanupFailureKeepsTheDeleteSuccessful(t *testing.T) {
	ctx := context.Background()
	f := newFakeDisk(t)
	var logs bytes.Buffer
	s := newStorageWithLog(t, baseConfig(t, f), slog.New(slog.NewTextHandler(&logs, nil)))
	body := []byte("bye")
	if _, err := s.Put(ctx, storage.PutRequest{Key: "del/a/x.bin", Size: int64(len(body))}, bytes.NewReader(body)); err != nil {
		t.Fatal(err)
	}
	f.failDirDelete = http.StatusInternalServerError
	if err := s.Delete(ctx, "del/a/x.bin"); err != nil {
		t.Fatalf("a folder cleanup failure failed the delete: %v", err)
	}
	if _, err := s.Stat(ctx, "del/a/x.bin"); classOf(err) != pluginerrors.ClassNotFound {
		t.Errorf("the file was not deleted: %v", err)
	}
	if !f.folders["app:/cluster-backup/del/a"] {
		t.Fatal("the test did not exercise the failure: the folder is gone")
	}
	if got := f.Count("delete"); got != 2 {
		t.Errorf("delete requests: got %d, want 2 (the object, then one folder attempt)", got)
	}
	if !strings.Contains(logs.String(), "msg=storage.folder.empty") || !strings.Contains(logs.String(), "app:/cluster-backup/del/a") {
		t.Errorf("the folder left behind was not reported: %s", logs.String())
	}
}

// TestHrefNeverReachesErrors: a transport failure must not put the
// pre-authorised upload or download href — its query is the access grant — into
// an error the run logs and stores in the run report.
func TestHrefNeverReachesErrors(t *testing.T) {
	ctx := context.Background()
	closed := closedPortAddress(t)
	s, f := newStorage(t, nil)

	upload := "http://" + closed + "/up/secret-upload-token"
	f.hrefOverride = upload
	_, err := s.Put(ctx, storage.PutRequest{Key: "x.bin", Size: 1}, strings.NewReader("x"))
	if err == nil {
		t.Fatal("a PUT to an unreachable href was accepted")
	}
	if class := classOf(err); class != pluginerrors.ClassTransient {
		t.Errorf("class %q, want transient: %v", class, err)
	}
	if strings.Contains(err.Error(), "secret-upload-token") || strings.Contains(err.Error(), upload) {
		t.Errorf("the upload href leaked into the error: %v", err)
	}

	download := "http://" + closed + "/dl/secret-download-token"
	f.hrefOverride = download
	f.putObject("app:/cluster-backup/get.bin", []byte("x"))
	err = s.Get(ctx, "get.bin", io.Discard)
	if err == nil {
		t.Fatal("a GET from an unreachable href was accepted")
	}
	if strings.Contains(err.Error(), "secret-download-token") || strings.Contains(err.Error(), download) {
		t.Errorf("the download href leaked into the error: %v", err)
	}
}

// closedPortAddress returns a loopback address nothing listens on: a listener
// opened and closed, so the port cannot belong to another process.
func closedPortAddress(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

func TestKeyEscapesRoot(t *testing.T) {
	s, _ := newStorage(t, nil)
	ctx := context.Background()
	for _, key := range []string{"", "../outside", "/absolute", "a/../../b", "a//b"} {
		if _, err := s.Stat(ctx, key); err == nil {
			t.Errorf("Stat(%q) accepted an invalid key", key)
		}
		if err := s.Delete(ctx, key); err == nil {
			t.Errorf("Delete(%q) accepted an invalid key", key)
		}
		if err := s.Get(ctx, key, io.Discard); err == nil {
			t.Errorf("Get(%q) accepted an invalid key", key)
		}
	}
}

func TestMissingTokenFileIsPermanent(t *testing.T) {
	s, _ := newStorage(t, func(raw map[string]any) {
		raw["token"] = map[string]any{"file": filepath.Join(t.TempDir(), "absent")}
	})
	_, err := s.Stat(context.Background(), "x.bin")
	if err == nil || classOf(err) != pluginerrors.ClassPermanent {
		t.Fatalf("a missing token file: got %v", err)
	}
}

func TestNewRejectsBadConfig(t *testing.T) {
	r := testRegistry(t)
	good := `{"api_url": "https://cloud-api.yandex.net/v1/disk", "token": {"file": "/t"},` +
		` "root": "app:/cluster-backup", "permanent_delete": true, "prune_keep_depth": 1,` +
		` "request_timeout": "30s", "commit_poll_interval": "5s", "commit_poll_timeout": "2m"}`
	bad := map[string]string{
		"root outside the application folder": strings.Replace(good, `"app:/cluster-backup"`, `"/absolute"`, 1),
		"root is the application folder":      strings.Replace(good, `"app:/cluster-backup"`, `"app:/"`, 1),
		"api_url is not a URL":                strings.Replace(good, `"https://cloud-api.yandex.net/v1/disk"`, `"cloud-api"`, 1),
		"empty token file":                    strings.Replace(good, `{"file": "/t"}`, `{"file": ""}`, 1),
		"zero request timeout":                strings.Replace(good, `"30s"`, `"0s"`, 1),
		"zero poll interval":                  strings.Replace(good, `"5s"`, `"0s"`, 1),
		"zero poll timeout":                   strings.Replace(good, `"2m"`, `"0s"`, 1),
		"negative keep depth":                 strings.Replace(good, `"prune_keep_depth": 1`, `"prune_keep_depth": -1`, 1),
		"unknown member":                      strings.Replace(good, `{`, `{"zzz": 1,`, 1),
	}
	build := func(raw string) error {
		t.Helper()
		_, err := storage.Build(context.Background(), r, "yandex_disk", jsontext.Value(raw),
			storage.BuildInput{CallParallelism: 1}, instances.Deps{Instance: "yandex"})
		return err
	}
	for name, raw := range bad {
		if err := build(raw); err == nil {
			t.Errorf("%s: config accepted: %s", name, raw)
		}
	}
	if err := build(good); err != nil {
		t.Errorf("the good config was rejected: %v", err)
	}
}

func TestExampleDecodesToAWorkingStorage(t *testing.T) {
	// The example is documentation; its construction must not need the token
	// file to exist (credentials are read per request, not at build time).
	s, err := storage.Build(context.Background(), testRegistry(t), "yandex_disk",
		jsontext.Value(`{"api_url": "https://cloud-api.yandex.net/v1/disk", "token": {"file": "/run/secrets/yandex/token"},`+
			` "root": "app:/cluster-backup", "permanent_delete": true, "prune_keep_depth": 1,`+
			` "request_timeout": "30s", "commit_poll_interval": "5s", "commit_poll_timeout": "2m"}`),
		storage.BuildInput{CallParallelism: 1}, instances.Deps{Instance: "yandex"})
	if err != nil {
		t.Fatal(err)
	}
	if s == nil {
		t.Fatal("the example built no storage")
	}
}
