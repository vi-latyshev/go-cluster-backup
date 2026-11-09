package yandexdisk_test

// The failure paths every request of this plugin has to classify: a link that
// dies mid-answer, an href that cannot be used, an API answer that cannot be
// read, and the guards the listing and pruning walks lean on. Each case is one
// scripted fault of the fake, asserted through the class execution reads.

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"encoding/json/jsontext"
	json "encoding/json/v2"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage"
)

func TestBuildRejectsCanceledContext(t *testing.T) {
	f := newFakeDisk(t)
	body, err := json.Marshal(baseConfig(t, f))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := storage.Build(ctx, testRegistry(t), "yandex_disk", body,
		storage.BuildInput{CallParallelism: 1}, instances.Deps{Instance: "yandex"}); err == nil {
		t.Fatal("a cancelled context built a storage")
	}
}

func TestDescribeRejectsACanceledContext(t *testing.T) {
	s, _ := newStorage(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Describe(ctx); err == nil {
		t.Fatal("Describe with a cancelled context returned success")
	}
}

func TestPutRejectsBadKeys(t *testing.T) {
	s, _ := newStorage(t, nil)
	for _, key := range []string{"", "../x", "/abs", "a//b"} {
		if _, err := s.Put(context.Background(), storage.PutRequest{Key: key, Size: 1}, strings.NewReader("x")); err == nil {
			t.Errorf("Put(%q) accepted an invalid key", key)
		}
	}
}

// TestMkdirTransportFailureIsTransient: a folder creation that gets no answer is
// a transient error execution retries the whole Put for.
func TestMkdirTransportFailureIsTransient(t *testing.T) {
	s, f := newStorage(t, nil)
	f.dropTransport["mkdir"] = true
	_, err := s.Put(context.Background(), storage.PutRequest{Key: "a/b.bin", Size: 1}, strings.NewReader("x"))
	if err == nil || classOf(err) != pluginerrors.ClassTransient {
		t.Fatalf("a folder creation with no answer: %v", err)
	}
}

func TestUploadHrefTransportFailureIsTransient(t *testing.T) {
	s, f := newStorage(t, nil)
	f.dropTransport["uploadhref"] = true
	_, err := s.Put(context.Background(), storage.PutRequest{Key: "x.bin", Size: 1}, strings.NewReader("x"))
	if err == nil || classOf(err) != pluginerrors.ClassTransient {
		t.Fatalf("an upload href with no answer: %v", err)
	}
}

func TestUploadHrefWithoutAnHrefIsTransient(t *testing.T) {
	s, f := newStorage(t, nil)
	f.uploadHrefBad = true
	_, err := s.Put(context.Background(), storage.PutRequest{Key: "x.bin", Size: 1}, strings.NewReader("x"))
	if err == nil || classOf(err) != pluginerrors.ClassTransient {
		t.Fatalf("an upload href answer with no href: %v", err)
	}
}

// TestSendRejectsAnUnusableHref: an href that cannot even build a request is a
// permanent failure of this attempt, not a retry of a broken URL.
func TestSendRejectsAnUnusableHref(t *testing.T) {
	s, f := newStorage(t, nil)
	f.hrefOverride = "http://[::1"
	_, err := s.Put(context.Background(), storage.PutRequest{Key: "x.bin", Size: 1}, strings.NewReader("x"))
	if err == nil {
		t.Fatal("an unusable upload href was accepted")
	}
	if class := classOf(err); class != pluginerrors.ClassPermanent {
		t.Errorf("class %q, want permanent: %v", class, err)
	}
}

// TestSendReportsABrokenAnswer: the bytes were sent, the answer never arrived
// whole — a transient failure of the attempt.
func TestSendReportsABrokenAnswer(t *testing.T) {
	s, f := newStorage(t, nil)
	f.shortAnswer["putdata"] = true
	_, err := s.Put(context.Background(), storage.PutRequest{Key: "x.bin", Size: 1}, strings.NewReader("x"))
	if err == nil || classOf(err) != pluginerrors.ClassTransient {
		t.Fatalf("a data PUT whose answer was cut: %v", err)
	}
}

// TestAwaitVisibleStopsOnAPermanentAnswer: a metadata answer that will not
// change (the token was rejected) ends the poll at once rather than at the
// timeout.
func TestAwaitVisibleStopsOnAPermanentAnswer(t *testing.T) {
	s, f := newStorage(t, nil)
	f.uploadMode = "202"
	f.failAlways["stat"] = http.StatusForbidden
	start := time.Now()
	_, err := s.Put(context.Background(), storage.PutRequest{Key: "x.bin", Size: 1}, strings.NewReader("x"))
	if err == nil || classOf(err) != pluginerrors.ClassPermanent {
		t.Fatalf("a rejected metadata poll: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("the poll ran for %s; a permanent answer must end it at once", elapsed)
	}
	if got := f.Count("stat"); got != 1 {
		t.Errorf("metadata polls: got %d, want exactly 1", got)
	}
}

// TestAwaitVisibleEndsWithTheContext: the poll waits on the caller's context,
// and a long poll interval keeps the wait off the clock, so the cancellation is
// the only thing that can end it.
func TestAwaitVisibleEndsWithTheContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, f := newStorage(t, func(raw map[string]any) {
		raw["commit_poll_interval"] = "1h"
		raw["commit_poll_timeout"] = "1h"
	})
	f.uploadMode = "never"
	done := make(chan error, 1)
	go func() {
		_, err := s.Put(ctx, storage.PutRequest{Key: "x.bin", Size: 1}, strings.NewReader("x"))
		done <- err
	}()
	waitFor(t, func() bool { return f.Count("stat") >= 1 }, 5*time.Second)
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("a poll whose context ended returned success")
		}
		if !errors.Is(err, context.Canceled) {
			t.Errorf("err %v, want the context's own error", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the poll did not return after its context was cancelled")
	}
}

func TestStatRejectsAnUnreadableAnswer(t *testing.T) {
	s, f := newStorage(t, nil)
	f.putObject("app:/cluster-backup/a.bin", []byte("x"))
	f.statBody = `{"type": 5}`
	_, err := s.Stat(context.Background(), "a.bin")
	if err == nil || classOf(err) != pluginerrors.ClassTransient {
		t.Fatalf("an unreadable metadata answer: %v", err)
	}
}

func TestStatFailureIsTransient(t *testing.T) {
	s, f := newStorage(t, nil)
	f.failAlways["stat"] = http.StatusInternalServerError
	if _, err := s.Stat(context.Background(), "a.bin"); err == nil || classOf(err) != pluginerrors.ClassTransient {
		t.Fatalf("a failed metadata call: %v", err)
	}
}

func TestListRejectsEscapingPrefixes(t *testing.T) {
	s, _ := newStorage(t, nil)
	ctx := context.Background()
	for _, prefix := range []string{"/abs", "../x", "a/../b"} {
		if _, err := s.List(ctx, storage.ListRequest{Prefix: prefix}); err == nil || classOf(err) != pluginerrors.ClassPermanent {
			t.Errorf("List(%q) accepted an escaping prefix: %v", prefix, err)
		}
	}
}

func TestListRejectsAnUnreadableToken(t *testing.T) {
	s, _ := newStorage(t, nil)
	if _, err := s.List(context.Background(), storage.ListRequest{Prefix: "runs/", Token: "not json"}); err == nil || classOf(err) != pluginerrors.ClassPermanent {
		t.Fatalf("an unreadable listing token: %v", err)
	}
}

func TestListFailureIsTransient(t *testing.T) {
	s, f := newStorage(t, nil)
	f.failAlways["list"] = http.StatusInternalServerError
	if _, err := s.List(context.Background(), storage.ListRequest{}); err == nil || classOf(err) != pluginerrors.ClassTransient {
		t.Fatalf("a failed listing: %v", err)
	}
}

// TestListStopsOnAPageThatCannotAdvance: an answer that states children exist
// while carrying none can never be paged past; the walk stops with an error
// rather than spending the storage's budget on the same page.
func TestListStopsOnAPageThatCannotAdvance(t *testing.T) {
	s, f := newStorage(t, nil)
	// The root folder has to exist, or the walk never reaches the page.
	f.putObject("app:/cluster-backup/runs/x.bin", []byte("x"))
	f.listEmptyPage = true
	_, err := s.List(context.Background(), storage.ListRequest{})
	if err == nil || classOf(err) != pluginerrors.ClassTransient {
		t.Fatalf("a listing that cannot advance: %v", err)
	}
}

// TestListTokenRefusalsNameTheirCause covers every strict token shape one at a
// time: an unreadable document, an unknown member (including the old token's
// queued-folder field), a wrong type, a negative offset, a folder outside the
// root, a folder beside the prefix, and a token with no folder at all. Each
// refusal must be permanent and must name the specific cause — a vacuous
// permanent error would hide which guard fired.
func TestListTokenRefusalsNameTheirCause(t *testing.T) {
	cases := []struct {
		name  string
		token string
		want  string
	}{
		{name: "not json", token: "not json", want: "listing token"},
		{name: "unknown member", token: `{"dir":"app:/cluster-backup","offset":0,"zzz":1}`,
			want: "zzz"},
		{name: "old queued-folder member", token: `{"dir":"app:/cluster-backup","offset":0,"queue":["app:/Backups"]}`,
			want: "queue"},
		{name: "wrong offset type", token: `{"dir":"app:/cluster-backup","offset":"1"}`,
			want: "offset"},
		{name: "negative offset", token: `{"dir":"app:/cluster-backup","offset":-1}`,
			want: "offset -1 is negative"},
		{name: "outside the root", token: `{"dir":"app:/Backups","offset":0}`,
			want: "outside the root"},
		{name: "beside the prefix", token: `{"dir":"app:/cluster-backup/other","offset":0}`,
			want: "outside the prefix"},
		{name: "no folder", token: `{"offset":0}`, want: "outside the root"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, f := newStorage(t, nil)
			before := f.Count("list")
			page, err := s.List(context.Background(), storage.ListRequest{Prefix: "runs/", Token: tc.token})
			if err == nil || classOf(err) != pluginerrors.ClassPermanent {
				t.Fatalf("token %s: %v", tc.token, err)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("the refusal does not name %q: %v", tc.want, err)
			}
			if len(page.Objects) != 0 || page.NextToken != "" {
				t.Errorf("a refused token returned %+v", page)
			}
			if got := f.Count("list"); got != before {
				t.Errorf("a refused token made %d listing requests, want none", got-before)
			}
		})
	}
}

// TestListBranchWithNoAnswerIsTransient: a connection that dies while one
// branch is being read is a transient failure of that branch, and the whole
// call still returns no objects.
func TestListBranchWithNoAnswerIsTransient(t *testing.T) {
	s, f := newStorage(t, nil)
	f.putObject("app:/cluster-backup/d1/x", []byte("x"))
	f.dropList("app:/cluster-backup/d1")
	page, err := s.List(context.Background(), storage.ListRequest{})
	if err == nil || classOf(err) != pluginerrors.ClassTransient {
		t.Fatalf("a branch with no answer: page %+v, err %v", page, err)
	}
	if len(page.Objects) != 0 || page.NextToken != "" {
		t.Errorf("a failed walk returned a partial view %+v", page)
	}
}

// TestListReplaysTheWholeWalkAfterATransientFailure: a failure in one branch
// makes the whole call fail with no partial view; the replay starts at the same
// input token and reads the entire traversal again, so every object still comes
// back exactly once.
func TestListReplaysTheWholeWalkAfterATransientFailure(t *testing.T) {
	ctx := context.Background()
	s, f := newStorage(t, nil)
	for _, key := range []string{"d1/x", "d2/y", "d3/z"} {
		f.putObject("app:/cluster-backup/"+key, []byte(key))
	}
	page, err := s.List(ctx, storage.ListRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Objects) != 3 {
		t.Fatalf("the clean walk returned %+v", page.Objects)
	}
	firstRun := f.Count("list")

	f.failListOnce("app:/cluster-backup/d2", http.StatusInternalServerError, 0)
	page, err = s.List(ctx, storage.ListRequest{})
	if err == nil || classOf(err) != pluginerrors.ClassTransient {
		t.Fatalf("a branch failure: quiet page %+v, err %v", page, err)
	}
	if len(page.Objects) != 0 || page.NextToken != "" {
		t.Errorf("a failed walk returned a partial view %+v", page)
	}
	if !strings.Contains(err.Error(), "/d2") {
		t.Errorf("the cause does not name the failed folder: %v", err)
	}
	failedRun := f.Count("list") - firstRun
	// root, d1, d2 and d3: one failure does not stop the siblings.
	if failedRun != 4 {
		t.Errorf("the failed walk made %d requests, want 4 (every reachable folder read)", failedRun)
	}

	page, err = s.List(ctx, storage.ListRequest{})
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]int{}
	for _, o := range page.Objects {
		seen[o.Key]++
	}
	for _, key := range []string{"d1/x", "d2/y", "d3/z"} {
		if seen[key] != 1 {
			t.Errorf("key %s seen %d times after replay, want 1", key, seen[key])
		}
	}
	if got := f.Count("list") - firstRun; got != failedRun+4 {
		t.Errorf("the replay made %d requests in total, want %d (the whole walk read twice)",
			got, failedRun+4)
	}
}

// TestListReportsTheLargestRetryHint: two branches fail at once, and the walk
// reports one cause — the transient one with the largest Retry-After hint, not
// a join of both.
func TestListReportsTheLargestRetryHint(t *testing.T) {
	s, f := newStorageWithParallelism(t, 2)
	for _, key := range []string{"d1/x", "d2/y"} {
		f.putObject("app:/cluster-backup/"+key, []byte(key))
	}
	f.failListOnce("app:/cluster-backup/d1", http.StatusInternalServerError, 0)
	f.failListOnce("app:/cluster-backup/d2", http.StatusTooManyRequests, 7)
	_, err := s.List(context.Background(), storage.ListRequest{})
	if err == nil {
		t.Fatal("two failed branches returned success")
	}
	class, after := pluginerrors.ClassOf(err)
	if class != pluginerrors.ClassTransient || after != 7*time.Second {
		t.Errorf("class %q after %s, want the largest hint (transient after 7s): %v", class, after, err)
	}
	if !strings.Contains(err.Error(), "/d2") || strings.Contains(err.Error(), "/d1") {
		t.Errorf("the reported cause is not the largest-hint failure alone: %v", err)
	}
}

// TestListCancelWhileFetching: the caller's context ends while one branch is
// held open; the walk returns the context's own error and no objects, and does
// not wait for the held request to answer.
func TestListCancelWhileFetching(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, f := newStorage(t, nil)
	f.putObject("app:/cluster-backup/d1/x", []byte("x"))
	gate := f.holdList("app:/cluster-backup/d1")
	done := make(chan error, 1)
	go func() { _, err := s.List(ctx, storage.ListRequest{}); done <- err }()
	select {
	case <-gate.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the branch never reached the service")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err %v, want the context's own error", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the walk did not return after its context was cancelled")
	}
}

func TestListTransportFailureIsTransient(t *testing.T) {
	s, f := newStorage(t, nil)
	f.dropTransport["list"] = true
	if _, err := s.List(context.Background(), storage.ListRequest{}); err == nil || classOf(err) != pluginerrors.ClassTransient {
		t.Fatalf("a listing with no answer: %v", err)
	}
}

func TestListRejectsAnUnreadableAnswer(t *testing.T) {
	s, f := newStorage(t, nil)
	f.listBody = `{"_embedded": 5}`
	if _, err := s.List(context.Background(), storage.ListRequest{}); err == nil || classOf(err) != pluginerrors.ClassTransient {
		t.Fatalf("an unreadable listing answer: %v", err)
	}
}

func TestDeleteTransportFailureIsTransient(t *testing.T) {
	s, f := newStorage(t, nil)
	f.dropTransport["delete"] = true
	if err := s.Delete(context.Background(), "a.bin"); err == nil || classOf(err) != pluginerrors.ClassTransient {
		t.Fatalf("a delete with no answer: %v", err)
	}
}

func TestDeleteFailureIsTransient(t *testing.T) {
	s, f := newStorage(t, nil)
	f.failAlways["delete"] = http.StatusInternalServerError
	if err := s.Delete(context.Background(), "a.bin"); err == nil || classOf(err) != pluginerrors.ClassTransient {
		t.Fatalf("a failed delete: %v", err)
	}
}

// TestFolderCleanupFailureKeepsTheDeleteSuccessful is in the main test file for
// the status case; this is the transport one: the folder delete gets no answer
// at all, and the object delete still succeeded.
func TestFolderCleanupTransportFailureKeepsTheDeleteSuccessful(t *testing.T) {
	ctx := context.Background()
	f := newFakeDisk(t)
	var logs bytes.Buffer
	s := newStorageWithLog(t, baseConfig(t, f), slog.New(slog.NewTextHandler(&logs, nil)))
	body := []byte("bye")
	if _, err := s.Put(ctx, storage.PutRequest{Key: "del/a/x.bin", Size: int64(len(body))}, bytes.NewReader(body)); err != nil {
		t.Fatal(err)
	}
	f.dropFolderDel = true
	if err := s.Delete(ctx, "del/a/x.bin"); err != nil {
		t.Fatalf("a folder cleanup with no answer failed the delete: %v", err)
	}
	if _, err := s.Stat(ctx, "del/a/x.bin"); classOf(err) != pluginerrors.ClassNotFound {
		t.Errorf("the file was not deleted: %v", err)
	}
	if !strings.Contains(logs.String(), "app:/cluster-backup/del/a") {
		t.Errorf("the folder left behind was not reported: %s", logs.String())
	}
}

func TestGetRejectsAnUnusableHref(t *testing.T) {
	ctx := context.Background()
	s, f := newStorage(t, nil)
	f.putObject("app:/cluster-backup/a.bin", []byte("x"))
	f.hrefOverride = "http://[::1"
	err := s.Get(ctx, "a.bin", io.Discard)
	if err == nil || classOf(err) != pluginerrors.ClassPermanent {
		t.Fatalf("an unusable download href: %v", err)
	}
}

func TestGetAbsentHrefIsNotFound(t *testing.T) {
	ctx := context.Background()
	s, f := newStorage(t, nil)
	f.putObject("app:/cluster-backup/a.bin", []byte("x"))
	f.hrefOverride = f.srv.URL + "/missing"
	err := s.Get(ctx, "a.bin", io.Discard)
	if err == nil || classOf(err) != pluginerrors.ClassNotFound {
		t.Fatalf("a download href that answers 404: %v", err)
	}
}

func TestGetFailedHrefIsClassified(t *testing.T) {
	ctx := context.Background()
	s, f := newStorage(t, nil)
	f.putObject("app:/cluster-backup/a.bin", []byte("x"))
	f.hrefOverride = f.srv.URL + "/boom"
	err := s.Get(ctx, "a.bin", io.Discard)
	if err == nil || classOf(err) != pluginerrors.ClassTransient {
		t.Fatalf("a download href that answers 500: %v", err)
	}
}

// TestGetCancelMidBodyReturnsPromptly cancels while the object's body is
// already flowing.
func TestGetCancelMidBodyReturnsPromptly(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, f := newStorage(t, nil)
	body := bytes.Repeat([]byte("x"), 4096)
	f.putObject("app:/cluster-backup/stall.bin", body)
	f.downloadStall = true
	dst := &signalWriter{done: make(chan struct{})}
	done := make(chan error, 1)
	go func() { done <- s.Get(ctx, "stall.bin", dst) }()
	// The cancel has to land inside the read, not before the answer began:
	// the first byte written to the destination is what proves the call is
	// past its response headers and inside io.Copy.
	select {
	case <-dst.done:
	case <-time.After(5 * time.Second):
		t.Fatal("the download never started reading")
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("a download whose context ended returned success")
		}
		if !errors.Is(err, context.Canceled) {
			t.Errorf("err %v, want the context's own error", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Get did not return after its context was cancelled")
	}
}

// signalWriter reports the first byte a download wrote: proof the client is
// inside its read loop rather than still waiting for the answer to begin.
type signalWriter struct {
	once sync.Once
	done chan struct{}
}

func (w *signalWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.done) })
	return len(p), nil
}

func TestDownloadHrefTransportFailureIsTransient(t *testing.T) {
	ctx := context.Background()
	s, f := newStorage(t, nil)
	f.putObject("app:/cluster-backup/a.bin", []byte("x"))
	f.dropTransport["download"] = true
	if err := s.Get(ctx, "a.bin", io.Discard); err == nil || classOf(err) != pluginerrors.ClassTransient {
		t.Fatalf("a download href with no answer: %v", err)
	}
}

func TestDownloadHrefWithoutAnHrefIsTransient(t *testing.T) {
	ctx := context.Background()
	s, f := newStorage(t, nil)
	f.putObject("app:/cluster-backup/a.bin", []byte("x"))
	f.downloadHrefBad = true
	if err := s.Get(ctx, "a.bin", io.Discard); err == nil || classOf(err) != pluginerrors.ClassTransient {
		t.Fatalf("a download href answer with no href: %v", err)
	}
}

func TestDownloadHrefFailureIsClassified(t *testing.T) {
	ctx := context.Background()
	s, f := newStorage(t, nil)
	f.putObject("app:/cluster-backup/a.bin", []byte("x"))
	f.failAlways["download"] = http.StatusInternalServerError
	if err := s.Get(ctx, "a.bin", io.Discard); err == nil || classOf(err) != pluginerrors.ClassTransient {
		t.Fatalf("a failed download href: %v", err)
	}
}

func TestAPICallReportsABrokenAnswer(t *testing.T) {
	s, f := newStorage(t, nil)
	f.shortAnswer["stat"] = true
	if _, err := s.Stat(context.Background(), "a.bin"); err == nil || classOf(err) != pluginerrors.ClassTransient {
		t.Fatalf("an API answer cut mid-body: %v", err)
	}
}

func TestEmptyTokenFileIsPermanent(t *testing.T) {
	f := newFakeDisk(t)
	token := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(token, []byte("  \n"), 0o600); err != nil {
		t.Fatal(err)
	}
	raw := baseConfig(t, f)
	raw["token"] = map[string]any{"file": token}
	s := newStorageFrom(t, raw)
	if _, err := s.Stat(context.Background(), "a.bin"); err == nil || classOf(err) != pluginerrors.ClassPermanent {
		t.Fatalf("an empty token file: %v", err)
	}
}

func TestNewRejectsRootWithADoubleSlash(t *testing.T) {
	r := testRegistry(t)
	raw := `{"api_url": "https://cloud-api.yandex.net/v1/disk", "token": {"file": "/t"},` +
		` "root": "app://", "permanent_delete": true, "prune_keep_depth": 1,` +
		` "request_timeout": "30s", "commit_poll_interval": "5s", "commit_poll_timeout": "2m"}`
	if _, err := storage.Build(context.Background(), r, "yandex_disk", jsontext.Value(raw),
		storage.BuildInput{CallParallelism: 1}, instances.Deps{Instance: "yandex"}); err == nil {
		t.Fatal("a root that trims to the application folder itself was accepted")
	}
}

func TestExampleIsAValidConfig(t *testing.T) {
	r := testRegistry(t)
	if err := r.Check("storage", "yandex_disk", jsontext.Value(
		`{"api_url": "https://cloud-api.yandex.net/v1/disk", "token": {"file": "/run/secrets/yandex/token"},`+
			` "root": "app:/cluster-backup", "permanent_delete": true, "prune_keep_depth": 1,`+
			` "request_timeout": "30s", "commit_poll_interval": "5s", "commit_poll_timeout": "2m"}`)); err != nil {
		t.Fatalf("the documented config was rejected: %v", err)
	}
}
