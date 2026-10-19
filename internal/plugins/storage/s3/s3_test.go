package s3_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
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

	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin/plugintest"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage/s3"
	"github.com/vi-latyshev/go-cluster-backup/internal/storage"
	"github.com/vi-latyshev/go-cluster-backup/internal/storage/storagetest"
)

const testBucket = "test-bucket"

func testRegistry(t *testing.T) *plugin.Registry {
	t.Helper()
	r := plugin.NewRegistry()
	if err := s3.Register(r); err != nil {
		t.Fatal(err)
	}
	return r
}

// writeSecret writes a secret file of mode 600 for a plugin.Secret{file} field.
func writeSecret(t *testing.T, value string) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "secret-*")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Chmod(0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(value); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return f.Name()
}

func baseConfig(t *testing.T, f *fakeS3Server, mutate func(raw map[string]any)) map[string]any {
	t.Helper()
	raw := map[string]any{
		"endpoint":          f.srv.URL,
		"region":            "ru-central-1",
		"bucket":            testBucket,
		"prefix":            "root",
		"access_key_id":     map[string]any{"file": writeSecret(t, "tenant:key-id")},
		"secret_access_key": map[string]any{"file": writeSecret(t, "secret-key")},
		"addressing":        "path",
		"storage_class":     "STANDARD",
		"request_checksum":  "when_required",
		"part_bytes":        16 << 20,
		"max_request_bytes": 5 << 30,
		"max_object_bytes":  5 << 40,
		"request_timeout":   "30s",
	}
	if mutate != nil {
		mutate(raw)
	}
	return raw
}

func newStorageFrom(t *testing.T, raw map[string]any) storage.Storage {
	t.Helper()
	body, err := json.Marshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	s, err := plugin.Build(context.Background(), testRegistry(t), storage.Kind, "s3", body,
		plugin.Deps{Instance: "offsite", Log: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func newStorage(t *testing.T, mutate func(raw map[string]any)) (storage.Storage, *fakeS3Server) {
	t.Helper()
	f := newFakeS3(t, testBucket)
	return newStorageFrom(t, baseConfig(t, f, mutate)), f
}

func classOf(err error) plugin.Class {
	class, _ := plugin.ClassOf(err)
	return class
}

func TestGeneralConformance(t *testing.T) {
	plugintest.CheckSpec(t, testRegistry(t))
}

func TestStorageConformance(t *testing.T) {
	storagetest.Run(t, func(t *testing.T) storage.Storage {
		s, _ := newStorage(t, nil)
		return s
	})
}

func TestDescribeTraits(t *testing.T) {
	s, _ := newStorage(t, nil)
	tr, err := s.Describe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(tr.Checksums) != 1 || tr.Checksums[0] != "md5" {
		t.Errorf("checksums %v", tr.Checksums)
	}
}

// TestSinglePutSequence pins one attempt's requests: one signed PutObject with
// the configured storage class and no aws-chunked framing, then one HeadObject
// that proves the object is there. A hidden SDK retry would break the counts.
func TestSinglePutSequence(t *testing.T) {
	ctx := context.Background()
	s, f := newStorage(t, nil)
	body := []byte("one attempt")
	info, err := s.Put(ctx, storage.PutRequest{Key: "a/b.bin", Size: int64(len(body))}, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if info.Size != int64(len(body)) || info.MD5 == "" {
		t.Errorf("info %+v, want the size and the single-part md5", info)
	}
	for op, want := range map[string]int{"put": 1, "head": 1, "create": 0, "part": 0, "complete": 0, "abort": 0} {
		if got := f.Count(op); got != want {
			t.Errorf("%s requests: got %d, want %d", op, got, want)
		}
	}
	if f.sawStorage != "STANDARD" {
		t.Errorf("storage class sent: %q", f.sawStorage)
	}
	if f.sawChunked {
		t.Error("a request body arrived chunked; request_checksum when_required must keep the SDK from aws-chunked framing")
	}
	if f.sawUnsigned != 0 {
		t.Errorf("%d requests arrived without an Authorization header", f.sawUnsigned)
	}
	if _, ok := f.objectBody("root/a/b.bin"); !ok {
		t.Error("the object was not stored under the storage prefix")
	}
}

func TestNoSdkRetryOnPut(t *testing.T) {
	ctx := context.Background()
	s, f := newStorage(t, nil)
	f.failAlways["put"] = 503
	f.retryAfter["put"] = 3
	_, err := s.Put(ctx, storage.PutRequest{Key: "x.bin", Size: 1}, strings.NewReader("x"))
	if err == nil {
		t.Fatal("a 503 was accepted")
	}
	class, after := plugin.ClassOf(err)
	if class != plugin.ClassTransient || after != 3*time.Second {
		t.Errorf("class %q after %s, want transient after 3s: %v", class, after, err)
	}
	if got := f.Count("put"); got != 1 {
		t.Errorf("put requests: got %d, want exactly 1 (the SDK retryer must stay off)", got)
	}
}

func TestPutNotVisibleIsFailure(t *testing.T) {
	ctx := context.Background()
	s, f := newStorage(t, nil)
	f.putDrops = true
	_, err := s.Put(ctx, storage.PutRequest{Key: "gone.bin", Size: 4}, strings.NewReader("gone"))
	if err == nil {
		t.Fatal("a PutObject that stored nothing was accepted")
	}
	if class := classOf(err); class != plugin.ClassTransient {
		t.Errorf("class %q, want transient: %v", class, err)
	}
}

// TestForbiddenHeadIsNotNotFound: an existence probe answered 403 must never be
// read as absence — the object may be there and simply unreadable.
func TestForbiddenHeadIsNotNotFound(t *testing.T) {
	ctx := context.Background()
	s, f := newStorage(t, nil)
	f.putObject("root/exists.bin", []byte("there"))
	f.head403 = true
	_, err := s.Stat(ctx, "exists.bin")
	if err == nil {
		t.Fatal("a 403 on HEAD was read as success")
	}
	if class := classOf(err); class != plugin.ClassPermanent {
		t.Errorf("class %q, want permanent (never not_found): %v", class, err)
	}
	if _, err := s.Put(ctx, storage.PutRequest{Key: "new.bin", Size: 1}, strings.NewReader("x")); classOf(err) != plugin.ClassPermanent {
		t.Errorf("Put whose verification got a 403: %v", err)
	}
}

func TestOversizeObjectRejectedWithoutRequests(t *testing.T) {
	ctx := context.Background()
	s, f := newStorage(t, func(raw map[string]any) {
		raw["part_bytes"] = 5 << 20
		raw["max_request_bytes"] = 5 << 20
		raw["max_object_bytes"] = 10 << 20
	})
	_, err := s.Put(ctx, storage.PutRequest{Key: "huge.bin", Size: 20 << 20}, bytes.NewReader(make([]byte, 1)))
	if err == nil || classOf(err) != plugin.ClassPermanent {
		t.Fatalf("oversize object: got %v", err)
	}
	if f.Count("put") != 0 || f.Count("create") != 0 || f.Count("part") != 0 {
		t.Errorf("an oversized object reached the service: %+v", f.counts)
	}
}

func TestTruncatedBodyIsRejected(t *testing.T) {
	s, _ := newStorage(t, nil)
	_, err := s.Put(context.Background(), storage.PutRequest{Key: "short.bin", Size: 100}, strings.NewReader("too short"))
	if err == nil {
		t.Fatal("a body shorter than the declared size was accepted")
	}
}

func TestListPagingAndPrefix(t *testing.T) {
	ctx := context.Background()
	s, f := newStorage(t, nil)
	for i := range 5 {
		f.putObject("root/a/"+string(rune('0'+i)), []byte("x"))
	}
	f.putObject("root/other/x", []byte("x"))
	f.putObject("outside/foreign", []byte("x"))
	f.pageSize = 2
	seen := map[string]int{}
	token := ""
	pages := 0
	for {
		page, err := s.List(ctx, storage.ListRequest{Prefix: "a/", Token: token})
		if err != nil {
			t.Fatal(err)
		}
		pages++
		for _, o := range page.Objects {
			seen[o.Key]++
		}
		if page.NextToken == "" {
			break
		}
		if pages > 10 {
			t.Fatal("listing did not terminate")
		}
		token = page.NextToken
	}
	if pages != 3 {
		t.Errorf("pages: got %d, want 3", pages)
	}
	if len(seen) != 5 {
		t.Errorf("listing returned %v", seen)
	}
	for key, n := range seen {
		if n != 1 {
			t.Errorf("key %s returned %d times", key, n)
		}
		if !strings.HasPrefix(key, "a/") {
			t.Errorf("key %s is outside the prefix", key)
		}
	}
	if f.Count("list") != 3 {
		t.Errorf("list requests: got %d, want 3", f.Count("list"))
	}
}

func TestListRejectsEscapingPrefix(t *testing.T) {
	s, _ := newStorage(t, nil)
	if _, err := s.List(context.Background(), storage.ListRequest{Prefix: "../x"}); classOf(err) != plugin.ClassPermanent {
		t.Fatalf("a prefix with .. was accepted: %v", err)
	}
}

func TestListOfEmptyStorage(t *testing.T) {
	s, _ := newStorage(t, nil)
	page, err := s.List(context.Background(), storage.ListRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Objects) != 0 || page.NextToken != "" {
		t.Errorf("page %+v", page)
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

func TestGetDeleteRoundTrip(t *testing.T) {
	ctx := context.Background()
	s, _ := newStorage(t, nil)
	body := []byte("read me")
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
	if err := s.Delete(ctx, "get/x.bin"); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, "get/x.bin"); err != nil {
		t.Errorf("second Delete must be idempotent: %v", err)
	}
	if _, err := s.Stat(ctx, "get/x.bin"); classOf(err) != plugin.ClassNotFound {
		t.Errorf("Stat after Delete: %v", err)
	}
	if err := s.Get(ctx, "get/x.bin", io.Discard); classOf(err) != plugin.ClassNotFound {
		t.Errorf("Get of an absent key: %v", err)
	}
}

func TestMissingCredentialFileIsPermanent(t *testing.T) {
	s, _ := newStorage(t, func(raw map[string]any) {
		raw["access_key_id"] = map[string]any{"file": filepath.Join(t.TempDir(), "absent")}
	})
	_, err := s.Stat(context.Background(), "x.bin")
	if err == nil || classOf(err) != plugin.ClassPermanent {
		t.Fatalf("a missing credential file: got %v", err)
	}
}

func TestNewRejectsBadConfig(t *testing.T) {
	r := testRegistry(t)
	good := `{"endpoint": "https://s3.example.com", "region": "ru-central-1", "bucket": "b", "prefix": "root",` +
		` "access_key_id": {"file": "/k"}, "secret_access_key": {"file": "/s"}, "addressing": "path",` +
		` "storage_class": "STANDARD", "request_checksum": "when_required", "part_bytes": 16777216,` +
		` "max_request_bytes": 5368709120, "max_object_bytes": 5497558138880, "request_timeout": "5m"}`
	bad := map[string]string{
		"bad addressing":         strings.Replace(good, `"path"`, `"banana"`, 1),
		"bad checksum mode":      strings.Replace(good, `"when_required"`, `"always"`, 1),
		"bad storage class":      strings.Replace(good, `"STANDARD"`, `"GOLD"`, 1),
		"part below the minimum": strings.Replace(good, `"part_bytes": 16777216`, `"part_bytes": 1048576`, 1),
		"part above request cap": strings.Replace(good, `"part_bytes": 16777216`, `"part_bytes": 8589934592`, 1),
		"empty bucket":           strings.Replace(good, `"bucket": "b"`, `"bucket": ""`, 1),
		"empty region":           strings.Replace(good, `"region": "ru-central-1"`, `"region": ""`, 1),
		"empty storage class":    strings.Replace(good, `"STANDARD"`, `""`, 1),
		"empty key file":         strings.Replace(good, `{"file": "/k"}`, `{"file": ""}`, 1),
		"empty secret file":      strings.Replace(good, `{"file": "/s"}`, `{"file": ""}`, 1),
		"prefix with slash":      strings.Replace(good, `"prefix": "root"`, `"prefix": "/root"`, 1),
		"not a URL":              strings.Replace(good, `"https://s3.example.com"`, `"s3.example.com"`, 1),
		"zero request timeout":   strings.Replace(good, `"5m"`, `"0s"`, 1),
		"zero max request":       strings.Replace(good, `"max_request_bytes": 5368709120`, `"max_request_bytes": 0`, 1),
		"object below request":   strings.Replace(good, `"max_object_bytes": 5497558138880`, `"max_object_bytes": 1`, 1),
		"unknown member":         strings.Replace(good, `{`, `{"zzz": 1,`, 1),
	}
	for name, raw := range bad {
		if _, err := r.BuildAny(context.Background(), "storage", "s3", jsontext.Value(raw), plugin.Deps{Instance: "s3"}); err == nil {
			t.Errorf("%s: config accepted: %s", name, raw)
		}
	}
	if _, err := r.BuildAny(context.Background(), "storage", "s3", jsontext.Value(good), plugin.Deps{Instance: "s3"}); err != nil {
		t.Errorf("the good config was rejected: %v", err)
	}
	supported := strings.Replace(good, `"when_required"`, `"when_supported"`, 1)
	if _, err := r.BuildAny(context.Background(), "storage", "s3", jsontext.Value(supported), plugin.Deps{Instance: "s3"}); err != nil {
		t.Errorf("a when_supported config was rejected: %v", err)
	}
}

func TestRegisterRejectsCanceledContext(t *testing.T) {
	f := newFakeS3(t, testBucket)
	body, err := json.Marshal(baseConfig(t, f, nil))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := plugin.Build(ctx, testRegistry(t), storage.Kind, "s3", body,
		plugin.Deps{Instance: "s3", Log: slog.New(slog.DiscardHandler)}); err == nil {
		t.Fatal("a cancelled context built a storage")
	}
}

func TestCanceledContexts(t *testing.T) {
	s, _ := newStorage(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Describe(ctx); err == nil {
		t.Error("Describe")
	}
	if _, err := s.Put(ctx, storage.PutRequest{Key: "k", Size: 1}, strings.NewReader("x")); err == nil {
		t.Error("Put")
	}
	if _, err := s.Stat(ctx, "k"); err == nil {
		t.Error("Stat")
	}
	if _, err := s.List(ctx, storage.ListRequest{}); err == nil {
		t.Error("List")
	}
	if err := s.Delete(ctx, "k"); err == nil {
		t.Error("Delete")
	}
	if err := s.Get(ctx, "k", io.Discard); err == nil {
		t.Error("Get")
	}
	if err := checkerOf(t, s).Check(ctx); err == nil {
		t.Error("Check")
	}
}

// TestOperationsReportAnUnreadableSecret: the credential files are read per
// call, so a secret that cannot be read fails the call, never the construction.
func TestOperationsReportAnUnreadableSecret(t *testing.T) {
	s, _ := newStorage(t, func(raw map[string]any) {
		raw["secret_access_key"] = map[string]any{"file": filepath.Join(t.TempDir(), "absent")}
	})
	ctx := context.Background()
	if _, err := s.Put(ctx, storage.PutRequest{Key: "k", Size: 1}, strings.NewReader("x")); err == nil {
		t.Error("Put with an unreadable secret succeeded")
	}
	if _, err := s.Stat(ctx, "k"); err == nil {
		t.Error("Stat with an unreadable secret succeeded")
	}
	if _, err := s.List(ctx, storage.ListRequest{}); err == nil {
		t.Error("List with an unreadable secret succeeded")
	}
	if err := s.Delete(ctx, "k"); err == nil {
		t.Error("Delete with an unreadable secret succeeded")
	}
	if err := s.Get(ctx, "k", io.Discard); err == nil {
		t.Error("Get with an unreadable secret succeeded")
	}
	if err := checkerOf(t, s).Check(ctx); err == nil {
		t.Error("Check with an unreadable secret succeeded")
	}
}

// TestPutRejectsASizeTheHeadDisagreesWith: a write is successful only once
// HeadObject reports the committed size.
func TestPutRejectsASizeTheHeadDisagreesWith(t *testing.T) {
	ctx := context.Background()
	s, f := newStorage(t, nil)
	f.headWrongSize = true
	body := []byte("x")
	_, err := s.Put(ctx, storage.PutRequest{Key: "size.bin", Size: int64(len(body))}, bytes.NewReader(body))
	if err == nil || classOf(err) != plugin.ClassTransient {
		t.Fatalf("a head that disagrees with the sent size: got %v", err)
	}
}

func TestListDropsKeysOutsideTheRoot(t *testing.T) {
	s, f := newStorage(t, nil)
	f.listForeign = true
	page, err := s.List(context.Background(), storage.ListRequest{})
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range page.Objects {
		if strings.HasPrefix(o.Key, "outside") {
			t.Errorf("a key outside the storage root was listed: %+v", o)
		}
	}
}

func TestListFailureIsTransient(t *testing.T) {
	s, f := newStorage(t, nil)
	f.failAlways["list"] = http.StatusInternalServerError
	if _, err := s.List(context.Background(), storage.ListRequest{}); err == nil || classOf(err) != plugin.ClassTransient {
		t.Fatalf("a failed listing: got %v", err)
	}
}

func TestDeleteFailureIsTransient(t *testing.T) {
	s, f := newStorage(t, nil)
	f.failAlways["delete"] = http.StatusInternalServerError
	if err := s.Delete(context.Background(), "k"); err == nil || classOf(err) != plugin.ClassTransient {
		t.Fatalf("a failed delete: got %v", err)
	}
}

func TestGetFailureIsTransient(t *testing.T) {
	s, f := newStorage(t, nil)
	f.failAlways["get"] = http.StatusInternalServerError
	if err := s.Get(context.Background(), "k", io.Discard); err == nil || classOf(err) != plugin.ClassTransient {
		t.Fatalf("a failed download: got %v", err)
	}
}

// TestGetCancelMidBodyReturnsPromptly cancels while the object's body is
// already flowing — not before the request starts, which would prove nothing
// but the first ctx check.
func TestGetCancelMidBodyReturnsPromptly(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, f := newStorage(t, nil)
	body := bytes.Repeat([]byte("x"), 4096)
	if _, err := s.Put(ctx, storage.PutRequest{Key: "stall.bin", Size: int64(len(body))}, bytes.NewReader(body)); err != nil {
		t.Fatal(err)
	}
	f.getStall = true
	dst := &signalWriter{done: make(chan struct{})}
	done := make(chan error, 1)
	go func() { done <- s.Get(ctx, "stall.bin", dst) }()
	// The cancel has to land inside the read, not before the answer began: the
	// first byte written to the destination is what proves the call is past its
	// response headers and inside io.Copy.
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

// newPattern returns deterministic bytes for upload tests.
func newPattern(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return b
}

func newRandomHex(t *testing.T, n int) string {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
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
