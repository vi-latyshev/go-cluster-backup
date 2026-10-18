package s3_test

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin/plugintest"
	"github.com/vi-latyshev/go-cluster-backup/internal/storage"
)

func checkerOf(t *testing.T, s storage.Storage) plugin.Checker {
	t.Helper()
	c, ok := s.(plugin.Checker)
	if !ok {
		t.Fatal("the storage implements no Check")
	}
	return c
}

// TestCheckOK is the healthy case: one read-only listing probe, no other
// request, and nothing written.
func TestCheckOK(t *testing.T) {
	ctx := context.Background()
	s, f := newStorage(t, nil)
	if err := checkerOf(t, s).Check(ctx); err != nil {
		t.Fatalf("Check against a reachable bucket: %v", err)
	}
	if got := f.Count("list"); got != 1 {
		t.Errorf("list requests: got %d, want exactly 1", got)
	}
	for _, op := range []string{"put", "head", "get", "create", "part", "complete", "abort", "delete"} {
		if got := f.Count(op); got != 0 {
			t.Errorf("%s requests during Check: got %d, want 0 — Check must be read-only", op, got)
		}
	}
	if left := f.OpenUploads(); len(left) != 0 {
		t.Errorf("multipart uploads opened by Check: %v", left)
	}
}

// TestCheckAbsentBucketIsPermanent: a bucket that does not exist fails the
// instance permanently — never as not_found — and the error names the bucket.
func TestCheckAbsentBucketIsPermanent(t *testing.T) {
	const wrong = "no-such-bucket-4c1f"
	s, f := newStorage(t, func(raw map[string]any) { raw["bucket"] = wrong })
	err := checkerOf(t, s).Check(context.Background())
	if err == nil {
		t.Fatal("Check against a bucket that does not exist returned success")
	}
	if class := classOf(err); class != plugin.ClassPermanent {
		t.Errorf("class %q, want permanent (never not_found): %v", class, err)
	}
	if !strings.Contains(err.Error(), wrong) {
		t.Errorf("the error does not name the bucket: %v", err)
	}
	if got := f.Count("wrongbucket"); got != 1 {
		t.Errorf("requests naming the wrong bucket: got %d, want exactly 1", got)
	}
}

func TestCheckUnauthorizedIsPermanent(t *testing.T) {
	s, f := newStorage(t, nil)
	f.failAlways["list"] = http.StatusUnauthorized
	err := checkerOf(t, s).Check(context.Background())
	if err == nil || classOf(err) != plugin.ClassPermanent {
		t.Fatalf("401: got %v", err)
	}
	if got := f.Count("list"); got != 1 {
		t.Errorf("list requests: got %d, want exactly 1", got)
	}
}

func TestCheckForbiddenIsPermanent(t *testing.T) {
	s, f := newStorage(t, nil)
	f.failAlways["list"] = http.StatusForbidden
	if err := checkerOf(t, s).Check(context.Background()); err == nil || classOf(err) != plugin.ClassPermanent {
		t.Fatalf("403: got %v", err)
	}
}

func TestCheckServerErrorIsOneAttempt(t *testing.T) {
	s, f := newStorage(t, nil)
	f.failAlways["list"] = http.StatusInternalServerError
	err := checkerOf(t, s).Check(context.Background())
	if err == nil || classOf(err) != plugin.ClassTransient {
		t.Fatalf("500: got %v", err)
	}
	if got := f.Count("list"); got != 1 {
		t.Errorf("list requests: got %d, want exactly 1 (no retry inside the plugin)", got)
	}
}

func TestCheckRateLimitPassesRetryAfter(t *testing.T) {
	s, f := newStorage(t, nil)
	f.failAlways["list"] = http.StatusTooManyRequests
	f.retryAfter["list"] = 3
	err := checkerOf(t, s).Check(context.Background())
	if err == nil {
		t.Fatal("a 429 was accepted")
	}
	class, after := plugin.ClassOf(err)
	if class != plugin.ClassTransient || after != 3*time.Second {
		t.Errorf("got class %q after %s, want transient after 3s: %v", class, after, err)
	}
	if got := f.Count("list"); got != 1 {
		t.Errorf("list requests: got %d, want exactly 1", got)
	}
}

// TestCheckReadsCredentialsAtCheckTime: the credential files are read per
// check, never at construction — a missing file fails this check alone, and a
// key that appears later is picked up without a rebuilt instance.
func TestCheckReadsCredentialsAtCheckTime(t *testing.T) {
	f := newFakeS3(t, testBucket)
	keyFile := filepath.Join(t.TempDir(), "key-id")
	secretFile := filepath.Join(t.TempDir(), "secret")
	raw := baseConfig(t, f, nil)
	raw["access_key_id"] = map[string]any{"file": keyFile}
	raw["secret_access_key"] = map[string]any{"file": secretFile}
	c := checkerOf(t, newStorageFrom(t, raw))
	if err := c.Check(context.Background()); err == nil || classOf(err) != plugin.ClassPermanent {
		t.Fatalf("Check with credential files that are not there yet: got %v", err)
	}
	if err := os.WriteFile(keyFile, []byte("tenant:key-id"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(secretFile, []byte("secret-key"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := c.Check(context.Background()); err != nil {
		t.Fatalf("Check after the credentials appeared: %v", err)
	}
}

func TestCheckCancelReturnsPromptly(t *testing.T) {
	s, f := newStorage(t, nil)
	c := checkerOf(t, s)
	if err := plugintest.MustReturnWithin(t, 5*time.Second, "Check", c.Check); err == nil {
		t.Fatal("Check with a cancelled context returned success")
	}
	// Cancelled while the listing request is in flight — not before it starts,
	// which would prove nothing but the first ctx check.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.hang["list"] = true
	done := make(chan error, 1)
	go func() { done <- c.Check(ctx) }()
	waitFor(t, func() bool { return f.Count("list") >= 1 }, 5*time.Second)
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Check with a cancelled context returned success")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Check did not return after its context was cancelled")
	}
}
