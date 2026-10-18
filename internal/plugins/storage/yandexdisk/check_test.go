package yandexdisk_test

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

// TestCheckAbsentRootIsOK is the first-run case: the root does not exist yet,
// and that is not an error — Put creates it. The check must be read-only, must
// make exactly one request, and must never reach the old archives outside the
// configured root.
func TestCheckAbsentRootIsOK(t *testing.T) {
	ctx := context.Background()
	s, f := newStorage(t, nil)
	f.putObject("app:/Backups/2026/09/backup-2026-09-14.tar.zst", []byte("old archive"))
	if err := checkerOf(t, s).Check(ctx); err != nil {
		t.Fatalf("Check on a first run (no root yet): %v", err)
	}
	if got := f.Count("stat"); got != 1 {
		t.Errorf("stat requests: got %d, want exactly 1", got)
	}
	for _, op := range []string{"mkdir", "list", "uploadhref", "putdata", "download", "getdata", "delete"} {
		if got := f.Count(op); got != 0 {
			t.Errorf("%s requests during Check: got %d, want 0 — Check must be read-only", op, got)
		}
	}
	if f.folders["app:/cluster-backup"] {
		t.Error("Check created the root folder; only Put may")
	}
	if got, ok := f.fileBody("app:/Backups/2026/09/backup-2026-09-14.tar.zst"); !ok || string(got) != "old archive" {
		t.Error("Check touched the old archives outside the root")
	}
}

func TestCheckRootFolderIsOK(t *testing.T) {
	s, f := newStorage(t, nil)
	// Seeding one object creates the root folder and every folder on the way:
	// the state a second run finds.
	f.putObject("app:/cluster-backup/runs/2026/x.bin", []byte("x"))
	if err := checkerOf(t, s).Check(context.Background()); err != nil {
		t.Fatalf("Check on an existing root folder: %v", err)
	}
}

// TestCheckRootFileIsPermanent pins the probe to the configured root: a path
// occupied by a file is the wrong kind of thing, and the error names it.
func TestCheckRootFileIsPermanent(t *testing.T) {
	s, f := newStorage(t, nil)
	f.putObject("app:/cluster-backup", []byte("not a folder"))
	err := checkerOf(t, s).Check(context.Background())
	if err == nil {
		t.Fatal("Check accepted a root that is a file")
	}
	if class := classOf(err); class != plugin.ClassPermanent {
		t.Errorf("class %q, want permanent (never not_found): %v", class, err)
	}
	if !strings.Contains(err.Error(), "app:/cluster-backup") {
		t.Errorf("the error does not name the root: %v", err)
	}
}

// TestCheckUnreadableAnswerIsTransient: a 200 whose answer cannot be read
// proves nothing about the root — an existence probe that cannot tell must
// never come back as not_found.
func TestCheckUnreadableAnswerIsTransient(t *testing.T) {
	for name, body := range map[string]string{
		"not a resource": `{"type": 5}`,
		"unknown type":   `{"type": "other"}`,
	} {
		t.Run(name, func(t *testing.T) {
			s, f := newStorage(t, nil)
			f.statBody = body
			err := checkerOf(t, s).Check(context.Background())
			if class := classOf(err); err == nil || class != plugin.ClassTransient {
				t.Fatalf("class %q err %v, want transient", class, err)
			}
			if got := f.Count("stat"); got != 1 {
				t.Errorf("stat requests: got %d, want exactly 1", got)
			}
		})
	}
}

func TestCheckUnauthorizedIsPermanent(t *testing.T) {
	s, f := newStorage(t, nil)
	f.failAlways["stat"] = http.StatusUnauthorized
	err := checkerOf(t, s).Check(context.Background())
	if err == nil || classOf(err) != plugin.ClassPermanent {
		t.Fatalf("401: got %v", err)
	}
	if got := f.Count("stat"); got != 1 {
		t.Errorf("stat requests: got %d, want exactly 1", got)
	}
}

func TestCheckForbiddenIsPermanent(t *testing.T) {
	s, f := newStorage(t, nil)
	f.failAlways["stat"] = http.StatusForbidden
	if err := checkerOf(t, s).Check(context.Background()); err == nil || classOf(err) != plugin.ClassPermanent {
		t.Fatalf("403: got %v", err)
	}
}

func TestCheckServerErrorIsOneAttempt(t *testing.T) {
	s, f := newStorage(t, nil)
	f.failAlways["stat"] = http.StatusInternalServerError
	err := checkerOf(t, s).Check(context.Background())
	if err == nil || classOf(err) != plugin.ClassTransient {
		t.Fatalf("500: got %v", err)
	}
	if got := f.Count("stat"); got != 1 {
		t.Errorf("stat requests: got %d, want exactly 1 (no retry inside the plugin)", got)
	}
}

func TestCheckRateLimitPassesRetryAfter(t *testing.T) {
	s, f := newStorage(t, nil)
	f.failAlways["stat"] = http.StatusTooManyRequests
	f.retryAfter["stat"] = 7
	err := checkerOf(t, s).Check(context.Background())
	if err == nil {
		t.Fatal("a 429 was accepted")
	}
	class, after := plugin.ClassOf(err)
	if class != plugin.ClassTransient || after != 7*time.Second {
		t.Errorf("got class %q after %s, want transient after 7s: %v", class, after, err)
	}
	if got := f.Count("stat"); got != 1 {
		t.Errorf("stat requests: got %d, want exactly 1", got)
	}
}

// TestCheckReadsTokenAtCheckTime: the token file is read per check, never at
// construction — a missing file fails this check alone, and a token that
// appears later is picked up without a rebuilt instance.
func TestCheckReadsTokenAtCheckTime(t *testing.T) {
	f := newFakeDisk(t)
	token := filepath.Join(t.TempDir(), "token")
	raw := baseConfig(t, f)
	raw["token"] = map[string]any{"file": token}
	c := checkerOf(t, newStorageFrom(t, raw))
	if err := c.Check(context.Background()); err == nil || classOf(err) != plugin.ClassPermanent {
		t.Fatalf("Check with a token file that is not there yet: got %v", err)
	}
	if err := os.WriteFile(token, []byte("test-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := c.Check(context.Background()); err != nil {
		t.Fatalf("Check after the token appeared: %v", err)
	}
}

func TestCheckCancelReturnsPromptly(t *testing.T) {
	s, f := newStorage(t, nil)
	c := checkerOf(t, s)
	if err := plugintest.MustReturnWithin(t, 5*time.Second, "Check", c.Check); err == nil {
		t.Fatal("Check with a cancelled context returned success")
	}
	// Cancelled while the metadata request is in flight — not before it starts,
	// which would prove nothing but the first ctx check.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.hangStat = true
	done := make(chan error, 1)
	go func() { done <- c.Check(ctx) }()
	waitFor(t, func() bool { return f.Count("stat") >= 1 }, 5*time.Second)
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
