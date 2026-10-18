package extcmd_test

import (
	"context"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/extcmd"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
)

func TestBoundedBufferKeepsTheLimit(t *testing.T) {
	b := extcmd.NewBoundedBuffer()
	chunk := strings.Repeat("x", 8<<10)
	for range 4 {
		if _, err := b.Write([]byte(chunk)); err != nil {
			t.Fatal(err)
		}
	}
	if got := len(b.String()); got > 16<<10 {
		t.Fatalf("buffer kept %d bytes, want at most %d", got, 16<<10)
	}
	if b.String() == "" {
		t.Fatal("buffer dropped everything")
	}
}

func TestReplaceEnvReplacesInsteadOfShadowing(t *testing.T) {
	env := []string{"PATH=/bin", "PGPASSWORD=old", "OTHER=1"}
	got := extcmd.ReplaceEnv(env, "PGPASSWORD=new")
	joined := strings.Join(got, " ")
	if strings.Contains(joined, "old") {
		t.Fatalf("the inherited value survived: %v", got)
	}
	if !strings.Contains(joined, "PGPASSWORD=new") || !strings.Contains(joined, "OTHER=1") {
		t.Fatalf("env = %v", got)
	}
	// exec reads an environment literally: without the replacement the first
	// match would win, so the count matters, not just the presence.
	if n := strings.Count(joined, "PGPASSWORD="); n != 1 {
		t.Fatalf("%d PGPASSWORD entries, want exactly one", n)
	}
}

func TestClassifyExit(t *testing.T) {
	permanent := regexp.MustCompile(`refused`)
	transient := regexp.MustCompile(`timeout`)
	fail := errors.New("exit status 1")
	if class, _ := plugin.ClassOf(extcmd.ClassifyExit("bin", fail, "connection refused", permanent, transient)); class != plugin.ClassPermanent {
		t.Fatalf("class = %q, want permanent", class)
	}
	if class, _ := plugin.ClassOf(extcmd.ClassifyExit("bin", fail, "i/o timeout", permanent, transient)); class != plugin.ClassTransient {
		t.Fatalf("class = %q, want transient", class)
	}
	if class, _ := plugin.ClassOf(extcmd.ClassifyExit("bin", fail, "something else", permanent, transient)); class != "" {
		t.Fatalf("class = %q, want unclassified", class)
	}
	if msg := extcmd.ClassifyExit("pg_dump", fail, "boom", permanent, transient).Error(); !strings.Contains(msg, "pg_dump failed: boom") {
		t.Fatalf("message = %q", msg)
	}
}

func TestOutputReturnsStdoutAndClassifies(t *testing.T) {
	plain := regexp.MustCompile(`never`)
	out, err := extcmd.Output(context.Background(), exec.Command("sh", "-c", "echo hello"), classifyPlain(plain))
	if err != nil || strings.TrimSpace(out) != "hello" {
		t.Fatalf("Output = %q, %v", out, err)
	}
	_, err = extcmd.Output(context.Background(), exec.Command("sh", "-c", "echo bad >&2; exit 3"), classifyPlain(plain))
	if err == nil || !strings.Contains(err.Error(), "bad") {
		t.Fatalf("a failed command reported %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// The command itself is context-bound by its caller; a cancelled context
	// is reported as ctx.Err() rather than as the killed process's failure.
	if _, err := extcmd.Output(ctx, exec.CommandContext(ctx, "sh", "-c", "echo x"), classifyPlain(plain)); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled context = %v, want context.Canceled", err)
	}
}

func classifyPlain(permanent *regexp.Regexp) func(bin string, err error, detail string) error {
	return func(bin string, err error, detail string) error {
		return extcmd.ClassifyExit(bin, err, detail, permanent, regexp.MustCompile(`never`))
	}
}

func TestRunMergesStreamsAndHonoursTimeout(t *testing.T) {
	out, err := extcmd.Run(context.Background(), time.Minute, "/bin/sh", nil, "-c", "echo one; echo two >&2")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "one") || !strings.Contains(out, "two") {
		t.Fatalf("merged output = %q", out)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := extcmd.Run(ctx, time.Minute, "/bin/sh", nil, "-c", "echo x"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled run = %v, want context.Canceled", err)
	}
}

func TestWaitTCP(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	host, port, _ := net.SplitHostPort(l.Addr().String())
	if err := extcmd.WaitTCP(context.Background(), host, port, 2, 20*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := extcmd.WaitTCP(context.Background(), "127.0.0.1", "1", 2, 10*time.Millisecond); err == nil {
		t.Fatal("an unreachable address was accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := extcmd.WaitTCP(ctx, "127.0.0.1", "1", 5, time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled wait = %v, want context.Canceled", err)
	}
}

func TestClassifierBindsThePatterns(t *testing.T) {
	classify := extcmd.Classifier(regexp.MustCompile(`refused`), regexp.MustCompile(`timeout`))
	fail := errors.New("exit status 1")
	if class, _ := plugin.ClassOf(classify("psql", fail, "connection refused")); class != plugin.ClassPermanent {
		t.Fatalf("class = %q, want permanent", class)
	}
	if class, _ := plugin.ClassOf(classify("psql", fail, "i/o timeout")); class != plugin.ClassTransient {
		t.Fatalf("class = %q, want transient", class)
	}
}

func TestFindBinaryNamesWhatItLookedFor(t *testing.T) {
	path, err := extcmd.FindBinary("sh")
	if err != nil {
		t.Fatalf("FindBinary(sh) = %v", err)
	}
	if !filepath.IsAbs(path) {
		t.Fatalf("FindBinary returned %q, want an absolute path", path)
	}
	_, err = extcmd.FindBinary("go-cluster-backup-no-such-binary")
	if err == nil {
		t.Fatal("a missing binary was accepted")
	}
	if !strings.Contains(err.Error(), "go-cluster-backup-no-such-binary") {
		t.Fatalf("error does not name the binary: %v", err)
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassPermanent {
		t.Fatalf("class = %q, want permanent", class)
	}
}

// TestWaitTCPReturnsOnCancellationDuringTheWait: a cancelled context interrupts
// the wait between attempts, not only before the first dial — an operator's
// interrupt must not wait out the remaining attempts.
func TestWaitTCPReturnsOnCancellationDuringTheWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	if err := extcmd.WaitTCP(ctx, "127.0.0.1", "1", 50, 200*time.Millisecond); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled wait = %v, want context.Canceled", err)
	}
}

func TestReadPasswordTrimsTheTrailingNewline(t *testing.T) {
	path := filepath.Join(t.TempDir(), "password")
	if err := os.WriteFile(path, []byte("secret-value\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := extcmd.ReadPassword(path)
	if err != nil || got != "secret-value" {
		t.Fatalf("ReadPassword = %q, %v", got, err)
	}
	if _, err := extcmd.ReadPassword(filepath.Join(t.TempDir(), "gone")); err == nil {
		t.Fatal("a missing password file was accepted")
	}
}

func TestCheckScratchDir(t *testing.T) {
	if err := extcmd.CheckScratchDir(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	missing := filepath.Join(t.TempDir(), "gone")
	if err := extcmd.CheckScratchDir(missing); err == nil || !strings.Contains(err.Error(), "scratch_dir") {
		t.Fatalf("missing dir = %v", err)
	}
	file := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := extcmd.CheckScratchDir(file); err == nil {
		t.Fatal("a file passed as a scratch dir")
	}
	if os.Getuid() != 0 {
		readOnly := t.TempDir()
		if err := os.Chmod(readOnly, 0o500); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(readOnly, 0o700) })
		if err := extcmd.CheckScratchDir(readOnly); err == nil || !strings.Contains(err.Error(), "not writable") {
			t.Fatalf("read-only dir = %v", err)
		}
	}
}
