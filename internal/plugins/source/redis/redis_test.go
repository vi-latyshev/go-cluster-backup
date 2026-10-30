package redis_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin/plugintest"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source/redis"
	"github.com/vi-latyshev/go-cluster-backup/internal/source"
	"github.com/vi-latyshev/go-cluster-backup/internal/source/sourcetest"
)

func testRegistry(t *testing.T) *plugin.Registry {
	t.Helper()
	r := plugin.NewRegistry()
	if err := redis.Register(r); err != nil {
		t.Fatal(err)
	}
	return r
}

func build(t *testing.T, cfg string) (source.Source, error) {
	t.Helper()
	return plugin.Build(context.Background(), testRegistry(t), source.Kind, "redis",
		jsontext.Value(cfg), plugin.Deps{Instance: "redis", Log: slog.New(slog.DiscardHandler)})
}

func config(t *testing.T, addr string) string {
	t.Helper()
	return configWithScratch(t, addr, t.TempDir())
}

func configWithScratch(t *testing.T, addr, scratch string) string {
	t.Helper()
	return fmt.Sprintf(`{"address": %q, "password": {"file": %q}, "scratch_dir": %q, "connect_attempts": 2, "connect_interval": "20ms"}`,
		addr, passwordFile(t), scratch)
}

func passwordFile(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "password")
	if err := os.WriteFile(path, []byte("rd-secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func listener(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			_ = c.Close()
		}
	}()
	return l.Addr().String()
}

// stubs writes a fake redis-cli that records argv and env, answers PING from
// out.redis-cli, and writes the RDB into the path given after --rdb.
func stubs(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("STUB_LOG", t.TempDir())
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	script := `#!/bin/sh
printf '%s\n' "$@" > "$STUB_LOG/argv"
env | grep -E '^REDISCLI_AUTH=' > "$STUB_LOG/env"
prev=""
for a in "$@"; do
  if [ "$prev" = "--rdb" ]; then
    if [ ! -f "$STUB_LOG/norDB" ]; then printf 'RDB-BYTES' > "$a"; fi
    if [ -f "$STUB_LOG/unlink" ]; then rm -f "$a"; fi
  fi
  prev="$a"
done
cat "$STUB_LOG/out" 2>/dev/null
if [ -f "$STUB_LOG/stderr" ]; then cat "$STUB_LOG/stderr" >&2; fi
if [ -f "$STUB_LOG/sleep" ]; then : > "$STUB_LOG/started"; exec sleep "$(cat "$STUB_LOG/sleep")"; fi
code=0; if [ -f "$STUB_LOG/exit" ]; then code=$(cat "$STUB_LOG/exit"); fi
exit "$code"
`
	if err := os.WriteFile(filepath.Join(dir, "redis-cli"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
}

func stubSet(t *testing.T, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(os.Getenv("STUB_LOG"), name), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func stubLog(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(os.Getenv("STUB_LOG"), name))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// waitStarted waits for the stub to report that it has begun the work a test
// wants to interrupt, instead of betting a fixed sleep that it probably has.
func waitStarted(t *testing.T) {
	t.Helper()
	started := filepath.Join(os.Getenv("STUB_LOG"), "started")
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(started); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the redis-cli stub never started")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestGeneralConformance(t *testing.T) {
	plugintest.CheckSpec(t, testRegistry(t))
}

func TestSourceConformance(t *testing.T) {
	sourcetest.Run(t, func(t *testing.T) source.Source {
		stubs(t)
		s, err := build(t, config(t, listener(t)))
		if err != nil {
			t.Fatal(err)
		}
		return s
	})
}

func TestDescribe(t *testing.T) {
	stubs(t)
	s, err := build(t, config(t, listener(t)))
	if err != nil {
		t.Fatal(err)
	}
	tr, err := s.Describe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if tr.Ext != ".rdb" {
		t.Errorf("Ext = %q", tr.Ext)
	}
	if !strings.HasPrefix(tr.Origin, "redis://") {
		t.Errorf("Origin = %q", tr.Origin)
	}
	if tr.Sealed {
		t.Error("Sealed is true for an RDB snapshot")
	}
}

func TestDumpArgsCarryNoPassword(t *testing.T) {
	stubs(t)
	addr := listener(t)
	scratch := t.TempDir()
	s, err := build(t, configWithScratch(t, addr, scratch))
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := s.Dump(context.Background(), &buf); err != nil {
		t.Fatal(err)
	}
	if buf.String() != "RDB-BYTES" {
		t.Fatalf("dumped %q", buf.String())
	}
	host, port, _ := net.SplitHostPort(addr)
	argv := strings.Split(strings.TrimSpace(stubLog(t, "argv")), "\n")
	if len(argv) != 6 || argv[0] != "-h" || argv[1] != host || argv[2] != "-p" || argv[3] != port || argv[4] != "--rdb" {
		t.Fatalf("argv = %q", argv)
	}
	if !strings.HasPrefix(argv[5], scratch+string(os.PathSeparator)) || !strings.Contains(argv[5], "redis-rdb-") {
		t.Fatalf("argv = %q, want the RDB temp file in the scratch dir %q", argv, scratch)
	}
	for _, arg := range argv {
		if strings.Contains(arg, "rd-secret") {
			t.Fatalf("the password appears on argv: %q", argv)
		}
	}
	env := stubLog(t, "env")
	if !strings.Contains(env, "REDISCLI_AUTH=rd-secret") {
		t.Fatalf("REDISCLI_AUTH not passed through env: %q", env)
	}
}

func TestDumpConfigPasswordOutranksTheEnvironment(t *testing.T) {
	stubs(t)
	t.Setenv("REDISCLI_AUTH", "inherited-and-wrong")
	s, err := build(t, config(t, listener(t)))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Dump(context.Background(), io.Discard); err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(stubLog(t, "env"), "\n") {
		if strings.HasPrefix(line, "REDISCLI_AUTH=") && line != "REDISCLI_AUTH=rd-secret" {
			t.Fatalf("the environment shadowed the configured password: %q", line)
		}
	}
}

func TestDumpRefusedPasswordIsPermanent(t *testing.T) {
	stubs(t)
	stubSet(t, "exit", "1")
	stubSet(t, "stderr", "AUTH failed: WRONGPASS invalid username-password pair or user is disabled.")
	s, err := build(t, config(t, listener(t)))
	if err != nil {
		t.Fatal(err)
	}
	err = s.Dump(context.Background(), io.Discard)
	if err == nil {
		t.Fatal("a refused password was not an error")
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassPermanent {
		t.Fatalf("class = %q, want permanent (%v)", class, err)
	}
}

func TestDumpConnectionFailureIsTransient(t *testing.T) {
	stubs(t)
	stubSet(t, "exit", "1")
	stubSet(t, "stderr", "Could not connect to Redis at 10.0.0.1:6379: Connection refused")
	s, err := build(t, config(t, listener(t)))
	if err != nil {
		t.Fatal(err)
	}
	err = s.Dump(context.Background(), io.Discard)
	if class, _ := plugin.ClassOf(err); class != plugin.ClassTransient {
		t.Fatalf("class = %q, want transient (%v)", class, err)
	}
}

func TestDumpUnreachableIsTransient(t *testing.T) {
	stubs(t)
	s, err := build(t, config(t, "127.0.0.1:1"))
	if err != nil {
		t.Fatal(err)
	}
	err = s.Dump(context.Background(), io.Discard)
	if class, _ := plugin.ClassOf(err); class != plugin.ClassTransient {
		t.Fatalf("class = %q, want transient (%v)", class, err)
	}
}

func TestDumpLeavesNoTempFile(t *testing.T) {
	stubs(t)
	scratch := t.TempDir()
	s, err := build(t, configWithScratch(t, listener(t), scratch))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Dump(context.Background(), io.Discard); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(scratch)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("temp files left in the scratch dir: %v", entries)
	}
}

func TestDumpWriterFailureStopsTheCopy(t *testing.T) {
	stubs(t)
	s, err := build(t, config(t, listener(t)))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Dump(context.Background(), &plugintest.ErrWriter{}); err == nil {
		t.Fatal("a failing destination was ignored")
	}
}

func TestDumpReturnsOnCancellation(t *testing.T) {
	stubs(t)
	stubSet(t, "sleep", "10")
	s, err := build(t, config(t, listener(t)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Dump(ctx, io.Discard) }()
	waitStarted(t)
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("a cancelled dump returned success")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Dump did not return after cancellation")
	}
}

func TestCheckPingsWithThePassword(t *testing.T) {
	stubs(t)
	stubSet(t, "out", "PONG")
	s, err := build(t, config(t, listener(t)))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.(plugin.Checker).Check(context.Background()); err != nil {
		t.Fatal(err)
	}
	argv := strings.Split(strings.TrimSpace(stubLog(t, "argv")), "\n")
	if argv[len(argv)-1] != "PING" {
		t.Fatalf("argv = %q", argv)
	}
	for _, arg := range argv {
		if strings.Contains(arg, "rd-secret") {
			t.Fatalf("the password appears on argv: %q", argv)
		}
	}

	stubSet(t, "out", "AUTH failed: WRONGPASS")
	if err := s.(plugin.Checker).Check(context.Background()); err == nil {
		t.Fatal("a refused password passed Check")
	}
}

func TestCheckWithoutBinary(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	s, err := build(t, config(t, "127.0.0.1:1"))
	if err != nil {
		t.Fatal(err)
	}
	err = s.(plugin.Checker).Check(context.Background())
	if err == nil || !strings.Contains(err.Error(), "redis-cli") {
		t.Fatalf("Check = %v, want a missing redis-cli named", err)
	}
}

func TestCheckJudgesTheScratchDir(t *testing.T) {
	stubs(t)
	stubSet(t, "out", "PONG")
	missing := filepath.Join(t.TempDir(), "gone")
	s, err := build(t, configWithScratch(t, listener(t), missing))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.(plugin.Checker).Check(context.Background()); err == nil || !strings.Contains(err.Error(), "scratch_dir") {
		t.Fatalf("Check = %v, want the missing scratch_dir named", err)
	}
	file := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	notDir, err := build(t, configWithScratch(t, listener(t), file))
	if err != nil {
		t.Fatal(err)
	}
	if err := notDir.(plugin.Checker).Check(context.Background()); err == nil || !strings.Contains(err.Error(), "scratch_dir") {
		t.Fatalf("Check = %v, want a scratch_dir that is a file refused", err)
	}
}

func TestNewRejectsBadConfig(t *testing.T) {
	for _, cfg := range []string{
		`{"address": "no-port", "password": {"file": "/p"}, "scratch_dir": "/s", "connect_attempts": 1, "connect_interval": "1s"}`,
		`{"address": "h:notanumber", "password": {"file": "/p"}, "scratch_dir": "/s", "connect_attempts": 1, "connect_interval": "1s"}`,
		`{"address": "h:1", "password": {"file": ""}, "scratch_dir": "/s", "connect_attempts": 1, "connect_interval": "1s"}`,
		`{"address": "h:1", "password": {"file": "/p"}, "connect_attempts": 1, "connect_interval": "1s"}`,
		`{"address": "h:1", "password": {"file": "/p"}, "scratch_dir": "", "connect_attempts": 1, "connect_interval": "1s"}`,
		`{"address": "h:1", "password": {"file": "/p"}, "scratch_dir": "relative", "connect_attempts": 1, "connect_interval": "1s"}`,
		`{"address": "h:1", "password": {"file": "/p"}, "scratch_dir": "/s", "connect_attempts": 0, "connect_interval": "1s"}`,
		`{"address": "h:1", "password": {"file": "/p"}, "scratch_dir": "/s", "connect_attempts": 1, "connect_interval": "0s"}`,
	} {
		if _, err := build(t, cfg); err == nil {
			t.Errorf("config %s accepted", cfg)
		}
	}
}

// slowConfig keeps WaitTCP looping long enough for a test to cancel it: the
// cancellation branch is unreachable once the wait has already given up.
func slowConfig(t *testing.T, addr string) string {
	t.Helper()
	return fmt.Sprintf(`{"address": %q, "password": {"file": %q}, "scratch_dir": %q, "connect_attempts": 20, "connect_interval": "1s"}`,
		addr, passwordFile(t), t.TempDir())
}

// TestNewRespectsCancellation: the constructor refuses to build on a cancelled
// context instead of handing out an instance whose first call would.
func TestNewRespectsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := plugin.Build(ctx, testRegistry(t), source.Kind, "redis",
		jsontext.Value(config(t, "127.0.0.1:1")), plugin.Deps{Instance: "redis", Log: slog.New(slog.DiscardHandler)})
	if err == nil {
		t.Fatal("New accepted a cancelled context")
	}
}

func TestDescribeRespectsCancellation(t *testing.T) {
	stubs(t)
	s, err := build(t, config(t, listener(t)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Describe(ctx); err == nil {
		t.Fatal("Describe accepted a cancelled context")
	}
}

func TestCheckRespectsCancellation(t *testing.T) {
	stubs(t)
	s, err := build(t, config(t, listener(t)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.(plugin.Checker).Check(ctx); err == nil {
		t.Fatal("Check accepted a cancelled context")
	}
}

// TestCheckUnreachableIsTransient: a server that does not answer is worth a
// retry — a Check that classified it permanent would cost the night's snapshot.
func TestCheckUnreachableIsTransient(t *testing.T) {
	stubs(t)
	s, err := build(t, config(t, "127.0.0.1:1"))
	if err != nil {
		t.Fatal(err)
	}
	err = s.(plugin.Checker).Check(context.Background())
	if class, _ := plugin.ClassOf(err); class != plugin.ClassTransient {
		t.Fatalf("class = %q, want transient (%v)", class, err)
	}
}

// TestCheckCancelledDuringConnectReturnsTheContextError: cancellation is not
// the server's fault — a Check that reported the connection failure instead
// would mis-classify the operator's own interrupt.
func TestCheckCancelledDuringConnectReturnsTheContextError(t *testing.T) {
	stubs(t)
	s, err := build(t, slowConfig(t, "127.0.0.1:1"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	// The first attempt fails at once and the wait then sleeps a full second:
	// cancelling 20ms in lands inside it with a fiftyfold margin.
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	if err := s.(plugin.Checker).Check(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Check = %v, want context.Canceled", err)
	}
}

// TestCheckFailsWithoutThePasswordFile: the mounted secret is read at use
// time, and a missing one fails the preflight before any command runs.
func TestCheckFailsWithoutThePasswordFile(t *testing.T) {
	stubs(t)
	pass := passwordFile(t)
	cfg := fmt.Sprintf(`{"address": %q, "password": {"file": %q}, "scratch_dir": %q, "connect_attempts": 2, "connect_interval": "20ms"}`,
		listener(t), pass, t.TempDir())
	s, err := build(t, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(pass); err != nil {
		t.Fatal(err)
	}
	err = s.(plugin.Checker).Check(context.Background())
	if err == nil || !strings.Contains(err.Error(), "reading password file") {
		t.Fatalf("Check = %v, want the missing password file named", err)
	}
	if _, statErr := os.Stat(filepath.Join(os.Getenv("STUB_LOG"), "argv")); statErr == nil {
		t.Fatal("redis-cli ran without a password file")
	}
}

// TestCheckRefusedPingIsPermanent: a PING that exits non-zero with WRONGPASS
// is a refused credential, not a blip — the error class must carry that.
func TestCheckRefusedPingIsPermanent(t *testing.T) {
	stubs(t)
	stubSet(t, "exit", "1")
	stubSet(t, "stderr", "AUTH failed: WRONGPASS invalid username-password pair or user is disabled.")
	s, err := build(t, config(t, listener(t)))
	if err != nil {
		t.Fatal(err)
	}
	err = s.(plugin.Checker).Check(context.Background())
	if class, _ := plugin.ClassOf(err); class != plugin.ClassPermanent {
		t.Fatalf("class = %q, want permanent (%v)", class, err)
	}
}

// TestDumpFailsWithoutThePasswordFile: no password, no snapshot — before any
// command is started.
func TestDumpFailsWithoutThePasswordFile(t *testing.T) {
	stubs(t)
	pass := passwordFile(t)
	cfg := fmt.Sprintf(`{"address": %q, "password": {"file": %q}, "scratch_dir": %q, "connect_attempts": 2, "connect_interval": "20ms"}`,
		listener(t), pass, t.TempDir())
	s, err := build(t, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(pass); err != nil {
		t.Fatal(err)
	}
	if err := s.Dump(context.Background(), io.Discard); err == nil || !strings.Contains(err.Error(), "reading password file") {
		t.Fatalf("Dump = %v, want the missing password file named", err)
	}
}

func TestDumpCancelledDuringConnectReturnsTheContextError(t *testing.T) {
	stubs(t)
	s, err := build(t, slowConfig(t, "127.0.0.1:1"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	// The first attempt fails at once and the wait then sleeps a full second:
	// cancelling 20ms in lands inside it with a fiftyfold margin.
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	if err := s.Dump(ctx, io.Discard); !errors.Is(err, context.Canceled) {
		t.Fatalf("Dump = %v, want context.Canceled", err)
	}
}

// TestDumpScratchDirVanishedIsTransient: the scratch directory disappeared
// after the preflight — the temp file cannot be spooled, and that is a retry,
// not a data defect.
func TestDumpScratchDirVanishedIsTransient(t *testing.T) {
	stubs(t)
	scratch := t.TempDir()
	s, err := build(t, configWithScratch(t, listener(t), scratch))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(scratch); err != nil {
		t.Fatal(err)
	}
	err = s.Dump(context.Background(), io.Discard)
	if class, _ := plugin.ClassOf(err); class != plugin.ClassTransient {
		t.Fatalf("class = %q, want transient (%v)", class, err)
	}
}

// TestDumpRefusesAnEmptySnapshot: redis-cli exiting 0 without a snapshot is
// not an empty dataset — it is a failure that must never be uploaded as one.
func TestDumpRefusesAnEmptySnapshot(t *testing.T) {
	stubs(t)
	stubSet(t, "norDB", "1")
	s, err := build(t, config(t, listener(t)))
	if err != nil {
		t.Fatal(err)
	}
	err = s.Dump(context.Background(), io.Discard)
	if err == nil || !strings.Contains(err.Error(), "empty snapshot") {
		t.Fatalf("Dump = %v, want an empty snapshot refused", err)
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassTransient {
		t.Fatalf("class = %q, want transient (%v)", class, err)
	}
}

// TestDumpFailsWhenTheSnapshotVanishes: the file redis-cli was told to write
// is gone by the time this plugin opens it — that is a retry with a reason,
// never a zero-byte artifact.
func TestDumpFailsWhenTheSnapshotVanishes(t *testing.T) {
	stubs(t)
	stubSet(t, "unlink", "1")
	s, err := build(t, config(t, listener(t)))
	if err != nil {
		t.Fatal(err)
	}
	err = s.Dump(context.Background(), io.Discard)
	if err == nil {
		t.Fatal("a snapshot that vanished before it was read was streamed")
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassTransient {
		t.Fatalf("class = %q, want transient (%v)", class, err)
	}
}
