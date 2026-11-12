package postgresglobals_test

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
	json "encoding/json/v2"

	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
	pluginerrors "github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances/plugintest"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source/postgresglobals"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source/sourcetest"
)

// globalsDump is the shape pg_dumpall --globals-only prints: the header, the
// roles with a password hash and a role setting, the completion line and the
// closing comment line.
const globalsDump = `--
-- PostgreSQL database cluster dump
--

\restrict TOKEN

SET default_transaction_read_only = off;

--
-- Roles
--

CREATE ROLE app;
ALTER ROLE app WITH NOSUPERUSER INHERIT NOCREATEROLE NOCREATEDB LOGIN NOREPLICATION NOBYPASSRLS PASSWORD 'SCRAM-SHA-256$4096:abc';
ALTER ROLE app SET pgrst.jwt_secret TO 'secret-setting';

--
-- PostgreSQL database cluster dump complete
--

`

func testRegistry(t *testing.T) *instances.Registry {
	t.Helper()
	lr := loader.New()
	r := instances.NewRegistry()
	if err := loader.Add(lr, postgresglobals.Spec, func(lr *loader.Registry) error {
		return postgresglobals.Apply(lr, r)
	}); err != nil {
		t.Fatal(err)
	}
	return r
}

func build(t *testing.T, cfg string) (source.Source, error) {
	t.Helper()
	return instances.Build(context.Background(), testRegistry(t), source.Kind, "postgres_globals",
		jsontext.Value(cfg), instances.Deps{Instance: "postgres_globals", Log: slog.New(slog.DiscardHandler)})
}

// config is a complete globals config: role passwords are the deployment's
// choice, and client_dirs names a directory that is not installed so the PATH
// suite is the one that resolves.
func config(t *testing.T, addr string, rolePasswords bool) string {
	t.Helper()
	return configPath(t, addr, rolePasswords, passwordFile(t))
}

func configPath(t *testing.T, addr string, rolePasswords bool, password string) string {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"address": addr, "username": "app",
		"password":         map[string]any{"file": password},
		"client_dirs":      []string{filepath.Join(t.TempDir(), "postgresql18")},
		"role_hashes":      rolePasswords,
		"connect_attempts": 2, "connect_interval": "20ms",
	})
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func passwordFile(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "password")
	if err := os.WriteFile(path, []byte("s3cret-pw\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// listener hands out an address that accepts TCP connections, so the connect
// wait succeeds without a database anywhere.
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

// stubs writes fake client binaries recording their argv, env and stdin, and
// puts them at the front of PATH. Every binary answers --version with 18.6 and
// the read-only server_version_num query with 180006 unless a STUB_LOG file
// overrides it.
func stubs(t *testing.T, names ...string) {
	t.Helper()
	dir := t.TempDir()
	logDir := t.TempDir()
	t.Setenv("STUB_LOG", logDir)
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	for _, name := range names {
		script := `#!/bin/sh
printf '%s\n' "$@" > "$STUB_LOG/argv.` + name + `"
env | grep -E '^(PGPASSWORD|PGCONNECT_TIMEOUT)=' > "$STUB_LOG/env.` + name + `"
code=0; if [ -f "$STUB_LOG/exit.` + name + `" ]; then code=$(cat "$STUB_LOG/exit.` + name + `"); fi
for a in "$@"; do
  if [ "$a" = "--version" ]; then
    if [ -f "$STUB_LOG/version-exit" ]; then exit "$(cat "$STUB_LOG/version-exit")"; fi
    if [ -f "$STUB_LOG/version.` + name + `" ]; then cat "$STUB_LOG/version.` + name + `"; else printf '` + name + ` (PostgreSQL) 18.6\n'; fi
    exit 0
  fi
  if [ "$a" = "select current_setting('server_version_num')" ] && [ "$code" = 0 ]; then
    if [ -f "$STUB_LOG/server-major" ]; then cat "$STUB_LOG/server-major"; else printf '180006\n'; fi
    exit 0
  fi
done
if [ ! -t 0 ]; then cat > "$STUB_LOG/stdin.` + name + `"; fi
cat "$STUB_LOG/out.` + name + `" 2>/dev/null
if [ -f "$STUB_LOG/stderr.` + name + `" ]; then cat "$STUB_LOG/stderr.` + name + `" >&2; fi
if [ -f "$STUB_LOG/sleep.` + name + `" ]; then : > "$STUB_LOG/started.` + name + `"; exec sleep "$(cat "$STUB_LOG/sleep.` + name + `")"; fi
exit "$code"
`
		if err := os.WriteFile(filepath.Join(dir, name), []byte(script), 0o755); err != nil {
			t.Fatal(err)
		}
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

func TestGeneralConformance(t *testing.T) {
	plugintest.CheckSpec(t, testRegistry(t))
}

func TestSourceConformance(t *testing.T) {
	// The conformance kit dumps with an empty output: the artifact is still a
	// successful stream, which is what the source contract promises.
	stubs(t, "pg_dump", "pg_dumpall", "psql", "pg_restore")
	sourcetest.Run(t, func(t *testing.T) source.Source {
		s, err := build(t, config(t, listener(t), false))
		if err != nil {
			t.Fatal(err)
		}
		return s
	})
}

// TestDescribeCredentialsUnconditional: role settings can carry secrets with
// or without password hashes, so the artifact is credential content either way,
// and the origin names no database and carries no credentials.
func TestDescribeCredentialsUnconditional(t *testing.T) {
	stubs(t, "pg_dump", "pg_dumpall", "psql", "pg_restore")
	for _, rolePasswords := range []bool{false, true} {
		s, err := build(t, config(t, listener(t), rolePasswords))
		if err != nil {
			t.Fatal(err)
		}
		tr, err := s.Describe(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if !tr.Credentials {
			t.Fatalf("role_hashes %t: Credentials = false, want true", rolePasswords)
		}
		if tr.Ext != ".sql" {
			t.Fatalf("Ext = %q, want .sql", tr.Ext)
		}
		if !strings.HasPrefix(tr.Origin, "postgres://") || strings.Contains(tr.Origin, "@") || strings.Contains(tr.Origin, "/app") {
			t.Fatalf("Origin = %q, want a credential-free server origin", tr.Origin)
		}
	}
}

// TestDumpEmitsRawGlobals: the artifact is pg_dumpall's own output byte for
// byte — no line read, rewritten or reordered — and the password never appears
// on the command line.
func TestDumpEmitsRawGlobals(t *testing.T) {
	for _, rolePasswords := range []bool{false, true} {
		t.Run(fmt.Sprintf("role_hashes=%t", rolePasswords), func(t *testing.T) {
			stubs(t, "pg_dump", "pg_dumpall", "psql", "pg_restore")
			stubSet(t, "out.pg_dumpall", globalsDump)
			s, err := build(t, config(t, listener(t), rolePasswords))
			if err != nil {
				t.Fatal(err)
			}
			var buf bytes.Buffer
			if err := s.Dump(context.Background(), &buf); err != nil {
				t.Fatal(err)
			}
			if buf.String() != globalsDump {
				t.Fatalf("Dump = %q, want the server's own output", buf.String())
			}
			argv := strings.Split(strings.TrimSpace(stubLog(t, "argv.pg_dumpall")), "\n")
			if !strings.Contains(strings.Join(argv, " "), "--globals-only") {
				t.Fatalf("pg_dumpall argv = %q, want --globals-only", argv)
			}
			hasNoPasswords := strings.Contains(strings.Join(argv, " "), "--no-role-passwords")
			if hasNoPasswords == rolePasswords {
				t.Fatalf("role_hashes %t and --no-role-passwords %t disagree (argv %q)", rolePasswords, hasNoPasswords, argv)
			}
			for _, arg := range argv {
				if strings.Contains(arg, "s3cret-pw") {
					t.Fatalf("the password appears on argv: %q", argv)
				}
			}
			if env := stubLog(t, "env.pg_dumpall"); !strings.Contains(env, "PGPASSWORD=s3cret-pw") {
				t.Fatalf("the password did not reach pg_dumpall's environment: %q", env)
			}
			// pg_dumpall connects to the maintenance database; no database is a
			// config field of this type.
			if strings.Contains(strings.Join(argv, " "), "--dbname=") {
				t.Fatalf("pg_dumpall argv = %q, want no configured database", argv)
			}
		})
	}
}

// TestCheckSelectsTheServersExactMajor: the same suite selection a dump makes
// happens at preflight, and a server major no local suite holds is permanent.
func TestCheckSelectsTheServersExactMajor(t *testing.T) {
	stubs(t, "pg_dump", "pg_dumpall", "psql", "pg_restore")
	s, err := build(t, config(t, listener(t), false))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.(instances.Checker).Check(context.Background()); err != nil {
		t.Fatalf("Check: %v", err)
	}
	stubSet(t, "server-major", "170006\n")
	s, err = build(t, config(t, listener(t), false))
	if err != nil {
		t.Fatal(err)
	}
	err = s.(instances.Checker).Check(context.Background())
	if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassPermanent {
		t.Fatalf("class = %q, want permanent (%v)", class, err)
	}
}

// TestCheckRefusals: every preflight failure is scoped to the instance and
// classified, so the run can decide.
func TestCheckRefusals(t *testing.T) {
	t.Run("a broken client suite", func(t *testing.T) {
		stubs(t, "pg_dump", "pg_dumpall", "psql", "pg_restore")
		dir := filepath.Join(t.TempDir(), "postgresql18")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(map[string]any{
			"address": listener(t), "username": "app",
			"password":    map[string]any{"file": passwordFile(t)},
			"client_dirs": []string{dir}, "role_hashes": false,
			"connect_attempts": 2, "connect_interval": "20ms",
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Setenv("PATH", t.TempDir())
		s, err := build(t, string(raw))
		if err != nil {
			t.Fatal(err)
		}
		if err := s.(instances.Checker).Check(context.Background()); err == nil {
			t.Fatal("Check passed with a broken client suite")
		}
	})
	t.Run("a missing password file", func(t *testing.T) {
		stubs(t, "pg_dump", "pg_dumpall", "psql", "pg_restore")
		pass := passwordFile(t)
		s, err := build(t, configPath(t, listener(t), false, pass))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(pass); err != nil {
			t.Fatal(err)
		}
		if err := s.(instances.Checker).Check(context.Background()); err == nil {
			t.Fatal("Check passed without the password file")
		}
	})
	t.Run("cancelled during the connect", func(t *testing.T) {
		stubs(t, "pg_dump", "pg_dumpall", "psql", "pg_restore")
		s, err := build(t, slowConfig(t, "127.0.0.1:1"))
		if err != nil {
			t.Fatal(err)
		}
		// Warm the local suite resolution first: this case is about the connect
		// wait, and cancelling while the binaries are probed would be a
		// different (also covered) path.
		if err := s.(instances.CheckBinaries).CheckBinaries(context.Background()); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(150 * time.Millisecond)
			cancel()
		}()
		if err := s.(instances.Checker).Check(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("Check = %v, want context.Canceled", err)
		}
	})
}

// TestCheckBinariesIsTheLocalCapability: the optional health check proves the
// configured suites without a server.
func TestCheckBinariesIsTheLocalCapability(t *testing.T) {
	stubs(t, "pg_dump", "pg_dumpall", "psql", "pg_restore")
	s, err := build(t, config(t, "127.0.0.1:1", false))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.(instances.CheckBinaries).CheckBinaries(context.Background()); err != nil {
		t.Fatalf("CheckBinaries: %v", err)
	}
	t.Setenv("PATH", t.TempDir())
	fresh, err := build(t, config(t, "127.0.0.1:1", false))
	if err != nil {
		t.Fatal(err)
	}
	if err := fresh.(instances.CheckBinaries).CheckBinaries(context.Background()); err == nil {
		t.Fatal("CheckBinaries passed with no usable suite")
	}
}

// slowConfig keeps the connect wait looping long enough for a test to cancel
// it.
func slowConfig(t *testing.T, addr string) string {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"address": addr, "username": "app",
		"password":    map[string]any{"file": passwordFile(t)},
		"client_dirs": []string{filepath.Join(t.TempDir(), "postgresql18")},
		"role_hashes": false, "connect_attempts": 20, "connect_interval": "200ms",
	})
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

// TestDumpRefusalsCoverTheConnectAndSelectionPaths: the dump's own refusals
// before pg_dumpall runs.
func TestDumpRefusalsCoverTheConnectAndSelectionPaths(t *testing.T) {
	t.Run("an unreachable target", func(t *testing.T) {
		stubs(t, "pg_dump", "pg_dumpall", "psql", "pg_restore")
		s, err := build(t, config(t, "127.0.0.1:1", false))
		if err != nil {
			t.Fatal(err)
		}
		err = s.Dump(context.Background(), io.Discard)
		if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassTransient {
			t.Fatalf("class = %q, want transient (%v)", class, err)
		}
	})
	t.Run("cancelled during the connect", func(t *testing.T) {
		stubs(t, "pg_dump", "pg_dumpall", "psql", "pg_restore")
		s, err := build(t, slowConfig(t, "127.0.0.1:1"))
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(150 * time.Millisecond)
			cancel()
		}()
		if err := s.Dump(ctx, io.Discard); !errors.Is(err, context.Canceled) {
			t.Fatalf("Dump = %v, want context.Canceled", err)
		}
	})
	t.Run("no suite of the server's major", func(t *testing.T) {
		stubs(t, "pg_dump", "pg_dumpall", "psql", "pg_restore")
		stubSet(t, "server-major", "170006\n")
		s, err := build(t, config(t, listener(t), false))
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Dump(context.Background(), io.Discard); err == nil {
			t.Fatal("Dump accepted a server major no suite holds")
		}
	})
}

// TestCheckUnreachableIsTransient: a server that does not answer is worth
// another attempt.
func TestCheckUnreachableIsTransient(t *testing.T) {
	stubs(t, "pg_dump", "pg_dumpall", "psql", "pg_restore")
	s, err := build(t, config(t, "127.0.0.1:1", false))
	if err != nil {
		t.Fatal(err)
	}
	err = s.(instances.Checker).Check(context.Background())
	if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassTransient {
		t.Fatalf("class = %q, want transient (%v)", class, err)
	}
}

// TestDumpRefusedCredentialsArePermanent: a refused credential is the server's
// answer, not a reason to retry.
func TestDumpRefusedCredentialsArePermanent(t *testing.T) {
	stubs(t, "pg_dump", "pg_dumpall", "psql", "pg_restore")
	stubSet(t, "exit.pg_dumpall", "1")
	stubSet(t, "stderr.pg_dumpall", `pg_dumpall: error: connection to server failed: FATAL:  password authentication failed for user "app"`)
	s, err := build(t, config(t, listener(t), false))
	if err != nil {
		t.Fatal(err)
	}
	err = s.Dump(context.Background(), io.Discard)
	if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassPermanent {
		t.Fatalf("class = %q, want permanent (%v)", class, err)
	}
}

// TestDumpStopsOnAWriterFailure: a failing destination kills pg_dumpall rather
// than being swallowed.
func TestDumpStopsOnAWriterFailure(t *testing.T) {
	stubs(t, "pg_dump", "pg_dumpall", "psql", "pg_restore")
	stubSet(t, "out.pg_dumpall", strings.Repeat("x", 256<<10))
	s, err := build(t, config(t, listener(t), false))
	if err != nil {
		t.Fatal(err)
	}
	fw := &plugintest.ErrWriter{}
	if err := s.Dump(context.Background(), fw); err == nil {
		t.Fatal("a failing destination was ignored")
	}
	if fw.Wrote == 0 {
		t.Fatal("the stub produced no bytes at all")
	}
}

// TestDumpFailsWithoutThePasswordFile: the mounted secret is read at use time.
func TestDumpFailsWithoutThePasswordFile(t *testing.T) {
	stubs(t, "pg_dump", "pg_dumpall", "psql", "pg_restore")
	pass := passwordFile(t)
	s, err := build(t, configPath(t, listener(t), false, pass))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(pass); err != nil {
		t.Fatal(err)
	}
	if err := s.Dump(context.Background(), io.Discard); err == nil {
		t.Fatal("Dump passed without the password file")
	}
}

// TestProbe proves completeness only: the header and the completion line must
// be there, and no claim about the SQL is made.
func TestProbe(t *testing.T) {
	stubs(t, "pg_dump", "pg_dumpall", "psql", "pg_restore")
	s, err := build(t, config(t, listener(t), false))
	if err != nil {
		t.Fatal(err)
	}
	rep, err := s.(source.Prober).Probe(context.Background(), strings.NewReader(globalsDump))
	if err != nil {
		t.Fatalf("Probe: %v", err)
	}
	if rep.Bytes != int64(len(globalsDump)) {
		t.Fatalf("bytes = %d, want the whole stream %d", rep.Bytes, len(globalsDump))
	}
	if !strings.Contains(rep.Detail, "globals") {
		t.Fatalf("detail = %q", rep.Detail)
	}

	for _, tc := range []struct{ name, body string }{
		{"a missing header", "SET client_encoding = 'UTF8';\n--\n-- PostgreSQL database cluster dump complete\n--\n\n"},
		{"a truncated stream", "--\n-- PostgreSQL database cluster dump\n--\n\nCREATE ROLE app;\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.(source.Prober).Probe(context.Background(), strings.NewReader(tc.body))
			if err == nil {
				t.Fatal("an incomplete globals dump probed clean")
			}
			if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassPermanent {
				t.Fatalf("class = %q, want permanent (%v)", class, err)
			}
		})
	}
}

// TestProbeReportsAStreamError: an upstream read error is the probe's error,
// not a verdict about the artifact.
func TestProbeReportsAStreamError(t *testing.T) {
	stubs(t, "pg_dump", "pg_dumpall", "psql", "pg_restore")
	s, err := build(t, config(t, listener(t), false))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.(source.Prober).Probe(context.Background(), errReader{}); err == nil {
		t.Fatal("an upstream error probed clean")
	}
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("injected upstream failure") }

// TestProbeCancelledBeforeItReads: a cancelled context is the cancellation.
func TestProbeCancelledBeforeItReads(t *testing.T) {
	stubs(t, "pg_dump", "pg_dumpall", "psql", "pg_restore")
	s, err := build(t, config(t, listener(t), false))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.(source.Prober).Probe(ctx, strings.NewReader(globalsDump)); !errors.Is(err, context.Canceled) {
		t.Fatalf("Probe = %v, want context.Canceled", err)
	}
}

// TestNoRestorer: globals are fetch-only by design. Applying arbitrary
// pg_dumpall SQL automatically is exactly the parser this plugin refuses to be.
func TestNoRestorer(t *testing.T) {
	stubs(t, "pg_dump", "pg_dumpall", "psql", "pg_restore")
	s, err := build(t, config(t, listener(t), false))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := s.(source.Restorer); ok {
		t.Fatal("the globals source implements Restorer; the operator restores raw globals")
	}
}

// TestCancelledContextIsRefusedEverywhere: every call reports the cancellation
// as such.
func TestCancelledContextIsRefusedEverywhere(t *testing.T) {
	stubs(t, "pg_dump", "pg_dumpall", "psql", "pg_restore")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := instances.Build(ctx, testRegistry(t), source.Kind, "postgres_globals",
		jsontext.Value(config(t, listener(t), false)), instances.Deps{Instance: "postgres_globals"}); err == nil {
		t.Error("New accepted a cancelled context")
	}
	s, err := build(t, config(t, listener(t), false))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Describe(ctx); err == nil {
		t.Error("Describe accepted a cancelled context")
	}
	if err := s.(instances.Checker).Check(ctx); err == nil {
		t.Error("Check accepted a cancelled context")
	}
	if err := s.Dump(ctx, io.Discard); err == nil {
		t.Error("Dump accepted a cancelled context")
	}
}

// TestNewRejectsBadConfig: every shared field is the deployment's to state, and
// a malformed one is refused at construction.
func TestNewRejectsBadConfig(t *testing.T) {
	base := func(mutate func(map[string]any)) string {
		m := map[string]any{
			"address": "h:5432", "username": "app",
			"password":    map[string]any{"file": "/p"},
			"client_dirs": []string{"/nonexistent/postgresql18"}, "role_hashes": false,
			"connect_attempts": 1, "connect_interval": "1s",
		}
		mutate(m)
		raw, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}
	for name, mutate := range map[string]func(map[string]any){
		"address without a port": func(m map[string]any) { m["address"] = "no-port" },
		"empty username":         func(m map[string]any) { m["username"] = "" },
		"empty password file":    func(m map[string]any) { m["password"] = map[string]any{"file": ""} },
		"no client dirs":         func(m map[string]any) { m["client_dirs"] = []string{} },
		"zero connect attempts":  func(m map[string]any) { m["connect_attempts"] = 0 },
		"zero connect interval":  func(m map[string]any) { m["connect_interval"] = "0s" },
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := build(t, base(mutate)); err == nil {
				t.Fatal("the config was accepted")
			}
		})
	}
}
