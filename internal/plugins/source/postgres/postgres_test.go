package postgres_test

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

	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin/plugintest"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source/postgres"
	"github.com/vi-latyshev/go-cluster-backup/internal/source"
	"github.com/vi-latyshev/go-cluster-backup/internal/source/sourcetest"
)

func testRegistry(t *testing.T) *plugin.Registry {
	t.Helper()
	r := plugin.NewRegistry()
	if err := postgres.Register(r); err != nil {
		t.Fatal(err)
	}
	return r
}

func build(t *testing.T, cfg string) (source.Source, error) {
	t.Helper()
	return plugin.Build(context.Background(), testRegistry(t), source.Kind, "postgres",
		jsontext.Value(cfg), plugin.Deps{Instance: "postgres", Log: slog.New(slog.DiscardHandler)})
}

func config(t *testing.T, addr string) string {
	t.Helper()
	return configDB(t, addr, "app")
}

// configDB is the single-database config of a named database.
func configDB(t *testing.T, addr, database string) string {
	t.Helper()
	return fmt.Sprintf(`{"address": %q, "database": %q, "username": "app", "password": {"file": %q}, "format": "custom", "compression": "none", "connect_attempts": 2, "connect_interval": "20ms", "all_databases": false, "exclude_databases": []}`,
		addr, database, passwordFile(t))
}

// configFormat is the single-database config in a named dump format.
func configFormat(t *testing.T, addr, format string) string {
	t.Helper()
	return fmt.Sprintf(`{"address": %q, "database": "app", "username": "app", "password": {"file": %q}, "format": %q, "compression": "none", "connect_attempts": 2, "connect_interval": "20ms", "all_databases": false, "exclude_databases": []}`,
		addr, passwordFile(t), format)
}

func passwordFile(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "password")
	if err := os.WriteFile(path, []byte("s3cret-pw\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// listener hands out an address that accepts TCP connections, so waitTCP
// succeeds without a database anywhere.
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

// stubs writes fake binaries recording their argv, env and stdin, and puts
// them at the front of PATH.
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
for a in "$@"; do
  if [ "$a" = "--version" ]; then
    if [ -f "$STUB_LOG/version-exit" ]; then exit "$(cat "$STUB_LOG/version-exit")"; fi
    cat "$STUB_LOG/version.` + name + `" 2>/dev/null; exit 0
  fi
done
if [ ! -t 0 ]; then cat > "$STUB_LOG/stdin.` + name + `"; fi
cat "$STUB_LOG/out.` + name + `" 2>/dev/null
if [ -f "$STUB_LOG/stderr.` + name + `" ]; then cat "$STUB_LOG/stderr.` + name + `" >&2; fi
if [ -f "$STUB_LOG/sleep.` + name + `" ]; then : > "$STUB_LOG/started.` + name + `"; exec sleep "$(cat "$STUB_LOG/sleep.` + name + `")"; fi
code=0; if [ -f "$STUB_LOG/exit.` + name + `" ]; then code=$(cat "$STUB_LOG/exit.` + name + `"); fi
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

// waitStarted waits for the stub of one binary to report that it has begun the
// work the test wants to interrupt. A sleep long enough for "the process
// probably started" is a wall-clock bet that passes on an idle machine and
// fails under -race on a loaded one.
func waitStarted(t *testing.T, name string) {
	t.Helper()
	started := filepath.Join(os.Getenv("STUB_LOG"), "started."+name)
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(started); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the %s stub never started", name)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestGeneralConformance(t *testing.T) {
	plugintest.CheckSpec(t, testRegistry(t))
}

func TestSourceConformance(t *testing.T) {
	sourcetest.Run(t, func(t *testing.T) source.Source {
		stubs(t, "pg_dump", "psql", "pg_restore")
		s, err := build(t, config(t, listener(t)))
		if err != nil {
			t.Fatal(err)
		}
		return s
	})
}

func TestDescribe(t *testing.T) {
	stubs(t, "pg_dump", "psql", "pg_restore")
	s, err := build(t, config(t, listener(t)))
	if err != nil {
		t.Fatal(err)
	}
	tr, err := s.Describe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if tr.Ext != ".dump" {
		t.Errorf("Ext = %q, want .dump", tr.Ext)
	}
	if !strings.HasPrefix(tr.Origin, "postgres://") || !strings.Contains(tr.Origin, "/app") {
		t.Errorf("Origin = %q", tr.Origin)
	}
	if tr.Sealed {
		t.Error("Sealed is true for a plain dump")
	}
}

func TestDumpArgsCarryNoPassword(t *testing.T) {
	stubs(t, "pg_dump", "psql", "pg_restore")
	stubSet(t, "out.pg_dump", "dump-bytes")
	addr := listener(t)
	s, err := build(t, config(t, addr))
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := s.Dump(context.Background(), &buf); err != nil {
		t.Fatal(err)
	}
	if buf.String() != "dump-bytes" {
		t.Errorf("dumped %q", buf.String())
	}
	host, port, _ := net.SplitHostPort(addr)
	want := []string{"-h", host, "-p", port, "-U", "app", "--no-password", "-Fc", "--compress=none", "app"}
	got := strings.Split(strings.TrimSpace(stubLog(t, "argv.pg_dump")), "\n")
	if len(got) != len(want) {
		t.Fatalf("argv = %q, want %q", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("argv = %q, want %q", got, want)
		}
	}
	for _, arg := range got {
		if strings.Contains(arg, "s3cret-pw") {
			t.Fatalf("the password appears on argv: %q", got)
		}
	}
	env := stubLog(t, "env.pg_dump")
	if !strings.Contains(env, "PGPASSWORD=s3cret-pw") {
		t.Errorf("PGPASSWORD not passed through env: %q", env)
	}
	if !strings.Contains(env, "PGCONNECT_TIMEOUT=1") {
		t.Errorf("PGCONNECT_TIMEOUT not derived from connect_interval: %q", env)
	}
}

func TestDumpConfigPasswordOutranksTheEnvironment(t *testing.T) {
	stubs(t, "pg_dump", "psql", "pg_restore")
	t.Setenv("PGPASSWORD", "inherited-and-wrong")
	s, err := build(t, config(t, listener(t)))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Dump(context.Background(), io.Discard); err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(stubLog(t, "env.pg_dump"), "\n") {
		if strings.HasPrefix(line, "PGPASSWORD=") && line != "PGPASSWORD=s3cret-pw" {
			t.Fatalf("the environment shadowed the configured password: %q", line)
		}
	}
}

func TestDumpRefusedCredentialsArePermanent(t *testing.T) {
	stubs(t, "pg_dump", "psql", "pg_restore")
	stubSet(t, "exit.pg_dump", "1")
	stubSet(t, "stderr.pg_dump", "pg_dump: error: connection to server failed: FATAL:  password authentication failed for user \"app\"")
	s, err := build(t, config(t, listener(t)))
	if err != nil {
		t.Fatal(err)
	}
	err = s.Dump(context.Background(), io.Discard)
	if err == nil {
		t.Fatal("a refused credential was not an error")
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassPermanent {
		t.Fatalf("class = %q, want permanent (%v)", class, err)
	}
}

func TestDumpConnectionRefusedIsTransient(t *testing.T) {
	stubs(t, "pg_dump", "psql", "pg_restore")
	stubSet(t, "exit.pg_dump", "1")
	stubSet(t, "stderr.pg_dump", "pg_dump: error: connection to server at \"10.0.0.1\", port 5432 failed: Connection refused")
	s, err := build(t, config(t, listener(t)))
	if err != nil {
		t.Fatal(err)
	}
	err = s.Dump(context.Background(), io.Discard)
	if err == nil {
		t.Fatal("a refused connection was not an error")
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassTransient {
		t.Fatalf("class = %q, want transient (%v)", class, err)
	}
}

func TestDumpUnreachableTargetIsTransient(t *testing.T) {
	stubs(t, "pg_dump", "psql", "pg_restore")
	s, err := build(t, config(t, "127.0.0.1:1"))
	if err != nil {
		t.Fatal(err)
	}
	err = s.Dump(context.Background(), io.Discard)
	if err == nil {
		t.Fatal("an unreachable target was not an error")
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassTransient {
		t.Fatalf("class = %q, want transient (%v)", class, err)
	}
}

func TestDumpWriterFailureStopsTheDump(t *testing.T) {
	stubs(t, "pg_dump", "psql", "pg_restore")
	stubSet(t, "out.pg_dump", strings.Repeat("x", 256<<10))
	s, err := build(t, config(t, listener(t)))
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

func TestDumpReturnsOnCancellationMidRun(t *testing.T) {
	stubs(t, "pg_dump", "psql", "pg_restore")
	stubSet(t, "sleep.pg_dump", "10")
	s, err := build(t, config(t, listener(t)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	start := time.Now()
	go func() { done <- s.Dump(ctx, io.Discard) }()
	waitStarted(t, "pg_dump")
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("a cancelled dump returned success")
		}
		if elapsed := time.Since(start); elapsed > 5*time.Second {
			t.Fatalf("cancellation took %s", elapsed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Dump did not return after cancellation")
	}
}

func TestCheckComparesMajors(t *testing.T) {
	stubs(t, "pg_dump", "psql", "pg_restore")
	stubSet(t, "version.pg_dump", "pg_dump (PostgreSQL) 18.6\n")
	stubSet(t, "out.psql", "180006\n")
	s, err := build(t, config(t, listener(t)))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.(plugin.Checker).Check(context.Background()); err != nil {
		t.Fatalf("Check: %v", err)
	}
	got := strings.Split(strings.TrimSpace(stubLog(t, "argv.psql")), "\n")
	if got[len(got)-1] != "select current_setting('server_version_num')" {
		t.Fatalf("psql argv = %q", got)
	}
	for _, arg := range got {
		if strings.Contains(arg, "s3cret-pw") {
			t.Fatalf("the password appears on psql argv: %q", got)
		}
	}

	stubSet(t, "version.pg_dump", "pg_dump (PostgreSQL) 17.5\n")
	err = s.(plugin.Checker).Check(context.Background())
	if err == nil {
		t.Fatal("a major version mismatch was accepted")
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassPermanent {
		t.Fatalf("mismatch class = %q, want permanent (%v)", class, err)
	}
}

func TestCheckWithoutBinaries(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	s, err := build(t, config(t, "127.0.0.1:1"))
	if err != nil {
		t.Fatal(err)
	}
	err = s.(plugin.Checker).Check(context.Background())
	if err == nil {
		t.Fatal("a missing pg_dump was accepted")
	}
	if !strings.Contains(err.Error(), "pg_dump") {
		t.Fatalf("error does not name the binary: %v", err)
	}
}

func TestNewRejectsBadConfig(t *testing.T) {
	// Every case starts from a config that decodes completely — every field
	// present — and mutates one member, so nothing here is refused for a
	// missing field instead of its own fault.
	cfg := func(mutate func(map[string]any)) string {
		m := map[string]any{
			"address": "h:5432", "database": "app", "username": "app",
			"password": map[string]any{"file": "/p"}, "format": "custom", "compression": "none",
			"connect_attempts": 1, "connect_interval": "1s",
			"all_databases": false, "exclude_databases": []string{},
		}
		mutate(m)
		raw, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}
	for name, mutate := range map[string]func(map[string]any){
		"address without a port":  func(m map[string]any) { m["address"] = "no-port" },
		"address with a bad port": func(m map[string]any) { m["address"] = "h:notanumber" },
		"empty database":          func(m map[string]any) { m["database"] = "" },
		"empty username":          func(m map[string]any) { m["username"] = "" },
		"empty password file":     func(m map[string]any) { m["password"] = map[string]any{"file": ""} },
		"unknown format":          func(m map[string]any) { m["format"] = "zip" },
		"bad compression":         func(m map[string]any) { m["compression"] = "brotli:9" },
		"zero connect attempts":   func(m map[string]any) { m["connect_attempts"] = 0 },
		"zero connect interval":   func(m map[string]any) { m["connect_interval"] = "0s" },
		// Exclusions beside a single-database instance: nothing is enumerated
		// to exclude, so the config asks for something that cannot happen.
		"exclusions without the enumeration": func(m map[string]any) { m["exclude_databases"] = []string{"postgres"} },
		// An empty name is a typo, not a database.
		"empty exclusion": func(m map[string]any) {
			m["all_databases"] = true
			m["exclude_databases"] = []string{""}
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := build(t, cfg(mutate)); err == nil {
				t.Fatal("the config was accepted")
			}
		})
	}
}

const restoreOptions = `{"jobs": 1, "no_owner": false, "no_acl": false, "connect_attempts": 2, "connect_interval": "20ms"}`

func TestApplySideBySide(t *testing.T) {
	stubs(t, "pg_dump", "psql", "pg_restore")
	s, err := build(t, config(t, listener(t)))
	if err != nil {
		t.Fatal(err)
	}
	payload := "archive-bytes"
	rep, err := s.(source.Restorer).Apply(context.Background(), source.ApplyRequest{
		Mode: source.ApplySideBySide, Label: "20261012T042000Z", Options: jsontext.Value(restoreOptions),
	}, strings.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	if rep.Target != "app_restore_20261012T042000Z" {
		t.Fatalf("target = %q", rep.Target)
	}
	if rep.Bytes != int64(len(payload)) {
		t.Fatalf("bytes = %d", rep.Bytes)
	}
	psqlArgv := strings.Split(strings.TrimSpace(stubLog(t, "argv.psql")), "\n")
	if last := psqlArgv[len(psqlArgv)-1]; last != `CREATE DATABASE "app_restore_20261012T042000Z"` {
		t.Fatalf("psql argv = %q", psqlArgv)
	}
	for _, arg := range psqlArgv {
		if strings.Contains(arg, "s3cret-pw") {
			t.Fatalf("the password appears on psql argv: %q", psqlArgv)
		}
	}
	restoreArgv := strings.Split(strings.TrimSpace(stubLog(t, "argv.pg_restore")), "\n")
	if !strings.Contains(strings.Join(restoreArgv, " "), "--exit-on-error -d app_restore_20261012T042000Z") {
		t.Fatalf("pg_restore argv = %q", restoreArgv)
	}
	if strings.Contains(strings.Join(restoreArgv, " "), "--clean") {
		t.Fatalf("side_by_side must not clean: %q", restoreArgv)
	}
	if got := stubLog(t, "stdin.pg_restore"); got != payload {
		t.Fatalf("pg_restore stdin = %q", got)
	}
}

func TestApplyInPlace(t *testing.T) {
	stubs(t, "pg_dump", "psql", "pg_restore")
	s, err := build(t, config(t, listener(t)))
	if err != nil {
		t.Fatal(err)
	}
	opts := `{"jobs": 1, "no_owner": true, "no_acl": true, "connect_attempts": 2, "connect_interval": "20ms"}`
	rep, err := s.(source.Restorer).Apply(context.Background(), source.ApplyRequest{
		Mode: source.ApplyInPlace, Label: "20261012T042000Z", Options: jsontext.Value(opts),
	}, strings.NewReader("archive-bytes"))
	if err != nil {
		t.Fatal(err)
	}
	if rep.Target != "app" || rep.Previous != "" {
		t.Fatalf("report = %+v", rep)
	}
	argv := strings.Join(strings.Split(strings.TrimSpace(stubLog(t, "argv.pg_restore")), "\n"), " ")
	want := "--no-password --no-owner --no-acl --clean --if-exists --single-transaction -d app"
	if !strings.Contains(argv, want) {
		t.Fatalf("pg_restore argv = %q, want it to contain %q", argv, want)
	}
	if strings.Contains(argv, "CREATE DATABASE") {
		t.Fatalf("in_place must not create a database: %q", argv)
	}
}

func TestApplyJobsReadTheFileBackedArtifactDirectly(t *testing.T) {
	stubs(t, "pg_dump", "psql", "pg_restore")
	s, err := build(t, config(t, listener(t)))
	if err != nil {
		t.Fatal(err)
	}
	payload := "archive-bytes-for-jobs"
	f, err := os.CreateTemp(t.TempDir(), "artifact-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	if _, err := f.WriteString(payload); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	opts := `{"jobs": 4, "no_owner": false, "no_acl": false, "connect_attempts": 2, "connect_interval": "20ms"}`
	rep, err := s.(source.Restorer).Apply(context.Background(), source.ApplyRequest{
		Mode: source.ApplySideBySide, Label: "20261012T042000Z", Options: jsontext.Value(opts),
	}, f)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Bytes != int64(len(payload)) {
		t.Fatalf("bytes = %d", rep.Bytes)
	}
	argv := strings.Split(strings.TrimSpace(stubLog(t, "argv.pg_restore")), "\n")
	n := len(argv)
	if argv[n-3] != "-j" || argv[n-2] != "4" || argv[n-1] != f.Name() {
		t.Fatalf("argv = %q, want the -j flag and the artifact's own path", argv)
	}
	if got := stubLog(t, "stdin.pg_restore"); got != "" {
		t.Fatalf("a jobs restore still read stdin: %q", got)
	}
}

func TestApplyJobsRefuseAnArtifactFromAStream(t *testing.T) {
	stubs(t, "pg_dump", "psql", "pg_restore")
	s, err := build(t, config(t, listener(t)))
	if err != nil {
		t.Fatal(err)
	}
	opts := `{"jobs": 4, "no_owner": false, "no_acl": false, "connect_attempts": 2, "connect_interval": "20ms"}`
	_, err = s.(source.Restorer).Apply(context.Background(), source.ApplyRequest{
		Mode: source.ApplySideBySide, Label: "20261012T042000Z", Options: jsontext.Value(opts),
	}, strings.NewReader("archive-bytes"))
	if err == nil {
		t.Fatal("jobs > 1 accepted a stream with no path to hand pg_restore")
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassPermanent {
		t.Fatalf("class = %q, want permanent (%v)", class, err)
	}
	if !strings.Contains(err.Error(), "jobs 4") {
		t.Fatalf("the refusal does not name the option: %v", err)
	}
	if _, err := os.Stat(filepath.Join(os.Getenv("STUB_LOG"), "argv.pg_restore")); err == nil {
		t.Fatal("pg_restore ran despite the refusal")
	}
}

func TestApplyRefusesBadRequests(t *testing.T) {
	stubs(t, "pg_dump", "psql", "pg_restore")
	s, err := build(t, config(t, listener(t)))
	if err != nil {
		t.Fatal(err)
	}
	restorer := s.(source.Restorer)
	for _, req := range []source.ApplyRequest{
		{Mode: "", Label: "l", Options: jsontext.Value(restoreOptions)},
		{Mode: "overwrite", Label: "l", Options: jsontext.Value(restoreOptions)},
		{Mode: source.ApplySideBySide, Label: "", Options: jsontext.Value(restoreOptions)},
		{Mode: source.ApplySideBySide, Label: "l", Options: nil},
		{Mode: source.ApplySideBySide, Label: "l", Options: jsontext.Value(`{"jobs": 1, "no_owner": false, "no_acl": false, "connect_attempts": 2, "connect_interval": "20ms", "zzz": 1}`)},
		{Mode: source.ApplySideBySide, Label: "l", Options: jsontext.Value(`{"jobs": 1}`)},
		{Mode: source.ApplySideBySide, Label: "l", Options: jsontext.Value(`{"jobs": 0, "no_owner": false, "no_acl": false, "connect_attempts": 2, "connect_interval": "20ms"}`)},
		{Mode: source.ApplySideBySide, Label: "l", Options: jsontext.Value(`{"jobs": 1, "no_owner": false, "no_acl": false, "connect_attempts": 0, "connect_interval": "20ms"}`)},
		{Mode: source.ApplySideBySide, Label: "l", Options: jsontext.Value(`{"jobs": 1, "no_owner": false, "no_acl": false, "connect_attempts": 2, "connect_interval": "0s"}`)},
	} {
		if _, err := restorer.Apply(context.Background(), req, strings.NewReader("x")); err == nil {
			t.Errorf("request %+v accepted", req)
		}
	}
}

func TestApplySideBySideLabelSanitized(t *testing.T) {
	stubs(t, "pg_dump", "psql", "pg_restore")
	s, err := build(t, config(t, listener(t)))
	if err != nil {
		t.Fatal(err)
	}
	rep, err := s.(source.Restorer).Apply(context.Background(), source.ApplyRequest{
		Mode: source.ApplySideBySide, Label: "2026-10-12 04:20", Options: jsontext.Value(restoreOptions),
	}, strings.NewReader("x"))
	if err != nil {
		t.Fatal(err)
	}
	if rep.Target != "app_restore_2026_10_12_04_20" {
		t.Fatalf("target = %q", rep.Target)
	}
}

func TestApplySideBySideQuotesADatabaseNameWithAQuote(t *testing.T) {
	stubs(t, "pg_dump", "psql", "pg_restore")
	s, err := build(t, configDB(t, listener(t), `app"quoted`))
	if err != nil {
		t.Fatal(err)
	}
	rep, err := s.(source.Restorer).Apply(context.Background(), source.ApplyRequest{
		Mode: source.ApplySideBySide, Label: "l", Options: jsontext.Value(restoreOptions),
	}, strings.NewReader("x"))
	if err != nil {
		t.Fatal(err)
	}
	if rep.Target != `app"quoted_restore_l` {
		t.Fatalf("target = %q", rep.Target)
	}
	argv := strings.Split(strings.TrimSpace(stubLog(t, "argv.psql")), "\n")
	if last := argv[len(argv)-1]; last != `CREATE DATABASE "app""quoted_restore_l"` {
		t.Fatalf("psql argv = %q, want the embedded quote doubled", argv)
	}
}

func TestPlainFormatIsFetchOnly(t *testing.T) {
	stubs(t, "pg_dump", "psql", "pg_restore")
	s, err := build(t, configFormat(t, listener(t), "plain"))
	if err != nil {
		t.Fatal(err)
	}
	tr, err := s.Describe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if tr.Ext != ".sql" {
		t.Fatalf("Ext = %q", tr.Ext)
	}
	if _, err := s.(source.Restorer).Apply(context.Background(), source.ApplyRequest{
		Mode: source.ApplyInPlace, Label: "l", Options: jsontext.Value(restoreOptions),
	}, strings.NewReader("sql text")); !errors.Is(err, errors.ErrUnsupported) {
		t.Fatalf("Apply = %v, want errors.ErrUnsupported", err)
	}
	if _, err := s.(source.Prober).Probe(context.Background(), strings.NewReader("sql text")); !errors.Is(err, errors.ErrUnsupported) {
		t.Fatalf("Probe = %v, want errors.ErrUnsupported", err)
	}
}

func TestProbe(t *testing.T) {
	stubs(t, "pg_dump", "psql", "pg_restore")
	s, err := build(t, config(t, listener(t)))
	if err != nil {
		t.Fatal(err)
	}
	rep, err := s.(source.Prober).Probe(context.Background(), strings.NewReader("archive"))
	if err != nil {
		t.Fatal(err)
	}
	if rep.Bytes != int64(len("archive")) {
		t.Fatalf("bytes = %d", rep.Bytes)
	}
	argv := strings.Split(strings.TrimSpace(stubLog(t, "argv.pg_restore")), "\n")
	if argv[len(argv)-1] != "/dev/null" {
		t.Fatalf("argv = %q, want -f /dev/null", argv)
	}
	stubSet(t, "exit.pg_restore", "1")
	stubSet(t, "stderr.pg_restore", "pg_restore: error: could not read from input file: end of file")
	if _, err := s.(source.Prober).Probe(context.Background(), strings.NewReader("torn")); err == nil {
		t.Fatal("a torn archive probed clean")
	}
}

func TestErrorsAreJoined(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	s, err := build(t, config(t, "127.0.0.1:1"))
	if err != nil {
		t.Fatal(err)
	}
	err = s.(plugin.Checker).Check(context.Background())
	var joined interface{ Unwrap() []error }
	if !errors.As(err, &joined) {
		t.Fatalf("expected every missing binary reported at once, got %v", err)
	}
	if len(joined.Unwrap()) != 3 {
		t.Fatalf("joined %d errors, want 3: %v", len(joined.Unwrap()), err)
	}
}

// TestApplyInPlaceRefusesJobs: pg_restore's --single-transaction (the in-place
// contract) and --jobs are mutually exclusive; the combination is refused by
// the plugin before a connection or a command, and side_by_side keeps jobs.
func TestApplyInPlaceRefusesJobs(t *testing.T) {
	stubs(t, "pg_dump", "psql", "pg_restore")
	s, err := build(t, config(t, listener(t)))
	if err != nil {
		t.Fatal(err)
	}
	opts := `{"jobs": 4, "no_owner": false, "no_acl": false, "connect_attempts": 2, "connect_interval": "20ms"}`
	_, err = s.(source.Restorer).Apply(context.Background(), source.ApplyRequest{
		Mode: source.ApplyInPlace, Label: "20261012T042000Z", Options: jsontext.Value(opts),
	}, strings.NewReader("archive-bytes"))
	if err == nil {
		t.Fatal("in_place with jobs > 1 was accepted")
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassPermanent {
		t.Fatalf("class = %q, want permanent (%v)", class, err)
	}
	if !strings.Contains(err.Error(), "jobs 4") || !strings.Contains(err.Error(), "in_place") {
		t.Fatalf("the refusal names neither the option nor the mode: %v", err)
	}
	for _, bin := range []string{"psql", "pg_restore"} {
		if _, statErr := os.Stat(filepath.Join(os.Getenv("STUB_LOG"), "argv."+bin)); statErr == nil {
			t.Fatalf("%s ran despite the refusal", bin)
		}
	}
}

// slowConfig keeps WaitTCP looping long enough for a test to cancel it: the
// cancellation branch is unreachable once the wait has already given up.
func slowConfig(t *testing.T, addr string) string {
	t.Helper()
	return fmt.Sprintf(`{"address": %q, "database": "app", "username": "app", "password": {"file": %q}, "format": "custom", "compression": "none", "connect_attempts": 20, "connect_interval": "200ms", "all_databases": false, "exclude_databases": []}`,
		addr, passwordFile(t))
}

func TestCancelledContextIsRefusedEverywhere(t *testing.T) {
	stubs(t, "pg_dump", "psql", "pg_restore")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := plugin.Build(ctx, testRegistry(t), source.Kind, "postgres",
		jsontext.Value(config(t, listener(t))), plugin.Deps{Instance: "postgres"}); err == nil {
		t.Error("New accepted a cancelled context")
	}
	s, err := build(t, config(t, listener(t)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Describe(ctx); err == nil {
		t.Error("Describe accepted a cancelled context")
	}
	if err := s.(plugin.Checker).Check(ctx); err == nil {
		t.Error("Check accepted a cancelled context")
	}
	if err := s.Dump(ctx, io.Discard); err == nil {
		t.Error("Dump accepted a cancelled context")
	}
	if _, err := s.(source.Enumerator).Units(ctx); err == nil {
		t.Error("Units accepted a cancelled context")
	}
	if _, err := s.(source.Restorer).Apply(ctx, source.ApplyRequest{
		Mode: source.ApplyInPlace, Label: "l", Options: jsontext.Value(restoreOptions),
	}, strings.NewReader("x")); err == nil {
		t.Error("Apply accepted a cancelled context")
	}
	if _, err := s.(source.Prober).Probe(ctx, strings.NewReader("x")); err == nil {
		t.Error("Probe accepted a cancelled context")
	}
}

// TestCheckUnreachableIsTransient: a server that does not answer is worth
// another attempt; the preflight must say so rather than refuse the run.
func TestCheckUnreachableIsTransient(t *testing.T) {
	stubs(t, "pg_dump", "psql", "pg_restore")
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
// the server's fault, and the preflight must report it as such.
func TestCheckCancelledDuringConnectReturnsTheContextError(t *testing.T) {
	stubs(t, "pg_dump", "psql", "pg_restore")
	s, err := build(t, slowConfig(t, "127.0.0.1:1"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(150 * time.Millisecond)
		cancel()
	}()
	if err := s.(plugin.Checker).Check(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Check = %v, want context.Canceled", err)
	}
}

func TestCheckFailsWithoutThePasswordFile(t *testing.T) {
	stubs(t, "pg_dump", "psql", "pg_restore")
	pass := passwordFile(t)
	cfg := fmt.Sprintf(`{"address": %q, "database": "app", "username": "app", "password": {"file": %q}, "format": "custom", "compression": "none", "connect_attempts": 2, "connect_interval": "20ms", "all_databases": false, "exclude_databases": []}`,
		listener(t), pass)
	s, err := build(t, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(pass); err != nil {
		t.Fatal(err)
	}
	if err := s.(plugin.Checker).Check(context.Background()); err == nil {
		t.Fatal("Check passed without the password file")
	}
}

// TestCheckReportsWhatItCannotRead: an unreadable client version, an
// unreadable server version and a psql query that fails are all the
// preflight's own failures.
func TestCheckReportsWhatItCannotRead(t *testing.T) {
	t.Run("a failing pg_dump --version", func(t *testing.T) {
		stubs(t, "pg_dump", "psql", "pg_restore")
		stubSet(t, "version-exit", "1")
		s, err := build(t, config(t, listener(t)))
		if err != nil {
			t.Fatal(err)
		}
		if err := s.(plugin.Checker).Check(context.Background()); err == nil {
			t.Fatal("a failing pg_dump --version passed Check")
		}
	})
	t.Run("no version in the output", func(t *testing.T) {
		stubs(t, "pg_dump", "psql", "pg_restore")
		stubSet(t, "version.pg_dump", "pg_dump (PostgreSQL) unknown\n")
		s, err := build(t, config(t, listener(t)))
		if err != nil {
			t.Fatal(err)
		}
		if err := s.(plugin.Checker).Check(context.Background()); err == nil {
			t.Fatal("an unreadable pg_dump version passed Check")
		}
	})
	t.Run("a version number that does not fit", func(t *testing.T) {
		stubs(t, "pg_dump", "psql", "pg_restore")
		stubSet(t, "version.pg_dump", "pg_dump (PostgreSQL) 99999999999999999999999.1\n")
		s, err := build(t, config(t, listener(t)))
		if err != nil {
			t.Fatal(err)
		}
		if err := s.(plugin.Checker).Check(context.Background()); err == nil {
			t.Fatal("an unparsable pg_dump version passed Check")
		}
	})
	t.Run("a failing psql query", func(t *testing.T) {
		stubs(t, "pg_dump", "psql", "pg_restore")
		stubSet(t, "version.pg_dump", "pg_dump (PostgreSQL) 18.6\n")
		stubSet(t, "exit.psql", "1")
		stubSet(t, "stderr.psql", "psql: error: connection to server failed: Connection refused")
		s, err := build(t, config(t, listener(t)))
		if err != nil {
			t.Fatal(err)
		}
		if err := s.(plugin.Checker).Check(context.Background()); err == nil {
			t.Fatal("a failing psql query passed Check")
		}
	})
	t.Run("a server version that is not a number", func(t *testing.T) {
		stubs(t, "pg_dump", "psql", "pg_restore")
		stubSet(t, "version.pg_dump", "pg_dump (PostgreSQL) 18.6\n")
		stubSet(t, "out.psql", "not-a-number\n")
		s, err := build(t, config(t, listener(t)))
		if err != nil {
			t.Fatal(err)
		}
		if err := s.(plugin.Checker).Check(context.Background()); err == nil {
			t.Fatal("an unparsable server version passed Check")
		}
	})
}

// TestUnitsCancelledDuringConnectReturnsTheContextError: the enumeration is a
// plugin call like any other.
func TestUnitsCancelledDuringConnectReturnsTheContextError(t *testing.T) {
	stubs(t, "pg_dump", "psql", "pg_restore")
	cfg := fmt.Sprintf(`{"address": "127.0.0.1:1", "database": "app", "username": "app", "password": {"file": %q}, "format": "custom", "compression": "none", "connect_attempts": 20, "connect_interval": "200ms", "all_databases": true, "exclude_databases": []}`,
		passwordFile(t))
	s, err := build(t, cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(150 * time.Millisecond)
		cancel()
	}()
	if _, err := enumerator(t, s).Units(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Units = %v, want context.Canceled", err)
	}
}

// TestDumpFailsWhenPgDumpIsMissing: the binary is resolved at the dump, and a
// deployment without it is a transient fault of this call.
func TestDumpFailsWhenPgDumpIsMissing(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	s, err := build(t, config(t, listener(t)))
	if err != nil {
		t.Fatal(err)
	}
	err = s.Dump(context.Background(), io.Discard)
	if err == nil {
		t.Fatal("Dump reported success without pg_dump")
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassTransient {
		t.Fatalf("class = %q, want transient (%v)", class, err)
	}
}

// TestProbeCancelledMidRunReturnsTheContextError: the probe waits on a command,
// and a cancelled context is reported as the cancellation, not the kill.
func TestProbeCancelledMidRunReturnsTheContextError(t *testing.T) {
	stubs(t, "pg_dump", "psql", "pg_restore")
	stubSet(t, "sleep.pg_restore", "10")
	s, err := build(t, config(t, listener(t)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := s.(source.Prober).Probe(ctx, strings.NewReader("archive")); done <- err }()
	time.Sleep(150 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Probe = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Probe did not return after cancellation")
	}
}

// TestApplyRefusalsCoverTheConnectAndNamingPaths: the restore's own refusals
// before a command runs.
func TestApplyRefusalsCoverTheConnectAndNamingPaths(t *testing.T) {
	t.Run("an unreachable target", func(t *testing.T) {
		stubs(t, "pg_dump", "psql", "pg_restore")
		s, err := build(t, config(t, "127.0.0.1:1"))
		if err != nil {
			t.Fatal(err)
		}
		_, err = s.(source.Restorer).Apply(context.Background(), source.ApplyRequest{
			Mode: source.ApplyInPlace, Label: "l", Options: jsontext.Value(restoreOptions),
		}, strings.NewReader("x"))
		if class, _ := plugin.ClassOf(err); class != plugin.ClassTransient {
			t.Fatalf("class = %q, want transient (%v)", class, err)
		}
	})
	t.Run("cancelled during the connect", func(t *testing.T) {
		stubs(t, "pg_dump", "psql", "pg_restore")
		s, err := build(t, slowConfig(t, "127.0.0.1:1"))
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(150 * time.Millisecond)
			cancel()
		}()
		_, err = s.(source.Restorer).Apply(ctx, source.ApplyRequest{
			Mode: source.ApplyInPlace, Label: "l", Options: jsontext.Value(slowRestoreOptions),
		}, strings.NewReader("x"))
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Apply = %v, want context.Canceled", err)
		}
	})
	t.Run("a label that yields no name", func(t *testing.T) {
		stubs(t, "pg_dump", "psql", "pg_restore")
		s, err := build(t, config(t, listener(t)))
		if err != nil {
			t.Fatal(err)
		}
		_, err = s.(source.Restorer).Apply(context.Background(), source.ApplyRequest{
			Mode: source.ApplySideBySide, Label: "---", Options: jsontext.Value(restoreOptions),
		}, strings.NewReader("x"))
		if err == nil {
			t.Fatal("a label that yields no usable database name was accepted")
		}
	})
	t.Run("a name longer than PostgreSQL allows", func(t *testing.T) {
		stubs(t, "pg_dump", "psql", "pg_restore")
		s, err := build(t, config(t, listener(t)))
		if err != nil {
			t.Fatal(err)
		}
		_, err = s.(source.Restorer).Apply(context.Background(), source.ApplyRequest{
			Mode: source.ApplySideBySide, Label: strings.Repeat("a", 70), Options: jsontext.Value(restoreOptions),
		}, strings.NewReader("x"))
		if err == nil {
			t.Fatal("a side-by-side name beyond 63 characters was accepted")
		}
	})
	t.Run("a database the server refuses to create", func(t *testing.T) {
		stubs(t, "pg_dump", "psql", "pg_restore")
		stubSet(t, "exit.psql", "1")
		stubSet(t, "stderr.psql", `psql: error: ERROR:  database "app_restore_l" already exists`)
		s, err := build(t, config(t, listener(t)))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.(source.Restorer).Apply(context.Background(), source.ApplyRequest{
			Mode: source.ApplySideBySide, Label: "l", Options: jsontext.Value(restoreOptions),
		}, strings.NewReader("x")); err == nil {
			t.Fatal("a refused CREATE DATABASE was reported as success")
		}
	})
	t.Run("an in-place restore that fails", func(t *testing.T) {
		stubs(t, "pg_dump", "psql", "pg_restore")
		stubSet(t, "exit.pg_restore", "1")
		stubSet(t, "stderr.pg_restore", "pg_restore: error: could not execute query: ERROR:  relation already exists")
		s, err := build(t, config(t, listener(t)))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.(source.Restorer).Apply(context.Background(), source.ApplyRequest{
			Mode: source.ApplyInPlace, Label: "l", Options: jsontext.Value(restoreOptions),
		}, strings.NewReader("x")); err == nil {
			t.Fatal("a failed in-place restore was reported as success")
		}
	})
	t.Run("without the password file", func(t *testing.T) {
		stubs(t, "pg_dump", "psql", "pg_restore")
		pass := passwordFile(t)
		cfg := fmt.Sprintf(`{"address": %q, "database": "app", "username": "app", "password": {"file": %q}, "format": "custom", "compression": "none", "connect_attempts": 2, "connect_interval": "20ms", "all_databases": false, "exclude_databases": []}`,
			listener(t), pass)
		s, err := build(t, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(pass); err != nil {
			t.Fatal(err)
		}
		if _, err := s.(source.Restorer).Apply(context.Background(), source.ApplyRequest{
			Mode: source.ApplyInPlace, Label: "l", Options: jsontext.Value(restoreOptions),
		}, strings.NewReader("x")); err == nil {
			t.Fatal("Apply passed without the password file")
		}
	})
}

// TestApplyJobsNeedsARegularFile: pg_restore --jobs reads a path itself, so an
// artifact that is not a regular file is refused by name.
func TestApplyJobsNeedsARegularFile(t *testing.T) {
	stubs(t, "pg_dump", "psql", "pg_restore")
	s, err := build(t, config(t, listener(t)))
	if err != nil {
		t.Fatal(err)
	}
	devNull, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = devNull.Close() })
	opts := `{"jobs": 4, "no_owner": false, "no_acl": false, "connect_attempts": 2, "connect_interval": "20ms"}`
	_, err = s.(source.Restorer).Apply(context.Background(), source.ApplyRequest{
		Mode: source.ApplySideBySide, Label: "l", Options: jsontext.Value(opts),
	}, devNull)
	if err == nil {
		t.Fatal("jobs > 1 accepted an artifact that is not a regular file")
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassPermanent {
		t.Fatalf("class = %q, want permanent (%v)", class, err)
	}
}

// TestApplyJobsReportsAFailingRestore: the parallel restore's own command
// failure is the call's error.
func TestApplyJobsReportsAFailingRestore(t *testing.T) {
	stubs(t, "pg_dump", "psql", "pg_restore")
	stubSet(t, "exit.pg_restore", "1")
	stubSet(t, "stderr.pg_restore", "pg_restore: error: could not execute query: ERROR:  relation already exists")
	s, err := build(t, config(t, listener(t)))
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.CreateTemp(t.TempDir(), "artifact-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	if _, err := f.WriteString("archive-bytes"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	opts := `{"jobs": 4, "no_owner": false, "no_acl": false, "connect_attempts": 2, "connect_interval": "20ms"}`
	if _, err := s.(source.Restorer).Apply(context.Background(), source.ApplyRequest{
		Mode: source.ApplySideBySide, Label: "l", Options: jsontext.Value(opts),
	}, f); err == nil {
		t.Fatal("a failed parallel restore was reported as success")
	}
}

// slowRestoreOptions keeps the restore's own connect wait looping long enough
// for a test to cancel it: the wait is the options', not the config's.
const slowRestoreOptions = `{"jobs": 1, "no_owner": false, "no_acl": false, "connect_attempts": 20, "connect_interval": "200ms"}`

// cancelFailWriter cancels the run and fails the write at once: the shape of a
// destination that gives up while the operator is already stopping the run.
type cancelFailWriter struct{ cancel context.CancelFunc }

func (w cancelFailWriter) Write([]byte) (int, error) {
	w.cancel()
	return 0, errors.New("injected write failure")
}

func TestDumpFailsWithoutThePasswordFile(t *testing.T) {
	stubs(t, "pg_dump", "psql", "pg_restore")
	pass := passwordFile(t)
	cfg := fmt.Sprintf(`{"address": %q, "database": "app", "username": "app", "password": {"file": %q}, "format": "custom", "compression": "none", "connect_attempts": 2, "connect_interval": "20ms", "all_databases": false, "exclude_databases": []}`,
		listener(t), pass)
	s, err := build(t, cfg)
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

func TestDumpCancelledDuringConnectReturnsTheContextError(t *testing.T) {
	stubs(t, "pg_dump", "psql", "pg_restore")
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
}

// TestDumpReportsTheContextWhenTheDestinationFailsUnderIt: a write that fails
// while the run is being cancelled is the cancellation, not the destination.
func TestDumpReportsTheContextWhenTheDestinationFailsUnderIt(t *testing.T) {
	stubs(t, "pg_dump", "psql", "pg_restore")
	stubSet(t, "out.pg_dump", "some dump bytes")
	s, err := build(t, config(t, listener(t)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := s.Dump(ctx, cancelFailWriter{cancel: cancel}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Dump = %v, want context.Canceled", err)
	}
}

// countingCtx reports context.Canceled from Err only from its cancelAt-th call
// on: it places the cancellation exactly between the connect wait and the
// command start, which no clock can do reliably.
type countingCtx struct {
	calls    int
	cancelAt int
	done     chan struct{}
}

func (c *countingCtx) Deadline() (time.Time, bool) { return time.Time{}, false }
func (c *countingCtx) Done() <-chan struct{}       { return c.done }
func (c *countingCtx) Value(any) any               { return nil }
func (c *countingCtx) Err() error {
	c.calls++
	if c.calls >= c.cancelAt {
		return context.Canceled
	}
	return nil
}

// TestDumpReportsTheContextWhenTheCommandCannotStart: a pg_dump that cannot be
// started while the run is being cancelled is the cancellation, not the
// missing binary.
func TestDumpReportsTheContextWhenTheCommandCannotStart(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	s, err := build(t, config(t, listener(t)))
	if err != nil {
		t.Fatal(err)
	}
	ctx := &countingCtx{cancelAt: 3, done: make(chan struct{})}
	if err := s.Dump(ctx, io.Discard); !errors.Is(err, context.Canceled) {
		t.Fatalf("Dump = %v, want context.Canceled", err)
	}
}

// TestApplyJobsReportsAClosedArtifact: pg_restore --jobs stats the artifact it
// is handed, and a handle that cannot be asked is a transient fault.
func TestApplyJobsReportsAClosedArtifact(t *testing.T) {
	stubs(t, "pg_dump", "psql", "pg_restore")
	s, err := build(t, config(t, listener(t)))
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.CreateTemp(t.TempDir(), "artifact-*")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	opts := `{"jobs": 4, "no_owner": false, "no_acl": false, "connect_attempts": 2, "connect_interval": "20ms"}`
	if _, err := s.(source.Restorer).Apply(context.Background(), source.ApplyRequest{
		Mode: source.ApplySideBySide, Label: "l", Options: jsontext.Value(opts),
	}, f); err == nil {
		t.Fatal("jobs > 1 accepted a closed artifact handle")
	}
}
