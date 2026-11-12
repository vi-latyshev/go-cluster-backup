package pgclient_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/configuration"
	pluginerrors "github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/pgclient"
)

// stubSuite writes a complete fake client suite into dir: one shell script per
// program. Every program answers --version with the major the versions map
// names for it (the "*" key is the default), psql answers the read-only
// server-version query with server, and every invocation records its path,
// argv, PGPASSWORD and PGCONNECT_TIMEOUT under STUB_LOG. The scripts use shell
// builtins only, so they run with PATH naming no suite at all.
func stubSuite(t *testing.T, dir string, versions map[string]int, server string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"pg_dump", "pg_dumpall", "pg_restore", "psql"} {
		major, ok := versions[name]
		if !ok {
			major = versions["*"]
		}
		script := "#!/bin/sh\n" +
			`printf '%s\n' "$0" > "$STUB_LOG/bin.` + name + `"` + "\n" +
			`printf '%s\n' "$@" > "$STUB_LOG/argv.` + name + `"` + "\n" +
			`printf '%s' "$PGPASSWORD" > "$STUB_LOG/pass.` + name + `"` + "\n" +
			`printf '%s' "$PGCONNECT_TIMEOUT" > "$STUB_LOG/timeout.` + name + `"` + "\n" +
			"for a in \"$@\"; do\n" +
			"  if [ \"$a\" = \"--version\" ]; then printf '" + name + " (PostgreSQL) " + strconv.Itoa(major) + ".0\\n'; exit 0; fi\n" +
			"done\n" +
			"for a in \"$@\"; do\n" +
			"  if [ \"$a\" = \"select current_setting('server_version_num')\" ]; then printf '" + server + "\\n'; exit 0; fi\n" +
			"done\n" +
			"exit 0\n"
		if err := os.WriteFile(filepath.Join(dir, name), []byte(script), 0o755); err != nil {
			t.Fatal(err)
		}
	}
}

// suiteDir is one usable suite of a named major, in a directory whose own name
// states that major, answering the server-version query with the same major.
func suiteDir(t *testing.T, major int) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "postgresql"+strconv.Itoa(major))
	stubSuite(t, dir, map[string]int{"*": major}, strconv.Itoa(major)+"0006")
	return dir
}

func passwordFile(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "password")
	if err := os.WriteFile(path, []byte("s3cret-pw\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// newConn builds a Conn with a valid password file; dirs are the configured
// client directories.
func newConn(t *testing.T, dirs ...string) *pgclient.Conn {
	t.Helper()
	conn, err := pgclient.New(pgclient.ConnConfig{
		Address:         "db.example:5432",
		Username:        "app",
		Password:        configuration.Secret{File: passwordFile(t)},
		ClientDirs:      dirs,
		ConnectAttempts: 2,
		ConnectInterval: model.Duration(20 * time.Millisecond),
	})
	if err != nil {
		t.Fatal(err)
	}
	return conn
}

// TestNewValidatesTheSharedConfig: the constructor owns no defaults, so every
// malformed shared setting is refused at once.
func TestNewValidatesTheSharedConfig(t *testing.T) {
	base := func() pgclient.ConnConfig {
		return pgclient.ConnConfig{
			Address: "db.example:5432", Username: "app",
			Password:        configuration.Secret{File: passwordFile(t)},
			ClientDirs:      []string{"/nonexistent/postgresql18"},
			ConnectAttempts: 1, ConnectInterval: model.Duration(time.Second),
		}
	}
	for name, mutate := range map[string]func(*pgclient.ConnConfig){
		"address without a port":  func(c *pgclient.ConnConfig) { c.Address = "no-port" },
		"address with a bad port": func(c *pgclient.ConnConfig) { c.Address = "h:notanumber" },
		"empty username":          func(c *pgclient.ConnConfig) { c.Username = "" },
		"empty password file":     func(c *pgclient.ConnConfig) { c.Password.File = "" },
		"no client dirs":          func(c *pgclient.ConnConfig) { c.ClientDirs = nil },
		"an empty client dir":     func(c *pgclient.ConnConfig) { c.ClientDirs = []string{""} },
		"a relative client dir":   func(c *pgclient.ConnConfig) { c.ClientDirs = []string{"postgresql18"} },
		"zero connect attempts":   func(c *pgclient.ConnConfig) { c.ConnectAttempts = 0 },
		"zero connect interval":   func(c *pgclient.ConnConfig) { c.ConnectInterval = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			cfg := base()
			mutate(&cfg)
			if _, err := pgclient.New(cfg); err == nil {
				t.Fatal("the config was accepted")
			}
		})
	}
	if _, err := pgclient.New(base()); err != nil {
		t.Fatalf("the valid config was refused: %v", err)
	}
}

// TestHostPortAndEnvironment: the connection prefix carries no password, the
// environment replaces an inherited one, and the connect timeout is derived
// from the interval with a one-second floor.
func TestHostPortAndEnvironment(t *testing.T) {
	t.Setenv("PGPASSWORD", "inherited-and-wrong")
	conn := newConn(t, "/nonexistent/postgresql18")
	if conn.Host() != "db.example" || conn.Port() != "5432" {
		t.Fatalf("address split = %q:%q", conn.Host(), conn.Port())
	}
	args := conn.ConnArgs()
	want := []string{"-h", "db.example", "-p", "5432", "-U", "app"}
	if strings.Join(args, " ") != strings.Join(want, " ") {
		t.Fatalf("ConnArgs = %q, want %q", args, want)
	}
	env := conn.Env("pw")
	var passwords, timeouts int
	for _, e := range env {
		switch {
		case strings.HasPrefix(e, "PGPASSWORD="):
			passwords++
			if e != "PGPASSWORD=pw" {
				t.Fatalf("the environment did not replace the inherited password: %q", e)
			}
		case strings.HasPrefix(e, "PGCONNECT_TIMEOUT="):
			timeouts++
			if e != "PGCONNECT_TIMEOUT=1" {
				t.Fatalf("PGCONNECT_TIMEOUT = %q, want the one-second floor", e)
			}
		}
	}
	if passwords != 1 || timeouts != 1 {
		t.Fatalf("environment has %d passwords and %d timeouts, want one each", passwords, timeouts)
	}
}

// TestReadPassword: the mounted secret is read at use time, never at
// construction, and a trailing newline is not part of it. A file that is gone
// by the time a call needs it is a permanent fault of that call.
func TestReadPassword(t *testing.T) {
	file := passwordFile(t)
	conn, err := pgclient.New(pgclient.ConnConfig{
		Address: "db.example:5432", Username: "app",
		Password:        configuration.Secret{File: file},
		ClientDirs:      []string{"/nonexistent/postgresql18"},
		ConnectAttempts: 1, ConnectInterval: model.Duration(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	pass, err := conn.ReadPassword()
	if err != nil {
		t.Fatal(err)
	}
	if pass != "s3cret-pw" {
		t.Fatalf("password = %q", pass)
	}
	if err := os.Remove(file); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ReadPassword(); err == nil {
		t.Fatal("ReadPassword passed without the mounted file")
	}
}

// TestCheckBinariesFindsConfiguredSuitesWithoutPATH: the configured directory
// is the proof; PATH naming no client is not a failure.
func TestCheckBinariesFindsConfiguredSuitesWithoutPATH(t *testing.T) {
	t.Setenv("STUB_LOG", t.TempDir())
	t.Setenv("PATH", t.TempDir())
	conn := newConn(t, suiteDir(t, 17))
	if err := conn.CheckBinaries(context.Background()); err != nil {
		t.Fatalf("CheckBinaries: %v", err)
	}
}

// TestCheckBinariesSkipsMissingDirectories: a configured directory that does
// not exist is an optional installation, and another suite carries the proof.
func TestCheckBinariesSkipsMissingDirectories(t *testing.T) {
	t.Setenv("STUB_LOG", t.TempDir())
	t.Setenv("PATH", t.TempDir())
	missing := filepath.Join(t.TempDir(), "postgresql18")
	conn := newConn(t, missing, suiteDir(t, 16))
	if err := conn.CheckBinaries(context.Background()); err != nil {
		t.Fatalf("CheckBinaries: %v", err)
	}
}

// TestCheckBinariesReportsEveryBrokenSuite: every existing configured
// directory is judged, and all their failures come back together.
func TestCheckBinariesReportsEveryBrokenSuite(t *testing.T) {
	t.Setenv("STUB_LOG", t.TempDir())
	t.Setenv("PATH", t.TempDir())
	dirA := filepath.Join(t.TempDir(), "postgresql18")
	dirB := filepath.Join(t.TempDir(), "postgresql17")
	for _, dir := range []string{dirA, dirB} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	conn := newConn(t, dirA, dirB)
	err := conn.CheckBinaries(context.Background())
	var joined interface{ Unwrap() []error }
	if !errors.As(err, &joined) {
		t.Fatalf("expected every broken suite at once, got %v", err)
	}
	if len(joined.Unwrap()) != 2 {
		t.Fatalf("joined %d errors, want 2: %v", len(joined.Unwrap()), err)
	}
}

// TestCheckBinariesRefusesUnusableDirectories: a directory the deployment names
// is either a complete suite or a fixed fault; nothing about it is guessed.
func TestCheckBinariesRefusesUnusableDirectories(t *testing.T) {
	t.Setenv("STUB_LOG", t.TempDir())
	t.Setenv("PATH", t.TempDir())
	t.Run("not a directory", func(t *testing.T) {
		file := filepath.Join(t.TempDir(), "postgresql18")
		if err := os.WriteFile(file, []byte("not a directory"), 0o600); err != nil {
			t.Fatal(err)
		}
		err := newConn(t, file).CheckBinaries(context.Background())
		if err == nil || !strings.Contains(err.Error(), "is not a directory") {
			t.Fatalf("CheckBinaries = %v, want a not-a-directory refusal", err)
		}
	})
	t.Run("a stat failure", func(t *testing.T) {
		file := filepath.Join(t.TempDir(), "plainfile")
		if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		// A child of a regular file fails to stat with ENOTDIR, not ENOENT.
		err := newConn(t, filepath.Join(file, "postgresql18")).CheckBinaries(context.Background())
		if err == nil || !strings.Contains(err.Error(), "client_dirs") {
			t.Fatalf("CheckBinaries = %v, want the stat failure named", err)
		}
	})
	t.Run("an incomplete suite", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "postgresql18")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "pg_dump"), []byte("#!/bin/sh\nprintf 'pg_dump (PostgreSQL) 18.6\\n'\n"), 0o755); err != nil {
			t.Fatal(err)
		}
		err := newConn(t, dir).CheckBinaries(context.Background())
		if err == nil || !strings.Contains(err.Error(), "pg_dumpall") {
			t.Fatalf("CheckBinaries = %v, want the missing program named", err)
		}
	})
	t.Run("a program that is not a regular file", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "postgresql18")
		if err := os.MkdirAll(filepath.Join(dir, "pg_dump"), 0o755); err != nil {
			t.Fatal(err)
		}
		err := newConn(t, dir).CheckBinaries(context.Background())
		if err == nil || !strings.Contains(err.Error(), "not a program") {
			t.Fatalf("CheckBinaries = %v, want a not-a-program refusal", err)
		}
	})
	t.Run("a directory whose name states another major", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "postgresql15")
		stubSuite(t, dir, map[string]int{"*": 16}, "160006")
		err := newConn(t, dir).CheckBinaries(context.Background())
		if err == nil || !strings.Contains(err.Error(), "names major 15") {
			t.Fatalf("CheckBinaries = %v, want the directory major named", err)
		}
	})
	t.Run("a suite that mixes majors", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "suite")
		stubSuite(t, dir, map[string]int{"*": 18, "psql": 17}, "180006")
		err := newConn(t, dir).CheckBinaries(context.Background())
		if err == nil || !strings.Contains(err.Error(), "mixes majors") {
			t.Fatalf("CheckBinaries = %v, want a mixed-suite refusal", err)
		}
	})
	t.Run("a custom directory name that states no major", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "clients")
		stubSuite(t, dir, map[string]int{"*": 18}, "180006")
		if err := newConn(t, dir).CheckBinaries(context.Background()); err != nil {
			t.Fatalf("CheckBinaries: %v", err)
		}
	})
	t.Run("a directory name too long to be a major", func(t *testing.T) {
		// A name that ends in digits too large for an int states no major,
		// exactly like a custom directory name with no digits at all.
		dir := filepath.Join(t.TempDir(), "postgresql99999999999999999999999999")
		stubSuite(t, dir, map[string]int{"*": 18}, "180006")
		if err := newConn(t, dir).CheckBinaries(context.Background()); err != nil {
			t.Fatalf("CheckBinaries: %v", err)
		}
	})
}

// TestCheckBinariesRefusesUnreadableVersions: a program that cannot say its
// own version is not a proof of the suite.
func TestCheckBinariesRefusesUnreadableVersions(t *testing.T) {
	t.Setenv("STUB_LOG", t.TempDir())
	t.Setenv("PATH", t.TempDir())
	write := func(t *testing.T, dir, body string) {
		t.Helper()
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"pg_dump", "pg_dumpall", "pg_restore", "psql"} {
			if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o755); err != nil {
				t.Fatal(err)
			}
		}
	}
	t.Run("a failing --version", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "suite")
		write(t, dir, "#!/bin/sh\nprintf 'broken\\n' >&2\nexit 1\n")
		err := newConn(t, dir).CheckBinaries(context.Background())
		if err == nil || !strings.Contains(err.Error(), "--version") {
			t.Fatalf("CheckBinaries = %v, want the failing version probe named", err)
		}
	})
	t.Run("no version in the output", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "suite")
		write(t, dir, "#!/bin/sh\nprintf 'unknown\\n'\n")
		err := newConn(t, dir).CheckBinaries(context.Background())
		if err == nil || !strings.Contains(err.Error(), "cannot read a version") {
			t.Fatalf("CheckBinaries = %v, want the unreadable version named", err)
		}
	})
	t.Run("a version number that does not fit", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "suite")
		write(t, dir, "#!/bin/sh\nprintf 'pg_dump (PostgreSQL) 99999999999999999999999999.1\\n'\n")
		err := newConn(t, dir).CheckBinaries(context.Background())
		if err == nil || !strings.Contains(err.Error(), "not a number") {
			t.Fatalf("CheckBinaries = %v, want the unparsable version named", err)
		}
	})
	t.Run("a program that cannot be executed", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "suite")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"pg_dump", "pg_dumpall", "pg_restore", "psql"} {
			if err := os.WriteFile(filepath.Join(dir, name), []byte("#!/bin/sh\n"), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		err := newConn(t, dir).CheckBinaries(context.Background())
		if err == nil || !strings.Contains(err.Error(), "--version") {
			t.Fatalf("CheckBinaries = %v, want the unexecutable program named", err)
		}
	})
}

// TestCheckBinariesFindsAPATHSuite: a suite installed the ordinary way — all
// four programs on PATH — is the last candidate when no configured directory
// holds one.
func TestCheckBinariesFindsAPATHSuite(t *testing.T) {
	t.Setenv("STUB_LOG", t.TempDir())
	pathDir := filepath.Join(t.TempDir(), "bin")
	stubSuite(t, pathDir, map[string]int{"*": 18}, "180006")
	t.Setenv("PATH", pathDir)
	if err := newConn(t, filepath.Join(t.TempDir(), "postgresql18")).CheckBinaries(context.Background()); err != nil {
		t.Fatalf("CheckBinaries: %v", err)
	}
}

// TestCheckBinariesJudgesPATHLast: a partial PATH is not a suite and does not
// fail a deployment whose configured directory is complete; with nothing else
// usable, the missing suite is refused.
func TestCheckBinariesJudgesPATHLast(t *testing.T) {
	t.Setenv("STUB_LOG", t.TempDir())
	pathDir := filepath.Join(t.TempDir(), "bin")
	if err := os.MkdirAll(pathDir, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"pg_dump", "psql"} {
		if err := os.WriteFile(filepath.Join(pathDir, name), []byte("#!/bin/sh\n"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("PATH", pathDir)
	if err := newConn(t, suiteDir(t, 16)).CheckBinaries(context.Background()); err != nil {
		t.Fatalf("a partial PATH failed a complete configured suite: %v", err)
	}
	err := newConn(t, filepath.Join(t.TempDir(), "postgresql18")).CheckBinaries(context.Background())
	if err == nil || !strings.Contains(err.Error(), "no PostgreSQL client suite") {
		t.Fatalf("CheckBinaries = %v, want the missing suite refused", err)
	}
}

// TestCheckBinariesReportsABrokenPATHSuite: a complete PATH match whose --version
// fails is a found suite and a fault, not a reason to fall through silently.
func TestCheckBinariesReportsABrokenPATHSuite(t *testing.T) {
	t.Setenv("STUB_LOG", t.TempDir())
	pathDir := filepath.Join(t.TempDir(), "bin")
	if err := os.MkdirAll(pathDir, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"pg_dump", "pg_dumpall", "pg_restore", "psql"} {
		if err := os.WriteFile(filepath.Join(pathDir, name), []byte("#!/bin/sh\nexit 1\n"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("PATH", pathDir)
	err := newConn(t, filepath.Join(t.TempDir(), "postgresql18")).CheckBinaries(context.Background())
	if err == nil || !strings.Contains(err.Error(), "--version") {
		t.Fatalf("CheckBinaries = %v, want the broken PATH suite named", err)
	}
}

// TestCheckBinariesReportsACancelledVersionProbe: cancelling while a program
// is asked its version is the cancellation, not the program's failure.
func TestCheckBinariesReportsACancelledVersionProbe(t *testing.T) {
	t.Setenv("STUB_LOG", t.TempDir())
	t.Setenv("PATH", t.TempDir())
	dir := filepath.Join(t.TempDir(), "postgresql18")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"pg_dump", "pg_dumpall", "pg_restore", "psql"} {
		script := "#!/bin/sh\n" +
			`: > "$STUB_LOG/started"; exec /bin/sleep 10` + "\n"
		if err := os.WriteFile(filepath.Join(dir, name), []byte(script), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	conn := newConn(t, dir)
	go func() { done <- conn.CheckBinaries(ctx) }()
	waitStarted(t, filepath.Join(os.Getenv("STUB_LOG"), "started"))
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("CheckBinaries = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("CheckBinaries did not return after cancellation")
	}
}

// TestCheckBinariesRefusesAMixedPATHSuite: a complete PATH suite that mixes
// majors is a found suite, and a broken one.
func TestCheckBinariesRefusesAMixedPATHSuite(t *testing.T) {
	t.Setenv("STUB_LOG", t.TempDir())
	pathDir := filepath.Join(t.TempDir(), "bin")
	stubSuite(t, pathDir, map[string]int{"*": 18, "psql": 16}, "180006")
	t.Setenv("PATH", pathDir)
	err := newConn(t, filepath.Join(t.TempDir(), "postgresql18")).CheckBinaries(context.Background())
	if err == nil || !strings.Contains(err.Error(), "mixes majors") {
		t.Fatalf("CheckBinaries = %v, want the mixed PATH suite refused", err)
	}
}

// TestSuitesAreMemoizedPerInstance: the local suites are resolved once per
// instance — a directory that changes after the first successful resolution is
// not re-read by this Conn, and no process-global cache is shared.
func TestSuitesAreMemoizedPerInstance(t *testing.T) {
	t.Setenv("STUB_LOG", t.TempDir())
	t.Setenv("PATH", t.TempDir())
	dir := suiteDir(t, 18)
	conn := newConn(t, dir)
	if err := conn.CheckBinaries(context.Background()); err != nil {
		t.Fatalf("first CheckBinaries: %v", err)
	}
	// Break the directory: a fresh Conn refuses it, the memoized one still
	// answers its first resolution.
	if err := os.Remove(filepath.Join(dir, "psql")); err != nil {
		t.Fatal(err)
	}
	if err := newConn(t, dir).CheckBinaries(context.Background()); err == nil {
		t.Fatal("a fresh Conn accepted the broken directory")
	}
	if err := conn.CheckBinaries(context.Background()); err != nil {
		t.Fatalf("the memoized resolution changed under the instance: %v", err)
	}
}

// TestSelectReportsAServerItCannotRead: a server major that cannot be read is
// the selection's own error.
func TestSelectReportsAServerItCannotRead(t *testing.T) {
	t.Setenv("STUB_LOG", t.TempDir())
	t.Setenv("PATH", t.TempDir())
	dir := filepath.Join(t.TempDir(), "postgresql18")
	stubSuite(t, dir, map[string]int{"*": 18}, "garbage")
	conn := newConn(t, dir)
	pass, err := conn.ReadPassword()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Select(context.Background(), pass, "app"); err == nil {
		t.Fatal("Select accepted an unreadable server version")
	}
}

// TestServerMajorReportsAFailedQuery: psql's own failure is the call's error,
// classified from its stderr.
func TestServerMajorReportsAFailedQuery(t *testing.T) {
	t.Setenv("STUB_LOG", t.TempDir())
	t.Setenv("PATH", t.TempDir())
	dir := filepath.Join(t.TempDir(), "postgresql18")
	stubSuite(t, dir, map[string]int{"*": 18}, "180006")
	psql := filepath.Join(dir, "psql")
	script := "#!/bin/sh\n" +
		"for a in \"$@\"; do if [ \"$a\" = \"--version\" ]; then printf 'psql (PostgreSQL) 18.6\\n'; exit 0; fi; done\n" +
		"printf 'connection refused\\n' >&2\nexit 1\n"
	if err := os.WriteFile(psql, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	conn := newConn(t, dir)
	pass, err := conn.ReadPassword()
	if err != nil {
		t.Fatal(err)
	}
	_, err = conn.ServerMajor(context.Background(), pass, "app")
	if err == nil {
		t.Fatal("ServerMajor accepted a failing psql")
	}
	if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassTransient {
		t.Fatalf("class = %q, want transient (%v)", class, err)
	}
}

// TestSelectUsesTheServersMajor: the suite is chosen by what the server
// answers, not by the order the directories are configured in.
func TestSelectUsesTheServersMajor(t *testing.T) {
	t.Setenv("STUB_LOG", t.TempDir())
	t.Setenv("PATH", t.TempDir())
	older := suiteDir(t, 17)
	newer := filepath.Join(t.TempDir(), "postgresql18")
	stubSuite(t, newer, map[string]int{"*": 18}, "170006") // the server is 17
	conn := newConn(t, older, newer)
	pass, err := conn.ReadPassword()
	if err != nil {
		t.Fatal(err)
	}
	suite, err := conn.Select(context.Background(), pass, "app")
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	if suite.Major != 17 {
		t.Fatalf("selected major %d, want the server's 17", suite.Major)
	}
	if !strings.Contains(suite.Path("pg_dump"), older) {
		t.Fatalf("pg_dump resolves to %q, want the 17 suite %q", suite.Path("pg_dump"), older)
	}
}

// TestSelectRefusesAnAbsentMajor: a server whose major no local suite holds is
// refused by name, listing the majors that are there.
func TestSelectRefusesAnAbsentMajor(t *testing.T) {
	t.Setenv("STUB_LOG", t.TempDir())
	t.Setenv("PATH", t.TempDir())
	dir := filepath.Join(t.TempDir(), "postgresql17")
	stubSuite(t, dir, map[string]int{"*": 17}, "150006")
	// A second suite of the same major proves the refusal lists one major once.
	conn := newConn(t, dir, suiteDir(t, 17))
	pass, err := conn.ReadPassword()
	if err != nil {
		t.Fatal(err)
	}
	_, err = conn.Select(context.Background(), pass, "app")
	if err == nil {
		t.Fatal("a server major no suite holds was accepted")
	}
	if !strings.Contains(err.Error(), "major(s) 17,") {
		t.Fatalf("the refusal does not list the held major once: %v", err)
	}
	if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassPermanent {
		t.Fatalf("class = %q, want permanent (%v)", class, err)
	}
	if !strings.Contains(err.Error(), "server major 15") || !strings.Contains(err.Error(), "17") {
		t.Fatalf("the refusal names neither the server major nor the held one: %v", err)
	}
}

// TestServerMajorReadsTheServersNumber: server_version_num packs the major
// into its leading digits.
func TestServerMajorReadsTheServersNumber(t *testing.T) {
	t.Setenv("STUB_LOG", t.TempDir())
	t.Setenv("PATH", t.TempDir())
	dir := filepath.Join(t.TempDir(), "postgresql18")
	stubSuite(t, dir, map[string]int{"*": 18}, "170006")
	conn := newConn(t, dir)
	pass, err := conn.ReadPassword()
	if err != nil {
		t.Fatal(err)
	}
	major, err := conn.ServerMajor(context.Background(), pass, "app")
	if err != nil {
		t.Fatalf("ServerMajor: %v", err)
	}
	if major != 17 {
		t.Fatalf("server major = %d, want 17", major)
	}
}

// TestServerMajorReportsAnUnparsableAnswer: a server that answers something
// that is not a number is a permanent refusal.
func TestServerMajorReportsAnUnparsableAnswer(t *testing.T) {
	t.Setenv("STUB_LOG", t.TempDir())
	t.Setenv("PATH", t.TempDir())
	dir := filepath.Join(t.TempDir(), "postgresql18")
	stubSuite(t, dir, map[string]int{"*": 18}, "not-a-number")
	conn := newConn(t, dir)
	pass, err := conn.ReadPassword()
	if err != nil {
		t.Fatal(err)
	}
	_, err = conn.ServerMajor(context.Background(), pass, "app")
	if err == nil || !strings.Contains(err.Error(), "is not a number") {
		t.Fatalf("ServerMajor = %v, want the unparsable answer named", err)
	}
	if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassPermanent {
		t.Fatalf("class = %q, want permanent (%v)", class, err)
	}
}

// TestNewestPrefersTheHighestMajor: a probe needs no server and runs the
// newest local pg_restore.
func TestNewestPrefersTheHighestMajor(t *testing.T) {
	t.Setenv("STUB_LOG", t.TempDir())
	t.Setenv("PATH", t.TempDir())
	conn := newConn(t, suiteDir(t, 17), suiteDir(t, 18), suiteDir(t, 15))
	suite, err := conn.Newest(context.Background())
	if err != nil {
		t.Fatalf("Newest: %v", err)
	}
	if suite.Major != 18 {
		t.Fatalf("newest major = %d, want 18", suite.Major)
	}
}

// TestCancelledContextIsRefusedEverywhere: every local call reports the
// cancellation as such, before a directory or a server is touched.
func TestCancelledContextIsRefusedEverywhere(t *testing.T) {
	t.Setenv("STUB_LOG", t.TempDir())
	t.Setenv("PATH", t.TempDir())
	conn := newConn(t, suiteDir(t, 18))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	pass, err := conn.ReadPassword()
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.CheckBinaries(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("CheckBinaries = %v, want context.Canceled", err)
	}
	if _, err := conn.Newest(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("Newest = %v, want context.Canceled", err)
	}
	if _, err := conn.ServerMajor(ctx, pass, "app"); !errors.Is(err, context.Canceled) {
		t.Errorf("ServerMajor = %v, want context.Canceled", err)
	}
	if _, err := conn.Select(ctx, pass, "app"); !errors.Is(err, context.Canceled) {
		t.Errorf("Select = %v, want context.Canceled", err)
	}
}

// TestCommandAppliesTheConnectionPrefixAndPassword: the one builder of suite
// commands puts the connection prefix first, `--no-password` right after it,
// and the password only in PGPASSWORD.
func TestCommandAppliesTheConnectionPrefixAndPassword(t *testing.T) {
	t.Setenv("STUB_LOG", t.TempDir())
	t.Setenv("PATH", t.TempDir())
	dir := suiteDir(t, 18)
	conn := newConn(t, dir)
	pass, err := conn.ReadPassword()
	if err != nil {
		t.Fatal(err)
	}
	suite, err := conn.Newest(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	cmd := conn.Command(context.Background(), suite, "pg_dump", pass, "-Fc", "tail")
	if _, err := conn.Stream(context.Background(), cmd, io.Discard); err != nil {
		t.Fatalf("Stream: %v", err)
	}
	argv := strings.Join(strings.Split(strings.TrimSpace(readFile(t, "argv.pg_dump")), "\n"), " ")
	want := "-h db.example -p 5432 -U app --no-password -Fc tail"
	if argv != want {
		t.Fatalf("argv = %q, want %q", argv, want)
	}
	if got := readFile(t, "pass.pg_dump"); got != "s3cret-pw" {
		t.Fatalf("PGPASSWORD = %q, want the mounted password", got)
	}
	if got := readFile(t, "timeout.pg_dump"); got != "1" {
		t.Fatalf("PGCONNECT_TIMEOUT = %q, want the one-second floor", got)
	}
}

func readFile(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(os.Getenv("STUB_LOG"), name))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// TestStreamCopiesTheOutput: the bytes stream through without a whole-artifact
// buffer.
func TestStreamCopiesTheOutput(t *testing.T) {
	conn := newConn(t, "/nonexistent/postgresql18")
	stdout := &bytes.Buffer{}
	cmd := exec.CommandContext(context.Background(), "/bin/sh", "-c", "printf 'dump-bytes'")
	if _, err := conn.Stream(context.Background(), cmd, stdout); err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if stdout.String() != "dump-bytes" {
		t.Fatalf("stdout = %q", stdout.String())
	}
}

// errWriter fails every write.
type errWriter struct{ wrote int }

func (w *errWriter) Write(p []byte) (int, error) {
	w.wrote += len(p)
	return 0, errors.New("injected write failure")
}

// TestStreamStopsOnAWriteFailure: a failing destination kills the command
// rather than being swallowed.
func TestStreamStopsOnAWriteFailure(t *testing.T) {
	conn := newConn(t, "/nonexistent/postgresql18")
	cmd := exec.CommandContext(context.Background(), "/bin/sh", "-c", "i=0; while [ $i -lt 1000 ]; do printf 'xxxxxxxxxxxxxxxx'; i=$((i+1)); done")
	fw := &errWriter{}
	if _, err := conn.Stream(context.Background(), cmd, fw); err == nil {
		t.Fatal("a failing destination was ignored")
	}
	if fw.wrote == 0 {
		t.Fatal("the command produced no bytes at all")
	}
}

// TestStreamClassifiesTheExit: the two PostgreSQL patterns decide the class of
// a failed command, and text matching neither stays unclassified.
func TestStreamClassifiesTheExit(t *testing.T) {
	conn := newConn(t, "/nonexistent/postgresql18")
	for _, tc := range []struct {
		name  string
		body  string
		class pluginerrors.Class
	}{
		{"a refused credential", "printf 'FATAL: password authentication failed\\n' >&2; exit 1", pluginerrors.ClassPermanent},
		{"a refused connection", "printf 'connection refused\\n' >&2; exit 1", pluginerrors.ClassTransient},
		{"anything else", "printf 'unexpected\\n' >&2; exit 1", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := exec.CommandContext(context.Background(), "/bin/sh", "-c", tc.body)
			_, err := conn.Stream(context.Background(), cmd, io.Discard)
			if err == nil {
				t.Fatal("a failing command was reported as success")
			}
			if class, _ := pluginerrors.ClassOf(err); class != tc.class {
				t.Fatalf("class = %q, want %q (%v)", class, tc.class, err)
			}
		})
	}
}

// TestStreamReportsACancelledContext: cancellation is not the command's
// fault.
func TestStreamReportsACancelledContext(t *testing.T) {
	conn := newConn(t, "/nonexistent/postgresql18")
	t.Setenv("STUB_LOG", t.TempDir())
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, "/bin/sh", "-c", `: > "$STUB_LOG/started"; exec /bin/sleep 10`)
	done := make(chan error, 1)
	go func() { _, err := conn.Stream(ctx, cmd, io.Discard); done <- err }()
	waitStarted(t, filepath.Join(os.Getenv("STUB_LOG"), "started"))
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Stream = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Stream did not return after cancellation")
	}
}

func waitStarted(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(path); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s never appeared", path)
		}
		time.Sleep(time.Millisecond)
	}
}

// cancelFailWriter cancels the run and fails the write at once: the shape of a
// destination that gives up while the operator is already stopping the run.
type cancelFailWriter struct{ cancel context.CancelFunc }

func (w cancelFailWriter) Write([]byte) (int, error) {
	w.cancel()
	return 0, errors.New("injected write failure")
}

// TestStreamReportsTheContextWhenTheDestinationFailsUnderIt: a write that
// fails while the run is being cancelled is the cancellation, not the
// destination.
func TestStreamReportsTheContextWhenTheDestinationFailsUnderIt(t *testing.T) {
	conn := newConn(t, "/nonexistent/postgresql18")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cmd := exec.CommandContext(ctx, "/bin/sh", "-c", "printf 'some dump bytes'")
	if _, err := conn.Stream(ctx, cmd, cancelFailWriter{cancel: cancel}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Stream = %v, want context.Canceled", err)
	}
}

// TestStreamReportsACancelledWait: the archive's bytes ended and the command
// then failed while the run was stopping — the cancellation wins.
func TestStreamReportsACancelledWait(t *testing.T) {
	conn := newConn(t, "/nonexistent/postgresql18")
	t.Setenv("STUB_LOG", t.TempDir())
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, "/bin/sh", "-c",
		`exec 1>&-; : > "$STUB_LOG/started"; exec /bin/sleep 10`)
	done := make(chan error, 1)
	go func() { _, err := conn.Stream(ctx, cmd, io.Discard); done <- err }()
	waitStarted(t, filepath.Join(os.Getenv("STUB_LOG"), "started"))
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Stream = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Stream did not return after cancellation")
	}
}

// TestStreamReportsACommandThatCannotStart: a missing program is a transient
// fault of the call; under cancellation it is the cancellation.
func TestStreamReportsACommandThatCannotStart(t *testing.T) {
	conn := newConn(t, "/nonexistent/postgresql18")
	_, err := conn.Stream(context.Background(), exec.CommandContext(context.Background(), "/nonexistent/program"), io.Discard)
	if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassTransient {
		t.Fatalf("class = %q, want transient (%v)", class, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = conn.Stream(ctx, exec.CommandContext(ctx, "/nonexistent/program"), io.Discard)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Stream = %v, want context.Canceled", err)
	}
}

// TestRunCountsTheInput: the bytes fed to stdin are counted, and a nil source
// leaves stdin empty.
func TestRunCountsTheInput(t *testing.T) {
	conn := newConn(t, "/nonexistent/postgresql18")
	n, err := conn.Run(context.Background(), exec.CommandContext(context.Background(), "/bin/cat"), strings.NewReader("archive"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if n != int64(len("archive")) {
		t.Fatalf("bytes = %d, want %d", n, len("archive"))
	}
	n, err = conn.Run(context.Background(), exec.CommandContext(context.Background(), "/bin/cat"), nil)
	if err != nil || n != 0 {
		t.Fatalf("Run with no input = %d, %v", n, err)
	}
}

// TestRunReportsAFailedCommand: a failed restore is the call's error,
// classified from its own stderr.
func TestRunReportsAFailedCommand(t *testing.T) {
	conn := newConn(t, "/nonexistent/postgresql18")
	cmd := exec.CommandContext(context.Background(), "/bin/sh", "-c", "printf 'connection refused\\n' >&2; exit 1")
	if _, err := conn.Run(context.Background(), cmd, strings.NewReader("x")); err == nil {
		t.Fatal("a failing command was reported as success")
	} else if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassTransient {
		t.Fatalf("class = %q, want transient (%v)", class, err)
	}
}

// TestRunReportsACancelledContext: cancellation wins over the command's kill.
func TestRunReportsACancelledContext(t *testing.T) {
	conn := newConn(t, "/nonexistent/postgresql18")
	t.Setenv("STUB_LOG", t.TempDir())
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, "/bin/sh", "-c", `: > "$STUB_LOG/started"; exec /bin/sleep 10`)
	done := make(chan error, 1)
	go func() { _, err := conn.Run(ctx, cmd, nil); done <- err }()
	waitStarted(t, filepath.Join(os.Getenv("STUB_LOG"), "started"))
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}
}

// TestOutputReturnsStdout: a successful command's stdout is the answer, and a
// failure is classified.
func TestOutputReturnsStdout(t *testing.T) {
	conn := newConn(t, "/nonexistent/postgresql18")
	out, err := conn.Output(context.Background(), exec.CommandContext(context.Background(), "/bin/sh", "-c", "printf 'answer'"))
	if err != nil || out != "answer" {
		t.Fatalf("Output = %q, %v", out, err)
	}
	_, err = conn.Output(context.Background(), exec.CommandContext(context.Background(), "/bin/sh", "-c", "printf 'password authentication failed\\n' >&2; exit 1"))
	if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassPermanent {
		t.Fatalf("class = %q, want permanent (%v)", class, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cmd := exec.CommandContext(ctx, "/bin/sh", "-c", "exec /bin/sleep 10")
	if _, err := conn.Output(ctx, cmd); !errors.Is(err, context.Canceled) {
		t.Fatalf("Output = %v, want context.Canceled", err)
	}
}

// TestDBNameArg: a database name travels as a quoted conninfo value, so '='
// and a leading "--" inside it mean nothing and a quote or backslash cannot end
// the value early.
func TestDBNameArg(t *testing.T) {
	for _, tc := range []struct{ name, want string }{
		{"app", `--dbname=dbname='app'`},
		{"host=evil.example", `--dbname=dbname='host=evil.example'`},
		{"--file=/tmp/x", `--dbname=dbname='--file=/tmp/x'`},
		{"a'b", `--dbname=dbname='a\'b'`},
		{`a\b`, `--dbname=dbname='a\\b'`},
	} {
		if got := pgclient.DBNameArg(tc.name); got != tc.want {
			t.Errorf("DBNameArg(%q) = %q, want %q", tc.name, got, tc.want)
		}
	}
}
