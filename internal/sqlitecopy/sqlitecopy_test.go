package sqlitecopy

import (
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// TestBackupArgsQuotesTheDestination: the destination is a double-quoted
// dot-command argument with `\` and `"` escaped, never SQL-style.
func TestBackupArgsQuotesTheDestination(t *testing.T) {
	got := BackupArgs("/data/it's.db", `/work/it's "q" back\slash.db`, 10*time.Second)
	want := []string{"-readonly", "-cmd", ".timeout 10000", "/data/it's.db",
		`.backup "/work/it's \"q\" back\\slash.db"`}
	if !slices.Equal(got, want) {
		t.Fatalf("BackupArgs = %q, want %q", got, want)
	}
}

func TestQuoteArg(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"/plain/path.db", `"/plain/path.db"`},
		{"/with space/x.db", `"/with space/x.db"`},
		{`/quote".db`, `"/quote\".db"`},
		{`/back\slash.db`, `"/back\\slash.db"`},
	} {
		if got := QuoteArg(tc.in); got != tc.want {
			t.Errorf("QuoteArg(%q) = %s, want %s", tc.in, got, tc.want)
		}
	}
}

// TestBackupRoundTripsThroughTheShell is the real check: a source database is
// copied into a destination whose PATH carries every byte a POSIX path may — a
// single quote, a double quote, a backslash and a space — by running the real
// sqlite3 binary with BackupArgs. A string comparison would not catch the bug
// this protects against: the shell's parser, not SQL, reads the argument.
//
// sqlite3 is present on the GitHub ubuntu runners; locally the test skips when
// the binary is absent and fails instead when CI is set, so a CI image missing it
// is loud rather than silently green.
func TestBackupRoundTripsThroughTheShell(t *testing.T) {
	sqlite3, err := exec.LookPath("sqlite3")
	if err != nil {
		if os.Getenv("CI") != "" {
			t.Fatalf("sqlite3 is required in CI and was not found in PATH: %v", err)
		}
		t.Skipf("sqlite3 not found in PATH, so the round-trip did not run: %v", err)
	}
	base := t.TempDir()
	destDir := filepath.Join(base, `it's "q" back\slash space`)
	if err := os.MkdirAll(destDir, 0o755); err != nil {
		t.Fatal(err)
	}
	src := filepath.Join(base, "src.db")
	if out, err := exec.Command(sqlite3, src, "create table t(a); insert into t values (42);").CombinedOutput(); err != nil {
		t.Fatalf("seeding the source database: %v: %s", err, out)
	}

	dst := filepath.Join(destDir, "copy.db")
	args := BackupArgs(src, dst, 10*time.Second)
	if out, err := exec.Command(sqlite3, args...).CombinedOutput(); err != nil {
		t.Fatalf("sqlite3 %q: %v: %s", args, err, strings.TrimSpace(string(out)))
	}
	if _, err := os.Stat(dst); err != nil {
		t.Fatalf("the copy was not written where the path named: %v", err)
	}
	out, err := exec.Command(sqlite3, dst, "select a from t;").Output()
	if err != nil {
		t.Fatalf("reading the copy: %v", err)
	}
	if got := strings.TrimSpace(string(out)); got != "42" {
		t.Fatalf("the copy answers %q, want 42", got)
	}
}

func TestPragma(t *testing.T) {
	for _, tc := range []struct {
		check string
		want  string
		ok    bool
	}{
		{"quick", "PRAGMA quick_check", true},
		{"full", "PRAGMA integrity_check", true},
		{"none", "", false},
		{"deep", "", false},
	} {
		got, ok := Pragma(tc.check)
		if got != tc.want || ok != tc.ok {
			t.Errorf("Pragma(%q) = %q, %t; want %q, %t", tc.check, got, ok, tc.want, tc.ok)
		}
	}
}
