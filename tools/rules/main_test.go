package main

import (
	"bytes"
	"os"
	"strings"
	"testing"
)

// TestRunExitCodes proves the command's whole contract: a clean tree, findings,
// a scan failure and the two usage errors.
func TestRunExitCodes(t *testing.T) {
	t.Run("clean", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		code := run([]string{"-root", writeTree(t, healthyTree())}, &stdout, &stderr)
		if code != 0 || stdout.String() != "rules: all checks passed\n" || stderr.Len() != 0 {
			t.Fatalf("code %d, stdout %q, stderr %q", code, stdout.String(), stderr.String())
		}
	})
	t.Run("findings", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		code := run([]string{"-root", writeTree(t, checkedTree(map[string]string{
			"internal/plugins/library/plugin.go": "",
			"internal/plugins/library/spec.go":   "package library\n",
		}))}, &stdout, &stderr)
		if code != 1 {
			t.Fatalf("code = %d, want 1", code)
		}
		if !strings.Contains(stderr.String(), "plugin package has no plugin.go") {
			t.Fatalf("stderr = %q", stderr.String())
		}
	})
	t.Run("type model incomplete", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		code := run([]string{"-root", writeTree(t, checkedTree(map[string]string{
			"internal/plugins/library/undefined.go": "package library\n\nvar broken = undefinedName\n",
		}))}, &stdout, &stderr)
		if code != 1 || !strings.Contains(stderr.String(), "the type model is incomplete") ||
			!strings.Contains(stderr.String(), "undefined: undefinedName") {
			t.Fatalf("code %d, stderr %q", code, stderr.String())
		}
		if strings.Contains(stdout.String(), "all checks passed") {
			t.Fatalf("stdout = %q", stdout.String())
		}
	})
	t.Run("no go on PATH", func(t *testing.T) {
		t.Setenv("PATH", t.TempDir())
		var stdout, stderr bytes.Buffer
		code := run([]string{"-root", writeTree(t, healthyTree())}, &stdout, &stderr)
		if code != 1 || !strings.Contains(stderr.String(), "go list (linux/amd64)") {
			t.Fatalf("code %d, stderr %q", code, stderr.String())
		}
	})
	t.Run("scan failure", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		code := run([]string{"-root", t.TempDir()}, &stdout, &stderr)
		if code != 1 || !strings.Contains(stderr.String(), "rules: read go.mod") {
			t.Fatalf("code %d, stderr %q", code, stderr.String())
		}
	})
	t.Run("unknown flag", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		if code := run([]string{"-bogus"}, &stdout, &stderr); code != 2 {
			t.Fatalf("code = %d, want 2", code)
		}
	})
	t.Run("unexpected argument", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		code := run([]string{"extra"}, &stdout, &stderr)
		if code != 2 || !strings.Contains(stderr.String(), "unexpected arguments: [extra]") {
			t.Fatalf("code %d, stderr %q", code, stderr.String())
		}
	})
}

// TestMainExitsWithTheRunCode proves main's one wiring call through the exit
// seam, without ending the test process.
func TestMainExitsWithTheRunCode(t *testing.T) {
	restoreExit, restoreArgs := osExit, os.Args
	t.Cleanup(func() { osExit, os.Args = restoreExit, restoreArgs })
	var code int
	osExit = func(exitCode int) { code = exitCode }
	os.Args = []string{"rules", "-root", writeTree(t, healthyTree())}
	main()
	if code != 0 {
		t.Fatalf("exit code = %d", code)
	}
}

// TestUniqueSorted proves the shared finding normaliser: duplicates report once
// and output is deterministic.
func TestUniqueSorted(t *testing.T) {
	got := uniqueSorted([]string{"b", "a", "b"})
	want := []string{"a", "b"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("uniqueSorted = %v", got)
	}
}
