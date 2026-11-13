package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/cli"
)

// configWithEvent replaces one notifier event's message in the reference config
// with the given template lines, keeping the block scalar's indentation.
func configWithEvent(t *testing.T, event, message string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "config.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	block := "        " + event + ": |\n"
	for _, line := range strings.Split(message, "\n") {
		block += "          " + line + "\n"
	}
	var start, end int
	switch event {
	case "run_started":
		start = strings.Index(src, "        run_started: |\n")
		end = strings.Index(src, "        # The finished message")
	case "run_finished":
		start = strings.Index(src, "        run_finished: |\n")
		end = strings.Index(src, "      # One body template")
	}
	if start < 0 || end < 0 || end <= start {
		t.Fatalf("the reference config no longer carries the %s message where the test expects it", event)
	}
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(src[:start]+block+src[end:]), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// validateConfig runs `validate --config` through the distribution and answers
// its exit code and output.
func validateConfig(t *testing.T, path string) (int, string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := runCommand(t, []string{"validate", "--config", path}, &stdout, &stderr)
	return code, stdout.String() + stderr.String()
}

// runCommand drives the real command entry with the compiled distribution.
func runCommand(t *testing.T, args []string, stdout, stderr *bytes.Buffer) int {
	t.Helper()
	return cli.Main(context.Background(), options(t), args, stdout, stderr)
}

// TestReviewProfileAdmitsUnknownLeaseWithoutCatalog: a lease holder kind only
// some deployments produce is not demanded (the scheduled pick passes on a
// config without a catalog), while a field that names nothing in the same
// branch is refused with or without one — validate no longer has a profile.
func TestReviewProfileAdmitsUnknownLeaseWithoutCatalog(t *testing.T) {
	admitted := configWithEvent(t, "run_started",
		`Backup {{ with .Run.Lease }}{{ pick .HolderKind "scheduled" "📅" }}{{ end }}`)
	if code, out := validateConfig(t, admitted); code != 0 {
		t.Fatalf("a deployment-dependent holder kind was demanded: exit %d\n%s", code, out)
	}
	refused := configWithEvent(t, "run_started",
		`Backup {{ with .Run.Lease }}{{ if .Error }}{{ .Eror }}{{ end }}{{ end }}`)
	code, out := validateConfig(t, refused)
	if code == 0 || !strings.Contains(out, "Eror") {
		t.Fatalf("validate accepted .Eror: exit %d\n%s", code, out)
	}
}

// TestReviewRealStagedWarningReachesProfile: a stage record's real warning code
// is not demanded by validate — the pick over it keeps working — while a field
// that names nothing on the artifact warning is refused.
func TestReviewRealStagedWarningReachesProfile(t *testing.T) {
	admitted := configWithEvent(t, "run_finished",
		`Backup {{ range .Run.Artifacts }}{{ range .Warnings }}{{ pick .Code "source.credentials.unsealed" "🔓" }}{{ end }}{{ end }}`)
	if code, out := validateConfig(t, admitted); code != 0 {
		t.Fatalf("a staged warning pick was refused: exit %d\n%s", code, out)
	}
	refused := configWithEvent(t, "run_finished",
		`Backup {{ range .Run.Artifacts }}{{ range .Warnings }}{{ .Cod }}{{ end }}{{ end }}`)
	code, out := validateConfig(t, refused)
	if code == 0 || !strings.Contains(out, "Cod") {
		t.Fatalf("validate accepted .Cod: exit %d\n%s", code, out)
	}
}

// TestReviewSingletonScopeDoesNotRequireElsewhere: a pick over the two verdicts
// a single writer decides passes; the elsewhere value a node-scoped deployment
// could add is not demanded.
func TestReviewSingletonScopeDoesNotRequireElsewhere(t *testing.T) {
	path := configWithEvent(t, "run_finished",
		`Backup {{ range .Run.Excluded }}{{ pick .Kind "ignored" "🚫" "covered" "♻️" }}{{ end }}`)
	if code, out := validateConfig(t, path); code != 0 {
		t.Fatalf("a pick over ignored and covered was refused: exit %d\n%s", code, out)
	}
}

// TestReviewSingletonPickPasses: a pick whose value it always names is a
// complete pick, whatever the value's type.
func TestReviewSingletonPickPasses(t *testing.T) {
	path := configWithEvent(t, "run_finished", `Backup {{ pick "x" "x" "✅" }}`)
	if code, out := validateConfig(t, path); code != 0 {
		t.Fatalf("a singleton pick was refused: exit %d\n%s", code, out)
	}
}
