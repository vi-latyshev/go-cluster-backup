package discoverytest

import (
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/discovery"
)

// brokenEnv makes the child of TestRunFailsTheSuiteOnABrokenDiscoverer run the
// suite instead of spawning another child.
const brokenEnv = "DISCOVERYTEST_BROKEN"

// TestRunFailsTheSuiteOnABrokenDiscoverer: Run is what every discovery's test
// calls, and a check that reports a problem must fail the test — a conformance
// suite that swallowed one would pass a plugin that does not conform. The
// failing test is the child process's, the way a conformance suite's failure is
// never this package's own.
func TestRunFailsTheSuiteOnABrokenDiscoverer(t *testing.T) {
	if os.Getenv(brokenEnv) == "1" {
		Run(t, func(*testing.T) discovery.Discoverer { return finder{err: errors.New("the API is unreachable")} })
		return
	}
	args := []string{"-test.run=TestRunFailsTheSuiteOnABrokenDiscoverer"}
	// The child is this same coverage-instrumented binary, and go test gave
	// this one a directory to report what it ran into: without it the child's
	// own run would be measured nowhere.
	if dir := os.Getenv("GOCOVERDIR"); dir != "" {
		args = append(args, "-test.gocoverdir="+dir)
	}
	cmd := exec.Command(os.Args[0], args...)
	cmd.Env = append(os.Environ(), brokenEnv+"=1")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("the conformance suite passed a discoverer whose Discover fails:\n%s", out)
	}
	var exit *exec.ExitError
	if !errors.As(err, &exit) {
		t.Fatalf("the child did not run to its own end: %v", err)
	}
	if !strings.Contains(string(out), "the API is unreachable") {
		t.Fatalf("the child's failure does not carry the check's error:\n%s", out)
	}
}
