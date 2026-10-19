package storagetest

// The kit's helpers report a broken storage or a bad input by failing the test —
// that is their contract, and the only place it can be asserted is a process of
// its own: a live *testing.T cannot survive asserting its own failure path. The
// child tests below drive each helper into the state it must refuse; the parent
// re-executes this binary over them once and requires every failure message.

import (
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/storage"
)

// kitChildEnv marks the re-executed child; without it the child tests skip, so
// an ordinary run of this package neither fails nor spawns anything.
const kitChildEnv = "STORAGETEST_KIT_CHILD"

func TestKitChildRunReportsAFailedCheck(t *testing.T) {
	if os.Getenv(kitChildEnv) != "1" {
		t.Skip("runs in the child process of TestKitReportsFailuresByFailing")
	}
	Run(t, func(*testing.T) storage.Storage { return broken{Fake: NewFake(), mode: "puterr"} })
}

func TestKitChildRunMultipartReportsABadPartSize(t *testing.T) {
	if os.Getenv(kitChildEnv) != "1" {
		t.Skip("runs in the child process of TestKitReportsFailuresByFailing")
	}
	RunMultipart(t, 0, func(*testing.T) storage.Storage { return newMemMultipart(5 << 20) })
}

func TestKitChildRunMultipartReportsAFailedCheck(t *testing.T) {
	if os.Getenv(kitChildEnv) != "1" {
		t.Skip("runs in the child process of TestKitReportsFailuresByFailing")
	}
	s := newMemMultipart(5 << 20)
	s.mode = "partfail"
	RunMultipart(t, 5<<20, func(*testing.T) storage.Storage { return s })
}

// TestKitChildGenerateReportsAnUngeneratableLocation: the generator's own guard
// fires when a location's shape and its generator disagree. Only a hand-built
// value can be inconsistent — which is what the guard is for.
func TestKitChildGenerateReportsAnUngeneratableLocation(t *testing.T) {
	if os.Getenv(kitChildEnv) != "1" {
		t.Skip("runs in the child process of TestKitReportsFailuresByFailing")
	}
	bad := &ConformanceLocation{base: "conformance-", hexDigits: 12, shape: regexp.MustCompile("^never$")}
	bad.Generate(t)
}

// TestKitReportsFailuresByFailing re-executes this binary over the child tests
// above and requires every one of them to fail with the message its helper owes.
func TestKitReportsFailuresByFailing(t *testing.T) {
	cmd := exec.Command(os.Args[0], "-test.run=^TestKitChild")
	cmd.Env = append(os.Environ(), kitChildEnv+"=1")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("the kit accepted every broken input:\n%s", out)
	}
	for _, want := range []string{
		"injected put failure",       // Run's Fatal carries the check's own error
		"put_stat_get",               // and names the check
		"outside the kit's range",    // RunMultipart refuses a part size it cannot drive
		"injected part failure",      // RunMultipart's Fatal carries the check's error
		"not a conformance location", // Generate's guard refuses a shape it cannot produce
	} {
		if !strings.Contains(string(out), want) {
			t.Errorf("the child's failures do not mention %q:\n%s", want, out)
		}
	}
}
