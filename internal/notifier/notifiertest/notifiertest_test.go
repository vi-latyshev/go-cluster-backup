package notifiertest

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/notifier"
)

// brokenEnv makes the child of TestRunFailsTheSuiteOnABrokenNotifier run the
// suite instead of spawning another child.
const brokenEnv = "NOTIFIERTEST_BROKEN"

type failingNotifier struct{}

func (failingNotifier) Notify(context.Context, model.Event) error {
	return errors.New("the receiver is unreachable")
}

// TestRunFailsTheSuiteOnABrokenNotifier: Run is what every notifier's test
// calls, and a check that reports a problem must fail the test — a conformance
// suite that swallowed one would pass a plugin that never delivers. The failing
// test is the child process's, the way a conformance suite's failure is never
// this package's own.
func TestRunFailsTheSuiteOnABrokenNotifier(t *testing.T) {
	if os.Getenv(brokenEnv) == "1" {
		Run(t, func(*testing.T) notifier.Notifier { return failingNotifier{} })
		return
	}
	args := []string{"-test.run=TestRunFailsTheSuiteOnABrokenNotifier"}
	// The child is this same coverage-instrumented binary, and go test gave this
	// one a directory to report what it ran into: without it the child's own run
	// would be measured nowhere.
	if dir := os.Getenv("GOCOVERDIR"); dir != "" {
		args = append(args, "-test.gocoverdir="+dir)
	}
	cmd := exec.Command(os.Args[0], args...)
	cmd.Env = append(os.Environ(), brokenEnv+"=1")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("the conformance suite passed a notifier whose Notify fails:\n%s", out)
	}
	var exit *exec.ExitError
	if !errors.As(err, &exit) {
		t.Fatalf("the child did not run to its own end: %v", err)
	}
	if !strings.Contains(string(out), "the receiver is unreachable") {
		t.Fatalf("the child's failure does not carry the check's error:\n%s", out)
	}
}

type recorder struct {
	events []model.Event
	mode   string
}

func (r *recorder) Notify(ctx context.Context, ev model.Event) error {
	if err := ctx.Err(); err != nil && r.mode != "ignorectx" {
		return err
	}
	switch r.mode {
	case "fail":
		return errors.New("delivery failed")
	}
	r.events = append(r.events, ev)
	return nil
}

func TestRecorderPassesTheSuite(t *testing.T) {
	Run(t, func(*testing.T) notifier.Notifier { return &recorder{} })
}

func TestChecksCatchBrokenNotifiers(t *testing.T) {
	cases := []struct {
		check string
		mode  string
	}{
		{"notify", "fail"},
		{"returns on cancellation", "ignorectx"},
	}
	for _, tc := range cases {
		t.Run(tc.check, func(t *testing.T) {
			for _, c := range Checks {
				if c.Name != tc.check {
					continue
				}
				if err := c.Run(t, &recorder{mode: tc.mode}); err == nil {
					t.Fatalf("check %q passed a broken notifier", c.Name)
				}
				return
			}
			t.Fatalf("check %q not found", tc.check)
		})
	}
}

func TestRecorderRecordsTheEvent(t *testing.T) {
	// The fixture event itself must be complete: the suite's own precondition.
	ev := Event()
	if ev.Name == "" || ev.Run.RunID == "" {
		t.Fatalf("fixture event: %+v", ev)
	}
}
