package configuration

import (
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"text/template"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
)

// fix9ChildEnv marks the re-executed child, where time.Local is still cold:
// the first Location.String on the real sentinel initialises it, and a copied
// receiver would read an empty name instead.
const fix9ChildEnv = "GCB_REVIEW_FIX9_CHILD"

// fatalReporter is what the cold run's failure needs from a test handle; the
// failure path itself is driven through a fake in its own test.
type fatalReporter interface {
	Fatalf(format string, args ...any)
}

// coldIdentityRun re-executes this test binary over one test with the child
// marker, so the child starts with a cold time.Local.
func coldIdentityRun(exe string) ([]byte, error) {
	cmd := exec.Command(exe, "-test.run=^TestReviewFix9ReceiverIdentity$", "-test.v")
	cmd.Env = append(os.Environ(), fix9ChildEnv+"=1", "TZ=UTC")
	return cmd.CombinedOutput()
}

// reportColdIdentity reports a failed cold run; a broken validation copy must
// name the child's output, never pass silently.
func reportColdIdentity(t fatalReporter, out []byte, err error) {
	if err != nil {
		t.Fatalf("the cold receiver run failed: %v\n%s", err, out)
	}
}

// captureFatal records one reported failure.
type captureFatal struct{ message string }

func (c *captureFatal) Fatalf(format string, args ...any) {
	c.message = fmt.Sprintf(format, args...)
}

// warmLocal pins the process-local zone for the warm pass, where earlier tests
// may already have initialised the real sentinel under the host zone. The cold
// child keeps the real sentinel, so its first Location.String initialises it.
func warmLocal(t *testing.T) {
	t.Helper()
	old := time.Local
	time.Local = time.UTC
	t.Cleanup(func() { time.Local = old })
}

// TestReviewFix9ReceiverIdentity is the reviewer's counterexample: on a cold
// time.Local, a pointer method must run on the received receiver, not a copy.
// The test re-executes itself once so the child starts cold; the same body then
// also runs warm, and both must agree with the executor.
func TestReviewFix9ReceiverIdentity(t *testing.T) {
	if os.Getenv(fix9ChildEnv) == "" {
		out, err := coldIdentityRun(os.Args[0])
		reportColdIdentity(t, out, err)
		warmLocal(t)
	}
	t.Setenv("TZ", "UTC")
	src := `{{ pick .At.Local.Location.String "" "" "UTC" "Backup" }}`
	funcs := CoreFuncs()
	original := template.Must(template.New("review").Option("missingkey=error").Funcs(funcs).Parse(src))
	judged, err := JudgeTemplate(original, reflect.TypeFor[model.Event](), funcs)
	if err != nil {
		t.Fatal(err)
	}
	var got, want strings.Builder
	if err := judged.Execute(&got, model.Event{}); err != nil {
		t.Fatal(err)
	}
	if err := original.Execute(&want, model.Event{}); err != nil {
		t.Fatal(err)
	}
	if got.String() != want.String() {
		t.Fatalf("judged = %q; executor = %q", got.String(), want.String())
	}
}

// TestFix9ColdRunReportsFailure drives the cold run's failure path: a missing
// binary must be reported, never swallowed.
func TestFix9ColdRunReportsFailure(t *testing.T) {
	out, err := coldIdentityRun(os.Args[0] + ".missing")
	var report captureFatal
	reportColdIdentity(&report, out, err)
	if report.message == "" {
		t.Fatal("a failed cold run was not reported")
	}
}

// TestFix9ReceiverIdentityMatrix holds the receiver identity across warm
// receivers, both pick functions and every surface time: the judged copy
// produces the executor's output byte for byte.
func TestFix9ReceiverIdentityMatrix(t *testing.T) {
	warmLocal(t)
	t.Setenv("TZ", "UTC")
	event := model.Event{
		At:  time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		Run: model.RunReport{StartedAt: time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)},
	}
	cases := []string{
		`{{ pick .At.Local.Location.String "" "" "UTC" "Backup" }}`,
		`{{ pickdef .At.Local.Location.String "" "UTC" "Backup" }}`,
		`{{ pickdef .At.Local.Location.String "" "" "Backup" }}`,
		`{{ pick .Run.StartedAt.Local.Location.String "" "" "UTC" "Backup" }}`,
		`{{ pickdef .Run.StartedAt.Local.Location.String "" "UTC" "Backup" }}`,
	}
	funcs := mergeCore(testExtras())
	for _, src := range cases {
		original := template.Must(template.New("fix9").Option("missingkey=error").Funcs(funcs).Parse(src))
		judged, err := JudgeTemplate(original, reflect.TypeFor[model.Event](), funcs)
		if err != nil {
			t.Fatalf("%s: %v", src, err)
		}
		var got, want strings.Builder
		if err := judged.Execute(&got, event); err != nil {
			t.Fatalf("%s: judged: %v", src, err)
		}
		if err := original.Execute(&want, event); err != nil {
			t.Fatalf("%s: executor: %v", src, err)
		}
		if got.String() != want.String() {
			t.Errorf("%s: judged = %q; executor = %q", src, got.String(), want.String())
		}
	}
}
