package retentiontest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/retention/gfs"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/retention/keeplast"
	"github.com/vi-latyshev/go-cluster-backup/internal/retention"
)

func TestGfsPassesTheSuite(t *testing.T) {
	Run(t, func() retention.Retention { return newPolicy(t, gfs.Register) })
}

func TestKeepLastPassesTheSuite(t *testing.T) {
	Run(t, func() retention.Retention { return newPolicy(t, keeplast.Register) })
}

// broken classifies runs in ways the suite must catch.
type broken struct{ mode string }

func (b broken) Decide(_ context.Context, req retention.DecideRequest) (retention.Decision, error) {
	d := retention.Decision{}
	switch b.mode {
	case "nothing":
		return d, nil
	case "order":
		// Keep-all or drop-all by the input order alone.
		if len(req.Runs) > 0 && req.Runs[0].RunID < req.Runs[len(req.Runs)-1].RunID {
			for _, r := range req.Runs {
				d.Keep = append(d.Keep, r.RunID)
			}
		} else {
			for _, r := range req.Runs {
				d.Drop = append(d.Drop, r.RunID)
			}
		}
		return d, nil
	case "alwayserror":
		return d, errors.New("decide failed")
	case "reversederror":
		// Errors only when the runs arrive oldest first, so the order check's
		// second call is the one that fails.
		if len(req.Runs) > 1 && req.Runs[0].At.Before(req.Runs[len(req.Runs)-1].At) {
			return d, errors.New("decide failed on reversed input")
		}
		for _, r := range req.Runs {
			d.Keep = append(d.Keep, r.RunID)
		}
		return d, nil
	case "duplicate":
		for _, r := range req.Runs {
			d.Keep = append(d.Keep, r.RunID)
			d.Drop = append(d.Drop, r.RunID)
		}
		return d, nil
	case "invented":
		for _, r := range req.Runs {
			d.Keep = append(d.Keep, r.RunID)
		}
		d.Keep = append(d.Keep, "invented-run")
		return d, nil
	case "nonempty":
		d.Keep = append(d.Keep, "phantom")
		return d, nil
	default: // ignorectx
		for _, r := range req.Runs {
			d.Keep = append(d.Keep, r.RunID)
		}
		return d, nil
	}
}

func TestChecksCatchBrokenPolicies(t *testing.T) {
	cases := []struct {
		check string
		mode  string
	}{
		{"classifies every run exactly once", "nothing"},
		{"classifies every run exactly once", "alwayserror"},
		{"classifies every run exactly once", "duplicate"},
		{"classifies every run exactly once", "invented"},
		{"decision does not depend on input order", "order"},
		{"decision does not depend on input order", "alwayserror"},
		{"decision does not depend on input order", "reversederror"},
		{"empty request", "alwayserror"},
		{"empty request", "nonempty"},
		{"returns on cancellation", "ignorectx"},
	}
	for _, tc := range cases {
		t.Run(tc.check, func(t *testing.T) {
			for _, c := range Checks {
				if c.Name != tc.check {
					continue
				}
				if err := c.Run(t, broken{mode: tc.mode}); err == nil {
					t.Fatalf("check %q passed a broken policy", c.Name)
				}
				return
			}
			t.Fatalf("check %q not found", tc.check)
		})
	}
}

// recorder stands in for *testing.T where the suite's own failure path is what
// is under test.
type recorder struct{ failures []string }

func (r *recorder) Helper() {}

func (r *recorder) Fatalf(format string, args ...any) {
	r.failures = append(r.failures, fmt.Sprintf(format, args...))
}

func TestFailCheckReportsOnlyFailures(t *testing.T) {
	rec := &recorder{}
	failCheck(rec, errors.New("the check failed"))
	if len(rec.failures) != 1 || !strings.Contains(rec.failures[0], "the check failed") {
		t.Fatalf("a failed check was not reported: %v", rec.failures)
	}
	rec = &recorder{}
	failCheck(rec, nil)
	if len(rec.failures) != 0 {
		t.Fatalf("a passing check was reported as a failure: %v", rec.failures)
	}
}

// newPolicy builds the plugin from its own registered Example — the same path
// the conformance kit takes.
func newPolicy(t *testing.T, register func(*plugin.Registry) error) retention.Retention {
	t.Helper()
	r := plugin.NewRegistry()
	if err := register(r); err != nil {
		t.Fatal(err)
	}
	d := r.Descriptors()[0]
	p, err := plugin.Build(context.Background(), r, retention.Kind, d.Type, d.Example,
		plugin.Deps{Instance: d.Type, Log: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	return p
}
