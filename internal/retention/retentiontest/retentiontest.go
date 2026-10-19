// Package retentiontest is the conformance suite every retention policy runs:
//
//	retentiontest.Run(t, func() retention.Retention { return gfs.New(cfg) })
//
// The checks are policy-agnostic: they prove the classification is well-formed
// for any policy, not that one policy made a particular choice.
package retentiontest

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin/plugintest"
	"github.com/vi-latyshev/go-cluster-backup/internal/retention"
)

type Check struct {
	Name string
	Run  func(t *testing.T, r retention.Retention) error
}

var Checks = []Check{
	{"classifies every run exactly once", checkClassification},
	{"decision does not depend on input order", checkOrderInvariance},
	{"empty request", checkEmpty},
	{"returns on cancellation", checkCancel},
}

func Run(t *testing.T, newFn func() retention.Retention) {
	t.Helper()
	for _, c := range Checks {
		t.Run(c.Name, func(t *testing.T) {
			failCheck(t, c.Run(t, newFn()))
		})
	}
}

// fatalReporter is what reporting a failed check needs from a test handle:
// *testing.T satisfies it, and the suite's own test drives the failure path
// with a recorder — a live T cannot survive asserting its own failure path.
type fatalReporter interface {
	Helper()
	Fatalf(format string, args ...any)
}

// failCheck ends the test on a check error: a conformance failure is never a
// warning.
func failCheck(t fatalReporter, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("retentiontest: %v", err)
	}
}

// Now is the fixed clock every fixture run is relative to.
var Now = time.Date(2026, 10, 12, 4, 20, 0, 0, time.UTC)

// FixtureRuns is a mixed two-weeks-of-runs set any policy must classify sanely.
func FixtureRuns() []model.StoredRun {
	var runs []model.StoredRun
	for i := range 15 {
		runs = append(runs, model.StoredRun{
			RunID:    fmt.Sprintf("20261%03dT042000Z", 100+i),
			At:       Now.AddDate(0, 0, -i),
			Bytes:    int64(1000 + i*10),
			Verified: i%5 != 0,
		})
	}
	return runs
}

func checkClassification(_ *testing.T, r retention.Retention) error {
	runs := FixtureRuns()
	d, err := r.Decide(context.Background(), retention.DecideRequest{Now: Now, Runs: runs})
	if err != nil {
		return fmt.Errorf("Decide: %w", err)
	}
	seen := map[string]int{}
	for _, id := range d.Keep {
		seen[id]++
	}
	for _, id := range d.Drop {
		seen[id]++
	}
	for _, run := range runs {
		switch seen[run.RunID] {
		case 1:
			delete(seen, run.RunID)
		case 0:
			return fmt.Errorf("run %s was classified neither keep nor drop", run.RunID)
		default:
			return fmt.Errorf("run %s was classified %d times", run.RunID, seen[run.RunID])
		}
	}
	if len(seen) > 0 {
		return fmt.Errorf("decision invented %d run ids not in the request", len(seen))
	}
	return nil
}

func checkOrderInvariance(_ *testing.T, r retention.Retention) error {
	runs := FixtureRuns()
	d1, err := r.Decide(context.Background(), retention.DecideRequest{Now: Now, Runs: runs})
	if err != nil {
		return fmt.Errorf("Decide: %w", err)
	}
	reversed := slices.Clone(runs)
	slices.Reverse(reversed)
	d2, err := r.Decide(context.Background(), retention.DecideRequest{Now: Now, Runs: reversed})
	if err != nil {
		return fmt.Errorf("Decide (reversed): %w", err)
	}
	if !sameSet(d1.Keep, d2.Keep) || !sameSet(d1.Drop, d2.Drop) {
		return fmt.Errorf("decision changed with input order: keep %v vs %v", d1.Keep, d2.Keep)
	}
	return nil
}

func sameSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	ab := slices.Clone(a)
	bb := slices.Clone(b)
	slices.Sort(ab)
	slices.Sort(bb)
	return slices.Equal(ab, bb)
}

func checkEmpty(_ *testing.T, r retention.Retention) error {
	d, err := r.Decide(context.Background(), retention.DecideRequest{Now: Now})
	if err != nil {
		return fmt.Errorf("Decide on an empty request: %w", err)
	}
	if len(d.Keep) != 0 || len(d.Drop) != 0 {
		return fmt.Errorf("empty request produced %d keep / %d drop", len(d.Keep), len(d.Drop))
	}
	return nil
}

func checkCancel(t *testing.T, r retention.Retention) error {
	return plugintest.MustReturnWithError(t, 5*time.Second, "Decide", func(ctx context.Context) error {
		_, err := r.Decide(ctx, retention.DecideRequest{Now: Now, Runs: FixtureRuns()})
		return err
	})
}
