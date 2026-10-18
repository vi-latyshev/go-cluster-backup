// Package discoverytest is the conformance suite every discovery implementation runs:
//
//	discoverytest.Run(t, func(t *testing.T) discovery.Discoverer { return newTestDiscoverer(t) })
package discoverytest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/discovery"
	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin/plugintest"
)

type Check struct {
	Name string
	Run  func(t *testing.T, d discovery.Discoverer) error
}

var Checks = []Check{
	{"discover returns well-formed sources", checkDiscover},
	{"returns on cancellation", checkCancel},
}

func Run(t *testing.T, newFn func(t *testing.T) discovery.Discoverer) {
	t.Helper()
	for _, c := range Checks {
		t.Run(c.Name, func(t *testing.T) {
			if err := c.Run(t, newFn(t)); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func checkDiscover(_ *testing.T, d discovery.Discoverer) error {
	found, err := d.Discover(context.Background())
	if err != nil {
		return fmt.Errorf("Discover: %w", err)
	}
	seen := map[string]bool{}
	for i, f := range found {
		switch {
		case f.Name == "":
			return fmt.Errorf("found[%d]: Name is empty", i)
		case f.Artifact.Group == "":
			return fmt.Errorf("found[%d] (%s): Artifact.Group is empty", i, f.Name)
		case f.Artifact.Name == "":
			return fmt.Errorf("found[%d] (%s): Artifact.Name is empty", i, f.Name)
		case f.Artifact.Sensitivity != model.SensitivityPlain && f.Artifact.Sensitivity != model.SensitivitySecret:
			return fmt.Errorf("found[%d] (%s): sensitivity %q", i, f.Name, f.Artifact.Sensitivity)
		case f.Artifact.Importance != model.ImportanceRequired && f.Artifact.Importance != model.ImportanceBestEffort:
			return fmt.Errorf("found[%d] (%s): importance %q", i, f.Name, f.Artifact.Importance)
		// ProblemRetryable is meaningful only beside a Problem (Found.Problem).
		case f.Problem == "" && f.ProblemRetryable:
			return fmt.Errorf("found[%d] (%s): ProblemRetryable is set without a Problem", i, f.Name)
		// An exclusion is a decision about an object that was seen, a Problem
		// is a fault: one object cannot be both.
		case f.Problem != "" && f.Excluded != "":
			return fmt.Errorf("found[%d] (%s): exclusion %q is set beside a Problem", i, f.Name, f.Excluded)
		case f.Excluded != "" && f.Excluded != model.ExclusionIgnored && f.Excluded != model.ExclusionCovered:
			return fmt.Errorf("found[%d] (%s): exclusion %q", i, f.Name, f.Excluded)
		// The reason is what an operator reads: an exclusion without one is a
		// silent skip, and a reason without an exclusion explains nothing.
		case f.Excluded != "" && f.ExcludedReason == "":
			return fmt.Errorf("found[%d] (%s): Excluded is set without an ExcludedReason", i, f.Name)
		case f.Excluded == "" && f.ExcludedReason != "":
			return fmt.Errorf("found[%d] (%s): ExcludedReason is set without an Excluded", i, f.Name)
		// An excluded Found builds no source, so Type, Config and Pipeline are
		// not required of it. A Found carrying a Problem is an identity plus an
		// envelope too: Type and Config may be empty for it, but its Pipeline
		// stays, because the failed artifact is reported under it.
		case f.Excluded == "" && len(f.Pipeline) == 0:
			return fmt.Errorf("found[%d] (%s): Pipeline is empty", i, f.Name)
		case f.Problem == "" && f.Excluded == "" && f.Type == "":
			return fmt.Errorf("found[%d] (%s): Type is empty", i, f.Name)
		case f.Problem == "" && f.Excluded == "" && (len(f.Config) == 0 || !jsontext.Value(f.Config).IsValid()):
			return fmt.Errorf("found[%d] (%s): Config is not valid JSON", i, f.Name)
		}
		if seen[f.Name] {
			return fmt.Errorf("found[%d] (%s): Name repeats within one result", i, f.Name)
		}
		seen[f.Name] = true
	}
	return nil
}

func checkCancel(t *testing.T, d discovery.Discoverer) error {
	return plugintest.MustReturnWithError(t, 15*time.Second, "Discover", func(ctx context.Context) error {
		_, err := d.Discover(ctx)
		return err
	})
}
