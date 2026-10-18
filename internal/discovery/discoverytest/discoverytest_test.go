package discoverytest

import (
	"context"
	"errors"
	"testing"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/discovery"
	"github.com/vi-latyshev/go-cluster-backup/internal/model"
)

type finder struct {
	found []discovery.Found
	mode  string
	// err is what Discover answers with, for a discoverer that cannot answer
	// at all.
	err error
}

func (f finder) Discover(ctx context.Context) ([]discovery.Found, error) {
	if err := ctx.Err(); err != nil && f.mode != "ignorectx" {
		return nil, err
	}
	if f.err != nil {
		return nil, f.err
	}
	return f.found, nil
}

func goodFind() discovery.Found {
	return discovery.Found{
		Name:     "workloads/postgres",
		Type:     "postgres",
		Config:   jsontext.Value(`{"address":"db:5432"}`),
		Artifact: model.Artifact{Group: "databases", Name: "ns/postgres", Importance: model.ImportanceRequired, Sensitivity: model.SensitivityPlain},
		Pipeline: []string{"compress"},
	}
}

func TestFinderPassesTheSuite(t *testing.T) {
	Run(t, func(*testing.T) discovery.Discoverer { return finder{found: []discovery.Found{goodFind()}} })
}

func TestChecksCatchBrokenDiscoverers(t *testing.T) {
	badName := goodFind()
	badName.Name = ""
	badConfig := goodFind()
	badConfig.Config = jsontext.Value(`{not json`)
	badSensitivity := goodFind()
	badSensitivity.Artifact.Sensitivity = "very"
	repeat := goodFind()

	cases := []struct {
		check string
		d     discovery.Discoverer
	}{
		{"discover returns well-formed sources", finder{found: []discovery.Found{badName}}},
		{"discover returns well-formed sources", finder{found: []discovery.Found{badConfig}}},
		{"discover returns well-formed sources", finder{found: []discovery.Found{badSensitivity}}},
		{"discover returns well-formed sources", finder{found: []discovery.Found{repeat, repeat}}},
		// A discoverer whose Discover fails has no sources to check: the
		// failure is the suite's, named as the check that met it.
		{"discover returns well-formed sources", finder{err: errors.New("the API is unreachable")}},
		{"returns on cancellation", finder{mode: "ignorectx", found: []discovery.Found{goodFind()}}},
	}
	for _, tc := range cases {
		t.Run(tc.check, func(t *testing.T) {
			for _, c := range Checks {
				if c.Name != tc.check {
					continue
				}
				if err := c.Run(t, tc.d); err == nil {
					t.Fatalf("check %q passed a broken discoverer", c.Name)
				}
				return
			}
			t.Fatalf("check %q not found", tc.check)
		})
	}
}

// TestExcludedFoundNeedsNoSource: an exclusion is a decision about an object,
// not a source to build, so the suite accepts one that carries no Type, Config
// or Pipeline — and still demands its reason (TestDiscoverCatchesEachMalformedField).
func TestExcludedFoundNeedsNoSource(t *testing.T) {
	f := goodFind()
	f.Excluded = model.ExclusionIgnored
	f.ExcludedReason = "the marker the rule's config names"
	f.Type, f.Config, f.Pipeline = "", nil, nil
	Run(t, func(*testing.T) discovery.Discoverer { return finder{found: []discovery.Found{f}} })
}

// TestExcludedNamesCountForUniqueness: the instance name of an excluded object
// is as real as any other — two objects of one result may not share it.
func TestExcludedNamesCountForUniqueness(t *testing.T) {
	a := goodFind()
	a.Name, a.Excluded, a.ExcludedReason = "same", model.ExclusionIgnored, "why"
	b := a
	for _, c := range Checks {
		if c.Name != "discover returns well-formed sources" {
			continue
		}
		if err := c.Run(t, finder{found: []discovery.Found{a, b}}); err == nil {
			t.Fatal("two excluded Founds with one name passed")
		}
		return
	}
	t.Fatal("the well-formed check was not found")
}

func TestDiscoverCatchesEachMalformedField(t *testing.T) {
	mutate := map[string]func(*discovery.Found){
		"missing type":     func(f *discovery.Found) { f.Type = "" },
		"missing group":    func(f *discovery.Found) { f.Artifact.Group = "" },
		"missing artifact": func(f *discovery.Found) { f.Artifact.Name = "" },
		"missing pipeline": func(f *discovery.Found) { f.Pipeline = nil },
		"bad importance":   func(f *discovery.Found) { f.Artifact.Importance = "maybe" },
		"duplicate names":  func(f *discovery.Found) { f.Name = "same" },
		"empty config":     func(f *discovery.Found) { f.Config = jsontext.Value(``) },
		// ProblemRetryable without a Problem is meaningless and refused.
		"retryable without a problem": func(f *discovery.Found) { f.ProblemRetryable = true },
		"exclusion without a reason":  func(f *discovery.Found) { f.Excluded = model.ExclusionIgnored },
		"reason without an exclusion": func(f *discovery.Found) { f.ExcludedReason = "why" },
		"unknown exclusion": func(f *discovery.Found) {
			f.Excluded, f.ExcludedReason = "maybe", "why"
		},
		"exclusion beside a problem": func(f *discovery.Found) {
			f.Excluded, f.ExcludedReason, f.Problem = model.ExclusionCovered, "why", "boom"
		},
	}
	for _, c := range Checks {
		if c.Name != "discover returns well-formed sources" {
			continue
		}
		for name, fix := range mutate {
			f1, f2 := goodFind(), goodFind()
			fix(&f1)
			fix(&f2)
			if name == "duplicate names" {
				if err := c.Run(t, finder{found: []discovery.Found{f1, f2}}); err == nil {
					t.Errorf("%s passed", name)
				}
				continue
			}
			if err := c.Run(t, finder{found: []discovery.Found{f1}}); err == nil {
				t.Errorf("%s passed", name)
			}
		}
		return
	}
}
