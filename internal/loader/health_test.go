package loader_test

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
)

func TestHealthIsEmptyForAValidRegistry(t *testing.T) {
	r := loader.New()
	addPlugin(t, r, loader.Spec{ID: "alpha"}, func(r *loader.Registry) error {
		return loader.OnRole(r, "host", loader.Lifecycle{Stop: func(context.Context) error { return nil }})
	})
	addPlugin(t, r, loader.Spec{ID: "beta", Requires: []string{"alpha"}}, nil)
	addHostRole(t, r)
	if problems := loader.Health(r); len(problems) != 0 {
		t.Fatalf("Health = %q, want a clean registry", problems)
	}
}

func TestHealthReportsIntrinsicProblems(t *testing.T) {
	unique := loader.NewKind[seam]("unique", loader.KindOptions[seam]{Unique: true})
	r := loader.New()
	addPlugin(t, r, loader.Spec{ID: "core", Floor: true, Requires: []string{"optional"}}, nil)
	addPlugin(t, r, loader.Spec{ID: "optional"}, nil)
	addPlugin(t, r, loader.Spec{ID: "self", Requires: []string{"self"}}, nil)
	addPlugin(t, r, loader.Spec{ID: "orphan", Requires: []string{"ghost"}}, nil)
	addPlugin(t, r, loader.Spec{ID: "left", Requires: []string{"right"}}, nil)
	addPlugin(t, r, loader.Spec{ID: "right", Requires: []string{"left"}}, nil)
	addPlugin(t, r, loader.Spec{ID: "hooked"}, func(r *loader.Registry) error {
		return loader.OnRole(r, "missing-role", loader.Lifecycle{Stop: func(context.Context) error { return nil }})
	})
	for _, id := range []string{"alpha", "beta"} {
		addPlugin(t, r, loader.Spec{ID: id}, func(r *loader.Registry) error {
			if err := loader.Provide(r, capability, id); err != nil {
				return err
			}
			return loader.Register(r, unique, seam(id))
		})
	}
	problems := loader.Health(r)
	for _, want := range []string{
		`plugin "self" depends on itself`,
		`plugin "orphan" depends on unregistered plugin "ghost"`,
		`loader: "orphan" needs unknown id "ghost"`,
		"no default role is registered",
		`plugin "hooked" has a lifecycle hook for unregistered role "missing-role"`,
		`kind "unique" has 2 contributions, want at most one`,
		`service "capability" has 2 active providers, want one`,
	} {
		assertContains(t, problems, want)
	}
	for _, problem := range problems {
		if strings.Contains(problem, "depends on optional plugin") {
			t.Fatalf("Health = %q, a floor dependency must join the closure instead", problems)
		}
	}
	if !slices.IsSorted(problems) {
		t.Fatalf("Health = %q, want a sorted report", problems)
	}
	if got := loader.FloorIDs(r); !slices.Contains(got, "optional") {
		t.Fatalf("FloorIDs = %v, want the dependency of a floor plugin promoted", got)
	}
}

func TestHealthReportsCycles(t *testing.T) {
	r := loader.New()
	addPlugin(t, r, loader.Spec{ID: "left", Requires: []string{"right"}}, nil)
	addPlugin(t, r, loader.Spec{ID: "right", Requires: []string{"left"}}, nil)
	addHostRole(t, r)
	assertContains(t, loader.Health(r), "dependency cycle includes")
}
