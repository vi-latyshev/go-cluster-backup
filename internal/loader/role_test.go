package loader_test

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
)

func TestOrderSortsByNeedsAndAfter(t *testing.T) {
	got, err := loader.Order([]loader.Ordering{
		{ID: "gamma", Needs: []string{"beta"}},
		{ID: "alpha"},
		{ID: "beta", Needs: []string{"alpha"}, After: []string{"ghost"}},
	})
	if err != nil || !slices.Equal(got, []string{"alpha", "beta", "gamma"}) {
		t.Fatalf("Order = %v, %v, want [alpha beta gamma]", got, err)
	}
}

func TestOrderKeepsTheInputOrderForPeers(t *testing.T) {
	got, err := loader.Order([]loader.Ordering{{ID: "zulu"}, {ID: "alpha"}})
	if err != nil || !slices.Equal(got, []string{"zulu", "alpha"}) {
		t.Fatalf("Order = %v, %v, want input order for independent ids", got, err)
	}
	got, err = loader.Order([]loader.Ordering{{ID: "beta", After: []string{"alpha"}}, {ID: "alpha"}})
	if err != nil || !slices.Equal(got, []string{"alpha", "beta"}) {
		t.Fatalf("Order with a soft After = %v, %v, want [alpha beta]", got, err)
	}
}

func TestOrderIgnoresASelfAfter(t *testing.T) {
	got, err := loader.Order([]loader.Ordering{{ID: "alpha", After: []string{"alpha"}}})
	if err != nil || !slices.Equal(got, []string{"alpha"}) {
		t.Fatalf("Order = %v, %v, want a self hint ignored", got, err)
	}
}

func TestOrderRefusesBrokenSets(t *testing.T) {
	tests := []struct {
		name  string
		nodes []loader.Ordering
		want  string
	}{
		{"empty id", []loader.Ordering{{ID: ""}}, "ordering id is empty"},
		{"duplicate id", []loader.Ordering{{ID: "alpha"}, {ID: "alpha"}}, "appears twice"},
		{"unknown need", []loader.Ordering{{ID: "alpha", Needs: []string{"ghost"}}}, `needs unknown id "ghost"`},
		{"cycle", []loader.Ordering{
			{ID: "alpha", Needs: []string{"beta"}},
			{ID: "beta", Needs: []string{"alpha"}},
		}, "dependency cycle includes"},
		{"self need", []loader.Ordering{{ID: "alpha", Needs: []string{"alpha"}}}, "dependency cycle includes"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got, err := loader.Order(test.nodes); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Order = %v, %v, want error containing %q", got, err, test.want)
			}
		})
	}
}

func TestRoleRefusesInvalidSpecs(t *testing.T) {
	spec := func(name string) loader.RoleSpec {
		return loader.RoleSpec{
			Name:     name,
			Run:      func(context.Context, loader.Session) int { return 0 },
			Disabled: func(loader.Session) int { return 1 },
		}
	}
	tests := []struct {
		name  string
		apply func(*loader.Registry) error
		want  string
	}{
		{"empty name", func(r *loader.Registry) error { return loader.Role(r, spec("")) }, "role name is empty"},
		{"no run", func(r *loader.Registry) error {
			role := spec("host")
			role.Run = nil
			return loader.Role(r, role)
		}, "has no Run"},
		{"no disabled outcome", func(r *loader.Registry) error {
			role := spec("host")
			role.Disabled = nil
			return loader.Role(r, role)
		}, "has no Disabled outcome"},
		{"duplicate role", func(r *loader.Registry) error {
			if err := loader.Role(r, spec("host")); err != nil {
				return err
			}
			return loader.Role(r, spec("host"))
		}, "registered twice"},
		{"second default", func(r *loader.Registry) error {
			first := spec("first")
			first.Default = true
			second := spec("second")
			second.Default = true
			if err := loader.Role(r, first); err != nil {
				return err
			}
			return loader.Role(r, second)
		}, "second default role"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := pluginErr(test.apply)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Add = %v, want error containing %q", err, test.want)
			}
		})
	}
	if err := loader.Role(loader.New(), spec("host")); err == nil ||
		!strings.Contains(err.Error(), "outside Add") {
		t.Fatalf("Role outside Add = %v, want refusal", err)
	}
}

func TestOnRoleRefusesInvalidSpecs(t *testing.T) {
	lifecycle := loader.Lifecycle{Stop: func(context.Context) error { return nil }}
	tests := []struct {
		name  string
		apply func(*loader.Registry) error
		want  string
	}{
		{"empty role", func(r *loader.Registry) error { return loader.OnRole(r, "", lifecycle) }, "lifecycle role is empty"},
		{"no phase", func(r *loader.Registry) error { return loader.OnRole(r, "host", loader.Lifecycle{}) }, "has no phase"},
		{"negative budget", func(r *loader.Registry) error {
			return loader.OnRole(r, "host", loader.Lifecycle{Stop: lifecycle.Stop, Budget: -time.Second})
		}, "negative budget"},
		{"duplicate hook", func(r *loader.Registry) error {
			if err := loader.OnRole(r, "host", lifecycle); err != nil {
				return err
			}
			return loader.OnRole(r, "host", lifecycle)
		}, "registered twice"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := pluginErr(test.apply)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Add = %v, want error containing %q", err, test.want)
			}
		})
	}
	if err := loader.OnRole(loader.New(), "host", lifecycle); err == nil ||
		!strings.Contains(err.Error(), "outside Add") {
		t.Fatalf("OnRole outside Add = %v, want refusal", err)
	}
}

func TestOnRoleAcceptsEveryValidPhase(t *testing.T) {
	phase := func(context.Context) error { return nil }
	r := loader.New()
	addPlugin(t, r, loader.Spec{ID: "owner"}, func(r *loader.Registry) error {
		if err := loader.Role(r, loader.RoleSpec{
			Name:     "host",
			Default:  true,
			Run:      func(context.Context, loader.Session) int { return 0 },
			Disabled: func(loader.Session) int { return 1 },
		}); err != nil {
			return err
		}
		return loader.OnRole(r, "host", loader.Lifecycle{Start: phase, Quiesce: phase, Stop: phase})
	})
	if problems := loader.Health(r); len(problems) != 0 {
		t.Fatalf("Health = %v, want a valid lifecycle", problems)
	}
}

// pluginErr runs an apply inside one plugin's registration.
func pluginErr(apply func(*loader.Registry) error) error {
	return loader.Add(loader.New(), loader.Spec{ID: "owner"}, apply)
}

func TestRolesListsEveryProvider(t *testing.T) {
	role := func(name string, def bool) loader.RoleSpec {
		return loader.RoleSpec{
			Name:     name,
			Default:  def,
			Run:      func(context.Context, loader.Session) int { return 0 },
			Disabled: func(loader.Session) int { return 1 },
		}
	}
	r := loader.New()
	addPlugin(t, r, loader.Spec{ID: "host"}, func(r *loader.Registry) error {
		return loader.Role(r, role("host", true))
	})
	addPlugin(t, r, loader.Spec{ID: "worker"}, func(r *loader.Registry) error {
		if err := loader.Role(r, role("worker", false)); err != nil {
			return err
		}
		return loader.Role(r, role("stage", false))
	})
	check := func(when string) {
		t.Helper()
		got := loader.Roles(r)
		if len(got) != 2 || !slices.Equal(got["host"], []string{"host"}) ||
			!slices.Equal(got["worker"], []string{"worker", "stage"}) {
			t.Fatalf("Roles %s = %v", when, got)
		}
	}
	check("before selection")
	loader.ApplySelection(r, []string{"worker"})
	check("after disabling the owner")
}
