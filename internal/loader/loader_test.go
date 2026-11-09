package loader_test

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
)

func TestAddRegistersPluginsInOrder(t *testing.T) {
	r := loader.New()
	if err := loader.Add(r, loader.Spec{ID: "alpha", Summary: "first"}, nil); err != nil {
		t.Fatalf("Add(alpha) = %v", err)
	}
	if err := loader.Add(r, loader.Spec{ID: "beta", Summary: "second", Floor: true}, func(*loader.Registry) error {
		return nil
	}); err != nil {
		t.Fatalf("Add(beta) = %v", err)
	}
	want := []loader.Plugin{{ID: "alpha", Summary: "first"}, {ID: "beta", Summary: "second", Floor: true}}
	if got := loader.Plugins(r); !slices.Equal(got, want) {
		t.Fatalf("Plugins = %+v, want %+v", got, want)
	}
}

func TestAddRejectsInvalidRegistration(t *testing.T) {
	tests := []struct {
		name string
		add  func(*loader.Registry) error
		want string
	}{
		{
			name: "empty id",
			add:  func(r *loader.Registry) error { return loader.Add(r, loader.Spec{}, nil) },
			want: "plugin id is empty",
		},
		{
			name: "duplicate id",
			add: func(r *loader.Registry) error {
				if err := loader.Add(r, loader.Spec{ID: "alpha"}, nil); err != nil {
					return err
				}
				return loader.Add(r, loader.Spec{ID: "alpha"}, nil)
			},
			want: "registered twice",
		},
		{
			name: "empty requirement",
			add: func(r *loader.Registry) error {
				return loader.Add(r, loader.Spec{ID: "alpha", Requires: []string{""}}, nil)
			},
			want: "requires an empty id",
		},
		{
			name: "nested registration",
			add: func(r *loader.Registry) error {
				return loader.Add(r, loader.Spec{ID: "alpha"}, func(r *loader.Registry) error {
					return loader.Add(r, loader.Spec{ID: "beta"}, nil)
				})
			},
			want: "registration is not nested",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.add(loader.New())
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Add = %v, want error containing %q", err, test.want)
			}
		})
	}
}

func TestAddWrapsApplyError(t *testing.T) {
	sentinel := errors.New("broken wiring")
	err := loader.Add(loader.New(), loader.Spec{ID: "alpha"}, func(*loader.Registry) error {
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("Add = %v, want the apply error", err)
	}
	if !strings.Contains(err.Error(), "alpha") {
		t.Fatalf("Add = %v, want the plugin id", err)
	}
}

func TestSetImportsOnce(t *testing.T) {
	r := loader.New()
	imports := loader.Imports{"alpha": {"beta"}}
	if err := loader.SetImports(r, imports); err != nil {
		t.Fatalf("SetImports = %v", err)
	}
	if err := loader.SetImports(r, imports); err == nil || !strings.Contains(err.Error(), "already set") {
		t.Fatalf("second SetImports = %v, want refusal", err)
	}
	if err := loader.SetImports(loader.New(), loader.Imports{"": {"alpha"}}); err == nil ||
		!strings.Contains(err.Error(), "empty plugin id") {
		t.Fatalf("SetImports with an empty id = %v, want refusal", err)
	}
	if err := loader.SetImports(loader.New(), loader.Imports{"alpha": {""}}); err == nil ||
		!strings.Contains(err.Error(), "imports an empty plugin id") {
		t.Fatalf("SetImports with an empty dependency = %v, want refusal", err)
	}
}

func TestDependenciesUniteImportsAndRequires(t *testing.T) {
	r := loader.New()
	if err := loader.SetImports(r, loader.Imports{"alpha": {"beta", "gamma"}}); err != nil {
		t.Fatalf("SetImports = %v", err)
	}
	if err := loader.Add(r, loader.Spec{ID: "alpha", Requires: []string{"gamma", "delta"}}, nil); err != nil {
		t.Fatalf("Add(alpha) = %v", err)
	}
	deps, ok := loader.Dependencies(r, "alpha")
	if !ok || !slices.Equal(deps, []string{"beta", "gamma", "delta"}) {
		t.Fatalf("Dependencies(alpha) = %v, %v, want [beta gamma delta] without duplicates", deps, ok)
	}
	if deps, ok := loader.Dependencies(r, "ghost"); ok || deps != nil {
		t.Fatalf("Dependencies(ghost) = %v, %v, want absence", deps, ok)
	}
	if err := loader.Add(r, loader.Spec{ID: "bare"}, nil); err != nil {
		t.Fatalf("Add(bare) = %v", err)
	}
	if deps, ok := loader.Dependencies(r, "bare"); !ok || len(deps) != 0 {
		t.Fatalf("Dependencies(bare) = %v, %v, want no dependencies", deps, ok)
	}
}

func TestActiveFollowsSelection(t *testing.T) {
	r := loader.New()
	for _, id := range []string{"alpha", "beta"} {
		addSelectable(t, r, loader.Spec{ID: id})
	}
	addPlugin(t, r, loader.Spec{ID: "library"}, nil)
	if !loader.Active(r, "alpha") || !loader.Active(r, "beta") || !loader.Active(r, "library") {
		t.Fatal("every compiled plugin must be active before a selection")
	}
	if loader.Active(r, "ghost") {
		t.Fatal("an uncompiled id is active")
	}
	loader.ApplySelection(r, []string{"beta"})
	if !loader.Active(r, "alpha") || loader.Active(r, "beta") || !loader.Active(r, "library") {
		t.Fatal("the active set ignores the selection or drops a library")
	}
}

func TestActiveIDsListTheEffectiveSetInRegistrationOrder(t *testing.T) {
	r := loader.New()
	for _, spec := range []loader.Spec{{ID: "alpha"}, {ID: "beta"}, {ID: "gamma"}, {ID: "library"}} {
		if spec.ID == "library" {
			addPlugin(t, r, spec, nil)
			continue
		}
		addSelectable(t, r, spec)
	}
	if got := loader.ActiveIDs(r); !slices.Equal(got, []string{"alpha", "beta", "gamma", "library"}) {
		t.Fatalf("ActiveIDs before selection = %v", got)
	}
	loader.ApplySelection(r, []string{"alpha", "gamma"})
	if got := loader.ActiveIDs(r); !slices.Equal(got, []string{"beta", "library"}) {
		t.Fatalf("ActiveIDs = %v, want [beta library]", got)
	}
}

func TestFloorIDsPromoteDependencies(t *testing.T) {
	r := loader.New()
	if err := loader.SetImports(r, loader.Imports{"core": {"imported"}}); err != nil {
		t.Fatalf("SetImports = %v", err)
	}
	addSelectable(t, r, loader.Spec{ID: "loose"})
	addSelectable(t, r, loader.Spec{ID: "core", Floor: true, Requires: []string{"required"}})
	addSelectable(t, r, loader.Spec{ID: "required", Requires: []string{"leaf"}})
	addSelectable(t, r, loader.Spec{ID: "imported"})
	addSelectable(t, r, loader.Spec{ID: "leaf"})
	addSelectable(t, r, loader.Spec{ID: "tail", Requires: []string{"leaf"}})
	if got := loader.FloorIDs(r); !slices.Equal(got, []string{"core", "required", "imported", "leaf"}) {
		t.Fatalf("FloorIDs = %v, want the closure in registration order", got)
	}
	if problems := loader.ApplySelection(r, nil); len(problems) != 0 {
		t.Fatalf("ApplySelection = %q, want no problem without a disable", problems)
	}
	if !loader.Active(r, "tail") {
		t.Fatal("a plugin depending on a promoted dependency is ordinary, not floor")
	}
}

func TestFloorIDsSkipAnUnregisteredDependency(t *testing.T) {
	r := loader.New()
	addSelectable(t, r, loader.Spec{ID: "core", Floor: true, Requires: []string{"ghost"}})
	if got := loader.FloorIDs(r); !slices.Equal(got, []string{"core"}) {
		t.Fatalf("FloorIDs = %v, want only compiled plugins", got)
	}
	assertContains(t, loader.Health(r), `plugin "core" depends on unregistered plugin "ghost"`)
}
