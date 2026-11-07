package loader_test

import (
	"context"
	"slices"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
)

// fixedSelector is a selection that answers a literal disabled list.
type fixedSelector []string

func (s fixedSelector) Disabled(context.Context) ([]string, error) { return s, nil }

// brokenSelector is a selection that cannot read its configuration.
type brokenSelector struct{}

func (brokenSelector) Disabled(context.Context) ([]string, error) {
	return nil, errSelectionUnreadable
}

var errSelectionUnreadable = fixedError("selection source is unreadable")

type fixedError string

func (e fixedError) Error() string { return string(e) }

// withSelection registers one plugin that provides Selection.
func withSelection(t *testing.T, r *loader.Registry, owner string, disabled ...string) {
	t.Helper()
	addPlugin(t, r, loader.Spec{ID: owner}, func(r *loader.Registry) error {
		return loader.Provide[loader.Selector](r, loader.Selection, fixedSelector(disabled))
	})
}

func TestApplySelectionRefusesInapplicableDisables(t *testing.T) {
	r := loader.New()
	addSelectable(t, r, loader.Spec{ID: "core", Floor: true})
	addSelectable(t, r, loader.Spec{ID: "alpha", Requires: []string{"beta"}})
	addSelectable(t, r, loader.Spec{ID: "beta", Requires: []string{"gamma"}})
	addSelectable(t, r, loader.Spec{ID: "gamma"})
	addSelectable(t, r, loader.Spec{ID: "solo", Requires: []string{"dep"}})
	addSelectable(t, r, loader.Spec{ID: "dep"})
	addPlugin(t, r, loader.Spec{ID: "library"}, nil)
	withSelection(t, r, "config")

	problems := loader.ApplySelection(r, []string{
		"ghost", "core", "library", "config", "beta", "gamma", "solo", "dep", "beta",
	})
	want := []string{
		`selection: unknown plugin "ghost"`,
		`selection: floor plugin "core" cannot be disabled`,
		`selection: library plugin "library" is always active and cannot be disabled`,
		`selection: the selection provider "config" cannot be disabled`,
		`selection: plugin "beta" cannot be disabled while active plugin "alpha" depends on it`,
		`selection: plugin "gamma" cannot be disabled while active plugin "beta" depends on it`,
	}
	if !slices.Equal(problems, want) {
		t.Fatalf("ApplySelection = %q, want %q", problems, want)
	}
	for _, id := range []string{"core", "library", "alpha", "beta", "gamma", "config"} {
		if !loader.Active(r, id) {
			t.Fatalf("%s must stay active", id)
		}
	}
	for _, id := range []string{"solo", "dep"} {
		if loader.Active(r, id) {
			t.Fatalf("%s must stay disabled", id)
		}
	}
}

func TestApplySelectionRefusesFloorDependencies(t *testing.T) {
	r := loader.New()
	addSelectable(t, r, loader.Spec{ID: "core", Floor: true, Requires: []string{"mid"}})
	addSelectable(t, r, loader.Spec{ID: "mid", Requires: []string{"leaf"}})
	addSelectable(t, r, loader.Spec{ID: "leaf"})
	addSelectable(t, r, loader.Spec{ID: "free"})
	problems := loader.ApplySelection(r, []string{"mid", "leaf", "free"})
	want := []string{
		`selection: plugin "mid" is a floor dependency and cannot be disabled`,
		`selection: plugin "leaf" is a floor dependency and cannot be disabled`,
	}
	if !slices.Equal(problems, want) {
		t.Fatalf("ApplySelection = %q, want %q", problems, want)
	}
	if loader.Active(r, "free") {
		t.Fatal("a plugin outside the floor closure must be disableable")
	}
}

func TestApplySelectionIgnoresDuplicateEntries(t *testing.T) {
	r := loader.New()
	addSelectable(t, r, loader.Spec{ID: "alpha"})
	addPlugin(t, r, loader.Spec{ID: "library"}, nil)
	if problems := loader.ApplySelection(r, []string{"alpha", "alpha"}); len(problems) != 0 {
		t.Fatalf("ApplySelection = %q, want no problem for a repeated id", problems)
	}
	if loader.Active(r, "alpha") {
		t.Fatal("alpha must be disabled")
	}
	if problems := loader.ApplySelection(r, []string{"library", "library"}); len(problems) != 1 {
		t.Fatalf("ApplySelection(library twice) = %q, want one refusal for a repeated id", problems)
	}
	if !loader.Active(r, "library") {
		t.Fatal("a library must stay active")
	}
}

func TestApplySelectionRecordsProblemsInHealth(t *testing.T) {
	r := loader.New()
	addSelectable(t, r, loader.Spec{ID: "core", Floor: true})
	loader.ApplySelection(r, []string{"core"})
	addHostRole(t, r)
	assertContains(t, loader.Health(r), `selection: floor plugin "core" cannot be disabled`)
}

func TestSelectionProviderNeedsExactlyOneBinding(t *testing.T) {
	r := loader.New()
	withSelection(t, r, "config")
	addSelectable(t, r, loader.Spec{ID: "other"})
	if _, ok := loader.Use(r, loader.Selection); !ok {
		t.Fatal("the single selection provider is not usable")
	}
	problems := loader.ApplySelection(r, []string{"other"})
	if len(problems) != 0 || loader.Active(r, "other") {
		t.Fatalf("ApplySelection = %q, want an ordinary disable", problems)
	}
}

func TestSelectionProviderRefusesAmbiguity(t *testing.T) {
	r := loader.New()
	withSelection(t, r, "one")
	withSelection(t, r, "two")
	if _, ok := loader.Use(r, loader.Selection); ok {
		t.Fatal("an ambiguous selection is usable")
	}
	problem := loader.ApplySelection(r, []string{"one"})
	if len(problem) != 0 {
		t.Fatalf("ApplySelection = %q, want no selection-provider protection when the binding is ambiguous", problem)
	}
}
