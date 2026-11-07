package loader_test

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
)

type seam string

var (
	behaviorKind = loader.NewKind[seam]("behavior", loader.KindOptions[seam]{})
	schemaKind   = loader.NewKind[seam]("schema", loader.KindOptions[seam]{Schema: true})
	capability   = loader.NewService[string]("capability")
)

func TestRegisterNeedsAnOwningPlugin(t *testing.T) {
	err := loader.Register(loader.New(), behaviorKind, "x")
	if err == nil || !strings.Contains(err.Error(), "outside Add") {
		t.Fatalf("Register outside Add = %v, want refusal", err)
	}
}

func TestRegisterRejectsAnEmptyKindName(t *testing.T) {
	r := loader.New()
	err := loader.Add(r, loader.Spec{ID: "owner"}, func(r *loader.Registry) error {
		return loader.Register(r, loader.NewKind[seam]("", loader.KindOptions[seam]{}), "x")
	})
	if err == nil || !strings.Contains(err.Error(), "kind name is empty") {
		t.Fatalf("Add = %v, want an empty kind name refusal", err)
	}
}

func TestRegisterRejectsAMismatchedKindType(t *testing.T) {
	r := loader.New()
	if err := loader.Add(r, loader.Spec{ID: "alpha"}, func(r *loader.Registry) error {
		return loader.Register(r, loader.NewKind[seam]("mixed", loader.KindOptions[seam]{}), "x")
	}); err != nil {
		t.Fatalf("Add(alpha) = %v", err)
	}
	err := loader.Add(r, loader.Spec{ID: "beta"}, func(r *loader.Registry) error {
		return loader.Register(r, loader.NewKind[int]("mixed", loader.KindOptions[int]{}), 1)
	})
	if err == nil || !strings.Contains(err.Error(), "different value type") {
		t.Fatalf("Add(beta) = %v, want a mismatched type refusal", err)
	}
}

func TestAllSchemaSeesDisabledContributions(t *testing.T) {
	r := loader.New()
	addPlugin(t, r, loader.Spec{ID: "alpha"}, func(r *loader.Registry) error {
		if err := loader.Register(r, schemaKind, "a-schema"); err != nil {
			return err
		}
		return loader.Register(r, behaviorKind, "a-behavior")
	})
	addPlugin(t, r, loader.Spec{ID: "beta"}, func(r *loader.Registry) error {
		if err := loader.Register(r, schemaKind, "b-schema"); err != nil {
			return err
		}
		return loader.Register(r, behaviorKind, "b-behavior")
	})
	loader.ApplySelection(r, []string{"beta"})
	if got := loader.All(r, schemaKind); !slices.Equal(got, []seam{"a-schema", "b-schema"}) {
		t.Fatalf("All(schema) = %v, want every compiled contribution", got)
	}
	if got := loader.All(r, behaviorKind); !slices.Equal(got, []seam{"a-behavior"}) {
		t.Fatalf("All(behavior) = %v, want active contributions only", got)
	}
	if got := loader.All(r, loader.NewKind[seam]("missing", loader.KindOptions[seam]{})); got != nil {
		t.Fatalf("All(missing) = %v, want nil", got)
	}
	if got := loader.All(r, loader.NewKind[int]("behavior", loader.KindOptions[int]{})); got != nil {
		t.Fatalf("All with a mismatched token = %v, want nil", got)
	}
}

func TestHealthChecksKindUniquenessAndValidation(t *testing.T) {
	unique := loader.NewKind[seam]("unique", loader.KindOptions[seam]{Unique: true})
	invalid := loader.NewKind[seam]("invalid", loader.KindOptions[seam]{
		Validate: func(all []seam) error {
			if len(all) != 0 {
				return errors.New("not acceptable")
			}
			return nil
		},
	})
	r := loader.New()
	addPlugin(t, r, loader.Spec{ID: "alpha"}, func(r *loader.Registry) error {
		if err := loader.Register(r, unique, "one"); err != nil {
			return err
		}
		if err := loader.Register(r, unique, "two"); err != nil {
			return err
		}
		return loader.Register(r, invalid, "bad")
	})
	addHostRole(t, r)
	problems := loader.Health(r)
	assertContains(t, problems, `kind "unique" has 2 contributions, want at most one`)
	assertContains(t, problems, `kind "invalid": not acceptable`)
}

func TestProvideNeedsAnOwningPlugin(t *testing.T) {
	err := loader.Provide(loader.New(), capability, "value")
	if err == nil || !strings.Contains(err.Error(), "outside Add") {
		t.Fatalf("Provide outside Add = %v, want refusal", err)
	}
}

func TestProvideRejectsAnEmptyServiceName(t *testing.T) {
	r := loader.New()
	err := loader.Add(r, loader.Spec{ID: "owner"}, func(r *loader.Registry) error {
		return loader.Provide(r, loader.NewService[string](""), "value")
	})
	if err == nil || !strings.Contains(err.Error(), "service name is empty") {
		t.Fatalf("Add = %v, want an empty service name refusal", err)
	}
}

func TestProvideRejectsAMismatchedServiceType(t *testing.T) {
	r := loader.New()
	if err := loader.Add(r, loader.Spec{ID: "alpha"}, func(r *loader.Registry) error {
		return loader.Provide(r, loader.NewService[string]("mixed"), "value")
	}); err != nil {
		t.Fatalf("Add(alpha) = %v", err)
	}
	err := loader.Add(r, loader.Spec{ID: "beta"}, func(r *loader.Registry) error {
		return loader.Provide(r, loader.NewService[int]("mixed"), 1)
	})
	if err == nil || !strings.Contains(err.Error(), "different value type") {
		t.Fatalf("Add(beta) = %v, want a mismatched type refusal", err)
	}
}

func TestUseAnswersOnlyOneActiveBinding(t *testing.T) {
	r := loader.New()
	addPlugin(t, r, loader.Spec{ID: "alpha"}, func(r *loader.Registry) error {
		return loader.Provide(r, capability, "alpha-value")
	})
	if value, ok := loader.Use(r, capability); !ok || value != "alpha-value" {
		t.Fatalf("Use = %q, %v, want the single binding", value, ok)
	}
	if value, ok := loader.Use(r, loader.NewService[string]("missing")); ok || value != "" {
		t.Fatalf("Use(missing) = %q, %v, want absence", value, ok)
	}
	if value, ok := loader.Use(r, loader.NewService[int]("capability")); ok || value != 0 {
		t.Fatalf("Use with a mismatched token = %d, %v, want absence", value, ok)
	}
	loader.ApplySelection(r, []string{"alpha"})
	if value, ok := loader.Use(r, capability); ok || value != "" {
		t.Fatalf("Use after disabling the owner = %q, %v, want absence", value, ok)
	}
}

func TestUseRefusesAnAmbiguousBinding(t *testing.T) {
	r := loader.New()
	for _, id := range []string{"alpha", "beta"} {
		addPlugin(t, r, loader.Spec{ID: id}, func(r *loader.Registry) error {
			return loader.Provide(r, capability, id)
		})
	}
	if value, ok := loader.Use(r, capability); ok || value != "" {
		t.Fatalf("Use with two providers = %q, %v, want refusal", value, ok)
	}
	addHostRole(t, r)
	assertContains(t, loader.Health(r), `service "capability" has 2 active providers, want one`)
}

func TestUseAfterDisablingOneOfTwoProviders(t *testing.T) {
	r := loader.New()
	for _, id := range []string{"alpha", "beta"} {
		addPlugin(t, r, loader.Spec{ID: id}, func(r *loader.Registry) error {
			return loader.Provide(r, capability, id)
		})
	}
	loader.ApplySelection(r, []string{"beta"})
	if value, ok := loader.Use(r, capability); !ok || value != "alpha" {
		t.Fatalf("Use = %q, %v, want alpha", value, ok)
	}
}
