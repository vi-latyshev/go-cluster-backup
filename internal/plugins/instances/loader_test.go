package instances_test

import (
	"context"
	"strings"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
)

// addSample registers one sample plugin in a loader registry and contributes its
// factory exactly the way a backend does: one AddFactory call inside the apply.
func addSample(t *testing.T, lr *loader.Registry, factories *instances.Registry, id string) {
	t.Helper()
	err := loader.Add(lr, loader.Spec{ID: id, Summary: "a sample"}, func(r *loader.Registry) error {
		return instances.AddFactory(r, factories, sampleKind, instances.FactorySpec[treeConfig, sample]{
			Type: "tree", Summary: "a sample", Stability: instances.Stable, Example: example(), New: newSample,
		})
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestAddFactoryContributesADescriptorThatOutlivesTheSelection: the schema kind
// is compiled, not active — disabling the backend's plugin removes its
// behaviour, never its config from merge, shape, describe or schema.
func TestAddFactoryContributesADescriptorThatOutlivesTheSelection(t *testing.T) {
	lr := loader.New()
	factories := instances.NewRegistry()
	addSample(t, lr, factories, "sample/tree")

	descs := loader.All(lr, instances.Descriptors)
	if len(descs) != 1 {
		t.Fatalf("Descriptors = %d, want one", len(descs))
	}
	d := descs[0]
	if d.ID != "sample/tree" || d.Kind != "sample" || d.Type != "tree" ||
		d.Summary != "a sample" || d.Stability != instances.Stable {
		t.Fatalf("contributed descriptor: %+v", d)
	}
	if _, ok := factories.ConfigType("sample", "tree"); !ok {
		t.Fatal("the factory did not land in the shared registry")
	}

	if problems := loader.ApplySelection(lr, []string{"sample/tree"}); len(problems) != 0 {
		t.Fatalf("disabling the plugin: %v", problems)
	}
	if loader.Active(lr, "sample/tree") {
		t.Fatal("the plugin is still active")
	}
	if descs := loader.All(lr, instances.Descriptors); len(descs) != 1 {
		t.Fatalf("the compiled schema lost a disabled plugin's descriptor: %d", len(descs))
	}
}

// TestBuildActiveConstructsOnlyAnActiveFactory: the factory builds while its
// plugin is active and is refused by id once the deployment disables it.
func TestBuildActiveConstructsOnlyAnActiveFactory(t *testing.T) {
	lr := loader.New()
	factories := instances.NewRegistry()
	addSample(t, lr, factories, "sample/tree")
	ctx := context.Background()
	deps := instances.Deps{Instance: "one"}

	v, err := instances.BuildActive(ctx, lr, factories, sampleKind, "tree", example(), deps)
	if err != nil {
		t.Fatal(err)
	}
	if v.Sample() != "x" {
		t.Fatalf("Sample = %q", v.Sample())
	}

	if problems := loader.ApplySelection(lr, []string{"sample/tree"}); len(problems) != 0 {
		t.Fatalf("disabling the plugin: %v", problems)
	}
	if _, err := instances.BuildActive(ctx, lr, factories, sampleKind, "tree", example(), deps); err == nil ||
		!strings.Contains(err.Error(), "sample/tree: plugin is disabled") {
		t.Fatalf("a disabled factory was built: %v", err)
	}
}

// TestBuildActiveReportsAnUnknownFactory: a type nobody registered is an
// unknown type, not a disabled plugin.
func TestBuildActiveReportsAnUnknownFactory(t *testing.T) {
	lr := loader.New()
	factories := instances.NewRegistry()
	addSample(t, lr, factories, "sample/tree")
	_, err := instances.BuildActive(context.Background(), lr, factories, sampleKind, "ghost", example(),
		instances.Deps{Instance: "one"})
	if err == nil || !strings.Contains(err.Error(), `unknown sample type "ghost"`) {
		t.Fatalf("an unknown factory = %v", err)
	}
}

// TestAddFactoryRejectsADuplicateFactory: the loader registry gets the error
// from the typed one and no second descriptor reaches the schema.
func TestAddFactoryRejectsADuplicateFactory(t *testing.T) {
	lr := loader.New()
	factories := instances.NewRegistry()
	addSample(t, lr, factories, "sample/tree")
	err := loader.Add(lr, loader.Spec{ID: "sample/two"}, func(r *loader.Registry) error {
		return instances.AddFactory(r, factories, sampleKind, instances.FactorySpec[treeConfig, sample]{
			Type: "tree", New: newSample,
		})
	})
	if err == nil || !strings.Contains(err.Error(), "registered twice") {
		t.Fatalf("a duplicate factory = %v", err)
	}
	if descs := loader.All(lr, instances.Descriptors); len(descs) != 1 {
		t.Fatalf("Descriptors = %d, want the first one only", len(descs))
	}
}

// TestAddFactoryOutsideAddFails: a contribution needs the plugin whose apply
// made it, and the loader refuses one made from nowhere.
func TestAddFactoryOutsideAddFails(t *testing.T) {
	err := instances.AddFactory(loader.New(), instances.NewRegistry(), sampleKind,
		instances.FactorySpec[treeConfig, sample]{Type: "tree", New: newSample})
	if err == nil || !strings.Contains(err.Error(), "outside Add") {
		t.Fatalf("a factory contributed outside Add = %v", err)
	}
}
