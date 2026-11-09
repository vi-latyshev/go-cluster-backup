package components

import (
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
)

// TestComposeInstallsTheHostlessHalf proves the generated hostless half: every
// compiled plugin except the process hosts, over one factory registry, with the
// generated import graph installed once.
func TestComposeInstallsTheHostlessHalf(t *testing.T) {
	r := loader.New()
	factories := instances.NewRegistry()
	if err := Compose(r, factories); err != nil {
		t.Fatalf("compose: %v", err)
	}
	registered := make(map[string]bool, len(loader.Plugins(r)))
	for _, p := range loader.Plugins(r) {
		registered[p.ID] = true
	}
	if registered["cli"] {
		t.Error("the hostless half registered the CLI host")
	}
	for _, want := range []string{"assembly", "workflow-backup", "reporting"} {
		if !registered[want] {
			t.Errorf("plugin %s was not registered", want)
		}
	}
	if got := len(factories.Descriptors()); got == 0 {
		t.Error("no factory descriptor reached the factory registry")
	}
	if err := Compose(r, instances.NewRegistry()); err == nil {
		t.Fatal("a second composition was accepted")
	}
}

// TestComposeReportsADuplicatePlugin proves a plugin already registered under a
// compiled id stops the build of the hostless half.
func TestComposeReportsADuplicatePlugin(t *testing.T) {
	r := loader.New()
	if err := loader.Add(r, loader.Spec{ID: "assembly", Summary: "probe"}, nil); err != nil {
		t.Fatalf("probe registration: %v", err)
	}
	if err := Compose(r, instances.NewRegistry()); err == nil {
		t.Fatal("a duplicate plugin was accepted")
	}
}
