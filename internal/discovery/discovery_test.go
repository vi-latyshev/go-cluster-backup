package discovery_test

import (
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/discovery"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
)

// TestKindIsTheDiscoverySeam: Kind is the registry key a discovery registers
// under and the one a config file's `kind: discovery` instance is looked up
// with, so its name is pinned — a rename would leave every declared discovery
// unbuildable, and nothing at run time would say why.
func TestKindIsTheDiscoverySeam(t *testing.T) {
	want := plugin.NewKind[discovery.Discoverer]("discovery")
	if discovery.Kind != want {
		t.Fatal("discovery.Kind is no longer the \"discovery\" seam")
	}
}
