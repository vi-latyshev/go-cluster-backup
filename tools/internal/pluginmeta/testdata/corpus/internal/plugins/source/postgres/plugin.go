package postgres

import (
	"example.test/app/internal/loader"
	"example.test/app/internal/plugins/instances"
	"example.test/app/internal/plugins/model"
	"example.test/app/internal/plugins/source"
)

// Probe is referenced from a platform-specific file of the files plugin.
const Probe = "postgres"

// Spec is the compiled plugin identity of the PostgreSQL source.
var Spec = loader.Spec{
	ID:       instances.ID(source.Kind.Name(), "postgres"),
	Summary:  "dumps a PostgreSQL database",
	Requires: []string{"configuration"},
	After:    []string{"discovery/kubernetes_labels"},
}

// Apply contributes the PostgreSQL factory.
func Apply(lr *loader.Registry, factories *instances.Registry) error {
	_ = model.Spec
	return nil
}
