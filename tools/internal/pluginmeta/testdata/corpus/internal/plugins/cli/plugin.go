package cli

import (
	"example.test/app/internal/loader"
	"example.test/app/internal/plugins/instances"
)

// Spec is the compiled identity of the CLI host: Floor, because every process
// role is dispatched here.
var Spec = loader.Spec{
	ID:      "cli",
	Summary: "the command host",
	Floor:   true,
}

// Apply registers the host roles in the loader.
func Apply(lr *loader.Registry, factories *instances.Registry) error {
	return nil
}
