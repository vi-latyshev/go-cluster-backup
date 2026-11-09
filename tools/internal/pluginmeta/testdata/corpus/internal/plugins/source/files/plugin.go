package files

import (
	"example.test/app/internal/loader"
	"example.test/app/internal/plugins/instances"
	"example.test/app/internal/plugins/source"
)

// Spec is the compiled plugin identity of the local-files source.
var Spec = loader.Spec{
	ID:      instances.ID(source.Kind.Name(), "files"),
	Summary: "copies local files",
}

// Apply contributes the local-files factory.
func Apply(lr *loader.Registry, factories *instances.Registry) error {
	return nil
}
