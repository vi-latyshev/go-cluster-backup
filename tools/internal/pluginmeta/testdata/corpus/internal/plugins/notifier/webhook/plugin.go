package webhook

import (
	"example.test/app/internal/loader"
	"example.test/app/internal/plugins/instances"
	"example.test/app/internal/plugins/model"
	"example.test/app/internal/plugins/notifier"
)

// Spec is the compiled plugin identity of the webhook notifier.
var Spec = loader.Spec{
	ID:       instances.ID(notifier.Kind.Name(), "webhook"),
	Summary:  "posts an event to an HTTP endpoint",
	Floor:    true,
	Requires: []string{"model"},
}

// Apply contributes the webhook factory.
func Apply(lr *loader.Registry, factories *instances.Registry) error {
	_ = model.Spec
	return nil
}
