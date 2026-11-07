package reporting

import (
	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
)

// Spec is the compiled plugin identity of this application provider. It
// contributes no factory, so the name is its own literal: settings live under
// `plugins.config.reporting` and nothing keys an instance off it.
var Spec = loader.Spec{
	ID:      "reporting",
	Summary: "the previous-run record: what the storages hold of earlier runs and why the previous Job failed",
}

// Apply publishes this provider's settings type under its stable ID, so the
// compiled schema walks `plugins.config.reporting` the way it walks every
// factory block.
func Apply(_ *loader.Registry, factories *instances.Registry) error {
	return instances.RegisterProvider[Config](factories, Spec.ID)
}
