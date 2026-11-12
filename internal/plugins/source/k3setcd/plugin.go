package k3setcd

import (
	"context"

	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source"
)

// Spec is the compiled plugin identity of this factory: kind and type decide
// it, never a literal of its own.
var Spec = loader.Spec{
	ID:      instances.ID(source.Kind.Name(), "k3s_etcd"),
	Summary: "k3s embedded etcd snapshot + server token as one checksum-verified tar stream",
}

// Apply contributes the k3s_etcd source factory and its descriptor to the
// compiled schema in one call.
func Apply(lr *loader.Registry, factories *instances.Registry) error {
	return instances.AddFactory(lr, factories, source.Kind, instances.FactorySpec[Config, source.Source]{
		Type:      "k3s_etcd",
		Summary:   Spec.Summary,
		Stability: instances.Stable,
		Example: []byte(`{"snapshot_dir": "/var/lib/rancher/k3s/server/db/snapshots", ` +
			`"token": {"file": "/var/lib/rancher/k3s/server/token"}, "max_age": "26h"}`),
		New: func(ctx context.Context, cfg Config, _ instances.Deps) (source.Source, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			// cfg is already validated: Decode runs Config.Validate when its own
			// walk is clean. No filesystem access here: the conformance kit
			// constructs every plugin from its example, and the mounts exist only
			// in the init container. Check verifies them at preflight.
			return &Source{cfg: cfg}, nil
		},
	})
}
