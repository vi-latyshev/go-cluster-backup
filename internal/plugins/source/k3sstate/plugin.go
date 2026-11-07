package k3sstate

import (
	"context"

	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source"
)

// Spec is the compiled plugin identity of this factory: kind and type decide
// it, never a literal of its own.
var Spec = loader.Spec{
	ID:      instances.ID(source.Kind.Name(), "k3s_state"),
	Summary: "k3s sqlite datastore + server token as one tar stream, with a configurable integrity check",
}

// Apply contributes the k3s_state source factory and its descriptor to the
// compiled schema in one call.
func Apply(lr *loader.Registry, factories *instances.Registry) error {
	return instances.AddFactory(lr, factories, source.Kind, instances.FactorySpec[Config, source.Source]{
		Type:      "k3s_state",
		Summary:   Spec.Summary,
		Stability: instances.Stable,
		Binaries:  []string{"sqlite3"},
		Example: []byte(`{"datastore": "/k3s-db/state.db", "token": {"file": "/k3s-token"}, ` +
			`"scratch_dir": "/plain", "busy_timeout": "10s", "integrity_check": "quick", "timeout": "5m"}`),
		New: func(ctx context.Context, cfg Config, _ instances.Deps) (source.Source, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			// cfg is already validated: Decode runs Config.Validate when its own
			// walk is clean, and this constructor is only reached through it.
			// No filesystem access here: the conformance kit constructs every
			// plugin from its example, and the mounts exist only in the init
			// container. Check verifies them at preflight; Dump fails loudly.
			return &Source{cfg: cfg, sqlite3: "sqlite3"}, nil
		},
	})
}
