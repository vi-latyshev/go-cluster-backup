package keeplast

import (
	"context"
	"fmt"

	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/retention"
)

// Spec is the compiled plugin identity of this factory: kind and type decide
// it, never a literal of its own.
var Spec = loader.Spec{
	ID:      instances.ID(retention.Kind.Name(), "keep_last"),
	Summary: "keep the newest N runs",
}

// Apply contributes the keep_last policy factory and its descriptor to the
// compiled schema in one call.
func Apply(lr *loader.Registry, factories *instances.Registry) error {
	return instances.AddFactory(lr, factories, retention.Kind, instances.FactorySpec[Config, retention.Retention]{
		Type:      "keep_last",
		Summary:   Spec.Summary,
		Stability: instances.Stable,
		Example:   []byte(`{"count": 3, "min_keep": 1, "count_incomplete": true}`),
		New: func(ctx context.Context, cfg Config, _ instances.Deps) (retention.Retention, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if cfg.Count < 1 {
				return nil, fmt.Errorf("count must be at least 1, got %d", cfg.Count)
			}
			if cfg.MinKeep < 1 {
				return nil, fmt.Errorf("min_keep must be at least 1, got %d", cfg.MinKeep)
			}
			if cfg.MinKeep > cfg.Count {
				return nil, fmt.Errorf("min_keep %d must not exceed count %d", cfg.MinKeep, cfg.Count)
			}
			return &Policy{cfg: cfg}, nil
		},
	})
}
