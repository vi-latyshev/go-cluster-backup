package gfs

import (
	"context"
	"fmt"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/retention"
)

// Spec is the compiled plugin identity of this factory: kind and type decide
// it, never a literal of its own.
var Spec = loader.Spec{
	ID:      instances.ID(retention.Kind.Name(), "gfs"),
	Summary: "keep the newest run per day, week and month",
}

// Apply contributes the gfs policy factory and its descriptor to the compiled
// schema in one call.
func Apply(lr *loader.Registry, factories *instances.Registry) error {
	return instances.AddFactory(lr, factories, retention.Kind, instances.FactorySpec[Config, retention.Retention]{
		Type:      "gfs",
		Summary:   Spec.Summary,
		Stability: instances.Stable,
		Example:   []byte(`{"daily": 30, "weekly": 12, "monthly": 12, "timezone": "UTC", "min_keep": 1, "count_incomplete": true}`),
		New: func(ctx context.Context, cfg Config, _ instances.Deps) (retention.Retention, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if cfg.Daily < 1 {
				return nil, fmt.Errorf("daily must be at least 1, got %d", cfg.Daily)
			}
			if cfg.Weekly < 0 {
				return nil, fmt.Errorf("weekly must not be negative, got %d", cfg.Weekly)
			}
			if cfg.Monthly < 0 {
				return nil, fmt.Errorf("monthly must not be negative, got %d", cfg.Monthly)
			}
			if cfg.MinKeep < 1 {
				return nil, fmt.Errorf("min_keep must be at least 1, got %d", cfg.MinKeep)
			}
			loc, err := time.LoadLocation(cfg.Timezone)
			if err != nil {
				return nil, err
			}
			return &Policy{cfg: cfg, loc: loc}, nil
		},
	})
}
