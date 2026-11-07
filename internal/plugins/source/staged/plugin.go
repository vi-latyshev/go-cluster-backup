package staged

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source"
)

// Spec is the compiled plugin identity of this factory: kind and type decide
// it, never a literal of its own.
var Spec = loader.Spec{
	ID:      instances.ID(source.Kind.Name(), "staged"),
	Summary: "a sealed artifact an earlier stage left in a directory",
}

// Apply contributes the staged source factory and its descriptor to the
// compiled schema in one call.
func Apply(lr *loader.Registry, factories *instances.Registry) error {
	return instances.AddFactory(lr, factories, source.Kind, instances.FactorySpec[Config, source.Source]{
		Type:      "staged",
		Summary:   Spec.Summary,
		Stability: instances.Stable,
		Example:   []byte(`{"dir": "/backup/staged", "name": "example-state"}`),
		New: func(ctx context.Context, cfg Config, _ instances.Deps) (source.Source, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if cfg.Dir == "" {
				return nil, errors.New("dir is empty")
			}
			if !filepath.IsAbs(cfg.Dir) {
				return nil, fmt.Errorf("dir %q must be absolute", cfg.Dir)
			}
			if cfg.Name == "" {
				return nil, errors.New("name is empty")
			}
			if strings.ContainsAny(cfg.Name, "/\x00") {
				return nil, fmt.Errorf("name %q must be a plain file name prefix", cfg.Name)
			}
			return &Source{cfg: cfg}, nil
		},
	})
}
