package files

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"

	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source"
)

// Spec is the compiled plugin identity of this factory: kind and type decide
// it, never a literal of its own.
var Spec = loader.Spec{
	ID:      instances.ID(source.Kind.Name(), "files"),
	Summary: "one directory tree as a single tar stream",
}

// Apply contributes the files source factory and its descriptor to the
// compiled schema in one call.
func Apply(lr *loader.Registry, factories *instances.Registry) error {
	return instances.AddFactory(lr, factories, source.Kind, instances.FactorySpec[Config, source.Source]{
		Type:      "files",
		Summary:   Spec.Summary,
		Stability: instances.Stable,
		Example:   []byte(`{"root": "/volumes/example/name", "ignore_vanished": true}`),
		New: func(ctx context.Context, cfg Config, deps instances.Deps) (source.Source, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if cfg.Root == "" {
				return nil, errors.New("root is empty")
			}
			if !filepath.IsAbs(cfg.Root) {
				return nil, fmt.Errorf("root %q must be absolute", cfg.Root)
			}
			log := deps.Log
			if log == nil {
				log = slog.New(slog.DiscardHandler)
			}
			return &Source{cfg: cfg, log: log}, nil
		},
	})
}
