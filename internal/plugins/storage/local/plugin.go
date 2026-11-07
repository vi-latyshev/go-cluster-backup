package local

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage"
)

// Spec is the compiled plugin identity of this factory: kind and type decide
// it, never a literal of its own.
var Spec = loader.Spec{
	ID:      instances.ID(storage.Kind.Name(), "local"),
	Summary: "a local directory",
}

// Apply contributes the local storage factory and its descriptor to the
// compiled schema in one call.
func Apply(lr *loader.Registry, factories *instances.Registry) error {
	return storage.AddFactory(lr, factories, storage.FactorySpec[Config]{
		Type:      "local",
		Summary:   Spec.Summary,
		Stability: instances.Stable,
		Example:   []byte(`{"root": "/backup/exchange", "page_size": 1000}`),
		New: func(ctx context.Context, cfg Config, _ instances.Deps, _ storage.BuildInput) (storage.Storage, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if cfg.Root == "" {
				return nil, errors.New("root is empty")
			}
			if cfg.PageSize < 1 {
				return nil, fmt.Errorf("page_size must be at least 1, got %d", cfg.PageSize)
			}
			if !filepath.IsAbs(cfg.Root) {
				return nil, fmt.Errorf("root %q must be absolute", cfg.Root)
			}
			return &Storage{root: cfg.Root, page: cfg.PageSize}, nil
		},
	})
}
