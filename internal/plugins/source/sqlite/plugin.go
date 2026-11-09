package sqlite

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/sqlitecopy"
)

// Spec is the compiled plugin identity of this factory: kind and type decide
// it, never a literal of its own.
var Spec = loader.Spec{
	ID:      instances.ID(source.Kind.Name(), "sqlite"),
	Summary: "one SQLite database, copied with sqlite3's online .backup",
}

// binaries every capability of this plugin may invoke.
var binaries = []string{"sqlite3"}

// Apply contributes the sqlite source factory and its descriptor to the
// compiled schema in one call.
func Apply(lr *loader.Registry, factories *instances.Registry) error {
	return instances.AddFactory(lr, factories, source.Kind, instances.FactorySpec[Config, source.Source]{
		Type:      "sqlite",
		Summary:   Spec.Summary,
		Stability: instances.Stable,
		Binaries:  binaries,
		Example:   []byte(`{"path": "/data/app.db", "scratch_dir": "/scratch", "busy_timeout": "10s", "integrity_check": "quick"}`),
		New: func(ctx context.Context, cfg Config, _ instances.Deps) (source.Source, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if cfg.Path == "" {
				return nil, errors.New("path is empty")
			}
			if !filepath.IsAbs(cfg.Path) {
				return nil, fmt.Errorf("path %q must be absolute", cfg.Path)
			}
			if cfg.ScratchDir == "" {
				return nil, errors.New("scratch_dir is empty")
			}
			if !filepath.IsAbs(cfg.ScratchDir) {
				return nil, fmt.Errorf("scratch_dir %q must be absolute", cfg.ScratchDir)
			}
			if cfg.BusyTimeout.Std() <= 0 {
				return nil, errors.New("busy_timeout must be positive")
			}
			if _, ok := sqlitecopy.Pragma(cfg.IntegrityCheck); !ok {
				return nil, fmt.Errorf("integrity_check %q must be quick or full", cfg.IntegrityCheck)
			}
			return &Source{cfg: cfg}, nil
		},
	})
}
