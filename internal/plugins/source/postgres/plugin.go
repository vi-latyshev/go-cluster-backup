package postgres

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"

	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source"
)

// Spec is the compiled plugin identity of this factory: kind and type decide
// it, never a literal of its own.
var Spec = loader.Spec{
	ID:      instances.ID(source.Kind.Name(), "postgres"),
	Summary: "one PostgreSQL database, dumped with pg_dump",
}

// Apply contributes the postgres source factory and its descriptor to the
// compiled schema in one call.
func Apply(lr *loader.Registry, factories *instances.Registry) error {
	return instances.AddFactory(lr, factories, source.Kind, instances.FactorySpec[Config, source.Source]{
		Type:      "postgres",
		Summary:   Spec.Summary,
		Stability: instances.Stable,
		Binaries:  binaries,
		Example:   []byte(`{"address": "postgres.example.svc.cluster.local:5432", "database": "app", "username": "app", "password": {"file": "/run/secrets/postgres/password"}, "format": "custom", "compression": "none", "connect_attempts": 6, "connect_interval": "5s", "all_databases": true, "exclude_databases": ["postgres"]}`),
		New: func(ctx context.Context, cfg Config, _ instances.Deps) (source.Source, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			host, port, err := net.SplitHostPort(cfg.Address)
			if err != nil {
				return nil, fmt.Errorf("address %q must be host:port: %w", cfg.Address, err)
			}
			if _, err := strconv.ParseUint(port, 10, 16); err != nil {
				return nil, fmt.Errorf("address %q: port %q is not a number", cfg.Address, port)
			}
			if cfg.Database == "" {
				return nil, errors.New("database is empty")
			}
			if cfg.Username == "" {
				return nil, errors.New("username is empty")
			}
			if cfg.Password.File == "" {
				return nil, errors.New("password/file is empty")
			}
			if _, ok := formatFlag[cfg.Format]; !ok {
				return nil, fmt.Errorf("format %q must be custom, tar or plain", cfg.Format)
			}
			if !compressionRe().MatchString(cfg.Compression) {
				return nil, fmt.Errorf("compression %q is not a pg_dump --compress value", cfg.Compression)
			}
			if cfg.ConnectAttempts < 1 {
				return nil, fmt.Errorf("connect_attempts must be at least 1, got %d", cfg.ConnectAttempts)
			}
			if cfg.ConnectInterval.Std() <= 0 {
				return nil, errors.New("connect_interval must be positive")
			}
			// The instance dumps either one database or all of them: a list of
			// exclusions beside a single-database instance is a config that
			// asks for something it cannot do.
			for i, name := range cfg.ExcludeDatabases {
				if name == "" {
					return nil, fmt.Errorf("exclude_databases/%d is empty", i)
				}
			}
			if len(cfg.ExcludeDatabases) > 0 && !cfg.AllDatabases {
				return nil, errors.New("exclude_databases is set but all_databases is false: the instance dumps one database, so nothing is enumerated to exclude")
			}
			return &Source{cfg: cfg, host: host, port: port}, nil
		},
	})
}
