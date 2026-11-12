package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/pgclient"
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
		// No Binaries: the programs are a configured suite, not PATH lookups, so
		// doctor and preflight prove them through this instance's CheckBinaries
		// capability instead of a metadata list a valid client_dirs deployment
		// would fail.
		Example: []byte(`{"address": "postgres.example.svc.cluster.local:5432", "database": "app", "username": "app", "password": {"file": "/run/secrets/postgres/password"}, "client_dirs": ["/usr/libexec/postgresql18", "/usr/libexec/postgresql17", "/usr/libexec/postgresql16", "/usr/libexec/postgresql15"], "format": "custom", "compression": "none", "connect_attempts": 6, "connect_interval": "5s", "all_databases": true, "exclude_databases": ["postgres"]}`),
		New: func(ctx context.Context, cfg Config, _ instances.Deps) (source.Source, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if cfg.Database == "" {
				return nil, errors.New("database is empty")
			}
			if _, ok := formatFlag[cfg.Format]; !ok {
				return nil, fmt.Errorf("format %q must be custom, tar or plain", cfg.Format)
			}
			if !compressionRe().MatchString(cfg.Compression) {
				return nil, fmt.Errorf("compression %q is not a pg_dump --compress value", cfg.Compression)
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
			conn, err := pgclient.New(pgclient.ConnConfig{
				Address:         cfg.Address,
				Username:        cfg.Username,
				Password:        cfg.Password,
				ClientDirs:      cfg.ClientDirs,
				ConnectAttempts: cfg.ConnectAttempts,
				ConnectInterval: cfg.ConnectInterval,
			})
			if err != nil {
				return nil, err
			}
			return &Source{cfg: cfg, conn: conn}, nil
		},
	})
}
