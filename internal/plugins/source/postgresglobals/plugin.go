package postgresglobals

import (
	"context"

	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/pgclient"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source"
)

// Spec is the compiled plugin identity of this factory: kind and type decide
// it, never a literal of its own.
var Spec = loader.Spec{
	ID:      instances.ID(source.Kind.Name(), "postgres_globals"),
	Summary: "the PostgreSQL cluster's roles, memberships, settings and tablespaces, dumped raw with pg_dumpall --globals-only",
}

// Apply contributes the postgres_globals source factory and its descriptor to
// the compiled schema in one call.
func Apply(lr *loader.Registry, factories *instances.Registry) error {
	return instances.AddFactory(lr, factories, source.Kind, instances.FactorySpec[Config, source.Source]{
		Type:      "postgres_globals",
		Summary:   Spec.Summary,
		Stability: instances.Stable,
		// No Binaries: the programs are a configured suite, not PATH lookups, so
		// doctor and preflight prove them through this instance's CheckBinaries
		// capability instead of a metadata list a valid client_dirs deployment
		// would fail.
		Example: []byte(`{"address": "postgres.example.svc.cluster.local:5432", "username": "app", "password": {"file": "/run/secrets/postgres/password"}, "client_dirs": ["/usr/libexec/postgresql18", "/usr/libexec/postgresql17", "/usr/libexec/postgresql16", "/usr/libexec/postgresql15"], "role_hashes": false, "connect_attempts": 6, "connect_interval": "5s"}`),
		New: func(ctx context.Context, cfg Config, _ instances.Deps) (source.Source, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
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
