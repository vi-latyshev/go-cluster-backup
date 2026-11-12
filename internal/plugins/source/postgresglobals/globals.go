// Package postgresglobals produces one artifact of the PostgreSQL cluster's
// global objects: the roles, their settings, memberships and tablespaces that
// per-database pg_dump output does not carry. The artifact is the unmodified
// output of pg_dumpall --globals-only and nothing else: this package never
// rewrites a statement, and it deliberately implements no Restorer. A globals
// dump is arbitrary SQL from the source's superuser, including role password
// hashes and role settings that can hold secrets, so apply is the operator's
// reviewed procedure against an isolated matching-major target — never an
// automatic second parser with a silent error policy.
//
// Every artifact of this type is credential content regardless of
// role_passwords: --no-role-passwords drops the hashes, not the ALTER ROLE …
// SET settings, and the pipeline must seal the bytes before they leave the
// process.
package postgresglobals

import (
	"context"
	"fmt"
	"io"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/configuration"
	pluginerrors "github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/extcmd"
	pluginio "github.com/vi-latyshev/go-cluster-backup/internal/plugins/io"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/pgclient"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source"
)

// maintenanceDatabase is the database pg_dumpall connects to by default when
// it dumps globals: it reads the cluster's global catalogs, not one database's
// data, and a cluster always has this one.
const maintenanceDatabase = "postgres"

// Config is the plugin config. Every field is required of the merged config:
// the connection and client settings are defaulted by the distribution's
// defaults document where a value suits every deployment, and the plugin
// receives them complete.
type Config struct {
	Address         string               `json:"address" required:"true" doc:"host:port of the PostgreSQL server"`
	Username        string               `json:"username" required:"true" doc:"role pg_dumpall connects as; it must be able to read the cluster's roles and tablespaces"`
	Password        configuration.Secret `json:"password" doc:"file holding the role's password; never passed on a command line"`
	ClientDirs      []string             `json:"client_dirs" doc:"directories holding a PostgreSQL client suite — pg_dump, pg_dumpall, pg_restore and psql of one major; a directory that does not exist is skipped, and PATH is the last candidate"`
	RoleHashes      bool                 `json:"role_hashes" required:"true" doc:"true emits the roles' password hashes (pg_dumpall's default); false passes --no-role-passwords. Either way role settings can carry secrets, so the artifact is credential content"`
	ConnectAttempts int                  `json:"connect_attempts" doc:"TCP connect attempts before a dump gives up"`
	ConnectInterval model.Duration       `json:"connect_interval" doc:"wait between connect attempts; also the per-attempt dial timeout"`
}

type Source struct {
	cfg  Config
	conn *pgclient.Conn
}

// Describe reports the artifact's static facts: SQL text, credential content
// unconditionally — role settings hold secrets even with no password hash —
// and an origin that names no database and carries no credentials.
func (s *Source) Describe(ctx context.Context) (source.SourceTraits, error) {
	if err := ctx.Err(); err != nil {
		return source.SourceTraits{}, err
	}
	return source.SourceTraits{
		Ext:         ".sql",
		Origin:      "postgres://" + s.cfg.Address,
		Credentials: true,
	}, nil
}

// Check runs at preflight: the configured client suites are complete and of
// one major, the target is reachable, and a suite of the server's exact major
// exists — pg_dumpall of a different major is refused by the server.
func (s *Source) Check(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.conn.CheckBinaries(ctx); err != nil {
		return err
	}
	if err := extcmd.WaitTCP(ctx, s.conn.Host(), s.conn.Port(), s.cfg.ConnectAttempts, s.cfg.ConnectInterval.Std()); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return pluginerrors.Transient(err)
	}
	pass, err := s.conn.ReadPassword()
	if err != nil {
		return err
	}
	_, err = s.conn.Select(ctx, pass, maintenanceDatabase)
	return err
}

// CheckBinaries is the optional local health capability: the configured
// client suites are complete and internally consistent, proved without a
// server.
func (s *Source) CheckBinaries(ctx context.Context) error {
	return s.conn.CheckBinaries(ctx)
}

// Dump streams pg_dumpall --globals-only into dst: the output is the
// server's own, byte for byte, with no line read, rewritten or reordered. A
// non-zero exit is this call's error, never a short artifact.
func (s *Source) Dump(ctx context.Context, dst io.Writer) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	pass, err := s.conn.ReadPassword()
	if err != nil {
		return err
	}
	if err := extcmd.WaitTCP(ctx, s.conn.Host(), s.conn.Port(), s.cfg.ConnectAttempts, s.cfg.ConnectInterval.Std()); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return pluginerrors.Transient(err)
	}
	suite, err := s.conn.Select(ctx, pass, maintenanceDatabase)
	if err != nil {
		return err
	}
	args := []string{"--globals-only"}
	if !s.cfg.RoleHashes {
		args = append(args, "--no-role-passwords")
	}
	_, err = s.conn.Stream(ctx, s.conn.Command(ctx, suite, "pg_dumpall", pass, args...), dst)
	return err
}

// Probe proves the artifact is the whole output of pg_dumpall --globals-only
// by its own header and completion lines, streamed: the header and footer are
// checked as the bytes pass and nothing is buffered whole. The check is
// completeness only — it never claims the SQL is safe to execute or that a
// restore would succeed.
func (s *Source) Probe(ctx context.Context, src io.Reader) (source.ProbeReport, error) {
	if err := ctx.Err(); err != nil {
		return source.ProbeReport{}, err
	}
	p := &globalsProbe{}
	n, err := pluginio.CopyContext(ctx, p, src)
	if err != nil {
		return source.ProbeReport{}, err
	}
	if err := p.verify(); err != nil {
		return source.ProbeReport{}, pluginerrors.Permanent(fmt.Errorf("the globals artifact is not a complete pg_dumpall --globals-only output: %w", err))
	}
	return source.ProbeReport{Bytes: n, Detail: "the pg_dumpall globals header and completion lines are present"}, nil
}
