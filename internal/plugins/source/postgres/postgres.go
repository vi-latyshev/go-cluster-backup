// Package postgres dumps one PostgreSQL database with pg_dump. The dump
// streams out of pg_dump's stdout; the call returns after pg_dump has exited,
// and a non-zero exit is an error of this call, never a short dump reported as
// success. The client suite is selected by the server's exact major: the image
// carries PostgreSQL 15 through 18, and a server is dumped only by the client
// of its own major.
//
// The instance is also the restorer and the prober of such an artifact:
// side-by-side restores into a database labelled with the run id, in-place
// restores in one transaction, and a probe that reads the whole archive without
// a server.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"sync"

	json "encoding/json/v2"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/configuration"
	pluginerrors "github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/extcmd"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/must"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/pgclient"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source"
)

// Config is the plugin config. Every field is required of the merged config:
// no value in code stands in for what the file or the distribution's defaults
// document omits. client_dirs and the connect settings are defaulted, so a
// deployment may leave them out, but the plugin receives them complete.
type Config struct {
	Address         string               `json:"address" required:"true" doc:"host:port of the PostgreSQL server"`
	Database        string               `json:"database" required:"true" doc:"database to dump; with all_databases true, the database the enumeration connects to"`
	Username        string               `json:"username" required:"true" doc:"role pg_dump and psql connect as"`
	Password        configuration.Secret `json:"password" doc:"file holding the role's password; never passed on a command line"`
	ClientDirs      []string             `json:"client_dirs" doc:"directories holding a PostgreSQL client suite — pg_dump, pg_dumpall, pg_restore and psql of one major; a directory that does not exist is skipped, and PATH is the last candidate"`
	Format          string               `json:"format" doc:"pg_dump output format: custom, tar or plain"`
	Compression     string               `json:"compression" doc:"pg_dump --compress argument, e.g. none or zstd:3; a PostgreSQL 15 server, whose pg_dump takes only a numeric level, is mapped when the dump runs"`
	ConnectAttempts int                  `json:"connect_attempts" doc:"TCP connect attempts before a dump gives up"`
	ConnectInterval model.Duration       `json:"connect_interval" doc:"wait between connect attempts; also the per-attempt dial timeout"`
	// AllDatabases makes this instance enumerate the server's databases instead
	// of dumping one: the planning layer builds one source per database, and each of
	// those artifacts carries the database it holds.
	AllDatabases bool `json:"all_databases" required:"true" doc:"true when this instance enumerates every database of the server as its own artifact"`
	// ExcludeDatabases is exact names, not a pattern: a database listed here is
	// left out of the enumeration. Excluding the maintenance database is the
	// usual reason.
	ExcludeDatabases []string `json:"exclude_databases" doc:"databases the enumeration leaves out, by exact name; an empty list excludes none"`
}

// unitsQuery lists the databases a dump can connect to, the server's own order
// being by name. Templates are not databases and cannot be dumped; a database
// that refuses connections (datallowconn false) is one nobody can read.
const unitsQuery = "select datname from pg_database where datallowconn and not datistemplate order by datname"

var (
	formatExt  = map[string]string{"custom": ".dump", "tar": ".tar", "plain": ".sql"}
	formatFlag = map[string]string{"custom": "c", "tar": "t", "plain": "p"}
	// pg_dump --compress: none, a bare gzip level, or METHOD[:LEVEL].
	compressionRe = sync.OnceValue(func() *regexp.Regexp {
		return regexp.MustCompile(`^(none|[0-9]|gzip(:[0-9]{1,2})?|lz4(:[0-9]{1,2})?|zstd(:[0-9]{1,2})?)$`)
	})
)

type Source struct {
	cfg  Config
	conn *pgclient.Conn
}

func (s *Source) Describe(ctx context.Context) (source.SourceTraits, error) {
	if err := ctx.Err(); err != nil {
		return source.SourceTraits{}, err
	}
	return source.SourceTraits{
		Ext:    formatExt[s.cfg.Format],
		Origin: "postgres://" + s.cfg.Address + "/" + s.cfg.Database,
	}, nil
}

// Check runs at preflight: the configured client suites are complete and of
// one major, the target is reachable, and a suite of the server's exact major
// exists — a dump from a client of the wrong major is refused by the server,
// and it must be refused here, not at 04:20.
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
	_, err = s.conn.Select(ctx, pass, s.cfg.Database)
	return err
}

// CheckBinaries is the optional local health capability: the configured
// client suites are complete and internally consistent, proved without a
// server and without touching the deployment's files beyond the suites.
func (s *Source) CheckBinaries(ctx context.Context) error {
	return s.conn.CheckBinaries(ctx)
}

// Dump streams the dump into dst. The bytes never pass through a buffer: read
// errors from dst stop pg_dump rather than being swallowed.
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
	suite, err := s.conn.Select(ctx, pass, s.cfg.Database)
	if err != nil {
		return err
	}
	compress, err := compressionValue(suite.Major, s.cfg.Compression)
	if err != nil {
		return err
	}
	cmd := s.conn.Command(ctx, suite, "pg_dump", pass, s.dumpArgs(compress)...)
	_, err = s.conn.Stream(ctx, cmd, dst)
	return err
}

func (s *Source) dumpArgs(compress string) []string {
	return []string{
		"-F" + formatFlag[s.cfg.Format],
		"--compress=" + compress,
		pgclient.DBNameArg(s.cfg.Database),
	}
}

// compressionValue is the value pg_dump --compress takes on the selected
// client's major. PostgreSQL 16 introduced METHOD[:LEVEL]; PostgreSQL 15's
// pg_dump takes only a numeric level, so every configured value is mapped
// there — none is 0 and gzip:N is N — and a method it cannot express, or a
// gzip level it does not state, is refused by name instead of being handed to
// pg_dump as a value it reads as a number or rejects at 04:20.
func compressionValue(major int, compression string) (string, error) {
	if major >= 16 {
		return compression, nil
	}
	switch {
	case compression == "none":
		return "0", nil
	case len(compression) == 1 && compression[0] >= '0' && compression[0] <= '9':
		return compression, nil
	case strings.HasPrefix(compression, "gzip:"):
		level := strings.TrimPrefix(compression, "gzip:")
		if len(level) > 2 {
			break
		}
		n, err := strconv.Atoi(level)
		if err == nil && n >= 0 && n <= 9 {
			return level, nil
		}
	}
	return "", pluginerrors.Permanent(fmt.Errorf(
		"compression %q cannot be expressed to a PostgreSQL %d server's pg_dump: its --compress takes a numeric level 0-9, so use none, a level or gzip:LEVEL; the METHOD[:LEVEL] syntax needs a PostgreSQL 16 or newer client",
		compression, major))
}

// Units enumerates the server's databases as one artifact each: with
// all_databases false the instance dumps exactly one database and answers
// errors.ErrUnsupported — the planning layer's "this instance is one artifact". A
// unit's config is this instance's own config with the database substituted and
// the enumeration switched off — a unit that enumerated again would never end —
// and every unit is dumped by a source of this same type.
func (s *Source) Units(ctx context.Context) ([]source.Unit, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !s.cfg.AllDatabases {
		return nil, errors.ErrUnsupported
	}
	pass, err := s.conn.ReadPassword()
	if err != nil {
		return nil, err
	}
	// A server that is not answering yet is the same transient fault a dump
	// waits out; enumerating is the first thing the run does with it.
	if err := extcmd.WaitTCP(ctx, s.conn.Host(), s.conn.Port(), s.cfg.ConnectAttempts, s.cfg.ConnectInterval.Std()); err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, pluginerrors.Transient(err)
	}
	suite, err := s.conn.Select(ctx, pass, s.cfg.Database)
	if err != nil {
		return nil, err
	}
	out, err := s.query(ctx, suite, pass, s.cfg.Database, unitsQuery)
	if err != nil {
		return nil, err
	}
	excluded := make(map[string]bool, len(s.cfg.ExcludeDatabases))
	for _, name := range s.cfg.ExcludeDatabases {
		excluded[name] = true
	}
	var units []source.Unit
	for _, line := range strings.Split(out, "\n") {
		name := strings.TrimSpace(line)
		if name == "" || excluded[name] {
			continue
		}
		// A database name is the server's, not this plugin's to judge: one the
		// planning layer cannot build an artifact name from is that database's own
		// failed artifact, and every other database of the instance is dumped.
		// A Config is plain JSON-encodable data — strings, numbers, a duration
		// and a secret reference — so the encode cannot fail.
		raw := must.Do(json.Marshal(unitConfig(s.cfg, name)))
		units = append(units, source.Unit{Name: name, Config: raw})
	}
	if len(units) == 0 {
		return nil, pluginerrors.Permanent(fmt.Errorf("server %s holds no database to dump (%d excluded; templates and databases that refuse connections are not dumped)",
			s.cfg.Address, len(s.cfg.ExcludeDatabases)))
	}
	return units, nil
}

// query runs one -At query as this instance's role and returns what the
// server answered. The password reaches psql through the environment, never a
// command line.
func (s *Source) query(ctx context.Context, suite pgclient.Suite, pass, database, query string) (string, error) {
	cmd := s.conn.Command(ctx, suite, "psql", pass, pgclient.DBNameArg(database), "-At", "-c", query)
	return s.conn.Output(ctx, cmd)
}

// unitConfig is the config one unit is dumped with: the instance's own with the
// database substituted and the enumeration off. It exists so the round trip
// through JSON — a Secret field, a duration — is one function a test can pin.
func unitConfig(cfg Config, database string) Config {
	cfg.Database = database
	cfg.AllDatabases = false
	cfg.ExcludeDatabases = []string{}
	return cfg
}
