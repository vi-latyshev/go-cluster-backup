// Package postgres dumps one PostgreSQL database with pg_dump, the binary the
// image already carries (postgresql18-client). The dump streams out of
// pg_dump's stdout; the call returns after pg_dump has exited, and a non-zero
// exit is an error of this call, never a short dump reported as success.
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
	"net"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	json "encoding/json/v2"

	"github.com/vi-latyshev/go-cluster-backup/internal/extcmd"
	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/must"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/source"
)

// Config is the plugin config. Every field is required: no value in code can
// stand in for what the file omits.
type Config struct {
	Address         string         `json:"address" required:"true" doc:"host:port of the PostgreSQL server"`
	Database        string         `json:"database" required:"true" doc:"database to dump; with all_databases true, the database the enumeration connects to"`
	Username        string         `json:"username" required:"true" doc:"role pg_dump and psql connect as"`
	Password        plugin.Secret  `json:"password" doc:"file holding the role's password; never passed on a command line"`
	Format          string         `json:"format" doc:"pg_dump output format: custom, tar or plain"`
	Compression     string         `json:"compression" doc:"pg_dump --compress argument, e.g. none or zstd:3"`
	ConnectAttempts int            `json:"connect_attempts" doc:"TCP connect attempts before a dump gives up"`
	ConnectInterval model.Duration `json:"connect_interval" doc:"wait between connect attempts; also the per-attempt dial timeout"`
	// AllDatabases makes this instance enumerate the server's databases instead
	// of dumping one: the engine builds one source per database, and each of
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

// binaries every capability of this plugin may invoke.
var binaries = []string{"pg_dump", "psql", "pg_restore"}

var (
	formatExt  = map[string]string{"custom": ".dump", "tar": ".tar", "plain": ".sql"}
	formatFlag = map[string]string{"custom": "c", "tar": "t", "plain": "p"}
	// pg_dump --compress: none, a bare gzip level, or METHOD[:LEVEL].
	compressionRe = regexp.MustCompile(`^(none|[0-9]|gzip(:[0-9]{1,2})?|lz4(:[0-9]{1,2})?|zstd(:[0-9]{1,2})?)$`)
)

// Register adds the postgres source to a registry.
func Register(r *plugin.Registry) error {
	return plugin.Register(r, source.Kind, plugin.Spec[Config, source.Source]{
		Type:      "postgres",
		Summary:   "one PostgreSQL database, dumped with pg_dump",
		Stability: plugin.Stable,
		Requires:  binaries,
		Example:   []byte(`{"address": "postgres.example.svc.cluster.local:5432", "database": "app", "username": "app", "password": {"file": "/run/secrets/postgres/password"}, "format": "custom", "compression": "none", "connect_attempts": 6, "connect_interval": "5s", "all_databases": true, "exclude_databases": ["postgres"]}`),
		New: func(ctx context.Context, cfg Config, _ plugin.Deps) (source.Source, error) {
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
			if !compressionRe.MatchString(cfg.Compression) {
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

type Source struct {
	cfg  Config
	host string
	port string
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

// Check runs at preflight: the binaries are present, the target is reachable,
// and pg_dump's major version equals the server's — a dump from a client of the
// wrong major is refused by the server, and it must be refused here, not at 04:20.
func (s *Source) Check(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var errs []error
	for _, bin := range binaries {
		if _, err := extcmd.FindBinary(bin); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	if err := extcmd.WaitTCP(ctx, s.host, s.port, s.cfg.ConnectAttempts, s.cfg.ConnectInterval.Std()); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return plugin.Transient(err)
	}
	pass, err := extcmd.ReadPassword(s.cfg.Password.File)
	if err != nil {
		return err
	}
	client, err := s.pgDumpMajor(ctx)
	if err != nil {
		return err
	}
	server, err := s.serverMajor(ctx, pass)
	if err != nil {
		return err
	}
	if client != server {
		return plugin.Permanent(fmt.Errorf("pg_dump major %d does not equal server major %d: the image's postgresql client must match the server", client, server))
	}
	return nil
}

// Dump streams the dump into dst. The bytes never pass through a buffer: read
// errors from dst stop pg_dump rather than being swallowed.
func (s *Source) Dump(ctx context.Context, dst io.Writer) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	pass, err := extcmd.ReadPassword(s.cfg.Password.File)
	if err != nil {
		return err
	}
	if err := extcmd.WaitTCP(ctx, s.host, s.port, s.cfg.ConnectAttempts, s.cfg.ConnectInterval.Std()); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return plugin.Transient(err)
	}
	cmd := exec.CommandContext(ctx, "pg_dump", s.dumpArgs()...)
	cmd.Env = s.pgEnv(pass)
	_, err = streamCommand(ctx, cmd, dst)
	return err
}

func (s *Source) dumpArgs() []string {
	return append(s.connArgs(),
		"--no-password",
		"-F"+formatFlag[s.cfg.Format],
		"--compress="+s.cfg.Compression,
		s.cfg.Database,
	)
}

// connArgs is the shared connection prefix; the password is never here.
func (s *Source) connArgs() []string {
	return []string{"-h", s.host, "-p", s.port, "-U", s.cfg.Username}
}

func (s *Source) pgEnv(pass string) []string {
	return extcmd.ReplaceEnv(os.Environ(),
		"PGPASSWORD="+pass,
		"PGCONNECT_TIMEOUT="+strconv.Itoa(connectTimeoutSeconds(s.cfg.ConnectInterval.Std())),
	)
}

// pgDumpMajor reads the client's major version from pg_dump --version.
func (s *Source) pgDumpMajor(ctx context.Context) (int, error) {
	out, err := extcmd.Output(ctx, exec.CommandContext(ctx, "pg_dump", "--version"), classifyExit)
	if err != nil {
		return 0, err
	}
	m := versionRe.FindStringSubmatch(out)
	if m == nil {
		return 0, plugin.Permanent(fmt.Errorf("cannot read a version from pg_dump --version output %q", strings.TrimSpace(out)))
	}
	major, err := strconv.Atoi(m[1])
	if err != nil {
		return 0, plugin.Permanent(fmt.Errorf("pg_dump version %q is not a number", m[1]))
	}
	return major, nil
}

// serverMajor asks the server itself; server_version_num packs the major into
// its leading digits (180006 is 18.6).
func (s *Source) serverMajor(ctx context.Context, pass string) (int, error) {
	out, err := s.psqlQuery(ctx, s.cfg.Database, pass, "select current_setting('server_version_num')")
	if err != nil {
		return 0, err
	}
	num, err := strconv.Atoi(strings.TrimSpace(out))
	if err != nil {
		return 0, plugin.Permanent(fmt.Errorf("server version %q is not a number: %w", strings.TrimSpace(out), err))
	}
	return num / 10000, nil
}

// psqlQuery runs one -At query as this instance's role and returns what it
// answered. The password reaches psql through the environment, never a command
// line, and the caller has already read it — a unit enumeration and a version
// check both ask the server, and both classify a refusal the same way.
func (s *Source) psqlQuery(ctx context.Context, database, pass, query string) (string, error) {
	cmd := exec.CommandContext(ctx, "psql", append(s.connArgs(),
		"--no-password", "-d", database, "-At",
		"-c", query,
	)...)
	cmd.Env = s.pgEnv(pass)
	return extcmd.Output(ctx, cmd, classifyExit)
}

// Units enumerates the server's databases as one artifact each: with
// all_databases false the instance dumps exactly one database and answers
// errors.ErrUnsupported — the engine's "this instance is one artifact". A
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
	pass, err := extcmd.ReadPassword(s.cfg.Password.File)
	if err != nil {
		return nil, err
	}
	// A server that is not answering yet is the same transient fault a dump
	// waits out; enumerating is the first thing the run does with it.
	if err := extcmd.WaitTCP(ctx, s.host, s.port, s.cfg.ConnectAttempts, s.cfg.ConnectInterval.Std()); err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, plugin.Transient(err)
	}
	out, err := s.psqlQuery(ctx, s.cfg.Database, pass, unitsQuery)
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
		// engine cannot build an artifact name from is that database's own
		// failed artifact, and every other database of the instance is dumped.
		// A Config is plain JSON-encodable data — strings, numbers, a duration
		// and a secret reference — so the encode cannot fail.
		raw := must.Do(json.Marshal(unitConfig(s.cfg, name)))
		units = append(units, source.Unit{Name: name, Config: raw})
	}
	if len(units) == 0 {
		return nil, plugin.Permanent(fmt.Errorf("server %s holds no database to dump (%d excluded; templates and databases that refuse connections are not dumped)",
			s.cfg.Address, len(s.cfg.ExcludeDatabases)))
	}
	return units, nil
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

var versionRe = regexp.MustCompile(`(\d+)\.`)

func connectTimeoutSeconds(d time.Duration) int {
	return max(int((d+time.Second-1)/time.Second), 1)
}

// stdoutPipe is the pipe a command's output streams through. It is a variable
// because os.Pipe can fail on a host out of descriptors, which no test can make
// it do on demand.
var stdoutPipe = func(cmd *exec.Cmd) (io.ReadCloser, error) { return cmd.StdoutPipe() }

// streamCommand starts cmd, copies its stdout into dst, and returns only after
// the command has exited. A dst failure kills the command; a non-zero exit is
// classified from the command's stderr.
func streamCommand(ctx context.Context, cmd *exec.Cmd, dst io.Writer) (int64, error) {
	stdout, err := stdoutPipe(cmd)
	if err != nil {
		// A host out of descriptors is worth another attempt, never a panic.
		return 0, plugin.Transient(fmt.Errorf("taking the command's stdout: %w", err))
	}
	stderr := extcmd.NewBoundedBuffer()
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return 0, ctxErr
		}
		return 0, plugin.Transient(err)
	}
	n, copyErr := plugin.CopyContext(ctx, dst, stdout)
	if copyErr != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		if ctxErr := ctx.Err(); ctxErr != nil {
			return n, ctxErr
		}
		return n, copyErr
	}
	if err := cmd.Wait(); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return n, ctxErr
		}
		return n, classifyExit(cmd.Path, err, stderr.String())
	}
	return n, nil
}

// runCommand runs cmd with src as its stdin (nil leaves stdin empty) and
// returns the bytes fed to it.
func runCommand(ctx context.Context, cmd *exec.Cmd, src io.Reader) (int64, error) {
	stderr := extcmd.NewBoundedBuffer()
	cmd.Stderr = stderr
	counted := &countingReader{r: src}
	if src != nil {
		cmd.Stdin = counted
	}
	if err := cmd.Run(); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return counted.n, ctxErr
		}
		return counted.n, classifyExit(cmd.Path, err, stderr.String())
	}
	return counted.n, nil
}

var (
	// A connection the server never answered is worth another attempt; a
	// credential the server refused is not.
	transientRe = regexp.MustCompile(`(?i)connection refused|connection timed out|could not connect|connection reset|no route to host|i/o timeout|the database system is starting up|is in recovery|terminating connection due to|server closed the connection unexpectedly`)
	permanentRe = regexp.MustCompile(`(?i)password authentication failed|authentication failed|no pg_hba.conf entry|does not exist|permission denied|too many connections|server version mismatch`)
)

// classifyExit turns a failed command into a framework-marked error with this
// plugin's patterns; text that matches neither stays unclassified, and the
// engine's config then decides what an unclassified error means.
var classifyExit = extcmd.Classifier(permanentRe, transientRe)

type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}
