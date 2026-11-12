// Package pgclient is the shared PostgreSQL client layer of the sources that
// shell out to pg_dump, pg_dumpall, pg_restore and psql. One Conn owns the flat
// connection settings of such a source — address, role, mounted password,
// connect attempts and interval — and the client-suite directories the
// deployment names; every command a source runs is built here, so the
// password reaches the process only through PGPASSWORD, the connection prefix
// is spelled once, and an exit is classified by the same two patterns.
//
// The client suite is selected by the server's own major: binaries of the
// wrong major are refused by the server, so a dump, an enumeration, an apply
// or a probe resolves the suite of the exact major first. The one exception is
// the read-only server_version_num query, which may run through any available
// psql before that selection — asking which suite to use must not need the
// suite already.
//
// The package is internal: it is the module's own library, not part of the
// plugin API a third-party plugin is written against.
package pgclient

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/configuration"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/extcmd"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
)

// ConnConfig is the connection and client-suite half of a PostgreSQL source's
// config: flat fields the source type spells in its own config, because the
// strict decoder and the defaults document read the source's type, never this
// one. The source builds it once in its constructor.
type ConnConfig struct {
	Address         string
	Username        string
	Password        configuration.Secret
	ClientDirs      []string
	ConnectAttempts int
	ConnectInterval model.Duration
}

// Conn is one source's connection settings and the cache of its local client
// suites. The cache is per instance and never process-global: two instances of
// one process may resolve different directories, and a test can build a fresh
// one.
type Conn struct {
	cfg  ConnConfig
	host string
	port string

	mu    sync.Mutex
	cache []Suite
}

// New validates the shared settings and splits the address. It reports every
// malformed setting at once: the constructor of a source owns no defaults, so
// what it receives must already be complete.
func New(cfg ConnConfig) (*Conn, error) {
	var errs []error
	var host, port string
	if h, p, err := net.SplitHostPort(cfg.Address); err != nil {
		errs = append(errs, fmt.Errorf("address %q must be host:port: %w", cfg.Address, err))
	} else if _, err := strconv.ParseUint(p, 10, 16); err != nil {
		errs = append(errs, fmt.Errorf("address %q: port %q is not a number", cfg.Address, p))
	} else {
		host, port = h, p
	}
	if cfg.Username == "" {
		errs = append(errs, errors.New("username is empty"))
	}
	if cfg.Password.File == "" {
		errs = append(errs, errors.New("password/file is empty"))
	}
	if len(cfg.ClientDirs) == 0 {
		errs = append(errs, errors.New("client_dirs is empty: name at least one directory holding a PostgreSQL client suite"))
	}
	for i, dir := range cfg.ClientDirs {
		switch {
		case dir == "":
			errs = append(errs, fmt.Errorf("client_dirs/%d is empty", i))
		case !filepath.IsAbs(dir):
			errs = append(errs, fmt.Errorf("client_dirs/%d %q must be an absolute path", i, dir))
		}
	}
	if cfg.ConnectAttempts < 1 {
		errs = append(errs, fmt.Errorf("connect_attempts must be at least 1, got %d", cfg.ConnectAttempts))
	}
	if cfg.ConnectInterval.Std() <= 0 {
		errs = append(errs, errors.New("connect_interval must be positive"))
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	return &Conn{cfg: cfg, host: host, port: port}, nil
}

// Host and Port are the address the connection settings name, for the TCP wait
// a source runs before its first command.
func (c *Conn) Host() string { return c.host }

func (c *Conn) Port() string { return c.port }

// ReadPassword reads the mounted password at use time, never at construction:
// a missing file is a preflight failure, not a reason the whole config cannot
// even be built. A trailing newline (docker secrets style) is not part of it.
func (c *Conn) ReadPassword() (string, error) {
	return extcmd.ReadPassword(c.cfg.Password.File)
}

// ConnArgs is the connection prefix every command of the suite shares; the
// password is never here.
func (c *Conn) ConnArgs() []string {
	return []string{"-h", c.host, "-p", c.port, "-U", c.cfg.Username}
}

// Env is the process environment one command runs with: the configured
// password replaces whatever the process inherited, and PGCONNECT_TIMEOUT is
// derived from connect_interval so a hanging dial is bounded by the config.
func (c *Conn) Env(pass string) []string {
	return extcmd.ReplaceEnv(os.Environ(),
		"PGPASSWORD="+pass,
		"PGCONNECT_TIMEOUT="+strconv.Itoa(connectTimeoutSeconds(c.cfg.ConnectInterval.Std())),
	)
}

func connectTimeoutSeconds(d time.Duration) int {
	return max(int((d+time.Second-1)/time.Second), 1)
}
