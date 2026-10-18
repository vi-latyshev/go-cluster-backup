// Package sqlite snapshots one SQLite database through the sqlite3 shell's
// online .backup command — the binary the image carries, since CGO is disabled
// in this module and no Go sqlite driver is available. The shell writes a file,
// so the snapshot goes through a temporary file and is streamed to dst from
// there; the temp file lives in the configured scratch_dir (never in the
// process temp dir, a 64 Mi emptyDir in the cluster) and is removed on every
// path out.
package sqlite

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/vi-latyshev/go-cluster-backup/internal/extcmd"
	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/source"
	"github.com/vi-latyshev/go-cluster-backup/internal/sqlitecopy"
)

// Config is the plugin config. Every field is required.
type Config struct {
	Path           string         `json:"path" required:"true" doc:"path of the SQLite database file"`
	ScratchDir     string         `json:"scratch_dir" required:"true" doc:"existing writable directory the snapshot's temporary file is spooled into, e.g. the big emptyDir a run mounts; the process temp dir is not used"`
	BusyTimeout    model.Duration `json:"busy_timeout" doc:"how long sqlite3 waits for a lock held by the live writer"`
	IntegrityCheck string         `json:"integrity_check" doc:"pragma a restored or probed copy must answer ok to: quick or full"`
}

// Register adds the sqlite source to a registry.
func Register(r *plugin.Registry) error {
	return plugin.Register(r, source.Kind, plugin.Spec[Config, source.Source]{
		Type:      "sqlite",
		Summary:   "one SQLite database, copied with sqlite3's online .backup",
		Stability: plugin.Stable,
		Requires:  []string{"sqlite3"},
		Example:   []byte(`{"path": "/data/app.db", "scratch_dir": "/scratch", "busy_timeout": "10s", "integrity_check": "quick"}`),
		New: func(ctx context.Context, cfg Config, _ plugin.Deps) (source.Source, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if cfg.Path == "" {
				return nil, errors.New("path is empty")
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

// The filesystem primitives the dump and the restore drive. Each is a variable
// so a test can make it fail: no filesystem fails them on demand, and a branch
// no test reaches is a branch nobody has run.
var (
	chmodFile  = os.Chmod
	lstatFile  = os.Lstat
	renameFile = os.Rename
	closeFile  = func(f *os.File) error { return f.Close() }
	statFile   = func(f *os.File) (os.FileInfo, error) { return f.Stat() }
)

// pragma is the integrity statement a restored or probed copy must answer ok to.
func (s *Source) pragma() string {
	pragma, _ := sqlitecopy.Pragma(s.cfg.IntegrityCheck)
	return pragma
}

type Source struct {
	cfg Config
}

func (s *Source) Describe(ctx context.Context) (source.SourceTraits, error) {
	if err := ctx.Err(); err != nil {
		return source.SourceTraits{}, err
	}
	return source.SourceTraits{Ext: ".db", Origin: "file://" + s.cfg.Path}, nil
}

// Check runs at preflight: sqlite3 is present, the scratch directory is
// writable, and the database file exists and is readable right now.
func (s *Source) Check(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := exec.LookPath("sqlite3"); err != nil {
		return plugin.Permanent(fmt.Errorf("sqlite3 is not in PATH: %w", err))
	}
	if err := extcmd.CheckScratchDir(s.cfg.ScratchDir); err != nil {
		return plugin.Permanent(err)
	}
	st, err := os.Stat(s.cfg.Path)
	if err != nil {
		return plugin.Permanent(fmt.Errorf("database file: %w", err))
	}
	if st.IsDir() {
		return plugin.Permanent(fmt.Errorf("%s is a directory, not a database file", s.cfg.Path))
	}
	f, err := os.Open(s.cfg.Path)
	if err != nil {
		return plugin.Permanent(fmt.Errorf("database file is not readable: %w", err))
	}
	return f.Close()
}

// Dump copies the database with .backup, which takes the correct lock against
// the process holding the file open, and streams the copy into dst.
func (s *Source) Dump(ctx context.Context, dst io.Writer) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(s.cfg.ScratchDir, "sqlite-backup-*")
	if err != nil {
		return plugin.Transient(err)
	}
	name := tmp.Name()
	if err := closeFile(tmp); err != nil {
		_ = os.Remove(name)
		return plugin.Transient(err)
	}
	defer func() { _ = os.Remove(name) }()

	if err := s.backup(ctx, name); err != nil {
		return err
	}
	f, err := os.Open(name)
	if err != nil {
		return plugin.Transient(err)
	}
	defer func() { _ = f.Close() }()
	st, err := statFile(f)
	if err != nil {
		return plugin.Transient(err)
	}
	if st.Size() == 0 {
		return plugin.Transient(errors.New("sqlite3 exited 0 but wrote an empty copy"))
	}
	_, err = plugin.CopyContext(ctx, dst, f)
	return err
}

// backup runs the shell's .backup into out.
func (s *Source) backup(ctx context.Context, out string) error {
	cmd := exec.CommandContext(ctx, "sqlite3", sqlitecopy.BackupArgs(s.cfg.Path, out, s.cfg.BusyTimeout.Std())...)
	if _, err := extcmd.Output(ctx, cmd, classifyExit); err != nil {
		return err
	}
	return nil
}

// check runs the configured pragma over file and requires the "ok" answer:
// sqlite3 exits 0 even when integrity_check reports corruption, so the output
// is the only proof.
func (s *Source) check(ctx context.Context, file string) error {
	cmd := exec.CommandContext(ctx, "sqlite3", "-readonly", file, s.pragma()+";")
	out, err := extcmd.Output(ctx, cmd, classifyExit)
	if err != nil {
		return err
	}
	if answer := strings.TrimSpace(out); answer != "ok" {
		return plugin.Permanent(fmt.Errorf("%s answered %q for %s", s.pragma(), answer, file))
	}
	return nil
}

var (
	transientRe = regexp.MustCompile(`(?i)database is locked|database is busy|unable to close due to unfinalized|i/o error|disk i/o error`)
	// A corrupt or missing database will not repair itself on the next attempt.
	permanentRe = regexp.MustCompile(`(?i)malformed|not a database|unable to open database file|file is encrypted|no such file`)
)

// classifyExit marks a failed sqlite3 run with this plugin's patterns; text
// matching neither stays unclassified and the engine's config decides what
// that means.
var classifyExit = extcmd.Classifier(permanentRe, transientRe)
