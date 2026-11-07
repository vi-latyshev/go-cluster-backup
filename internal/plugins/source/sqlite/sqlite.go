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
	"regexp"
	"strings"
	"sync"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/extcmd"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/io"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/sqlitecopy"
)

// Config is the plugin config. Every field is required.
type Config struct {
	Path           string         `json:"path" required:"true" doc:"absolute path of the SQLite database file"`
	ScratchDir     string         `json:"scratch_dir" required:"true" doc:"existing writable directory the snapshot's temporary file is spooled into, e.g. the big emptyDir a run mounts; the process temp dir is not used"`
	BusyTimeout    model.Duration `json:"busy_timeout" doc:"how long sqlite3 waits for a lock held by the live writer"`
	IntegrityCheck string         `json:"integrity_check" doc:"pragma a restored or probed copy must answer ok to: quick or full"`
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
		return pluginerrors.Permanent(fmt.Errorf("sqlite3 is not in PATH: %w", err))
	}
	if err := extcmd.CheckScratchDir(s.cfg.ScratchDir); err != nil {
		return pluginerrors.Permanent(err)
	}
	st, err := os.Stat(s.cfg.Path)
	if err != nil {
		return pluginerrors.Permanent(fmt.Errorf("database file: %w", err))
	}
	if st.IsDir() {
		return pluginerrors.Permanent(fmt.Errorf("%s is a directory, not a database file", s.cfg.Path))
	}
	f, err := os.Open(s.cfg.Path)
	if err != nil {
		return pluginerrors.Permanent(fmt.Errorf("database file is not readable: %w", err))
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
		return pluginerrors.Transient(err)
	}
	name := tmp.Name()
	if err := closeFile(tmp); err != nil {
		_ = os.Remove(name)
		return pluginerrors.Transient(err)
	}
	defer func() { _ = os.Remove(name) }()

	if err := s.backup(ctx, name); err != nil {
		return err
	}
	f, err := os.Open(name)
	if err != nil {
		return pluginerrors.Transient(err)
	}
	defer func() { _ = f.Close() }()
	st, err := statFile(f)
	if err != nil {
		return pluginerrors.Transient(err)
	}
	if st.Size() == 0 {
		return pluginerrors.Transient(errors.New("sqlite3 exited 0 but wrote an empty copy"))
	}
	_, err = pluginio.CopyContext(ctx, dst, f)
	return err
}

// backup runs the shell's .backup into out.
func (s *Source) backup(ctx context.Context, out string) error {
	cmd := exec.CommandContext(ctx, "sqlite3", sqlitecopy.BackupArgs(s.cfg.Path, out, s.cfg.BusyTimeout.Std())...)
	if _, err := extcmd.Output(ctx, cmd, classifyExit()); err != nil {
		return err
	}
	return nil
}

// check runs the configured pragma over file and requires the "ok" answer:
// sqlite3 exits 0 even when integrity_check reports corruption, so the output
// is the only proof.
func (s *Source) check(ctx context.Context, file string) error {
	cmd := exec.CommandContext(ctx, "sqlite3", "-readonly", file, s.pragma()+";")
	out, err := extcmd.Output(ctx, cmd, classifyExit())
	if err != nil {
		return err
	}
	if answer := strings.TrimSpace(out); answer != "ok" {
		return pluginerrors.Permanent(fmt.Errorf("%s answered %q for %s", s.pragma(), answer, file))
	}
	return nil
}

// The patterns and the classifier they build are lazily-built singletons: a
// package initializer does no work.
var (
	transientRe = sync.OnceValue(func() *regexp.Regexp {
		return regexp.MustCompile(`(?i)database is locked|database is busy|unable to close due to unfinalized|i/o error|disk i/o error`)
	})
	// A corrupt or missing database will not repair itself on the next attempt.
	permanentRe = sync.OnceValue(func() *regexp.Regexp {
		return regexp.MustCompile(`(?i)malformed|not a database|unable to open database file|file is encrypted|no such file`)
	})
)

// classifyExit marks a failed sqlite3 run with this plugin's patterns; text
// matching neither stays unclassified and the execution config decides what
// that means.
var classifyExit = sync.OnceValue(func() func(bin string, err error, detail string) error {
	return extcmd.Classifier(permanentRe(), transientRe())
})
