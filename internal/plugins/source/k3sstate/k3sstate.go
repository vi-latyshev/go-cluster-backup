// Package k3sstate is the source that snapshots a k3s sqlite datastore: an
// online-backup copy of the database, the integrity check the config asks for,
// the server token, all as ONE tar stream. It runs in a privileged init
// container, so it knows node-level facts — but every path it touches comes
// from the config, not from this file.
//
// The plaintext copy of the datastore exists only inside a private working
// directory this package creates inside the configured scratch directory
// (mode 0700, created by this process, removed on every exit path including a
// cancelled context and a failed step). The scratch directory itself is
// kubelet's — a Kubernetes emptyDir is 0777 with setgid added by fsGroup, so
// its mode says nothing about who can read what is inside it.
package k3sstate

import (
	"archive/tar"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/extcmd"
	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/source"
	"github.com/vi-latyshev/go-cluster-backup/internal/sqlitecopy"
)

// IntegrityCheck names the `PRAGMA integrity_check` depth the copy is held to.
// "none" exists because the full check re-reads the whole datastore and can
// take minutes on a large one; it is a config decision, never a silent default.
type IntegrityCheck string

const (
	IntegrityQuick IntegrityCheck = "quick"
	IntegrityFull  IntegrityCheck = "full"
	IntegrityNone  IntegrityCheck = "none"
)

type Config struct {
	// Datastore is the k3s sqlite database. It is read, never written, but the
	// mount is read-write: sqlite needs to write state.db-shm beside a WAL-mode
	// database just to read it.
	Datastore string `json:"datastore" required:"true" doc:"path of the k3s sqlite datastore, e.g. /k3s-db/state.db"`
	// Token is the k3s server token as a mounted secret file: the framework's
	// rule types a secret-named field as plugin.Secret, and this one is
	// exactly that — a reference to a file, never a literal value. The token
	// is copied into the tar stream straight from the mount, never to a file.
	Token plugin.Secret `json:"token" doc:"the k3s server token, as a mounted secret file reference"`
	// ScratchDir is the directory this source works in — a tmpfs emptyDir in
	// the cluster. Only the working directory created inside it holds
	// plaintext; the scratch directory's own mode is not judged, because
	// kubelet creates an emptyDir 0777 and only the mounted tmpfs keeps the
	// copy off any disk.
	ScratchDir string `json:"scratch_dir" required:"true" doc:"directory this source works in, e.g. the tmpfs emptyDir /plain; a private 0700 working directory is created inside it and removed on every exit path"`
	// BusyTimeout bounds how long sqlite waits for the live k3s writer's locks.
	BusyTimeout model.Duration `json:"busy_timeout" doc:"how long sqlite3 waits for the live writer's locks, e.g. \"10s\""`
	// IntegrityCheck runs against the COPY, after the copy is complete.
	IntegrityCheck IntegrityCheck `json:"integrity_check" doc:"integrity check on the copy: \"quick\", \"full\", or \"none\" to skip it"`
	// Timeout bounds each external command.
	Timeout model.Duration `json:"timeout" doc:"time budget for each sqlite3 invocation"`
}

// Config implements plugin.Validator through Validate.
var _ plugin.Validator = Config{}

func (c Config) Validate() error {
	if c.Datastore == "" {
		return errors.New("datastore is empty")
	}
	if c.Token.File == "" {
		return errors.New("token: file is empty")
	}
	if c.ScratchDir == "" {
		return errors.New("scratch_dir is empty")
	}
	for _, field := range []struct {
		name string
		path string
	}{{"datastore", c.Datastore}, {"token", c.Token.File}, {"scratch_dir", c.ScratchDir}} {
		name, path := field.name, field.path
		if !filepath.IsAbs(path) {
			return fmt.Errorf("%s %q must be absolute", name, path)
		}
	}
	switch c.IntegrityCheck {
	case IntegrityQuick, IntegrityFull, IntegrityNone:
	default:
		return fmt.Errorf("integrity_check %q is not quick, full or none", c.IntegrityCheck)
	}
	if c.BusyTimeout.Std() <= 0 {
		return fmt.Errorf("busy_timeout must be positive, got %s", c.BusyTimeout)
	}
	if c.Timeout.Std() <= 0 {
		return fmt.Errorf("timeout must be positive, got %s", c.Timeout)
	}
	return nil
}

// Register adds the k3s_state source to a registry.
func Register(r *plugin.Registry) error {
	return plugin.Register(r, source.Kind, plugin.Spec[Config, source.Source]{
		Type:      "k3s_state",
		Summary:   "k3s sqlite datastore + server token as one tar stream, with a configurable integrity check",
		Stability: plugin.Stable,
		Requires:  []string{"sqlite3"},
		Example: []byte(`{"datastore": "/k3s-db/state.db", "token": {"file": "/k3s-token"}, ` +
			`"scratch_dir": "/plain", "busy_timeout": "10s", "integrity_check": "quick", "timeout": "5m"}`),
		New: func(ctx context.Context, cfg Config, _ plugin.Deps) (source.Source, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			// cfg is already validated: Decode runs Config.Validate when its own
			// walk is clean, and this constructor is only reached through it.
			// No filesystem access here: the conformance kit constructs every
			// plugin from its example, and the mounts exist only in the init
			// container. Check verifies them at preflight; Dump fails loudly.
			return &Source{cfg: cfg, sqlite3: "sqlite3"}, nil
		},
	})
}

// The filesystem primitives this package drives. Each is a variable so a test
// can make it fail: no filesystem fails them on demand, and a branch no test
// reaches is a branch nobody has run.
var (
	closeFile = func(f *os.File) error { return f.Close() }
	removeAll = os.RemoveAll
	chmodFile = os.Chmod
	openFile  = os.Open
)

type Source struct {
	cfg     Config
	sqlite3 string
}

var _ plugin.Checker = (*Source)(nil)

// Check is the preflight, and it asserts only what it can know from here: the
// scratch directory exists and this process can write in it, the datastore and
// the token can be opened, and sqlite3 is on PATH. The scratch directory's
// mode is deliberately not judged — kubelet creates an emptyDir 0777 and
// fsGroup adds setgid, so a mode test would refuse every real mount; privacy
// comes from the 0700 working directory Dump creates inside it.
func (s *Source) Check(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := extcmd.CheckScratchDir(s.cfg.ScratchDir); err != nil {
		return plugin.Permanent(err)
	}
	for _, field := range []struct {
		name string
		path string
	}{{"datastore", s.cfg.Datastore}, {"token", s.cfg.Token.File}} {
		f, err := os.Open(field.path)
		if err != nil {
			return plugin.Permanent(fmt.Errorf("%s: %w", field.name, err))
		}
		if err := closeFile(f); err != nil {
			return plugin.Permanent(fmt.Errorf("%s: %w", field.name, err))
		}
	}
	if _, err := extcmd.FindBinary(s.sqlite3); err != nil {
		return err
	}
	return nil
}

// Describe names the datastore as the origin — a path, no credentials. The
// artifact is not sealed here: the stage pipeline's processor is what seals it.
func (s *Source) Describe(ctx context.Context) (source.SourceTraits, error) {
	if err := ctx.Err(); err != nil {
		return source.SourceTraits{}, err
	}
	return source.SourceTraits{
		Ext:    ".tar",
		Origin: (&url.URL{Scheme: "sqlite3", Path: s.cfg.Datastore}).String(),
	}, nil
}

// workPrefix names the entries this package creates in the scratch directory —
// the working directory of a dump. The prefix is also how leftovers are
// recognised after a run was killed; the preflight's writability probe belongs
// to extcmd and uses its own prefix.
const workPrefix = "k3s-state-"

// Dump builds the artifact: an online-backup copy of the datastore, the
// integrity check, and the token, in one tar stream. Every plaintext byte
// lives in a working directory this function creates (mode 0700) and removes
// on every exit path.
func (s *Source) Dump(ctx context.Context, dst io.Writer) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	// A working directory left behind by a run killed before its cleanup is
	// plaintext this package promised not to leave: remove it and go on.
	// Only entries with this package's own prefix are touched — the scratch
	// directory belongs to this source by config, but a file inside it that
	// this package did not create is not ours to delete.
	if err := s.removeWorkDirs(); err != nil {
		return fmt.Errorf("sweeping leftovers in %s: %w", s.cfg.ScratchDir, err)
	}
	// The scratch directory is kubelet's (0777, setgid from fsGroup), so the
	// plaintext does not go into it directly: os.MkdirTemp creates 0700, and
	// only this process knows the name. The tmpfs underneath is what keeps
	// the bytes off any disk.
	work, err := os.MkdirTemp(s.cfg.ScratchDir, workPrefix)
	if err != nil {
		return fmt.Errorf("private working directory in %s: %w", s.cfg.ScratchDir, err)
	}
	defer func() {
		if rmErr := removeAll(work); rmErr != nil {
			err = errors.Join(err, fmt.Errorf("removing the private working directory %s: %w", work, rmErr))
		}
	}()
	// On Linux a directory created inside a setgid parent inherits the setgid
	// bit, and the scratch directory is exactly that (kubelet's emptyDir with
	// fsGroup), so MkdirTemp's 0700 comes back as 02700. The bit is cleared so
	// the mode this package promises — 0700, owner-only, nothing inherited — is
	// what stands there, the same on every platform and in the cluster.
	if err := chmodFile(work, 0o700); err != nil {
		return fmt.Errorf("setting the mode of the private working directory %s: %w", work, err)
	}

	copyPath := filepath.Join(work, "state.db")
	if err := s.copyDatastore(ctx, work, copyPath); err != nil {
		return err
	}
	if err := s.checkIntegrity(ctx, work, copyPath); err != nil {
		return err
	}
	if err := s.pack(ctx, dst, copyPath); err != nil {
		return err
	}
	return nil
}

// copyDatastore runs sqlite3's online-backup command: the .backup dot-command
// takes the correct locks against the live k3s writer and writes one consistent
// file, with no -wal or -shm beside it.
func (s *Source) copyDatastore(ctx context.Context, work, copyPath string) error {
	if _, err := os.Stat(s.cfg.Datastore); err != nil {
		return plugin.Permanent(fmt.Errorf("datastore: %w", err))
	}
	_, err := s.runSQLite(ctx, work, sqlitecopy.BackupArgs(s.cfg.Datastore, copyPath, s.cfg.BusyTimeout.Std())...)
	if err != nil {
		return fmt.Errorf("snapshotting the datastore: %w", err)
	}
	if _, err := os.Stat(copyPath); err != nil {
		return fmt.Errorf("sqlite3 reported success but wrote no copy: %w", err)
	}
	return nil
}

func (s *Source) checkIntegrity(ctx context.Context, work, copyPath string) error {
	pragma, ok := sqlitecopy.Pragma(string(s.cfg.IntegrityCheck))
	if !ok {
		// "none" is the one depth this source offers that runs no pragma: skip
		// the check rather than fail the run.
		return nil
	}
	out, err := s.runSQLite(ctx, work, "-cmd", ".timeout "+sqlitecopy.BusyMillis(s.cfg.BusyTimeout.Std()), copyPath, pragma)
	if err != nil {
		return fmt.Errorf("integrity check on the copy: %w", err)
	}
	if strings.TrimSpace(out) != "ok" {
		return plugin.Permanent(fmt.Errorf("the datastore copy failed %s: %s", s.cfg.IntegrityCheck, strings.TrimSpace(out)))
	}
	return nil
}

// pack writes one tar stream holding the copy and the token. The token is read
// from its source file straight into the stream: it never lands in the scratch
// directory, so the only plaintext copy on disk is the datastore's.
func (s *Source) pack(ctx context.Context, dst io.Writer, copyPath string) error {
	token, err := os.ReadFile(s.cfg.Token.File)
	if err != nil {
		return plugin.Permanent(fmt.Errorf("token: %w", err))
	}
	info, err := os.Stat(copyPath)
	if err != nil {
		return fmt.Errorf("stat of the datastore copy: %w", err)
	}
	tw := tar.NewWriter(dst)
	// Header times are fixed: two snapshots of an unchanged datastore must
	// produce identical bytes, so the artifact is compared by content.
	epoch := time.Unix(0, 0)
	entries := []struct {
		name string
		mode int64
		body []byte
		size int64
	}{
		{"state.db", 0o644, nil, info.Size()},
		{"token", 0o600, token, int64(len(token))},
	}
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := tw.WriteHeader(&tar.Header{
			Name: entry.name, Mode: entry.mode, Size: entry.size,
			Typeflag: tar.TypeReg, ModTime: epoch,
		}); err != nil {
			return fmt.Errorf("tar header %s: %w", entry.name, err)
		}
		if entry.body != nil {
			if _, err := tw.Write(entry.body); err != nil {
				return fmt.Errorf("tar body %s: %w", entry.name, err)
			}
			continue
		}
		f, err := openFile(copyPath)
		if err != nil {
			return fmt.Errorf("open of the datastore copy: %w", err)
		}
		_, copyErr := plugin.CopyContext(ctx, tw, f)
		closeErr := closeFile(f)
		if copyErr != nil {
			return fmt.Errorf("tar body %s: %w", entry.name, copyErr)
		}
		if closeErr != nil {
			return fmt.Errorf("close of the datastore copy: %w", closeErr)
		}
	}
	if err := tw.Close(); err != nil {
		return fmt.Errorf("tar trailer: %w", err)
	}
	return nil
}

// runSQLite runs the sqlite3 binary with a per-call deadline composed with the
// caller's context, returns what it printed on stdout, and classifies a
// non-zero exit as an error that names the exit code and sqlite's own message.
func (s *Source) runSQLite(ctx context.Context, work string, args ...string) (string, error) {
	path, err := extcmd.FindBinary(s.sqlite3)
	if err != nil {
		return "", err
	}
	// sqlite's temp files belong in the working directory on the tmpfs — the
	// root filesystem of the init container is read-only, so /tmp and
	// /var/tmp are not writable, and a temp file next to the copy is also one
	// more thing this package must be able to delete.
	out, err := extcmd.Run(ctx, s.cfg.Timeout.Std(), path, extcmd.ReplaceEnv(os.Environ(), "SQLITE_TMPDIR="+work), args...)
	if err != nil {
		return out, fmt.Errorf("sqlite3 %s: %w: %s", args[len(args)-1], err, strings.TrimSpace(out))
	}
	return out, nil
}

// removeWorkDirs deletes the scratch-directory entries this package created
// and left behind — a dump killed before its own cleanup, or a probe that
// could not be removed. Anything else in there was put there by someone else.
func (s *Source) removeWorkDirs() error {
	entries, err := os.ReadDir(s.cfg.ScratchDir)
	if err != nil {
		return err
	}
	var errs []error
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), workPrefix) {
			continue
		}
		if err := removeAll(filepath.Join(s.cfg.ScratchDir, entry.Name())); err != nil && !errors.Is(err, fs.ErrNotExist) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
