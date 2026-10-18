package postgres

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"

	"github.com/vi-latyshev/go-cluster-backup/internal/extcmd"
	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/source"
)

// ApplyOptions is restore.apply.postgres: the knobs that shape one restore.
// It arrives as JSON bytes in the apply request and is decoded strictly, so a
// mistyped member is an error rather than a silently ignored intention.
type ApplyOptions struct {
	Jobs            int            `json:"jobs" doc:"pg_restore --jobs; 1 restores from a stream, more spools to a file first"`
	NoOwner         bool           `json:"no_owner" doc:"pass --no-owner"`
	NoACL           bool           `json:"no_acl" doc:"pass --no-acl"`
	ConnectAttempts int            `json:"connect_attempts" doc:"TCP connect attempts before the restore gives up"`
	ConnectInterval model.Duration `json:"connect_interval" doc:"wait between connect attempts; also the per-attempt dial timeout"`
}

func (o ApplyOptions) Validate() error {
	if o.Jobs < 1 {
		return fmt.Errorf("jobs must be at least 1, got %d", o.Jobs)
	}
	if o.ConnectAttempts < 1 {
		return fmt.Errorf("connect_attempts must be at least 1, got %d", o.ConnectAttempts)
	}
	if o.ConnectInterval.Std() <= 0 {
		return errors.New("connect_interval must be positive")
	}
	return nil
}

// Apply writes the decoded artifact into this instance's server. The mode is
// mandatory: nothing is replaced without one. side_by_side creates a database
// named after the target plus the run label; in_place replaces the configured
// database inside a single transaction, so a failure changes nothing.
func (s *Source) Apply(ctx context.Context, req source.ApplyRequest, src io.Reader) (source.ApplyReport, error) {
	if err := ctx.Err(); err != nil {
		return source.ApplyReport{}, err
	}
	if req.Mode != source.ApplySideBySide && req.Mode != source.ApplyInPlace {
		return source.ApplyReport{}, fmt.Errorf("apply mode %q must be side_by_side or in_place", req.Mode)
	}
	if req.Label == "" {
		return source.ApplyReport{}, errors.New("apply label is empty; a restore target is always labelled with the run id")
	}
	if !restorable(s.cfg.Format) {
		// A plain dump is psql input, not a pg_restore archive: this instance is
		// fetch-only, and says so by call rather than by failing deep inside.
		return source.ApplyReport{}, fmt.Errorf("a %s-format dump is restored with psql, which this plugin does not drive: %w", s.cfg.Format, errors.ErrUnsupported)
	}
	opts, err := plugin.DecodeOptions[ApplyOptions](req.Options, "restore.apply.postgres")
	if err != nil {
		return source.ApplyReport{}, err
	}
	if opts.Jobs > 1 && req.Mode == source.ApplyInPlace {
		// pg_restore refuses --single-transaction together with --jobs, and an
		// in-place restore is one transaction by contract: the combination is
		// refused here, before a byte is read or a connection is made.
		return source.ApplyReport{}, plugin.Permanent(fmt.Errorf("in_place with jobs %d is refused: pg_restore's --single-transaction and --jobs are mutually exclusive; set jobs to 1 or restore side_by_side", opts.Jobs))
	}
	pass, err := extcmd.ReadPassword(s.cfg.Password.File)
	if err != nil {
		return source.ApplyReport{}, err
	}
	if err := extcmd.WaitTCP(ctx, s.host, s.port, opts.ConnectAttempts, opts.ConnectInterval.Std()); err != nil {
		if ctx.Err() != nil {
			return source.ApplyReport{}, ctx.Err()
		}
		return source.ApplyReport{}, plugin.Transient(err)
	}

	target := s.cfg.Database
	if req.Mode == source.ApplySideBySide {
		target, err = sideBySideName(s.cfg.Database, req.Label)
		if err != nil {
			return source.ApplyReport{}, plugin.Permanent(err)
		}
		if err := s.createDatabase(ctx, pass, target); err != nil {
			return source.ApplyReport{}, err
		}
	}
	n, err := s.restore(ctx, pass, target, req.Mode, opts, src)
	if err != nil {
		if req.Mode == source.ApplySideBySide {
			return source.ApplyReport{}, fmt.Errorf("restoring into the new database %q (it may hold a partial restore): %w", target, err)
		}
		return source.ApplyReport{}, err
	}
	return source.ApplyReport{Target: target, Bytes: n}, nil
}

// restorable reports whether pg_restore can read this dump format at all.
func restorable(format string) bool { return format == "custom" || format == "tar" }

// Probe proves the archive is whole without a server: pg_restore reads every
// block and writes the SQL it would have run to /dev/null.
func (s *Source) Probe(ctx context.Context, src io.Reader) (source.ProbeReport, error) {
	if err := ctx.Err(); err != nil {
		return source.ProbeReport{}, err
	}
	if !restorable(s.cfg.Format) {
		return source.ProbeReport{}, fmt.Errorf("a %s-format dump is not a pg_restore archive: %w", s.cfg.Format, errors.ErrUnsupported)
	}
	cmd := exec.CommandContext(ctx, "pg_restore", "--no-password", "-f", os.DevNull)
	n, err := runCommand(ctx, cmd, src)
	if err != nil {
		return source.ProbeReport{}, err
	}
	return source.ProbeReport{Bytes: n, Detail: "pg_restore read the archive without a server"}, nil
}

// createDatabase makes the side-by-side target. A target that already exists is
// a refusal, never a second restore over whatever is inside it.
func (s *Source) createDatabase(ctx context.Context, pass, target string) error {
	cmd := exec.CommandContext(ctx, "psql", append(s.connArgs(),
		"--no-password", "-d", s.cfg.Database, "-At",
		"-c", "CREATE DATABASE "+quoteIdent(target),
	)...)
	cmd.Env = s.pgEnv(pass)
	if _, err := extcmd.Output(ctx, cmd, classifyExit); err != nil {
		return err
	}
	return nil
}

// quoteIdent quotes a PostgreSQL identifier: an embedded double quote is
// doubled, so a name can never end the quoted identifier early and turn the
// rest of itself into SQL.
func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func (s *Source) restore(ctx context.Context, pass, target string, mode source.ApplyMode, opts ApplyOptions, src io.Reader) (int64, error) {
	args := s.connArgs()
	args = append(args, "--no-password")
	if opts.NoOwner {
		args = append(args, "--no-owner")
	}
	if opts.NoACL {
		args = append(args, "--no-acl")
	}
	switch mode {
	case source.ApplySideBySide:
		// --exit-on-error: a failed statement in the new database must fail the
		// restore, not be counted and skipped.
		args = append(args, "--exit-on-error", "-d", target)
	case source.ApplyInPlace:
		// One transaction: either every statement lands or none does.
		args = append(args, "--clean", "--if-exists", "--single-transaction", "-d", target)
	}
	cmd := exec.CommandContext(ctx, "pg_restore", args...)
	cmd.Env = s.pgEnv(pass)
	if opts.Jobs > 1 {
		return s.restoreParallel(ctx, cmd, opts.Jobs, src)
	}
	return runCommand(ctx, cmd, src)
}

// restoreParallel serves pg_restore --jobs, which refuses anything but a
// regular file and opens the path itself. The engine hands an apply a
// file-backed artifact (the decoded spool), so that file is passed through
// directly: spooling it a second time would cost another multi-gigabyte copy
// in the process temp dir, the 64 Mi emptyDir of the cluster. An artifact that
// arrives as a plain stream is refused by name — there is no path to hand over.
func (s *Source) restoreParallel(ctx context.Context, cmd *exec.Cmd, jobs int, src io.Reader) (int64, error) {
	f, ok := src.(*os.File)
	if !ok {
		return 0, plugin.Permanent(fmt.Errorf("jobs %d needs a file-backed artifact: pg_restore reads a path for a parallel restore and this one arrived as a stream (set jobs to 1 to restore from a stream)", jobs))
	}
	st, err := f.Stat()
	if err != nil {
		return 0, plugin.Transient(err)
	}
	if !st.Mode().IsRegular() {
		return 0, plugin.Permanent(fmt.Errorf("jobs %d needs a regular file: %s is %s", jobs, f.Name(), st.Mode()))
	}
	cmd.Args = append(cmd.Args, "-j", strconv.Itoa(jobs), f.Name())
	if _, err := runCommand(ctx, cmd, nil); err != nil {
		return 0, err
	}
	return st.Size(), nil
}

// sideBySideName builds "<database>_restore_<label>". Everything outside
// [A-Za-z0-9_] collapses to one underscore, and the result must still fit an
// identifier: a silently truncated name would collide with the next one.
func sideBySideName(database, label string) (string, error) {
	var b strings.Builder
	for _, r := range label {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_':
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	clean := strings.Trim(b.String(), "_")
	if clean == "" {
		return "", fmt.Errorf("label %q yields no usable database name", label)
	}
	name := database + "_restore_" + clean
	if len(name) > 63 {
		return "", fmt.Errorf("side-by-side database name %q exceeds PostgreSQL's 63-character identifier limit", name)
	}
	return name, nil
}
