package sqlite

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/source"
)

// ApplyOptions is restore.apply.sqlite: the knobs that shape one restore.
type ApplyOptions struct {
	FileMode     string `json:"file_mode" doc:"octal mode of the restored database file, e.g. \"0644\""`
	KeepPrevious bool   `json:"keep_previous" doc:"an in-place restore moves the current file aside instead of dropping it"`
}

func (o ApplyOptions) Validate() error {
	if o.FileMode == "" {
		return errors.New("file_mode is empty")
	}
	if _, err := strconv.ParseUint(o.FileMode, 8, 32); err != nil {
		return fmt.Errorf("file_mode %q is not an octal mode: %w", o.FileMode, err)
	}
	return nil
}

var labelRe = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

// Apply writes the decoded artifact beside or over this instance's database.
// The copy lands in a temp file in the same directory, the configured integrity
// check must answer ok before it is published, and the publish itself is a
// rename, so a torn restore never becomes the database.
//
// side_by_side writes <path>.<label> and refuses to touch an existing file;
// in_place replaces <path>, keeping the current file as <path>.previous_<label>
// when keep_previous says so. SQLite's -wal and -shm files belong to the
// database beside them: an in-place restore moves them aside with the previous
// file, or drops them when no previous copy is kept — a replaced database must
// never inherit another database's write-ahead log.
func (s *Source) Apply(ctx context.Context, req source.ApplyRequest, src io.Reader) (source.ApplyReport, error) {
	if err := ctx.Err(); err != nil {
		return source.ApplyReport{}, err
	}
	if req.Mode != source.ApplySideBySide && req.Mode != source.ApplyInPlace {
		return source.ApplyReport{}, fmt.Errorf("apply mode %q must be side_by_side or in_place", req.Mode)
	}
	if !labelRe.MatchString(req.Label) {
		return source.ApplyReport{}, fmt.Errorf("apply label %q is empty or carries characters no file name should carry", req.Label)
	}
	opts, err := plugin.DecodeOptions[ApplyOptions](req.Options, "restore.apply.sqlite")
	if err != nil {
		return source.ApplyReport{}, err
	}
	// Validate ran during decode, so the mode parses; a failure here would be a
	// decode that never validated, and there is no behaviour to give it.
	mode, _ := strconv.ParseUint(opts.FileMode, 8, 32)

	dir := filepath.Dir(s.cfg.Path)
	tmp, err := os.CreateTemp(dir, ".restore-*")
	if err != nil {
		return source.ApplyReport{}, plugin.Transient(err)
	}
	tmpName := tmp.Name()
	removeTmp := func() { _ = os.Remove(tmpName) }
	n, copyErr := plugin.CopyContext(ctx, tmp, src)
	if closeErr := tmp.Close(); copyErr == nil {
		copyErr = closeErr
	}
	if copyErr != nil {
		removeTmp()
		return source.ApplyReport{}, copyErr
	}
	if err := chmodFile(tmpName, os.FileMode(mode)); err != nil {
		removeTmp()
		return source.ApplyReport{}, plugin.Transient(err)
	}
	if err := s.check(ctx, tmpName); err != nil {
		removeTmp()
		return source.ApplyReport{}, err
	}

	report := source.ApplyReport{Bytes: n}
	switch req.Mode {
	case source.ApplySideBySide:
		target := s.cfg.Path + "." + req.Label
		if err := refuseExisting(target); err != nil {
			removeTmp()
			return source.ApplyReport{}, err
		}
		if err := renameFile(tmpName, target); err != nil {
			removeTmp()
			return source.ApplyReport{}, plugin.Transient(err)
		}
		report.Target = target
	case source.ApplyInPlace:
		if opts.KeepPrevious {
			if _, statErr := lstatFile(s.cfg.Path); statErr == nil {
				previous := s.cfg.Path + ".previous_" + req.Label
				if err := refuseExisting(previous); err != nil {
					removeTmp()
					return source.ApplyReport{}, err
				}
				if err := renameFile(s.cfg.Path, previous); err != nil {
					removeTmp()
					return source.ApplyReport{}, plugin.Transient(err)
				}
				if err := moveSidecars(s.cfg.Path, previous); err != nil {
					removeTmp()
					return source.ApplyReport{}, fmt.Errorf("the current database is at %q and its write-ahead files could not follow it: %w", previous, err)
				}
				report.Previous = previous
			} else if !errors.Is(statErr, os.ErrNotExist) {
				removeTmp()
				return source.ApplyReport{}, plugin.Transient(statErr)
			}
		} else if err := removeSidecars(s.cfg.Path); err != nil {
			removeTmp()
			return source.ApplyReport{}, err
		}
		if err := renameFile(tmpName, s.cfg.Path); err != nil {
			removeTmp()
			if report.Previous != "" {
				return source.ApplyReport{}, fmt.Errorf("the current database is at %q and %q could not be replaced: %w", report.Previous, s.cfg.Path, err)
			}
			return source.ApplyReport{}, plugin.Transient(err)
		}
		report.Target = s.cfg.Path
	}
	return report, nil
}

// sidecarSuffixes are SQLite's write-ahead files: each belongs to the database
// file whose path it extends, and only to it.
var sidecarSuffixes = []string{"-wal", "-shm"}

// moveSidecars moves <from>-wal and <from>-shm beside the file that was moved
// to <to>, each only when it exists; an existing destination is refused, never
// overwritten.
func moveSidecars(from, to string) error {
	for _, ext := range sidecarSuffixes {
		if _, err := lstatFile(from + ext); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return plugin.Transient(err)
		}
		if err := refuseExisting(to + ext); err != nil {
			return err
		}
		if err := renameFile(from+ext, to+ext); err != nil {
			return plugin.Transient(err)
		}
	}
	return nil
}

// removeSidecars drops the write-ahead files of a database that is being
// replaced without a previous copy kept.
func removeSidecars(path string) error {
	for _, ext := range sidecarSuffixes {
		if err := os.Remove(path + ext); err != nil && !errors.Is(err, os.ErrNotExist) {
			return plugin.Transient(err)
		}
	}
	return nil
}

// Probe copies the artifact into a temp file and runs the configured integrity
// check over it: the shell reads every page, so a torn file is caught here.
func (s *Source) Probe(ctx context.Context, src io.Reader) (source.ProbeReport, error) {
	if err := ctx.Err(); err != nil {
		return source.ProbeReport{}, err
	}
	tmp, err := os.CreateTemp(s.cfg.ScratchDir, "sqlite-probe-*")
	if err != nil {
		return source.ProbeReport{}, plugin.Transient(err)
	}
	name := tmp.Name()
	defer func() { _ = os.Remove(name) }()
	n, copyErr := plugin.CopyContext(ctx, tmp, src)
	if closeErr := tmp.Close(); copyErr == nil {
		copyErr = closeErr
	}
	if copyErr != nil {
		return source.ProbeReport{}, copyErr
	}
	if err := s.check(ctx, name); err != nil {
		return source.ProbeReport{}, err
	}
	return source.ProbeReport{Bytes: n, Detail: s.pragma() + " answered ok"}, nil
}

// refuseExisting keeps a second restore from landing on the first one's target.
func refuseExisting(path string) error {
	if _, err := lstatFile(path); err == nil {
		return plugin.Permanent(fmt.Errorf("%s already exists; a side-by-side target is never overwritten", path))
	} else if !errors.Is(err, os.ErrNotExist) {
		return plugin.Transient(err)
	}
	return nil
}
