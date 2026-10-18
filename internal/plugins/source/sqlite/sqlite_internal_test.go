package sqlite

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/source"
)

// internalStub installs a sqlite3 that answers every pragma with ok and writes
// a copy for every .backup.
func internalStub(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	script := `#!/bin/sh
last=""
for a in "$@"; do last="$a"; done
case "$last" in
  .backup*)
    d=${last#".backup "}
    d=${d#\"}
    d=${d%\"}
    printf 'SQLITE-COPY' > "$d"
    ;;
  *)
    printf 'ok\n'
    ;;
esac
exit 0
`
	if err := os.WriteFile(filepath.Join(dir, "sqlite3"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
}

func internalSource(t *testing.T, path, scratch string) source.Source {
	t.Helper()
	raw := fmt.Sprintf(`{"path": %q, "scratch_dir": %q, "busy_timeout": "10s", "integrity_check": "quick"}`, path, scratch)
	r := plugin.NewRegistry()
	if err := Register(r); err != nil {
		t.Fatal(err)
	}
	s, err := plugin.Build(context.Background(), r, source.Kind, "sqlite", jsontext.Value(raw),
		plugin.Deps{Instance: "sqlite", Log: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// internalApply runs one restore and returns its error.
func internalApply(t *testing.T, s source.Source, mode source.ApplyMode, opts string) error {
	t.Helper()
	_, err := s.(source.Restorer).Apply(context.Background(), source.ApplyRequest{
		Mode: mode, Label: "l", Options: jsontext.Value(opts),
	}, strings.NewReader("RESTORED"))
	return err
}

const (
	keepOptions = `{"file_mode": "0640", "keep_previous": true}`
	dropOptions = `{"file_mode": "0640", "keep_previous": false}`
)

// liveDatabase writes the database file a restore replaces.
func liveDatabase(t *testing.T, dir string) string {
	t.Helper()
	path := filepath.Join(dir, "app.db")
	if err := os.WriteFile(path, []byte("LIVE-DB"), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestApplyReportsAModeItCannotSet(t *testing.T) {
	internalStub(t)
	orig := chmodFile
	chmodFile = func(string, fs.FileMode) error { return errors.New("injected chmod failure") }
	t.Cleanup(func() { chmodFile = orig })
	dir := t.TempDir()
	if err := internalApply(t, internalSource(t, liveDatabase(t, dir), t.TempDir()), source.ApplyInPlace, keepOptions); err == nil {
		t.Fatal("a copy whose mode could not be set was applied")
	}
}

func TestApplyReportsARenameItCannotDo(t *testing.T) {
	internalStub(t)
	orig := renameFile
	renameFile = func(string, string) error { return errors.New("injected rename failure") }
	t.Cleanup(func() { renameFile = orig })

	t.Run("side_by_side", func(t *testing.T) {
		dir := t.TempDir()
		if err := internalApply(t, internalSource(t, liveDatabase(t, dir), t.TempDir()), source.ApplySideBySide, dropOptions); err == nil {
			t.Fatal("a side-by-side target that could not be renamed into place was accepted")
		}
	})
	t.Run("the current database aside", func(t *testing.T) {
		dir := t.TempDir()
		if err := internalApply(t, internalSource(t, liveDatabase(t, dir), t.TempDir()), source.ApplyInPlace, keepOptions); err == nil {
			t.Fatal("a current database that could not be moved aside was accepted")
		}
	})
	t.Run("the replacement into place", func(t *testing.T) {
		dir := t.TempDir()
		path := liveDatabase(t, dir)
		calls := 0
		renameFile = func(from, to string) error {
			calls++
			if calls == 2 {
				return errors.New("injected rename failure")
			}
			return orig(from, to)
		}
		err := internalApply(t, internalSource(t, path, t.TempDir()), source.ApplyInPlace, keepOptions)
		if err == nil {
			t.Fatal("a replacement that could not be renamed into place was accepted")
		}
		if !strings.Contains(err.Error(), "could not be replaced") {
			t.Fatalf("the refusal does not name where the current database went: %v", err)
		}
	})
	t.Run("a write-ahead file", func(t *testing.T) {
		dir := t.TempDir()
		path := liveDatabase(t, dir)
		if err := os.WriteFile(path+"-wal", []byte("OLD-WAL"), 0o644); err != nil {
			t.Fatal(err)
		}
		calls := 0
		renameFile = func(from, to string) error {
			calls++
			if calls == 2 {
				return errors.New("injected rename failure")
			}
			return orig(from, to)
		}
		err := internalApply(t, internalSource(t, path, t.TempDir()), source.ApplyInPlace, keepOptions)
		if err == nil {
			t.Fatal("a write-ahead file that could not follow its database was accepted")
		}
		if !strings.Contains(err.Error(), "write-ahead") {
			t.Fatalf("the refusal does not name the write-ahead files: %v", err)
		}
	})
}

func TestApplyReportsAStatThatIsNotAbsence(t *testing.T) {
	internalStub(t)
	orig := lstatFile
	lstatFile = func(string) (os.FileInfo, error) { return nil, errors.New("injected lstat failure") }
	t.Cleanup(func() { lstatFile = orig })

	t.Run("the current database", func(t *testing.T) {
		dir := t.TempDir()
		if err := internalApply(t, internalSource(t, liveDatabase(t, dir), t.TempDir()), source.ApplyInPlace, keepOptions); err == nil {
			t.Fatal("a current database that could not be stat'd was accepted")
		}
	})
	t.Run("a target", func(t *testing.T) {
		dir := t.TempDir()
		if err := internalApply(t, internalSource(t, liveDatabase(t, dir), t.TempDir()), source.ApplySideBySide, dropOptions); err == nil {
			t.Fatal("a target that could not be stat'd was accepted")
		}
	})
	t.Run("a write-ahead file", func(t *testing.T) {
		dir := t.TempDir()
		path := liveDatabase(t, dir)
		if err := os.WriteFile(path+"-wal", []byte("OLD-WAL"), 0o644); err != nil {
			t.Fatal(err)
		}
		lstatFile = func(p string) (os.FileInfo, error) {
			if strings.HasSuffix(p, "-wal") {
				return nil, errors.New("injected lstat failure")
			}
			return orig(p)
		}
		if err := internalApply(t, internalSource(t, path, t.TempDir()), source.ApplyInPlace, keepOptions); err == nil {
			t.Fatal("a write-ahead file that could not be stat'd was accepted")
		}
	})
}

func TestDumpReportsWhatItCannotHandBack(t *testing.T) {
	internalStub(t)

	origClose := closeFile
	closeFile = func(f *os.File) error {
		_ = f.Close()
		return errors.New("injected close failure")
	}
	if err := internalSource(t, liveDatabase(t, t.TempDir()), t.TempDir()).Dump(context.Background(), io.Discard); err == nil {
		t.Fatal("a snapshot whose temp file could not be closed was accepted")
	}
	closeFile = origClose

	origStat := statFile
	statFile = func(*os.File) (os.FileInfo, error) { return nil, errors.New("injected stat failure") }
	t.Cleanup(func() { statFile = origStat })
	if err := internalSource(t, liveDatabase(t, t.TempDir()), t.TempDir()).Dump(context.Background(), io.Discard); err == nil {
		t.Fatal("a snapshot that could not be measured was accepted")
	}
}
