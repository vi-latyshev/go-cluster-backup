package k3sstate

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/source"
)

const (
	stubDB    = "SQLITE-FORMAT-3-DATASTORE-BYTES"
	stubToken = "K10thisisasecret"
)

// internalStub is a fake node: a datastore, a token, a scratch directory and a
// sqlite3 that copies the datastore and answers the integrity check.
func internalStub(t *testing.T) (datastore, token, scratch string, s source.Source) {
	t.Helper()
	root := t.TempDir()
	bin := filepath.Join(root, "bin")
	datastore = filepath.Join(root, "db", "state.db")
	token = filepath.Join(root, "token")
	scratch = filepath.Join(root, "plain")
	for _, dir := range []string{filepath.Dir(datastore), scratch, bin} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(datastore, []byte(stubDB), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(token, []byte(stubToken), 0o600); err != nil {
		t.Fatal(err)
	}
	script := `#!/bin/sh
case " $* " in
  *" PRAGMA "*) printf 'ok\n'; exit 0 ;;
esac
for a in "$@"; do
  case "$a" in
    .backup\ *)
      dest=$(printf '%s' "$a" | sed 's/^\.backup //; s/^"//; s/"$//')
      printf '%s' "$K3S_INTERNAL_DB" > "$dest"
      ;;
  esac
done
exit 0
`
	if err := os.WriteFile(filepath.Join(bin, "sqlite3"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("K3S_INTERNAL_DB", stubDB)

	raw := fmt.Sprintf(`{"datastore": %q, "token": {"file": %q}, "scratch_dir": %q, "busy_timeout": "10s", "integrity_check": "quick", "timeout": "1m"}`,
		datastore, token, scratch)
	r := plugin.NewRegistry()
	if err := Register(r); err != nil {
		t.Fatal(err)
	}
	s, err := plugin.Build(context.Background(), r, source.Kind, "k3s_state", jsontext.Value(raw),
		plugin.Deps{Instance: "k3s_state", Log: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	return datastore, token, scratch, s
}

// withSeam swaps one filesystem primitive for a failing one and restores it.
func withSeam[T any](t *testing.T, slot *T, fake T) {
	t.Helper()
	orig := *slot
	*slot = fake
	t.Cleanup(func() { *slot = orig })
}

func TestCheckReportsAFileItCannotClose(t *testing.T) {
	_, _, _, s := internalStub(t)
	withSeam(t, &closeFile, func(f *os.File) error {
		_ = f.Close()
		return errors.New("injected close failure")
	})
	if err := s.(plugin.Checker).Check(context.Background()); err == nil {
		t.Fatal("a datastore or token that could not be closed passed the preflight")
	}
}

func TestDumpReportsADirectoryItCannotMakeOrRemove(t *testing.T) {
	t.Run("the mode of the private working directory", func(t *testing.T) {
		_, _, _, s := internalStub(t)
		withSeam(t, &chmodFile, func(string, fs.FileMode) error { return errors.New("injected chmod failure") })
		if err := s.Dump(context.Background(), &bytes.Buffer{}); err == nil {
			t.Fatal("a private working directory whose mode could not be set was accepted")
		}
	})
	t.Run("a leftover it cannot sweep", func(t *testing.T) {
		_, _, scratch, s := internalStub(t)
		if err := os.Mkdir(filepath.Join(scratch, "k3s-state-leftover"), 0o700); err != nil {
			t.Fatal(err)
		}
		withSeam(t, &removeAll, func(string) error { return errors.New("injected remove failure") })
		if err := s.Dump(context.Background(), &bytes.Buffer{}); err == nil {
			t.Fatal("a leftover working directory that could not be swept was accepted")
		}
	})
	t.Run("the cleanup of its own working directory", func(t *testing.T) {
		_, _, scratch, s := internalStub(t)
		withSeam(t, &removeAll, func(string) error { return errors.New("injected remove failure") })
		err := s.Dump(context.Background(), &bytes.Buffer{})
		if err == nil {
			t.Fatal("a working directory that could not be removed was reported as success")
		}
		if !strings.Contains(err.Error(), "private working directory") {
			t.Fatalf("the failure does not name the working directory: %v", err)
		}
		// The sweep left nothing of its own behind: the fake never removed it.
		entries, readErr := os.ReadDir(scratch)
		if readErr != nil {
			t.Fatal(readErr)
		}
		for _, e := range entries {
			_ = removeAll(filepath.Join(scratch, e.Name()))
		}
	})
}

func TestPackReportsWhatItCannotHandBack(t *testing.T) {
	t.Run("the copy", func(t *testing.T) {
		_, _, _, s := internalStub(t)
		withSeam(t, &openFile, func(string) (*os.File, error) { return nil, errors.New("injected open failure") })
		if err := s.Dump(context.Background(), &bytes.Buffer{}); err == nil {
			t.Fatal("a copy that could not be opened was packed")
		}
	})
	t.Run("the handle of the copy", func(t *testing.T) {
		_, _, _, s := internalStub(t)
		calls := 0
		withSeam(t, &closeFile, func(f *os.File) error {
			calls++
			if calls == 1 {
				return errors.New("injected close failure")
			}
			return f.Close()
		})
		if err := s.Dump(context.Background(), &bytes.Buffer{}); err == nil {
			t.Fatal("a copy whose handle could not be closed was packed")
		}
	})
}

// countingCtx reports context.Canceled from Err only from its cancelAt-th call
// on: it places the cancellation exactly at the pack's own loop, which no clock
// can do reliably.
type countingCtx struct {
	calls    int
	cancelAt int
	done     chan struct{}
}

func (c *countingCtx) Deadline() (time.Time, bool) { return time.Time{}, false }
func (c *countingCtx) Done() <-chan struct{}       { return c.done }
func (c *countingCtx) Value(any) any               { return nil }
func (c *countingCtx) Err() error {
	c.calls++
	if c.calls >= c.cancelAt {
		return context.Canceled
	}
	return nil
}

// TestDumpReturnsOnCancellationAtThePack: the pack checks the context at every
// entry, so a cancelled run stops before writing the next one.
func TestDumpReturnsOnCancellationAtThePack(t *testing.T) {
	_, _, scratch, s := internalStub(t)
	ctx := &countingCtx{cancelAt: 2, done: make(chan struct{})}
	if err := s.Dump(ctx, &bytes.Buffer{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Dump = %v, want context.Canceled", err)
	}
	assertEmpty(t, scratch)
}

func assertEmpty(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Fatalf("the scratch directory still holds %v", names)
	}
}
