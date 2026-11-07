package redis

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"testing"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source"
)

// internalSource builds a redis source against a redis-cli that writes an RDB
// into the path it is given, and a listener that answers the connect wait.
func internalSource(t *testing.T) source.Source {
	t.Helper()
	dir := t.TempDir()
	script := `#!/bin/sh
prev=""
for a in "$@"; do
  if [ "$prev" = "--rdb" ]; then printf 'RDB-BYTES' > "$a"; fi
  prev="$a"
done
`
	if err := os.WriteFile(filepath.Join(dir, "redis-cli"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			_ = c.Close()
		}
	}()

	pass := filepath.Join(t.TempDir(), "password")
	if err := os.WriteFile(pass, []byte("pw\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	raw := fmt.Sprintf(`{"address": %q, "password": {"file": %q}, "scratch_dir": %q, "connect_attempts": 2, "connect_interval": "20ms"}`,
		l.Addr().String(), pass, t.TempDir())
	lr := loader.New()
	r := instances.NewRegistry()
	if err := loader.Add(lr, Spec, func(lr *loader.Registry) error {
		return Apply(lr, r)
	}); err != nil {
		t.Fatal(err)
	}
	s, err := instances.Build(context.Background(), r, source.Kind, "redis", jsontext.Value(raw),
		instances.Deps{Instance: "redis", Log: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// TestDumpReportsASnapshotItCannotInspect: the temp file redis-cli was told to
// write is closed for it and asked for its size afterwards, and a failure of
// either step is the call's own.
func TestDumpReportsASnapshotItCannotInspect(t *testing.T) {
	s := internalSource(t)

	origClose := closeSnapshot
	closeSnapshot = func(f *os.File) error {
		_ = f.Close()
		return errors.New("injected close failure")
	}
	if err := s.Dump(context.Background(), io.Discard); err == nil {
		t.Fatal("a snapshot whose temp file could not be closed was accepted")
	}
	closeSnapshot = origClose

	origStat := statSnapshot
	statSnapshot = func(*os.File) (os.FileInfo, error) { return nil, errors.New("injected stat failure") }
	t.Cleanup(func() { statSnapshot = origStat })
	if err := s.Dump(context.Background(), io.Discard); err == nil {
		t.Fatal("a snapshot that could not be measured was accepted")
	}
}
