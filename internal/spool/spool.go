// Package spool is the run's buffer on disk: one file per artifact, written once
// by the pipeline, re-opened fresh for every upload attempt, removed after the
// artifact is done with.
package spool

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
)

type Spool struct {
	dir string
	mu  sync.Mutex
	seq int
}

// New prepares a spool directory, creating it when missing.
func New(dir string) (*Spool, error) {
	if dir == "" {
		return nil, errors.New("spool: directory is empty")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("spool: %w", err)
	}
	return &Spool{dir: dir}, nil
}

// FreeBytes reports the free space of the file system holding the spool.
func (s *Spool) FreeBytes() (int64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(s.dir, &st); err != nil {
		return 0, fmt.Errorf("spool: statfs %s: %w", s.dir, err)
	}
	return int64(st.Bavail) * int64(st.Bsize), nil
}

// Writer is one artifact being written: the pipeline's output, its size and its
// SHA256, computed as it flows.
type Writer struct {
	f    *os.File
	hash hash.Hash
	n    int64
	name string
}

// Create starts a new spool file. The name must be a plain base name: the engine
// sanitizes artifact names before calling; a separator here is a bug, not input.
func (s *Spool) Create(name string) (*Writer, error) {
	if err := checkName(name); err != nil {
		return nil, err
	}
	path := filepath.Join(s.dir, name)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, fmt.Errorf("spool: %w", err)
	}
	return &Writer{f: f, hash: sha256.New(), name: name}, nil
}

func (w *Writer) Write(p []byte) (int, error) {
	n, err := w.f.Write(p)
	w.n += int64(n)
	w.hash.Write(p[:n])
	return n, err
}

func (w *Writer) Close() error { return w.f.Close() }

func (w *Writer) Name() string { return w.name }
func (w *Writer) Size() int64  { return w.n }

func (w *Writer) SHA256() string { return hex.EncodeToString(w.hash.Sum(nil)) }

// Open returns a fresh reader over the spool file — every upload attempt reads
// from its own handle, so a retry never reuses an exhausted reader.
func (s *Spool) Open(name string) (*os.File, error) {
	if err := checkName(name); err != nil {
		return nil, err
	}
	f, err := os.Open(filepath.Join(s.dir, name))
	if err != nil {
		return nil, fmt.Errorf("spool: %w", err)
	}
	return f, nil
}

// Remove deletes a spool file; a file that is already gone is not an error.
func (s *Spool) Remove(name string) error {
	if err := checkName(name); err != nil {
		return err
	}
	if err := os.Remove(filepath.Join(s.dir, name)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("spool: %w", err)
	}
	return nil
}

// NextName produces a unique, safe spool file name for one artifact: an index
// prefix plus a sanitized artifact name.
func (s *Spool) NextName(artifact string) string {
	s.mu.Lock()
	s.seq++
	seq := s.seq
	s.mu.Unlock()
	return fmt.Sprintf("%03d-%s", seq, sanitize(artifact))
}

func checkName(name string) error {
	if name == "" || name != filepath.Base(name) || strings.Contains(name, "..") {
		return fmt.Errorf("spool: invalid file name %q", name)
	}
	return nil
}

func sanitize(name string) string {
	var b strings.Builder
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '.', r == '-', r == '_':
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	out := strings.Trim(b.String(), ".")
	if out == "" {
		return "artifact"
	}
	if len(out) > 120 {
		out = out[:120]
	}
	return out
}
