package k3setcd

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/configuration"
	pluginerrors "github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
)

const internalToken = "K10thisisasecret"

// internalStub builds one source over a temporary snapshot directory.
func internalStub(t *testing.T) (dir, token string, s *Source) {
	t.Helper()
	root := t.TempDir()
	dir = filepath.Join(root, "snapshots")
	token = filepath.Join(root, "token")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(token, []byte(internalToken), 0o600); err != nil {
		t.Fatal(err)
	}
	return dir, token, &Source{cfg: Config{
		SnapshotDir: dir,
		Token:       configuration.Secret{File: token},
		MaxAge:      model.Duration(2 * time.Hour),
	}}
}

// withSeam swaps one package variable for a fake and restores it.
func withSeam[T any](t *testing.T, slot *T, fake T) {
	t.Helper()
	orig := *slot
	*slot = fake
	t.Cleanup(func() { *slot = orig })
}

func writeSnapshot(t *testing.T, dir, name string, data []byte, mtime time.Time) string {
	t.Helper()
	path := filepath.Join(dir, name)
	sum := sha256.Sum256(data)
	if err := os.WriteFile(path, append(append([]byte(nil), data...), sum[:]...), 0o600); err != nil {
		t.Fatal(err)
	}
	if !mtime.IsZero() {
		if err := os.Chtimes(path, mtime, mtime); err != nil {
			t.Fatal(err)
		}
	}
	return path
}

func TestOpenSnapshotRejectsAFileItDidNotSee(t *testing.T) {
	dir, _, _ := internalStub(t)
	first := writeSnapshot(t, dir, "snap", []byte("one"), time.Time{})
	second := writeSnapshot(t, dir, "other", []byte("two"), time.Time{})
	info, err := os.Lstat(first)
	if err != nil {
		t.Fatal(err)
	}
	other, err := os.Open(second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = other.Close() })
	withSeam(t, &openSnapshotFile, func(string, int, fs.FileMode) (*os.File, error) { return other, nil })
	_, err = openSnapshot(candidate{path: first, name: "snap", info: info})
	if err == nil {
		t.Fatal("a different file than the directory showed was accepted")
	}
	if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassTransient {
		t.Fatalf("class = %q, want transient", class)
	}
}

func TestOpenSnapshotReportsOpenFailures(t *testing.T) {
	dir, _, _ := internalStub(t)
	path := writeSnapshot(t, dir, "snap", []byte("one"), time.Time{})
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	snap := candidate{path: path, name: "snap", info: info}

	t.Run("vanished", func(t *testing.T) {
		withSeam(t, &openSnapshotFile, func(string, int, fs.FileMode) (*os.File, error) {
			return nil, fs.ErrNotExist
		})
		_, err := openSnapshot(snap)
		if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassTransient {
			t.Fatalf("class = %q, want transient (%v)", class, err)
		}
	})
	t.Run("refused", func(t *testing.T) {
		withSeam(t, &openSnapshotFile, func(string, int, fs.FileMode) (*os.File, error) {
			return nil, errors.New("injected open failure")
		})
		_, err := openSnapshot(snap)
		if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassPermanent {
			t.Fatalf("class = %q, want permanent (%v)", class, err)
		}
	})
	t.Run("unreadable", func(t *testing.T) {
		withSeam(t, &statOpenSnapshot, func(*os.File) (fs.FileInfo, error) {
			return nil, errors.New("injected stat failure")
		})
		_, err := openSnapshot(snap)
		if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassTransient {
			t.Fatalf("class = %q, want transient (%v)", class, err)
		}
	})
}

func TestDumpReportsAPostStatFailure(t *testing.T) {
	dir, _, s := internalStub(t)
	writeSnapshot(t, dir, "snap", []byte("one"), time.Time{})
	calls := 0
	withSeam(t, &statOpenSnapshot, func(f *os.File) (fs.FileInfo, error) {
		calls++
		if calls > 1 {
			return nil, errors.New("injected stat failure")
		}
		return f.Stat()
	})
	if err := s.Dump(context.Background(), io.Discard); err == nil {
		t.Fatal("a snapshot whose identity could not be re-read passed")
	}
}

func TestSelectSnapshotReportsAnUnreadableEntry(t *testing.T) {
	dir, _, s := internalStub(t)
	writeSnapshot(t, dir, "snap", []byte("one"), time.Time{})
	withSeam(t, &readDir, func(string) ([]fs.DirEntry, error) { return []fs.DirEntry{badEntry{}}, nil })
	if _, err := s.selectSnapshot(context.Background()); err == nil {
		t.Fatal("a listed entry whose info could not be read was accepted")
	}
}

// badEntry is a directory entry whose Info fails, which no real filesystem does
// on demand.
type badEntry struct{}

func (badEntry) Name() string               { return "snap" }
func (badEntry) IsDir() bool                { return false }
func (badEntry) Type() fs.FileMode          { return 0 }
func (badEntry) Info() (fs.FileInfo, error) { return nil, errors.New("injected info failure") }

func TestChecksumReader(t *testing.T) {
	data := bytes.Repeat([]byte("snapshot-data-"), 1000)
	sum := sha256.Sum256(data)
	content := append(append([]byte(nil), data...), sum[:]...)

	t.Run("verifies", func(t *testing.T) {
		var got bytes.Buffer
		n, err := io.Copy(&got, newChecksumReader(bytes.NewReader(content), int64(len(data))))
		if err != nil {
			t.Fatal(err)
		}
		if n != int64(len(content)) || !bytes.Equal(got.Bytes(), content) {
			t.Fatalf("read %d bytes, want the whole %d-byte snapshot", n, len(content))
		}
	})
	t.Run("refuses a mismatching trailer", func(t *testing.T) {
		tampered := append([]byte(nil), content...)
		tampered[len(tampered)-1] ^= 0xff
		_, err := io.Copy(io.Discard, newChecksumReader(bytes.NewReader(tampered), int64(len(data))))
		if !errors.Is(err, errSnapshotChecksum) {
			t.Fatalf("read = %v, want the checksum refusal", err)
		}
	})
	t.Run("refuses a short snapshot", func(t *testing.T) {
		_, err := io.Copy(io.Discard, newChecksumReader(bytes.NewReader(content[:len(content)-10]), int64(len(data))))
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("read = %v, want io.ErrUnexpectedEOF", err)
		}
	})
	t.Run("propagates a read error", func(t *testing.T) {
		_, err := io.Copy(io.Discard, newChecksumReader(failReader{}, int64(len(data))))
		if err == nil {
			t.Fatal("a failing source stream was reported as verified")
		}
	})
	t.Run("crosses the trailer boundary in several reads", func(t *testing.T) {
		// One-byte reads put the data/trailer boundary in the middle of the
		// reader's own bookkeeping, not just at a chunk edge.
		at := 0
		oneByte := readerFunc(func(p []byte) (int, error) {
			if len(p) == 0 {
				return 0, nil
			}
			if at == len(content) {
				return 0, io.EOF
			}
			p[0] = content[at]
			at++
			return 1, nil
		})
		if _, err := io.Copy(io.Discard, newChecksumReader(oneByte, int64(len(data)))); err != nil {
			t.Fatalf("one-byte reads = %v", err)
		}
	})
}

func TestSelectSnapshotReportsAContextAndDirectoryFailure(t *testing.T) {
	_, _, s := internalStub(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.selectSnapshot(ctx); err == nil {
		t.Fatal("selectSnapshot accepted a cancelled context")
	}
	withSeam(t, &readDir, func(string) ([]fs.DirEntry, error) {
		return nil, errors.New("injected read failure")
	})
	if _, err := s.selectSnapshot(context.Background()); err == nil {
		t.Fatal("an unreadable snapshot directory was accepted")
	}
}

func TestDumpReportsAnOpenFailure(t *testing.T) {
	dir, _, s := internalStub(t)
	writeSnapshot(t, dir, "snap", []byte("one"), time.Time{})
	withSeam(t, &openSnapshotFile, func(string, int, fs.FileMode) (*os.File, error) {
		return nil, errors.New("injected open failure")
	})
	if err := s.Dump(context.Background(), io.Discard); err == nil {
		t.Fatal("a snapshot that could not be opened was packed")
	}
}

// countingCtx reports context.Canceled from Err only from its cancelAt-th call
// on: it places the cancellation exactly at a streamed copy, which no clock can
// do reliably.
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

// TestProbeStagesCancellationInsideACopy covers the copy failures that are
// neither a checksum mismatch nor a short stream: the probe returns them as the
// stream's own error instead of calling the artifact corrupt.
func TestProbeStagesCancellationInsideACopy(t *testing.T) {
	for _, name := range []string{snapshotEntry, tokenEntry} {
		t.Run(name, func(t *testing.T) {
			raw := probeTar(t, name)
			ctx := &countingCtx{cancelAt: 2, done: make(chan struct{})}
			if _, err := (&Source{}).Probe(ctx, bytes.NewReader(raw)); !errors.Is(err, context.Canceled) {
				t.Fatalf("Probe = %v, want context.Canceled", err)
			}
		})
	}
}

// probeTar builds an artifact whose first entry is name, with the other entry
// after it.
func probeTar(t *testing.T, name string) []byte {
	t.Helper()
	other := snapshotEntry
	if name == snapshotEntry {
		other = tokenEntry
	}
	body := append(snapshotBytes([]byte("DATA")), []byte("t")...)
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, entry := range []string{name, other} {
		hdr := &tar.Header{Name: entry, Mode: 0o600, Typeflag: tar.TypeReg, Size: int64(len(body))}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write(body); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func snapshotBytes(data []byte) []byte {
	sum := sha256.Sum256(data)
	return append(append([]byte(nil), data...), sum[:]...)
}

type readerFunc func([]byte) (int, error)

func (f readerFunc) Read(p []byte) (int, error) { return f(p) }

type failReader struct{}

func (failReader) Read([]byte) (int, error) { return 0, errors.New("disk fell over") }
