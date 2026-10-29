package spool

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWriteOpenRemove(t *testing.T) {
	sp, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	w, err := sp.Create("a-01.bin")
	if err != nil {
		t.Fatal(err)
	}
	name := w.Name()
	body := []byte("artifact bytes")
	if _, err := w.Write(body); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if w.Size() != int64(len(body)) {
		t.Fatalf("Size = %d", w.Size())
	}
	sum := sha256.Sum256(body)
	if w.SHA256() != hex.EncodeToString(sum[:]) {
		t.Fatalf("SHA256 = %s", w.SHA256())
	}
	// Every attempt gets a fresh reader: two Opens both read the same bytes.
	for i := 0; i < 2; i++ {
		f, err := sp.Open(name)
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(f)
		if cerr := f.Close(); cerr != nil {
			t.Fatal(cerr)
		}
		if err != nil || string(got) != string(body) {
			t.Fatalf("Open %d: %q, %v", i, got, err)
		}
	}
	if err := sp.Remove(name); err != nil {
		t.Fatal(err)
	}
	if _, err := sp.Open(name); err == nil {
		t.Fatal("removed file still opens")
	}
	if err := sp.Remove(name); err != nil {
		t.Fatalf("removing an absent file must be fine: %v", err)
	}
}

// TestCreateNeverReusesAName: two Creates under one label are two files with
// their own bytes — a retry cannot truncate the file another attempt or another
// process is reading, because neither ever learns the other's name.
func TestCreateNeverReusesAName(t *testing.T) {
	sp, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	first, err := sp.Create("retry.bin")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Write([]byte("first attempt, longer")); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := sp.Create("retry.bin")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := second.Write([]byte("second")); err != nil {
		t.Fatal(err)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	if first.Name() == second.Name() {
		t.Fatalf("two creates chose the same name %q", first.Name())
	}
	for name, want := range map[string]string{first.Name(): "first attempt, longer", second.Name(): "second"} {
		f, err := sp.Open(name)
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(f)
		if cerr := f.Close(); cerr != nil {
			t.Fatal(cerr)
		}
		if err != nil || string(got) != want {
			t.Fatalf("file %q holds %q, %v; want %q", name, got, err, want)
		}
	}
}

func TestInvalidNames(t *testing.T) {
	sp, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"", ".", "..", "../escape", "a/b", ".hidden/../../x"} {
		if _, err := sp.Open(name); err == nil {
			t.Errorf("Open(%q) accepted", name)
		}
		if err := sp.Remove(name); err == nil {
			t.Errorf("Remove(%q) accepted", name)
		}
	}
}

// TestCreatedNamesAreOpenedAndRemovedByTheSpool: every name Create picks must
// pass the check Open and Remove apply — including a label that keeps an inner
// ".." or starts with a dot. A name the spool cannot read back is a file no
// clean-up can remove.
func TestCreatedNamesAreOpenedAndRemovedByTheSpool(t *testing.T) {
	sp, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	labels := []string{"db..dump", "../escape", "a/b", ".hidden", "...", "sp ace*", "..dots..", strings.Repeat("x", 500)}
	for _, label := range labels {
		w, err := sp.Create(label)
		if err != nil {
			t.Fatalf("Create(%q): %v", label, err)
		}
		name := w.Name()
		if _, err := w.Write([]byte("bytes")); err != nil {
			t.Fatalf("Create(%q) write: %v", label, err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Create(%q) close: %v", label, err)
		}
		f, err := sp.Open(name)
		if err != nil {
			t.Fatalf("Open(Create(%q).Name()) = %q: %v", label, name, err)
		}
		got, err := io.ReadAll(f)
		if cerr := f.Close(); cerr != nil {
			t.Fatal(cerr)
		}
		if err != nil || string(got) != "bytes" {
			t.Fatalf("the file Create(%q) made holds %q, %v", label, got, err)
		}
		if err := sp.Remove(name); err != nil {
			t.Fatalf("Remove(Create(%q).Name()) = %q: %v", label, name, err)
		}
	}
}

func TestCreateSanitizesTheLabel(t *testing.T) {
	sp, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	w, err := sp.Create("databases/ns/postgres.dump")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()
	if w.Name() != filepath.Base(w.Name()) || strings.ContainsAny(w.Name(), "/\\") {
		t.Fatalf("Create produced a path: %q", w.Name())
	}
	if !strings.HasPrefix(w.Name(), "databases_ns_postgres.dump-") {
		t.Fatalf("Create(%q) = %q, want the sanitized label as the prefix", "databases/ns/postgres.dump", w.Name())
	}
	empty, err := sp.Create("...")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = empty.Close() }()
	if !strings.HasPrefix(empty.Name(), "artifact-") {
		t.Fatalf("a fully sanitized label gave %q", empty.Name())
	}
	long, err := sp.Create(strings.Repeat("x", 500))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = long.Close() }()
	if len(long.Name()) > 140 {
		t.Fatalf("long label not truncated: %d chars", len(long.Name()))
	}
}

// TestCreateLandsPrivate: an artifact is as sensitive as a secret one, so a
// spool file is 0600 — never wider, whatever the label says.
func TestCreateLandsPrivate(t *testing.T) {
	dir := t.TempDir()
	sp, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	w, err := sp.Create("public.bin")
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(dir, w.Name()))
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm&^0o600 != 0 {
		t.Fatalf("spool file mode %o is wider than 0600", perm)
	}
}

// TestCreateFailsWhenTheDirectoryIsGone: a spool directory that vanished or was
// replaced by a file fails the Create — the error path the engine turns into a
// permanent refusal, never a silent retry.
func TestCreateFailsWhenTheDirectoryIsGone(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "spool")
	sp, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	if _, err := sp.Create("artifact"); err == nil {
		t.Fatal("Create succeeded with the spool directory gone")
	}
	if err := os.WriteFile(dir, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := sp.Create("artifact"); err == nil {
		t.Fatal("Create succeeded with a file where the spool directory was")
	}
}

func TestFreeBytes(t *testing.T) {
	sp, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	free, err := sp.FreeBytes()
	if err != nil {
		t.Fatal(err)
	}
	if free <= 0 {
		t.Fatalf("FreeBytes = %d", free)
	}
}

func TestFreeBytesOnAMissingDirectory(t *testing.T) {
	sp := &Spool{dir: filepath.Join(t.TempDir(), "absent")}
	if _, err := sp.FreeBytes(); err == nil {
		t.Fatal("FreeBytes of a missing directory returned success")
	}
}

// TestNewChecksItsDirectory: an empty path is refused, and a nested one is
// created — a deployment mounts a fresh emptyDir, so the spool may be the first
// thing to write there.
func TestNewChecksItsDirectory(t *testing.T) {
	if _, err := New(""); err == nil {
		t.Fatal("empty dir accepted")
	}
	dir := filepath.Join(t.TempDir(), "a", "b")
	if _, err := New(dir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatal(err)
	}
}

func TestNewFailsWhenTheDirectoryCannotBeCreated(t *testing.T) {
	blocker := filepath.Join(t.TempDir(), "blocker")
	if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := New(filepath.Join(blocker, "sub")); err == nil {
		t.Fatal("a spool directory under a regular file was created")
	}
}

func TestRemoveFailsOnANonEmptyDirectory(t *testing.T) {
	dir := t.TempDir()
	sp, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "d"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "d", "x"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := sp.Remove("d"); err == nil {
		t.Fatal("Remove of a non-empty directory succeeded")
	}
}
