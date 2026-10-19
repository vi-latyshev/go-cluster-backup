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
		f, err := sp.Open("a-01.bin")
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
	if err := sp.Remove("a-01.bin"); err != nil {
		t.Fatal(err)
	}
	if _, err := sp.Open("a-01.bin"); err == nil {
		t.Fatal("removed file still opens")
	}
	if err := sp.Remove("a-01.bin"); err != nil {
		t.Fatalf("removing an absent file must be fine: %v", err)
	}
}

func TestCreateTruncatesOnRetry(t *testing.T) {
	sp, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	w, err := sp.Create("retry.bin")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("first attempt, longer")); err != nil {
		t.Fatal(err)
	}
	_ = w.Close()
	w2, err := sp.Create("retry.bin")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w2.Write([]byte("second")); err != nil {
		t.Fatal(err)
	}
	_ = w2.Close()
	f, err := sp.Open("retry.bin")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			t.Error(err)
		}
	}()
	got, _ := io.ReadAll(f)
	if string(got) != "second" {
		t.Fatalf("retry left %q", got)
	}
}

func TestInvalidNames(t *testing.T) {
	sp, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"", "../escape", "a/b", "..", ".hidden/../../x"} {
		if _, err := sp.Create(name); err == nil {
			t.Errorf("Create(%q) accepted", name)
		}
		if _, err := sp.Open(name); err == nil {
			t.Errorf("Open(%q) accepted", name)
		}
		if err := sp.Remove(name); err == nil {
			t.Errorf("Remove(%q) accepted", name)
		}
	}
}

func TestNextNameSanitizes(t *testing.T) {
	sp, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	got := sp.NextName("databases/ns/postgres.dump")
	if strings.ContainsAny(got, "/\\") {
		t.Fatalf("NextName produced a path: %q", got)
	}
	if !strings.HasPrefix(got, "001-") {
		t.Fatalf("NextName = %q", got)
	}
	second := sp.NextName("x")
	if !strings.HasPrefix(second, "002-") {
		t.Fatalf("NextName = %q", second)
	}
	if empty := sp.NextName("..."); !strings.Contains(empty, "artifact") {
		t.Fatalf("NextName of a fully sanitized name = %q", empty)
	}
	long := sp.NextName(strings.Repeat("x", 500))
	if len(long) > 200 {
		t.Fatalf("long name not truncated: %d", len(long))
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

func TestNewRejectsEmptyDir(t *testing.T) {
	if _, err := New(""); err == nil {
		t.Fatal("empty dir accepted")
	}
}

func TestNewCreatesNestedDir(t *testing.T) {
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

func TestFreeBytesOnAMissingDirectory(t *testing.T) {
	sp := &Spool{dir: filepath.Join(t.TempDir(), "absent")}
	if _, err := sp.FreeBytes(); err == nil {
		t.Fatal("FreeBytes of a missing directory returned success")
	}
}

func TestCreateFailsOverADirectory(t *testing.T) {
	dir := t.TempDir()
	sp, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "d"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := sp.Create("d"); err == nil {
		t.Fatal("Create over a directory succeeded")
	}
}

func TestWriterNameIsTheRequestedName(t *testing.T) {
	sp, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	w, err := sp.Create("named.bin")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := w.Close(); err != nil {
			t.Error(err)
		}
	}()
	if w.Name() != "named.bin" {
		t.Fatalf("Name = %q", w.Name())
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
