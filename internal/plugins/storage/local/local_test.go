package local_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"encoding/json/jsontext"
	json "encoding/json/v2"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin/plugintest"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage/local"
	"github.com/vi-latyshev/go-cluster-backup/internal/storage"
	"github.com/vi-latyshev/go-cluster-backup/internal/storage/storagetest"
)

func testRegistry(t *testing.T) *plugin.Registry {
	t.Helper()
	r := plugin.NewRegistry()
	if err := local.Register(r); err != nil {
		t.Fatal(err)
	}
	return r
}

func newStorageIn(t *testing.T, dir string) storage.Storage {
	t.Helper()
	raw, err := json.Marshal(map[string]any{"root": dir, "page_size": 3})
	if err != nil {
		t.Fatal(err)
	}
	s, err := plugin.Build(context.Background(), testRegistry(t), storage.Kind, "local", raw,
		plugin.Deps{Instance: "local", Log: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestGeneralConformance(t *testing.T) {
	plugintest.CheckSpec(t, testRegistry(t))
}

func TestStorageConformance(t *testing.T) {
	storagetest.Run(t, func(t *testing.T) storage.Storage {
		return newStorageIn(t, t.TempDir())
	})
}

func TestKeyEscapesRoot(t *testing.T) {
	s := newStorageIn(t, t.TempDir())
	ctx := context.Background()
	for _, key := range []string{"../outside", "/absolute", "a/../../b"} {
		if _, err := s.Stat(ctx, key); err == nil {
			t.Errorf("Stat(%q) accepted an escaping key", key)
		}
		if err := s.Delete(ctx, key); err == nil {
			t.Errorf("Delete(%q) accepted an escaping key", key)
		}
		if err := s.Get(ctx, key, &bytes.Buffer{}); err == nil {
			t.Errorf("Get(%q) accepted an escaping key", key)
		}
	}
}

func TestPutLeavesNoTempFiles(t *testing.T) {
	dir := t.TempDir()
	s := newStorageIn(t, dir)
	body := bytes.Repeat([]byte("x"), 4096)
	if _, err := s.Put(context.Background(), storage.PutRequest{Key: "sub/atomic.bin", Size: int64(len(body))}, bytes.NewReader(body)); err != nil {
		t.Fatal(err)
	}
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasPrefix(d.Name(), ".put-") {
			t.Errorf("temp file left behind: %s", path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestPutRejectsShortBody(t *testing.T) {
	s := newStorageIn(t, t.TempDir())
	_, err := s.Put(context.Background(), storage.PutRequest{Key: "short.bin", Size: 100}, bytes.NewReader([]byte("too short")))
	if err == nil {
		t.Fatal("a body shorter than the declared size was accepted")
	}
}

func TestNewRejectsBadConfig(t *testing.T) {
	r := testRegistry(t)
	for _, raw := range []string{
		`{"root":"relative/path","page_size":10}`,
		`{"root":"","page_size":10}`,
		`{"root":"/tmp","page_size":0}`,
	} {
		_, err := r.BuildAny(context.Background(), "storage", "local", jsontext.Value(raw), plugin.Deps{Instance: "local"})
		if err == nil {
			t.Errorf("config %s accepted", raw)
		}
	}
}

func TestListPagesAndPrefixes(t *testing.T) {
	dir := t.TempDir()
	s := newStorageIn(t, dir)
	ctx := context.Background()
	for _, key := range []string{"a/1", "a/2", "a/3", "a/4", "b/1"} {
		if err := os.MkdirAll(filepath.Join(dir, filepath.Dir(key)), 0o755); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Put(ctx, storage.PutRequest{Key: key, Size: 1}, strings.NewReader("x")); err != nil {
			t.Fatal(err)
		}
	}
	var keys []string
	token := ""
	for {
		page, err := s.List(ctx, storage.ListRequest{Prefix: "a/", Token: token})
		if err != nil {
			t.Fatal(err)
		}
		for _, o := range page.Objects {
			keys = append(keys, o.Key)
		}
		if page.NextToken == "" {
			break
		}
		token = page.NextToken
	}
	if len(keys) != 4 {
		t.Fatalf("paged listing returned %v", keys)
	}
}

func TestCanceledContexts(t *testing.T) {
	s := newStorageIn(t, t.TempDir())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Describe(ctx); err == nil {
		t.Error("Describe")
	}
	if _, err := s.Put(ctx, storage.PutRequest{Key: "k", Size: 1}, strings.NewReader("x")); err == nil {
		t.Error("Put")
	}
	if _, err := s.Stat(ctx, "k"); err == nil {
		t.Error("Stat")
	}
	if _, err := s.List(ctx, storage.ListRequest{}); err == nil {
		t.Error("List")
	}
	if err := s.Delete(ctx, "k"); err == nil {
		t.Error("Delete")
	}
	if err := s.Get(ctx, "k", io.Discard); err == nil {
		t.Error("Get")
	}
}

func TestListRejectsEscapingPrefix(t *testing.T) {
	s := newStorageIn(t, t.TempDir())
	if _, err := s.List(context.Background(), storage.ListRequest{Prefix: "../x"}); err == nil {
		t.Fatal("a prefix with .. was accepted")
	}
	if _, err := s.List(context.Background(), storage.ListRequest{Prefix: "/abs"}); err == nil {
		t.Fatal("an absolute prefix was accepted")
	}
}

func TestGetPropagatesWriterErrors(t *testing.T) {
	s := newStorageIn(t, t.TempDir())
	ctx := context.Background()
	body := bytes.Repeat([]byte("x"), 1024)
	if _, err := s.Put(ctx, storage.PutRequest{Key: "w.bin", Size: int64(len(body))}, bytes.NewReader(body)); err != nil {
		t.Fatal(err)
	}
	if err := s.Get(ctx, "w.bin", failWriter{}); err == nil {
		t.Fatal("a failing destination was ignored")
	}
}

type failWriter struct{}

func (failWriter) Write([]byte) (int, error) { return 0, errors.New("no space left") }

func TestRegisterRejectsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := testRegistry(t)
	if _, err := r.BuildAny(ctx, "storage", "local", jsontext.Value(`{"root":"/tmp","page_size":1}`),
		plugin.Deps{Instance: "local"}); err == nil {
		t.Fatal("a cancelled context built a storage")
	}
}

func TestPutRejectsBadKeys(t *testing.T) {
	s := newStorageIn(t, t.TempDir())
	for _, key := range []string{"", "../x", "/abs", "a/../../b"} {
		if _, err := s.Put(context.Background(), storage.PutRequest{Key: key, Size: 1}, strings.NewReader("x")); err == nil {
			t.Errorf("Put(%q) accepted an invalid key", key)
		}
	}
}

// TestPutFailsWhenTheRootIsAFile: the folder the object lives in cannot be
// created, so the attempt is a transient error rather than a panic.
func TestPutFailsWhenTheRootIsAFile(t *testing.T) {
	root := filepath.Join(t.TempDir(), "rootfile")
	if err := os.WriteFile(root, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	s := newStorageIn(t, root)
	if _, err := s.Put(context.Background(), storage.PutRequest{Key: "k", Size: 1}, strings.NewReader("x")); err == nil {
		t.Fatal("Put under a root that is a file succeeded")
	}
}

// TestPutFailsWhenTheTempFileCannotBeCreated: the folder is writable, but no
// name fits inside it, so the temporary file this Put writes first cannot be
// created. The path is grown one short segment at a time until the platform
// refuses it, so the case holds whether or not the test runs as root.
func TestPutFailsWhenTheTempFileCannotBeCreated(t *testing.T) {
	dir := t.TempDir()
	for range 2000 {
		next := filepath.Join(dir, "abcd")
		if err := os.Mkdir(next, 0o755); err != nil {
			break
		}
		dir = next
	}
	s := newStorageIn(t, dir)
	if _, err := s.Put(context.Background(), storage.PutRequest{Key: "k", Size: 1}, strings.NewReader("x")); err == nil {
		t.Fatal("Put into a folder whose names do not fit succeeded")
	}
}

// TestPutFailsWhenTheKeyIsADirectory: the rename onto the final path fails, and
// the temporary file must not survive it.
func TestPutFailsWhenTheKeyIsADirectory(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "k"), 0o755); err != nil {
		t.Fatal(err)
	}
	s := newStorageIn(t, dir)
	if _, err := s.Put(context.Background(), storage.PutRequest{Key: "k", Size: 1}, strings.NewReader("x")); err == nil {
		t.Fatal("Put onto a directory succeeded")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".put-") {
			t.Errorf("temp file left behind: %s", e.Name())
		}
	}
}

// TestStatUnderAFilePath: a path whose component is a regular file is an IO
// failure, not an absent object.
func TestStatUnderAFilePath(t *testing.T) {
	ctx := context.Background()
	s := newStorageIn(t, t.TempDir())
	if _, err := s.Put(ctx, storage.PutRequest{Key: "a", Size: 1}, strings.NewReader("x")); err != nil {
		t.Fatal(err)
	}
	_, err := s.Stat(ctx, "a/b")
	if err == nil {
		t.Fatal("Stat under a path that is a file succeeded")
	}
	if class, _ := plugin.ClassOf(err); class == plugin.ClassNotFound {
		t.Errorf("a broken path was read as absence: %v", err)
	}
}

func TestGetAbsentAndUnderAFilePath(t *testing.T) {
	ctx := context.Background()
	s := newStorageIn(t, t.TempDir())
	err := s.Get(ctx, "absent.bin", io.Discard)
	if class, _ := plugin.ClassOf(err); class != plugin.ClassNotFound {
		t.Fatalf("Get of an absent key: class %q err %v, want not_found", class, err)
	}
	if _, err := s.Put(ctx, storage.PutRequest{Key: "a", Size: 1}, strings.NewReader("x")); err != nil {
		t.Fatal(err)
	}
	if err := s.Get(ctx, "a/b", io.Discard); err == nil {
		t.Fatal("Get under a path that is a file succeeded")
	}
}

func TestDeleteFailsOnANonEmptyDirectory(t *testing.T) {
	ctx := context.Background()
	s := newStorageIn(t, t.TempDir())
	if _, err := s.Put(ctx, storage.PutRequest{Key: "d/x", Size: 1}, strings.NewReader("x")); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, "d"); err == nil {
		t.Fatal("Delete of a non-empty directory succeeded")
	}
}

// flipContext reports Err() nil for its first after calls and context.Canceled
// afterwards: a context that ends partway through a walk, which no cancel
// function can arrange deterministically from outside the call.
type flipContext struct {
	context.Context
	after int
	calls int
}

func (c *flipContext) Err() error {
	c.calls++
	if c.calls > c.after {
		return context.Canceled
	}
	return nil
}

func TestListWalkEndsWithTheContext(t *testing.T) {
	s := newStorageIn(t, t.TempDir())
	ctx := &flipContext{Context: context.Background(), after: 1}
	_, err := s.List(ctx, storage.ListRequest{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("a listing whose context ended mid-walk: err %v, want the context's own error", err)
	}
}

func TestListWalkReportsAMissingRoot(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "gone")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	s := newStorageIn(t, dir)
	if err := os.Remove(dir); err != nil {
		t.Fatal(err)
	}
	if _, err := s.List(context.Background(), storage.ListRequest{}); err == nil {
		t.Fatal("a listing over a root that vanished returned success")
	}
}

// TestListSkipsAnEntryThatVanishesBeforeTheStat: a dangling symlink is walked
// as a plain entry and fails the stat that builds the page, so it is skipped
// rather than reported with a zero size.
func TestListSkipsAnEntryThatVanishesBeforeTheStat(t *testing.T) {
	dir := t.TempDir()
	s := newStorageIn(t, dir)
	if err := os.Symlink(filepath.Join(dir, "nowhere"), filepath.Join(dir, "dead")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	page, err := s.List(context.Background(), storage.ListRequest{})
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range page.Objects {
		if o.Key == "dead" {
			t.Errorf("a dangling symlink was listed: %+v", o)
		}
	}
}
