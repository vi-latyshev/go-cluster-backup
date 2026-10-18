package files

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
)

// fakeEntry stands in for a directory entry whose file vanished between the
// walk that listed it and the stat that follows — a race no test can provoke
// through the filesystem deterministically.
type fakeEntry struct {
	name string
	err  error
}

func (f fakeEntry) Name() string      { return f.name }
func (f fakeEntry) IsDir() bool       { return false }
func (f fakeEntry) Type() fs.FileMode { return 0 }
func (f fakeEntry) Info() (fs.FileInfo, error) {
	return nil, f.err
}

func TestArchiveEntryVanishedFile(t *testing.T) {
	root := t.TempDir()
	s := &Source{cfg: Config{Root: root, IgnoreVanished: true}}
	r, err := os.OpenRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	vanished := fakeEntry{name: "gone.txt", err: &fs.PathError{Op: "lstat", Path: filepath.Join(root, "gone.txt"), Err: fs.ErrNotExist}}

	var buf []byte
	tw := tar.NewWriter(writeTo(&buf))
	if err := s.archiveEntry(t.Context(), tw, r, filepath.Join(root, "gone.txt"), vanished, map[fileKey]string{}); err != nil {
		t.Fatalf("a vanished file failed an ignore_vanished archive: %v", err)
	}
	if len(buf) != 0 {
		t.Fatalf("a vanished file still wrote %d bytes of headers", len(buf))
	}
	// A tolerated skip is part of the artifact's report: the entry is gone from
	// the stream, and the count says so.
	if s.stats.SkippedEntries != 1 {
		t.Fatalf("skipped = %d, want the vanished entry counted", s.stats.SkippedEntries)
	}

	s.cfg.IgnoreVanished = false
	if err := s.archiveEntry(t.Context(), tw, r, filepath.Join(root, "gone.txt"), vanished, map[fileKey]string{}); err == nil {
		t.Fatal("a vanished file passed an archive that does not tolerate it")
	}
	if s.stats.SkippedEntries != 1 {
		t.Fatalf("skipped = %d, want a failure not to be counted as a skip", s.stats.SkippedEntries)
	}

	denied := fakeEntry{name: "locked.txt", err: &fs.PathError{Op: "lstat", Path: filepath.Join(root, "locked.txt"), Err: fs.ErrPermission}}
	s.cfg.IgnoreVanished = true
	err = s.archiveEntry(t.Context(), tw, r, filepath.Join(root, "locked.txt"), denied, map[fileKey]string{})
	if err == nil {
		t.Fatal("a permission failure was treated as the tolerated delete race")
	}
	if !errors.Is(err, fs.ErrPermission) {
		t.Fatalf("error = %v, want it to carry the permission failure", err)
	}
}

type byteWriter struct{ buf *[]byte }

func (w byteWriter) Write(p []byte) (int, error) {
	*w.buf = append(*w.buf, p...)
	return len(p), nil
}

func writeTo(buf *[]byte) io.Writer { return byteWriter{buf: buf} }

// swapCarried replaces the path with something else, the way a workload owning
// the volume can between the walk's lstat and the open.
func swapCarried(t *testing.T, path string, make func() error) fs.FileInfo {
	t.Helper()
	walked, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := make(); err != nil {
		t.Fatal(err)
	}
	return walked
}

// TestOpenCarriedRefusesWhatChangedUnderTheWalk: the run reads volumes other
// people write, as root. A FIFO swapped over a regular file must not block this
// process until the job's deadline — no context can interrupt a blocked open —
// and a link swapped in must not be followed out of the volume, where the
// storage token, the discovered secrets and the ServiceAccount token live.
func TestOpenCarriedRefusesWhatChangedUnderTheWalk(t *testing.T) {
	root := t.TempDir()
	r, err := os.OpenRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()

	t.Run("a fifo must not block the read", func(t *testing.T) {
		path := filepath.Join(root, "payload")
		if err := os.WriteFile(path, []byte("data"), 0o644); err != nil {
			t.Fatal(err)
		}
		walked := swapCarried(t, path, func() error { return syscall.Mkfifo(path, 0o644) })

		done := make(chan error, 1)
		go func() {
			f, err := openCarried(r, "payload", walked)
			if f != nil {
				_ = f.Close()
			}
			done <- err
		}()
		select {
		case err := <-done:
			if err == nil || !errors.Is(err, errChanged) {
				t.Fatalf("openCarried = %v, want the entry refused as changed", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("openCarried is still blocked on a FIFO: the run would hang to the job's deadline")
		}
	})

	t.Run("a link out of the volume must not be followed", func(t *testing.T) {
		secret := filepath.Join(t.TempDir(), "token")
		if err := os.WriteFile(secret, []byte("the storage token"), 0o600); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(root, "link")
		if err := os.WriteFile(path, []byte("data"), 0o644); err != nil {
			t.Fatal(err)
		}
		walked := swapCarried(t, path, func() error { return os.Symlink(secret, path) })

		f, err := openCarried(r, "link", walked)
		if f != nil {
			_ = f.Close()
		}
		if err == nil || !errors.Is(err, errChanged) {
			t.Fatalf("openCarried = %v, want the link refused", err)
		}
	})

	t.Run("another regular file must not be copied in its place", func(t *testing.T) {
		path := filepath.Join(root, "swapped")
		if err := os.WriteFile(path, []byte("data"), 0o644); err != nil {
			t.Fatal(err)
		}
		walked := swapCarried(t, path, func() error { return os.WriteFile(path, []byte("other data"), 0o644) })

		f, err := openCarried(r, "swapped", walked)
		if f != nil {
			_ = f.Close()
		}
		if err == nil || !errors.Is(err, errChanged) {
			t.Fatalf("openCarried = %v, want the replacement refused", err)
		}
	})

	t.Run("the file the walk saw opens", func(t *testing.T) {
		path := filepath.Join(root, "kept")
		if err := os.WriteFile(path, []byte("data"), 0o644); err != nil {
			t.Fatal(err)
		}
		walked, err := os.Lstat(path)
		if err != nil {
			t.Fatal(err)
		}
		f, err := openCarried(r, "kept", walked)
		if err != nil {
			t.Fatalf("openCarried = %v, want the file the walk saw", err)
		}
		defer func() { _ = f.Close() }()
		if body, err := io.ReadAll(f); err != nil || string(body) != "data" {
			t.Fatalf("read %q, %v", body, err)
		}
	})
}

// statInfo is an fs.FileInfo with a chosen identity, size and mtime. It lets the
// identity check be exercised where a filesystem cannot be made to hand a freed
// inode number to a replacement on demand.
type statInfo struct {
	sys     *syscall.Stat_t
	size    int64
	modTime time.Time
}

func (i statInfo) Name() string       { return "x" }
func (i statInfo) Size() int64        { return i.size }
func (i statInfo) Mode() fs.FileMode  { return 0 }
func (i statInfo) ModTime() time.Time { return i.modTime }
func (i statInfo) IsDir() bool        { return false }
func (i statInfo) Sys() any           { return i.sys }

// TestSameAsWalkedRefusesAnInodeReusedReplacement pins what the swap test below
// only catches when the filesystem happens to reuse the inode: dev+ino alone
// accept a different file that landed on the walked inode, and the size and the
// modification time are what refuse it. On Linux unlink and re-create really do
// hand the freed inode number to the replacement, which is the shape here. Each
// field of the identity is varied on its own so removing any one comparison
// fails a case.
func TestSameAsWalkedRefusesAnInodeReusedReplacement(t *testing.T) {
	base := time.Unix(1_700_000_000, 123)
	// newID is the walked identity: one device, one inode, one change time.
	// Every case below varies exactly one field of it, so each comparison in the
	// identity has a case that fails when that comparison is removed.
	newID := func() *syscall.Stat_t {
		st := &syscall.Stat_t{Dev: 7, Ino: 42}
		setChangeTime(st, 100, 1)
		return st
	}
	walked := statInfo{sys: newID(), size: 4, modTime: base}

	if !sameAsWalked(statInfo{sys: newID(), size: 4, modTime: base}, walked) {
		t.Fatal("the very file the walk saw was refused")
	}

	grown := newID()
	if sameAsWalked(statInfo{sys: grown, size: 5, modTime: base}, walked) {
		t.Fatal("a same-inode file of a different size was accepted")
	}
	if sameAsWalked(statInfo{sys: newID(), size: 4, modTime: base.Add(time.Millisecond)}, walked) {
		t.Fatal("a same-inode file rewritten in place was accepted")
	}

	otherDevice := newID()
	otherDevice.Dev = 8
	if sameAsWalked(statInfo{sys: otherDevice, size: 4, modTime: base}, walked) {
		t.Fatal("a file of another device was accepted")
	}
	otherInode := newID()
	otherInode.Ino = 43
	if sameAsWalked(statInfo{sys: otherInode, size: 4, modTime: base}, walked) {
		t.Fatal("a file of another inode was accepted")
	}
	otherChange := newID()
	setChangeTime(otherChange, 101, 1)
	if sameAsWalked(statInfo{sys: otherChange, size: 4, modTime: base}, walked) {
		t.Fatal("a file whose change time moved was accepted")
	}
}

// TestDumpRefusesWhatChangedUnderTheWalk binds the swap fix to the call deployment
// that must run it: the entry the walk looked at is opened by the very handle
// openCarried hands back, and a plain open would read whatever stands at the
// path now. The swap below happens in the one window a test can reach — after
// the walk looked at the entry, before the dump opens it — which is exactly the
// window a workload owning the volume writes into.
func TestDumpRefusesWhatChangedUnderTheWalk(t *testing.T) {
	// outside holds bytes the artifact must never carry: it stands in for the
	// storage token, the discovered secrets and the ServiceAccount token a
	// volume's workload can reach by a link it plants.
	outside := filepath.Join(t.TempDir(), "outside")
	if err := os.WriteFile(outside, []byte("FOREIGN-BYTES"), 0o600); err != nil {
		t.Fatal(err)
	}

	cases := map[string]func(t *testing.T, path string){
		"swapped for a link out of the volume": func(t *testing.T, path string) {
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(outside, path); err != nil {
				t.Fatal(err)
			}
		},
		"swapped for a fifo": func(t *testing.T, path string) {
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			if err := syscall.Mkfifo(path, 0o644); err != nil {
				t.Skipf("fifo not creatable here: %v", err)
			}
		},
	}
	for name, swap := range cases {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			target := filepath.Join(root, "payload")
			if err := os.WriteFile(target, []byte("the volume's own bytes"), 0o644); err != nil {
				t.Fatal(err)
			}
			s := &Source{cfg: Config{Root: root, IgnoreVanished: true}, log: slog.New(slog.DiscardHandler)}
			swapped := false
			s.testBeforeOpen = func(path string) {
				if path != target || swapped {
					return
				}
				swapped = true
				swap(t, path)
			}

			var buf []byte
			done := make(chan error, 1)
			go func() { done <- s.Dump(t.Context(), writeTo(&buf)) }()
			var err error
			select {
			case err = <-done:
			case <-time.After(15 * time.Second):
				// A plain open of a fifo with no writer blocks until the job's
				// deadline: the artifact is never written, and this is what the
				// fix exists to prevent.
				t.Fatal("Dump blocked on the entry it was opening")
			}
			if err != nil {
				t.Fatalf("Dump: %v", err)
			}
			if !swapped {
				t.Fatal("the entry was never swapped, so this proves nothing")
			}
			stats, err := s.DumpStats(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if stats.SkippedEntries != 1 {
				t.Errorf("skipped = %d, want the changed entry left out and counted", stats.SkippedEntries)
			}
			if bytes.Contains(buf, []byte("FOREIGN-BYTES")) {
				t.Errorf("the artifact carries bytes from outside the volume, so the open followed the swap")
			}
			assertNoEntry(t, buf, "payload")
		})
	}
}

// assertNoEntry fails when the archive holds an entry of that name.
func assertNoEntry(t *testing.T, archive []byte, name string) {
	t.Helper()
	tr := tar.NewReader(bytes.NewReader(archive))
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return
		}
		if err != nil {
			t.Fatalf("the archive does not read back: %v", err)
		}
		if hdr.Name == name {
			t.Errorf("the archive carries %q, want the changed entry left out", name)
			return
		}
	}
}

// tarStream builds a raw tar archive from headers and bodies.
func tarStream(t *testing.T, headers []tar.Header, bodies []string) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for i, hdr := range headers {
		hdr := hdr
		if hdr.Typeflag == tar.TypeReg {
			hdr.Size = int64(len(bodies[i]))
		}
		if err := tw.WriteHeader(&hdr); err != nil {
			t.Fatal(err)
		}
		if hdr.Typeflag == tar.TypeReg {
			if _, err := tw.Write([]byte(bodies[i])); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

type failWriter struct{}

func (failWriter) Write([]byte) (int, error) { return 0, errors.New("injected write failure") }

type errorReader struct{}

func (errorReader) Read([]byte) (int, error) { return 0, errors.New("injected read failure") }

// TestDumpReturnsOnCancellationDuringTheWalk: the walk checks the context at
// every entry, so a cancelled run stops at the next one instead of archiving
// the whole tree.
func TestDumpReturnsOnCancellationDuringTheWalk(t *testing.T) {
	root := t.TempDir()
	// The first entry is empty, so its copy finishes without a further context
	// check and the walk reaches the next entry to find the context done.
	if err := os.WriteFile(filepath.Join(root, "a.txt"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "b.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &Source{cfg: Config{Root: root, IgnoreVanished: true}, log: slog.New(slog.DiscardHandler)}
	s.testBeforeOpen = func(string) { cancel() }
	if err := s.Dump(ctx, io.Discard); !errors.Is(err, context.Canceled) {
		t.Fatalf("Dump = %v, want context.Canceled", err)
	}
}

// TestDumpReportsAHeaderWriteFailure: the header of the first regular file is
// written before its body, and a failing destination fails the archive there.
func TestDumpReportsAHeaderWriteFailure(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	s := &Source{cfg: Config{Root: root, IgnoreVanished: true}, log: slog.New(slog.DiscardHandler)}
	if err := s.Dump(t.Context(), failWriter{}); err == nil {
		t.Fatal("Dump reported success into a failing writer")
	}
}

// failAfterWriter accepts its first ok non-empty writes and fails the rest. An
// empty write is the tar writer's own Flush with nothing buffered, never data,
// so it is not counted. A non-empty path is truncated on the first counted
// write: the shape of a live file shrinking under the read.
type failAfterWriter struct {
	path string
	ok   int
	n    int
}

func (w *failAfterWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	w.n++
	if w.n == 1 && w.path != "" {
		if err := os.Truncate(w.path, 0); err != nil {
			return 0, err
		}
	}
	if w.n <= w.ok {
		return len(p), nil
	}
	return 0, errors.New("injected write failure")
}

// TestDumpReportsABodyWriteFailure: a destination that accepts the header and
// fails during the body stops the copy of that entry.
func TestDumpReportsABodyWriteFailure(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a.txt"), bytes.Repeat([]byte("a"), 4<<10), 0o644); err != nil {
		t.Fatal(err)
	}
	s := &Source{cfg: Config{Root: root, IgnoreVanished: true}, log: slog.New(slog.DiscardHandler)}
	if err := s.Dump(t.Context(), &failAfterWriter{ok: 1}); err == nil {
		t.Fatal("Dump reported success when the body could not be written")
	}
}

// TestDumpReportsAPadWriteFailure: a file that shrank under the read is
// zero-padded, and a destination that fails during the padding fails the
// archive rather than writing a short entry.
func TestDumpReportsAPadWriteFailure(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "log.txt")
	if err := os.WriteFile(target, bytes.Repeat([]byte("a"), 4<<10), 0o644); err != nil {
		t.Fatal(err)
	}
	s := &Source{cfg: Config{Root: root, IgnoreVanished: true}, log: slog.New(slog.DiscardHandler)}
	if err := s.Dump(t.Context(), &failAfterWriter{ok: 1, path: target}); err == nil {
		t.Fatal("Dump reported success when the padding could not be written")
	}
}

// TestDumpToleratesADirectoryVanishingUnderTheWalk: WalkDir lists a directory
// and reads it afterwards; one deleted in between is the tolerated race when
// ignore_vanished says so, and it is counted rather than failing the archive.
func TestDumpToleratesADirectoryVanishingUnderTheWalk(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	victim := filepath.Join(root, "zsub")
	if err := os.Mkdir(victim, 0o755); err != nil {
		t.Fatal(err)
	}
	s := &Source{cfg: Config{Root: root, IgnoreVanished: true}, log: slog.New(slog.DiscardHandler)}
	removed := false
	s.testBeforeOpen = func(string) {
		if removed {
			return
		}
		removed = true
		if err := os.Remove(victim); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Dump(t.Context(), io.Discard); err != nil {
		t.Fatalf("a directory that vanished under the walk failed an ignore_vanished archive: %v", err)
	}
	if !removed {
		t.Fatal("the directory was never removed, so this proves nothing")
	}
	// WalkDir reports the directory twice: once from the listing, once from the
	// read that failed. Both are the same tolerated race, and both are counted.
	if s.stats.SkippedEntries != 2 {
		t.Fatalf("skipped = %d, want the vanished directory counted on both reports", s.stats.SkippedEntries)
	}
}

// TestDumpFailsWhenADirectoryCannotBeRead: WalkDir lists a directory and then
// reads it; a read that fails for a reason other than a delete is a fault of
// the tree, never a tolerated skip, whatever ignore_vanished says.
func TestDumpFailsWhenADirectoryCannotBeRead(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root reads anything; the permission failure cannot be provoked")
	}
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	locked := filepath.Join(root, "zsub")
	if err := os.Mkdir(locked, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(locked, 0o700) })
	s := &Source{cfg: Config{Root: root, IgnoreVanished: true}, log: slog.New(slog.DiscardHandler)}
	err := s.Dump(t.Context(), io.Discard)
	if err == nil {
		t.Fatal("a directory that could not be read passed the archive")
	}
	if !errors.Is(err, fs.ErrPermission) {
		t.Fatalf("Dump = %v, want it to carry the read failure", err)
	}
}

// TestDumpToleratesAFileVanishingBeforeItsOpen: the window between the walk's
// look and the open is the tolerated delete race, and the entry is counted.
func TestDumpToleratesAFileVanishingBeforeItsOpen(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "a.txt")
	if err := os.WriteFile(target, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	s := &Source{cfg: Config{Root: root, IgnoreVanished: true}, log: slog.New(slog.DiscardHandler)}
	removed := false
	s.testBeforeOpen = func(path string) {
		if path != target || removed {
			return
		}
		removed = true
		if err := os.Remove(target); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Dump(t.Context(), io.Discard); err != nil {
		t.Fatalf("a file that vanished before its open failed an ignore_vanished archive: %v", err)
	}
	if !removed {
		t.Fatal("the file was never removed, so this proves nothing")
	}
	if s.stats.SkippedEntries != 1 {
		t.Fatalf("skipped = %d, want the vanished file counted", s.stats.SkippedEntries)
	}
}

// TestEntryRefusalRejectsWhatApplyCannotCarry: the one rule both directions
// share, checked on the shapes a stream can carry and this package cannot.
func TestEntryRefusalRejectsWhatApplyCannotCarry(t *testing.T) {
	refused := map[string]*tar.Header{
		"empty name":            {Name: "", Typeflag: tar.TypeReg},
		"NUL in name":           {Name: "a\x00b", Typeflag: tar.TypeReg},
		"empty symlink target":  {Name: "l", Typeflag: tar.TypeSymlink, Linkname: ""},
		"NUL in symlink target": {Name: "l", Typeflag: tar.TypeSymlink, Linkname: "a\x00b"},
		"unsupported type":      {Name: "p", Typeflag: tar.TypeFifo},
	}
	for name, hdr := range refused {
		if why := entryRefusal(hdr); why == "" {
			t.Errorf("%s was carried by the rule", name)
		}
	}
	if why := entryRefusal(&tar.Header{Name: "ok.txt", Typeflag: tar.TypeReg}); why != "" {
		t.Errorf("a plain file was refused: %s", why)
	}
}

// extractRoot opens a restore root for the internal tests.
func extractRoot(t *testing.T, dir string) *os.Root {
	t.Helper()
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = root.Close() })
	return root
}

// withSeam swaps one filesystem primitive for a failing one and restores it.
func withSeam[T any](t *testing.T, slot *T, fake T) {
	t.Helper()
	orig := *slot
	*slot = fake
	t.Cleanup(func() { *slot = orig })
}

func TestExtractReturnsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := extract(ctx, extractRoot(t, t.TempDir()), bytes.NewReader(nil), false); !errors.Is(err, context.Canceled) {
		t.Fatalf("extract = %v, want context.Canceled", err)
	}
}

func TestExtractRejectsAGarbageStream(t *testing.T) {
	if _, err := extract(context.Background(), extractRoot(t, t.TempDir()), strings.NewReader("not a tar stream"), false); err == nil {
		t.Fatal("extract accepted a stream that is not a tar")
	}
}

// TestExtractRefusesAnUnsupportedEntryType: Probe refuses such an entry first
// in the real flow, so this is extract's own guard.
func TestExtractRefusesAnUnsupportedEntryType(t *testing.T) {
	archive := tarStream(t, []tar.Header{{Name: "pipe", Typeflag: tar.TypeFifo, Mode: 0o644}}, []string{""})
	if _, err := extract(context.Background(), extractRoot(t, t.TempDir()), bytes.NewReader(archive), false); err == nil {
		t.Fatal("extract carried an entry type it cannot materialise")
	}
}

func TestExtractFailsOnATruncatedBody(t *testing.T) {
	body := strings.Repeat("x", 2000)
	archive := tarStream(t, []tar.Header{{Name: "f.txt", Typeflag: tar.TypeReg, Mode: 0o644}}, []string{body})
	dir := t.TempDir()
	if _, err := extract(context.Background(), extractRoot(t, dir), bytes.NewReader(archive[:len(archive)-1100]), false); err == nil {
		t.Fatal("extract reported success on a truncated body")
	}
}

// TestProbeRefusesAnEmptyArtifact: a tar stream always carries its
// end-of-archive marker, so a zero-byte artifact is a truncated file, never an
// empty tree. The marker alone is a valid empty archive.
func TestProbeRefusesAnEmptyArtifact(t *testing.T) {
	s := &Source{cfg: Config{Root: t.TempDir()}, log: slog.New(slog.DiscardHandler)}
	_, err := s.Probe(t.Context(), bytes.NewReader(nil))
	if err == nil {
		t.Fatal("Probe accepted a zero-byte artifact")
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassPermanent {
		t.Fatalf("class = %q, want permanent (%v)", class, err)
	}
	if _, err := s.Probe(t.Context(), bytes.NewReader(make([]byte, 1024))); err != nil {
		t.Fatalf("Probe refused a valid empty archive: %v", err)
	}
}

// noSysInfo is an fs.FileInfo whose Sys is not a unix stat — the shape a
// filesystem without syscall.Stat_t hands back.
type noSysInfo struct{}

func (noSysInfo) Name() string       { return "x" }
func (noSysInfo) Size() int64        { return 0 }
func (noSysInfo) Mode() fs.FileMode  { return 0 }
func (noSysInfo) ModTime() time.Time { return time.Time{} }
func (noSysInfo) IsDir() bool        { return false }
func (noSysInfo) Sys() any           { return nil }

func TestFileKeyAndLinksFallBackWithoutUnixStat(t *testing.T) {
	if k := fileKeyOf(noSysInfo{}); k != (fileKey{}) {
		t.Fatalf("fileKeyOf without a unix stat = %+v, want the zero key", k)
	}
	if hasLinks(noSysInfo{}) {
		t.Fatal("hasLinks claimed links without a unix stat")
	}
}

func TestCopyNStopsOnCancellationWriteErrorAndReadError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := copyN(ctx, io.Discard, strings.NewReader("x"), 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled copyN = %v, want context.Canceled", err)
	}
	if _, err := copyN(context.Background(), failWriter{}, strings.NewReader("x"), 1); err == nil {
		t.Fatal("copyN reported success into a failing writer")
	}
	if _, err := copyN(context.Background(), io.Discard, errorReader{}, 1); err == nil {
		t.Fatal("copyN reported success over a failing reader")
	}
}

func TestPadZeroesReportsAWriteFailure(t *testing.T) {
	if err := padZeroes(failWriter{}, 1); err == nil {
		t.Fatal("padZeroes reported success into a failing writer")
	}
}

// TestOpenCarriedReportsAHandleItCannotAsk: the open succeeded but the
// descriptor cannot be interrogated, and the entry fails with that reason.
func TestOpenCarriedReportsAHandleItCannotAsk(t *testing.T) {
	withSeam(t, &statOpenFile, func(*os.File) (fs.FileInfo, error) {
		return nil, errors.New("injected stat failure")
	})
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	s := &Source{cfg: Config{Root: root, IgnoreVanished: true}, log: slog.New(slog.DiscardHandler)}
	if err := s.Dump(t.Context(), io.Discard); err == nil {
		t.Fatal("a descriptor that could not be interrogated passed the archive")
	}
}

// TestArchiveEntryReportsASymlinkItCannotRead: a symlink the walk listed but
// whose target cannot be read is the tolerated delete race when it vanished,
// and the entry's own failure otherwise.
func TestArchiveEntryReportsASymlinkItCannotRead(t *testing.T) {
	root := t.TempDir()
	if err := os.Symlink("nowhere", filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}

	withSeam(t, &readlink, func(string) (string, error) { return "", fs.ErrNotExist })
	skipping := &Source{cfg: Config{Root: root, IgnoreVanished: true}, log: slog.New(slog.DiscardHandler)}
	if err := skipping.Dump(t.Context(), io.Discard); err != nil {
		t.Fatalf("a vanished symlink failed an ignore_vanished archive: %v", err)
	}
	if skipping.stats.SkippedEntries != 1 {
		t.Fatalf("skipped = %d, want the vanished symlink counted", skipping.stats.SkippedEntries)
	}

	withSeam(t, &readlink, func(string) (string, error) { return "", errors.New("injected readlink failure") })
	failing := &Source{cfg: Config{Root: root, IgnoreVanished: true}, log: slog.New(slog.DiscardHandler)}
	if err := failing.Dump(t.Context(), io.Discard); err == nil {
		t.Fatal("a symlink that could not be read passed the archive")
	}
}

// TestExtractReportsAModeItCannotSet: the umask must not decide the restored
// mode, so the working mode and the final mode of a directory, and a file's
// own mode, each refuse when they cannot be set.
func TestExtractReportsAModeItCannotSet(t *testing.T) {
	dirArchive := tarStream(t, []tar.Header{{Name: "d/", Typeflag: tar.TypeDir, Mode: 0o555}}, []string{""})
	fileArchive := tarStream(t, []tar.Header{{Name: "f.txt", Typeflag: tar.TypeReg, Mode: 0o644}}, []string{"x"})
	always := func(*os.Root, string, fs.FileMode) error { return errors.New("injected chmod failure") }

	t.Run("the working mode of a directory", func(t *testing.T) {
		withSeam(t, &chmodRoot, always)
		if _, err := extract(context.Background(), extractRoot(t, t.TempDir()), bytes.NewReader(dirArchive), false); err == nil {
			t.Fatal("a directory whose working mode could not be set was restored")
		}
	})
	t.Run("the final mode of a directory", func(t *testing.T) {
		calls := 0
		withSeam(t, &chmodRoot, func(r *os.Root, name string, mode fs.FileMode) error {
			calls++
			if calls == 1 {
				return r.Chmod(name, mode)
			}
			return errors.New("injected chmod failure")
		})
		if _, err := extract(context.Background(), extractRoot(t, t.TempDir()), bytes.NewReader(dirArchive), false); err == nil {
			t.Fatal("a directory whose final mode could not be set was restored")
		}
	})
	t.Run("a file", func(t *testing.T) {
		withSeam(t, &chmodRoot, always)
		if _, err := extract(context.Background(), extractRoot(t, t.TempDir()), bytes.NewReader(fileArchive), false); err == nil {
			t.Fatal("a file whose mode could not be set was restored")
		}
	})
}

// TestExtractReportsADirectoryOwnerItCannotRestore: a directory's owner lands
// after its children, and a refusal is a permanent fault, not a retry.
func TestExtractReportsADirectoryOwnerItCannotRestore(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root chowns anything; the refusal cannot be provoked")
	}
	archive := tarStream(t, []tar.Header{
		{Name: "d/", Typeflag: tar.TypeDir, Mode: 0o755, Uid: 1, Gid: 1},
	}, []string{""})
	if _, err := extract(context.Background(), extractRoot(t, t.TempDir()), bytes.NewReader(archive), true); err == nil {
		t.Fatal("a directory restored to another owner was accepted without the privilege")
	}
}
