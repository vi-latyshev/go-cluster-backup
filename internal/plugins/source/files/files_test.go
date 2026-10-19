package files_test

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin/plugintest"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source/files"
	"github.com/vi-latyshev/go-cluster-backup/internal/source"
	"github.com/vi-latyshev/go-cluster-backup/internal/source/sourcetest"
)

func testRegistry(t *testing.T) *plugin.Registry {
	t.Helper()
	r := plugin.NewRegistry()
	if err := files.Register(r); err != nil {
		t.Fatal(err)
	}
	return r
}

func build(t *testing.T, cfg string) (source.Source, error) {
	t.Helper()
	return buildLogged(t, cfg, slog.New(slog.DiscardHandler))
}

func buildLogged(t *testing.T, cfg string, log *slog.Logger) (source.Source, error) {
	t.Helper()
	return plugin.Build(context.Background(), testRegistry(t), source.Kind, "files",
		jsontext.Value(cfg), plugin.Deps{Instance: "files", Log: log})
}

func config(root string, ignoreVanished bool) string {
	return fmt.Sprintf(`{"root": %q, "ignore_vanished": %t}`, root, ignoreVanished)
}

// tree writes a small tree: a file, a subdirectory with a file and a symlink.
func tree(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "top.txt"), []byte("top"), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "sub", "nested.txt"), []byte("nested"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("top.txt", filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	return root
}

// unlockTree registers a cleanup that makes a tree a restore left read-only
// deletable again: WalkDir visits a directory before it reads it, so giving
// every directory owner write+execute there lets t.TempDir's RemoveAll unlink
// its children. It must be registered before the restore — a restore that fails
// halfway still runs cleanups — and a path that never appeared is skipped.
func unlockTree(t *testing.T, root string) {
	t.Helper()
	t.Cleanup(func() { unlockTreeNow(root) })
}

// unlockTreeNow walks root and gives every directory owner write+execute and
// every regular file owner write, tolerating a root that does not exist.
func unlockTreeNow(root string) {
	_ = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		switch {
		case d.IsDir():
			_ = os.Chmod(path, 0o700)
		case d.Type().IsRegular():
			_ = os.Chmod(path, 0o600)
		}
		return nil
	})
}

func TestGeneralConformance(t *testing.T) {
	plugintest.CheckSpec(t, testRegistry(t))
}

func TestSourceConformance(t *testing.T) {
	sourcetest.Run(t, func(t *testing.T) source.Source {
		s, err := build(t, config(tree(t), true))
		if err != nil {
			t.Fatal(err)
		}
		return s
	})
}

func TestDescribe(t *testing.T) {
	root := tree(t)
	s, err := build(t, config(root, true))
	if err != nil {
		t.Fatal(err)
	}
	tr, err := s.Describe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if tr.Ext != ".tar" || tr.Origin != "file://"+root || tr.Sealed {
		t.Fatalf("traits = %+v", tr)
	}
}

func dump(t *testing.T, root string, ignoreVanished bool) []*tar.Header {
	t.Helper()
	s, err := build(t, config(root, ignoreVanished))
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := s.Dump(context.Background(), &buf); err != nil {
		t.Fatal(err)
	}
	tr := tar.NewReader(&buf)
	var headers []*tar.Header
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return headers
		}
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.Copy(io.Discard, tr); err != nil {
			t.Fatal(err)
		}
		h := *hdr
		headers = append(headers, &h)
	}
}

func TestDumpCarriesTheTree(t *testing.T) {
	headers := dump(t, tree(t), true)
	byName := map[string]*tar.Header{}
	for _, h := range headers {
		byName[h.Name] = h
	}
	names := make([]string, 0, len(headers))
	for _, h := range headers {
		names = append(names, h.Name)
	}
	for _, want := range []string{"link", "sub/", "sub/nested.txt", "top.txt"} {
		if byName[want] == nil {
			t.Fatalf("entry %q missing from %v", want, names)
		}
	}
	if h := byName["top.txt"]; h.Typeflag != tar.TypeReg || h.Mode != 0o640 {
		t.Fatalf("top.txt header = %+v", h)
	}
	if h := byName["link"]; h.Typeflag != tar.TypeSymlink || h.Linkname != "top.txt" {
		t.Fatalf("link header = %+v", h)
	}
	if h := byName["sub/"]; h.Typeflag != tar.TypeDir {
		t.Fatalf("sub header = %+v", h)
	}
	if h := byName["top.txt"]; h.Uid != os.Getuid() {
		t.Fatalf("top.txt uid = %d, want %d", h.Uid, os.Getuid())
	}
}

func TestDumpStoresHardLinkedFileOnce(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a.txt"), []byte("linked"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(filepath.Join(root, "a.txt"), filepath.Join(root, "b.txt")); err != nil {
		t.Fatal(err)
	}
	headers := dump(t, root, true)
	var links int
	var regular []string
	for _, h := range headers {
		switch h.Typeflag {
		case tar.TypeLink:
			links++
			if h.Linkname != "a.txt" && h.Linkname != "b.txt" {
				t.Fatalf("link target = %q", h.Linkname)
			}
		case tar.TypeReg:
			regular = append(regular, h.Name)
		}
	}
	if links != 1 || len(regular) != 1 {
		t.Fatalf("links = %d, regular = %v", links, regular)
	}
}

func TestDumpFailsOnAnUnreadableFile(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "secret.txt")
	if err := os.WriteFile(path, []byte("x"), 0o000); err != nil {
		t.Fatal(err)
	}
	unlockTree(t, root)
	if os.Getuid() == 0 {
		t.Skip("root reads anything; the permission failure cannot be provoked")
	}
	s, err := build(t, config(root, true))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Dump(context.Background(), io.Discard); err == nil {
		t.Fatal("an unreadable file did not fail the archive")
	}
}

func TestCheckRefusesMissingRoot(t *testing.T) {
	s, err := build(t, config(filepath.Join(t.TempDir(), "gone"), true))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.(plugin.Checker).Check(context.Background()); err == nil {
		t.Fatal("a missing root passed Check")
	}
	ok, err := build(t, config(tree(t), true))
	if err != nil {
		t.Fatal(err)
	}
	if err := ok.(plugin.Checker).Check(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestNewRejectsBadConfig(t *testing.T) {
	for _, cfg := range []string{
		`{"root": "", "ignore_vanished": true}`,
		`{"root": "relative", "ignore_vanished": true}`,
	} {
		if _, err := build(t, cfg); err == nil {
			t.Errorf("config %s accepted", cfg)
		}
	}
}

// applyTar builds an archive from raw headers and bodies.
func applyTar(t *testing.T, entries []tar.Header, bodies []string) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for i, hdr := range entries {
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

const applyOptions = `{"preserve_owner": false, "side_by_side_dir": ".restore-{{ .Label }}"}`

func TestApplySideBySide(t *testing.T) {
	root := tree(t)
	s, err := build(t, config(root, true))
	if err != nil {
		t.Fatal(err)
	}
	archive := applyTar(t, []tar.Header{
		{Name: "dir/", Typeflag: tar.TypeDir, Mode: 0o755},
		{Name: "dir/file.txt", Typeflag: tar.TypeReg, Mode: 0o640},
		{Name: "dir/link", Typeflag: tar.TypeSymlink, Linkname: "file.txt"},
	}, []string{"", "content", ""})
	rep, err := s.(source.Restorer).Apply(context.Background(), source.ApplyRequest{
		Mode: source.ApplySideBySide, Label: "20261012T042000Z", Options: jsontext.Value(applyOptions),
	}, bytes.NewReader(archive))
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(root, ".restore-20261012T042000Z")
	if rep.Target != want || rep.Bytes != int64(len("content")) {
		t.Fatalf("report = %+v", rep)
	}
	if got, err := os.ReadFile(filepath.Join(want, "dir", "file.txt")); err != nil || string(got) != "content" {
		t.Fatalf("restored file = %q, %v", got, err)
	}
	if target, err := os.Readlink(filepath.Join(want, "dir", "link")); err != nil || target != "file.txt" {
		t.Fatalf("restored link = %q, %v", target, err)
	}
	if _, err := os.Stat(filepath.Join(root, "top.txt")); err != nil {
		t.Fatalf("the live tree changed: %v", err)
	}
}

func TestApplySideBySideRefusesExistingTarget(t *testing.T) {
	root := tree(t)
	existing := filepath.Join(root, ".restore-20261012T042000Z")
	if err := os.Mkdir(existing, 0o755); err != nil {
		t.Fatal(err)
	}
	s, err := build(t, config(root, true))
	if err != nil {
		t.Fatal(err)
	}
	archive := applyTar(t, []tar.Header{{Name: "a.txt", Typeflag: tar.TypeReg, Mode: 0o644}}, []string{"x"})
	if _, err := s.(source.Restorer).Apply(context.Background(), source.ApplyRequest{
		Mode: source.ApplySideBySide, Label: "20261012T042000Z", Options: jsontext.Value(applyOptions),
	}, bytes.NewReader(archive)); err == nil {
		t.Fatal("an existing side-by-side target was reused")
	}
	entries, err := os.ReadDir(existing)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("the existing directory gained %d entries", len(entries))
	}
}

func TestApplyInPlaceReplacesFilesAndKeepsOthers(t *testing.T) {
	root := tree(t)
	s, err := build(t, config(root, true))
	if err != nil {
		t.Fatal(err)
	}
	archive := applyTar(t, []tar.Header{{Name: "top.txt", Typeflag: tar.TypeReg, Mode: 0o644}}, []string{"replaced"})
	rep, err := s.(source.Restorer).Apply(context.Background(), source.ApplyRequest{
		Mode: source.ApplyInPlace, Label: "l", Options: jsontext.Value(applyOptions),
	}, bytes.NewReader(archive))
	if err != nil {
		t.Fatal(err)
	}
	if rep.Target != root || rep.Previous != "" {
		t.Fatalf("report = %+v", rep)
	}
	if got, _ := os.ReadFile(filepath.Join(root, "top.txt")); string(got) != "replaced" {
		t.Fatalf("top.txt = %q", got)
	}
	if _, err := os.Stat(filepath.Join(root, "sub", "nested.txt")); err != nil {
		t.Fatalf("a file the archive did not carry was deleted: %v", err)
	}
}

func TestApplyRefusesEscapingArchivesBeforeWriting(t *testing.T) {
	attacks := map[string][]tar.Header{
		"absolute":          {{Name: "/etc/passwd", Typeflag: tar.TypeReg, Mode: 0o644}},
		"parent":            {{Name: "../escape.txt", Typeflag: tar.TypeReg, Mode: 0o644}},
		"deep parent":       {{Name: "a/../../escape.txt", Typeflag: tar.TypeReg, Mode: 0o644}},
		"absolute symlink":  {{Name: "link", Typeflag: tar.TypeSymlink, Linkname: "/etc"}},
		"escaping symlink":  {{Name: "link", Typeflag: tar.TypeSymlink, Linkname: "../../etc"}},
		"escaping hardlink": {{Name: "hard", Typeflag: tar.TypeLink, Linkname: "../../etc/passwd"}},
	}
	for name, headers := range attacks {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			s, err := build(t, config(root, true))
			if err != nil {
				t.Fatal(err)
			}
			bodies := make([]string, len(headers))
			for i := range bodies {
				bodies[i] = "x"
			}
			archive := applyTar(t, headers, bodies)
			if _, err := s.(source.Restorer).Apply(context.Background(), source.ApplyRequest{
				Mode: source.ApplyInPlace, Label: "l", Options: jsontext.Value(applyOptions),
			}, bytes.NewReader(archive)); err == nil {
				t.Fatal("an escaping archive was applied")
			}
			entries, err := os.ReadDir(root)
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != 0 {
				t.Fatalf("the refusal still wrote %d entries", len(entries))
			}
			if _, err := os.Lstat(filepath.Join(filepath.Dir(root), "escape.txt")); err == nil {
				t.Fatal("an entry escaped the root")
			}
		})
	}
}

func TestApplyRefusesToWriteThroughALiveSymlink(t *testing.T) {
	outside := t.TempDir()
	root := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(root, "d")); err != nil {
		t.Fatal(err)
	}
	s, err := build(t, config(root, true))
	if err != nil {
		t.Fatal(err)
	}
	archive := applyTar(t, []tar.Header{{Name: "d/planted.txt", Typeflag: tar.TypeReg, Mode: 0o644}}, []string{"x"})
	if _, err := s.(source.Restorer).Apply(context.Background(), source.ApplyRequest{
		Mode: source.ApplyInPlace, Label: "l", Options: jsontext.Value(applyOptions),
	}, bytes.NewReader(archive)); err == nil {
		t.Fatal("a write through a live symlink was allowed")
	}
	if _, err := os.Stat(filepath.Join(outside, "planted.txt")); err == nil {
		t.Fatal("the archive wrote outside the root through a symlink")
	}
}

func TestApplyRefusesUnknownOptions(t *testing.T) {
	root := t.TempDir()
	s, err := build(t, config(root, true))
	if err != nil {
		t.Fatal(err)
	}
	archive := applyTar(t, []tar.Header{{Name: "a", Typeflag: tar.TypeReg, Mode: 0o644}}, []string{"x"})
	restorer := s.(source.Restorer)
	for _, req := range []source.ApplyRequest{
		{Mode: "", Label: "l", Options: jsontext.Value(applyOptions)},
		{Mode: "overwrite", Label: "l", Options: jsontext.Value(applyOptions)},
		{Mode: source.ApplySideBySide, Label: "", Options: jsontext.Value(applyOptions)},
		{Mode: source.ApplySideBySide, Label: "l", Options: nil},
		{Mode: source.ApplySideBySide, Label: "l", Options: jsontext.Value(`{"preserve_owner": false, "side_by_side_dir": ".restore-{{ .Label }}", "zzz": 1}`)},
		{Mode: source.ApplySideBySide, Label: "l", Options: jsontext.Value(`{"preserve_owner": false}`)},
		{Mode: source.ApplySideBySide, Label: "l", Options: jsontext.Value(`{"preserve_owner": false, "side_by_side_dir": "../outside"}`)},
	} {
		if _, err := restorer.Apply(context.Background(), req, bytes.NewReader(archive)); err == nil {
			t.Errorf("request %+v accepted", req)
		}
	}
}

func TestProbeWalksTheArchive(t *testing.T) {
	s, err := build(t, config(t.TempDir(), true))
	if err != nil {
		t.Fatal(err)
	}
	archive := applyTar(t, []tar.Header{
		{Name: "dir/", Typeflag: tar.TypeDir, Mode: 0o755},
		{Name: "dir/file.txt", Typeflag: tar.TypeReg, Mode: 0o644},
	}, []string{"", "content"})
	rep, err := s.(source.Prober).Probe(context.Background(), bytes.NewReader(archive))
	if err != nil {
		t.Fatal(err)
	}
	if rep.Entries != 2 || rep.Bytes != int64(len("content")) {
		t.Fatalf("probe report = %+v", rep)
	}
	bad := applyTar(t, []tar.Header{{Name: "../escape", Typeflag: tar.TypeReg, Mode: 0o644}}, []string{"x"})
	if _, err := s.(source.Prober).Probe(context.Background(), bytes.NewReader(bad)); err == nil {
		t.Fatal("a probe accepted an escaping archive")
	}
}

func TestApplyCarriesHardLinks(t *testing.T) {
	root := t.TempDir()
	s, err := build(t, config(root, true))
	if err != nil {
		t.Fatal(err)
	}
	archive := applyTar(t, []tar.Header{
		{Name: "a.txt", Typeflag: tar.TypeReg, Mode: 0o644},
		{Name: "b.txt", Typeflag: tar.TypeLink, Linkname: "a.txt"},
	}, []string{"linked", ""})
	if _, err := s.(source.Restorer).Apply(context.Background(), source.ApplyRequest{
		Mode: source.ApplyInPlace, Label: "l", Options: jsontext.Value(applyOptions),
	}, bytes.NewReader(archive)); err != nil {
		t.Fatal(err)
	}
	a, err := os.Stat(filepath.Join(root, "a.txt"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.Stat(filepath.Join(root, "b.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(a, b) {
		t.Fatal("b.txt is not a hard link to a.txt")
	}
}

func TestApplyPreserveOwner(t *testing.T) {
	root := t.TempDir()
	s, err := build(t, config(root, true))
	if err != nil {
		t.Fatal(err)
	}
	archive := applyTar(t, []tar.Header{
		{Name: "owned.txt", Typeflag: tar.TypeReg, Mode: 0o644, Uid: os.Getuid(), Gid: os.Getgid()},
	}, []string{"x"})
	rep, err := s.(source.Restorer).Apply(context.Background(), source.ApplyRequest{
		Mode: source.ApplySideBySide, Label: "l",
		Options: jsontext.Value(`{"preserve_owner": true, "side_by_side_dir": ".restore-{{ .Label }}"}`),
	}, bytes.NewReader(archive))
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(rep.Target, "owned.txt"))
	if err != nil {
		t.Fatal(err)
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		t.Skip("no unix stat details on this platform")
	}
	if int(st.Uid) != os.Getuid() || int(st.Gid) != os.Getgid() {
		t.Fatalf("owner = %d:%d, want %d:%d", st.Uid, st.Gid, os.Getuid(), os.Getgid())
	}
}

// TestDumpLeavesOutWhatApplyRefuses pins the one rule Dump and Apply share: an
// entry the Apply side refuses — a socket, a FIFO, a device node, a link that
// resolves outside the tree — never enters the stream, so a green artifact is
// exactly what the tool can restore. What was left out is counted, not silent.
func TestDumpLeavesOutWhatApplyRefuses(t *testing.T) {
	root := t.TempDir()
	if err := syscall.Mkfifo(filepath.Join(root, "pipe"), 0o644); err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("unix", filepath.Join(root, "sock"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	if err := os.WriteFile(filepath.Join(root, "keep.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("keep.txt", filepath.Join(root, "rel")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("/etc/passwd", filepath.Join(root, "abs")); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("../../outside", filepath.Join(root, "sub", "up")); err != nil {
		t.Fatal(err)
	}

	s, err := build(t, config(root, true))
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := s.Dump(context.Background(), &buf); err != nil {
		t.Fatalf("a tree holding a socket, a FIFO and links out of the root failed the archive: %v", err)
	}
	tr := tar.NewReader(bytes.NewReader(buf.Bytes()))
	var names []string
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		names = append(names, hdr.Name)
	}
	// The walk is lexical: abs, keep.txt, pipe, rel, sock, sub/, sub/up.
	if got := strings.Join(names, " "); got != "keep.txt rel sub/" {
		t.Fatalf("the archive carries %q, want only what an Apply accepts", got)
	}

	stats, err := s.(source.DumpStatsReporter).DumpStats(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.SkippedEntries != 4 || stats.PaddedFiles != 0 {
		t.Fatalf("stats = %+v, want the four carried-nowhere entries counted", stats)
	}
	// The other half of the pin: what Dump wrote, this package's own validation
	// accepts — probe runs validateEntry over every header of the archive.
	if _, err := s.(source.Prober).Probe(context.Background(), bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatalf("an archive Dump wrote is not one this package restores: %v", err)
	}
}

func TestDescribeAndCheckRespectCancellationAndShape(t *testing.T) {
	root := tree(t)
	s, err := build(t, config(root, true))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Describe(ctx); err == nil {
		t.Error("Describe accepted a cancelled context")
	}
	if err := s.(plugin.Checker).Check(ctx); err == nil {
		t.Error("Check accepted a cancelled context")
	}

	file := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	notDir, err := build(t, config(file, true))
	if err != nil {
		t.Fatal(err)
	}
	if err := notDir.(plugin.Checker).Check(context.Background()); err == nil {
		t.Fatal("a file passed Check as a root")
	}
}

func TestApplyRefusesAFileWhereADirectoryStands(t *testing.T) {
	root := tree(t)
	s, err := build(t, config(root, true))
	if err != nil {
		t.Fatal(err)
	}
	archive := applyTar(t, []tar.Header{{Name: "sub", Typeflag: tar.TypeReg, Mode: 0o644}}, []string{"x"})
	if _, err := s.(source.Restorer).Apply(context.Background(), source.ApplyRequest{
		Mode: source.ApplyInPlace, Label: "l", Options: jsontext.Value(applyOptions),
	}, bytes.NewReader(archive)); err == nil {
		t.Fatal("a file entry replaced a live directory")
	}
}

func TestApplyFailsOnAFileWithoutItsParent(t *testing.T) {
	root := t.TempDir()
	s, err := build(t, config(root, true))
	if err != nil {
		t.Fatal(err)
	}
	archive := applyTar(t, []tar.Header{{Name: "nodir/f.txt", Typeflag: tar.TypeReg, Mode: 0o644}}, []string{"x"})
	if _, err := s.(source.Restorer).Apply(context.Background(), source.ApplyRequest{
		Mode: source.ApplyInPlace, Label: "l", Options: jsontext.Value(applyOptions),
	}, bytes.NewReader(archive)); err == nil {
		t.Fatal("a file whose parent directory is missing was written")
	}
}

func TestDumpCarriesABrokenSymlink(t *testing.T) {
	// A broken symlink is a legitimate entry, not an unreadable file.
	root := t.TempDir()
	if err := os.Symlink("nowhere", filepath.Join(root, "broken")); err != nil {
		t.Fatal(err)
	}
	headers := dump(t, root, true)
	if len(headers) != 1 || headers[0].Typeflag != tar.TypeSymlink || headers[0].Linkname != "nowhere" {
		t.Fatalf("headers = %+v", headers)
	}
	if headers[0].Name != "broken" {
		t.Fatalf("entry name = %q, want a path relative to the root", headers[0].Name)
	}
}

// hookWriter runs hook once, before the first non-empty write passes through:
// the seam that lets a test mutate a file while Dump is reading it.
type hookWriter struct {
	w     io.Writer
	hook  func()
	fired bool
}

func (h *hookWriter) Write(p []byte) (int, error) {
	if !h.fired && len(p) > 0 {
		h.fired = true
		h.hook()
	}
	return h.w.Write(p)
}

func TestDumpSurvivesAFileGrowingMidRead(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "log.txt")
	original := bytes.Repeat([]byte("a"), 512<<10)
	if err := os.WriteFile(path, original, 0o644); err != nil {
		t.Fatal(err)
	}
	s, err := build(t, config(root, false))
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	hw := &hookWriter{w: &buf, hook: func() {
		// A live volume appends while the archive is read.
		f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write(bytes.Repeat([]byte("b"), 64<<10)); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}}
	if err := s.Dump(context.Background(), hw); err != nil {
		t.Fatalf("a file that grew while it was read failed the archive: %v", err)
	}
	if !hw.fired {
		t.Fatal("the growing file was never read; the test proves nothing")
	}
	tr := tar.NewReader(&buf)
	hdr, err := tr.Next()
	if err != nil {
		t.Fatal(err)
	}
	if hdr.Name != "log.txt" || hdr.Size != int64(len(original)) {
		t.Fatalf("header = %+v, want the size the walk recorded", hdr)
	}
	got, err := io.ReadAll(tr)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, original) {
		t.Fatalf("the entry carries %d bytes, want exactly the %d walked bytes with no appended tail", len(got), len(original))
	}
}

func TestDumpPadsAndReportsAFileThatShrankMidRead(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "log.txt")
	original := bytes.Repeat([]byte("a"), 512<<10)
	if err := os.WriteFile(path, original, 0o644); err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	s, err := buildLogged(t, config(root, false), slog.New(slog.NewTextHandler(&logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	hw := &hookWriter{w: &buf, hook: func() {
		if err := os.Truncate(path, 100); err != nil {
			t.Fatal(err)
		}
	}}
	if err := s.Dump(context.Background(), hw); err != nil {
		t.Fatalf("a file that shrank while it was read failed the archive: %v", err)
	}
	if !hw.fired {
		t.Fatal("the shrinking file was never read; the test proves nothing")
	}
	tr := tar.NewReader(&buf)
	hdr, err := tr.Next()
	if err != nil {
		t.Fatal(err)
	}
	if hdr.Size != int64(len(original)) {
		t.Fatalf("header size = %d, want the walked %d", hdr.Size, len(original))
	}
	got, err := io.ReadAll(tr)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(original) {
		t.Fatalf("the entry carries %d bytes, want the walked %d", len(got), len(original))
	}
	read := 0
	for read < len(got) && got[read] == 'a' {
		read++
	}
	for _, b := range got[read:] {
		if b != 0 {
			t.Fatalf("the short read was not zero-padded at offset %d", read)
		}
	}
	if !strings.Contains(logs.String(), "shrank") || !strings.Contains(logs.String(), path) {
		t.Fatalf("the shrink was silent in the log: %q", logs.String())
	}
	// The event is part of the artifact's report, not only of the log: the run
	// manifest and the nightly message carry this count.
	stats, err := s.(source.DumpStatsReporter).DumpStats(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.PaddedFiles != 1 || stats.SkippedEntries != 0 {
		t.Fatalf("stats = %+v, want the padded file counted", stats)
	}
}

func TestApplyRefusesToMkdirThroughALiveSymlink(t *testing.T) {
	outside := t.TempDir()
	if err := os.Chmod(outside, 0o750); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(root, "d")); err != nil {
		t.Fatal(err)
	}
	s, err := build(t, config(root, true))
	if err != nil {
		t.Fatal(err)
	}
	// A lone directory entry: MkdirAll on the symlink and the Chmod after it
	// would succeed quietly, chmodding the target outside the root.
	archive := applyTar(t, []tar.Header{{Name: "d/", Typeflag: tar.TypeDir, Mode: 0o777}}, []string{""})
	if _, err := s.(source.Restorer).Apply(context.Background(), source.ApplyRequest{
		Mode: source.ApplyInPlace, Label: "l", Options: jsontext.Value(applyOptions),
	}, bytes.NewReader(archive)); err == nil {
		t.Fatal("a directory entry positioned on a live symlink was followed")
	}
	info, err := os.Stat(outside)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o750 {
		t.Fatalf("outside mode = %o, want it untouched at 750", info.Mode().Perm())
	}
}

func TestApplyRejectsUnsupportedEntryTypesBeforeWriting(t *testing.T) {
	// A regular file first: if the type check ran during extraction instead of
	// in the validation pass, this file would already be on disk when the pipe
	// entry is refused.
	archive := applyTar(t, []tar.Header{
		{Name: "a.txt", Typeflag: tar.TypeReg, Mode: 0o644},
		{Name: "pipe", Typeflag: tar.TypeFifo, Mode: 0o644},
	}, []string{"x", ""})
	root := t.TempDir()
	s, err := build(t, config(root, true))
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.(source.Restorer).Apply(context.Background(), source.ApplyRequest{
		Mode: source.ApplyInPlace, Label: "l", Options: jsontext.Value(applyOptions),
	}, bytes.NewReader(archive))
	if err == nil {
		t.Fatal("an unsupported entry type was accepted")
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassPermanent {
		t.Fatalf("class = %q, want permanent (%v)", class, err)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("the refusal happened after %d entries were written", len(entries))
	}
	if _, err := os.Lstat(filepath.Join(root, "a.txt")); err == nil {
		t.Fatal("a file was written before the unsupported entry was refused")
	}

	if _, err := s.(source.Prober).Probe(context.Background(), bytes.NewReader(archive)); err == nil {
		t.Fatal("a probe accepted an entry type this plugin cannot restore")
	}
}

// errReader fails on its first read: the source of a copy that cannot finish.
type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("injected read failure") }

// cancelOnRead cancels ctx on the nth read, so a probe can finish one entry and
// find the context done at the next one.
type cancelOnRead struct {
	r      io.Reader
	cancel context.CancelFunc
	n      int
	calls  int
}

func (c *cancelOnRead) Read(p []byte) (int, error) {
	c.calls++
	if c.calls == c.n {
		c.cancel()
	}
	return c.r.Read(p)
}

func TestRegisterRefusesACancelledContextAndDefaultsTheLog(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := plugin.Build(ctx, testRegistry(t), source.Kind, "files",
		jsontext.Value(config(tree(t), true)), plugin.Deps{Instance: "files"}); err == nil {
		t.Fatal("New accepted a cancelled context")
	}
	// A nil logger is replaced rather than dereferenced later.
	s, err := plugin.Build(context.Background(), testRegistry(t), source.Kind, "files",
		jsontext.Value(config(tree(t), true)), plugin.Deps{Instance: "files"})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Dump(context.Background(), io.Discard); err != nil {
		t.Fatalf("Dump with the defaulted logger: %v", err)
	}
}

func TestDumpFailsWhenTheRootIsNotADirectory(t *testing.T) {
	file := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	s, err := build(t, config(file, true))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Dump(context.Background(), io.Discard); err == nil {
		t.Fatal("a root that is a file was opened as a directory")
	}
}

func TestApplyRefusesACancelledContext(t *testing.T) {
	s, err := build(t, config(tree(t), true))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.(source.Restorer).Apply(ctx, source.ApplyRequest{
		Mode: source.ApplyInPlace, Label: "l", Options: jsontext.Value(applyOptions),
	}, bytes.NewReader(nil)); err == nil {
		t.Fatal("Apply accepted a cancelled context")
	}
}

// TestApplySideBySideRefusesAStatFailureThatIsNotAbsence: the target's parent
// is not a directory, so the existence check fails for a reason other than
// absence and the restore is a transient fault, not "the target is free".
func TestApplySideBySideRefusesAStatFailureThatIsNotAbsence(t *testing.T) {
	file := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	s, err := build(t, config(file, true))
	if err != nil {
		t.Fatal(err)
	}
	archive := applyTar(t, []tar.Header{{Name: "a.txt", Typeflag: tar.TypeReg, Mode: 0o644}}, []string{"x"})
	if _, err := s.(source.Restorer).Apply(context.Background(), source.ApplyRequest{
		Mode: source.ApplySideBySide, Label: "l", Options: jsontext.Value(applyOptions),
	}, bytes.NewReader(archive)); err == nil {
		t.Fatal("a side-by-side target under a non-directory was accepted")
	}
}

// TestApplyReadsTheArtifactInPlace: the restore validates and consumes the
// artifact it is handed, so it needs no process temp directory at all — the
// chart's container gives that directory 64 Mi and it is not an artifact store.
func TestApplyReadsTheArtifactInPlace(t *testing.T) {
	root := t.TempDir()
	s, err := build(t, config(root, true))
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("TMPDIR", filepath.Join(t.TempDir(), "gone"))
	archive := applyTar(t, []tar.Header{{Name: "a.txt", Typeflag: tar.TypeReg, Mode: 0o644}}, []string{"x"})
	rep, err := s.(source.Restorer).Apply(context.Background(), source.ApplyRequest{
		Mode: source.ApplyInPlace, Label: "l", Options: jsontext.Value(applyOptions),
	}, bytes.NewReader(archive))
	if err != nil {
		t.Fatalf("Apply with no usable process temp dir: %v", err)
	}
	if got, _ := os.ReadFile(filepath.Join(root, "a.txt")); string(got) != "x" {
		t.Fatalf("restored file = %q", got)
	}
	if rep.Bytes != 1 {
		t.Fatalf("report = %+v", rep)
	}
}

// TestApplyRefusesANonSeekableArtifact: the archive is probed and then
// rewound, so a stream that cannot seek is refused before anything is written
// — the engine hands this plugin a file.
func TestApplyRefusesANonSeekableArtifact(t *testing.T) {
	root := t.TempDir()
	s, err := build(t, config(root, true))
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.(source.Restorer).Apply(context.Background(), source.ApplyRequest{
		Mode: source.ApplyInPlace, Label: "l", Options: jsontext.Value(applyOptions),
	}, errReader{})
	if err == nil {
		t.Fatal("Apply accepted a stream that cannot be rewound")
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassPermanent {
		t.Fatalf("class = %q, want permanent (%v)", class, err)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("the refusal still wrote %v", entries)
	}
}

// TestApplyReportsAClosedArtifactHandle: a handle the caller already closed
// fails the probe before the first write.
func TestApplyReportsAClosedArtifactHandle(t *testing.T) {
	root := t.TempDir()
	s, err := build(t, config(root, true))
	if err != nil {
		t.Fatal(err)
	}
	archive := applyTar(t, []tar.Header{{Name: "a.txt", Typeflag: tar.TypeReg, Mode: 0o644}}, []string{"x"})
	f, err := os.CreateTemp(t.TempDir(), "artifact-*")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(archive); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.(source.Restorer).Apply(context.Background(), source.ApplyRequest{
		Mode: source.ApplyInPlace, Label: "l", Options: jsontext.Value(applyOptions),
	}, f); err == nil {
		t.Fatal("Apply rewound a closed artifact handle")
	}
}

// seekFails reads a valid artifact and cannot rewind it: the shape of a handle
// that answered the probe and then refused the second pass.
type seekFails struct{ io.Reader }

func (seekFails) Seek(int64, int) (int64, error) { return 0, errors.New("injected seek failure") }

// TestApplyReportsAnArtifactItCannotRewind: the probe consumed the stream and
// the rewind is what fails, before the first write.
func TestApplyReportsAnArtifactItCannotRewind(t *testing.T) {
	root := t.TempDir()
	s, err := build(t, config(root, true))
	if err != nil {
		t.Fatal(err)
	}
	archive := applyTar(t, []tar.Header{{Name: "a.txt", Typeflag: tar.TypeReg, Mode: 0o644}}, []string{"x"})
	if _, err := s.(source.Restorer).Apply(context.Background(), source.ApplyRequest{
		Mode: source.ApplyInPlace, Label: "l", Options: jsontext.Value(applyOptions),
	}, seekFails{Reader: bytes.NewReader(archive)}); err == nil {
		t.Fatal("Apply extracted a stream it could not rewind")
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("the refusal still wrote %v", entries)
	}
}

// TestApplyRefusesARootItCannotOpen: the root exists and is a directory, but
// this process cannot open it as one; no entry is written.
func TestApplyRefusesARootItCannotOpen(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root opens anything; the permission failure cannot be provoked")
	}
	root := t.TempDir()
	if err := os.Chmod(root, 0o300); err != nil {
		t.Fatal(err)
	}
	unlockTree(t, root)
	s, err := build(t, config(root, true))
	if err != nil {
		t.Fatal(err)
	}
	archive := applyTar(t, []tar.Header{{Name: "a.txt", Typeflag: tar.TypeReg, Mode: 0o644}}, []string{"x"})
	if _, err := s.(source.Restorer).Apply(context.Background(), source.ApplyRequest{
		Mode: source.ApplyInPlace, Label: "l", Options: jsontext.Value(applyOptions),
	}, bytes.NewReader(archive)); err == nil {
		t.Fatal("Apply wrote through a root it could not open")
	}
}

// TestApplyFailsWritingAFileWhereTheDirectoryRefusesIt: the entry is free, but
// the directory it lands in is not writable by this process.
func TestApplyFailsWritingAFileWhereTheDirectoryRefusesIt(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root ignores directory modes")
	}
	root := t.TempDir()
	ro := filepath.Join(root, "ro")
	if err := os.Mkdir(ro, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(ro, 0o500); err != nil {
		t.Fatal(err)
	}
	unlockTree(t, root)
	s, err := build(t, config(root, true))
	if err != nil {
		t.Fatal(err)
	}
	archive := applyTar(t, []tar.Header{{Name: "ro/new.txt", Typeflag: tar.TypeReg, Mode: 0o644}}, []string{"x"})
	if _, err := s.(source.Restorer).Apply(context.Background(), source.ApplyRequest{
		Mode: source.ApplyInPlace, Label: "l", Options: jsontext.Value(applyOptions),
	}, bytes.NewReader(archive)); err == nil {
		t.Fatal("a file was created in a directory that refused it")
	}
}

// TestApplyRefusesAHardLinkOutOfTheRootThroughASymlink is the confinement
// gate: a symlink planted in the live tree must not turn a hard link entry
// into a link to a file outside the root — a hard link shares its inode, so
// preserve_owner would then chown the foreign file too.
func TestApplyRefusesAHardLinkOutOfTheRootThroughASymlink(t *testing.T) {
	outside := t.TempDir()
	secret := filepath.Join(outside, "secret")
	if err := os.WriteFile(secret, []byte("FOREIGN-BYTES"), 0o600); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(root, "jump")); err != nil {
		t.Fatal(err)
	}
	s, err := build(t, config(root, true))
	if err != nil {
		t.Fatal(err)
	}
	archive := applyTar(t, []tar.Header{
		{Name: "leak", Typeflag: tar.TypeLink, Linkname: "jump/secret"},
	}, []string{""})
	_, err = s.(source.Restorer).Apply(context.Background(), source.ApplyRequest{
		Mode: source.ApplyInPlace, Label: "l", Options: jsontext.Value(applyOptions),
	}, bytes.NewReader(archive))
	if err == nil {
		t.Fatal("a hard link through a symlink out of the root was created")
	}
	if _, statErr := os.Lstat(filepath.Join(root, "leak")); statErr == nil {
		t.Fatal("the refused entry still landed in the root")
	}
	info, statErr := os.Stat(secret)
	if statErr != nil {
		t.Fatal(statErr)
	}
	if !info.Mode().IsRegular() || info.Size() != int64(len("FOREIGN-BYTES")) {
		t.Fatalf("the file outside the root changed: %+v", info)
	}
}

// TestApplyRestoresAReadOnlyDirectoryTree: a directory's final mode lands after
// its children exist, so a valid 0555 tree restores as an unprivileged user.
func TestApplyRestoresAReadOnlyDirectoryTree(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root writes into a read-only directory; the ordering cannot be observed")
	}
	root := t.TempDir()
	s, err := build(t, config(root, true))
	if err != nil {
		t.Fatal(err)
	}
	archive := applyTar(t, []tar.Header{
		{Name: "locked/", Typeflag: tar.TypeDir, Mode: 0o555},
		{Name: "locked/file.txt", Typeflag: tar.TypeReg, Mode: 0o444},
	}, []string{"", "content"})
	unlockTree(t, root)
	if _, err := s.(source.Restorer).Apply(context.Background(), source.ApplyRequest{
		Mode: source.ApplyInPlace, Label: "l", Options: jsontext.Value(applyOptions),
	}, bytes.NewReader(archive)); err != nil {
		t.Fatalf("a valid read-only tree failed to restore: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(root, "locked", "file.txt")); err != nil || string(got) != "content" {
		t.Fatalf("restored file = %q, %v", got, err)
	}
	info, err := os.Stat(filepath.Join(root, "locked"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o555 {
		t.Fatalf("the directory mode is %o, want 555", info.Mode().Perm())
	}
}

func TestApplyFailsWhenTheRootCannotBeCreated(t *testing.T) {
	file := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	s, err := build(t, config(filepath.Join(file, "sub"), true))
	if err != nil {
		t.Fatal(err)
	}
	archive := applyTar(t, []tar.Header{{Name: "a.txt", Typeflag: tar.TypeReg, Mode: 0o644}}, []string{"x"})
	if _, err := s.(source.Restorer).Apply(context.Background(), source.ApplyRequest{
		Mode: source.ApplyInPlace, Label: "l", Options: jsontext.Value(applyOptions),
	}, bytes.NewReader(archive)); err == nil {
		t.Fatal("Apply accepted a root it could not create")
	}
}

func TestProbeRefusesACancelledContextAndABadStream(t *testing.T) {
	s, err := build(t, config(t.TempDir(), true))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.(source.Prober).Probe(ctx, bytes.NewReader(nil)); err == nil {
		t.Fatal("Probe accepted a cancelled context")
	}
	if _, err := s.(source.Prober).Probe(context.Background(), strings.NewReader("not a tar stream")); err == nil {
		t.Fatal("Probe accepted a stream that is not a tar")
	}
	body := strings.Repeat("x", 2000)
	archive := applyTar(t, []tar.Header{{Name: "f.txt", Typeflag: tar.TypeReg, Mode: 0o644}}, []string{body})
	if _, err := s.(source.Prober).Probe(context.Background(), bytes.NewReader(archive[:len(archive)-1100])); err == nil {
		t.Fatal("Probe reported success on a truncated entry")
	}
}

// TestProbeReturnsOnCancellationMidStream: the walk checks the context at every
// entry, so a cancelled probe stops at the next one.
func TestProbeReturnsOnCancellationMidStream(t *testing.T) {
	s, err := build(t, config(t.TempDir(), true))
	if err != nil {
		t.Fatal(err)
	}
	archive := applyTar(t, []tar.Header{{Name: "f.txt", Typeflag: tar.TypeReg, Mode: 0o644}}, []string{"content"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if _, err := s.(source.Prober).Probe(ctx, &cancelOnRead{r: bytes.NewReader(archive), cancel: cancel, n: 2}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Probe = %v, want context.Canceled", err)
	}
}

func TestApplyValidatesBeforeWritingAGarbageStream(t *testing.T) {
	root := t.TempDir()
	s, err := build(t, config(root, true))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.(source.Restorer).Apply(context.Background(), source.ApplyRequest{
		Mode: source.ApplyInPlace, Label: "l", Options: jsontext.Value(applyOptions),
	}, strings.NewReader("not a tar stream")); err == nil {
		t.Fatal("Apply unpacked a stream that is not a tar")
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("the refusal still wrote %v", entries)
	}
}

func TestApplyValidatesBeforeWritingATruncatedStream(t *testing.T) {
	root := t.TempDir()
	s, err := build(t, config(root, true))
	if err != nil {
		t.Fatal(err)
	}
	body := strings.Repeat("x", 2000)
	archive := applyTar(t, []tar.Header{{Name: "f.txt", Typeflag: tar.TypeReg, Mode: 0o644}}, []string{body})
	if _, err := s.(source.Restorer).Apply(context.Background(), source.ApplyRequest{
		Mode: source.ApplyInPlace, Label: "l", Options: jsontext.Value(applyOptions),
	}, bytes.NewReader(archive[:len(archive)-1100])); err == nil {
		t.Fatal("Apply unpacked a truncated stream")
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("the refusal still wrote %v", entries)
	}
}

// applyInPlace is one in-place restore of an archive under a fresh root.
func applyInPlace(t *testing.T, root string, entries []tar.Header, bodies []string) error {
	t.Helper()
	s, err := build(t, config(root, true))
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.(source.Restorer).Apply(context.Background(), source.ApplyRequest{
		Mode: source.ApplyInPlace, Label: "l", Options: jsontext.Value(applyOptions),
	}, bytes.NewReader(applyTar(t, entries, bodies)))
	return err
}

func TestApplyRefusesADirectoryEntryWhereAFileStands(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "blocker"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := applyInPlace(t, root, []tar.Header{{Name: "blocker", Typeflag: tar.TypeDir, Mode: 0o755}}, []string{""}); err == nil {
		t.Fatal("a directory entry replaced a live file")
	}
}

func TestApplyFailsOnADirectoryUnderAnUnwritableParent(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root ignores directory modes")
	}
	root := t.TempDir()
	ro := filepath.Join(root, "ro")
	if err := os.Mkdir(ro, 0o500); err != nil {
		t.Fatal(err)
	}
	unlockTree(t, root)
	if err := applyInPlace(t, root, []tar.Header{{Name: "ro/newdir/", Typeflag: tar.TypeDir, Mode: 0o755}}, []string{""}); err == nil {
		t.Fatal("a directory was created under an unwritable parent")
	}
}

func TestApplyFailsOnADirectoryUnderAFile(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "blocker"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := applyInPlace(t, root, []tar.Header{{Name: "blocker/sub/", Typeflag: tar.TypeDir, Mode: 0o755}}, []string{""}); err == nil {
		t.Fatal("a directory was created under a file")
	}
}

func TestApplyFailsOnAFileUnderAFile(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "blocker"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := applyInPlace(t, root, []tar.Header{{Name: "blocker/x", Typeflag: tar.TypeReg, Mode: 0o644}}, []string{"x"}); err == nil {
		t.Fatal("a file was written under a file")
	}
}

func TestApplyFailsOnADeepPathUnderAFile(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "blocker"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := applyInPlace(t, root, []tar.Header{{Name: "blocker/x/y", Typeflag: tar.TypeReg, Mode: 0o644}}, []string{"x"}); err == nil {
		t.Fatal("a deep path under a file was written")
	}
}

func TestApplyFailsReplacingAFileInAnUnwritableDirectory(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root ignores directory modes")
	}
	root := t.TempDir()
	ro := filepath.Join(root, "ro")
	if err := os.Mkdir(ro, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ro, "old"), []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(ro, 0o500); err != nil {
		t.Fatal(err)
	}
	unlockTree(t, root)
	if err := applyInPlace(t, root, []tar.Header{{Name: "ro/old", Typeflag: tar.TypeReg, Mode: 0o644}}, []string{"new"}); err == nil {
		t.Fatal("a file was replaced in an unwritable directory")
	}
}

func TestApplySymlinkWithoutItsParentFails(t *testing.T) {
	if err := applyInPlace(t, t.TempDir(), []tar.Header{{Name: "nodir/link", Typeflag: tar.TypeSymlink, Linkname: "target"}}, []string{""}); err == nil {
		t.Fatal("a symlink whose parent is missing was created")
	}
}

func TestApplySymlinkOverADirectoryFails(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "d"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := applyInPlace(t, root, []tar.Header{{Name: "d", Typeflag: tar.TypeSymlink, Linkname: "target"}}, []string{""}); err == nil {
		t.Fatal("a symlink replaced a live directory")
	}
}

func TestApplyHardLinkOverADirectoryFails(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "d"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := applyInPlace(t, root, []tar.Header{
		{Name: "a.txt", Typeflag: tar.TypeReg, Mode: 0o644},
		{Name: "d", Typeflag: tar.TypeLink, Linkname: "a.txt"},
	}, []string{"x", ""}); err == nil {
		t.Fatal("a hard link replaced a live directory")
	}
}

func TestApplyHardLinkToAMissingTargetFails(t *testing.T) {
	if err := applyInPlace(t, t.TempDir(), []tar.Header{
		{Name: "h", Typeflag: tar.TypeLink, Linkname: "missing"},
	}, []string{""}); err == nil {
		t.Fatal("a hard link to a missing target was created")
	}
}

// TestApplyRefusesAnOwnerItCannotRestore: preserving the archive's owner needs
// the privilege to chown, and a refusal is a permanent fault, not a retry.
func TestApplyRefusesAnOwnerItCannotRestore(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root chowns anything; the refusal cannot be provoked")
	}
	root := t.TempDir()
	s, err := build(t, config(root, true))
	if err != nil {
		t.Fatal(err)
	}
	archive := applyTar(t, []tar.Header{
		{Name: "owned.txt", Typeflag: tar.TypeReg, Mode: 0o644, Uid: 1, Gid: 1},
	}, []string{"x"})
	opts := `{"preserve_owner": true, "side_by_side_dir": ".restore-{{ .Label }}"}`
	_, err = s.(source.Restorer).Apply(context.Background(), source.ApplyRequest{
		Mode: source.ApplyInPlace, Label: "l", Options: jsontext.Value(opts),
	}, bytes.NewReader(archive))
	if err == nil {
		t.Fatal("a restore to another owner was accepted without the privilege")
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassPermanent {
		t.Fatalf("class = %q, want permanent (%v)", class, err)
	}
}

func TestApplyRefusesAnUnrenderableOrEmptySideBySideDir(t *testing.T) {
	root := t.TempDir()
	s, err := build(t, config(root, true))
	if err != nil {
		t.Fatal(err)
	}
	archive := applyTar(t, []tar.Header{{Name: "a.txt", Typeflag: tar.TypeReg, Mode: 0o644}}, []string{"x"})
	for _, opts := range []string{
		// A missing key is an execution error at render time.
		`{"preserve_owner": false, "side_by_side_dir": "{{ .Missing }}"}`,
		// An empty render has no directory to unpack into.
		`{"preserve_owner": false, "side_by_side_dir": ""}`,
	} {
		if _, err := s.(source.Restorer).Apply(context.Background(), source.ApplyRequest{
			Mode: source.ApplySideBySide, Label: "l", Options: jsontext.Value(opts),
		}, bytes.NewReader(archive)); err == nil {
			t.Errorf("options %s were accepted", opts)
		}
	}
}

// TestApplyRestoresNestedReadOnlyDirectories: every directory's final mode
// lands after every child exists, deepest first, so nested read-only trees
// restore as an unprivileged user.
func TestApplyRestoresNestedReadOnlyDirectories(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root writes into a read-only directory; the ordering cannot be observed")
	}
	root := t.TempDir()
	s, err := build(t, config(root, true))
	if err != nil {
		t.Fatal(err)
	}
	archive := applyTar(t, []tar.Header{
		{Name: "a/", Typeflag: tar.TypeDir, Mode: 0o555},
		{Name: "a/b/", Typeflag: tar.TypeDir, Mode: 0o500},
		{Name: "a/b/f.txt", Typeflag: tar.TypeReg, Mode: 0o444},
	}, []string{"", "", "x"})
	unlockTree(t, root)
	if _, err := s.(source.Restorer).Apply(context.Background(), source.ApplyRequest{
		Mode: source.ApplyInPlace, Label: "l", Options: jsontext.Value(applyOptions),
	}, bytes.NewReader(archive)); err != nil {
		t.Fatalf("a nested read-only tree failed to restore: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(root, "a", "b", "f.txt")); err != nil || string(got) != "x" {
		t.Fatalf("restored file = %q, %v", got, err)
	}
	for _, want := range []struct {
		path string
		mode os.FileMode
	}{{"a", 0o555}, {"a/b", 0o500}} {
		info, err := os.Stat(filepath.Join(root, want.path))
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != want.mode {
			t.Fatalf("%s mode = %o, want %o", want.path, info.Mode().Perm(), want.mode)
		}
	}
}

// TestAReadOnlyTreeIsUndeletableUntilUnlocked is the leak the walk cleanup
// exists for: a restore leaves a 0555 directory holding a file, and RemoveAll —
// what t.TempDir runs — cannot unlink the child until the directory is writable
// again. Root deletes through any mode, so the failure cannot be provoked there.
func TestAReadOnlyTreeIsUndeletableUntilUnlocked(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root deletes through any directory mode; the failure cannot be provoked")
	}
	root := t.TempDir()
	unlockTree(t, root)
	locked := filepath.Join(root, "locked")
	if err := os.Mkdir(locked, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(locked, "file.txt"), []byte("x"), 0o444); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(locked, 0o555); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(locked); err == nil {
		t.Fatal("a 0555 directory was deleted without being unlocked; the leak cannot be observed")
	}
	// A path the tree never held is skipped, not fatal.
	unlockTreeNow(filepath.Join(root, "never-created"))
	unlockTreeNow(root)
	if err := os.RemoveAll(locked); err != nil {
		t.Fatalf("an unlocked read-only tree still refused deletion: %v", err)
	}
}
