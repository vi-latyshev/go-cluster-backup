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

	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances/plugintest"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source/files"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source/sourcetest"
)

func testRegistry(t *testing.T) *instances.Registry {
	t.Helper()
	lr := loader.New()
	r := instances.NewRegistry()
	if err := loader.Add(lr, files.Spec, func(lr *loader.Registry) error {
		return files.Apply(lr, r)
	}); err != nil {
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
	return instances.Build(context.Background(), testRegistry(t), source.Kind, "files",
		jsontext.Value(cfg), instances.Deps{Instance: "files", Log: log})
}

func config(root string, ignoreVanished bool) string {
	return fmt.Sprintf(`{"root": %q, "ignore_vanished": %t, "on_change": "record"}`, root, ignoreVanished)
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
	if err := s.(instances.Checker).Check(context.Background()); err == nil {
		t.Fatal("a missing root passed Check")
	}
	ok, err := build(t, config(tree(t), true))
	if err != nil {
		t.Fatal(err)
	}
	if err := ok.(instances.Checker).Check(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestNewRejectsBadConfig(t *testing.T) {
	for _, cfg := range []string{
		`{"root": "", "ignore_vanished": true, "on_change": "record"}`,
		`{"root": "relative", "ignore_vanished": true, "on_change": "record"}`,
		`{"root": "/tmp", "ignore_vanished": true, "on_change": "sometimes"}`,
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

// TestApplyRefusesEscapingArchivesBeforeWriting: every macro is a whole archive
// that names something outside the root; none of them may write a byte.
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

// TestReviewCreatedSymlinkThroughInitiallySafeLiveChain: a live link is safe
// only if its own target obeys the archive rule at its own path. A live
// `a/live -> x/../marker` resolves inside while `a/x` points down, but the
// archive may replace `a/x`, and the ".." after the name then climbs from
// wherever it points. The live pre-pass refuses it, so no created link reads
// outside the root.
func TestReviewCreatedSymlinkThroughInitiallySafeLiveChain(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "root")
	if err := os.MkdirAll(filepath.Join(root, "a", "d"), 0o755); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{
		filepath.Join(root, "a", "marker"): "inside",
		filepath.Join(base, "marker"):      "outside",
	} {
		if err := os.WriteFile(name, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	for name, target := range map[string]string{"a/x": "d", "a/live": "x/../marker"} {
		if err := os.Symlink(target, filepath.Join(root, name)); err != nil {
			t.Fatal(err)
		}
	}
	if got, err := os.ReadFile(filepath.Join(root, "a/live")); err != nil || string(got) != "inside" {
		t.Fatalf("live chain before Apply = %q, %v", got, err)
	}
	err := applyAs(t, root, source.ApplyInPlace, []tar.Header{
		{Name: "restored", Typeflag: tar.TypeSymlink, Linkname: "a/live"},
		{Name: "a/x", Typeflag: tar.TypeSymlink, Linkname: ".."},
	}, []string{"", ""})
	if class, _ := pluginerrors.ClassOf(err); err == nil || class != pluginerrors.ClassPermanent {
		t.Fatalf("Apply = %v, want a permanent refusal of the live link", err)
	}
	if _, err := os.Lstat(filepath.Join(root, "restored")); err == nil {
		t.Fatal("the refused archive still wrote the created link")
	}
	if got, readErr := os.ReadFile(filepath.Join(root, "restored")); readErr == nil && string(got) == "outside" {
		t.Fatalf("created symlink reads outside root: %q", got)
	}
}

// TestLifecycleL15LiveLinkTargetClosure is the closure of the induction over
// live links: a live link whose target obeys the rule stays inside whatever the
// archive replaces around it (both entry orders), a live link whose target
// breaks it is refused whether or not the name it climbs through exists yet,
// and a cancellation never leaves a created link pointing out.
func TestLifecycleL15LiveLinkTargetClosure(t *testing.T) {
	live := func(t *testing.T, root string, withX bool) {
		t.Helper()
		if err := os.MkdirAll(filepath.Join(root, "a", "d"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, "a", "d", "marker"), []byte("inside"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, "marker"), []byte("root-inside"), 0o600); err != nil {
			t.Fatal(err)
		}
		if withX {
			if err := os.Symlink("d", filepath.Join(root, "a", "x")); err != nil {
				t.Fatal(err)
			}
		}
	}
	unsafeLive := func(t *testing.T, root string, withX bool) {
		live(t, root, withX)
		if err := os.Symlink("x/../marker", filepath.Join(root, "a", "live")); err != nil {
			t.Fatal(err)
		}
	}
	safeLive := func(t *testing.T, root string, withX bool) {
		live(t, root, withX)
		if err := os.Symlink("x/marker", filepath.Join(root, "a", "live")); err != nil {
			t.Fatal(err)
		}
	}
	cases := map[string]struct {
		live    func(*testing.T, string, bool)
		headers []tar.Header
		wantErr bool
	}{
		"a safe live link, the created link first": {
			live: safeLive,
			headers: []tar.Header{
				{Name: "restored", Typeflag: tar.TypeSymlink, Linkname: "a/live"},
				{Name: "a/x", Typeflag: tar.TypeSymlink, Linkname: ".."},
			},
		},
		"a safe live link, the replacement first": {
			live: safeLive,
			headers: []tar.Header{
				{Name: "a/x", Typeflag: tar.TypeSymlink, Linkname: ".."},
				{Name: "restored", Typeflag: tar.TypeSymlink, Linkname: "a/live"},
			},
		},
		"a live link that climbs after a name": {
			live:    unsafeLive,
			headers: []tar.Header{{Name: "restored", Typeflag: tar.TypeSymlink, Linkname: "a/live"}},
			wantErr: true,
		},
		"a live link that climbs after a name that does not exist yet": {
			live: func(t *testing.T, root string, _ bool) { unsafeLive(t, root, false) },
			headers: []tar.Header{
				{Name: "a/x", Typeflag: tar.TypeSymlink, Linkname: ".."},
				{Name: "restored", Typeflag: tar.TypeSymlink, Linkname: "a/live"},
			},
			wantErr: true,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "root")
			if err := os.MkdirAll(root, 0o755); err != nil {
				t.Fatal(err)
			}
			tc.live(t, root, true)
			err := applyAs(t, root, source.ApplyInPlace, tc.headers, make([]string, len(tc.headers)))
			if tc.wantErr {
				if class, _ := pluginerrors.ClassOf(err); err == nil || class != pluginerrors.ClassPermanent {
					t.Fatalf("Apply = %v, want a permanent refusal", err)
				}
				if _, statErr := os.Lstat(filepath.Join(root, "restored")); statErr == nil {
					t.Fatal("the refused archive still wrote the created link")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			// Everything the created link reads now lies inside the root.
			if got, readErr := os.ReadFile(filepath.Join(root, "restored")); readErr != nil || string(got) != "root-inside" {
				t.Fatalf("created link reads %q, %v, want the in-root marker", got, readErr)
			}
		})
	}
}

// TestLifecycleL15CancelLeavesLiveLinksAlone: cancellation before the archive
// replaces anything leaves every live link as it was, and the created link
// still resolves inside the root.
func TestLifecycleL15CancelLeavesLiveLinksAlone(t *testing.T) {
	root := filepath.Join(t.TempDir(), "root")
	if err := os.MkdirAll(filepath.Join(root, "a", "d"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "a", "d", "marker"), []byte("inside"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("d", filepath.Join(root, "a", "x")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("x/marker", filepath.Join(root, "a", "live")); err != nil {
		t.Fatal(err)
	}
	body := strings.Repeat("x", 1<<20)
	archive := applyTar(t, []tar.Header{
		{Name: "restored", Typeflag: tar.TypeSymlink, Linkname: "a/live"},
		{Name: "big.bin", Typeflag: tar.TypeReg, Mode: 0o644},
		{Name: "a/x", Typeflag: tar.TypeSymlink, Linkname: ".."},
	}, []string{"", body, ""})
	s, err := build(t, config(root, true))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err = s.(source.Restorer).Apply(ctx, source.ApplyRequest{
		Mode: source.ApplyInPlace, Label: "l", Options: jsontext.Value(applyOptions),
	}, &cancelMidBody{Reader: bytes.NewReader(archive), cancel: cancel, armAt: 300 << 10})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Apply = %v, want context.Canceled", err)
	}
	if got, err := os.ReadFile(filepath.Join(root, "restored")); err != nil || string(got) != "inside" {
		t.Fatalf("created link after cancel reads %q, %v, want the live chain inside", got, err)
	}
}

// TestLifecycleL16SymlinkedRootIsTheOperatorsChoice: the root path may itself
// be a symlink — opening it already followed it — so only links inside the root
// are judged. A safe tree restores as before; an inner link that breaks the
// rule refuses, naming it and the side_by_side remedy.
func TestLifecycleL16SymlinkedRootIsTheOperatorsChoice(t *testing.T) {
	base := t.TempDir()
	real := filepath.Join(base, "real")
	if err := os.Mkdir(real, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("real", filepath.Join(base, "root")); err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(base, "root")
	if err := applyAs(t, root, source.ApplyInPlace, []tar.Header{
		{Name: "a.txt", Typeflag: tar.TypeReg, Mode: 0o644},
	}, []string{"x"}); err != nil {
		t.Fatalf("Apply through a symlinked root = %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(real, "a.txt")); err != nil || string(got) != "x" {
		t.Fatalf("the restore did not land in the root's real directory: %q, %v", got, err)
	}
	if err := os.Symlink("x/../marker", filepath.Join(real, "bad")); err != nil {
		t.Fatal(err)
	}
	err := applyAs(t, root, source.ApplyInPlace, []tar.Header{
		{Name: "b.txt", Typeflag: tar.TypeReg, Mode: 0o644},
	}, []string{"y"})
	if class, _ := pluginerrors.ClassOf(err); err == nil || class != pluginerrors.ClassPermanent {
		t.Fatalf("Apply with a nonconforming inner link = %v, want permanent", err)
	}
	if !strings.Contains(err.Error(), `live symlink "bad"`) || !strings.Contains(err.Error(), "side_by_side") {
		t.Fatalf("the refusal does not name the link and the remedy: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(real, "b.txt")); err == nil {
		t.Fatal("the refused restore still wrote")
	}
}

// The lifecycle matrix of the symlink/hard-link rule: one row per L1–L14 of
// the redesign. The rule decides nothing by the archive's spelling of a name
// when the filesystem may disagree with it — identity comes from Lstat through
// os.Root — so alias rows ask their own filesystem what it treats as one name.

var bothApplyModes = []source.ApplyMode{source.ApplyInPlace, source.ApplySideBySide}

// applyAs runs one archive through the real restorer in the given mode.
func applyAs(t *testing.T, root string, mode source.ApplyMode, headers []tar.Header, bodies []string) error {
	t.Helper()
	s, err := build(t, config(root, true))
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.(source.Restorer).Apply(context.Background(), source.ApplyRequest{
		Mode: mode, Label: "l", Options: jsontext.Value(applyOptions),
	}, bytes.NewReader(applyTar(t, headers, bodies)))
	return err
}

// applyTarget is where a mode unpacks: the root itself or its labelled subdir.
func applyTarget(root string, mode source.ApplyMode) string {
	if mode == source.ApplySideBySide {
		return filepath.Join(root, ".restore-l")
	}
	return root
}

// fsAliasesNames reports whether the filesystem holding root treats probe and
// probeAlias as one name — a case-insensitive or Unicode-normalizing volume
// does, a plain case-sensitive one does not. The rows that depend on it assert
// only what their own filesystem actually does.
func fsAliasesNames(t *testing.T, root, probe, probeAlias string) bool {
	t.Helper()
	if err := os.WriteFile(filepath.Join(root, probe), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := os.Lstat(filepath.Join(root, probeAlias))
	return err == nil
}

// TestLifecycleL1EntryBelowAnArchivedSymlink: a link entry is materialised
// first, so a later entry whose parent in the tree is that link is refused at
// itself — the earlier entries stay, and the write never lands where the link
// points.
func TestLifecycleL1EntryBelowAnArchivedSymlink(t *testing.T) {
	for _, mode := range bothApplyModes {
		t.Run(string(mode), func(t *testing.T) {
			root := t.TempDir()
			err := applyAs(t, root, mode, []tar.Header{
				{Name: "a/", Typeflag: tar.TypeDir, Mode: 0o755},
				{Name: "a/x", Typeflag: tar.TypeSymlink, Linkname: ".."},
				{Name: "a/x/s", Typeflag: tar.TypeSymlink, Linkname: "../../q"},
			}, []string{"", "", ""})
			if err == nil {
				t.Fatal("an entry below an archived symlink was applied")
			}
			if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassPermanent {
				t.Fatalf("class = %q, want permanent (%v)", class, err)
			}
			target := applyTarget(root, mode)
			if link, err := os.Readlink(filepath.Join(target, "a", "x")); err != nil || link != ".." {
				t.Fatalf("a/x = %q, %v, want the link inside the root", link, err)
			}
			for _, name := range []string{"s", "q"} {
				if _, err := os.Lstat(filepath.Join(target, name)); err == nil {
					t.Fatalf("the refused entry still created %s", name)
				}
			}
		})
	}
}

// TestLifecycleL2TargetThatClimbsAfterAName: a ".." after a name is refused
// lexically, before the first write, in both modes.
func TestLifecycleL2TargetThatClimbsAfterAName(t *testing.T) {
	for _, mode := range bothApplyModes {
		t.Run(string(mode), func(t *testing.T) {
			root := t.TempDir()
			err := applyAs(t, root, mode, []tar.Header{
				{Name: "a/", Typeflag: tar.TypeDir, Mode: 0o755},
				{Name: "a/x", Typeflag: tar.TypeSymlink, Linkname: ".."},
				{Name: "a/s", Typeflag: tar.TypeSymlink, Linkname: "x/../../q"},
				{Name: "h", Typeflag: tar.TypeLink, Linkname: "missing"},
			}, []string{"", "", "", ""})
			if err == nil || !strings.Contains(err.Error(), "climbs after descending") {
				t.Fatalf("Apply = %v, want the lexical refusal", err)
			}
			if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassPermanent {
				t.Fatalf("class = %q, want permanent", class)
			}
			entries, readErr := os.ReadDir(applyTarget(root, mode))
			if readErr == nil && len(entries) != 0 {
				t.Fatalf("the lexical refusal still wrote %d entries", len(entries))
			}
		})
	}
}

// TestLifecycleL2bHardLinkToANonRegularTarget: a link whose target is not a
// regular file in the restored tree is refused at that entry; what came before
// stays and no link to a missing path exists.
func TestLifecycleL2bHardLinkToANonRegularTarget(t *testing.T) {
	for _, mode := range bothApplyModes {
		t.Run(string(mode), func(t *testing.T) {
			root := t.TempDir()
			err := applyAs(t, root, mode, []tar.Header{
				{Name: "a/", Typeflag: tar.TypeDir, Mode: 0o755},
				{Name: "a/x", Typeflag: tar.TypeSymlink, Linkname: ".."},
				{Name: "a/s", Typeflag: tar.TypeSymlink, Linkname: "x/q"},
				{Name: "h", Typeflag: tar.TypeLink, Linkname: "missing"},
			}, []string{"", "", "", ""})
			if err == nil {
				t.Fatal("a hard link to a missing target was created")
			}
			if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassPermanent {
				t.Fatalf("class = %q, want permanent (%v)", class, err)
			}
			target := applyTarget(root, mode)
			for name, want := range map[string]string{"a/x": "..", "a/s": "x/q"} {
				if link, err := os.Readlink(filepath.Join(target, filepath.FromSlash(name))); err != nil || link != want {
					t.Fatalf("%s = %q, %v, want %q", name, link, err, want)
				}
			}
			if _, err := os.Lstat(filepath.Join(target, "h")); err == nil {
				t.Fatal("the refused hard link still landed in the root")
			}
		})
	}
}

// TestLifecycleL3HardLinkThroughANameAlias: the target is looked up through
// the filesystem, so an alias of an archived symlink is seen as the symlink it
// is. On a case-sensitive volume the alias is a different name and the link is
// an ordinary hard link to the regular file.
func TestLifecycleL3HardLinkThroughANameAlias(t *testing.T) {
	root := t.TempDir()
	aliases := fsAliasesNames(t, root, "probe", "PROBE")
	err := applyAs(t, root, source.ApplyInPlace, []tar.Header{
		{Name: "deep/", Typeflag: tar.TypeDir, Mode: 0o755},
		{Name: "deep/x", Typeflag: tar.TypeReg, Mode: 0o644},
		{Name: "deep/X", Typeflag: tar.TypeSymlink, Linkname: ".."},
		{Name: "escape", Typeflag: tar.TypeLink, Linkname: "deep/x"},
	}, []string{"", "x", "", ""})
	// Where the volume merges the two names, the target is the aliased
	// symlink and the link is refused; where they are distinct, it is the
	// regular file and the link is created.
	wantClass := pluginerrors.ClassPermanent
	if !aliases {
		wantClass = ""
	}
	if class, _ := pluginerrors.ClassOf(err); class != wantClass {
		t.Fatalf("class = %q, want %q (%v)", class, wantClass, err)
	}
	_, statErr := os.Lstat(filepath.Join(root, "escape"))
	if linked := statErr == nil; linked != !aliases {
		t.Fatalf("escape exists = %v, want %v", linked, !aliases)
	}
}

// TestLifecycleL4EntryThroughANameAlias: the same name rule decides, not the
// archive's spelling — an entry whose parent is an alias of an archived
// symlink is refused; on a case-sensitive volume the parent is absent and the
// write fails as it always has.
func TestLifecycleL4EntryThroughANameAlias(t *testing.T) {
	for _, mode := range bothApplyModes {
		t.Run(string(mode), func(t *testing.T) {
			root := t.TempDir()
			aliases := fsAliasesNames(t, root, "probe", "PROBE")
			err := applyAs(t, root, mode, []tar.Header{
				{Name: "deep/", Typeflag: tar.TypeDir, Mode: 0o755},
				{Name: "deep/x", Typeflag: tar.TypeSymlink, Linkname: ".."},
				{Name: "deep/X/foo", Typeflag: tar.TypeSymlink, Linkname: "../../bar"},
			}, []string{"", "", ""})
			if err == nil {
				t.Fatal("an entry under a name alias of a symlink was applied")
			}
			// The merged name is refused at the archived symlink; a distinct
			// parent is simply absent, as it always was.
			wantClass := pluginerrors.ClassTransient
			if aliases {
				wantClass = pluginerrors.ClassPermanent
			}
			if class, _ := pluginerrors.ClassOf(err); class != wantClass {
				t.Fatalf("class = %q, want %q (%v)", class, wantClass, err)
			}
			if _, err := os.Lstat(filepath.Join(applyTarget(root, mode), "foo")); err == nil {
				t.Fatal("the refused entry still created the escape link")
			}
		})
	}
}

// TestLifecycleL5EntryThroughANormalizationAlias: a Unicode-equivalent
// spelling of a directory is the same directory where the volume normalizes;
// the entry is refused at the archived symlink inside it, and nowhere else.
func TestLifecycleL5EntryThroughANormalizationAlias(t *testing.T) {
	const nfc, nfd = "café", "café"
	root := t.TempDir()
	aliases := fsAliasesNames(t, root, "xé", "xé")
	err := applyAs(t, root, source.ApplyInPlace, []tar.Header{
		{Name: nfc + "/", Typeflag: tar.TypeDir, Mode: 0o755},
		{Name: nfc + "/up", Typeflag: tar.TypeSymlink, Linkname: ".."},
		{Name: nfd + "/up/foo", Typeflag: tar.TypeSymlink, Linkname: "../../bar"},
	}, []string{"", "", ""})
	if err == nil {
		t.Fatal("an entry under a normalized alias was applied")
	}
	// Normalized names meet the archived symlink; distinct ones leave an
	// absent parent and the same transient fault as any missing directory.
	wantClass := pluginerrors.ClassTransient
	if aliases {
		wantClass = pluginerrors.ClassPermanent
	}
	if class, _ := pluginerrors.ClassOf(err); class != wantClass {
		t.Fatalf("class = %q, want %q (%v)", class, wantClass, err)
	}
	if _, err := os.Lstat(filepath.Join(root, "foo")); err == nil {
		t.Fatal("the refused entry still created the escape link")
	}
}

// TestLifecycleL6LiveSymlinkParent: a live symlink parent inside the root
// refuses the entry at itself, and no link appears where it would have
// written.
func TestLifecycleL6LiveSymlinkParent(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "a"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("..", filepath.Join(root, "a", "d")); err != nil {
		t.Fatal(err)
	}
	err := applyAs(t, root, source.ApplyInPlace, []tar.Header{
		{Name: "a/d/s", Typeflag: tar.TypeSymlink, Linkname: "../../x"},
	}, []string{""})
	if err == nil {
		t.Fatal("an entry below a live symlink was applied")
	}
	if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassPermanent {
		t.Fatalf("class = %q, want permanent (%v)", class, err)
	}
	if _, err := os.Lstat(filepath.Join(root, "x")); err == nil {
		t.Fatal("the refused entry still created the escape link")
	}
}

// TestLifecycleL8CancelLeavesNoDangerousState: cancellation in the middle of a
// large body ends the apply with the caller's error, and every link the apply
// already created stays inside — no post-check runs on the way out.
func TestLifecycleL8CancelLeavesNoDangerousState(t *testing.T) {
	root := t.TempDir()
	body := strings.Repeat("x", 1<<20)
	archive := applyTar(t, []tar.Header{
		{Name: "b/", Typeflag: tar.TypeDir, Mode: 0o755},
		{Name: "b/q", Typeflag: tar.TypeSymlink, Linkname: ".."},
		{Name: "b/s", Typeflag: tar.TypeSymlink, Linkname: "q/z"},
		{Name: "big.bin", Typeflag: tar.TypeReg, Mode: 0o644},
	}, []string{"", "", "", body})
	s, err := build(t, config(root, true))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err = s.(source.Restorer).Apply(ctx, source.ApplyRequest{
		Mode: source.ApplyInPlace, Label: "l", Options: jsontext.Value(applyOptions),
	}, &cancelMidBody{Reader: bytes.NewReader(archive), cancel: cancel, armAt: 300 << 10})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Apply = %v, want context.Canceled", err)
	}
	for name, want := range map[string]string{"b/q": "..", "b/s": "q/z"} {
		if link, err := os.Readlink(filepath.Join(root, name)); err != nil || link != want {
			t.Fatalf("%s = %q, %v, want %q", name, link, err, want)
		}
	}
}

// cancelMidBody cancels when the stream has read armAt bytes after Apply's
// own rewind: the caller's stop lands inside the body of the large file.
type cancelMidBody struct {
	*bytes.Reader
	cancel   context.CancelFunc
	armAt    int64
	armed    bool
	consumed int64
}

func (r *cancelMidBody) Seek(offset int64, whence int) (int64, error) {
	// Only the caller's rewind arms: tar.Reader probes a seeker with
	// SeekCurrent while it discards a stream it walks.
	if offset == 0 && whence == io.SeekStart {
		r.armed = true
		r.consumed = 0
	}
	return r.Reader.Seek(offset, whence)
}

func (r *cancelMidBody) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if r.armed {
		r.consumed += int64(n)
		if r.consumed > r.armAt {
			r.cancel()
		}
	}
	return n, err
}

// TestLifecycleL9TheTargetRuleIsSharedByProbeAndDump: one lexical rule decides
// both directions — a leading climb within the link's own depth is carried,
// everything else is refused by Probe and left out (and counted) by Dump.
func TestLifecycleL9TheTargetRuleIsSharedByProbeAndDump(t *testing.T) {
	cases := map[string]struct {
		name, target string
		carried      bool
	}{
		"a leading climb within the link's depth": {name: "releases/1/log", target: "../../shared/log", carried: true},
		"a single climb above a nested link":      {name: "node_modules/.bin/a", target: "../a/cli.js", carried: true},
		"a dot component":                         {name: "a/s", target: "./x", carried: true},
		"an empty component":                      {name: "a/s", target: "a//b", carried: true},
		"a plain relative target":                 {name: "a/s", target: "b", carried: true},
		"a climb after a name":                    {name: "a/s", target: "x/../y", carried: false},
		"a climb deeper than the link":            {name: "a/s", target: "../../x", carried: false},
		"a climb at the root":                     {name: "s", target: "..", carried: false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			hdr := tar.Header{Name: tc.name, Typeflag: tar.TypeSymlink, Linkname: tc.target}
			archive := applyTar(t, []tar.Header{hdr}, []string{""})
			s, err := build(t, config(t.TempDir(), true))
			if err != nil {
				t.Fatal(err)
			}
			_, err = s.(source.Prober).Probe(context.Background(), bytes.NewReader(archive))
			if tc.carried != (err == nil) {
				t.Fatalf("Probe = %v, want carried=%v", err, tc.carried)
			}
			// Dump the same shape from a real tree: a refused link must not
			// fail the dump, only be counted.
			dumpRoot := t.TempDir()
			if err := os.MkdirAll(filepath.Join(dumpRoot, filepath.FromSlash(filepath.Dir(tc.name))), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(tc.target, filepath.Join(dumpRoot, filepath.FromSlash(tc.name))); err != nil {
				t.Fatal(err)
			}
			dumper, err := build(t, config(dumpRoot, true))
			if err != nil {
				t.Fatal(err)
			}
			var buf bytes.Buffer
			if err := dumper.Dump(context.Background(), &buf); err != nil {
				t.Fatalf("Dump failed on a link it must only leave out: %v", err)
			}
			stats, err := dumper.(source.DumpStatsReporter).DumpStats(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			wantSkipped := int64(0)
			if !tc.carried {
				wantSkipped = 1
			}
			if stats.SkippedEntries != wantSkipped {
				t.Fatalf("SkippedEntries = %d, want %d", stats.SkippedEntries, wantSkipped)
			}
			carried := dumpedName(t, &buf, filepath.ToSlash(tc.name))
			if carried != tc.carried {
				t.Fatalf("the dumped archive carries %q = %v, want %v", tc.name, carried, tc.carried)
			}
		})
	}
}

// dumpedName reports whether the archive bytes carry an entry of name.
func dumpedName(t *testing.T, archive *bytes.Buffer, name string) bool {
	t.Helper()
	tr := tar.NewReader(bytes.NewReader(archive.Bytes()))
	for {
		hdr, err := tr.Next()
		if err != nil {
			return false
		}
		if hdr.Name == name || hdr.Name == name+"/" {
			return true
		}
	}
}

// TestLifecycleL10RoundTrip: what Dump writes, Apply restores — nested "../"
// links, a broken link and hard links survive the round trip; a link with a
// climb after a name never enters the stream and is counted.
func TestLifecycleL10RoundTrip(t *testing.T) {
	src := t.TempDir()
	for _, dir := range []string{"releases/1", "node_modules/.bin", "a2"} {
		if err := os.MkdirAll(filepath.Join(src, filepath.FromSlash(dir)), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	links := map[string]string{
		"releases/1/log":      "../../shared/log",
		"node_modules/.bin/a": "../a/cli.js",
		"broken":              "nowhere",
	}
	for name, target := range links {
		if err := os.Symlink(target, filepath.Join(src, filepath.FromSlash(name))); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink("x/../q", filepath.Join(src, "a2", "s")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "a.txt"), []byte("linked"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(filepath.Join(src, "a.txt"), filepath.Join(src, "b.txt")); err != nil {
		t.Fatal(err)
	}

	dumper, err := build(t, config(src, true))
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := dumper.Dump(context.Background(), &buf); err != nil {
		t.Fatal(err)
	}
	stats, err := dumper.(source.DumpStatsReporter).DumpStats(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.SkippedEntries != 1 {
		t.Fatalf("SkippedEntries = %d, want the one climb-after-a-name link", stats.SkippedEntries)
	}

	dst := t.TempDir()
	restorer, err := build(t, config(dst, true))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := restorer.(source.Restorer).Apply(context.Background(), source.ApplyRequest{
		Mode: source.ApplySideBySide, Label: "l", Options: jsontext.Value(applyOptions),
	}, bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatal(err)
	}
	target := applyTarget(dst, source.ApplySideBySide)
	for name, want := range links {
		got, err := os.Readlink(filepath.Join(target, filepath.FromSlash(name)))
		if err != nil || got != want {
			t.Fatalf("%s = %q, %v, want %q", name, got, err, want)
		}
	}
	a, err := os.Stat(filepath.Join(target, "a.txt"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.Stat(filepath.Join(target, "b.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(a, b) {
		t.Fatal("b.txt is not a hard link to a.txt after the round trip")
	}
	if _, err := os.Lstat(filepath.Join(target, "a2", "s")); err == nil {
		t.Fatal("the refused link entered the restored tree")
	}
}

// TestLifecycleL11ALateSymlinkChangesAnEarlyOne: links are created in archive
// order and each stays inside the root whatever a later entry does.
func TestLifecycleL11ALateSymlinkChangesAnEarlyOne(t *testing.T) {
	root := t.TempDir()
	if err := applyAs(t, root, source.ApplyInPlace, []tar.Header{
		{Name: "b/", Typeflag: tar.TypeDir, Mode: 0o755},
		{Name: "b/s", Typeflag: tar.TypeSymlink, Linkname: "q/z"},
		{Name: "b/q", Typeflag: tar.TypeSymlink, Linkname: ".."},
	}, []string{"", "", ""}); err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]string{"b/s": "q/z", "b/q": ".."} {
		if got, err := os.Readlink(filepath.Join(root, name)); err != nil || got != want {
			t.Fatalf("%s = %q, %v, want %q", name, got, err, want)
		}
	}
}

// TestLifecycleL12ADirectoryIsNeverReplaced: a link entry on an existing
// directory is refused and the directory with its children stays.
func TestLifecycleL12ADirectoryIsNeverReplaced(t *testing.T) {
	cases := []struct {
		name  string
		alias bool
	}{
		{name: "a"},
		{name: "A", alias: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			aliases := fsAliasesNames(t, root, "probe", "PROBE")
			err := applyAs(t, root, source.ApplyInPlace, []tar.Header{
				{Name: "a/", Typeflag: tar.TypeDir, Mode: 0o755},
				{Name: "a/kid", Typeflag: tar.TypeReg, Mode: 0o644},
				{Name: tc.name, Typeflag: tar.TypeSymlink, Linkname: "x"},
			}, []string{"", "x", ""})
			// "a" is a directory the link may not replace; "A" is that same
			// directory where the volume merges the names, and a fresh link
			// where it does not.
			refused := !tc.alias || aliases
			wantClass := pluginerrors.Class("")
			if refused {
				wantClass = pluginerrors.ClassPermanent
			}
			if class, _ := pluginerrors.ClassOf(err); class != wantClass {
				t.Fatalf("class = %q, want %q (%v)", class, wantClass, err)
			}
			if refused && !strings.Contains(err.Error(), "is a directory") {
				t.Fatalf("error = %v, want it to name the directory", err)
			}
			if got, err := os.ReadFile(filepath.Join(root, "a", "kid")); err != nil || string(got) != "x" {
				t.Fatalf("the directory's child = %q, %v", got, err)
			}
			info, statErr := os.Lstat(filepath.Join(root, tc.name))
			if gotDir := statErr == nil && info.IsDir(); gotDir != refused {
				t.Fatalf("%s is a directory = %v, want %v", tc.name, gotDir, refused)
			}
		})
	}
}

// TestLifecycleL13HardLinkToAnArchivedSymlink: a link entry on a symlink
// already materialised is refused — its relative target is never moved to
// another depth.
func TestLifecycleL13HardLinkToAnArchivedSymlink(t *testing.T) {
	root := t.TempDir()
	err := applyAs(t, root, source.ApplyInPlace, []tar.Header{
		{Name: "d/", Typeflag: tar.TypeDir, Mode: 0o755},
		{Name: "d/s", Typeflag: tar.TypeSymlink, Linkname: ".."},
		{Name: "h", Typeflag: tar.TypeLink, Linkname: "d/s"},
	}, []string{"", "", ""})
	if err == nil {
		t.Fatal("a hard link to an archived symlink was created")
	}
	if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassPermanent {
		t.Fatalf("class = %q, want permanent (%v)", class, err)
	}
	if _, err := os.Lstat(filepath.Join(root, "h")); err == nil {
		t.Fatal("the refused link still landed in the root")
	}
}

// TestLifecycleL14HardLinkToALiveRegularFile: a regular file already in the
// root is a legitimate target — the link is created and shares its inode.
func TestLifecycleL14HardLinkToALiveRegularFile(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "t"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := applyAs(t, root, source.ApplyInPlace, []tar.Header{
		{Name: "h", Typeflag: tar.TypeLink, Linkname: "t"},
	}, []string{""}); err != nil {
		t.Fatal(err)
	}
	a, err := os.Stat(filepath.Join(root, "t"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.Stat(filepath.Join(root, "h"))
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(a, b) {
		t.Fatal("h is not a hard link to the live regular file t")
	}
}

// TestLifecycleLHardLinkOnItsOwnName: the target is read before the entry's own
// name is replaced, so link-on-itself fails at the link, never writes, and the
// transient class stays what it was.
func TestLifecycleLHardLinkOnItsOwnName(t *testing.T) {
	root := t.TempDir()
	err := applyAs(t, root, source.ApplyInPlace, []tar.Header{
		{Name: "a", Typeflag: tar.TypeReg, Mode: 0o644},
		{Name: "a", Typeflag: tar.TypeLink, Linkname: "a"},
	}, []string{"x", ""})
	if err == nil {
		t.Fatal("a hard link onto its own name was created")
	}
	if _, err := os.Lstat(filepath.Join(root, "a")); err == nil {
		t.Fatal("the entry remained after the failed link")
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
	if err := s.(instances.Checker).Check(ctx); err == nil {
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
	if err := notDir.(instances.Checker).Check(context.Background()); err == nil {
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
	if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassPermanent {
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

func TestApplyRefusesACancelledContextAndDefaultsTheLog(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := instances.Build(ctx, testRegistry(t), source.Kind, "files",
		jsontext.Value(config(tree(t), true)), instances.Deps{Instance: "files"}); err == nil {
		t.Fatal("New accepted a cancelled context")
	}
	// A nil logger is replaced rather than dereferenced later.
	s, err := instances.Build(context.Background(), testRegistry(t), source.Kind, "files",
		jsontext.Value(config(tree(t), true)), instances.Deps{Instance: "files"})
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

// TestApplyReportsARootItCannotCreate: an in_place restore whose root is a
// plain file cannot make a directory of it, and the failure is transient.
func TestApplyReportsARootItCannotCreate(t *testing.T) {
	file := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	err := applyAs(t, file, source.ApplyInPlace, []tar.Header{
		{Name: "a.txt", Typeflag: tar.TypeReg, Mode: 0o644},
	}, []string{"x"})
	if err == nil {
		t.Fatal("Apply made a restore root of a plain file")
	}
	if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassTransient {
		t.Fatalf("class = %q, want transient (%v)", class, err)
	}
}

// TestLifecycleL12bDirectoryEntryOnAnArchivedSymlink: a directory entry sitting
// on a symlink an earlier entry materialised is refused, the same rule that
// keeps the link's own location stable.
func TestLifecycleL12bDirectoryEntryOnAnArchivedSymlink(t *testing.T) {
	root := t.TempDir()
	err := applyAs(t, root, source.ApplyInPlace, []tar.Header{
		{Name: "a/", Typeflag: tar.TypeDir, Mode: 0o755},
		{Name: "a/x", Typeflag: tar.TypeSymlink, Linkname: ".."},
		{Name: "a/x/", Typeflag: tar.TypeDir, Mode: 0o755},
	}, []string{"", "", ""})
	if err == nil {
		t.Fatal("a directory entry was materialised through an archived symlink")
	}
	if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassPermanent {
		t.Fatalf("class = %q, want permanent (%v)", class, err)
	}
	if link, err := os.Readlink(filepath.Join(root, "a", "x")); err != nil || link != ".." {
		t.Fatalf("a/x = %q, %v, want the archived link", link, err)
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
// — the restore workflow hands this plugin a file.
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
	if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassPermanent {
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
	if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassPermanent {
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
