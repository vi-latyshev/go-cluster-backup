package files

import (
	"archive/tar"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/configuration"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/io"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source"
)

// ApplyOptions is restore.apply.files: the knobs that shape one restore.
type ApplyOptions struct {
	PreserveOwner bool                   `json:"preserve_owner" doc:"restore uid/gid from the archive; needs the privilege to chown"`
	SideBySideDir configuration.Template `json:"side_by_side_dir" doc:"subdirectory of root a side-by-side restore unpacks into; \"{{ .Label }}\" is the run id"`
}

// renderedOptions is the decoded and rendered form Apply actually uses.
type renderedOptions struct {
	preserveOwner bool
	sideBySideDir string
}

// Apply unpacks the artifact under this instance's root. The mode is mandatory:
// side_by_side unpacks into a labelled subdirectory of the root that must not
// exist yet; in_place unpacks over the root, replacing files as it goes (the
// unpack itself is not atomic).
//
// The stream is validated by Probe before the first write, then rewound: the
// artifact is read in place, never copied into the process temp dir. Every path
// is resolved through one os.Root handle, so no entry — a name, a symlink target
// or a hard link source — can address anything outside the restore root.
//
// The confinement is an induction over every link a later resolution can meet,
// created or live, and it rests on one lexical rule: after skipping "" and ".",
// a target is a leading run of ".." no deeper than the link's own directory,
// followed only by descending names. A created link lies at its cleaned path —
// Apply never replaces a directory — so its climbs walk real directories and the
// rest only descends; a live link in an in_place tree lies at its walked path
// for the same reason, and the live pre-pass refuses one whose target breaks the
// rule there, because a following ".." would climb from wherever an earlier name
// points. An archive may replace a live link, but what replaces it obeys the
// rule too. A side_by_side target is fresh, so it holds no live links. The root
// path itself may be a symlink — the operator's choice, already followed when
// the root is opened — so the walk judges only links inside it.
//
// Probe decides the archive's lexical class before the first write; a live link
// that breaks the rule, an entry whose parent in the restored tree is a symlink,
// and a hard link whose target is not a regular file there, are refused as they
// are reached — in_place is not atomic and a side_by_side target is then
// partial, as with any unpack error.
func (s *Source) Apply(ctx context.Context, req source.ApplyRequest, src io.Reader) (source.ApplyReport, error) {
	if err := ctx.Err(); err != nil {
		return source.ApplyReport{}, err
	}
	if req.Mode != source.ApplySideBySide && req.Mode != source.ApplyInPlace {
		return source.ApplyReport{}, fmt.Errorf("apply mode %q must be side_by_side or in_place", req.Mode)
	}
	if req.Label == "" {
		return source.ApplyReport{}, errors.New("apply label is empty; a side-by-side target is always labelled with the run id")
	}
	opts, err := decodeApplyOptions(req.Options, req.Label)
	if err != nil {
		return source.ApplyReport{}, err
	}

	rootPath := s.cfg.Root
	if req.Mode == source.ApplySideBySide {
		rootPath, err = sideBySideSubdir(rootPath, opts.sideBySideDir)
		if err != nil {
			return source.ApplyReport{}, pluginerrors.Permanent(err)
		}
		if _, err := os.Lstat(rootPath); err == nil {
			return source.ApplyReport{}, pluginerrors.Permanent(fmt.Errorf("%s already exists; a side-by-side target is never overwritten", rootPath))
		} else if !errors.Is(err, os.ErrNotExist) {
			return source.ApplyReport{}, pluginerrors.Transient(err)
		}
	}

	seeker, ok := src.(io.ReadSeeker)
	if !ok {
		return source.ApplyReport{}, pluginerrors.Permanent(errors.New("restore.apply.files needs a seekable artifact: the archive is probed and then rewound"))
	}
	if _, err := s.Probe(ctx, seeker); err != nil {
		return source.ApplyReport{}, err
	}
	if _, err := seeker.Seek(0, io.SeekStart); err != nil {
		return source.ApplyReport{}, pluginerrors.Transient(err)
	}

	if err := os.MkdirAll(rootPath, 0o755); err != nil {
		return source.ApplyReport{}, pluginerrors.Transient(err)
	}
	root, err := openRoot(rootPath)
	if err != nil {
		return source.ApplyReport{}, pluginerrors.Transient(err)
	}
	defer func() { _ = root.Close() }()
	if req.Mode == source.ApplyInPlace {
		if err := checkLiveLinks(ctx, root); err != nil {
			return source.ApplyReport{}, err
		}
	}

	written, err := extract(ctx, root, seeker, opts.preserveOwner)
	if err != nil {
		return source.ApplyReport{}, err
	}
	return source.ApplyReport{Target: rootPath, Bytes: written}, nil
}

// countingReader counts the bytes the tar reader pulled from the stream: a
// zero-byte artifact reaches io.EOF without ever carrying the end-of-archive
// marker, and tar.Reader reports that as a clean end.
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// Probe walks the archive and reads every entry without writing anything. An
// empty artifact is refused: a tar stream always carries at least its
// end-of-archive marker, so nothing at all is a truncated file, not an empty
// tree.
func (s *Source) Probe(ctx context.Context, src io.Reader) (source.ProbeReport, error) {
	if err := ctx.Err(); err != nil {
		return source.ProbeReport{}, err
	}
	counted := &countingReader{r: src}
	tr := tar.NewReader(counted)
	var report source.ProbeReport
	for {
		if err := ctx.Err(); err != nil {
			return source.ProbeReport{}, err
		}
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return source.ProbeReport{}, pluginerrors.Transient(fmt.Errorf("reading the tar stream: %w", err))
		}
		if err := validateEntry(hdr); err != nil {
			return source.ProbeReport{}, pluginerrors.Permanent(err)
		}
		report.Entries++
		n, err := pluginio.CopyContext(ctx, io.Discard, tr)
		if err != nil {
			return source.ProbeReport{}, err
		}
		report.Bytes += n
	}
	if counted.n == 0 {
		return source.ProbeReport{}, pluginerrors.Permanent(errors.New("the artifact is empty: a tar stream always carries its end-of-archive marker"))
	}
	return report, nil
}

// lstatRoot reads one path through the restore root. It is a variable so a test
// can make a path answer with an error no filesystem produces on demand.
var lstatRoot = func(root *os.Root, name string) (fs.FileInfo, error) { return root.Lstat(name) }

// parentRefusal answers why an entry may not be written where its name says: a
// parent of the entry's cleaned name exists through the root and is a symlink,
// so os.Root would resolve the operation through it. Its physical location,
// not the archive's spelling of the name, is what counts — that is why a
// case-insensitive filesystem and a live alias are caught here and no longer
// need a pre-pass over names.
//
// verified caches the prefixes already seen as real directories. The cache is
// sound because Apply never replaces or removes a directory: replaceEntry
// refuses one, ensureDir refuses a symlink or a non-directory. A missing parent
// or a non-directory one stops the walk: the operation below creates the
// directory or fails exactly as it does today.
func parentRefusal(root *os.Root, name string, verified map[string]struct{}) error {
	var prefixes []string
	for dir := path.Dir(name); dir != "."; dir = path.Dir(dir) {
		prefixes = append(prefixes, dir)
	}
	for i := len(prefixes) - 1; i >= 0; i-- {
		dir := prefixes[i]
		if _, ok := verified[dir]; ok {
			continue
		}
		info, err := lstatRoot(root, dir)
		switch {
		case err == nil && info.Mode()&fs.ModeSymlink != 0:
			return pluginerrors.Permanent(fmt.Errorf("tar entry %q is written through the symlink %q", name, dir))
		case err == nil && info.IsDir():
			verified[dir] = struct{}{}
		case err == nil:
			return nil
		case errors.Is(err, fs.ErrNotExist):
			return nil
		default:
			return pluginerrors.Transient(err)
		}
	}
	return nil
}

// readlinkRoot reads one link through the opened restore root. It is a variable
// so a test can make a link it cannot read.
var readlinkRoot = func(root *os.Root, name string) (string, error) { return root.Readlink(name) }

// openRoot opens the restore root. It is a variable so a test can make the open
// itself fail: after MkdirAll that is otherwise a race.
var openRoot = os.OpenRoot

// checkLiveLinks refuses an in_place restore into a tree holding a live symlink
// whose target the archive rule would refuse at that link's own path: the same
// entryRefusal decides, so no second copy of the rule exists. A live link that
// walks ".." after a name, climbs deeper than its own directory, or points at an
// absolute path is one a restore may not depend on — the caller restores
// side_by_side into a fresh directory instead.
//
// The walk is over the opened root's own FS, so the root path may itself be a
// symlink — that is the operator's choice and os.OpenRoot followed it — while
// only links inside the root are judged. The FS walk never follows a link and
// every name it yields is the link's own.
func checkLiveLinks(ctx context.Context, root *os.Root) error {
	return fs.WalkDir(root.FS(), ".", func(name string, d fs.DirEntry, err error) error {
		if err != nil {
			return pluginerrors.Transient(err)
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if d.Type()&fs.ModeSymlink == 0 || name == "." {
			return nil
		}
		target, err := readlinkRoot(root, name)
		if err != nil {
			return pluginerrors.Transient(err)
		}
		if why := entryRefusal(&tar.Header{Name: name, Typeflag: tar.TypeSymlink, Linkname: target}); why != "" {
			return pluginerrors.Permanent(fmt.Errorf("live symlink %q is not one a restore may depend on: %s; restore side_by_side into a fresh directory", name, why))
		}
		return nil
	})
}

// entryRefusal is the one rule about what this package carries, and both
// directions ask it: a Dump leaves out an entry it refuses and counts it, an
// Apply refuses the whole archive for one. Nothing else decides, so an archive
// this package wrote is always one it accepts: every name is relative and
// ".."-free after Clean, every symlink target stays lexically inside the root,
// and every entry has a type this package can materialise. The returned string
// is the reason, "" when the entry is carried.
func entryRefusal(hdr *tar.Header) string {
	name := hdr.Name
	if name == "" || strings.ContainsRune(name, 0) {
		return "the name is empty or carries a NUL"
	}
	switch hdr.Typeflag {
	case tar.TypeDir, tar.TypeSymlink, tar.TypeLink, tar.TypeReg:
	default:
		return fmt.Sprintf("the type %q is not a directory, a symlink, a hard link or a regular file", string(rune(hdr.Typeflag)))
	}
	clean := path.Clean(name)
	if clean == "." || path.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, "../") {
		return "the name escapes the restore root"
	}
	switch hdr.Typeflag {
	case tar.TypeSymlink:
		target := hdr.Linkname
		if target == "" || strings.ContainsRune(target, 0) {
			return "the symlink target is empty or carries a NUL"
		}
		if path.IsAbs(target) {
			return fmt.Sprintf("the symlink points at the absolute path %q", target)
		}
		// A target may climb with a leading run of "..", but no deeper than
		// the directory the link itself sits in, and a ".." after a name is
		// refused: the kernel resolves ".." physically, so once a name on the
		// way may be a symlink, ".." climbs from wherever that link points.
		// With a leading ".." run bound by the link's own depth, every climb
		// walks a real directory of the restored tree, and everything after it
		// only descends. Extraction additionally refuses to write through a
		// symlink parent, so a name here is a directory, not a link.
		up, named := 0, false
		for _, component := range strings.Split(target, "/") {
			switch component {
			case "", ".":
			case "..":
				if named {
					return fmt.Sprintf("the symlink target %q climbs after descending", target)
				}
				up++
			default:
				named = true
			}
		}
		if up > targetDepth(path.Dir(clean)) {
			return fmt.Sprintf("the symlink points at %q, outside the restore root", target)
		}
	case tar.TypeLink:
		link := path.Clean(hdr.Linkname)
		if path.IsAbs(link) || link == "." || link == ".." || strings.HasPrefix(link, "../") {
			return fmt.Sprintf("the hard link points at %q, outside the restore root", hdr.Linkname)
		}
	}
	return ""
}

// targetDepth is how many ".." a link directly under dir may carry, "." being
// the restore root itself.
func targetDepth(dir string) int {
	if dir == "." {
		return 0
	}
	return strings.Count(dir, "/") + 1
}

// validateEntry says why an archive cannot be unpacked, nil when it can: the
// same rule Dump writes with, so an archive that passes here can be unpacked
// without a lexical refusal.
func validateEntry(hdr *tar.Header) error {
	if why := entryRefusal(hdr); why != "" {
		return fmt.Errorf("tar entry %q: %s", hdr.Name, why)
	}
	return nil
}

// dirMode is one directory entry whose final mode and owner wait for the last
// pass: a directory's children are created after the entry itself, and a tree
// whose parent is 0555 cannot take them.
type dirMode struct {
	name string
	mode fs.FileMode
	uid  int
	gid  int
}

// extract unpacks the validated archive under root and returns the bytes of
// regular files written. Every path goes through root, so a symlink anywhere in
// the live tree cannot lead an entry out of it. Directory modes and owners land
// after every child exists, deepest first.
func extract(ctx context.Context, root *os.Root, src io.Reader, preserveOwner bool) (int64, error) {
	tr := tar.NewReader(src)
	var written int64
	var dirs []dirMode
	// verified is parentRefusal's cache of directory prefixes. It stays sound
	// because Apply never replaces a directory: replaceEntry refuses one and
	// ensureDir refuses a symlink or a non-directory.
	verified := make(map[string]struct{})
	for {
		if err := ctx.Err(); err != nil {
			return written, err
		}
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return written, pluginerrors.Transient(fmt.Errorf("reading the tar stream: %w", err))
		}
		name := path.Clean(hdr.Name)
		if err := parentRefusal(root, name, verified); err != nil {
			return written, err
		}
		switch hdr.Typeflag {
		case tar.TypeDir:
			mode := fs.FileMode(hdr.Mode) & 0o777
			if err := ensureDir(root, name, mode); err != nil {
				return written, err
			}
			verified[name] = struct{}{}
			dirs = append(dirs, dirMode{name: name, mode: mode, uid: hdr.Uid, gid: hdr.Gid})
			continue
		case tar.TypeSymlink:
			if err := replaceEntry(root, name); err != nil {
				return written, err
			}
			if err := root.Symlink(hdr.Linkname, name); err != nil {
				return written, pluginerrors.Transient(err)
			}
		case tar.TypeLink:
			// Only a regular file in the restored tree may be linked. Lstat does
			// not follow the final component, so a symlink — archived or live —
			// is seen as what it is and refused: a hard link to it would carry
			// its relative target to another depth, possibly out of the root.
			target := path.Clean(hdr.Linkname)
			info, err := lstatRoot(root, target)
			if err != nil || !info.Mode().IsRegular() {
				return written, pluginerrors.Permanent(fmt.Errorf("tar entry %q is a hard link to %q, which is not a regular file in the restored tree", hdr.Name, target))
			}
			if err := replaceEntry(root, name); err != nil {
				return written, err
			}
			if err := root.Link(target, name); err != nil {
				return written, pluginerrors.Transient(err)
			}
		case tar.TypeReg:
			if err := replaceEntry(root, name); err != nil {
				return written, err
			}
			n, err := writeFile(ctx, root, name, fs.FileMode(hdr.Mode)&0o777, tr)
			if err != nil {
				return written, err
			}
			written += n
			if !hdr.ModTime.IsZero() {
				_ = root.Chtimes(name, time.Now(), hdr.ModTime)
			}
		default:
			return written, pluginerrors.Permanent(fmt.Errorf("tar entry %q has unsupported type %q", hdr.Name, string(rune(hdr.Typeflag))))
		}
		if preserveOwner {
			if err := root.Lchown(name, hdr.Uid, hdr.Gid); err != nil {
				return written, pluginerrors.Permanent(fmt.Errorf("restoring owner %d:%d of %q: %w", hdr.Uid, hdr.Gid, name, err))
			}
		}
	}
	// Every child exists now, so the archive's own modes and owners can land
	// without standing in the way of its own extraction. Deepest first: closing
	// a child's directory only after its parent's mode is final would make a
	// read-only parent refuse the last change.
	sort.Slice(dirs, func(i, j int) bool {
		return strings.Count(dirs[i].name, "/") > strings.Count(dirs[j].name, "/")
	})
	for _, d := range dirs {
		if preserveOwner {
			if err := root.Lchown(d.name, d.uid, d.gid); err != nil {
				return written, pluginerrors.Permanent(fmt.Errorf("restoring owner %d:%d of %q: %w", d.uid, d.gid, d.name, err))
			}
		}
		if err := chmodRoot(root, d.name, d.mode); err != nil {
			return written, pluginerrors.Transient(err)
		}
	}
	return written, nil
}

// writeFile creates one regular file and streams its content into it.
func writeFile(ctx context.Context, root *os.Root, name string, mode fs.FileMode, src io.Reader) (int64, error) {
	f, err := root.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return 0, pluginerrors.Transient(err)
	}
	n, copyErr := pluginio.CopyContext(ctx, f, src)
	if closeErr := f.Close(); copyErr == nil {
		copyErr = closeErr
	}
	if copyErr != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return n, ctxErr
		}
		return n, pluginerrors.Transient(copyErr)
	}
	if err := chmodRoot(root, name, mode); err != nil {
		return n, pluginerrors.Transient(err)
	}
	return n, nil
}

// ensureDir creates or adopts one directory entry, writable enough for its
// children whatever mode the archive gives it: the final mode lands after them.
func ensureDir(root *os.Root, name string, mode fs.FileMode) error {
	info, err := root.Lstat(name)
	switch {
	case err == nil:
		if info.Mode()&fs.ModeSymlink != 0 {
			return pluginerrors.Permanent(fmt.Errorf("%s is a symlink; refusing to write a directory through it", name))
		}
		if !info.IsDir() {
			return pluginerrors.Permanent(fmt.Errorf("%s is not a directory; refusing to replace it with one", name))
		}
	case errors.Is(err, fs.ErrNotExist):
		if err := root.MkdirAll(name, mode|0o700); err != nil {
			return pluginerrors.Transient(err)
		}
	default:
		return pluginerrors.Transient(err)
	}
	// The umask must not decide the mode the extraction writes through; the
	// archive's own mode is applied once the children exist.
	if err := chmodRoot(root, name, mode|0o700); err != nil {
		return pluginerrors.Transient(err)
	}
	return nil
}

// replaceEntry removes whatever is in the way of a new entry; a directory is
// not in the way of anything this package writes and is refused.
func replaceEntry(root *os.Root, name string) error {
	info, err := root.Lstat(name)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return pluginerrors.Transient(err)
	}
	if info.IsDir() {
		return pluginerrors.Permanent(fmt.Errorf("%s is a directory; refusing to replace it with a file", name))
	}
	if err := root.Remove(name); err != nil {
		return pluginerrors.Transient(err)
	}
	return nil
}

// decodeApplyOptions decodes the options strictly and renders the side-by-side
// directory template against the run label.
func decodeApplyOptions(raw jsontext.Value, label string) (renderedOptions, error) {
	opts, err := configuration.DecodeOptions[ApplyOptions](raw, "restore.apply.files")
	if err != nil {
		return renderedOptions{}, err
	}
	dir, err := opts.SideBySideDir.Render(struct{ Label string }{Label: label})
	if err != nil {
		return renderedOptions{}, fmt.Errorf("restore.apply.files/side_by_side_dir: %w", err)
	}
	if dir == "" {
		return renderedOptions{}, errors.New("restore.apply.files/side_by_side_dir renders empty for this run")
	}
	return renderedOptions{preserveOwner: opts.PreserveOwner, sideBySideDir: dir}, nil
}

// sideBySideSubdir joins the rendered directory onto the root and refuses
// anything that would land outside it.
func sideBySideSubdir(root, dir string) (string, error) {
	clean := filepath.Clean(filepath.FromSlash(dir))
	if filepath.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("side_by_side_dir %q escapes the root", dir)
	}
	return filepath.Join(root, clean), nil
}
