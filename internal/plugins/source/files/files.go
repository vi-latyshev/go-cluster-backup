// Package files archives one directory tree as a single tar stream, read-only
// and without a staging copy. Entries are written in the walk's lexical order,
// with the mode, mtime, uid/gid and symlink targets of the tree; a file that is
// hard-linked twice is stored once and linked afterwards. A file that vanished
// between the walk and its open fails the archive unless the config says the
// tree tolerates deletes — any other read error always fails it.
//
// Dump writes only what an Apply of this package accepts (see entryRefusal), so
// an artifact that was written is one that can be restored: a socket, a FIFO, a
// device node or a link resolving outside the tree never enters the stream. Every
// entry left out that way, and every file that vanished into a tolerated delete,
// is counted instead.
//
// A tar entry has a fixed size, taken from the walk's stat: the copy of one
// file stops at that size, so a file that grows under a live volume cannot
// break the stream; a file that shrank is zero-padded and counted. The counts
// are part of the artifact's report (DumpStats), not only of a log line.
package files

import (
	"archive/tar"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"syscall"

	"github.com/vi-latyshev/go-cluster-backup/internal/human"
	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/must"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/source"
)

// Config is the plugin config. Every field is required.
type Config struct {
	Root           string `json:"root" required:"true" doc:"directory whose tree becomes one tar stream"`
	IgnoreVanished bool   `json:"ignore_vanished" doc:"files deleted between the walk and their open are skipped instead of failing the artifact"`
}

// Register adds the files source to a registry.
func Register(r *plugin.Registry) error {
	return plugin.Register(r, source.Kind, plugin.Spec[Config, source.Source]{
		Type:      "files",
		Summary:   "one directory tree as a single tar stream",
		Stability: plugin.Stable,
		Example:   []byte(`{"root": "/volumes/example/name", "ignore_vanished": true}`),
		New: func(ctx context.Context, cfg Config, deps plugin.Deps) (source.Source, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if cfg.Root == "" {
				return nil, errors.New("root is empty")
			}
			if !filepath.IsAbs(cfg.Root) {
				return nil, fmt.Errorf("root %q must be absolute", cfg.Root)
			}
			log := deps.Log
			if log == nil {
				log = slog.New(slog.DiscardHandler)
			}
			return &Source{cfg: cfg, log: log}, nil
		},
	})
}

type Source struct {
	cfg Config
	log *slog.Logger
	// stats is what the last Dump left out of the stream; DumpStats reports it.
	stats model.DumpStats
	// testBeforeOpen is a test seam, never configuration and nil in every run:
	// it runs between the walk's look at one regular entry and the open that
	// follows it, which is the window a workload owning the volume writes into.
	// It lives here because the walk cannot be reached from outside this
	// package, and a swap a test performs any earlier is a different entry
	// altogether.
	testBeforeOpen func(path string)
}

// DumpStats reports what the Dump calls on this instance left out of the
// stream: the entries an entryRefusal rejected, the files that vanished into a
// tolerated delete, and the files whose tail had to be zero-padded because they
// shrank under the read. The engine records the counts beside the artifact, so
// a tree read while it lived stays visible in the manifest and in the nightly
// message instead of only in a log line.
func (s *Source) DumpStats(ctx context.Context) (model.DumpStats, error) {
	if err := ctx.Err(); err != nil {
		return model.DumpStats{}, err
	}
	return s.stats, nil
}

func (s *Source) Describe(ctx context.Context) (source.SourceTraits, error) {
	if err := ctx.Err(); err != nil {
		return source.SourceTraits{}, err
	}
	return source.SourceTraits{Ext: ".tar", Origin: "file://" + s.cfg.Root}, nil
}

// Check runs at preflight: the tree must be there. A labelled volume that is
// not mounted fails here — as a missing mount it is a deployment fault to fix,
// never silently an empty artifact.
func (s *Source) Check(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	st, err := os.Stat(s.cfg.Root)
	if err != nil {
		return plugin.Permanent(fmt.Errorf("volume tree: %w", err))
	}
	if !st.IsDir() {
		return plugin.Permanent(fmt.Errorf("%s is not a directory", s.cfg.Root))
	}
	return nil
}

// Dump writes the whole tree as one tar stream into dst.
func (s *Source) Dump(ctx context.Context, dst io.Writer) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// The tree is read through a root: every entry is opened relative to it, so
	// no entry can reach anything outside the volume however the tree changes
	// under the walk.
	root, err := os.OpenRoot(s.cfg.Root)
	if err != nil {
		return plugin.Permanent(fmt.Errorf("opening the root %s: %w", s.cfg.Root, err))
	}
	defer func() { _ = root.Close() }()

	s.stats = model.DumpStats{}
	tw := tar.NewWriter(dst)
	seen := map[fileKey]string{}
	err = filepath.WalkDir(s.cfg.Root, func(path string, d fs.DirEntry, walkErr error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if walkErr != nil {
			if s.skip(path, walkErr) {
				return nil
			}
			return walkErr
		}
		if path == s.cfg.Root {
			return nil // the root itself is the stream, not an entry in it
		}
		return s.archiveEntry(ctx, tw, root, path, d, seen)
	})
	if err != nil {
		return err
	}
	return tw.Close()
}

// The filesystem primitives this package drives. Each is a variable so a test
// can make it fail: no filesystem fails them on demand, and a branch no test
// reaches is a branch nobody has run.
var (
	readlink     = os.Readlink
	statOpenFile = func(f *os.File) (fs.FileInfo, error) { return f.Stat() }
	chmodRoot    = func(r *os.Root, name string, mode fs.FileMode) error { return r.Chmod(name, mode) }
)

// sameAsWalked reports whether now carries the same device, inode, change time,
// size and modification time the walk saw.
func sameAsWalked(now, walked fs.FileInfo) bool {
	return fileKeyOf(now) == fileKeyOf(walked) &&
		now.Size() == walked.Size() &&
		now.ModTime().Equal(walked.ModTime())
}

// openCarried opens the regular file the walk described and refuses anything
// that changed under it. The volume belongs to the workload that writes it, and
// the process reading it is root over every namespace's volumes: an entry that
// became a link must not be followed out of the volume (the storage token, the
// discovered secrets and the ServiceAccount token all live in this pod), and one
// that became a FIFO or a device must not be opened — a FIFO would hold the run
// until the job's own deadline, unreported. O_NONBLOCK is what makes that open
// return instead of wait for a writer, and the descriptor is then asked whether
// it is a regular file and whether it is the very file the walk saw.
func openCarried(root *os.Root, rel string, walked fs.FileInfo) (*os.File, error) {
	f, err := root.OpenFile(rel, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		// A refused open is classified by what stands there now: a link — one
		// that leaves the volume is refused without ever being followed — or a
		// special file is a changed entry, while a permission failure or a file
		// that vanished is the volume's own answer.
		if now, lstatErr := root.Lstat(rel); lstatErr == nil && (!now.Mode().IsRegular() || !sameAsWalked(now, walked)) {
			return nil, fmt.Errorf("%w: it is a %s now, not the file the walk saw", errChanged, now.Mode())
		}
		return nil, fmt.Errorf("opening it as the file the walk saw: %w", err)
	}
	st, err := statOpenFile(f)
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("reading what was opened: %w", err)
	}
	switch {
	case !st.Mode().IsRegular():
		_ = f.Close()
		return nil, fmt.Errorf("%w: it is a %s now", errChanged, st.Mode())
	case !sameAsWalked(st, walked):
		_ = f.Close()
		return nil, fmt.Errorf("%w: another file stands there now", errChanged)
	}
	return f, nil
}

// errChanged marks an entry that is no longer the file the walk saw. It is not a
// fault of the volume: the tree changed under the read, which the archive
// reports by leaving the entry out and counting it, never by failing the copy of
// everything else.
var errChanged = errors.New("the entry changed under the walk")

// archiveEntry writes one walked entry. A directory, a symlink and a hard link
// each become one header; a hard-linked file is stored once and linked after.
// An entry this package cannot restore never enters the stream: it is left out
// and counted, because an artifact that validates and cannot be applied is
// worse than a counted gap.
func (s *Source) archiveEntry(ctx context.Context, tw *tar.Writer, root *os.Root, path string, d fs.DirEntry, seen map[fileKey]string) error {
	// Both paths are absolute and under the same root, so a relative one always
	// exists: the error is impossible by construction.
	rel := filepath.ToSlash(must.Do(filepath.Rel(s.cfg.Root, path)))
	info, err := d.Info()
	if err != nil {
		if s.skip(path, err) {
			return nil
		}
		return fmt.Errorf("%s: %w", path, err)
	}
	mode := info.Mode()
	// A socket has no tar entry type at all — the tar writer refuses to
	// describe one — so it is left out here; every other kind is decided by the
	// one rule both directions share, on the header below.
	if mode&fs.ModeSocket != 0 {
		return s.leaveOut(rel, "the entry is a socket, which has no tar entry type")
	}
	var link string
	if mode&fs.ModeSymlink != 0 {
		link, err = readlink(path)
		if err != nil {
			if s.skip(path, err) {
				return nil
			}
			return fmt.Errorf("%s: %w", path, err)
		}
	}
	// A socket is the only mode tar cannot describe, and it was left out above,
	// so the header always exists.
	hdr := must.Do(tar.FileInfoHeader(info, link))
	hdr.Name = rel
	if mode.IsDir() {
		hdr.Name = rel + "/"
	}
	// The same rule an Apply validates with decides here: a socket, a FIFO, a
	// device node and a link that resolves outside the tree are left out.
	if why := entryRefusal(hdr); why != "" {
		return s.leaveOut(rel, why)
	}
	if !mode.IsRegular() {
		return tw.WriteHeader(hdr)
	}

	if s.testBeforeOpen != nil {
		s.testBeforeOpen(path)
	}
	// The file opens before its header is written — a file that vanished in
	// between is either tolerated or an error, never a dangling entry — and what
	// opens is the very file the walk saw, not whatever stands there now.
	f, err := openCarried(root, rel, info)
	if err != nil {
		switch {
		case s.skip(path, err):
			// The tolerated delete race.
			return nil
		case errors.Is(err, errChanged):
			return s.leaveOut(rel, err.Error())
		default:
			// An unreadable file is a fault of the volume, not a race: it fails
			// the artifact as it always has.
			return fmt.Errorf("%s: %w", path, err)
		}
	}
	defer func() { _ = f.Close() }()
	if hasLinks(info) {
		key := fileKeyOf(info)
		if first, stored := seen[key]; stored {
			// The bytes are already in the stream; this entry links to them.
			hdr.Typeflag = tar.TypeLink
			hdr.Linkname = first
			hdr.Size = 0
			return tw.WriteHeader(hdr)
		}
		seen[key] = rel
	}
	if err := tw.WriteHeader(hdr); err != nil {
		return err
	}
	// The entry's size was fixed by the walk; the copy stops there, so bytes
	// appended to a live file cannot overflow the entry.
	n, err := copyN(ctx, tw, f, hdr.Size)
	if err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	if n < hdr.Size {
		// The file shrank after the walk. The stream must still carry hdr.Size
		// bytes, so the tail is zero-padded; the event is counted beside the
		// artifact and reported in the log, never silent.
		if err := padZeroes(tw, hdr.Size-n); err != nil {
			return err
		}
		s.stats.PaddedFiles++
		s.log.Warn("source.file.padded", slog.Any("event", human.Warning("source.file.padded",
			human.Text("path", path),
			human.Text("reason", fmt.Sprintf("it shrank while it was archived; the entry is zero-padded (%d of %d bytes read)", n, hdr.Size)))))
	}
	return nil
}

// leaveOut keeps one entry out of the stream and counts it.
func (s *Source) leaveOut(rel, why string) error {
	s.stats.SkippedEntries++
	s.log.Warn("source.entry.skipped", slog.Any("event", human.Warning("source.entry.skipped",
		human.Text("path", rel), human.Text("reason", "the archive cannot carry it: "+why))))
	return nil
}

// skip decides whether a per-file failure is the tolerated delete race.
func (s *Source) skip(path string, err error) bool {
	if s.cfg.IgnoreVanished && errors.Is(err, fs.ErrNotExist) && path != s.cfg.Root {
		s.stats.SkippedEntries++
		return true
	}
	return false
}

// fileKey is a file's identity: device and inode numbers together with the
// inode change time. Device and inode alone are not enough on Linux, where
// unlink and re-create hand the freed inode number to a different file; the
// change time is what a reused inode can never match, since the kernel moves it
// on every write, metadata change and rename and no process can set it.
type fileKey struct {
	dev        uint64
	ino        uint64
	changeSec  int64
	changeNsec int64
}

func fileKeyOf(info fs.FileInfo) fileKey {
	if st, ok := info.Sys().(*syscall.Stat_t); ok {
		sec, nsec := changeTime(st)
		return fileKey{dev: uint64(st.Dev), ino: uint64(st.Ino), changeSec: sec, changeNsec: nsec}
	}
	return fileKey{}
}

func hasLinks(info fs.FileInfo) bool {
	if st, ok := info.Sys().(*syscall.Stat_t); ok {
		return st.Nlink > 1
	}
	return false
}

const copyBufSize = 256 << 10

// copyN copies at most n bytes of src into dst. An EOF before n is not an
// error: the caller sees the short count and pads the entry.
func copyN(ctx context.Context, dst io.Writer, src io.Reader, n int64) (int64, error) {
	buf := make([]byte, copyBufSize)
	var total int64
	for total < n {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		chunk := min(int64(len(buf)), n-total)
		r, rerr := src.Read(buf[:chunk])
		if r > 0 {
			if _, werr := dst.Write(buf[:r]); werr != nil {
				return total, werr
			}
			total += int64(r)
		}
		if rerr != nil {
			if rerr == io.EOF {
				return total, nil
			}
			return total, rerr
		}
	}
	return total, nil
}

// padZeroes writes n zero bytes: the tar format pads a short entry to the size
// its header declared.
func padZeroes(dst io.Writer, n int64) error {
	var zeros [32 << 10]byte
	for n > 0 {
		chunk := min(n, int64(len(zeros)))
		if _, err := dst.Write(zeros[:chunk]); err != nil {
			return err
		}
		n -= chunk
	}
	return nil
}
