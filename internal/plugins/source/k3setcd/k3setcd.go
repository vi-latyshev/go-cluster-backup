// Package k3setcd is the source that snapshots a k3s embedded etcd datastore:
// the newest uncompressed etcd snapshot — which already ends with etcd's own
// 32-byte SHA256 of its data — and the server token, as ONE tar stream. It runs
// in a privileged init container and reads only paths the config names.
//
// A snapshot is selected, never guessed: the newest finished regular file in
// the configured directory wins (mtime, then name), hidden files and temporary
// names are ignored, and a compressed snapshot is refused by name because its
// bytes do not carry the trailing checksum this source verifies. The selected
// file is then opened without following a link, checked against the identity
// the directory showed, and streamed to the archive while the SHA256 of its
// data is recomputed and compared with the trailer. A short, changed or corrupt
// snapshot fails the artifact; no bytes of the token ever reach an error.
package k3setcd

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/configuration"
	pluginerrors "github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	pluginio "github.com/vi-latyshev/go-cluster-backup/internal/plugins/io"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source"
)

// The two entries of the artifact: the snapshot bytes exactly as etcd saved
// them (data plus its checksum trailer), and the token.
const (
	snapshotEntry = "snapshot.db"
	tokenEntry    = "token"
)

type Config struct {
	// SnapshotDir is the directory k3s saves its etcd snapshots in. The newest
	// finished snapshot there is the artifact; an older one is never a fallback
	// for a compressed, stale or corrupt newest one.
	SnapshotDir string `json:"snapshot_dir" required:"true" doc:"directory k3s saves its uncompressed etcd snapshots in"`
	// Token is the k3s server token as a mounted secret file. A snapshot alone
	// cannot bootstrap a cluster; the token is restored with it.
	Token configuration.Secret `json:"token" doc:"the k3s server token, as a mounted secret file reference"`
	// MaxAge is the oldest snapshot a run accepts. It is a deployment decision:
	// a stale snapshot still verifies, but it is not the backup anyone wanted.
	MaxAge model.Duration `json:"max_age" required:"true" doc:"maximum age of the newest snapshot a run accepts, e.g. \"26h\""`
}

// Config implements plugin.Validator through Validate.
var _ configuration.Validator = Config{}

func (c Config) Validate() error {
	if c.SnapshotDir == "" {
		return errors.New("snapshot_dir is empty")
	}
	if !filepath.IsAbs(c.SnapshotDir) {
		return fmt.Errorf("snapshot_dir %q must be absolute", c.SnapshotDir)
	}
	if c.Token.File == "" {
		return errors.New("token: file is empty")
	}
	if !filepath.IsAbs(c.Token.File) {
		return fmt.Errorf("token file %q must be absolute", c.Token.File)
	}
	if c.MaxAge.Std() <= 0 {
		return fmt.Errorf("max_age must be positive, got %s", c.MaxAge)
	}
	return nil
}

// The filesystem primitives this package drives. Each is a variable so a test
// can make it fail or hand back another file: no filesystem fails them on
// demand, and a branch no test reaches is a branch nobody has run.
var (
	readDir          = os.ReadDir
	openSnapshotFile = os.OpenFile
	statOpenSnapshot = func(f *os.File) (fs.FileInfo, error) { return f.Stat() }
)

type Source struct {
	cfg Config
}

var _ instances.Checker = (*Source)(nil)

var _ source.Prober = (*Source)(nil)

// Check is the preflight: the snapshot directory holds a fresh uncompressed
// snapshot with a trailer, and the token can be read. It reads the directory
// and the token, never the snapshot's data.
func (s *Source) Check(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	st, err := os.Stat(s.cfg.SnapshotDir)
	if err != nil {
		return pluginerrors.Permanent(fmt.Errorf("snapshot directory %s: %w", s.cfg.SnapshotDir, err))
	}
	if !st.IsDir() {
		return pluginerrors.Permanent(fmt.Errorf("snapshot directory %s is not a directory", s.cfg.SnapshotDir))
	}
	if _, err := s.selectSnapshot(ctx); err != nil {
		return err
	}
	if _, err := os.ReadFile(s.cfg.Token.File); err != nil {
		return pluginerrors.Permanent(fmt.Errorf("token: %w", err))
	}
	return nil
}

// Describe names the snapshot directory as the origin and reports the bytes as
// credential content: the tar carries the cluster token, and a server token
// with the datastore is bootstrap material. A pipeline that does not seal it is
// refused before the first byte; this source publishes no warning exception.
func (s *Source) Describe(ctx context.Context) (source.SourceTraits, error) {
	if err := ctx.Err(); err != nil {
		return source.SourceTraits{}, err
	}
	return source.SourceTraits{
		Ext:         ".tar",
		Origin:      "k3s-etcd://" + s.cfg.SnapshotDir,
		Credentials: true,
	}, nil
}

// candidate is one finished snapshot the directory showed.
type candidate struct {
	path string
	name string
	info fs.FileInfo
}

// ignoredName reports an entry that is not a finished snapshot: a hidden file,
// or the temporary name a writer uses while it produces one.
func ignoredName(name string) bool {
	return strings.HasPrefix(name, ".") ||
		strings.HasSuffix(name, ".part") ||
		strings.HasSuffix(name, ".tmp")
}

// compressedSuffixes name the compressed shapes k3s can save a snapshot in.
var compressedSuffixes = []string{".zip", ".gz", ".tgz", ".bz2", ".xz", ".zst"}

// compressed reports whether name carries a compressed snapshot's suffix.
func compressed(name string) bool {
	lower := strings.ToLower(name)
	for _, suffix := range compressedSuffixes {
		if strings.HasSuffix(lower, suffix) {
			return true
		}
	}
	return false
}

// selectSnapshot picks the newest finished snapshot: mtime decides, a
// lexicographically greater name breaks a tie, so the choice is deterministic.
// A newest compressed, stale or short snapshot fails the artifact instead of
// falling back to an older one — a fresh-looking artifact of yesterday's data
// is the failure this source exists to prevent.
func (s *Source) selectSnapshot(ctx context.Context) (candidate, error) {
	if err := ctx.Err(); err != nil {
		return candidate{}, err
	}
	entries, err := readDir(s.cfg.SnapshotDir)
	if err != nil {
		return candidate{}, pluginerrors.Permanent(fmt.Errorf("snapshot directory %s: %w", s.cfg.SnapshotDir, err))
	}
	var candidates []candidate
	for _, entry := range entries {
		if ignoredName(entry.Name()) {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return candidate{}, pluginerrors.Transient(fmt.Errorf("snapshot %s: %w", filepath.Join(s.cfg.SnapshotDir, entry.Name()), err))
		}
		if !info.Mode().IsRegular() {
			continue
		}
		candidates = append(candidates, candidate{
			path: filepath.Join(s.cfg.SnapshotDir, entry.Name()),
			name: entry.Name(),
			info: info,
		})
	}
	if len(candidates) == 0 {
		return candidate{}, pluginerrors.Permanent(fmt.Errorf(
			"no uncompressed etcd snapshot in %s: check k3s's etcd snapshot schedule and snapshot directory", s.cfg.SnapshotDir))
	}
	sort.Slice(candidates, func(i, j int) bool {
		ti, tj := candidates[i].info.ModTime(), candidates[j].info.ModTime()
		if !ti.Equal(tj) {
			return ti.After(tj)
		}
		return candidates[i].name > candidates[j].name
	})
	newest := candidates[0]
	if compressed(newest.name) {
		return candidate{}, pluginerrors.Permanent(fmt.Errorf(
			"the newest snapshot %s is compressed: etcd's trailing SHA256 is not there in a compressed file; configure k3s to save uncompressed etcd snapshots", newest.path))
	}
	age := time.Since(newest.info.ModTime())
	if age > s.cfg.MaxAge.Std() {
		return candidate{}, pluginerrors.Permanent(fmt.Errorf(
			"the newest snapshot %s is %s old, older than max_age %s: the etcd snapshot schedule is not producing fresh snapshots",
			newest.path, age.Round(time.Second), s.cfg.MaxAge))
	}
	if newest.info.Size() < sha256.Size {
		return candidate{}, pluginerrors.Permanent(fmt.Errorf(
			"the newest snapshot %s is %d bytes, shorter than etcd's trailing %d-byte SHA256", newest.path, newest.info.Size(), sha256.Size))
	}
	return newest, nil
}

// openSnapshot opens the selected file without following a link, refuses a
// special file, and proves the descriptor is the very file the directory
// showed. A file that changed in between is a rotation race, not a corrupt
// backup: the next attempt selects the newest one again.
func openSnapshot(snap candidate) (*os.File, error) {
	f, err := openSnapshotFile(snap.path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, pluginerrors.Transient(fmt.Errorf(
				"the selected snapshot %s vanished before it was opened; another attempt selects the newest one again: %w", snap.path, err))
		}
		return nil, pluginerrors.Permanent(fmt.Errorf("opening the snapshot %s without following a link: %w", snap.path, err))
	}
	st, err := statOpenSnapshot(f)
	if err != nil {
		_ = f.Close()
		return nil, pluginerrors.Transient(fmt.Errorf("reading the opened snapshot %s: %w", snap.path, err))
	}
	if !st.Mode().IsRegular() || !os.SameFile(st, snap.info) ||
		st.Size() != snap.info.Size() || !st.ModTime().Equal(snap.info.ModTime()) {
		_ = f.Close()
		return nil, pluginerrors.Transient(fmt.Errorf(
			"the snapshot %s changed between selection and open: another file stands there now", snap.path))
	}
	return f, nil
}

// Dump writes the snapshot and the token as one tar stream. The snapshot entry
// carries the file exactly as etcd saved it, and its data is hashed as it
// streams; the trailing checksum is compared before the artifact can succeed.
func (s *Source) Dump(ctx context.Context, dst io.Writer) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	snap, err := s.selectSnapshot(ctx)
	if err != nil {
		return err
	}
	f, err := openSnapshot(snap)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	token, err := os.ReadFile(s.cfg.Token.File)
	if err != nil {
		return pluginerrors.Permanent(fmt.Errorf("token: %w", err))
	}
	reader := newChecksumReader(f, snap.info.Size()-sha256.Size)
	packErr := pluginio.WriteTar(ctx, dst, []pluginio.TarEntry{
		{Name: snapshotEntry, Mode: 0o644, Read: reader, Size: snap.info.Size()},
		{Name: tokenEntry, Mode: 0o600, Body: token},
	})
	if packErr != nil {
		switch {
		case errors.Is(packErr, errSnapshotChecksum), errors.Is(packErr, io.ErrUnexpectedEOF):
			return pluginerrors.Transient(packErr)
		default:
			return packErr
		}
	}
	after, err := statOpenSnapshot(f)
	if err != nil {
		return pluginerrors.Transient(fmt.Errorf("the snapshot %s changed while it was read: %w", snap.path, err))
	}
	if !os.SameFile(snap.info, after) ||
		after.Size() != snap.info.Size() || !after.ModTime().Equal(snap.info.ModTime()) {
		return pluginerrors.Transient(fmt.Errorf(
			"the snapshot %s changed while it was read; another attempt selects the newest one again", snap.path))
	}
	return nil
}

// errSnapshotChecksum marks a snapshot whose data does not hash to its own
// trailing checksum. The message never carries bytes of the file.
var errSnapshotChecksum = errors.New("the snapshot's data does not match its trailing SHA256 checksum")

// checksumReader streams a snapshot file: it hashes the data bytes and keeps
// the trailing SHA256, then compares the two before reporting the end of the
// file. A file shorter than its declared length, or one whose trailer does not
// match, fails the read — the archive never records a snapshot nobody verified.
type checksumReader struct {
	src      io.Reader
	dataSize int64
	hash     hash.Hash
	trailer  [sha256.Size]byte
	trailerN int
	read     int64
	verified bool
}

func newChecksumReader(src io.Reader, dataSize int64) *checksumReader {
	return &checksumReader{src: src, dataSize: dataSize, hash: sha256.New()}
}

func (r *checksumReader) Read(p []byte) (int, error) {
	n, err := r.src.Read(p)
	if n > 0 {
		dataPart := 0
		if r.read < r.dataSize {
			dataPart = n
			if remaining := r.dataSize - r.read; int64(dataPart) > remaining {
				dataPart = int(remaining)
			}
			_, _ = r.hash.Write(p[:dataPart])
		}
		if dataPart < n {
			r.trailerN += copy(r.trailer[r.trailerN:], p[dataPart:n])
		}
		r.read += int64(n)
		if r.read == r.dataSize+sha256.Size {
			r.verified = bytes.Equal(r.trailer[:], r.hash.Sum(nil))
			if !r.verified {
				return n, errSnapshotChecksum
			}
		}
	}
	if err != nil && !errors.Is(err, io.EOF) {
		return n, err
	}
	if r.read == r.dataSize+sha256.Size {
		return n, err
	}
	if errors.Is(err, io.EOF) {
		return n, io.ErrUnexpectedEOF
	}
	return n, err
}

// Probe re-reads the artifact without a target: the tar must hold exactly one
// snapshot and the token, and the snapshot's trailing SHA256 is verified again
// from the archive's bytes. The artifact's own data is hashed, so a tampered or
// truncated stream fails here as it failed in the dump.
func (s *Source) Probe(ctx context.Context, src io.Reader) (source.ProbeReport, error) {
	if err := ctx.Err(); err != nil {
		return source.ProbeReport{}, err
	}
	tr := tar.NewReader(src)
	var report source.ProbeReport
	var snapshotSeen, tokenSeen bool
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return source.ProbeReport{}, fmt.Errorf("reading the artifact tar: %w", err)
		}
		report.Entries++
		switch hdr.Name {
		case snapshotEntry:
			if snapshotSeen {
				return source.ProbeReport{}, pluginerrors.Permanent(fmt.Errorf(
					"the artifact holds more than one %s entry", snapshotEntry))
			}
			snapshotSeen = true
			if hdr.Typeflag != tar.TypeReg {
				return source.ProbeReport{}, pluginerrors.Permanent(fmt.Errorf(
					"the artifact's %s is not a regular file", snapshotEntry))
			}
			if hdr.Size < sha256.Size {
				return source.ProbeReport{}, pluginerrors.Permanent(fmt.Errorf(
					"the artifact's %s is %d bytes, shorter than etcd's trailing %d-byte SHA256", snapshotEntry, hdr.Size, sha256.Size))
			}
			n, err := pluginio.CopyContext(ctx, io.Discard, newChecksumReader(tr, hdr.Size-sha256.Size))
			if err != nil {
				if errors.Is(err, errSnapshotChecksum) || errors.Is(err, io.ErrUnexpectedEOF) {
					return source.ProbeReport{}, pluginerrors.Permanent(err)
				}
				return source.ProbeReport{}, err
			}
			report.Bytes += n
		case tokenEntry:
			if tokenSeen {
				return source.ProbeReport{}, pluginerrors.Permanent(fmt.Errorf(
					"the artifact holds more than one %s entry", tokenEntry))
			}
			tokenSeen = true
			n, err := pluginio.CopyContext(ctx, io.Discard, tr)
			if err != nil {
				return source.ProbeReport{}, err
			}
			report.Bytes += n
		default:
			return source.ProbeReport{}, pluginerrors.Permanent(fmt.Errorf(
				"the artifact holds an unexpected entry %q", hdr.Name))
		}
	}
	if !snapshotSeen || !tokenSeen {
		return source.ProbeReport{}, pluginerrors.Permanent(fmt.Errorf(
			"the artifact is missing %s or %s", snapshotEntry, tokenEntry))
	}
	report.Detail = "the snapshot's trailing SHA256 matched its data"
	return report, nil
}
