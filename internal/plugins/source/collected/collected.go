// Package collected is the source that ingests one artifact file a vendor
// collector published, for formats whose native tools own consistency: Vault
// Raft, ClickHouse BACKUP, a validated natscli stream directory. The collector
// is responsible for semantic validity; this source is responsible for byte
// identity and freshness, and it proves those from the pair the collector
// publishes: the data file and a digest-only marker beside it, exactly
// <path>.sha256, holding the lowercase hex SHA256 of the data with an optional
// single LF or CRLF.
//
// Both files are opened through the data file's directory with O_NOFOLLOW and
// O_NONBLOCK and must be regular files — a marker that became a link to a
// secret, a FIFO that would hold the run or a device that would stream forever
// is refused, never read. The marker is bounded before it is parsed, its bytes
// never reach an error, and only the three canonical shapes are accepted: a
// GNU sha256sum line, a filename suffix, uppercase hex or a BOM is a refusal,
// not a digest. The data is hashed as it streams and must still be the file the
// open saw; a mismatch is the collector's data-then-marker publication in
// flight, so it is transient and a retry reads the finished pair.
package collected

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
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

const (
	// markerSuffix is appended to the data path: the one marker name, both sides.
	markerSuffix = ".sha256"
	// markerMaxBytes bounds a marker before it is parsed. A digest and one line
	// ending fit many times over; anything larger is not a marker this source
	// published a contract for.
	markerMaxBytes = 4096
)

type Config struct {
	// Path is the exact file the collector published — never a directory scan,
	// so no file the collector did not name can be read as this artifact.
	Path string `json:"path" required:"true" doc:"file the collector published; its checksum marker is exactly <path>.sha256"`
	// Ext is the artifact extension of the collector's bytes, e.g. ".tar".
	Ext string `json:"ext" required:"true" doc:"artifact extension of the collector's bytes, e.g. \".tar\""`
	// MaxAge is how stale the data or its marker may be. Both must be fresh: a
	// collector that stopped writing a new pair is a stopped backup.
	MaxAge model.Duration `json:"max_age" required:"true" doc:"how stale the data or its marker may be before the artifact is refused, e.g. \"26h\""`
}

// Config implements plugin.Validator through Validate.
var _ configuration.Validator = Config{}

func (c Config) Validate() error {
	if c.Path == "" {
		return errors.New("path is empty")
	}
	if !filepath.IsAbs(c.Path) {
		return fmt.Errorf("path %q must be absolute", c.Path)
	}
	if c.Ext == "" {
		return errors.New("ext is empty")
	}
	if !strings.HasPrefix(c.Ext, ".") || strings.ContainsAny(c.Ext, "/\x00") || strings.Contains(c.Ext, "..") {
		return fmt.Errorf("ext %q must be a plain suffix starting with a dot", c.Ext)
	}
	if c.MaxAge.Std() <= 0 {
		return fmt.Errorf("max_age must be positive, got %s", c.MaxAge)
	}
	return nil
}

// The filesystem primitives this package drives. Each is a variable so a test
// can make it fail: no filesystem fails them on demand, and a branch no test
// reaches is a branch nobody has run.
var (
	openRoot      = os.OpenRoot
	openInRoot    = (*os.Root).OpenFile
	statFile      = func(f *os.File) (fs.FileInfo, error) { return f.Stat() }
	readMarker    = io.ReadAll
	copyCollected = pluginio.CopyContext
)

type Source struct {
	cfg Config
}

var _ instances.Checker = (*Source)(nil)

// markerPath is the exact marker file the contract names.
func (s *Source) markerPath() string { return s.cfg.Path + markerSuffix }

// Describe reports the collector's file as the origin and its configured
// extension. It touches no file: the pair is checked at preflight and read at
// the dump. The bytes are the deployment's format: this source claims nothing
// about their content, so it does not call them credentials — a vendor snapshot
// carries whatever sensitivity and sealing the deployment gives it.
func (s *Source) Describe(ctx context.Context) (source.SourceTraits, error) {
	if err := ctx.Err(); err != nil {
		return source.SourceTraits{}, err
	}
	return source.SourceTraits{Ext: s.cfg.Ext, Origin: "file://" + s.cfg.Path}, nil
}

// Check is the preflight: both files are present, confined regular files of
// canonical shape, and both are fresh. It never hashes the data — preflight
// reads no artifact-sized file; the digest is verified where the bytes stream.
func (s *Source) Check(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	pair, err := s.open()
	if err != nil {
		return err
	}
	pair.close()
	return nil
}

// Dump streams the data into dst, hashing it on the way, and succeeds only when
// the streamed digest matches the marker and the file did not change under the
// read. A mismatch is the data-then-marker publication still in flight and is
// transient; the next attempt reads the finished pair.
func (s *Source) Dump(ctx context.Context, dst io.Writer) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	pair, err := s.open()
	if err != nil {
		return err
	}
	defer pair.close()
	h := sha256.New()
	if _, err := copyCollected(ctx, io.MultiWriter(dst, h), pair.data); err != nil {
		return err
	}
	after, err := statFile(pair.data)
	if err != nil {
		return pluginerrors.Transient(fmt.Errorf("reading %s after the copy: %w", s.cfg.Path, err))
	}
	if !sameFile(pair.dataInfo, after) {
		return pluginerrors.Transient(fmt.Errorf(
			"the data %s changed while it was read; the collector publishes data and then its marker, and a retry reads the finished pair", s.cfg.Path))
	}
	if !bytes.Equal(h.Sum(nil), pair.digest[:]) {
		return pluginerrors.Transient(fmt.Errorf(
			"the data %s does not match its marker %s; the collector may still be publishing, and a retry reads the finished pair",
			s.cfg.Path, s.markerPath()))
	}
	return nil
}

// pair is the opened publication: both files, what each was when it was opened,
// and the digest the marker names.
type pair struct {
	data       *os.File
	marker     *os.File
	dataInfo   fs.FileInfo
	markerInfo fs.FileInfo
	digest     [sha256.Size]byte
}

func (p *pair) close() {
	_ = p.marker.Close()
	_ = p.data.Close()
}

// open reads the publication through one confined directory: both files are
// opened without following a link, must be regular, and both must be fresh.
func (s *Source) open() (*pair, error) {
	dir := filepath.Dir(s.cfg.Path)
	root, err := openRoot(dir)
	if err != nil {
		return nil, pluginerrors.Permanent(fmt.Errorf("the data's directory %s: %w", dir, err))
	}
	defer func() { _ = root.Close() }()
	data, dataInfo, err := openConfined(root, filepath.Base(s.cfg.Path), s.cfg.Path)
	if err != nil {
		return nil, err
	}
	p := &pair{data: data, dataInfo: dataInfo}
	marker, markerInfo, err := openConfined(root, filepath.Base(s.cfg.Path)+markerSuffix, s.markerPath())
	if err != nil {
		p.close()
		return nil, err
	}
	p.marker, p.markerInfo = marker, markerInfo
	raw, err := readMarker(io.LimitReader(marker, markerMaxBytes+1))
	if err != nil {
		p.close()
		return nil, pluginerrors.Transient(fmt.Errorf("reading the marker %s: %w", s.markerPath(), err))
	}
	if len(raw) > markerMaxBytes {
		p.close()
		return nil, pluginerrors.Permanent(fmt.Errorf(
			"the marker %s is larger than %d bytes: a marker is only a SHA256 digest", s.markerPath(), markerMaxBytes))
	}
	digest, err := parseDigest(raw)
	if err != nil {
		p.close()
		return nil, pluginerrors.Permanent(fmt.Errorf("the marker %s %w", s.markerPath(), err))
	}
	p.digest = digest
	if err := checkFresh(s.cfg.Path, dataInfo, s.cfg.MaxAge.Std()); err != nil {
		p.close()
		return nil, err
	}
	if err := checkFresh(s.markerPath(), markerInfo, s.cfg.MaxAge.Std()); err != nil {
		p.close()
		return nil, err
	}
	return p, nil
}

// openConfined opens one regular file inside root. A missing file may be the
// publication in flight (the collector renames data first), so it is transient;
// a link, a special file or an unreadable entry is a deployment fault.
func openConfined(root *os.Root, name, path string) (*os.File, fs.FileInfo, error) {
	f, err := openInRoot(root, name, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil, pluginerrors.Transient(fmt.Errorf(
				"%s is not there yet: the collector publishes data and then its marker", path))
		}
		return nil, nil, pluginerrors.Permanent(fmt.Errorf("opening %s without following a link: %w", path, err))
	}
	info, err := statFile(f)
	if err != nil {
		_ = f.Close()
		return nil, nil, pluginerrors.Transient(fmt.Errorf("reading %s: %w", path, err))
	}
	if !info.Mode().IsRegular() {
		_ = f.Close()
		return nil, nil, pluginerrors.Permanent(fmt.Errorf(
			"%s is a %s, not a regular file: this source reads only the files the collector published", path, info.Mode().Type()))
	}
	return f, info, nil
}

// sameFile reports whether after is still the file before described.
func sameFile(before, after fs.FileInfo) bool {
	return os.SameFile(before, after) &&
		before.Size() == after.Size() &&
		before.ModTime().Equal(after.ModTime())
}

// checkFresh refuses a file older than maxAge.
func checkFresh(path string, info fs.FileInfo, maxAge time.Duration) error {
	age := time.Since(info.ModTime())
	if age > maxAge {
		return pluginerrors.Permanent(fmt.Errorf(
			"%s is %s old, older than max_age %s: the collector is not refreshing it", path, age.Round(time.Second), maxAge))
	}
	return nil
}

// errNotCanonicalDigest names the one accepted marker shape and never carries
// the bytes it refused.
var errNotCanonicalDigest = errors.New(
	"is not a canonical sha256 digest: want exactly 64 lowercase hex characters, optionally followed by one LF or CRLF")

// parseDigest accepts exactly the contract's three canonical shapes: 64
// lowercase hex characters, optionally followed by one LF or one CRLF. The
// error names the shape and never carries the bytes it refused.
func parseDigest(raw []byte) ([sha256.Size]byte, error) {
	var want [sha256.Size]byte
	body := raw
	switch {
	case bytes.HasSuffix(body, []byte("\r\n")):
		body = body[:len(body)-2]
	case bytes.HasSuffix(body, []byte("\n")):
		body = body[:len(body)-1]
	}
	if len(body) != sha256.Size*2 {
		return want, errNotCanonicalDigest
	}
	if _, err := hex.Decode(want[:], body); err != nil {
		return want, errNotCanonicalDigest
	}
	// hex.Decode also accepts uppercase; the contract is lowercase only, and
	// comparing against the canonical re-encoding is what enforces it.
	if hex.EncodeToString(want[:]) != string(body) {
		return want, errNotCanonicalDigest
	}
	return want, nil
}
