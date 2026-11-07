// Package local is the storage plugin over a local directory: the exchange
// directory the stage command writes into and the privileged init container
// reads from. One attempt per call, like every storage.
package local

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/io"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage"
)

type Config struct {
	Root     string `json:"root" required:"true" doc:"directory acting as the storage root; every key lives under it"`
	PageSize int    `json:"page_size" doc:"objects per listing page"`
}

type Storage struct {
	root string
	page int
}

// statPath is the stat Put runs on the path it has just renamed into place.
// The call is ordinary, but its failure cannot be provoked from a test without
// substituting it: a real filesystem can still refuse the metadata read (EIO, a
// network mount's ESTALE), so the failure is a classified transient error, and
// the seam is how a test proves that.
var statPath = os.Stat

func (s *Storage) Describe(ctx context.Context) (storage.StorageTraits, error) {
	if err := ctx.Err(); err != nil {
		return storage.StorageTraits{}, err
	}
	return storage.StorageTraits{}, nil
}

func (s *Storage) Put(ctx context.Context, req storage.PutRequest, body io.Reader) (model.ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return model.ObjectInfo{}, err
	}
	path, err := s.path(req.Key)
	if err != nil {
		return model.ObjectInfo{}, pluginerrors.Permanent(err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return model.ObjectInfo{}, pluginerrors.Transient(err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".put-*")
	if err != nil {
		return model.ObjectInfo{}, pluginerrors.Transient(err)
	}
	tmpName := tmp.Name()
	n, err := pluginio.CopyContext(ctx, tmp, body)
	if err == nil {
		if req.Size >= 0 && n != req.Size {
			err = fmt.Errorf("read %d bytes, want %d", n, req.Size)
		}
	}
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		_ = os.Remove(tmpName)
		return model.ObjectInfo{}, pluginerrors.Transient(err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		_ = os.Remove(tmpName)
		return model.ObjectInfo{}, pluginerrors.Transient(err)
	}
	// The bytes landed, but the metadata read after the rename is still an OS
	// call: a filesystem can refuse it (EIO, a network mount's ESTALE), and that
	// is a transient failure of this attempt — never a panic that would take the
	// run down without its retry classification.
	st, err := statPath(path)
	if err != nil {
		return model.ObjectInfo{}, pluginerrors.Transient(err)
	}
	return model.ObjectInfo{Key: req.Key, Size: st.Size(), ModTime: st.ModTime()}, nil
}

func (s *Storage) Stat(ctx context.Context, key string) (model.ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return model.ObjectInfo{}, err
	}
	path, err := s.path(key)
	if err != nil {
		return model.ObjectInfo{}, pluginerrors.Permanent(err)
	}
	st, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return model.ObjectInfo{}, pluginerrors.NotFound(err)
		}
		return model.ObjectInfo{}, pluginerrors.Transient(err)
	}
	return model.ObjectInfo{Key: key, Size: st.Size(), ModTime: st.ModTime()}, nil
}

func (s *Storage) List(ctx context.Context, req storage.ListRequest) (storage.ListPage, error) {
	if err := ctx.Err(); err != nil {
		return storage.ListPage{}, err
	}
	if strings.HasPrefix(req.Prefix, "/") || strings.Contains(req.Prefix, "..") {
		return storage.ListPage{}, pluginerrors.Permanent(fmt.Errorf("prefix %q must be a relative path without \"..\"", req.Prefix))
	}
	var keys []string
	// The walk is rooted at the storage root and yields paths already relative
	// to it, so no path has to be resolved back and no failure is possible
	// between the walk and the key it produces.
	err := fs.WalkDir(os.DirFS(s.root), ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel := filepath.ToSlash(p)
		if strings.HasPrefix(rel, req.Prefix) && rel > req.Token {
			keys = append(keys, rel)
		}
		return nil
	})
	if err != nil {
		if ctx.Err() != nil {
			return storage.ListPage{}, ctx.Err()
		}
		return storage.ListPage{}, pluginerrors.Transient(err)
	}
	sort.Strings(keys)
	res := storage.ListPage{}
	end := min(s.page, len(keys))
	for _, key := range keys[:end] {
		st, err := os.Stat(filepath.Join(s.root, filepath.FromSlash(key)))
		if err != nil {
			continue // vanished between the walk and the stat; not part of this page
		}
		res.Objects = append(res.Objects, model.ObjectInfo{Key: key, Size: st.Size(), ModTime: st.ModTime()})
	}
	if len(keys) > end && end > 0 {
		res.NextToken = keys[end-1]
	}
	return res, nil
}

func (s *Storage) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	path, err := s.path(key)
	if err != nil {
		return pluginerrors.Permanent(err)
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return pluginerrors.Transient(err)
	}
	return nil
}

func (s *Storage) Get(ctx context.Context, key string, dst io.Writer) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	path, err := s.path(key)
	if err != nil {
		return pluginerrors.Permanent(err)
	}
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return pluginerrors.NotFound(err)
		}
		return pluginerrors.Transient(err)
	}
	defer func() { _ = f.Close() }()
	if _, err := pluginio.CopyContext(ctx, dst, f); err != nil {
		return pluginerrors.Transient(err)
	}
	return nil
}

// path resolves a key under the root, refusing anything that would escape it.
func (s *Storage) path(key string) (string, error) {
	if key == "" {
		return "", errors.New("key is empty")
	}
	clean := filepath.Clean(filepath.FromSlash(key))
	if filepath.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("key %q escapes the storage root", key)
	}
	return filepath.Join(s.root, clean), nil
}
