// Package yandexdisk is the storage plugin over the Yandex Disk REST API.
//
// The plugin is scoped to one folder of the application folder (app:/…), the
// only place an OAuth token with the disk.app_folder scope can reach — so the
// old archives under app:/Backups stay invisible here and are never deleted.
// Each call is ONE attempt, classified like every storage plugin: a 500 on a
// folder creation is a transient error the engine retries the whole Put for.
//
// Two facts of this API shape the code:
//   - an upload href serves exactly one upload: a second PUT to the same URL is
//     accepted with 201 but does not replace the committed object (measured
//     live 2026-10-12), so every Put fetches a fresh href and never reuses one;
//   - a 201/202 upload answer means the server took the bytes, not that the
//     Disk has committed them: only metadata whose size equals the local one
//     proves the object landed. Put polls for that (commit_poll_interval /
//     commit_poll_timeout) and fails when it never becomes visible.
//
// This plugin implements no storage.MultipartUploader: its Put is one
// whole-object request per attempt, so a retry of this storage re-sends the
// object from the start, and the engine's part-by-part path never applies here.
package yandexdisk

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	json "encoding/json/v2"

	backuphuman "github.com/vi-latyshev/go-cluster-backup/internal/human"
	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/must"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/storage"
)

// appPrefix is where an app-folder OAuth token can write; a root outside it is
// a configuration error, and "app:/" itself is refused because it would put
// every other archive — the old app:/Backups among them — inside the root.
const appPrefix = "app:/"

type Config struct {
	APIURL             string         `json:"api_url" doc:"base URL of the Disk REST API, e.g. https://cloud-api.yandex.net/v1/disk"`
	Token              plugin.Secret  `json:"token" doc:"file holding the OAuth token (scope disk.app_folder)"`
	Root               string         `json:"root" required:"true" doc:"folder under app:/ acting as the storage root, e.g. app:/cluster-backup"`
	PermanentDelete    bool           `json:"permanent_delete" required:"true" doc:"true deletes an object instead of moving it to the trash, so space is freed at once"`
	PruneKeepDepth     int            `json:"prune_keep_depth" doc:"leading path segments under the root the emptiness cleanup never removes: 1 keeps the first key segment (the runs/ grouping) unpruned, 0 prunes every folder below the root"`
	RequestTimeout     model.Duration `json:"request_timeout" doc:"timeout of one API request; the bulk upload and download stream under the caller's deadline instead"`
	CommitPollInterval model.Duration `json:"commit_poll_interval" doc:"wait between visibility polls after an accepted upload"`
	CommitPollTimeout  model.Duration `json:"commit_poll_timeout" doc:"total time a Put waits for the uploaded object to become visible"`
}

// Register adds the Yandex Disk storage to a registry.
func Register(r *plugin.Registry) error {
	return plugin.Register(r, storage.Kind, plugin.Spec[Config, storage.Storage]{
		Type:      "yandex_disk",
		Summary:   "the Yandex Disk REST API, scoped to one folder of the application folder",
		Stability: plugin.Stable,
		Example: []byte(`{"api_url": "https://cloud-api.yandex.net/v1/disk",` +
			` "token": {"file": "/run/secrets/yandex/token"}, "root": "app:/cluster-backup",` +
			` "permanent_delete": true, "prune_keep_depth": 1, "request_timeout": "30s",` +
			` "commit_poll_interval": "5s", "commit_poll_timeout": "2m"}`),
		New: func(ctx context.Context, cfg Config, deps plugin.Deps) (storage.Storage, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			return newStorage(cfg, deps.Log)
		},
	})
}

func newStorage(cfg Config, log *slog.Logger) (*Storage, error) {
	u, err := url.Parse(cfg.APIURL)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("api_url %q is not an absolute URL", cfg.APIURL)
	}
	if cfg.Token.File == "" {
		return nil, errors.New("token file is empty")
	}
	root := strings.TrimSuffix(cfg.Root, "/")
	if !strings.HasPrefix(root, appPrefix) {
		return nil, fmt.Errorf("root %q must start with %q: the token is scoped to the application folder", cfg.Root, appPrefix)
	}
	if len(root) <= len(appPrefix) {
		return nil, fmt.Errorf("root %q must be a folder inside the application folder, not the application folder itself", cfg.Root)
	}
	if cfg.PruneKeepDepth < 0 {
		return nil, fmt.Errorf("prune_keep_depth must be zero or positive, got %d", cfg.PruneKeepDepth)
	}
	if cfg.RequestTimeout <= 0 {
		return nil, fmt.Errorf("request_timeout must be positive, got %s", cfg.RequestTimeout)
	}
	if cfg.CommitPollInterval <= 0 {
		return nil, fmt.Errorf("commit_poll_interval must be positive, got %s", cfg.CommitPollInterval)
	}
	if cfg.CommitPollTimeout <= 0 {
		return nil, fmt.Errorf("commit_poll_timeout must be positive, got %s", cfg.CommitPollTimeout)
	}
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Storage{cfg: cfg, root: root, http: &http.Client{}, log: log}, nil
}

type Storage struct {
	cfg  Config
	root string
	http *http.Client
	log  *slog.Logger
}

func (s *Storage) Describe(ctx context.Context) (storage.StorageTraits, error) {
	if err := ctx.Err(); err != nil {
		return storage.StorageTraits{}, err
	}
	// No checksum is claimed: the after-write evidence the production script
	// has proven is the committed size, and the metadata md5 field has not
	// been verified against the live service by this build. Stat still reports
	// the md5 when the service includes one.
	return storage.StorageTraits{}, nil
}

var _ plugin.Checker = (*Storage)(nil)

// Check is the instance's preflight: one read-only metadata request for the
// configured root proves the token is accepted and the root is reachable. An
// absent root is not an error — Put creates it and the folders on the way to a
// key on the first run — and no path outside the root is ever named, so the
// old app:/Backups archives stay untouched. One attempt: the engine retries a
// transient answer under the configured policy.
func (s *Storage) Check(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	resp, err := s.apiCall(ctx, http.MethodGet, "/resources",
		url.Values{"path": {s.root}, "fields": {"type"}})
	if err != nil {
		return err
	}
	switch resp.status {
	case http.StatusOK:
		var r resource
		if err := json.Unmarshal(resp.body, &r); err != nil {
			return plugin.Transient(fmt.Errorf("check root %s: unreadable answer: %w", s.root, err))
		}
		if r.Type == "file" {
			return plugin.Permanent(fmt.Errorf("root %s is a file, not a folder", s.root))
		}
		if r.Type != "dir" {
			return plugin.Transient(fmt.Errorf("check root %s: the answer names type %q", s.root, r.Type))
		}
		return nil
	case http.StatusNotFound:
		// A 404 proves the request was authorised — the API answers 401/403
		// otherwise — and that the root is simply not there yet.
		return nil
	case http.StatusUnauthorized, http.StatusForbidden:
		return plugin.Permanent(fmt.Errorf("check root %s: the token was rejected: HTTP %d: %s",
			s.root, resp.status, apiMessage(resp.body)))
	default:
		return classifyAPI(resp, "check root "+s.root)
	}
}

// Put stores one object: create the folders on the way, fetch a fresh upload
// href, send the bytes to it, then wait until the Disk reports the committed
// size (and md5, when the metadata carries one).
func (s *Storage) Put(ctx context.Context, req storage.PutRequest, body io.Reader) (model.ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return model.ObjectInfo{}, err
	}
	full, err := s.objectPath(req.Key)
	if err != nil {
		return model.ObjectInfo{}, plugin.Permanent(err)
	}
	if err := s.mkdirAll(ctx, path.Dir(full)); err != nil {
		return model.ObjectInfo{}, err
	}
	href, err := s.uploadHref(ctx, full)
	if err != nil {
		return model.ObjectInfo{}, err
	}
	if err := s.send(ctx, href, full, body, req.Size); err != nil {
		return model.ObjectInfo{}, err
	}
	return s.awaitVisible(ctx, req.Key, full, req.Size)
}

// mkdirAll creates every folder on the way to dir, one request per segment,
// each classified like any other call: a 500 on a folder creation must reach
// the engine as a transient error it retries the whole Put for, never as a
// failure that loses the night's archive.
func (s *Storage) mkdirAll(ctx context.Context, dir string) error {
	rest, ok := strings.CutPrefix(dir, appPrefix)
	if !ok {
		return plugin.Permanent(fmt.Errorf("folder %q is not inside %q", dir, appPrefix))
	}
	current := strings.TrimSuffix(appPrefix, "/")
	for _, seg := range strings.Split(rest, "/") {
		if seg == "" {
			continue
		}
		current += "/" + seg
		resp, err := s.apiCall(ctx, http.MethodPut, "/resources", url.Values{"path": {current}})
		if err != nil {
			return err
		}
		switch resp.status {
		case http.StatusCreated, http.StatusConflict: // created, or already there
		default:
			return classifyAPI(resp, "create folder "+current)
		}
	}
	return nil
}

// uploadHref asks for the single-use URL this Put will send its bytes to.
func (s *Storage) uploadHref(ctx context.Context, full string) (string, error) {
	resp, err := s.apiCall(ctx, http.MethodGet, "/resources/upload",
		url.Values{"path": {full}, "overwrite": {"true"}})
	if err != nil {
		return "", err
	}
	if resp.status != http.StatusOK {
		return "", classifyAPI(resp, "upload href for "+full)
	}
	var answer struct {
		Href string `json:"href"`
	}
	if err := json.Unmarshal(resp.body, &answer); err != nil || answer.Href == "" {
		return "", plugin.Transient(fmt.Errorf("upload href for %s: unreadable answer %q", full, resp.body))
	}
	return answer.Href, nil
}

// send streams body to the upload href. The href is pre-authorised and
// single-use, so no OAuth header goes with this request and the URL is not
// retried; the transfer is bounded by the caller's deadline (the storage time
// budget), not by request_timeout, because the archive is gigabytes. A body
// shorter than the declared size fails the request inside net/http, so
// ContentLength is never a claim the send did not honour.
func (s *Storage) send(ctx context.Context, href, full string, body io.Reader, size int64) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, href, body)
	if err != nil {
		return plugin.Permanent(err)
	}
	req.ContentLength = size
	resp, err := s.http.Do(req)
	if err != nil {
		return transportError(ctx, err, "upload data for "+full)
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return transportError(ctx, err, "upload data for "+full)
	}
	switch resp.StatusCode {
	case http.StatusCreated, http.StatusAccepted: // the poll decides whether it committed
		return nil
	default:
		return classifyAPI(apiResponse{status: resp.StatusCode, header: resp.Header, body: respBody},
			"upload data for "+full)
	}
}

// awaitVisible polls the object's metadata until its size matches what was
// sent — the only after-write evidence this build trusts. A 202 answers with
// the bytes accepted but not committed; a metadata outage is not evidence of
// loss either, so any answer short of the committed size keeps polling, and the
// poll timeout decides at the end. A metadata md5 is deliberately not compared:
// its semantics are unverified, and a wrong comparison would fail an upload
// that landed.
func (s *Storage) awaitVisible(ctx context.Context, key, full string, size int64) (model.ObjectInfo, error) {
	deadline := time.Now().Add(s.cfg.CommitPollTimeout.Std())
	for {
		info, err := s.stat(ctx, key, full)
		if err == nil && info.Size == size {
			return info, nil
		}
		if err != nil {
			if class, _ := plugin.ClassOf(err); class == plugin.ClassPermanent {
				return model.ObjectInfo{}, err
			}
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return model.ObjectInfo{}, plugin.Transient(
				fmt.Errorf("upload of %s was accepted but is not visible with %d bytes after %s",
					full, size, s.cfg.CommitPollTimeout))
		}
		select {
		case <-ctx.Done():
			return model.ObjectInfo{}, ctx.Err()
		case <-time.After(min(remaining, s.cfg.CommitPollInterval.Std())):
		}
	}
}

func (s *Storage) Stat(ctx context.Context, key string) (model.ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return model.ObjectInfo{}, err
	}
	full, err := s.objectPath(key)
	if err != nil {
		return model.ObjectInfo{}, plugin.Permanent(err)
	}
	return s.stat(ctx, key, full)
}

// stat reports one resource by its full path. A 404 is "definitely absent"; a
// path occupied by a folder is equally definite — the object is not there.
func (s *Storage) stat(ctx context.Context, key, full string) (model.ObjectInfo, error) {
	resp, err := s.apiCall(ctx, http.MethodGet, "/resources",
		url.Values{"path": {full}, "fields": {"type,size,modified,md5"}})
	if err != nil {
		return model.ObjectInfo{}, err
	}
	switch resp.status {
	case http.StatusOK:
		var r resource
		if err := json.Unmarshal(resp.body, &r); err != nil {
			return model.ObjectInfo{}, plugin.Transient(fmt.Errorf("stat %s: unreadable answer: %w", full, err))
		}
		if r.Type != "file" {
			return model.ObjectInfo{}, plugin.NotFound(
				fmt.Errorf("object %s is definitely absent: the path is a %s", full, orElse(r.Type, "resource")))
		}
		return model.ObjectInfo{Key: key, Size: r.Size, ModTime: parseModified(r.Modified),
			MD5: strings.ToLower(r.MD5)}, nil
	case http.StatusNotFound:
		return model.ObjectInfo{}, plugin.NotFound(
			fmt.Errorf("object %s is definitely absent: %s", full, apiMessage(resp.body)))
	default:
		return model.ObjectInfo{}, classifyAPI(resp, "stat "+full)
	}
}

// List walks the folder tree under the prefix, one API page per contract page:
// the next token carries the folder being paged and the folders still to visit.
//
// Every child path is built from the folder this call asked for plus the item's
// name. The response's own path field is never read: an app-folder request is
// made with app:/ paths, but the service answers with the resource's absolute
// location on the Disk in the disk:/ scheme (Yandex's app-folder documentation:
// "В ответах API пути указываются в схеме disk:/"), so a response path can
// never equal an app:/ folder and matching on it silently returns nothing.
func (s *Storage) List(ctx context.Context, req storage.ListRequest) (storage.ListPage, error) {
	if err := ctx.Err(); err != nil {
		return storage.ListPage{}, err
	}
	prefix, err := s.listPrefix(req.Prefix)
	if err != nil {
		return storage.ListPage{}, plugin.Permanent(err)
	}
	st := listState{Dir: s.baseDir(prefix)}
	if req.Token != "" {
		if err := json.Unmarshal([]byte(req.Token), &st); err != nil {
			return storage.ListPage{}, plugin.Permanent(fmt.Errorf("bad listing token %q: %w", req.Token, err))
		}
	}
	if !s.inRoot(st.Dir) {
		return storage.ListPage{}, plugin.Permanent(fmt.Errorf("listing token points at %q, outside the root %q", st.Dir, s.root))
	}
	page := storage.ListPage{}
	for {
		fp, err := s.listFolder(ctx, st.Dir, st.Offset)
		if err != nil {
			if class, _ := plugin.ClassOf(err); class == plugin.ClassNotFound {
				// The folder does not exist yet: nothing under this prefix.
				zero := 0
				fp = folderListing{itemsPresent: true, total: &zero}
			} else {
				return storage.ListPage{}, err
			}
		}
		items := fp.items
		next := st.Offset + len(items)
		for _, it := range items {
			child := st.Dir + "/" + it.Name
			switch it.Type {
			case "file":
				if strings.HasPrefix(child, prefix) && s.inRoot(child) {
					page.Objects = append(page.Objects, model.ObjectInfo{Key: s.keyOf(child), Size: it.Size,
						ModTime: parseModified(it.Modified), MD5: strings.ToLower(it.MD5)})
				}
			case "dir":
				if descendable(child, prefix) && s.inRoot(child) {
					st.Queue = append(st.Queue, child)
				}
			}
		}
		switch {
		case fp.total != nil && *fp.total > next:
			if len(items) == 0 {
				return storage.ListPage{}, plugin.Transient(
					fmt.Errorf("listing %s made no progress: offset %d of %d", st.Dir, st.Offset, *fp.total))
			}
			st.Offset = next
			page.NextToken = st.token()
			return page, nil
		case fp.total == nil && len(items) > 0:
			// The answer did not state how many children the folder holds: page
			// on until an empty page instead of assuming this was the last one.
			st.Offset = next
			page.NextToken = st.token()
			return page, nil
		}
		if len(st.Queue) == 0 {
			return page, nil
		}
		st.Dir = st.Queue[len(st.Queue)-1]
		st.Queue = st.Queue[:len(st.Queue)-1]
		st.Offset = 0
		if !s.inRoot(st.Dir) {
			return storage.ListPage{}, plugin.Permanent(fmt.Errorf("listing token points at %q, outside the root %q", st.Dir, s.root))
		}
	}
}

// folderListing is one listing answer as the plugin read it. total is nil and
// itemsPresent false when the answer did not carry that field at all — shapes
// the Delete path must treat as "cannot tell", because the service removes a
// non-empty folder together with its whole subtree.
type folderListing struct {
	items        []resource
	itemsPresent bool
	total        *int
}

// listFolder fetches one page of a folder's children. The page size is the
// service's own default limit; the offset is where the previous page ended.
// Only the fields the caller reads are requested: the total for paging, and per
// item the name (the path is built from it), type, size, modified and md5.
func (s *Storage) listFolder(ctx context.Context, dir string, offset int) (folderListing, error) {
	q := url.Values{"path": {dir}, "sort": {"name"}, "fields": {"_embedded.total," +
		"_embedded.items.name,_embedded.items.type,_embedded.items.size,_embedded.items.modified,_embedded.items.md5"}}
	if offset > 0 {
		q.Set("offset", strconv.Itoa(offset))
	}
	resp, err := s.apiCall(ctx, http.MethodGet, "/resources", q)
	if err != nil {
		return folderListing{}, err
	}
	switch resp.status {
	case http.StatusOK:
		var l listing
		if err := json.Unmarshal(resp.body, &l); err != nil {
			return folderListing{}, plugin.Transient(fmt.Errorf("list %s: unreadable answer: %w", dir, err))
		}
		out := folderListing{total: l.Embedded.Total, itemsPresent: l.Embedded.Items != nil}
		if l.Embedded.Items != nil {
			out.items = *l.Embedded.Items
		}
		return out, nil
	case http.StatusNotFound:
		return folderListing{}, plugin.NotFound(fmt.Errorf("folder %s is definitely absent: %s", dir, apiMessage(resp.body)))
	default:
		return folderListing{}, classifyAPI(resp, "list "+dir)
	}
}

func (s *Storage) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	full, err := s.objectPath(key)
	if err != nil {
		return plugin.Permanent(err)
	}
	q := url.Values{"path": {full}}
	if s.cfg.PermanentDelete {
		q.Set("permanently", "true")
	}
	resp, err := s.apiCall(ctx, http.MethodDelete, "/resources", q)
	if err != nil {
		return err
	}
	switch resp.status {
	case http.StatusOK, http.StatusAccepted, http.StatusNoContent:
		// The delete already succeeded; cleaning up the folders it emptied can
		// no longer fail it, only leave folders behind (logged below).
		s.pruneEmptyFolders(ctx, path.Dir(full))
		return nil
	case http.StatusNotFound:
		return nil // already gone: deleting an absent object is success
	default:
		return classifyAPI(resp, "delete "+full)
	}
}

// pruneEmptyFolders removes the folders a delete has emptied, walking up from
// dir. Without it every nightly retention sweep leaves its year/month folders
// behind and every later listing walks them forever.
//
// Two independent guards keep the walk away from anything that holds data: the
// emptiness proof (folderIsEmpty — only an answer that states a zero total and
// carries a present, empty item list authorises the delete) and the depth bound
// (prune_keep_depth — folders at or above that depth under the root are never
// removed, so a deployment's grouping folder survives even a wrong emptiness
// verdict). The walk stops at the configured root, which is never removed, and
// at the first folder that still holds anything. Failures are logged, never
// returned: the delete that triggered the walk has already succeeded.
func (s *Storage) pruneEmptyFolders(ctx context.Context, dir string) {
	for dir != s.root && s.inRoot(dir) {
		if s.depthBelowRoot(dir) <= s.cfg.PruneKeepDepth {
			return // the depth bound: this folder and every folder above it stay
		}
		empty, err := s.folderIsEmpty(ctx, dir)
		if err != nil {
			s.log.Warn("storage.folder.empty", slog.Any("event", backuphuman.Warning("storage.folder.empty",
				backuphuman.Text("folder", dir), backuphuman.Text("reason", err.Error()))))
			return
		}
		if !empty {
			return // a folder holding anything keeps every folder above it
		}
		if err := s.deleteFolder(ctx, dir); err != nil {
			s.log.Warn("storage.folder.empty", slog.Any("event", backuphuman.Warning("storage.folder.empty",
				backuphuman.Text("folder", dir), backuphuman.Text("reason", err.Error()))))
			return
		}
		dir = path.Dir(dir)
	}
}

// depthBelowRoot counts the path segments between dir and the configured root:
// the root itself is 0, root/runs is 1.
func (s *Storage) depthBelowRoot(dir string) int {
	return strings.Count(strings.TrimPrefix(dir, s.root), "/")
}

// folderIsEmpty asks the one question pruning needs, and answers it only from a
// complete proof: the answer must carry the total explicitly, the total must be
// zero, and the item list must be present and empty. A missing field, a
// non-zero total, a non-empty item list or a failed call is "cannot tell" — and
// the service removes a non-empty folder together with its whole subtree, so
// "cannot tell" must mean "leave it".
func (s *Storage) folderIsEmpty(ctx context.Context, dir string) (bool, error) {
	l, err := s.listFolder(ctx, dir, 0)
	if err != nil {
		if class, _ := plugin.ClassOf(err); class == plugin.ClassNotFound {
			return true, nil // already gone: nothing left to remove
		}
		return false, err
	}
	if l.total == nil {
		return false, fmt.Errorf("listing %s does not state the total", dir)
	}
	if !l.itemsPresent {
		return false, fmt.Errorf("listing %s does not state the items", dir)
	}
	switch {
	case *l.total == 0 && len(l.items) == 0:
		return true, nil
	case *l.total > 0 && len(l.items) > 0:
		return false, nil // the folder holds children: a definite answer
	default:
		return false, fmt.Errorf("listing %s contradicts itself: total %d over %d items", dir, *l.total, len(l.items))
	}
}

// deleteFolder removes one folder the caller has listed as empty.
func (s *Storage) deleteFolder(ctx context.Context, dir string) error {
	q := url.Values{"path": {dir}}
	if s.cfg.PermanentDelete {
		q.Set("permanently", "true")
	}
	resp, err := s.apiCall(ctx, http.MethodDelete, "/resources", q)
	if err != nil {
		return err
	}
	switch resp.status {
	case http.StatusOK, http.StatusAccepted, http.StatusNoContent, http.StatusNotFound:
		return nil
	default:
		return classifyAPI(resp, "delete folder "+dir)
	}
}

// Get reads one object. Absence is decided by the metadata call, and a download
// href is fetched from it — the href is pre-authorised, so no OAuth header goes
// to the storage host it points at.
func (s *Storage) Get(ctx context.Context, key string, dst io.Writer) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	full, err := s.objectPath(key)
	if err != nil {
		return plugin.Permanent(err)
	}
	if _, err := s.stat(ctx, key, full); err != nil {
		return err
	}
	href, err := s.downloadHref(ctx, full)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, href, nil)
	if err != nil {
		return plugin.Permanent(err)
	}
	resp, err := s.http.Do(req)
	if err != nil {
		return transportError(ctx, err, "download data for "+full)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == http.StatusNotFound {
		return plugin.NotFound(fmt.Errorf("object %s is definitely absent", full))
	}
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return classifyAPI(apiResponse{status: resp.StatusCode, header: resp.Header, body: respBody},
			"download data for "+full)
	}
	n, err := io.Copy(dst, resp.Body)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return plugin.Transient(fmt.Errorf("read %s: %w", full, err))
	}
	if resp.ContentLength >= 0 && n != resp.ContentLength {
		return plugin.Transient(fmt.Errorf("download of %s stopped at %d of %d bytes", full, n, resp.ContentLength))
	}
	return nil
}

func (s *Storage) downloadHref(ctx context.Context, full string) (string, error) {
	resp, err := s.apiCall(ctx, http.MethodGet, "/resources/download", url.Values{"path": {full}})
	if err != nil {
		return "", err
	}
	switch resp.status {
	case http.StatusOK:
		var answer struct {
			Href string `json:"href"`
		}
		if err := json.Unmarshal(resp.body, &answer); err != nil || answer.Href == "" {
			return "", plugin.Transient(fmt.Errorf("download href for %s: unreadable answer %q", full, resp.body))
		}
		return answer.Href, nil
	case http.StatusNotFound:
		return "", plugin.NotFound(fmt.Errorf("object %s is definitely absent: %s", full, apiMessage(resp.body)))
	default:
		return "", classifyAPI(resp, "download href for "+full)
	}
}

// apiCall runs one control-plane request under request_timeout and returns the
// status, headers and body. The token file is read per call: a rotated token is
// picked up without a restart, and a missing file is a classified error of this
// call rather than a construction failure of the whole run.
func (s *Storage) apiCall(ctx context.Context, method, endpoint string, q url.Values) (apiResponse, error) {
	token, err := s.token()
	if err != nil {
		return apiResponse{}, err
	}
	rctx, cancel := context.WithTimeout(ctx, s.cfg.RequestTimeout.Std())
	defer cancel()
	u := s.cfg.APIURL + endpoint
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	// The base URL was parsed and required to be absolute at construction, the
	// endpoint is a constant and the query is url-encoded: this request cannot
	// fail to build.
	req := must.Do(http.NewRequestWithContext(rctx, method, u, nil))
	req.Header.Set("Authorization", "OAuth "+token)
	resp, err := s.http.Do(req)
	if err != nil {
		return apiResponse{}, transportError(ctx, err, method+" "+endpoint)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return apiResponse{}, transportError(ctx, err, method+" "+endpoint)
	}
	return apiResponse{status: resp.StatusCode, header: resp.Header, body: body}, nil
}

func (s *Storage) token() (string, error) {
	b, err := os.ReadFile(s.cfg.Token.File)
	if err != nil {
		return "", plugin.Permanent(fmt.Errorf("read token file: %w", err))
	}
	token := strings.TrimSpace(string(b))
	if token == "" {
		return "", plugin.Permanent(fmt.Errorf("token file %s is empty", s.cfg.Token.File))
	}
	return token, nil
}

// objectPath resolves a key under the root, refusing anything that would escape
// it — the plugin must never touch a path it was not given.
func (s *Storage) objectPath(key string) (string, error) {
	if key == "" {
		return "", errors.New("key is empty")
	}
	if strings.HasPrefix(key, "/") {
		return "", fmt.Errorf("key %q must be relative to the storage root", key)
	}
	for _, seg := range strings.Split(key, "/") {
		if seg == "" || seg == "." || seg == ".." {
			return "", fmt.Errorf("key %q has an empty, %q or %q segment", key, ".", "..")
		}
	}
	return s.root + "/" + key, nil
}

func (s *Storage) listPrefix(prefix string) (string, error) {
	if prefix == "" {
		return s.root, nil
	}
	if strings.HasPrefix(prefix, "/") {
		return "", fmt.Errorf("prefix %q must be relative to the storage root", prefix)
	}
	for _, seg := range strings.Split(prefix, "/") {
		if seg == ".." {
			return "", fmt.Errorf("prefix %q must not contain %q", prefix, "..")
		}
	}
	return s.root + "/" + prefix, nil
}

// baseDir is the folder one listing page starts from: the deepest folder on the
// prefix's own path, never above the root.
func (s *Storage) baseDir(prefix string) string {
	if i := strings.LastIndex(prefix, "/"); i > len(s.root) {
		return prefix[:i]
	}
	return s.root
}

// descendable reports whether a folder may hold objects matching prefix: the
// prefix lies on the folder's own path, or the folder's path lies on the
// prefix's.
func descendable(dirPath, prefix string) bool {
	return strings.HasPrefix(dirPath, prefix) || strings.HasPrefix(prefix, dirPath+"/")
}

func (s *Storage) inRoot(p string) bool {
	return p == s.root || strings.HasPrefix(p, s.root+"/")
}

func (s *Storage) keyOf(full string) string {
	return strings.TrimPrefix(full, s.root+"/")
}

// apiResponse is one control-plane answer, read into memory: these bodies are
// small JSON documents, unlike the bulk transfers.
type apiResponse struct {
	status int
	header http.Header
	body   []byte
}

// classifyAPI maps an unexpected API status to a plugin error class. Absence
// (404) is left to the caller, which alone knows whether it means "definitely
// absent" for its operation; 429 and 5xx are transient with the service's
// retry-after hint passed through; everything else reaches here as a client
// problem and stays permanent, so a wrong class never burns retries silently.
func classifyAPI(resp apiResponse, what string) error {
	err := fmt.Errorf("%s: HTTP %d: %s", what, resp.status, apiMessage(resp.body))
	switch {
	case resp.status == http.StatusUnauthorized || resp.status == http.StatusForbidden:
		return plugin.Permanent(err)
	case resp.status == http.StatusInsufficientStorage || resp.status == http.StatusLocked:
		// 507 is a full Disk and 423 the monthly upload-traffic limit: quota
		// answers a retry of this run cannot change.
		return plugin.Permanent(err)
	case resp.status == http.StatusTooManyRequests || resp.status >= 500:
		if d := retryAfterHint(resp.header); d > 0 {
			return plugin.RetryAfter(err, d)
		}
		return plugin.Transient(err)
	default:
		return plugin.Permanent(err)
	}
}

// transportError classifies a request that got no HTTP answer at all: caller
// cancellation stays unclassified (the engine treats it as terminal), anything
// else — a timeout, a reset connection, DNS — is transient. Only the wrapped
// cause is reported: a *url.Error spells out the request URL, and for the bulk
// transfers that URL is a pre-authorised href whose signature must never reach
// a log or the stored run report.
func transportError(ctx context.Context, err error, what string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	var uerr *url.Error
	if errors.As(err, &uerr) && uerr.Err != nil {
		err = uerr.Err
	}
	return plugin.Transient(fmt.Errorf("%s: %w", what, err))
}

func retryAfterHint(h http.Header) time.Duration {
	v := h.Get("Retry-After")
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil && secs >= 0 {
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := time.Until(t); d > 0 {
			return d
		}
	}
	return 0
}

// apiMessage pulls the service's own words out of an error body.
func apiMessage(body []byte) string {
	var e struct {
		Message     string `json:"message"`
		Description string `json:"description"`
	}
	if err := json.Unmarshal(body, &e); err == nil {
		switch {
		case e.Message != "" && e.Description != "":
			return e.Message + ": " + e.Description
		case e.Message != "":
			return e.Message
		case e.Description != "":
			return e.Description
		}
	}
	return strings.TrimSpace(string(body))
}

func orElse(s, fallback string) string {
	if s == "" {
		return fallback
	}
	return s
}

// parseModified reads the Disk API's timestamp, which appears both as RFC 3339
// and without a zone depending on the endpoint.
func parseModified(s string) time.Time {
	for _, layout := range []string{time.RFC3339, "2006-01-02T15:04:05", "2006-01-02 15:04:05"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t
		}
	}
	return time.Time{}
}

// resource is the read slice of the service's Resource object. Its own path
// field is deliberately absent: responses name it in the disk:/ scheme, which
// is never the app:/ spelling this plugin works in, and every path it needs is
// built from the folder it asked for plus the name.
type resource struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Size     int64  `json:"size"`
	Modified string `json:"modified"`
	MD5      string `json:"md5"`
}

// listing is the raw answer. Total and Items are pointers so that a field the
// service did not send stays distinguishable from a zero value — the emptiness
// proof must never accept a missing total as "zero children".
type listing struct {
	Embedded struct {
		Total *int        `json:"total"`
		Items *[]resource `json:"items"`
	} `json:"_embedded"`
}

// listState is the opaque page token: the folder being paged with its offset,
// and the folders still to walk.
type listState struct {
	Dir    string   `json:"dir"`
	Offset int      `json:"offset"`
	Queue  []string `json:"queue"`
}

// token renders the paging state as the opaque token a caller hands back. Every
// field is a JSON scalar or a list of them, so the marshal has no failure left
// to report.
func (st listState) token() string {
	return string(must.Do(json.Marshal(st)))
}
