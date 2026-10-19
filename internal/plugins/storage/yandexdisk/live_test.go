package yandexdisk

// Live verification of the Yandex Disk plugin against the real service, gated
// on YDISK_LIVE_TOKEN_FILE (a mode-600 file holding an OAuth token with the
// disk.app_folder scope). Everything it writes lives under one fresh folder
// app:/cluster-backup-conformance-<random>; the top-level app:/ listing is
// compared before and after and must show the same entries apart from that
// folder. The token value is never printed, logged or embedded in an error;
// only status codes, hosts, our own conformance paths and Yandex's own
// message text are reported.
//
// This file is a live harness, not part of the offline suite: without the
// environment variable every test here skips.

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	json "encoding/json/v2"

	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/storage"
	"github.com/vi-latyshev/go-cluster-backup/internal/storage/storagetest"
)

const defaultLiveAPI = "https://cloud-api.yandex.net/v1/disk"

// yandexConformance is the only folder shape a live harness may write to or
// delete: a fresh folder under the conformance prefix. The owner's old
// archives under app:/Backups and the production root app:/cluster-backup are
// outside it by construction; storagetest.ConformanceLocation holds the one
// definition of the shape, shared with the S3 plugin's harness.
var yandexConformance = storagetest.NewConformanceLocation("app:/cluster-backup-conformance-", 12)

// liveRecorder is a recording RoundTripper wrapped around the plugin's own
// client: every request the plugin makes is captured, so the test can audit
// where the plugin went and what the service answered. Only the host of a
// pre-authorised href is ever printed — its query string is an access grant.
type liveRecorder struct {
	mu   sync.Mutex
	reqs []recorded
}

type recorded struct {
	method  string
	host    string
	path    string // URL path only
	apiPath string // the `path` query parameter of a control-plane request
	status  int
}

func (r *liveRecorder) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := http.DefaultTransport.RoundTrip(req)
	rec := recorded{method: req.Method, host: req.URL.Host, path: req.URL.Path,
		apiPath: req.URL.Query().Get("path")}
	if resp != nil {
		rec.status = resp.StatusCode
	}
	r.mu.Lock()
	r.reqs = append(r.reqs, rec)
	r.mu.Unlock()
	return resp, err
}

func (r *liveRecorder) snapshot() []recorded {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]recorded(nil), r.reqs...)
}

// liveClient is a plain client for the inspection calls the plugin does not
// offer: top-level listings, the probe experiment, and the cleanup proof.
type liveClient struct {
	t     *testing.T
	base  string
	token string
	http  *http.Client
}

// do issues one raw API call and retries a transport failure a few times, the
// way the engine would; callers pass no body, so a retry re-issues the same
// request unchanged.
func (c *liveClient) do(method, endpoint string, q url.Values, body io.Reader) (int, []byte, http.Header) {
	c.t.Helper()
	u := c.base + endpoint
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	req, err := http.NewRequest(method, u, body)
	if err != nil {
		c.t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Authorization", "OAuth "+c.token)
	var resp *http.Response
	for attempt := 1; attempt <= 3; attempt++ {
		resp, err = c.http.Do(req)
		if err == nil {
			break
		}
		c.t.Logf("%s %s: attempt %d: %v", method, endpoint, attempt, err)
		time.Sleep(5 * time.Second)
	}
	if err != nil {
		c.t.Fatalf("%s %s: %v", method, endpoint, err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		c.t.Fatalf("%s %s: read body: %v", method, endpoint, err)
	}
	return resp.StatusCode, b, resp.Header
}

// rawDo performs a request against an arbitrary URL with no Authorization
// header — the probe for whether a pre-authorised href is enough on its own.
func (c *liveClient) rawDo(method, href string, body io.Reader) (int, []byte) {
	c.t.Helper()
	req, err := http.NewRequest(method, href, body)
	if err != nil {
		c.t.Fatalf("build raw request: %v", err)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		c.t.Fatalf("raw %s %s: %v", method, hostOf(href), err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

func hostOf(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "?"
	}
	return u.Host
}

type dirEntry struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// listing reads a folder, paging with offset until every entry is seen.
func (c *liveClient) listing(folder string) []dirEntry {
	c.t.Helper()
	var out []dirEntry
	offset := 0
	for {
		q := url.Values{
			"path":   {folder},
			"limit":  {"200"},
			"fields": {"_embedded.total,_embedded.items.name,_embedded.items.type"},
		}
		if offset > 0 {
			q.Set("offset", strconv.Itoa(offset))
		}
		code, body, _ := c.do(http.MethodGet, "/resources", q, nil)
		if code != http.StatusOK {
			c.t.Fatalf("listing %s: HTTP %d: %s", folder, code, string(body))
		}
		var ans struct {
			Embedded struct {
				Total int        `json:"total"`
				Items []dirEntry `json:"items"`
			} `json:"_embedded"`
		}
		if err := json.Unmarshal(body, &ans); err != nil {
			c.t.Fatalf("listing %s: unreadable answer: %v", folder, err)
		}
		out = append(out, ans.Embedded.Items...)
		offset += len(ans.Embedded.Items)
		if offset >= ans.Embedded.Total || len(ans.Embedded.Items) == 0 {
			return out
		}
	}
}

// digestOf folds a set of entries into one short value so the proof can be
// stated without writing the owner's folder names into a log.
func digestOf(entries []dirEntry) (string, int) {
	lines := make([]string, 0, len(entries))
	for _, e := range entries {
		lines = append(lines, e.Name+"\t"+e.Type)
	}
	sort.Strings(lines)
	sum := sha256.Sum256([]byte(strings.Join(lines, "\n")))
	return hex.EncodeToString(sum[:8]), len(lines)
}

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func envOrLive(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

// liveBuilder constructs the live storage every gated test here shares: the
// token comes from a mode-600 file named by the environment, the root is a
// fresh conformance folder, and every request goes through rec so a test can
// audit where the plugin went.
func liveBuilder(t *testing.T, tokenFile, base, root string, rec *liveRecorder) func() *Storage {
	t.Helper()
	log := testLogger()
	return func() *Storage {
		s, err := newStorage(Config{
			APIURL:             base,
			Token:              plugin.Secret{File: tokenFile},
			Root:               root,
			PermanentDelete:    true,
			PruneKeepDepth:     1,
			RequestTimeout:     model.Duration(30 * time.Second),
			CommitPollInterval: model.Duration(2 * time.Second),
			CommitPollTimeout:  model.Duration(5 * time.Minute),
		}, log)
		if err != nil {
			t.Fatalf("constructing the live storage: %v", err)
		}
		s.http = &http.Client{Transport: rec}
		return s
	}
}

func TestLiveYandex(t *testing.T) {
	tokenFile := os.Getenv("YDISK_LIVE_TOKEN_FILE")
	if tokenFile == "" {
		t.Skip("YDISK_LIVE_TOKEN_FILE names a mode-600 file holding an OAuth token; skipping")
	}
	raw, err := os.ReadFile(tokenFile)
	if err != nil {
		t.Fatalf("reading the token file: %v", err)
	}
	token := strings.TrimSpace(string(raw))
	if token == "" {
		t.Fatal("the token file is empty")
	}
	base := envOrLive("YDISK_LIVE_API", defaultLiveAPI)
	root := yandexConformance.Generate(t)

	rec := &liveRecorder{}
	build := liveBuilder(t, tokenFile, base, root, rec)
	client := &liveClient{t: t, base: base, token: token, http: &http.Client{}}

	// First and last live action: the top-level app:/ listing, names and types
	// only, never descending into any other folder.
	before := client.listing("app:/")
	beforeDigest, beforeCount := digestOf(otherThan(before, baseName(root)))

	t.Cleanup(func() {
		after := client.listing("app:/")
		afterDigest, afterCount := digestOf(otherThan(after, baseName(root)))
		backups := false
		for _, e := range after {
			if e.Name == "Backups" && e.Type == "dir" {
				backups = true
			}
		}
		if beforeDigest != afterDigest || beforeCount != afterCount {
			t.Errorf("top-level app:/ changed: before %s/%d, after %s/%d", beforeDigest, beforeCount, afterDigest, afterCount)
		}
		if !backups {
			t.Error("Backups is not a folder after the run")
		}
		t.Logf("top-level app:/ other entries: count=%d digest=%s (before and after identical); Backups is a dir: %v",
			afterCount, afterDigest, backups)
	})
	t.Cleanup(func() {
		cleanupLive(t, build(), client, root)
		auditPaths(t, rec.snapshot(), root, base)
	})

	t.Logf("conformance root: %s", root)

	t.Run("check", func(t *testing.T) {
		if err := build().Check(context.Background()); err != nil {
			t.Fatalf("Check on an absent root: %v", err)
		}
	})

	t.Run("conformance", func(t *testing.T) {
		storagetest.Run(t, func(t *testing.T) storage.Storage { return build() })
	})

	t.Run("paging", func(t *testing.T) {
		ctx := context.Background()
		s := build()
		const n = 25
		want := map[string]string{}
		for i := range n {
			key := fmt.Sprintf("paging/file-%02d.bin", i)
			body := []byte(fmt.Sprintf("paging payload %d", i))
			if err := retryLive(t, "Put "+key, func() error {
				_, err := s.Put(ctx, storage.PutRequest{Key: key, Size: int64(len(body))}, bytes.NewReader(body))
				return err
			}); err != nil {
				t.Fatalf("Put %s: %v", key, err)
			}
			want[key] = fmt.Sprintf("%x", sha256.Sum256(body))
		}
		beforePaging := countListRequests(rec.snapshot(), root+"/paging")
		seen := map[string]int{}
		token := ""
		pages := 0
		for {
			page, err := s.List(ctx, storage.ListRequest{Prefix: "paging/", Token: token})
			if err != nil {
				t.Fatalf("List page %d: %v", pages, err)
			}
			pages++
			for _, o := range page.Objects {
				seen[o.Key]++
			}
			if page.NextToken == "" {
				break
			}
			if pages > 20 {
				t.Fatal("listing did not terminate")
			}
			token = page.NextToken
		}
		listed := countListRequests(rec.snapshot(), root+"/paging") - beforePaging
		for key := range want {
			if seen[key] != 1 {
				t.Errorf("key %s returned %d times", key, seen[key])
			}
		}
		t.Logf("paging: %d objects, %d contract pages, %d service listing requests", n, pages, listed)
	})

	t.Run("big-upload-and-get", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
		defer cancel()
		s := build()
		const size = 64 << 20
		body := make([]byte, size)
		if _, err := rand.Read(body); err != nil {
			t.Fatal(err)
		}
		want := sha256.Sum256(body)
		// A name from the production class: the measurement below shows the service
		// slows one name ending by an order of magnitude, and this check is about
		// the plugin's commit path, not about that throttle.
		key := "big/object.dump.zst"

		beforeReqs := len(rec.snapshot())
		var lastErr error
		for attempt := 1; attempt <= 2; attempt++ {
			start := time.Now()
			info, err := s.Put(ctx, storage.PutRequest{Key: key, Size: int64(size), SHA256: hex.EncodeToString(want[:])}, bytes.NewReader(body))
			elapsed := time.Since(start)
			uploadStatus, polls := describeUpload(rec.snapshot()[beforeReqs:], root+"/"+key)
			if err != nil {
				// A failed attempt's residue: did the bytes land anyway?
				var state string
				if st, serr := s.Stat(ctx, key); serr == nil {
					state = fmt.Sprintf("visible with %d bytes", st.Size)
				} else if class, _ := plugin.ClassOf(serr); class == plugin.ClassNotFound {
					state = "not_found (definitely absent)"
				} else {
					state = fmt.Sprintf("stat failed: %v", serr)
				}
				t.Logf("64 MiB attempt %d: %s in %s (%.1f MiB/s), upload href answered %s, commit polls %d, error: %v; object after the attempt: %s",
					attempt, human(size), elapsed.Round(time.Millisecond), float64(size)/elapsed.Seconds()/(1<<20), uploadStatus, polls, err, state)
				lastErr = err
				continue
			}
			if info.Size != int64(size) {
				t.Errorf("Put reported %+v", info)
			}
			t.Logf("64 MiB upload: %s in %s (%.1f MiB/s), upload href answered %s, commit polls on metadata: %d",
				human(size), elapsed.Round(time.Millisecond), float64(size)/elapsed.Seconds()/(1<<20), uploadStatus, polls)
			lastErr = nil
			break
		}
		if lastErr != nil {
			t.Fatalf("64 MiB Put failed on every attempt: %v", lastErr)
		}

		var got bytes.Buffer
		start := time.Now()
		if err := s.Get(ctx, key, &got); err != nil {
			t.Fatalf("Get of the big object: %v", err)
		}
		downloadElapsed := time.Since(start)
		sum := sha256.Sum256(got.Bytes())
		if sum != want || got.Len() != size {
			t.Errorf("downloaded %d bytes, sha256 equal: %v", got.Len(), sum == want)
		}
		t.Logf("64 MiB download: %s in %s (%.1f MiB/s), bytes equal: true",
			human(int64(got.Len())), downloadElapsed.Round(time.Millisecond), float64(got.Len())/downloadElapsed.Seconds()/(1<<20))
	})

	// A sized upload whose status and commit poll are reported: the service
	// answers 201 (committed) or 202 (accepted, poll needed) on the upload
	// href, and Put's own metadata poll follows an accepted upload.
	t.Run("upload-status", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
		defer cancel()
		s := build()
		const size = 8 << 20
		body := make([]byte, size)
		if _, err := rand.Read(body); err != nil {
			t.Fatal(err)
		}
		before := len(rec.snapshot())
		var elapsed time.Duration
		if err := retryLive(t, "8 MiB Put", func() error {
			start := time.Now()
			_, err := s.Put(ctx, storage.PutRequest{Key: "status/object.dump.zst", Size: int64(size)}, bytes.NewReader(body))
			if err == nil {
				elapsed = time.Since(start)
			}
			return err
		}); err != nil {
			t.Fatalf("8 MiB Put: %v", err)
		}
		status, polls := describeUpload(rec.snapshot()[before:], root+"/status/object.dump.zst")
		t.Logf("8 MiB upload: %s in %s, upload href answered %s, metadata polls after it: %d",
			human(size), elapsed.Round(time.Millisecond), status, polls)
	})

	t.Run("delete-and-prune", func(t *testing.T) {
		ctx := context.Background()
		s := build()
		key := "prune/a/b/c.bin"
		body := []byte("prune me")
		if err := retryLive(t, "Put "+key, func() error {
			_, err := s.Put(ctx, storage.PutRequest{Key: key, Size: int64(len(body))}, bytes.NewReader(body))
			return err
		}); err != nil {
			t.Fatal(err)
		}
		if err := s.Delete(ctx, key); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		for _, dir := range []string{root + "/prune/a/b", root + "/prune/a"} {
			code, _, _ := client.do(http.MethodGet, "/resources", url.Values{"path": {dir}, "fields": {"type"}}, nil)
			if code != http.StatusNotFound {
				t.Errorf("folder %s: HTTP %d, want 404 (the emptied folders below the keep depth must be gone)", dir, code)
			}
		}
		code, body2, _ := client.do(http.MethodGet, "/resources", url.Values{"path": {root + "/prune"}, "fields": {"type"}}, nil)
		if code != http.StatusOK || !strings.Contains(string(body2), `"dir"`) {
			t.Errorf("the grouping folder at the keep depth: HTTP %d body %s, want 200 dir", code, body2)
		}
		code, body3, _ := client.do(http.MethodGet, "/resources", url.Values{"path": {root}, "fields": {"type"}}, nil)
		if code != http.StatusOK || !strings.Contains(string(body3), `"dir"`) {
			t.Errorf("the configured root: HTTP %d body %s, want 200 dir", code, body3)
		}
	})

	// The production artifact keys sit five and six segments below the root
	// (runs/<run-id>/databases/<namespace>/<instance>/<database>…): the whole
	// chain must be created by Put, and the delete must remove exactly the
	// folders it emptied, stopping at the runs/ grouping at the keep depth.
	// Only the shallow shape had been exercised live before.
	t.Run("deep-key", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()
		s := build()
		const runID = "2026-10-14T00-00-00Z"
		keys := []string{
			"runs/" + runID + "/databases/example/db-0/postgres.dump.zst",
			"runs/" + runID + "/databases/example/db-0/pgdata/full/postgres.dump.zst",
		}
		bodies := map[string][]byte{}
		for _, key := range keys {
			body := make([]byte, 64<<10)
			if _, err := rand.Read(body); err != nil {
				t.Fatal(err)
			}
			bodies[key] = body
			if err := retryLive(t, "Put "+key, func() error {
				_, err := s.Put(ctx, storage.PutRequest{Key: key, Size: int64(len(body))}, bytes.NewReader(body))
				return err
			}); err != nil {
				t.Fatalf("Put %s: %v", key, err)
			}
			st, err := s.Stat(ctx, key)
			if err != nil {
				t.Fatalf("Stat %s: %v", key, err)
			}
			if st.Size != int64(len(body)) {
				t.Errorf("Stat %s reported %+v, want %d bytes", key, st, len(body))
			}
			var got bytes.Buffer
			if err := s.Get(ctx, key, &got); err != nil {
				t.Fatalf("Get %s: %v", key, err)
			}
			if !bytes.Equal(got.Bytes(), body) {
				t.Errorf("Get %s returned %d bytes, sha256 equal: %v", key, got.Len(),
					sha256.Sum256(got.Bytes()) == sha256.Sum256(body))
			}
			t.Logf("deep key %s: %d bytes stored, stat and get equal", key, len(body))
		}
		seen := map[string]int{}
		token := ""
		for page := 0; ; page++ {
			listPage, err := s.List(ctx, storage.ListRequest{Prefix: "runs/", Token: token})
			if err != nil {
				t.Fatalf("List page %d: %v", page, err)
			}
			for _, o := range listPage.Objects {
				seen[o.Key]++
			}
			if listPage.NextToken == "" {
				break
			}
			if page > 10 {
				t.Fatal("listing did not terminate")
			}
			token = listPage.NextToken
		}
		for _, key := range keys {
			if seen[key] != 1 {
				t.Errorf("List returned the deep key %s %d times, want 1", key, seen[key])
			}
		}
		for _, key := range keys {
			if err := s.Delete(ctx, key); err != nil {
				t.Fatalf("Delete %s: %v", key, err)
			}
		}
		gone := []string{
			root + "/runs/" + runID + "/databases/example/db-0/pgdata/full",
			root + "/runs/" + runID + "/databases/example/db-0/pgdata",
			root + "/runs/" + runID + "/databases/example/db-0",
			root + "/runs/" + runID + "/databases/example",
			root + "/runs/" + runID + "/databases",
			root + "/runs/" + runID,
		}
		for _, dir := range gone {
			code, _, _ := client.do(http.MethodGet, "/resources", url.Values{"path": {dir}, "fields": {"type"}}, nil)
			if code != http.StatusNotFound {
				t.Errorf("folder %s: HTTP %d, want 404 (every folder the deletes emptied must be gone)", dir, code)
			}
		}
		code, body, _ := client.do(http.MethodGet, "/resources", url.Values{"path": {root + "/runs"}, "fields": {"type"}}, nil)
		if code != http.StatusOK || !strings.Contains(string(body), `"dir"`) {
			t.Errorf("the runs/ grouping at the keep depth: HTTP %d body %s, want 200 dir", code, body)
		}
		code, body, _ = client.do(http.MethodGet, "/resources", url.Values{"path": {root}, "fields": {"type"}}, nil)
		if code != http.StatusOK || !strings.Contains(string(body), `"dir"`) {
			t.Errorf("the configured root: HTTP %d body %s, want 200 dir", code, body)
		}
		t.Logf("deep key cleanup: %d emptied folders below the keep depth are gone, %s/runs survived (HTTP %d)", len(gone), root, code)
	})

	// The emptiness proof depends on the service stating the total under the
	// plugin's own fields filter: report that shape for a folder with children
	// and for an empty one.
	t.Run("emptiness-shape", func(t *testing.T) {
		ctx := context.Background()
		s := build()
		dir := root + "/shape"
		// The harness has no engine to retry a stalled control call, so the
		// setup retries it the way the engine would.
		if err := retryLive(t, "mkdir "+dir, func() error { return s.mkdirAll(ctx, dir) }); err != nil {
			t.Fatal(err)
		}
		body := []byte("shape probe")
		if err := retryLive(t, "Put shape/one.bin", func() error {
			_, err := s.Put(ctx, storage.PutRequest{Key: "shape/one.bin", Size: int64(len(body))}, bytes.NewReader(body))
			return err
		}); err != nil {
			t.Fatal(err)
		}
		child := root + "/shape/child"
		if err := retryLive(t, "mkdir "+child, func() error { return s.mkdirAll(ctx, child) }); err != nil {
			t.Fatal(err)
		}
		for _, folder := range []string{dir, child} {
			code, raw, _ := client.do(http.MethodGet, "/resources", url.Values{
				"path":   {folder},
				"sort":   {"name"},
				"fields": {"_embedded.total,_embedded.items.name,_embedded.items.type,_embedded.items.size,_embedded.items.modified,_embedded.items.md5"}}, nil)
			var ans struct {
				Embedded struct {
					Total *int             `json:"total"`
					Items []map[string]any `json:"items"`
				} `json:"_embedded"`
			}
			totalPresent := false
			itemsPresent := false
			if code == http.StatusOK {
				if err := json.Unmarshal(raw, &ans); err != nil {
					t.Fatalf("shape answer unreadable: %v", err)
				}
				totalPresent = ans.Embedded.Total != nil
				itemsPresent = ans.Embedded.Items != nil
			}
			t.Logf("emptiness shape for %s: HTTP %d, _embedded.total present: %v, items present: %v, items: %d",
				baseName(folder), code, totalPresent, itemsPresent, len(ans.Embedded.Items))
			if code != http.StatusOK || !totalPresent || !itemsPresent {
				t.Errorf("the service did not state total and items under the plugin's fields filter (HTTP %d, total %v, items %v)", code, totalPresent, itemsPresent)
			}
		}
	})

	// The depth bound in action: after every object under a depth-1 grouping
	// folder is deleted, that folder — genuinely empty by then — is still there.
	t.Run("prune-depth-bound", func(t *testing.T) {
		ctx := context.Background()
		s := build()
		for _, key := range []string{"keep/sub/y.bin", "keep/x.bin"} {
			body := []byte(key)
			if err := retryLive(t, "Put "+key, func() error {
				_, err := s.Put(ctx, storage.PutRequest{Key: key, Size: int64(len(body))}, bytes.NewReader(body))
				return err
			}); err != nil {
				t.Fatal(err)
			}
		}
		if err := s.Delete(ctx, "keep/sub/y.bin"); err != nil {
			t.Fatal(err)
		}
		code, _, _ := client.do(http.MethodGet, "/resources", url.Values{"path": {root + "/keep/sub"}, "fields": {"type"}}, nil)
		if code != http.StatusNotFound {
			t.Errorf("the emptied folder below the keep depth: HTTP %d, want 404", code)
		}
		if _, err := s.Put(ctx, storage.PutRequest{Key: "keep/x.bin", Size: 1}, strings.NewReader("x")); err != nil {
			t.Fatal(err)
		}
		if err := s.Delete(ctx, "keep/x.bin"); err != nil {
			t.Fatal(err)
		}
		code, body, _ := client.do(http.MethodGet, "/resources", url.Values{"path": {root + "/keep"}, "fields": {"type"}}, nil)
		if code != http.StatusOK || !strings.Contains(string(body), `"dir"`) {
			t.Errorf("the grouping folder at the keep depth: HTTP %d body %s, want 200 dir", code, body)
		}
		t.Logf("prune depth bound: %s/keep survived the delete that emptied it (HTTP %d, dir)", root, code)
	})

	t.Run("probes", func(t *testing.T) {
		runProbes(t, client, rec, root)
	})
}

// TestLiveYandexNameThroughput measures upload throughput per object name on
// one network: the same random payload goes up under each name ending the tool
// produces, twice each in alternating order, so a network swing does not read
// as a per-name pattern. This is a laptop, not the cluster node — the question
// is the relative pattern, not the absolute rate. The payload starts at 16 MiB
// and is raised to 32 MiB only when every 16 MiB upload finished in seconds.
func TestLiveYandexNameThroughput(t *testing.T) {
	tokenFile := os.Getenv("YDISK_LIVE_TOKEN_FILE")
	if tokenFile == "" {
		t.Skip("YDISK_LIVE_TOKEN_FILE names a mode-600 file holding an OAuth token; skipping")
	}
	raw, err := os.ReadFile(tokenFile)
	if err != nil {
		t.Fatalf("reading the token file: %v", err)
	}
	token := strings.TrimSpace(string(raw))
	base := envOrLive("YDISK_LIVE_API", defaultLiveAPI)
	root := yandexConformance.Generate(t)
	rec := &liveRecorder{}
	build := liveBuilder(t, tokenFile, base, root, rec)
	client := &liveClient{t: t, base: base, token: token, http: &http.Client{}}

	before := client.listing("app:/")
	beforeDigest, beforeCount := digestOf(otherThan(before, baseName(root)))
	t.Cleanup(func() {
		after := client.listing("app:/")
		afterDigest, afterCount := digestOf(otherThan(after, baseName(root)))
		if beforeDigest != afterDigest || beforeCount != afterCount {
			t.Errorf("top-level app:/ changed: before %s/%d, after %s/%d", beforeDigest, beforeCount, afterDigest, afterCount)
		}
		t.Logf("top-level app:/ other entries: count=%d digest=%s (before and after identical)", afterCount, afterDigest)
	})
	t.Cleanup(func() {
		cleanupLive(t, build(), client, root)
		auditPaths(t, rec.snapshot(), root, base)
	})
	t.Logf("throughput root: %s", root)

	size := int64(16 << 20)
	rows := measureNames(t, build(), size)
	if slowestSeconds(rows) < 5 {
		t.Logf("every %s upload finished in under 5 s; re-measuring at 32 MiB", human(size))
		size = 32 << 20
		rows = measureNames(t, build(), size)
	}

	fastest, slowest := rows[0], rows[0]
	for _, r := range rows {
		t.Logf("%-22s pass %d: %8.2fs  %6.2f MiB/s", r.name, r.pass, r.seconds, r.mibps)
		if r.seconds < fastest.seconds {
			fastest = r
		}
		if r.seconds > slowest.seconds {
			slowest = r
		}
	}
	t.Logf("payload %s; fastest %s (%.2f s), slowest %s (%.2f s), ratio %.1fx",
		human(size), fastest.name, fastest.seconds, slowest.name, slowest.seconds, slowest.seconds/fastest.seconds)
}

type nameRow struct {
	name    string
	pass    int
	seconds float64
	mibps   float64
}

// measureNames uploads the same random payload once per name ending, twice
// each, the second pass in reverse order.
func measureNames(t *testing.T, s *Storage, size int64) []nameRow {
	t.Helper()
	payload := make([]byte, size)
	if _, err := rand.Read(payload); err != nil {
		t.Fatal(err)
	}
	names := []string{
		"probe.dump.zst", "probe.rdb.zst", "probe.tar.zst", "probe.db.zst.age", "probe.tar.age",
		"manifest.json", "object.bin",
	}
	var rows []nameRow
	for pass := 1; pass <= 2; pass++ {
		order := names
		if pass == 2 {
			order = append([]string(nil), names...)
			slices.Reverse(order)
		}
		for _, name := range order {
			key := fmt.Sprintf("speed/pass%d/%s", pass, name)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
			start := time.Now()
			_, err := s.Put(ctx, storage.PutRequest{Key: key, Size: size}, bytes.NewReader(payload))
			elapsed := time.Since(start)
			cancel()
			if err != nil {
				t.Fatalf("pass %d, %s: %v", pass, name, err)
			}
			rows = append(rows, nameRow{name: name, pass: pass,
				seconds: elapsed.Seconds(), mibps: float64(size) / elapsed.Seconds() / (1 << 20)})
		}
	}
	return rows
}

func slowestSeconds(rows []nameRow) float64 {
	var out float64
	for _, r := range rows {
		out = max(out, r.seconds)
	}
	return out
}

// retryLive runs one harness setup call under the engine's policy shape: a few
// attempts, because the test process has no retry loop of its own and a live
// control call can stall on a busy link.
func retryLive(t *testing.T, what string, fn func() error) error {
	t.Helper()
	var err error
	for attempt := 1; attempt <= 3; attempt++ {
		if err = fn(); err == nil {
			return nil
		}
		t.Logf("%s: attempt %d failed: %v", what, attempt, err)
		time.Sleep(5 * time.Second)
	}
	return err
}

// TestLiveYandexCleanup is a maintenance entry point for the end-to-end
// verification: it removes everything under YDISK_LIVE_CLEANUP_ROOT (which is
// the e2e conformance root) through the plugin and proves the folder 404s.
// It writes nothing outside that root and deletes the root itself last.
func TestLiveYandexCleanup(t *testing.T) {
	root, err := yandexConformance.Resolve(os.Getenv("YDISK_LIVE_CLEANUP_ROOT"))
	switch {
	case errors.Is(err, storagetest.ErrLocationUnset):
		t.Skip("YDISK_LIVE_CLEANUP_ROOT names the folder to remove; skipping")
	case err != nil:
		t.Fatalf("%v", err)
	}
	tokenFile := os.Getenv("YDISK_LIVE_TOKEN_FILE")
	raw, err := os.ReadFile(tokenFile)
	if err != nil {
		t.Fatalf("reading the token file: %v", err)
	}
	base := envOrLive("YDISK_LIVE_API", defaultLiveAPI)
	s, err := newStorage(Config{
		APIURL:             base,
		Token:              plugin.Secret{File: tokenFile},
		Root:               root,
		PermanentDelete:    true,
		RequestTimeout:     model.Duration(30 * time.Second),
		CommitPollInterval: model.Duration(2 * time.Second),
		CommitPollTimeout:  model.Duration(5 * time.Minute),
	}, testLogger())
	if err != nil {
		t.Fatalf("constructing the live storage: %v", err)
	}
	client := &liveClient{t: t, base: base, token: strings.TrimSpace(string(raw)), http: &http.Client{}}
	cleanupLive(t, s, client, root)
}

// runProbes measures the behaviours the offline fake only assumes, on the real
// service, inside the conformance root.
func runProbes(t *testing.T, c *liveClient, rec *liveRecorder, root string) {
	probeDir := root + "/probe"
	filePath := probeDir + "/file.bin"

	// The upload-href endpoint refuses a path whose parent folder is absent
	// (HTTP 409 DiskPathDoesntExistsError on the real service), which is why
	// Put creates the folders first — do the same here.
	if code, body, _ := c.do(http.MethodPut, "/resources", url.Values{"path": {probeDir}}, nil); code != http.StatusCreated {
		t.Fatalf("probe folder: HTTP %d: %s", code, body)
	}

	// A file to probe metadata and download against.
	code, hrefBody, _ := c.do(http.MethodGet, "/resources/upload",
		url.Values{"path": {filePath}, "overwrite": {"true"}}, nil)
	if code != http.StatusOK {
		t.Fatalf("upload href: HTTP %d: %s", code, hrefBody)
	}
	var href struct {
		Href string `json:"href"`
	}
	if err := json.Unmarshal(hrefBody, &href); err != nil || href.Href == "" {
		t.Fatalf("upload href answer unreadable: %s", hrefBody)
	}
	payload := bytes.Repeat([]byte("probe-"), 100)
	code, _ = c.rawDo(http.MethodPut, href.Href, bytes.NewReader(payload))
	t.Logf("probe 1a: first PUT to a fresh href -> HTTP %d", code)

	// Probe 1: is the href single-use?
	code, _ = c.rawDo(http.MethodPut, href.Href, strings.NewReader("second"))
	t.Logf("probe 1b: second PUT to the SAME href -> HTTP %d (the fake models the measured answer)", code)

	// Probe 2: status of an absent path.
	code, body, _ := c.do(http.MethodGet, "/resources",
		url.Values{"path": {probeDir + "/definitely-absent.bin"}, "fields": {"type"}}, nil)
	t.Logf("probe 2: GET of an absent path -> HTTP %d (fake assumes 404); body keys %v", code, bodyKeys(body))

	// Probe 6: error-body shape, from a real error answer.
	if len(body) > 0 {
		t.Logf("probe 6: error-body keys %v", bodyKeys(body))
	}

	// Probe 3: folder creation answers.
	code, _, _ = c.do(http.MethodPut, "/resources", url.Values{"path": {probeDir + "/folder"}}, nil)
	t.Logf("probe 3a: PUT of a new folder -> HTTP %d (the fake models 201)", code)
	code, body, _ = c.do(http.MethodPut, "/resources", url.Values{"path": {probeDir + "/folder"}}, nil)
	t.Logf("probe 3b: PUT of the same folder again -> HTTP %d (fake assumes 409); body keys %v", code, bodyKeys(body))

	// Probe 5: timestamp and md5 of a committed file.
	code, body, _ = c.do(http.MethodGet, "/resources",
		url.Values{"path": {filePath}, "fields": {"type,size,modified,md5"}}, nil)
	if code != http.StatusOK {
		t.Fatalf("stat probe: HTTP %d: %s", code, body)
	}
	var meta struct {
		Modified string `json:"modified"`
		MD5      string `json:"md5"`
		Size     int64  `json:"size"`
	}
	if err := json.Unmarshal(body, &meta); err != nil {
		t.Fatalf("stat probe unreadable: %v", err)
	}
	_, unzoned := time.Parse("2006-01-02T15:04:05", meta.Modified)
	_, spaced := time.Parse("2006-01-02 15:04:05", meta.Modified)
	t.Logf("probe 5: modified=%q (parses as RFC3339: %v, unzoned: %v, spaced: %v), md5=%d hex chars, size=%d",
		meta.Modified, parseableRFC3339(meta.Modified), unzoned, spaced, len(meta.MD5), meta.Size)

	// Probe 7: the download href, with and without the OAuth header.
	code, body, _ = c.do(http.MethodGet, "/resources/download", url.Values{"path": {filePath}}, nil)
	if code != http.StatusOK {
		t.Fatalf("download href: HTTP %d: %s", code, body)
	}
	var dl struct {
		Href string `json:"href"`
	}
	if err := json.Unmarshal(body, &dl); err != nil || dl.Href == "" {
		t.Fatalf("download href answer unreadable: %s", body)
	}
	code, _ = c.rawDo(http.MethodGet, dl.Href, nil)
	t.Logf("probe 7a: GET of the download href with NO OAuth header -> HTTP %d (the plugin sends none)", code)
	req, err := http.NewRequest(http.MethodGet, dl.Href, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "OAuth "+c.token)
	resp, err := c.http.Do(req)
	if err != nil {
		t.Fatalf("download with header: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	t.Logf("probe 7b: GET of the same href WITH the OAuth header -> HTTP %d", resp.StatusCode)

	// Probe 4: delete answers — a file, an absent path, an empty folder, a
	// non-empty folder (the last one is expected to be asynchronous).
	code, _, _ = c.do(http.MethodDelete, "/resources",
		url.Values{"path": {filePath}, "permanently": {"true"}}, nil)
	t.Logf("probe 4a: DELETE of a file -> HTTP %d (fake assumes the plugin only issues 204)", code)
	code, _, _ = c.do(http.MethodDelete, "/resources",
		url.Values{"path": {probeDir + "/never-existed.bin"}, "permanently": {"true"}}, nil)
	t.Logf("probe 4b: DELETE of an absent path -> HTTP %d (fake assumes 404)", code)
	code, _, _ = c.do(http.MethodDelete, "/resources",
		url.Values{"path": {probeDir + "/folder"}, "permanently": {"true"}}, nil)
	t.Logf("probe 4c: DELETE of an empty folder -> HTTP %d (fake assumes 204)", code)

	// A non-empty folder: create it, put one file inside, then delete it.
	if code, body, _ := c.do(http.MethodPut, "/resources", url.Values{"path": {probeDir + "/full"}}, nil); code != http.StatusCreated {
		t.Fatalf("probe folder: HTTP %d: %s", code, body)
	}
	code, hrefBody, _ = c.do(http.MethodGet, "/resources/upload",
		url.Values{"path": {probeDir + "/full/inner.bin"}, "overwrite": {"true"}}, nil)
	if code != http.StatusOK {
		t.Fatalf("probe inner href: HTTP %d: %s", code, hrefBody)
	}
	if err := json.Unmarshal(hrefBody, &href); err != nil || href.Href == "" {
		t.Fatalf("probe inner href unreadable: %s", hrefBody)
	}
	if code, _ = c.rawDo(http.MethodPut, href.Href, strings.NewReader("inner")); code != http.StatusCreated && code != http.StatusAccepted {
		t.Fatalf("probe inner upload: HTTP %d", code)
	}
	start := time.Now()
	code, body, _ = c.do(http.MethodDelete, "/resources",
		url.Values{"path": {probeDir + "/full"}, "permanently": {"true"}}, nil)
	t.Logf("probe 4d: DELETE of a NON-EMPTY folder -> HTTP %d (docs say 202 and delete the contents); body keys %v", code, bodyKeys(body))
	for i := 0; i < 60; i++ {
		gcode, _, _ := c.do(http.MethodGet, "/resources", url.Values{"path": {probeDir + "/full"}, "fields": {"type"}}, nil)
		if gcode == http.StatusNotFound {
			t.Logf("probe 4e: the non-empty folder was gone after %s", time.Since(start).Round(time.Second))
			break
		}
		time.Sleep(time.Second)
	}

	// The probe folder holds nothing after this except empty folders the
	// plugin's own delete path will prune in the final cleanup.
	code, _, _ = c.do(http.MethodDelete, "/resources",
		url.Values{"path": {probeDir}, "permanently": {"true"}}, nil)
	t.Logf("probe cleanup: DELETE of the probe folder -> HTTP %d", code)
}

func bodyKeys(body []byte) []string {
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		return nil
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func parseableRFC3339(s string) bool {
	_, err := time.Parse(time.RFC3339, s)
	return err == nil
}

func baseName(p string) string {
	if i := strings.LastIndex(p, "/"); i >= 0 {
		return p[i+1:]
	}
	return p
}

func otherThan(entries []dirEntry, name string) []dirEntry {
	out := make([]dirEntry, 0, len(entries))
	for _, e := range entries {
		if e.Name != name {
			out = append(out, e)
		}
	}
	return out
}

// countListRequests counts recorded control-plane listing requests whose path
// parameter is folder or lies under it.
func countListRequests(reqs []recorded, folder string) int {
	n := 0
	for _, r := range reqs {
		if r.apiPath == folder || strings.HasPrefix(r.apiPath, folder+"/") {
			n++
		}
	}
	return n
}

// describeUpload finds the data PUT of one object and the metadata polls that
// followed it: the status the upload href answered, and how many commit polls
// the plugin made afterwards.
func describeUpload(reqs []recorded, full string) (string, int) {
	uploadStatus := "not seen"
	polls := 0
	seenUpload := false
	for _, r := range reqs {
		if r.method == http.MethodPut && r.apiPath == "" && strings.Contains(r.host, "yandex") {
			seenUpload = true
			uploadStatus = fmt.Sprintf("HTTP %d", r.status)
			continue
		}
		if seenUpload && r.apiPath == full {
			polls++
		}
	}
	return uploadStatus, polls
}

// auditPaths proves the plugin never named a path outside its root: every
// control-plane request carrying a path parameter must sit inside the root,
// and every request must go to the configured API host (href hosts aside).
func auditPaths(t *testing.T, reqs []recorded, root, api string) {
	t.Helper()
	apiHost := hostOf(api)
	outside := 0
	for _, r := range reqs {
		if r.apiPath != "" && r.apiPath != root && !strings.HasPrefix(r.apiPath, root+"/") {
			outside++
			t.Errorf("a control request named %q, outside the root %q", r.apiPath, root)
		}
		if r.host == apiHost && !strings.HasPrefix(r.path, "/v1/disk/") {
			t.Errorf("unexpected endpoint %s on the API host", r.path)
		}
	}
	t.Logf("request audit: %d requests recorded, %d named a path outside the root", len(reqs), outside)
}

// cleanupLive removes everything the run wrote: every object through the
// plugin (which prunes the folders it empties), then the conformance root
// itself through the raw API, then proves the root is 404.
func cleanupLive(t *testing.T, s *Storage, c *liveClient, root string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	for i := 0; i < 200; i++ {
		page, err := s.List(ctx, storage.ListRequest{})
		if err != nil {
			t.Errorf("cleanup: list: %v", err)
			return
		}
		if len(page.Objects) == 0 {
			break
		}
		for _, o := range page.Objects {
			if err := s.Delete(ctx, o.Key); err != nil {
				t.Errorf("cleanup: delete %s: %v", o.Key, err)
			}
		}
	}
	code, body, _ := c.do(http.MethodDelete, "/resources",
		url.Values{"path": {root}, "permanently": {"true"}}, nil)
	if code != http.StatusOK && code != http.StatusNoContent && code != http.StatusAccepted {
		t.Errorf("cleanup: deleting the conformance root: HTTP %d: %s", code, body)
	}
	deadline := time.Now().Add(2 * time.Minute)
	for {
		code, _, _ = c.do(http.MethodGet, "/resources", url.Values{"path": {root}, "fields": {"type"}}, nil)
		if code == http.StatusNotFound {
			break
		}
		if time.Now().After(deadline) {
			t.Errorf("cleanup: the conformance root still answers HTTP %d", code)
			return
		}
		time.Sleep(2 * time.Second)
	}
	t.Logf("cleanup: %s no longer exists (HTTP 404 on its metadata)", root)
}

func human(n int64) string {
	switch {
	case n >= 1<<20:
		return fmt.Sprintf("%.1f MiB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1f KiB", float64(n)/(1<<10))
	default:
		return fmt.Sprintf("%d B", n)
	}
}
