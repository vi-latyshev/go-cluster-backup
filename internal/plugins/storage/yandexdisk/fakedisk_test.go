package yandexdisk_test

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	json "encoding/json/v2"
)

// fakeDisk is a minimal Yandex Disk REST server: folders, files, one-upload
// hrefs, uploads whose commit lags ("202"), offset paging, and per-operation
// fault injection. Every operation is counted, so a test can prove that one
// contract call made exactly the requests one attempt means — a plugin that
// retried on its own would break those counts.
//
// The answers follow what the real service did when measured live (2026-10-12;
// see each handler): paths in the disk:/ scheme, an href that serves one
// upload, DELETE of a non-empty folder that answers 202 and removes the whole
// subtree, folder creation 201/409, an absent path 404, error bodies carrying
// error/description/message, timestamps in RFC 3339 with an offset. A listing
// answer can be made incomplete (listNoTotal, listNoItems, listContradicts) so
// a test can hold the plugin to "cannot tell" where the service would delete a
// non-empty folder recursively.
type fakeDisk struct {
	mu              sync.Mutex
	folders         map[string]bool
	files           map[string]*fakeFile
	pending         map[string]*fakeFile // accepted uploads waiting to become visible
	polls           map[string]int       // visibility polls per path
	hrefs           map[string]*fakeHref
	hrefTokens      []string // href tokens PUT to, in order
	downloads       map[string]string
	nextID          int
	counts          map[string]int
	failAlways      map[string]int
	failNext        map[string]int
	retryAfter      map[string]int  // seconds of Retry-After per operation
	pageLimit       int             // children per listing response; 0 = all
	uploadMode      string          // "", "202", "never", or an HTTP status
	commitAfter     int             // for "202": visible after this many stat polls
	hangPutData     bool            // block the data PUT until the request context ends
	hangStat        bool            // block a metadata answer until the request context ends
	statBody        string          // when set, the raw 200 body of every metadata answer
	hrefOverride    string          // when set, the href every upload/download answer carries
	uploadHrefBad   bool            // the upload href answer carries no href
	downloadHrefBad bool            // the download href answer carries no href
	listEmptyPage   bool            // a listing answer states a total with no items
	listBody        string          // when set, the raw 200 body of every listing answer
	dropTransport   map[string]bool // close the connection without an answer, per operation
	shortAnswer     map[string]bool // answer with a body shorter than its declared length
	dropFolderDel   bool            // close the connection on a folder delete
	downloadStall   bool            // the download href sends half the body, then waits
	failDirDelete   int             // status injected into a folder delete; 0 = none
	listPaths       []string        // folder path of every listing request, in order
	listNoTotal     bool            // a listing answer without _embedded.total
	listNoItems     bool            // a listing answer without _embedded.items
	listContradicts bool            // a listing answer stating total 0 while items are non-empty
	badMD5          bool            // report an md5 that is not the body's, to pin the leniency
	sawChunkedBody  bool            // any request arrived with Transfer-Encoding: chunked or Content-Encoding: aws-chunked
	srv             *httptest.Server
}

type fakeFile struct {
	size     int64
	md5      string
	modified string
	body     []byte
}

type fakeHref struct {
	path     string
	consumed bool
}

type fakeDiskServer struct {
	*fakeDisk
}

func newFakeDisk(t *testing.T) *fakeDiskServer {
	t.Helper()
	f := &fakeDisk{
		folders:       map[string]bool{"app:": true},
		files:         map[string]*fakeFile{},
		pending:       map[string]*fakeFile{},
		polls:         map[string]int{},
		hrefs:         map[string]*fakeHref{},
		downloads:     map[string]string{},
		counts:        map[string]int{},
		failAlways:    map[string]int{},
		failNext:      map[string]int{},
		retryAfter:    map[string]int{},
		dropTransport: map[string]bool{},
		shortAnswer:   map[string]bool{},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/disk/resources", f.resources)
	mux.HandleFunc("/v1/disk/resources/upload", f.upload)
	mux.HandleFunc("/v1/disk/resources/download", f.download)
	mux.HandleFunc("/up/", f.putData)
	mux.HandleFunc("/dl/", f.getData)
	mux.HandleFunc("/boom", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusInternalServerError, errJSON("DiskError", "boom"))
	})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if len(r.TransferEncoding) > 0 && r.TransferEncoding[0] != "identity" {
			f.mu.Lock()
			f.sawChunkedBody = true
			f.mu.Unlock()
		}
		mux.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	f.srv = srv
	return &fakeDiskServer{fakeDisk: f}
}

func (f *fakeDisk) count(kind string) {
	f.mu.Lock()
	f.counts[kind]++
	f.mu.Unlock()
}

// Count reports how many requests of one operation the fake saw.
func (f *fakeDisk) Count(kind string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.counts[kind]
}

// fail returns an injected status for one operation, consuming a one-shot
// injection and applying the configured Retry-After seconds.
func (f *fakeDisk) fail(kind string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	st := f.failAlways[kind]
	if st == 0 {
		if s, ok := f.failNext[kind]; ok {
			st = s
			delete(f.failNext, kind)
		}
	}
	return st
}

func (f *fakeDisk) retryAfterSeconds(kind string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.retryAfter[kind]
}

func (f *fakeDisk) transport(op string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.dropTransport[op]
}

func (f *fakeDisk) short(op string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.shortAnswer[op]
}

// dropConn closes the connection with no answer: a broken link, not a status.
func dropConn(w http.ResponseWriter) {
	hj, ok := w.(http.Hijacker)
	if !ok {
		return
	}
	conn, _, err := hj.Hijack()
	if err != nil {
		return
	}
	_ = conn.Close()
}

// writeShort sends a 200 whose declared length exceeds the bytes sent, then
// closes: the client's read of the body fails part-way.
func writeShort(w http.ResponseWriter) {
	hj, ok := w.(http.Hijacker)
	if !ok {
		return
	}
	conn, buf, err := hj.Hijack()
	if err != nil {
		return
	}
	defer func() { _ = conn.Close() }()
	_, _ = buf.WriteString("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 4096\r\n\r\n{\"partial\":")
	_ = buf.Flush()
}

// putObject seeds a committed file with its folders, bypassing fault injection.
func (f *fakeDisk) putObject(path string, body []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.mkdirLocked(parentOf(path))
	f.files[path] = &fakeFile{size: int64(len(body)), md5: md5Hex(body), modified: liveModified(), body: body}
}

func (f *fakeDisk) mkdirLocked(p string) {
	for p != "" && p != "app:" && p != "app:/" {
		f.folders[p] = true
		p = parentOf(p)
	}
}

func parentOf(p string) string {
	if i := strings.LastIndex(p, "/"); i >= 0 {
		return p[:i]
	}
	return ""
}

func (f *fakeDisk) writeStatus(w http.ResponseWriter, kind string, status int) {
	if secs := f.retryAfterSeconds(kind); secs > 0 {
		w.Header().Set("Retry-After", strconv.Itoa(secs))
	}
	writeJSON(w, status, errJSON("DiskError", fmt.Sprintf("injected %d for %s", status, kind)))
}

func (f *fakeDisk) resources(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	p := q.Get("path")
	// The plugin's listing call is the only one asking for a sorted page.
	listing := q.Get("sort") != ""
	switch r.Method {
	case http.MethodPut:
		f.count("mkdir")
		if f.transport("mkdir") {
			dropConn(w)
			return
		}
		if st := f.fail("mkdir"); st != 0 {
			f.writeStatus(w, "mkdir", st)
			return
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		parent := parentOf(p)
		if !f.folders[parent] && parent != "app:" {
			writeJSON(w, http.StatusNotFound, errJSON("DiskNotFoundError", "Resource not found"))
			return
		}
		if f.folders[p] || f.files[p] != nil {
			writeJSON(w, http.StatusConflict, errJSON("DiskResourceAlreadyExistsError", "Resource already exists"))
			return
		}
		f.folders[p] = true
		writeJSON(w, http.StatusCreated, map[string]any{"path": diskPath(p), "type": "dir"})
	case http.MethodGet:
		kind := "stat"
		if listing {
			kind = "list"
		}
		f.count(kind)
		if f.transport(kind) {
			dropConn(w)
			return
		}
		if f.short(kind) {
			writeShort(w)
			return
		}
		if st := f.fail(kind); st != 0 {
			f.writeStatus(w, kind, st)
			return
		}
		if kind == "stat" {
			f.mu.Lock()
			hang := f.hangStat
			f.mu.Unlock()
			if hang {
				<-r.Context().Done()
				return
			}
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		if listing {
			f.listPaths = append(f.listPaths, p)
		}
		if !listing && f.statBody != "" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, f.statBody)
			return
		}
		if listing && f.listBody != "" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, f.listBody)
			return
		}
		if !listing {
			if pending, ok := f.pending[p]; ok {
				f.polls[p]++
				if f.commitAfter > 0 && f.polls[p] >= f.commitAfter {
					f.files[p] = pending
					delete(f.pending, p)
				}
			}
		}
		if file, ok := f.files[p]; ok {
			writeJSON(w, http.StatusOK, f.fileJSON(p, file))
			return
		}
		if f.folders[p] {
			writeJSON(w, http.StatusOK, f.dirJSON(p, q, listing))
			return
		}
		writeJSON(w, http.StatusNotFound, errJSON("DiskNotFoundError", "Resource not found"))
	case http.MethodDelete:
		f.count("delete")
		if f.transport("delete") {
			dropConn(w)
			return
		}
		if st := f.fail("delete"); st != 0 {
			f.writeStatus(w, "delete", st)
			return
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		if _, ok := f.files[p]; ok {
			delete(f.files, p)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if f.dropFolderDel && f.folders[p] {
			dropConn(w)
			return
		}
		if f.failDirDelete != 0 && f.folders[p] {
			writeJSON(w, f.failDirDelete, errJSON("DiskError", "injected folder delete failure"))
			return
		}
		delete(f.pending, p)
		if f.folders[p] {
			// The real service answers 202 and starts removing the folder
			// together with its whole subtree (measured live); an empty folder
			// is a plain 204. A plugin that deletes a folder it has not proved
			// empty loses everything inside it here, exactly as it would
			// against the real Disk.
			nonEmpty := f.removeSubtreeLocked(p)
			if nonEmpty {
				writeJSON(w, http.StatusAccepted, map[string]any{
					"href": f.srv.URL + "/operations/delete", "method": "GET", "templated": false})
				return
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}
		writeJSON(w, http.StatusNotFound, errJSON("DiskNotFoundError", "Resource not found"))
	default:
		writeJSON(w, http.StatusMethodNotAllowed, errJSON("DiskMethodNotAllowedError", "Method not allowed"))
	}
}

// removeSubtreeLocked deletes a folder with every file and folder under it,
// the way the service's asynchronous folder delete does, and reports whether
// the folder held anything. The caller holds the lock.
func (f *fakeDisk) removeSubtreeLocked(p string) bool {
	nonEmpty := false
	for path := range f.files {
		if strings.HasPrefix(path, p+"/") {
			delete(f.files, path)
			nonEmpty = true
		}
	}
	for path := range f.pending {
		if strings.HasPrefix(path, p+"/") {
			delete(f.pending, path)
		}
	}
	for path := range f.folders {
		if strings.HasPrefix(path, p+"/") {
			delete(f.folders, path)
			nonEmpty = true
		}
	}
	delete(f.folders, p)
	return nonEmpty
}

func (f *fakeDisk) fileJSON(p string, file *fakeFile) map[string]any {
	md5 := file.md5
	if f.badMD5 {
		md5 = strings.Repeat("0", 32)
	}
	return map[string]any{"name": basename(p), "path": diskPath(p), "type": "file",
		"size": file.size, "md5": md5, "modified": file.modified}
}

func (f *fakeDisk) dirJSON(p string, q url.Values, listing bool) map[string]any {
	out := map[string]any{"name": basename(p), "path": diskPath(p), "type": "dir"}
	if !listing {
		return out
	}
	var children []string
	for child := range f.folders {
		if parentOf(child) == p {
			children = append(children, child)
		}
	}
	for child := range f.files {
		if parentOf(child) == p {
			children = append(children, child)
		}
	}
	sort.Strings(children)
	offset := 0
	if v := q.Get("offset"); v != "" {
		offset, _ = strconv.Atoi(v)
	}
	limit := f.pageLimit
	if limit <= 0 || limit > len(children) {
		limit = len(children)
	}
	end := min(offset+limit, len(children))
	var items []map[string]any
	for _, child := range children[offset:end] {
		if file, ok := f.files[child]; ok {
			items = append(items, f.fileJSON(child, file))
		} else {
			items = append(items, map[string]any{"name": basename(child), "path": diskPath(child), "type": "dir"})
		}
	}
	embedded := map[string]any{"limit": limit, "offset": offset}
	if !f.listNoTotal {
		total := len(children)
		if f.listContradicts {
			total = 0 // a contradictory answer: zero total over a non-empty item list
		}
		embedded["total"] = total
	}
	if !f.listNoItems {
		embedded["items"] = items
	}
	if f.listEmptyPage {
		// A page that states children exist while carrying none: the walk cannot
		// make progress and must say so rather than page forever.
		embedded["total"] = 1
		embedded["items"] = []map[string]any{}
	}
	out["_embedded"] = embedded
	return out
}

func (f *fakeDisk) upload(w http.ResponseWriter, r *http.Request) {
	f.count("uploadhref")
	if f.transport("uploadhref") {
		dropConn(w)
		return
	}
	if st := f.fail("uploadhref"); st != 0 {
		f.writeStatus(w, "uploadhref", st)
		return
	}
	p := r.URL.Query().Get("path")
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.uploadHrefBad {
		writeJSON(w, http.StatusOK, map[string]any{"method": "PUT"})
		return
	}
	if !f.folders[parentOf(p)] && parentOf(p) != "app:" {
		// Measured live: the upload endpoint answers 409
		// DiskPathDoesntExistsError for a path whose parent folder is absent,
		// not a plain 404.
		writeJSON(w, http.StatusConflict, errJSON("DiskPathDoesntExistsError",
			fmt.Sprintf("Specified path %q doesn't exists.", p)))
		return
	}
	f.nextID++
	token := fmt.Sprintf("h%d", f.nextID)
	f.hrefs[token] = &fakeHref{path: p}
	href := f.srv.URL + "/up/" + token
	if f.hrefOverride != "" {
		href = f.hrefOverride
	}
	writeJSON(w, http.StatusOK, map[string]any{"href": href, "method": "PUT"})
}

// putData serves one upload per href, the way the real service did when
// measured live: a second PUT to a consumed href is accepted with 201 and the
// committed object keeps the bytes of the first upload.
func (f *fakeDisk) putData(w http.ResponseWriter, r *http.Request) {
	f.count("putdata")
	if f.short("putdata") {
		_, _ = io.Copy(io.Discard, r.Body)
		writeShort(w)
		return
	}
	token := strings.TrimPrefix(r.URL.Path, "/up/")
	f.mu.Lock()
	href, ok := f.hrefs[token]
	if ok && href.consumed {
		f.mu.Unlock()
		_, _ = io.Copy(io.Discard, r.Body)
		writeJSON(w, http.StatusCreated, map[string]any{"path": diskPath(href.path)})
		return
	}
	if ok {
		href.consumed = true
		f.hrefTokens = append(f.hrefTokens, token)
	}
	hang, mode := f.hangPutData, f.uploadMode
	f.mu.Unlock()
	if !ok {
		writeJSON(w, http.StatusNotFound, errJSON("DiskNotFoundError", "upload href not found"))
		return
	}
	if hang {
		// The body must be consumed before the context sees the client's
		// disconnect: an unread request body keeps the connection busy and the
		// server never learns the client is gone.
		_, _ = io.Copy(io.Discard, r.Body)
		<-r.Context().Done()
		return
	}
	if st := f.fail("putdata"); st != 0 {
		f.writeStatus(w, "putdata", st)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errJSON("DiskError", "unreadable body"))
		return
	}
	file := &fakeFile{size: int64(len(body)), md5: md5Hex(body), modified: liveModified(), body: body}
	if n, err := strconv.Atoi(mode); err == nil && n >= 400 {
		writeJSON(w, n, errJSON("DiskError", fmt.Sprintf("injected %d for putdata", n)))
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	switch mode {
	case "202", "never":
		f.pending[href.path] = file
		writeJSON(w, http.StatusAccepted, map[string]any{})
	default:
		f.files[href.path] = file
		writeJSON(w, http.StatusCreated, map[string]any{"path": diskPath(href.path)})
	}
}

func (f *fakeDisk) download(w http.ResponseWriter, r *http.Request) {
	f.count("download")
	if f.transport("download") {
		dropConn(w)
		return
	}
	if st := f.fail("download"); st != 0 {
		f.writeStatus(w, "download", st)
		return
	}
	p := r.URL.Query().Get("path")
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.downloadHrefBad {
		writeJSON(w, http.StatusOK, map[string]any{"method": "GET"})
		return
	}
	if f.files[p] == nil {
		writeJSON(w, http.StatusNotFound, errJSON("DiskNotFoundError", "Resource not found"))
		return
	}
	f.nextID++
	token := fmt.Sprintf("d%d", f.nextID)
	f.downloads[token] = p
	href := f.srv.URL + "/dl/" + token
	if f.hrefOverride != "" {
		href = f.hrefOverride
	}
	writeJSON(w, http.StatusOK, map[string]any{"href": href, "method": "GET"})
}

func (f *fakeDisk) getData(w http.ResponseWriter, r *http.Request) {
	f.count("getdata")
	token := strings.TrimPrefix(r.URL.Path, "/dl/")
	f.mu.Lock()
	p, ok := f.downloads[token]
	file := f.files[p]
	stall := f.downloadStall
	f.mu.Unlock()
	if !ok || file == nil {
		writeJSON(w, http.StatusNotFound, errJSON("DiskNotFoundError", "Resource not found"))
		return
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(file.body)))
	w.WriteHeader(http.StatusOK)
	if stall {
		// Half the body, then wait for the client to go: the download is cut
		// mid-body rather than before it started.
		if half := len(file.body) / 2; half > 0 {
			_, _ = w.Write(file.body[:half])
		}
		if fl, ok := w.(http.Flusher); ok {
			fl.Flush()
		}
		<-r.Context().Done()
		return
	}
	_, _ = w.Write(file.body)
}

// hrefTokensUsed returns the upload href tokens the plugin actually sent bytes
// to, in order.
// setModified overrides a stored file's timestamp, for the parsing tests.
func (f *fakeDisk) setModified(path, modified string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if file, ok := f.files[path]; ok {
		file.modified = modified
	}
}

func (f *fakeDisk) hrefTokensUsed() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.hrefTokens...)
}

// ListedPaths returns the folder path of every listing request the fake saw, in
// order — what a test uses to prove a walk never left the configured root.
func (f *fakeDisk) ListedPaths() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.listPaths...)
}

// diskPath is the path spelling the real service answers with: an app-folder
// request is made with app:/ paths, but every path in a response is the
// resource's absolute location on the Disk in the disk:/ scheme ("В ответах API
// пути указываются в схеме disk:/", Yandex's app-folder documentation). The
// fake must answer the same way — a plugin that matches on a response path
// finds nothing here, exactly as it would find nothing in production.
func diskPath(p string) string {
	return "disk:/Приложения" + strings.TrimPrefix(p, "app:")
}

func (f *fakeDisk) fileBody(path string) ([]byte, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	file, ok := f.files[path]
	if !ok {
		return nil, false
	}
	return append([]byte(nil), file.body...), true
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.MarshalWrite(w, v)
}

// errJSON is an error body in the shape the real service sends (measured
// live): the code, a human description and a message.
func errJSON(code, message string) map[string]any {
	return map[string]any{"error": code, "description": message, "message": message}
}

// liveModified is the timestamp shape the real service sends: RFC 3339 with an
// explicit +00:00 offset, not a bare Z.
func liveModified() string {
	return time.Now().UTC().Format("2006-01-02T15:04:05+00:00")
}

func basename(p string) string {
	if i := strings.LastIndex(p, "/"); i >= 0 {
		return p[i+1:]
	}
	return p
}

func md5Hex(b []byte) string {
	sum := md5.Sum(b)
	return hex.EncodeToString(sum[:])
}
