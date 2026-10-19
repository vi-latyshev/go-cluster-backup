package s3_test

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeS3 is a minimal S3 server speaking the subset the plugin uses: PutObject,
// multipart create/part/complete/abort, ListMultipartUploads with markers,
// HeadObject, ListObjectsV2 with continuation tokens, GetObject and
// DeleteObject. Every operation is counted and can be made to fail, so a test
// can prove the plugin made exactly one request where one attempt means one —
// the SDK's own retryer is disabled and a silently re-enabled one would break
// the counts.
type fakeS3 struct {
	mu              sync.Mutex
	bucket          string
	objects         map[string]*fakeObject
	uploads         map[string]*fakeUpload
	nextID          int
	counts          map[string]int
	partNumbers     []int32 // every part upload, in order, across uploads
	failAlways      map[string]int
	failNext        map[string]int
	retryAfter      map[string]int
	pageSize        int // objects per ListObjectsV2 response; 0 = all
	uploadsPageSize int // uploads per ListMultipartUploads response; 0 = all
	// dropInitiated omits the Initiated element for the named uploads: the
	// service answer that says nothing about when an upload started.
	dropInitiated map[string]bool
	// stuckUploads makes ListMultipartUploads answer truncated with the markers
	// it was asked for, echoing them: a page that never moves forward.
	stuckUploads    bool
	head403         bool // HEAD answers 403 instead of 404 — a proxy hiding existence
	headWrongSize   bool // HEAD reports one byte more than was stored
	listForeign     bool // a listing answer carries a key outside the storage root
	createNoID      bool // CreateMultipartUpload answers without an upload id
	partNoETag      bool // UploadPart answers 200 without an ETag
	uploadsForeign  bool // ListMultipartUploads answers with a foreign upload
	getStall        bool // GetObject sends half the body, then waits for the client to go
	hang            map[string]bool
	putDrops        bool // PutObject answers 200 and stores nothing
	completeDrop    bool // CompleteMultipartUpload answers 200 and commits nothing
	sawChunked      bool
	sawStorage      string
	sawUnsigned     int
	sawUploadPrefix string // the prefix ListMultipartUploads was asked for
	srv             *httptest.Server
}

type fakeObject struct {
	body     []byte
	etag     string
	modified time.Time
}

type fakeUpload struct {
	key       string
	parts     map[int32][]byte
	etags     map[int32]string
	initiated time.Time
}

type fakeS3Server struct {
	*fakeS3
}

func newFakeS3(t *testing.T, bucket string) *fakeS3Server {
	t.Helper()
	f := &fakeS3{
		bucket:     bucket,
		objects:    map[string]*fakeObject{},
		uploads:    map[string]*fakeUpload{},
		counts:     map[string]int{},
		failAlways: map[string]int{},
		failNext:   map[string]int{},
		retryAfter: map[string]int{},
		hang:       map[string]bool{},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		if r.Header.Get("Authorization") == "" {
			f.sawUnsigned++
		}
		if len(r.TransferEncoding) > 0 && r.TransferEncoding[0] != "identity" {
			f.sawChunked = true
		}
		f.mu.Unlock()
		f.serve(w, r)
	}))
	t.Cleanup(srv.Close)
	f.srv = srv
	return &fakeS3Server{fakeS3: f}
}

func (f *fakeS3) serve(w http.ResponseWriter, r *http.Request) {
	// Path-style addressing puts the bucket in the first path segment; a request
	// naming another bucket must answer NoSuchBucket, like the service does.
	if seg := firstSegment(r.URL.Path); seg != f.bucket {
		f.count("wrongbucket")
		f.writeErrorCode(w, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist")
		return
	}
	key := strings.TrimPrefix(strings.TrimPrefix(r.URL.Path, "/"+f.bucket), "/")
	q := r.URL.Query()
	var op string
	switch {
	case r.Method == http.MethodGet && q.Has("uploads"):
		op = "listuploads"
	case r.Method == http.MethodGet && key == "":
		op = "list"
	case r.Method == http.MethodGet:
		op = "get"
	case r.Method == http.MethodHead:
		op = "head"
	case r.Method == http.MethodDelete && q.Has("uploadId"):
		op = "abort"
	case r.Method == http.MethodDelete:
		op = "delete"
	case r.Method == http.MethodPost && q.Has("uploads"):
		op = "create"
	case r.Method == http.MethodPost && q.Has("uploadId"):
		op = "complete"
	case r.Method == http.MethodPut && q.Has("uploadId"):
		op = "part"
	case r.Method == http.MethodPut:
		op = "put"
	default:
		op = "unknown"
	}
	f.mu.Lock()
	f.counts[op]++
	hang := f.hang[op]
	f.mu.Unlock()
	if hang {
		// The body must be consumed before the context sees the client's
		// disconnect: an unread request body keeps the connection busy and the
		// server never learns the client is gone.
		_, _ = io.Copy(io.Discard, r.Body)
		<-r.Context().Done()
		return
	}
	if st := f.fail(op); st != 0 {
		f.writeError(w, op, st)
		return
	}
	switch op {
	case "list":
		f.list(w, r)
	case "listuploads":
		f.listuploads(w, r)
	case "get":
		f.get(w, r, key)
	case "head":
		f.head(w, key)
	case "delete":
		f.delete(w, key)
	case "create":
		f.create(w, key)
	case "part":
		f.part(w, r, idOf(q))
	case "complete":
		f.complete(w, r, key, idOf(q))
	case "abort":
		f.abort(w, idOf(q))
	case "put":
		f.put(w, key, r)
	default:
		f.writeErrorCode(w, http.StatusNotImplemented, "NotImplemented", "unexpected request")
	}
}

func (f *fakeS3) count(op string) {
	f.mu.Lock()
	f.counts[op]++
	f.mu.Unlock()
}

func (f *fakeS3) fail(op string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	if st := f.failAlways[op]; st != 0 {
		return st
	}
	if st, ok := f.failNext[op]; ok {
		delete(f.failNext, op)
		return st
	}
	return 0
}

// Count reports how many requests of one operation the fake saw.
func (f *fakeS3) Count(op string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.counts[op]
}

// OpenUploads reports the upload IDs that were never completed or aborted.
func (f *fakeS3) OpenUploads() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var ids []string
	for id := range f.uploads {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// Parts returns every part number requested for uploads, in order.
func (f *fakeS3) Parts() []int32 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]int32(nil), f.partNumbers...)
}

func (f *fakeS3) putObject(key string, body []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects[key] = &fakeObject{body: body, etag: quote(md5Hex(body)), modified: time.Now().UTC()}
}

func (f *fakeS3) objectBody(key string) ([]byte, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	o, ok := f.objects[key]
	if !ok {
		return nil, false
	}
	return append([]byte(nil), o.body...), true
}

func (f *fakeS3) writeError(w http.ResponseWriter, op string, status int) {
	if secs := f.retryAfterSeconds(op); secs > 0 {
		w.Header().Set("Retry-After", strconv.Itoa(secs))
	}
	code := map[int]string{400: "InvalidRequest", 401: "AccessDenied", 403: "AccessDenied",
		404: "NoSuchKey", 429: "SlowDown", 500: "InternalError", 503: "SlowDown"}[status]
	if code == "" {
		code = "InternalError"
	}
	f.writeErrorCode(w, status, code, fmt.Sprintf("injected %d for %s", status, op))
}

func (f *fakeS3) retryAfterSeconds(op string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.retryAfter[op]
}

func (f *fakeS3) writeErrorCode(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = fmt.Fprintf(w, `<Error><Code>%s</Code><Message>%s</Message></Error>`, code, message)
}

func (f *fakeS3) put(w http.ResponseWriter, key string, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		f.writeErrorCode(w, http.StatusBadRequest, "IncompleteBody", err.Error())
		return
	}
	etag := quote(md5Hex(body))
	f.mu.Lock()
	if sc := r.Header.Get("X-Amz-Storage-Class"); sc != "" {
		f.sawStorage = sc
	}
	if !f.putDrops {
		f.objects[key] = &fakeObject{body: body, etag: etag, modified: time.Now().UTC()}
	}
	f.mu.Unlock()
	w.Header().Set("ETag", etag)
	w.WriteHeader(http.StatusOK)
}

func (f *fakeS3) head(w http.ResponseWriter, key string) {
	f.mu.Lock()
	o, ok := f.objects[key]
	hide := f.head403
	wrong := f.headWrongSize
	f.mu.Unlock()
	if hide {
		f.writeErrorCode(w, http.StatusForbidden, "AccessDenied", "hidden")
		return
	}
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	size := len(o.body)
	if wrong {
		size++
	}
	w.Header().Set("Content-Length", strconv.Itoa(size))
	w.Header().Set("ETag", o.etag)
	w.Header().Set("Last-Modified", o.modified.Format(http.TimeFormat))
	w.WriteHeader(http.StatusOK)
}

func (f *fakeS3) get(w http.ResponseWriter, r *http.Request, key string) {
	f.mu.Lock()
	o, ok := f.objects[key]
	stall := f.getStall
	f.mu.Unlock()
	if !ok {
		f.writeErrorCode(w, http.StatusNotFound, "NoSuchKey", "The specified key does not exist.")
		return
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(o.body)))
	w.Header().Set("ETag", o.etag)
	w.Header().Set("Last-Modified", o.modified.Format(http.TimeFormat))
	w.WriteHeader(http.StatusOK)
	if stall {
		// Half the body, then wait for the client to go: the download is cut
		// mid-body rather than before it started.
		if half := len(o.body) / 2; half > 0 {
			_, _ = w.Write(o.body[:half])
		}
		if fl, ok := w.(http.Flusher); ok {
			fl.Flush()
		}
		<-r.Context().Done()
		return
	}
	_, _ = w.Write(o.body)
}

func (f *fakeS3) delete(w http.ResponseWriter, key string) {
	f.mu.Lock()
	delete(f.objects, key)
	f.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

func (f *fakeS3) create(w http.ResponseWriter, key string) {
	f.mu.Lock()
	f.nextID++
	id := fmt.Sprintf("upload-%d", f.nextID)
	noID := f.createNoID
	f.uploads[id] = &fakeUpload{key: key, parts: map[int32][]byte{}, etags: map[int32]string{}, initiated: time.Now().UTC()}
	f.mu.Unlock()
	answer := id
	if noID {
		answer = ""
	}
	w.Header().Set("Content-Type", "application/xml")
	_, _ = fmt.Fprintf(w, `<InitiateMultipartUploadResult><Bucket>%s</Bucket><Key>%s</Key><UploadId>%s</UploadId></InitiateMultipartUploadResult>`,
		f.bucket, key, answer)
}

func (f *fakeS3) part(w http.ResponseWriter, r *http.Request, id string) {
	num, err := strconv.Atoi(r.URL.Query().Get("partNumber"))
	if err != nil {
		f.writeErrorCode(w, http.StatusBadRequest, "InvalidArgument", "bad partNumber")
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		f.writeErrorCode(w, http.StatusBadRequest, "IncompleteBody", err.Error())
		return
	}
	f.mu.Lock()
	upload, ok := f.uploads[id]
	noETag := f.partNoETag
	if ok {
		upload.parts[int32(num)] = body
		upload.etags[int32(num)] = quote(md5Hex(body))
		f.partNumbers = append(f.partNumbers, int32(num))
	}
	f.mu.Unlock()
	if !ok {
		f.writeErrorCode(w, http.StatusNotFound, "NoSuchUpload", "The specified upload does not exist.")
		return
	}
	if noETag {
		w.WriteHeader(http.StatusOK)
		return
	}
	w.Header().Set("ETag", quote(md5Hex(body)))
	w.WriteHeader(http.StatusOK)
}

func (f *fakeS3) complete(w http.ResponseWriter, r *http.Request, key, id string) {
	f.mu.Lock()
	upload, ok := f.uploads[id]
	drop := f.completeDrop
	f.mu.Unlock()
	if !ok {
		f.writeErrorCode(w, http.StatusNotFound, "NoSuchUpload", "The specified upload does not exist.")
		return
	}
	var parsed struct {
		Parts []struct {
			ETag       string `xml:"ETag"`
			PartNumber int32  `xml:"PartNumber"`
		} `xml:"Part"`
	}
	if err := xml.NewDecoder(r.Body).Decode(&parsed); err != nil {
		f.writeErrorCode(w, http.StatusBadRequest, "MalformedXML", err.Error())
		return
	}
	if !drop {
		var body, digests []byte
		for _, p := range parsed.Parts {
			data, ok := upload.parts[p.PartNumber]
			if !ok || upload.etags[p.PartNumber] != p.ETag {
				f.writeErrorCode(w, http.StatusBadRequest, "InvalidPart", "part does not match")
				return
			}
			body = append(body, data...)
			sum := md5.Sum(data)
			digests = append(digests, sum[:]...)
		}
		etag := quote(fmt.Sprintf("%s-%d", md5Hex(digests), len(parsed.Parts)))
		f.mu.Lock()
		f.objects[key] = &fakeObject{body: body, etag: etag, modified: time.Now().UTC()}
		delete(f.uploads, id)
		f.mu.Unlock()
		w.Header().Set("ETag", etag)
		w.Header().Set("Content-Type", "application/xml")
		_, _ = fmt.Fprintf(w, `<CompleteMultipartUploadResult><Bucket>%s</Bucket><Key>%s</Key><ETag>%s</ETag></CompleteMultipartUploadResult>`,
			f.bucket, key, etag)
		return
	}
	// The service answered 200 while committing nothing; the upload stays open,
	// exactly the case the plugin's post-completion verification must catch.
	w.Header().Set("Content-Type", "application/xml")
	_, _ = fmt.Fprintf(w, `<CompleteMultipartUploadResult><Bucket>%s</Bucket><Key>%s</Key></CompleteMultipartUploadResult>`, f.bucket, key)
}

func idOf(q map[string][]string) string {
	if v := q["uploadId"]; len(v) > 0 {
		return v[0]
	}
	return ""
}

func (f *fakeS3) abort(w http.ResponseWriter, id string) {
	f.mu.Lock()
	_, ok := f.uploads[id]
	delete(f.uploads, id)
	f.mu.Unlock()
	if !ok {
		f.writeErrorCode(w, http.StatusNotFound, "NoSuchUpload", "The specified upload does not exist.")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (f *fakeS3) list(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	prefix := q.Get("prefix")
	token := q.Get("continuation-token")
	start := 0
	if token != "" {
		start, _ = strconv.Atoi(token)
	}
	f.mu.Lock()
	var keys []string
	for k := range f.objects {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	page := f.pageSize
	if page <= 0 {
		page = len(keys)
	}
	end := min(start+page, len(keys))
	contents := make([]map[string]string, 0, end-start)
	for _, k := range keys[start:end] {
		o := f.objects[k]
		contents = append(contents, map[string]string{
			"key": k, "etag": o.etag, "modified": o.modified.UTC().Format(time.RFC3339),
			"size": strconv.Itoa(len(o.body)),
		})
	}
	total := len(keys)
	foreign := f.listForeign
	f.mu.Unlock()
	if foreign {
		// A key the storage's own prefix never returns: a listing answer that
		// carries it must still be trimmed by the plugin.
		contents = append(contents, map[string]string{"key": "outside/foreign", "etag": quote(md5Hex([]byte("x"))),
			"modified": time.Now().UTC().Format(time.RFC3339), "size": "1"})
	}
	w.Header().Set("Content-Type", "application/xml")
	_, _ = io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?><ListBucketResult>`)
	_, _ = fmt.Fprintf(w, "<Name>%s</Name><Prefix>%s</Prefix><KeyCount>%d</KeyCount><MaxKeys>%d</MaxKeys>",
		f.bucket, prefix, len(contents), page)
	if end < total {
		_, _ = fmt.Fprintf(w, "<IsTruncated>true</IsTruncated><NextContinuationToken>%d</NextContinuationToken>", end)
	} else {
		_, _ = io.WriteString(w, "<IsTruncated>false</IsTruncated>")
	}
	for _, c := range contents {
		_, _ = fmt.Fprintf(w, "<Contents><Key>%s</Key><LastModified>%s</LastModified><ETag>%s</ETag><Size>%s</Size><StorageClass>STANDARD</StorageClass></Contents>",
			c["key"], c["modified"], c["etag"], c["size"])
	}
	_, _ = io.WriteString(w, "</ListBucketResult>")
}

// listuploads answers ListMultipartUploads: the open uploads under a prefix, in
// key order, one page when uploadsPageSize says so, carrying the markers the
// service uses. The prefix it was asked for is recorded, so a test can prove the
// plugin confined the listing to its own root.
func (f *fakeS3) listuploads(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	prefix := q.Get("prefix")
	keyMarker := q.Get("key-marker")
	uploadMarker := q.Get("upload-id-marker")
	type item struct {
		key       string
		id        string
		initiated time.Time
	}
	f.mu.Lock()
	f.sawUploadPrefix = prefix
	var all []item
	for id, u := range f.uploads {
		if strings.HasPrefix(u.key, prefix) {
			all = append(all, item{key: u.key, id: id, initiated: u.initiated})
		}
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].key != all[j].key {
			return all[i].key < all[j].key
		}
		return all[i].id < all[j].id
	})
	start := 0
	for start < len(all) {
		it := all[start]
		if it.key < keyMarker || (it.key == keyMarker && uploadMarker != "" && it.id <= uploadMarker) {
			start++
			continue
		}
		break
	}
	page := f.uploadsPageSize
	if page <= 0 {
		page = len(all)
	}
	end := min(start+page, len(all))
	uploads := all[start:end]
	dropInitiated := f.dropInitiated
	stuck := f.stuckUploads
	foreign := f.uploadsForeign
	f.mu.Unlock()
	if foreign {
		// An upload outside the storage's own prefix: the listing answer that
		// carries it must not hand it to the caller.
		uploads = append(uploads, item{key: "elsewhere/foreign.bin", id: "foreign-upload", initiated: time.Now().UTC()})
	}
	w.Header().Set("Content-Type", "application/xml")
	_, _ = io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?><ListMultipartUploadsResult>`)
	_, _ = fmt.Fprintf(w, "<Bucket>%s</Bucket><Prefix>%s</Prefix>", f.bucket, prefix)
	truncated := end < len(all)
	nextKey, nextID := "", ""
	if truncated {
		nextKey, nextID = uploads[len(uploads)-1].key, uploads[len(uploads)-1].id
	}
	if stuck {
		truncated, nextKey, nextID = true, keyMarker, uploadMarker
	}
	if truncated {
		_, _ = fmt.Fprintf(w, "<IsTruncated>true</IsTruncated><NextKeyMarker>%s</NextKeyMarker><NextUploadIdMarker>%s</NextUploadIdMarker>",
			nextKey, nextID)
	} else {
		_, _ = io.WriteString(w, "<IsTruncated>false</IsTruncated>")
	}
	for _, it := range uploads {
		_, _ = fmt.Fprintf(w, "<Upload><Key>%s</Key><UploadId>%s</UploadId>", it.key, it.id)
		if !dropInitiated[it.id] {
			_, _ = fmt.Fprintf(w, "<Initiated>%s</Initiated>", it.initiated.Format(time.RFC3339))
		}
		_, _ = io.WriteString(w, "</Upload>")
	}
	_, _ = io.WriteString(w, "</ListMultipartUploadsResult>")
}

// backdateUpload moves the initiation time of one open upload into the past, so a
// test can exercise a cutoff without waiting for one.
func (f *fakeS3) backdateUpload(id string, t time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if u, ok := f.uploads[id]; ok {
		u.initiated = t
	}
}

// seedUpload opens an upload directly, bypassing the API, for a key the plugin
// itself could never name — an object of another deployment sharing the bucket.
func (f *fakeS3) seedUpload(key string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextID++
	id := fmt.Sprintf("upload-%d", f.nextID)
	f.uploads[id] = &fakeUpload{key: key, parts: map[int32][]byte{}, etags: map[int32]string{}, initiated: time.Now().UTC()}
	return id
}

// firstSegment is the first path segment, bucket included and empty for "/".
func firstSegment(p string) string {
	p = strings.TrimPrefix(p, "/")
	if i := strings.IndexByte(p, '/'); i >= 0 {
		return p[:i]
	}
	return p
}

func quote(s string) string { return `"` + s + `"` }

func md5Hex(b []byte) string {
	sum := md5.Sum(b)
	return hex.EncodeToString(sum[:])
}
