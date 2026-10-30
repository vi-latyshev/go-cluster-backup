// Package kubeapitest is the fake Kubernetes API server the kubeapi client and
// the Kubernetes plugins are tested against: an httptest TLS server that answers
// the routes they use — pod, job, PVC, PV and event listings, pod and job reads,
// Secrets, and ConfigMap reads, puts and patches — with the JSON shapes the real
// API server returns — label selectors, phase field selectors, continue-token
// pagination, base64 Secret data, and a ConfigMap update that can conflict on
// resourceVersion.
package kubeapitest

import (
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"encoding/json/jsontext"
	json "encoding/json/v2"

	"github.com/vi-latyshev/go-cluster-backup/internal/kubeapi"
	"github.com/vi-latyshev/go-cluster-backup/internal/must"
)

// item is one listable object: its raw JSON plus the fields the fake filters
// and pages by.
type item struct {
	raw       jsontext.Value
	namespace string
	name      string
	labels    map[string]string
	phase     string
	// involvedUID and involvedKind are the Event fields a field selector filters
	// on (involvedObject.uid / involvedObject.kind); empty on every other object.
	involvedUID  string
	involvedKind string
}

type configMap struct {
	namespace  string
	name       string
	version    int
	data       map[string]string
	binaryData map[string]string
	// annotations and labels are what a merge patch must leave alone: a writer
	// that replaces the object wipes them, and a reconciler's own annotations
	// live here.
	annotations map[string]string
	labels      map[string]string
}

// nthFailure is one armed FailNth key: its code, the request number to fail and
// how many requests of the key were seen so far.
type nthFailure struct {
	code int
	nth  int
	seen int
}

// Server is a fake API server. Its fields may be set between requests; every
// handler takes the same lock, so a test may also drive it from one goroutine
// while the client is called from another.
type Server struct {
	t   *testing.T
	srv *httptest.Server

	mu      sync.Mutex
	pods    []item
	pvcs    []item
	pvs     []item
	jobs    []item
	events  []item
	secrets map[string]map[string]string
	configs map[string]*configMap

	// PageSize caps one list page; 0 serves the limit the client asked for.
	PageSize int
	// StuckContinue makes the list handler repeat the same continue token, the
	// failure mode a client must refuse to loop on.
	StuckContinue bool
	// ConflictNextPut makes the next ConfigMap update answer 409 without
	// applying, the way a writer that lost a version race sees it. The flag
	// clears itself.
	ConflictNextPut bool
	// Delay holds every response, for exercising a client timeout.
	Delay time.Duration
	// denied answers 403 the way RBAC refuses an object outside resourceNames.
	// Keys are a list resource ("pods", "persistentvolumeclaims",
	// "persistentvolumes"), "secrets", "configmaps",
	// "secrets/<namespace>/<name>" or "configmaps/<namespace>/<name>", most
	// specific first.
	denied map[string]bool
	// failures forces a status code for the same keys as denied.
	failures map[string]int
	// failNth forces a status code for the Nth request of a key (1-based) and
	// only that one — a blip aimed at a chosen call, e.g. past a preflight.
	failNth map[string]*nthFailure
	// calls counts the requests per key, the most specific key of each
	// request — how a test proves how many passes a caller made.
	calls map[string]int
	// Hold delays the answer of one key by the given duration: the request is
	// counted first and then held, so a test can land a cancellation inside
	// the read itself. The key is the request's most specific key — the one
	// Calls counts.
	Hold map[string]time.Duration
	// Tokens records the bearer token of every request, oldest first, so a test
	// can prove the client re-reads the token file.
	Tokens []string
}

// New starts the fake server and closes it when the test ends.
func New(t *testing.T) *Server {
	t.Helper()
	s := &Server{
		t:        t,
		secrets:  map[string]map[string]string{},
		configs:  map[string]*configMap{},
		denied:   map[string]bool{},
		failures: map[string]int{},
		failNth:  map[string]*nthFailure{},
		calls:    map[string]int{},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/pods", func(w http.ResponseWriter, r *http.Request) { s.handleList(w, r, "pods", "") })
	mux.HandleFunc("GET /api/v1/namespaces/{namespace}/pods", func(w http.ResponseWriter, r *http.Request) {
		s.handleList(w, r, "pods", r.PathValue("namespace"))
	})
	mux.HandleFunc("GET /api/v1/persistentvolumeclaims", func(w http.ResponseWriter, r *http.Request) { s.handleList(w, r, "persistentvolumeclaims", "") })
	mux.HandleFunc("GET /api/v1/persistentvolumes", func(w http.ResponseWriter, r *http.Request) { s.handleList(w, r, "persistentvolumes", "") })
	mux.HandleFunc("GET /apis/batch/v1/namespaces/{namespace}/jobs", func(w http.ResponseWriter, r *http.Request) {
		s.handleList(w, r, "jobs", r.PathValue("namespace"))
	})
	mux.HandleFunc("GET /apis/batch/v1/namespaces/{namespace}/jobs/{name}", s.handleJobGet)
	mux.HandleFunc("GET /api/v1/namespaces/{namespace}/events", func(w http.ResponseWriter, r *http.Request) {
		s.handleList(w, r, "events", r.PathValue("namespace"))
	})
	mux.HandleFunc("GET /api/v1/namespaces/{namespace}/secrets/{name}", s.handleSecret)
	mux.HandleFunc("GET /api/v1/namespaces/{namespace}/pods/{name}", s.handlePodGet)
	mux.HandleFunc("GET /api/v1/namespaces/{namespace}/configmaps/{name}", s.handleConfigMapGet)
	mux.HandleFunc("PUT /api/v1/namespaces/{namespace}/configmaps/{name}", s.handleConfigMapPut)
	mux.HandleFunc("PATCH /api/v1/namespaces/{namespace}/configmaps/{name}", s.handleConfigMapPatch)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeStatus(w, http.StatusNotFound, "NotFound", fmt.Sprintf("the server could not find the requested resource %s", r.URL.Path))
	})
	s.srv = httptest.NewTLSServer(s.delayed(mux))
	t.Cleanup(s.srv.Close)
	return s
}

// delayed holds every request for Delay before answering, for exercising a
// client timeout. A client that gives up first releases the handler: the hold
// must never outlive the request it is holding.
func (s *Server) delayed(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		delay := s.Delay
		s.mu.Unlock()
		if delay > 0 {
			select {
			case <-time.After(delay):
			case <-r.Context().Done():
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

// URL is the base URL of the fake server.
func (s *Server) URL() string { return s.srv.URL }

// writeFile is os.WriteFile behind a seam. The directory below is one
// t.TempDir just made writable, so the write cannot fail for a reason the test
// controls — but the filesystem still can (a full or read-only mount), and that
// is a real error the test reports. A re-executed child overrides this to prove
// the report fires.
var writeFile = os.WriteFile

// Connection writes the server's CA certificate and a token into a temp
// directory, points the two named environment variables at this server, and
// returns the client options that reach it.
func (s *Server) Connection(t *testing.T, hostEnv, portEnv string) kubeapi.Options {
	t.Helper()
	// httptest produced this address, so parsing it cannot fail.
	u := must.Do(url.Parse(s.srv.URL))
	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.crt")
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: s.srv.Certificate().Raw})
	if err := writeFile(caPath, ca, 0o600); err != nil {
		t.Fatalf("kubeapitest: write the CA file: %v", err)
	}
	tokenPath := filepath.Join(dir, "token")
	if err := writeFile(tokenPath, []byte("token-1"), 0o600); err != nil {
		t.Fatalf("kubeapitest: write the token file: %v", err)
	}
	t.Setenv(hostEnv, u.Hostname())
	t.Setenv(portEnv, u.Port())
	return kubeapi.Options{HostEnv: hostEnv, PortEnv: portEnv, TokenFile: tokenPath, CAFile: caPath, Timeout: 10 * time.Second}
}

// SetToken rewrites the token file a Connection returned options for.
func SetToken(t *testing.T, opts kubeapi.Options, value string) {
	t.Helper()
	if err := os.WriteFile(opts.TokenFile, []byte(value), 0o600); err != nil {
		t.Fatalf("kubeapitest: rewrite token file: %v", err)
	}
}

// handlePodGet serves one pod by namespace and name, the read a leaser's
// liveness check makes.
func (s *Server) handlePodGet(w http.ResponseWriter, r *http.Request) {
	namespace, name := r.PathValue("namespace"), r.PathValue("name")
	if s.refuse(r, w, "pods/"+namespace+"/"+name, "pods") {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, it := range s.pods {
		if it.namespace == namespace && it.name == name {
			writeJSON(w, http.StatusOK, it.raw)
			return
		}
	}
	writeStatus(w, http.StatusNotFound, "NotFound", fmt.Sprintf("pods %q not found", name))
}

// AddPod adds one pod object as raw JSON.
func (s *Server) AddPod(raw string) { s.add(&s.pods, raw) }

// AddPVC adds one persistent volume claim object as raw JSON.
func (s *Server) AddPVC(raw string) { s.add(&s.pvcs, raw) }

// AddPV adds one persistent volume object as raw JSON. A PV is cluster-scoped:
// its fixture carries no namespace.
func (s *Server) AddPV(raw string) { s.add(&s.pvs, raw) }

// AddJob adds one batch/v1 Job object as raw JSON.
func (s *Server) AddJob(raw string) { s.add(&s.jobs, raw) }

// AddEvent adds one core/v1 Event object as raw JSON.
func (s *Server) AddEvent(raw string) { s.add(&s.events, raw) }

// handleJobGet serves one Job by namespace and name.
func (s *Server) handleJobGet(w http.ResponseWriter, r *http.Request) {
	namespace, name := r.PathValue("namespace"), r.PathValue("name")
	if s.refuse(r, w, "jobs/"+namespace+"/"+name, "jobs") {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, it := range s.jobs {
		if it.namespace == namespace && it.name == name {
			writeJSON(w, http.StatusOK, it.raw)
			return
		}
	}
	writeStatus(w, http.StatusNotFound, "NotFound", fmt.Sprintf("jobs %q not found", name))
}

func (s *Server) add(list *[]item, raw string) {
	var meta struct {
		Metadata struct {
			Name      string            `json:"name"`
			Namespace string            `json:"namespace"`
			Labels    map[string]string `json:"labels"`
		} `json:"metadata"`
		Status struct {
			Phase string `json:"phase"`
		} `json:"status"`
		InvolvedObject struct {
			Kind string `json:"kind"`
			UID  string `json:"uid"`
		} `json:"involvedObject"`
	}
	if err := json.Unmarshal([]byte(raw), &meta); err != nil {
		s.t.Fatalf("kubeapitest: the fixture is not a JSON object: %v", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	*list = append(*list, item{
		raw:          jsontext.Value(raw),
		namespace:    meta.Metadata.Namespace,
		name:         meta.Metadata.Name,
		labels:       meta.Metadata.Labels,
		phase:        meta.Status.Phase,
		involvedUID:  meta.InvolvedObject.UID,
		involvedKind: meta.InvolvedObject.Kind,
	})
}

// Deny answers one object or one whole resource with 403, the way RBAC refuses
// a Secret outside resourceNames. Keys are "pods", "secrets", "configmaps",
// "secrets/<namespace>/<name>" or "configmaps/<namespace>/<name>".
func (s *Server) Deny(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.denied[key] = true
}

// Allow undoes Deny.
func (s *Server) Allow(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.denied, key)
}

// Fail forces a status code for the same keys as Deny; Fail(key, 0) clears it.
func (s *Server) Fail(key string, code int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if code == 0 {
		delete(s.failures, key)
		return
	}
	s.failures[key] = code
}

// AddSecret adds one Secret; values are plaintext here and base64-encoded on
// the wire, the way the API server stores them.
func (s *Server) AddSecret(namespace, name string, data map[string]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.secrets[namespace+"/"+name] = data
}

// AddConfigMap adds one ConfigMap at resourceVersion 1.
func (s *Server) AddConfigMap(namespace, name string, data map[string]string) {
	s.AddConfigMapBinary(namespace, name, data, nil)
}

// AddConfigMapMeta adds one ConfigMap carrying annotations and labels, so a test
// can prove a write left them alone.
func (s *Server) AddConfigMapMeta(namespace, name string, annotations, labels, data map[string]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.configs[namespace+"/"+name] = &configMap{namespace: namespace, name: name, version: 1,
		data: data, annotations: annotations, labels: labels}
}

// ConfigMapAnnotations returns a copy of one ConfigMap's annotations.
func (s *Server) ConfigMapAnnotations(namespace, name string) map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	cm := s.configs[namespace+"/"+name]
	if cm == nil {
		return nil
	}
	out := make(map[string]string, len(cm.annotations))
	for k, v := range cm.annotations {
		out[k] = v
	}
	return out
}

// AddConfigMapBinary adds one ConfigMap that also holds base64 binaryData keys.
func (s *Server) AddConfigMapBinary(namespace, name string, data, binaryData map[string]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.configs[namespace+"/"+name] = &configMap{namespace: namespace, name: name, version: 1, data: data, binaryData: binaryData}
}

// ConfigMapData returns a copy of the data currently stored in one ConfigMap.
func (s *Server) ConfigMapData(namespace, name string) map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	cm := s.configs[namespace+"/"+name]
	if cm == nil {
		return nil
	}
	out := make(map[string]string, len(cm.data))
	for k, v := range cm.data {
		out[k] = v
	}
	return out
}

// BumpConfigMap advances a ConfigMap's resourceVersion without touching its
// data, the way a concurrent writer would: the next update from a client that
// read the older version conflicts.
func (s *Server) BumpConfigMap(namespace, name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cm := s.configs[namespace+"/"+name]; cm != nil {
		cm.version++
	}
}

// FailNth forces a status code for the nth request since arming among every
// request whose path carries the key (1-based) — a resource key counts its
// objects' reads and its listings alike, unlike Calls — and only that one. A
// request that a more specific key or a higher-priority fault answers still
// takes its ordinal: the shot is spent there, never shifted to the next request.
// Arming again replaces the rule and resets the count. Keys are the same as
// Deny's.
func (s *Server) FailNth(key string, code, nth int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failNth[key] = &nthFailure{code: code, nth: nth}
}

// Calls counts the requests of one key so far, the most specific key of each
// request — Deny's keys, a list resource ("pods", "persistentvolumeclaims",
// "persistentvolumes") for a listing.
func (s *Server) Calls(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls[key]
}

// refuse counts one request of the path keys, advances every armed FailNth on
// that chain, then answers what forced selected — the accounting, the choice and
// the hold are three separate steps. A held request is released by the client's
// own cancellation as well as by the hold's end: cancellation skips only the
// response, never the accounting.
func (s *Server) refuse(r *http.Request, w http.ResponseWriter, keys ...string) bool {
	s.mu.Lock()
	// A caller always passes the request's most specific key first — it is the
	// one Calls counts and Hold matches.
	s.calls[keys[0]]++
	hold := s.Hold[keys[0]]
	// Every armed ordinal on the chain advances exactly once, whichever rule
	// answers: a shot masked by a specific key or a higher-priority fault is
	// spent where the request landed, never shifted to the next one.
	for _, key := range keys {
		if f, ok := s.failNth[key]; ok {
			f.seen++
		}
	}
	code, reason, message := s.forced(keys)
	s.mu.Unlock()

	if hold > 0 {
		select {
		case <-time.After(hold):
			// Counted, held, then answered: the caller is blocked in its read
			// for the whole hold.
		case <-r.Context().Done():
			// The client is gone; the request still counts and its ordinals
			// have already advanced.
			return true
		}
	}
	if code == 0 {
		return false
	}
	writeStatus(w, code, reason, message)
	return true
}

// forced is the answer to one request, chosen from the rule state its own
// arrival produced: most specific key first, and at one key a firing FailNth,
// then Fail, then Deny. It mutates nothing — the ordinals advanced before it
// ran — and answers 0 when no rule fires.
func (s *Server) forced(keys []string) (int, string, string) {
	for _, key := range keys {
		if f, ok := s.failNth[key]; ok && f.seen == f.nth {
			return f.code, "Forced", fmt.Sprintf("the fake answers %d for request %d of %s", f.code, f.nth, key)
		}
		if code, ok := s.failures[key]; ok {
			return code, "Forced", fmt.Sprintf("the fake answers %d for %s", code, key)
		}
		if s.denied[key] {
			return http.StatusForbidden, "Forbidden",
				fmt.Sprintf("secrets or configmaps %q is forbidden: User cannot get resource - add it to resourceNames", key)
		}
	}
	return 0, "", ""
}

func (s *Server) record(r *http.Request) {
	auth := r.Header.Get("Authorization")
	s.mu.Lock()
	defer s.mu.Unlock()
	if after, ok := strings.CutPrefix(auth, "Bearer "); ok {
		s.Tokens = append(s.Tokens, after)
	}
}

func (s *Server) handleList(w http.ResponseWriter, r *http.Request, resource, namespace string) {
	s.record(r)
	if s.refuse(r, w, resource) {
		return
	}
	q := r.URL.Query()
	fieldSelector := q.Get("fieldSelector")
	if bad := unsupportedFieldSelector(resource, fieldSelector); bad != "" {
		// The way the real API server refuses one: HTTP 400 naming the field.
		writeStatus(w, http.StatusBadRequest, "BadRequest", fmt.Sprintf(
			"Unable to find \"/v1, Resource=%s\" that match label selector %q, field selector %q: %q is not a known field selector: only %s",
			resource, q.Get("labelSelector"), fieldSelector, bad, knownFieldSelectors(resource)))
		return
	}
	var items []item
	s.mu.Lock()
	switch resource {
	case "pods":
		items = append(items, s.pods...)
	case "persistentvolumeclaims":
		items = append(items, s.pvcs...)
	case "persistentvolumes":
		items = append(items, s.pvs...)
	case "jobs":
		items = append(items, s.jobs...)
	case "events":
		items = append(items, s.events...)
	}
	pageSize := s.PageSize
	stuck := s.StuckContinue
	s.mu.Unlock()

	selector := q.Get("labelSelector")
	var matched []item
	for _, it := range items {
		if namespace != "" && it.namespace != namespace {
			continue
		}
		if !matchesSelector(selector, it.labels) || !matchesFieldSelector(fieldSelector, it) {
			continue
		}
		matched = append(matched, it)
	}

	limit := len(matched)
	if asked, err := strconv.Atoi(q.Get("limit")); err == nil && asked > 0 && asked < limit {
		limit = asked
	}
	if pageSize > 0 && pageSize < limit {
		limit = pageSize
	}
	start := 0
	if token := q.Get("continue"); token != "" {
		if parsed, err := strconv.Atoi(token); err == nil {
			start = parsed
		}
	}
	if start > len(matched) {
		start = len(matched)
	}
	end := min(start+limit, len(matched))

	body := struct {
		Metadata struct {
			Continue string `json:"continue,omitempty"`
		} `json:"metadata"`
		Items []jsontext.Value `json:"items"`
	}{}
	body.Items = []jsontext.Value{}
	for _, it := range matched[start:end] {
		body.Items = append(body.Items, it.raw)
	}
	switch {
	case stuck && end < len(matched):
		body.Metadata.Continue = strconv.Itoa(start)
	case end < len(matched):
		body.Metadata.Continue = strconv.Itoa(end)
	}
	writeJSON(w, http.StatusOK, body)
}

// acceptedFieldSelectors names, per resource, the field selectors the real API
// server accepts for it: Kubernetes registers status.phase for pods only, and a
// PVC takes metadata.name and metadata.namespace. A selector outside this set
// is refused with the server's own 400, so a client that starts sending a
// selector the server does not know fails a test instead of the 04:20 run.
var acceptedFieldSelectors = map[string][]string{
	"pods":                   {"metadata.name", "metadata.namespace", "status.phase"},
	"persistentvolumeclaims": {"metadata.name", "metadata.namespace"},
	"persistentvolumes":      {"metadata.name"},
	"jobs":                   {},
	"events":                 {"metadata.name", "metadata.namespace", "involvedObject.uid", "involvedObject.kind"},
}

// unsupportedFieldSelector returns the offending key of a selector the
// resource's server-side conversion does not know, "" when every term is
// accepted. Terms use "=", which is what the clients send.
func unsupportedFieldSelector(resource, selector string) string {
	if selector == "" {
		return ""
	}
	for _, term := range strings.Split(selector, ",") {
		term = strings.TrimSpace(term)
		if term == "" {
			continue
		}
		key, _, ok := strings.Cut(term, "=")
		if !ok {
			return term
		}
		if !slices.Contains(acceptedFieldSelectors[resource], strings.TrimSpace(key)) {
			return strings.TrimSpace(key)
		}
	}
	return ""
}

func knownFieldSelectors(resource string) string {
	quoted := make([]string, 0, len(acceptedFieldSelectors[resource]))
	for _, key := range acceptedFieldSelectors[resource] {
		quoted = append(quoted, strconv.Quote(key))
	}
	return strings.Join(quoted, ", ")
}

// matchesFieldSelector applies the field-selector keys the fake implements:
// status.phase for pods, metadata.name and metadata.namespace for both. Every
// other key was refused before this point.
func matchesFieldSelector(selector string, it item) bool {
	for _, term := range strings.Split(selector, ",") {
		term = strings.TrimSpace(term)
		if term == "" {
			continue
		}
		key, value, ok := strings.Cut(term, "=")
		if !ok {
			continue
		}
		switch strings.TrimSpace(key) {
		case "status.phase":
			if it.phase != strings.TrimSpace(value) {
				return false
			}
		case "metadata.name":
			if it.name != strings.TrimSpace(value) {
				return false
			}
		case "metadata.namespace":
			if it.namespace != strings.TrimSpace(value) {
				return false
			}
		case "involvedObject.uid":
			if it.involvedUID != strings.TrimSpace(value) {
				return false
			}
		case "involvedObject.kind":
			if it.involvedKind != strings.TrimSpace(value) {
				return false
			}
		}
	}
	return true
}

// matchesSelector understands what the plugins' selectors use: "k=v", "k!=v"
// and a bare "k" meaning existence.
func matchesSelector(selector string, labels map[string]string) bool {
	if selector == "" {
		return true
	}
	for _, term := range strings.Split(selector, ",") {
		term = strings.TrimSpace(term)
		if term == "" {
			continue
		}
		if k, v, ok := strings.Cut(term, "!="); ok {
			if labels[k] == v {
				return false
			}
			continue
		}
		if k, v, ok := strings.Cut(term, "="); ok {
			if labels[k] != v {
				return false
			}
			continue
		}
		if _, ok := labels[term]; !ok {
			return false
		}
	}
	return true
}

func (s *Server) handleSecret(w http.ResponseWriter, r *http.Request) {
	s.record(r)
	namespace, name := r.PathValue("namespace"), r.PathValue("name")
	if s.refuse(r, w, "secrets/"+namespace+"/"+name, "secrets") {
		return
	}
	s.mu.Lock()
	data, ok := s.secrets[namespace+"/"+name]
	s.mu.Unlock()
	if !ok {
		writeStatus(w, http.StatusNotFound, "NotFound", fmt.Sprintf("secrets %q not found", name))
		return
	}
	encoded := make(map[string]string, len(data))
	for k, v := range data {
		encoded[k] = base64.StdEncoding.EncodeToString([]byte(v))
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"kind":       "Secret",
		"apiVersion": "v1",
		"metadata":   map[string]any{"name": name, "namespace": namespace, "resourceVersion": "1"},
		"data":       encoded,
	})
}

func (s *Server) handleConfigMapGet(w http.ResponseWriter, r *http.Request) {
	s.record(r)
	namespace, name := r.PathValue("namespace"), r.PathValue("name")
	if s.refuse(r, w, "configmaps/"+namespace+"/"+name, "configmaps") {
		return
	}
	s.mu.Lock()
	cm := s.configs[namespace+"/"+name]
	s.mu.Unlock()
	if cm == nil {
		writeStatus(w, http.StatusNotFound, "NotFound", fmt.Sprintf("configmaps %q not found", name))
		return
	}
	writeJSON(w, http.StatusOK, configMapJSON(namespace, name, cm.version, cm.data, cm.binaryData))
}

// handleConfigMapPatch applies a JSON merge patch to one ConfigMap the way the
// API server does: only the members the patch carries are written, everything
// else — annotations, labels, binaryData — is left as it is, and a
// resourceVersion in the patch is a precondition.
func (s *Server) handleConfigMapPatch(w http.ResponseWriter, r *http.Request) {
	s.record(r)
	namespace, name := r.PathValue("namespace"), r.PathValue("name")
	if s.refuse(r, w, "configmaps/"+namespace+"/"+name, "configmaps") {
		return
	}
	if ct := r.Header.Get("Content-Type"); ct != "application/merge-patch+json" {
		writeStatus(w, http.StatusUnsupportedMediaType, "UnsupportedMediaType", "content type "+ct)
		return
	}
	var body struct {
		Metadata struct {
			ResourceVersion string            `json:"resourceVersion"`
			Annotations     map[string]string `json:"annotations"`
			Labels          map[string]string `json:"labels"`
		} `json:"metadata"`
		Data map[string]string `json:"data"`
	}
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		writeStatus(w, http.StatusBadRequest, "BadRequest", err.Error())
		return
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		writeStatus(w, http.StatusBadRequest, "BadRequest", err.Error())
		return
	}
	s.mu.Lock()
	conflict := s.ConflictNextPut
	s.ConflictNextPut = false
	s.mu.Unlock()
	if conflict {
		writeStatus(w, http.StatusConflict, "Conflict",
			"Operation cannot be fulfilled on configmaps: the object has been modified; please apply your changes to the latest version and try again")
		return
	}
	s.mu.Lock()
	cm := s.configs[namespace+"/"+name]
	if cm == nil {
		s.mu.Unlock()
		writeStatus(w, http.StatusNotFound, "NotFound", fmt.Sprintf("configmaps %q not found", name))
		return
	}
	if body.Metadata.ResourceVersion != strconv.Itoa(cm.version) {
		s.mu.Unlock()
		writeStatus(w, http.StatusConflict, "Conflict",
			"Operation cannot be fulfilled on configmaps: the object has been modified; please apply its changes to the latest version and try again")
		return
	}
	// Merge, not replace: a key the patch does not name keeps its stored value.
	for k, v := range body.Data {
		if cm.data == nil {
			cm.data = map[string]string{}
		}
		cm.data[k] = v
	}
	cm.version++
	version := cm.version
	bodyData := cm.data
	annotations, labels := cm.annotations, cm.labels
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, configMapJSONMeta(namespace, name, version, bodyData, nil, annotations, labels))
}

func (s *Server) handleConfigMapPut(w http.ResponseWriter, r *http.Request) {
	s.record(r)
	namespace, name := r.PathValue("namespace"), r.PathValue("name")
	if s.refuse(r, w, "configmaps/"+namespace+"/"+name, "configmaps") {
		return
	}
	var body struct {
		Metadata struct {
			ResourceVersion string            `json:"resourceVersion"`
			Annotations     map[string]string `json:"annotations"`
			Labels          map[string]string `json:"labels"`
		} `json:"metadata"`
		Data map[string]string `json:"data"`
	}
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		writeStatus(w, http.StatusBadRequest, "BadRequest", err.Error())
		return
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		writeStatus(w, http.StatusBadRequest, "BadRequest", err.Error())
		return
	}
	s.mu.Lock()
	conflict := s.ConflictNextPut
	s.ConflictNextPut = false
	s.mu.Unlock()
	if conflict {
		writeStatus(w, http.StatusConflict, "Conflict",
			"Operation cannot be fulfilled on configmaps: the object has been modified; please apply your changes to the latest version and try again")
		return
	}
	s.mu.Lock()
	cm := s.configs[namespace+"/"+name]
	var version int
	if cm != nil {
		if body.Metadata.ResourceVersion != strconv.Itoa(cm.version) {
			s.mu.Unlock()
			writeStatus(w, http.StatusConflict, "Conflict",
				"Operation cannot be fulfilled on configmaps: the object has been modified; please apply your changes to the latest version and try again")
			return
		}
		// A PUT REPLACES the object: metadata the body does not carry is gone.
		// That is exactly why the client writes a merge patch instead, and a
		// fake that preserved annotations here would hide the difference.
		cm.data = body.Data
		cm.annotations = body.Metadata.Annotations
		cm.labels = body.Metadata.Labels
		cm.version++
		version = cm.version
	}
	s.mu.Unlock()
	if cm == nil {
		writeStatus(w, http.StatusNotFound, "NotFound", fmt.Sprintf("configmaps %q not found", name))
		return
	}
	writeJSON(w, http.StatusOK, configMapJSON(namespace, name, version, body.Data, nil))
}

func configMapJSON(namespace, name string, version int, data, binaryData map[string]string) map[string]any {
	return configMapJSONMeta(namespace, name, version, data, binaryData, nil, nil)
}

// configMapJSONMeta is one ConfigMap as the API server answers it, annotations
// and labels included when it carries them.
func configMapJSONMeta(namespace, name string, version int, data, binaryData map[string]string, annotations, labels map[string]string) map[string]any {
	if data == nil {
		data = map[string]string{}
	}
	meta := map[string]any{
		"name":            name,
		"namespace":       namespace,
		"resourceVersion": strconv.Itoa(version),
	}
	if len(annotations) > 0 {
		meta["annotations"] = annotations
	}
	if len(labels) > 0 {
		meta["labels"] = labels
	}
	body := map[string]any{
		"kind":       "ConfigMap",
		"apiVersion": "v1",
		"metadata":   meta,
		"data":       data,
	}
	if len(binaryData) > 0 {
		encoded := make(map[string]string, len(binaryData))
		for k, v := range binaryData {
			encoded[k] = base64.StdEncoding.EncodeToString([]byte(v))
		}
		body["binaryData"] = encoded
	}
	return body
}

func writeJSON(w http.ResponseWriter, code int, body any) {
	raw, err := json.Marshal(body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, _ = w.Write(raw)
}

func writeStatus(w http.ResponseWriter, code int, reason, message string) {
	writeJSON(w, code, map[string]any{
		"kind":       "Status",
		"apiVersion": "v1",
		"status":     "Failure",
		"reason":     reason,
		"message":    message,
		"code":       code,
	})
}
