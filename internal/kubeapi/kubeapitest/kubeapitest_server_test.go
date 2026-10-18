package kubeapitest

import (
	"bufio"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/kubeapi"
)

// childEnv names the branch a re-executed child of this test binary runs; an
// unset value means the parent.
const childEnv = "KUBEAPITEST_CHILD"

// runChild re-executes this test binary, limited to the calling test, with the
// child branch named in the environment — the way a t.Fatalf path is proven to
// fire without failing the parent run.
func runChild(t *testing.T, branch string) ([]byte, error) {
	t.Helper()
	args := []string{"-test.run=^" + t.Name() + "$", "-test.count=1"}
	// The child is this same coverage-instrumented binary; without the directory
	// go test handed this run, the child's own statements would be measured
	// nowhere.
	if dir := os.Getenv("GOCOVERDIR"); dir != "" {
		args = append(args, "-test.gocoverdir="+dir)
	}
	cmd := exec.Command(os.Args[0], args...)
	cmd.Env = append(os.Environ(), childEnv+"="+branch)
	return cmd.CombinedOutput()
}

// request performs one request against the fake and returns the status and body.
func request(t *testing.T, s *Server, method, path, contentType, body string) (int, []byte) {
	t.Helper()
	var r io.Reader
	if body != "" {
		r = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, s.URL()+path, r)
	if err != nil {
		t.Fatal(err)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := s.srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, raw
}

// countItems returns how many items a list body carries.
func countItems(t *testing.T, body []byte) int {
	t.Helper()
	var page struct {
		Items []struct {
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
		} `json:"items"`
	}
	if err := json.Unmarshal(body, &page); err != nil {
		t.Fatalf("list body is not a list: %v: %s", err, body)
	}
	return len(page.Items)
}

// TestAddAndListEveryResource drives the namespaced pod route, the cluster-scoped
// volume route, and the job and event list routes, proving each Add helper and
// each list branch answers.
func TestAddAndListEveryResource(t *testing.T) {
	s := New(t)
	s.AddPod(`{"metadata":{"name":"p1","namespace":"ns"},"status":{"phase":"Running"}}`)
	s.AddPV(`{"metadata":{"name":"pv1"}}`)
	s.AddJob(`{"metadata":{"name":"j1","namespace":"ns"}}`)
	s.AddEvent(`{"metadata":{"name":"e1","namespace":"ns"}}`)

	for path, want := range map[string]int{
		"/api/v1/namespaces/ns/pods":        1,
		"/api/v1/persistentvolumes":         1,
		"/apis/batch/v1/namespaces/ns/jobs": 1,
		"/api/v1/namespaces/ns/events":      1,
	} {
		code, body := request(t, s, http.MethodGet, path, "", "")
		if code != http.StatusOK {
			t.Fatalf("GET %s = %d: %s", path, code, body)
		}
		if got := countItems(t, body); got != want {
			t.Fatalf("GET %s returned %d items, want %d", path, got, want)
		}
	}
}

// TestGetPodAndJobRoutes: a pod and a Job read back by name, and an absent one
// answers 404.
func TestGetPodAndJobRoutes(t *testing.T) {
	s := New(t)
	s.AddPod(`{"metadata":{"name":"p1","namespace":"ns"},"status":{"phase":"Running"}}`)
	s.AddJob(`{"metadata":{"name":"j1","namespace":"ns"}}`)
	for _, tc := range []struct {
		path string
		want int
	}{
		{"/api/v1/namespaces/ns/pods/p1", http.StatusOK},
		{"/api/v1/namespaces/ns/pods/missing", http.StatusNotFound},
		{"/apis/batch/v1/namespaces/ns/jobs/j1", http.StatusOK},
		{"/apis/batch/v1/namespaces/ns/jobs/missing", http.StatusNotFound},
	} {
		if code, body := request(t, s, http.MethodGet, tc.path, "", ""); code != tc.want {
			t.Fatalf("GET %s = %d: %s, want %d", tc.path, code, body, tc.want)
		}
	}
	s.Deny("pods/ns/p1")
	if code, _ := request(t, s, http.MethodGet, "/api/v1/namespaces/ns/pods/p1", "", ""); code != http.StatusForbidden {
		t.Fatalf("denied pod = %d", code)
	}
	s.Deny("jobs/ns/j1")
	if code, _ := request(t, s, http.MethodGet, "/apis/batch/v1/namespaces/ns/jobs/j1", "", ""); code != http.StatusForbidden {
		t.Fatalf("denied job = %d", code)
	}
}

// TestListDeniedResourceIsForbidden: a denied list resource answers 403 before
// any filtering, the way RBAC refuses a whole resource.
func TestListDeniedResourceIsForbidden(t *testing.T) {
	s := New(t)
	s.AddPod(`{"metadata":{"name":"a","namespace":"ns"},"status":{"phase":"Running"}}`)
	s.Deny("pods")
	if code, _ := request(t, s, http.MethodGet, "/api/v1/pods", "", ""); code != http.StatusForbidden {
		t.Fatalf("denied list = %d", code)
	}
}

// TestListNamespacedSkipsForeignObjects: a namespaced listing drops objects of
// another namespace.
func TestListNamespacedSkipsForeignObjects(t *testing.T) {
	s := New(t)
	s.AddPod(`{"metadata":{"name":"a","namespace":"ns"},"status":{"phase":"Running"}}`)
	s.AddPod(`{"metadata":{"name":"b","namespace":"other"},"status":{"phase":"Running"}}`)
	_, body := request(t, s, http.MethodGet, "/api/v1/namespaces/ns/pods", "", "")
	if got := countItems(t, body); got != 1 {
		t.Fatalf("namespaced listing returned %d items", got)
	}
}

// TestListLimitQueryAndContinueBeyondEnd: the client's own limit query caps one
// page, and a continue index past the end returns an empty page rather than
// failing.
func TestListLimitQueryAndContinueBeyondEnd(t *testing.T) {
	s := New(t)
	s.AddPod(`{"metadata":{"name":"a","namespace":"ns"},"status":{"phase":"Running"}}`)
	s.AddPod(`{"metadata":{"name":"b","namespace":"ns"},"status":{"phase":"Running"}}`)

	var page struct {
		Metadata struct {
			Continue string `json:"continue"`
		} `json:"metadata"`
	}
	code, body := request(t, s, http.MethodGet, "/api/v1/pods?limit=1", "", "")
	if code != http.StatusOK {
		t.Fatalf("limit listing = %d: %s", code, body)
	}
	if err := json.Unmarshal(body, &page); err != nil {
		t.Fatal(err)
	}
	if got := countItems(t, body); got != 1 || page.Metadata.Continue != "1" {
		t.Fatalf("limited page = %d items, continue %q", got, page.Metadata.Continue)
	}
	_, body = request(t, s, http.MethodGet, "/api/v1/pods?continue=99", "", "")
	if got := countItems(t, body); got != 0 {
		t.Fatalf("continue past the end returned %d items", got)
	}
}

// TestFailZeroClearsTheForcedStatus: Fail(key, 0) removes a forced status.
func TestFailZeroClearsTheForcedStatus(t *testing.T) {
	s := New(t)
	s.AddSecret("ns", "pw", map[string]string{"PASSWORD": "x"})
	s.Fail("secrets", http.StatusInternalServerError)
	if code, _ := request(t, s, http.MethodGet, "/api/v1/namespaces/ns/secrets/pw", "", ""); code != http.StatusInternalServerError {
		t.Fatalf("forced status = %d", code)
	}
	s.Fail("secrets", 0)
	if code, _ := request(t, s, http.MethodGet, "/api/v1/namespaces/ns/secrets/pw", "", ""); code != http.StatusOK {
		t.Fatalf("status after clearing = %d", code)
	}
}

// TestAddConfigMapMetaAndAnnotations: a ConfigMap with annotations and labels
// reads its annotations back, and an absent one reads back nil.
func TestAddConfigMapMetaAndAnnotations(t *testing.T) {
	s := New(t)
	s.AddConfigMapMeta("ns", "cm", map[string]string{"ann": "1"}, map[string]string{"lab": "2"}, map[string]string{"k": "v"})
	if got := s.ConfigMapAnnotations("ns", "cm"); got["ann"] != "1" {
		t.Fatalf("annotations = %v", got)
	}
	if got := s.ConfigMapAnnotations("ns", "missing"); got != nil {
		t.Fatalf("annotations of an absent configmap = %v", got)
	}
}

// TestConfigMapDataMissingAndBump: an absent ConfigMap reads back nil data, and
// BumpConfigMap advances a stored version without touching the data.
func TestConfigMapDataMissingAndBump(t *testing.T) {
	s := New(t)
	if got := s.ConfigMapData("ns", "missing"); got != nil {
		t.Fatalf("data of an absent configmap = %v", got)
	}
	s.AddConfigMap("ns", "cm", map[string]string{"k": "[]"})
	s.BumpConfigMap("ns", "cm")
	s.BumpConfigMap("ns", "missing")
	code, body := request(t, s, http.MethodGet, "/api/v1/namespaces/ns/configmaps/cm", "", "")
	if code != http.StatusOK {
		t.Fatalf("get = %d: %s", code, body)
	}
	var cm struct {
		Metadata struct {
			ResourceVersion string `json:"resourceVersion"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal(body, &cm); err != nil {
		t.Fatal(err)
	}
	if cm.Metadata.ResourceVersion != "2" {
		t.Fatalf("resourceVersion = %q, want 2", cm.Metadata.ResourceVersion)
	}
}

// TestConfigMapGetNotFoundAndDenied: a missing ConfigMap is 404 and a denied one
// is 403.
func TestConfigMapGetNotFoundAndDenied(t *testing.T) {
	s := New(t)
	if code, _ := request(t, s, http.MethodGet, "/api/v1/namespaces/ns/configmaps/missing", "", ""); code != http.StatusNotFound {
		t.Fatalf("missing configmap = %d", code)
	}
	s.Deny("configmaps/ns/cm")
	if code, _ := request(t, s, http.MethodGet, "/api/v1/namespaces/ns/configmaps/cm", "", ""); code != http.StatusForbidden {
		t.Fatalf("denied configmap = %d", code)
	}
}

// TestConfigMapPatch: a merge patch writes only the data it names under a
// resourceVersion precondition, and each refusal path answers its own status.
func TestConfigMapPatch(t *testing.T) {
	s := New(t)
	s.AddConfigMapMeta("ns", "cm", map[string]string{"ann": "1"}, map[string]string{"lab": "2"}, map[string]string{"k": "[]"})
	path := "/api/v1/namespaces/ns/configmaps/cm"

	code, body := request(t, s, http.MethodPatch, path, "application/merge-patch+json", `{"metadata":{"resourceVersion":"1"},"data":{"k":"[1]"}}`)
	if code != http.StatusOK {
		t.Fatalf("patch = %d: %s", code, body)
	}
	if got := s.ConfigMapData("ns", "cm")["k"]; got != "[1]" {
		t.Fatalf("stored = %q", got)
	}
	if got := s.ConfigMapAnnotations("ns", "cm"); got["ann"] != "1" {
		t.Fatalf("annotations did not survive the patch: %v", got)
	}
	var patched struct {
		Metadata struct {
			Annotations map[string]string `json:"annotations"`
			Labels      map[string]string `json:"labels"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal(body, &patched); err != nil {
		t.Fatal(err)
	}
	if patched.Metadata.Annotations["ann"] != "1" || patched.Metadata.Labels["lab"] != "2" {
		t.Fatalf("patch response metadata = %+v", patched.Metadata)
	}

	s.Deny("configmaps/ns/cm")
	if code, _ := request(t, s, http.MethodPatch, path, "application/merge-patch+json", `{"data":{}}`); code != http.StatusForbidden {
		t.Fatalf("denied patch = %d", code)
	}
	s.Allow("configmaps/ns/cm")
	if code, _ := request(t, s, http.MethodPatch, path, "application/json", `{"data":{}}`); code != http.StatusUnsupportedMediaType {
		t.Fatalf("wrong content type = %d", code)
	}
	if code, _ := request(t, s, http.MethodPatch, path, "application/merge-patch+json", `{`); code != http.StatusBadRequest {
		t.Fatalf("malformed patch = %d", code)
	}
	if code, _ := request(t, s, http.MethodPatch, "/api/v1/namespaces/ns/configmaps/missing", "application/merge-patch+json", `{"metadata":{"resourceVersion":"1"},"data":{}}`); code != http.StatusNotFound {
		t.Fatalf("patch of a missing configmap = %d", code)
	}
	if code, _ := request(t, s, http.MethodPatch, path, "application/merge-patch+json", `{"metadata":{"resourceVersion":"999"},"data":{}}`); code != http.StatusConflict {
		t.Fatalf("stale patch = %d", code)
	}

	s.ConflictNextPut = true
	if code, _ := request(t, s, http.MethodPatch, path, "application/merge-patch+json", `{"metadata":{"resourceVersion":"2"},"data":{}}`); code != http.StatusConflict {
		t.Fatalf("ConflictNextPut patch = %d", code)
	}
	if code, _ := request(t, s, http.MethodPatch, path, "application/merge-patch+json", `{"metadata":{"resourceVersion":"2"},"data":{"k":"[2]"}}`); code != http.StatusOK {
		t.Fatalf("patch after the conflict = %d", code)
	}

	// A ConfigMap whose data map is nil gains one on the first patch.
	s.AddConfigMapBinary("ns", "nilcm", nil, nil)
	if code, _ := request(t, s, http.MethodPatch, "/api/v1/namespaces/ns/configmaps/nilcm", "application/merge-patch+json", `{"metadata":{"resourceVersion":"1"},"data":{"k":"v"}}`); code != http.StatusOK {
		t.Fatalf("patch of a nil-data configmap = %d", code)
	}
	if got := s.ConfigMapData("ns", "nilcm")["k"]; got != "v" {
		t.Fatalf("stored = %q", got)
	}
}

// TestConfigMapPutErrorsAndRefuse: a PUT answers 403 when denied, 400 for a
// malformed body, and 404 for an absent ConfigMap.
func TestConfigMapPutErrorsAndRefuse(t *testing.T) {
	s := New(t)
	path := "/api/v1/namespaces/ns/configmaps/cm"
	s.Deny("configmaps/ns/cm")
	if code, _ := request(t, s, http.MethodPut, path, "application/json", `{}`); code != http.StatusForbidden {
		t.Fatalf("denied put = %d", code)
	}
	s.Allow("configmaps/ns/cm")
	if code, _ := request(t, s, http.MethodPut, path, "application/json", `{`); code != http.StatusBadRequest {
		t.Fatalf("malformed put = %d", code)
	}
	if code, _ := request(t, s, http.MethodPut, "/api/v1/namespaces/ns/configmaps/missing", "application/json", `{"metadata":{"resourceVersion":"1"},"data":{}}`); code != http.StatusNotFound {
		t.Fatalf("put of a missing configmap = %d", code)
	}
}

// TestConfigMapJSONMetaShape: a stored ConfigMap answers with its binaryData
// base64-encoded, an absent data map as an empty object, and its annotations and
// labels when it carries them.
func TestConfigMapJSONMetaShape(t *testing.T) {
	s := New(t)
	s.AddConfigMapBinary("ns", "bin", map[string]string{"k": "v"}, map[string]string{"blob": "bytes"})
	_, body := request(t, s, http.MethodGet, "/api/v1/namespaces/ns/configmaps/bin", "", "")
	var cm struct {
		Data       map[string]string `json:"data"`
		BinaryData map[string]string `json:"binaryData"`
	}
	if err := json.Unmarshal(body, &cm); err != nil {
		t.Fatal(err)
	}
	if cm.Data["k"] != "v" || cm.BinaryData["blob"] != "Ynl0ZXM=" {
		t.Fatalf("configmap = %+v", cm)
	}

	s.AddConfigMap("ns", "empty", nil)
	_, body = request(t, s, http.MethodGet, "/api/v1/namespaces/ns/configmaps/empty", "", "")
	var meta struct {
		Data map[string]string `json:"data"`
	}
	if err := json.Unmarshal(body, &meta); err != nil {
		t.Fatal(err)
	}
	if meta.Data == nil || len(meta.Data) != 0 {
		t.Fatalf("data = %v, want an empty object", meta.Data)
	}
}

// TestWriteJSONReportsMarshalFailure: a body the JSON encoder cannot represent
// answers 500 instead of a truncated 200.
func TestWriteJSONReportsMarshalFailure(t *testing.T) {
	rec := httptest.NewRecorder()
	writeJSON(rec, http.StatusOK, make(chan int))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
}

// TestAddRejectsNonJSONFixture: a fixture that is not a JSON object is reported
// through the test, never stored as a broken item. The Fatalf runs in a
// re-executed child, so the parent proves the branch fires without failing.
func TestAddRejectsNonJSONFixture(t *testing.T) {
	if os.Getenv(childEnv) == "add" {
		New(t).AddPod("{not json")
		return
	}
	out, err := runChild(t, "add")
	if err == nil {
		t.Fatalf("the child accepted a non-JSON fixture: %s", out)
	}
	if !strings.Contains(string(out), "not a JSON object") {
		t.Fatalf("child output = %s", out)
	}
}

// TestSetTokenReportsWriteFailure: a token path that cannot be written is
// reported through the test, proven by a re-executed child.
func TestSetTokenReportsWriteFailure(t *testing.T) {
	if os.Getenv(childEnv) == "settoken" {
		SetToken(t, kubeapi.Options{TokenFile: filepath.Join(t.TempDir(), "missing-dir", "token")}, "x")
		return
	}
	out, err := runChild(t, "settoken")
	if err == nil {
		t.Fatalf("the child accepted an unwritable token path: %s", out)
	}
	if !strings.Contains(string(out), "rewrite token file") {
		t.Fatalf("child output = %s", out)
	}
}

// TestConnectionReportsCAWriteFailure: the CA file write is a real filesystem
// error, reported through the test. The directory is writable, so the failure
// is proven by overriding the write behind the package's seam in a re-executed
// child.
func TestConnectionReportsCAWriteFailure(t *testing.T) {
	if os.Getenv(childEnv) == "connection-ca" {
		writeFile = func(string, []byte, os.FileMode) error { return errors.New("injected CA write failure") }
		New(t).Connection(t, "HOST", "PORT")
		return
	}
	out, err := runChild(t, "connection-ca")
	if err == nil {
		t.Fatalf("the child accepted a failing CA write: %s", out)
	}
	if !strings.Contains(string(out), "write the CA file") {
		t.Fatalf("child output = %s", out)
	}
}

// TestConnectionReportsTokenWriteFailure: the token write fails after the CA
// write succeeded, so the two reports are proven independently.
func TestConnectionReportsTokenWriteFailure(t *testing.T) {
	if os.Getenv(childEnv) == "connection-token" {
		calls := 0
		writeFile = func(string, []byte, os.FileMode) error {
			calls++
			if calls == 2 {
				return errors.New("injected token write failure")
			}
			return nil
		}
		New(t).Connection(t, "HOST", "PORT")
		return
	}
	out, err := runChild(t, "connection-token")
	if err == nil {
		t.Fatalf("the child accepted a failing token write: %s", out)
	}
	if !strings.Contains(string(out), "write the token file") {
		t.Fatalf("child output = %s", out)
	}
}

// TestConfigMapBodyReadFailure: a request body the server cannot read in full is
// answered 400 by both ConfigMap writers, never decoded from a partial body. The
// body is declared 100 bytes long and then cut short, so the server's own read
// fails the way an aborted client looks to it.
func TestConfigMapBodyReadFailure(t *testing.T) {
	for _, method := range []string{http.MethodPut, http.MethodPatch} {
		s := New(t)
		cfg := s.srv.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
		addr := strings.TrimPrefix(s.URL(), "https://")
		conn, err := tls.Dial("tcp", addr, cfg)
		if err != nil {
			t.Fatal(err)
		}
		head := method + " /api/v1/namespaces/ns/configmaps/cm HTTP/1.1\r\nHost: " + addr +
			"\r\nContent-Type: application/merge-patch+json\r\nContent-Length: 100\r\n\r\n{{{"
		if _, err := conn.Write([]byte(head)); err != nil {
			t.Fatal(err)
		}
		if err := conn.CloseWrite(); err != nil {
			t.Fatal(err)
		}
		resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		_ = conn.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("%s = %d, want 400", method, resp.StatusCode)
		}
	}
}

// TestUnsupportedFieldSelector: every shape of selector is classified — empty,
// accepted, a bare term without "=", an unknown key, and an empty term between
// commas.
func TestUnsupportedFieldSelector(t *testing.T) {
	cases := []struct {
		selector string
		want     string
	}{
		{"", ""},
		{"status.phase=Running", ""},
		{"metadata.name=1,,status.phase=Running", ""},
		{"bareterm", "bareterm"},
		{"spec.nodeName=node1", "spec.nodeName"},
	}
	for _, tc := range cases {
		if got := unsupportedFieldSelector("pods", tc.selector); got != tc.want {
			t.Fatalf("unsupportedFieldSelector(pods, %q) = %q, want %q", tc.selector, got, tc.want)
		}
	}
}

// TestMatchesFieldSelector: each field-selector key the fake implements matches
// and rejects, and a term without "=" is ignored.
func TestMatchesFieldSelector(t *testing.T) {
	it := item{name: "a", namespace: "ns", phase: "Running", involvedUID: "u1", involvedKind: "Job"}
	cases := []struct {
		selector string
		want     bool
	}{
		{"status.phase=Running", true},
		{"status.phase=Succeeded", false},
		{"metadata.name=a", true},
		{"metadata.name=b", false},
		{"metadata.namespace=ns", true},
		{"metadata.namespace=other", false},
		{"involvedObject.uid=u1", true},
		{"involvedObject.uid=other", false},
		{"involvedObject.kind=Job", true},
		{"involvedObject.kind=Pod", false},
		{"bareterm", true},
		{",,", true},
	}
	for _, tc := range cases {
		if got := matchesFieldSelector(tc.selector, it); got != tc.want {
			t.Fatalf("matchesFieldSelector(%q) = %v, want %v", tc.selector, got, tc.want)
		}
	}
}

// TestMatchesSelector: existence, equality and inequality terms match, an empty
// term between commas is skipped.
func TestMatchesSelector(t *testing.T) {
	labels := map[string]string{"a": "1"}
	cases := []struct {
		selector string
		want     bool
	}{
		{"", true},
		{"a", true},
		{"a=1", true},
		{"a=2", false},
		{"a!=2", true},
		{"a!=1", false},
		{"b", false},
		{"a=1,,a=1", true},
	}
	for _, tc := range cases {
		if got := matchesSelector(tc.selector, labels); got != tc.want {
			t.Fatalf("matchesSelector(%q) = %v, want %v", tc.selector, got, tc.want)
		}
	}
}
