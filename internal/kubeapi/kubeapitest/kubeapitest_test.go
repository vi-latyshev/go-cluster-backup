package kubeapitest

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

func get(t *testing.T, s *Server, path string) (int, []byte) {
	t.Helper()
	resp, err := s.srv.Client().Get(s.srv.URL + path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body := make([]byte, 0, 1024)
	buf := make([]byte, 512)
	for {
		n, err := resp.Body.Read(buf)
		body = append(body, buf[:n]...)
		if err != nil {
			break
		}
	}
	return resp.StatusCode, body
}

func list(t *testing.T, s *Server, path string) (items []json.RawMessage, continueToken string) {
	t.Helper()
	code, body := get(t, s, path)
	if code != http.StatusOK {
		t.Fatalf("GET %s = %d: %s", path, code, body)
	}
	var page struct {
		Metadata struct {
			Continue string `json:"continue"`
		} `json:"metadata"`
		Items []json.RawMessage `json:"items"`
	}
	if err := json.Unmarshal(body, &page); err != nil {
		t.Fatal(err)
	}
	return page.Items, page.Metadata.Continue
}

func TestListFiltersBySelectorAndPhase(t *testing.T) {
	s := New(t)
	s.AddPod(`{"metadata":{"name":"a","namespace":"ns","labels":{"type":"database","engine":"postgres"}},"status":{"phase":"Running"}}`)
	s.AddPod(`{"metadata":{"name":"b","namespace":"ns","labels":{"type":"database","engine":"redis"}},"status":{"phase":"Running"}}`)
	s.AddPod(`{"metadata":{"name":"c","namespace":"ns","labels":{"type":"cache"}},"status":{"phase":"Succeeded"}}`)

	items, next := list(t, s, "/api/v1/pods?labelSelector=type%3Ddatabase,engine%21%3Dredis&fieldSelector=status.phase%3DRunning")
	if len(items) != 1 || next != "" {
		t.Fatalf("items = %d, continue = %q", len(items), next)
	}
	items, _ = list(t, s, "/api/v1/pods?labelSelector=engine")
	if len(items) != 2 {
		t.Fatalf("bare key selector returned %d items", len(items))
	}
}

func TestListPaginatesAndContinueIndexes(t *testing.T) {
	s := New(t)
	s.PageSize = 1
	for _, name := range []string{"a", "b", "c"} {
		s.AddPod(`{"metadata":{"name":"` + name + `","namespace":"ns"},"status":{"phase":"Running"}}`)
	}
	items, next := list(t, s, "/api/v1/pods")
	if len(items) != 1 || next != "1" {
		t.Fatalf("first page: %d items, continue %q", len(items), next)
	}
	items, next = list(t, s, "/api/v1/pods?continue="+next)
	if len(items) != 1 || next != "2" {
		t.Fatalf("second page: %d items, continue %q", len(items), next)
	}
	items, next = list(t, s, "/api/v1/pods?continue="+next)
	if len(items) != 1 || next != "" {
		t.Fatalf("third page: %d items, continue %q", len(items), next)
	}
}

func TestStuckContinueRepeatsTheToken(t *testing.T) {
	s := New(t)
	s.PageSize = 1
	s.StuckContinue = true
	s.AddPod(`{"metadata":{"name":"a","namespace":"ns"},"status":{"phase":"Running"}}`)
	s.AddPod(`{"metadata":{"name":"b","namespace":"ns"},"status":{"phase":"Running"}}`)
	items, next := list(t, s, "/api/v1/pods")
	if len(items) != 1 || next != "0" {
		t.Fatalf("first page: %d items, continue %q", len(items), next)
	}
	// The token repeats, so a client that follows it makes no progress.
	if _, next = list(t, s, "/api/v1/pods?continue="+next); next != "0" {
		t.Fatalf("continue = %q, want the unchanged token 0", next)
	}
}

func TestSecretAnswersBase64And404(t *testing.T) {
	s := New(t)
	s.AddSecret("ns", "pw", map[string]string{"PASSWORD": "s3cret"})
	code, body := get(t, s, "/api/v1/namespaces/ns/secrets/pw")
	if code != http.StatusOK {
		t.Fatalf("status = %d: %s", code, body)
	}
	var secret struct {
		Data map[string]string `json:"data"`
	}
	if err := json.Unmarshal(body, &secret); err != nil {
		t.Fatal(err)
	}
	decoded, err := base64.StdEncoding.DecodeString(secret.Data["PASSWORD"])
	if err != nil || string(decoded) != "s3cret" {
		t.Fatalf("data = %q, %v", secret.Data["PASSWORD"], err)
	}
	if code, _ := get(t, s, "/api/v1/namespaces/ns/secrets/missing"); code != http.StatusNotFound {
		t.Fatalf("missing secret = %d", code)
	}
}

func TestDeniedAndForcedStatuses(t *testing.T) {
	s := New(t)
	s.AddSecret("ns", "pw", map[string]string{"PASSWORD": "x"})
	s.Deny("secrets/ns/pw")
	code, body := get(t, s, "/api/v1/namespaces/ns/secrets/pw")
	if code != http.StatusForbidden || !strings.Contains(string(body), "Forbidden") {
		t.Fatalf("denied secret = %d: %s", code, body)
	}
	s.Allow("secrets/ns/pw")
	if code, _ := get(t, s, "/api/v1/namespaces/ns/secrets/pw"); code != http.StatusOK {
		t.Fatalf("after Allow = %d", code)
	}
	s.Fail("secrets", 500)
	if code, _ := get(t, s, "/api/v1/namespaces/ns/secrets/pw"); code != 500 {
		t.Fatalf("forced status = %d", code)
	}
}

func put(t *testing.T, s *Server, path, body string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, s.srv.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw := make([]byte, 0, 1024)
	buf := make([]byte, 512)
	for {
		n, err := resp.Body.Read(buf)
		raw = append(raw, buf[:n]...)
		if err != nil {
			break
		}
	}
	return resp.StatusCode, raw
}

func TestConfigMapUpdateChecksResourceVersion(t *testing.T) {
	s := New(t)
	s.AddConfigMap("ns", "catalog", map[string]string{"k": "[]"})
	code, body := get(t, s, "/api/v1/namespaces/ns/configmaps/catalog")
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
	if cm.Metadata.ResourceVersion != "1" {
		t.Fatalf("resourceVersion = %q", cm.Metadata.ResourceVersion)
	}

	code, body = put(t, s, "/api/v1/namespaces/ns/configmaps/catalog", `{"metadata":{"resourceVersion":"stale"},"data":{"k":"[1]"}}`)
	if code != http.StatusConflict || !strings.Contains(string(body), "has been modified") {
		t.Fatalf("stale put = %d: %s", code, body)
	}
	code, _ = put(t, s, "/api/v1/namespaces/ns/configmaps/catalog", `{"metadata":{"resourceVersion":"1"},"data":{"k":"[1]"}}`)
	if code != http.StatusOK {
		t.Fatalf("put = %d", code)
	}
	if data := s.ConfigMapData("ns", "catalog"); data["k"] != "[1]" {
		t.Fatalf("stored = %v", data)
	}

	s.ConflictNextPut = true
	if code, _ := put(t, s, "/api/v1/namespaces/ns/configmaps/catalog", `{"metadata":{"resourceVersion":"2"},"data":{"k":"[2]"}}`); code != http.StatusConflict {
		t.Fatalf("ConflictNextPut = %d", code)
	}
	if code, _ := put(t, s, "/api/v1/namespaces/ns/configmaps/catalog", `{"metadata":{"resourceVersion":"2"},"data":{"k":"[2]"}}`); code != http.StatusOK {
		t.Fatalf("put after the conflict = %d", code)
	}
}

func TestRecordedTokensAndSetToken(t *testing.T) {
	s := New(t)
	opts := s.Connection(t, "GCB_TEST_HOST", "GCB_TEST_PORT")
	s.AddPod(`{"metadata":{"name":"a","namespace":"ns"},"status":{"phase":"Running"}}`)
	req, err := http.NewRequest(http.MethodGet, s.srv.URL+"/api/v1/pods", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer from-the-wire")
	resp, err := s.srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if len(s.Tokens) != 1 || s.Tokens[0] != "from-the-wire" {
		t.Fatalf("tokens = %v", s.Tokens)
	}
	SetToken(t, opts, "rewritten")
	content, err := os.ReadFile(opts.TokenFile)
	if err != nil || string(content) != "rewritten" {
		t.Fatalf("token file = %q, %v", content, err)
	}
}

func TestConnectionPointsAtTheServer(t *testing.T) {
	s := New(t)
	opts := s.Connection(t, "GCB_TEST_HOST", "GCB_TEST_PORT")
	if os.Getenv("GCB_TEST_HOST") == "" || os.Getenv("GCB_TEST_PORT") == "" {
		t.Fatal("Connection did not set the environment")
	}
	ca, err := os.ReadFile(opts.CAFile)
	if err != nil || !strings.Contains(string(ca), "BEGIN CERTIFICATE") {
		t.Fatalf("CA file = %q, %v", ca, err)
	}
}

func TestDelayHoldsTheResponse(t *testing.T) {
	s := New(t)
	s.Delay = 30 * time.Millisecond
	start := time.Now()
	if code, _ := get(t, s, "/api/v1/pods"); code != http.StatusOK {
		t.Fatalf("status = %d", code)
	}
	if elapsed := time.Since(start); elapsed < 30*time.Millisecond {
		t.Fatalf("answered after %s", elapsed)
	}
}

func TestUnknownRouteIs404(t *testing.T) {
	if code, _ := get(t, New(t), "/api/v1/namespaces"); code != http.StatusNotFound {
		t.Fatalf("status = %d", code)
	}
}

// TestFieldSelectorsRefusedPerResource: the fake mirrors the API server's
// per-resource field selectors, live-confirmed — status.phase is registered
// for pods only, and a PVC list carrying it answers 400 with the server's own
// message. A client that sends it therefore fails a test instead of the 04:20
// run, which is exactly how this one shipped.
func TestFieldSelectorsRefusedPerResource(t *testing.T) {
	s := New(t)
	s.AddPVC(`{"metadata":{"name":"media","namespace":"ns"},"status":{"phase":"Bound"}}`)
	code, body := get(t, s, "/api/v1/persistentvolumeclaims?fieldSelector=status.phase%3DBound")
	if code != http.StatusBadRequest || !strings.Contains(message(t, body), `"status.phase" is not a known field selector`) {
		t.Fatalf("PVC with a status.phase selector = %d: %s", code, body)
	}
	items, _ := list(t, s, "/api/v1/persistentvolumeclaims?fieldSelector=metadata.name%3Dmedia")
	if len(items) != 1 {
		t.Fatalf("PVC with a metadata.name selector returned %d items", len(items))
	}
	s.AddPod(`{"metadata":{"name":"a","namespace":"ns"},"status":{"phase":"Running"}}`)
	if items, _ = list(t, s, "/api/v1/pods?fieldSelector=status.phase%3DRunning"); len(items) != 1 {
		t.Fatalf("pod with a status.phase selector returned %d items", len(items))
	}
	code, body = get(t, s, "/api/v1/pods?fieldSelector=spec.nodeName%3Dnode1")
	if code != http.StatusBadRequest || !strings.Contains(message(t, body), `"spec.nodeName" is not a known field selector`) {
		t.Fatalf("pod with an unimplemented selector = %d: %s", code, body)
	}
}

// message returns the Status message of an error body.
func message(t *testing.T, body []byte) string {
	t.Helper()
	var st struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal(body, &st); err != nil {
		t.Fatalf("error body is not a Status: %v: %s", err, body)
	}
	return st.Message
}

// TestFailOnceForcesExactlyOneRequest: a single blip answers the forced status
// once and the next request of the same object succeeds — what a client that
// retries must survive.
func TestFailOnceForcesExactlyOneRequest(t *testing.T) {
	s := New(t)
	s.AddSecret("ns", "pw", map[string]string{"PASSWORD": "s3cret"})
	s.FailOnce("secrets/ns/pw", http.StatusInternalServerError)
	code, body := get(t, s, "/api/v1/namespaces/ns/secrets/pw")
	if code != http.StatusInternalServerError {
		t.Fatalf("first GET = %d: %s", code, body)
	}
	code, body = get(t, s, "/api/v1/namespaces/ns/secrets/pw")
	if code != http.StatusOK {
		t.Fatalf("second GET = %d: %s", code, body)
	}
}

// TestFailNthForcesTheChosenRequest: only the nth request of the key answers
// the forced status — a blip aimed past the calls that came before it.
func TestFailNthForcesTheChosenRequest(t *testing.T) {
	s := New(t)
	s.AddSecret("ns", "pw", map[string]string{"PASSWORD": "s3cret"})
	s.FailNth("secrets/ns/pw", http.StatusInternalServerError, 2)
	for n, want := range []int{http.StatusOK, http.StatusInternalServerError, http.StatusOK} {
		if code, body := get(t, s, "/api/v1/namespaces/ns/secrets/pw"); code != want {
			t.Fatalf("request %d = %d: %s, want %d", n+1, code, body, want)
		}
	}
}

// TestCallsCountsRequestsPerKey: the counter is how a test proves how many
// passes a caller made, and a refused request is counted like any other.
func TestCallsCountsRequestsPerKey(t *testing.T) {
	s := New(t)
	s.AddSecret("ns", "pw", map[string]string{"PASSWORD": "s3cret"})
	s.FailOnce("secrets/ns/pw", http.StatusInternalServerError)
	for range 2 {
		_, _ = get(t, s, "/api/v1/namespaces/ns/secrets/pw")
	}
	if got := s.Calls("secrets/ns/pw"); got != 2 {
		t.Fatalf("Calls = %d, want 2", got)
	}
	if got := s.Calls("secrets/ns/other"); got != 0 {
		t.Fatalf("an unread key counts %d, want 0", got)
	}
}

// TestHoldLandsInsideTheRead: the request is counted before the hold, so a test
// that polls Calls sees the read as started while the fake still holds it.
func TestHoldLandsInsideTheRead(t *testing.T) {
	s := New(t)
	s.Hold = map[string]time.Duration{"secrets/ns/pw": 80 * time.Millisecond}
	s.AddSecret("ns", "pw", map[string]string{"PASSWORD": "s3cret"})
	done := make(chan int, 1)
	go func() {
		code, _ := get(t, s, "/api/v1/namespaces/ns/secrets/pw")
		done <- code
	}()
	deadline := time.Now().Add(2 * time.Second)
	for s.Calls("secrets/ns/pw") == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if s.Calls("secrets/ns/pw") == 0 {
		t.Fatal("the request was never counted")
	}
	select {
	case code := <-done:
		t.Fatalf("the answer arrived (%d) although the read is held", code)
	default:
	}
	if code := <-done; code != http.StatusOK {
		t.Fatalf("status = %d", code)
	}
}
