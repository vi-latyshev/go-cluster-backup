package kubeapitest

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"testing/synctest"
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

// TestDelayReleasesOnCancellation: the delay must never outlive the request it
// holds. A client that gives up is the whole reason the handler watches the
// context; Close waits for outstanding handlers, so a handler still sleeping on
// its own timer makes this fail.
func TestDelayReleasesOnCancellation(t *testing.T) {
	// The branch itself, without a socket in the way: a request whose context is
	// already done must leave the delay at once and never reach the handler.
	s0 := New(t)
	s0.Delay = time.Hour
	reached := false
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/pods", nil).WithContext(ctx)
	s0.delayed(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { reached = true })).
		ServeHTTP(httptest.NewRecorder(), req)
	if reached {
		t.Fatal("a cancelled request reached the handler through the delay")
	}

	s := New(t)
	s.Delay = time.Hour
	ctx2, cancel2 := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel2()
	req2, err := http.NewRequestWithContext(ctx2, http.MethodGet, s.srv.URL+"/api/v1/pods", nil)
	if err != nil {
		t.Fatal(err)
	}
	if resp, err := s.srv.Client().Do(req2); err == nil {
		_ = resp.Body.Close()
		t.Fatal("a request cancelled inside the delay answered")
	}
	closed := make(chan struct{})
	go func() { s.srv.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		t.Fatal("the handler is still delayed after its client gave up")
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
	s.FailNth("secrets/ns/pw", http.StatusInternalServerError, 1)
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

// serve runs one request through the server's own handler chain — delayed, the
// mux, the handler — without a socket, so the test can hold and cancel it
// deterministically.
func serve(s *Server, ctx context.Context, path string) (*httptest.ResponseRecorder, chan struct{}) {
	rec := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		s.srv.Config.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil).WithContext(ctx))
		close(done)
	}()
	return rec, done
}

// TestEveryArmedOrdinalAdvancesOncePerRequest is the fault-injection contract in
// one table: every armed FailNth on a request's key chain advances exactly once
// at arrival — before any answer is chosen and before the hold — so a shot that
// a more specific key or a higher-priority fault masks is spent on the request
// that landed on it, never shifted to the next. Each row runs in four shapes of
// one held first request: answered after its hold, cancelled inside it, a second
// request overlapped with it, and an overlapped second request whose first is
// then cancelled. A re-arm row replaces a rule while its earlier count is
// non-zero: the fresh count starts over, it does not continue.
func TestEveryArmedOrdinalAdvancesOncePerRequest(t *testing.T) {
	const (
		p1   = "pods/ns/p1"
		get  = "/api/v1/namespaces/ns/pods/p1"
		list = "/api/v1/pods"
	)
	rows := []struct {
		name   string
		arm    func(*Server)
		second string
		want   [2]int
		// rearm, when set, replaces a rule after request 1 was counted and
		// before request 2 starts; third then asks for a third request and
		// want3 is what it answers.
		rearm func(*Server)
		third string
		want3 int
	}{
		{"specific FailNth alone", func(s *Server) { s.FailNth(p1, 502, 2) }, get, [2]int{200, 502}, nil, "", 0},
		{"resource FailNth alone", func(s *Server) { s.FailNth("pods", 502, 2) }, list, [2]int{200, 502}, nil, "", 0},
		{"specific FailNth fires first", func(s *Server) {
			s.FailNth(p1, 500, 1)
			s.FailNth("pods", 502, 2)
		}, list, [2]int{500, 502}, nil, "", 0},
		{"specific Fail first", func(s *Server) {
			s.Fail(p1, 500)
			s.FailNth("pods", 502, 2)
		}, list, [2]int{500, 502}, nil, "", 0},
		{"specific Deny first", func(s *Server) {
			s.Deny(p1)
			s.FailNth("pods", 502, 2)
		}, list, [2]int{403, 502}, nil, "", 0},
		{"same-key Fail", func(s *Server) {
			s.Fail(p1, 500)
			s.FailNth(p1, 502, 2)
		}, get, [2]int{500, 502}, nil, "", 0},
		{"same-key Deny", func(s *Server) {
			s.Deny(p1)
			s.FailNth(p1, 502, 2)
		}, get, [2]int{403, 502}, nil, "", 0},
		{"resource Fail, specific FailNth", func(s *Server) {
			s.Fail("pods", 500)
			s.FailNth(p1, 502, 2)
		}, get, [2]int{500, 502}, nil, "", 0},
		{"masked shot is spent", func(s *Server) {
			s.Fail(p1, 500)
			s.FailNth("pods", 502, 1)
		}, list, [2]int{500, 200}, nil, "", 0},
		{"re-arming resets the count", func(s *Server) { s.FailNth(p1, 502, 2) }, get, [2]int{200, 200},
			func(s *Server) { s.FailNth(p1, 502, 2) }, get, 502},
	}

	const (
		answered            = "answered"
		cancelled           = "cancelled"
		overlapped          = "overlapped"
		overlappedCancelled = "overlapped then cancelled"
	)
	for _, row := range rows {
		for _, shape := range []string{answered, cancelled, overlapped, overlappedCancelled} {
			t.Run(row.name+"/"+shape, func(t *testing.T) {
				s := New(t)
				s.AddPod(`{"metadata":{"name":"p1","namespace":"ns"},"status":{"phase":"Running"}}`)
				row.arm(s)
				// One hold, set before the bubble and never changed: the first
				// request is the one it holds.
				s.Hold = map[string]time.Duration{p1: time.Hour}

				synctest.Test(t, func(t *testing.T) {
					fail := func(which string, got, want int) {
						t.Helper()
						t.Fatalf("%s/%s: %s = %d, want %d; Calls(%s)=%d, Calls(pods)=%d",
							row.name, shape, which, got, want, p1, s.Calls(p1), s.Calls("pods"))
					}
					ctx1, cancel1 := context.WithCancel(t.Context())
					defer cancel1()
					rec1, done1 := serve(s, ctx1, get)

					synctest.Wait()
					if got := s.Calls(p1); got != 1 {
						fail("Calls before the hold ended", got, 1)
					}
					select {
					case <-done1:
						t.Fatalf("%s/%s: the first request finished while its hold was still on", row.name, shape)
					default:
					}

					var rec2 *httptest.ResponseRecorder
					var done2 chan struct{}
					rearm := func() {
						if row.rearm != nil {
							row.rearm(s)
						}
					}
					second := func() {
						rec2, done2 = serve(s, t.Context(), row.second)
					}
					switch shape {
					case answered:
						time.Sleep(2 * time.Hour)
						<-done1
						rearm()
						second()
					case cancelled:
						cancel1()
						<-done1
						if rec1.Body.Len() != 0 {
							t.Fatalf("%s/%s: a cancelled request wrote %q", row.name, shape, rec1.Body.String())
						}
						rearm()
						second()
					case overlapped:
						rearm()
						second()
						synctest.Wait()
						time.Sleep(2 * time.Hour)
						<-done1
					case overlappedCancelled:
						rearm()
						second()
						synctest.Wait()
						cancel1()
						<-done1
						if rec1.Body.Len() != 0 {
							t.Fatalf("%s/%s: a cancelled request wrote %q", row.name, shape, rec1.Body.String())
						}
					}
					if shape == answered || shape == overlapped {
						if rec1.Code != row.want[0] {
							fail("the first request", rec1.Code, row.want[0])
						}
					}
					time.Sleep(2 * time.Hour)
					<-done2
					if rec2.Code != row.want[1] {
						fail("the second request", rec2.Code, row.want[1])
					}
					if row.rearm == nil {
						return
					}
					rec3, done3 := serve(s, t.Context(), row.third)
					time.Sleep(2 * time.Hour)
					<-done3
					if rec3.Code != row.want3 {
						fail("the third request after the re-arm", rec3.Code, row.want3)
					}
				})
			})
		}
	}
}
