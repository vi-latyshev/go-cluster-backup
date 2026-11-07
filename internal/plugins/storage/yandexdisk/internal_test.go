package yandexdisk

// Unit tests for the small helpers every request path routes through. They are
// internal because these are unexported, and because the shapes they read (a
// Retry-After in HTTP-date form, an error body with only a description) are not
// every one of them reproducible through the fake endpoint.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/configuration"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
)

func TestRetryAfterHint(t *testing.T) {
	if d := retryAfterHint(http.Header{}); d != 0 {
		t.Errorf("hint of an absent header = %s", d)
	}
	h := http.Header{}
	h.Set("Retry-After", "7")
	if d := retryAfterHint(h); d != 7*time.Second {
		t.Errorf("hint of seconds = %s, want 7s", d)
	}
	bad := http.Header{}
	bad.Set("Retry-After", "soon")
	if d := retryAfterHint(bad); d != 0 {
		t.Errorf("hint of an unreadable value = %s", d)
	}
	past := http.Header{}
	past.Set("Retry-After", time.Now().Add(-time.Hour).UTC().Format(http.TimeFormat))
	if d := retryAfterHint(past); d != 0 {
		t.Errorf("hint of a past date = %s", d)
	}
	future := http.Header{}
	future.Set("Retry-After", time.Now().Add(time.Hour).UTC().Format(http.TimeFormat))
	if d := retryAfterHint(future); d <= 0 {
		t.Errorf("hint of a future date = %s, want a positive one", d)
	}
}

func TestClassifyAPIMapsEveryAnswer(t *testing.T) {
	cases := []struct {
		status int
		class  pluginerrors.Class
	}{
		{http.StatusUnauthorized, pluginerrors.ClassPermanent},
		{http.StatusForbidden, pluginerrors.ClassPermanent},
		{http.StatusInsufficientStorage, pluginerrors.ClassPermanent},
		{http.StatusLocked, pluginerrors.ClassPermanent},
		{http.StatusTooManyRequests, pluginerrors.ClassTransient},
		{http.StatusInternalServerError, pluginerrors.ClassTransient},
		{http.StatusBadRequest, pluginerrors.ClassPermanent},
	}
	for _, tc := range cases {
		err := classifyAPI(apiResponse{status: tc.status, header: http.Header{}, body: []byte(`{"message":"m"}`)}, "op")
		class, _ := pluginerrors.ClassOf(err)
		if class != tc.class {
			t.Errorf("HTTP %d: class %q, want %q (%v)", tc.status, class, tc.class, err)
		}
	}
	h := http.Header{}
	h.Set("Retry-After", "5")
	class, after := pluginerrors.ClassOf(classifyAPI(apiResponse{status: http.StatusTooManyRequests, header: h}, "op"))
	if class != pluginerrors.ClassTransient || after != 5*time.Second {
		t.Errorf("429 with a hint: class %q after %s", class, after)
	}
}

func TestTransportErrorKeepsTheContextAndHidesTheHref(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := transportError(ctx, errors.New("x"), "op"); !errors.Is(err, context.Canceled) {
		t.Errorf("a dead context: %v", err)
	}
	uerr := &url.Error{Op: "PUT", URL: "http://host/up/secret-token", Err: errors.New("connection refused")}
	err := transportError(context.Background(), uerr, "upload data")
	if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassTransient {
		t.Errorf("class %q, want transient: %v", class, err)
	}
	if strings.Contains(err.Error(), "secret-token") {
		t.Errorf("the pre-authorised href leaked into the error: %v", err)
	}
	// An error that is not a url.Error is reported as it is.
	if err := transportError(context.Background(), errors.New("reset"), "op"); err == nil {
		t.Error("a plain transport failure was swallowed")
	}
}

func TestAPIMessage(t *testing.T) {
	cases := map[string]string{
		`{"message":"m","description":"d"}`: "m: d",
		`{"message":"m"}`:                   "m",
		`{"description":"d"}`:               "d",
		`  plain text  `:                    "plain text",
		`{"message":"","description":""}`:   `{"message":"","description":""}`,
	}
	for body, want := range cases {
		if got := apiMessage([]byte(body)); got != want {
			t.Errorf("apiMessage(%s) = %q, want %q", body, got, want)
		}
	}
}

func TestOrElse(t *testing.T) {
	if got := orElse("", "fallback"); got != "fallback" {
		t.Errorf("orElse of an empty value = %q", got)
	}
	if got := orElse("dir", "fallback"); got != "dir" {
		t.Errorf("orElse = %q", got)
	}
}

func TestParseModified(t *testing.T) {
	for _, raw := range []string{"2026-10-12T04:29:54Z", "2026-10-12T04:29:54", "2026-10-12 04:29:54"} {
		if got := parseModified(raw); got.IsZero() {
			t.Errorf("parseModified(%q) = the zero time", raw)
		}
	}
	if got := parseModified("not a timestamp"); !got.IsZero() {
		t.Errorf("parseModified of a bad value = %s", got)
	}
}

// TestMkdirAllGuardsItsPath: the folder walk is only ever handed a path under
// the application folder by Put, so the guard is exercised directly here — a
// path outside it is refused without a request, and the application folder
// itself needs no creation.
func TestMkdirAllGuardsItsPath(t *testing.T) {
	s := &Storage{root: "app:/cluster-backup"}
	err := s.mkdirAll(context.Background(), "other:/folder")
	if err == nil {
		t.Fatal("a folder outside the application folder was accepted")
	}
	if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassPermanent {
		t.Errorf("class %q, want permanent", class)
	}
	if err := s.mkdirAll(context.Background(), "app:/"); err != nil {
		t.Errorf("mkdirAll of the application folder itself: %v", err)
	}
}

// shortBodyTransport answers the control calls the plugin makes, and hands the
// download href a body shorter than the length it declares: the one shape Get's
// length guard exists for, and one no real transport produces — net/http errors
// on a body shorter than Content-Length rather than ending it cleanly.
type shortBodyTransport struct{}

func (shortBodyTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": {"application/json"}},
		Request:    req,
	}
	switch req.URL.Path {
	case "/v1/disk/resources":
		resp.Body = io.NopCloser(strings.NewReader(`{"type":"file","size":3,"modified":"2026-10-12T04:29:54+00:00"}`))
	case "/v1/disk/resources/download":
		resp.Body = io.NopCloser(strings.NewReader(`{"href":"https://download.example/short"}`))
	case "/short":
		resp.Body = io.NopCloser(strings.NewReader("abc"))
		resp.ContentLength = 100
	default:
		return nil, fmt.Errorf("unexpected request %s", req.URL)
	}
	return resp, nil
}

// TestConfineRejectsCursorsOutsideTheListingScope covers the guard every walk
// cursor passes before a request: a negative offset, a folder outside the root,
// and a folder beside the prefix are refused by cause, while the root above the
// prefix, the prefix folder and a folder below it are accepted.
func TestConfineRejectsCursorsOutsideTheListingScope(t *testing.T) {
	const (
		root   = "app:/cluster-backup"
		prefix = "app:/cluster-backup/runs/"
	)
	s := &Storage{root: root}
	refused := []struct {
		name   string
		cursor listCursor
		want   string
	}{
		{"negative offset", listCursor{Dir: root, Offset: -1}, "offset -1 is negative"},
		{"outside the root", listCursor{Dir: "app:/Backups"}, "outside the root"},
		{"beside the prefix", listCursor{Dir: root + "/other"}, "outside the prefix"},
	}
	for _, tc := range refused {
		t.Run(tc.name, func(t *testing.T) {
			err := s.confine(tc.cursor, prefix)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("confine(%+v) = %v, want a refusal naming %q", tc.cursor, err, tc.want)
			}
		})
	}
	for _, cursor := range []listCursor{{Dir: root}, {Dir: root + "/runs"}, {Dir: root + "/runs/2026/x"}} {
		if err := s.confine(cursor, prefix); err != nil {
			t.Errorf("confine(%+v) refused a cursor on the prefix path: %v", cursor, err)
		}
	}
}

// TestFetchPageRefusesAForgedCursorBeforeAnyRequest: the walk library treats a
// cursor as opaque, so the adapter checks every cursor it is handed — even one
// it did not produce — before naming a path in a request.
func TestFetchPageRefusesAForgedCursorBeforeAnyRequest(t *testing.T) {
	s := &Storage{root: "app:/cluster-backup", parallelism: 1}
	_, err := s.fetchPage(context.Background(), listCursor{Dir: "app:/Backups"}, "app:/cluster-backup/runs/")
	if err == nil || !strings.Contains(err.Error(), "outside the root") {
		t.Fatalf("fetchPage with a forged cursor = %v, want the root refusal", err)
	}
	if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassPermanent {
		t.Errorf("class %q, want permanent", class)
	}
}

func TestGetRejectsABodyShorterThanItsDeclaredLength(t *testing.T) {
	token := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(token, []byte("test-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	s, err := open(Config{
		APIURL: "https://cloud-api.example/v1/disk", Token: configuration.Secret{File: token},
		Root: "app:/cluster-backup", PermanentDelete: true,
		RequestTimeout:     model.Duration(time.Second),
		CommitPollInterval: model.Duration(time.Second),
		CommitPollTimeout:  model.Duration(time.Second),
	}, nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	s.http = &http.Client{Transport: shortBodyTransport{}}
	err = s.Get(context.Background(), "a.bin", io.Discard)
	if err == nil {
		t.Fatal("a download shorter than its declared length was accepted")
	}
	if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassTransient {
		t.Errorf("class %q, want transient: %v", class, err)
	}
}
