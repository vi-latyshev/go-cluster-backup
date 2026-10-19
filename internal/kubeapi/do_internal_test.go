package kubeapi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
)

// tokenFile writes a token file and returns its path.
func tokenFile(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte("token-1"), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// newClient builds a Client pointed at a test server, bypassing New's
// environment and CA plumbing so a single doWith branch can be exercised.
func newClient(t *testing.T, base string) *Client {
	t.Helper()
	return &Client{base: base, tokenFile: tokenFile(t), http: &http.Client{}}
}

// TestDoWithEncodeRequestIsPermanent: a body the JSON encoder cannot represent
// is a permanent error naming the encode step, never a retried request.
func TestDoWithEncodeRequestIsPermanent(t *testing.T) {
	c := newClient(t, "http://example.com")
	err := c.doWith(context.Background(), http.MethodPost, "/x", "application/json", nil, make(chan int), &struct{}{})
	if err == nil {
		t.Fatal("an unencodable body was accepted")
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassPermanent {
		t.Fatalf("class = %q, want permanent", class)
	}
	if !strings.Contains(err.Error(), "encode request") {
		t.Fatalf("error = %v", err)
	}
}

// TestDoWithInvalidRequestIsPermanent: a request the standard library refuses to
// build — an invalid method — is permanent, and it is reported before any I/O.
func TestDoWithInvalidRequestIsPermanent(t *testing.T) {
	c := newClient(t, "http://example.com")
	err := c.doWith(context.Background(), "BAD METHOD", "/x", "application/json", nil, nil, nil)
	if err == nil {
		t.Fatal("an invalid method was accepted")
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassPermanent {
		t.Fatalf("class = %q, want permanent", class)
	}
}

// TestDoWithNilOutDiscardsBody: a call that wants no decoded result reads and
// discards the body, so a caller with nothing to decode still drains the
// connection.
func TestDoWithNilOutDiscardsBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ignored":true}`))
	}))
	defer srv.Close()
	c := newClient(t, srv.URL)
	if err := c.doWith(context.Background(), http.MethodGet, "/", "application/json", nil, nil, nil); err != nil {
		t.Fatalf("doWith = %v", err)
	}
}

// TestDoWithDecodeResponseIsTransient: a 2xx answer whose body is not the JSON
// the caller expected is transient, so the engine retries it.
func TestDoWithDecodeResponseIsTransient(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`not json`))
	}))
	defer srv.Close()
	c := newClient(t, srv.URL)
	err := c.doWith(context.Background(), http.MethodGet, "/", "application/json", nil, nil, &struct{}{})
	if err == nil {
		t.Fatal("a non-JSON body was decoded")
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassTransient {
		t.Fatalf("class = %q, want transient", class)
	}
	if !strings.Contains(err.Error(), "decode response") {
		t.Fatalf("error = %v", err)
	}
}

// TestDoWithReadResponseIsTransient: a body the server promised but did not
// deliver is transient, so a truncated answer is retried rather than decoded.
func TestDoWithReadResponseIsTransient(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "100")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("short"))
	}))
	defer srv.Close()
	c := newClient(t, srv.URL)
	err := c.doWith(context.Background(), http.MethodGet, "/", "application/json", nil, nil, &struct{}{})
	if err == nil {
		t.Fatal("a truncated body was decoded")
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassTransient {
		t.Fatalf("class = %q, want transient", class)
	}
	if !strings.Contains(err.Error(), "read response") {
		t.Fatalf("error = %v", err)
	}
}

// TestDoWithCancelledDuringRequestReturnsContext: when the context is cancelled
// while the request is in flight, the context's own error is returned, not a
// transport error wrapped as transient — a cancelled run is not a retryable one.
func TestDoWithCancelledDuringRequestReturnsContext(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
	}))
	defer srv.Close()
	defer close(release)

	c := newClient(t, srv.URL)
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() {
		errc <- c.doWith(ctx, http.MethodGet, "/", "application/json", nil, nil, &struct{}{})
	}()
	<-entered
	cancel()
	err := <-errc
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if class, _ := plugin.ClassOf(err); class != "" {
		t.Fatalf("a cancelled request was classified %q: %v", class, err)
	}
}

// TestTokenReadErrorIsTransient: a token file that cannot be read is transient —
// a projected token mount can come back — and the error names the file.
func TestTokenReadErrorIsTransient(t *testing.T) {
	c := &Client{tokenFile: filepath.Join(t.TempDir(), "missing")}
	_, err := c.token()
	if err == nil {
		t.Fatal("a missing token file was accepted")
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassTransient {
		t.Fatalf("class = %q, want transient", class)
	}
	if !strings.Contains(err.Error(), "read token file") {
		t.Fatalf("error = %v", err)
	}
}
