package webhook_test

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	json "encoding/json/v2"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/notifier"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/notifier/notifiertest"
)

// check runs one raw configuration through the registry's offline check, the
// same path `validate` takes.
func check(t *testing.T, cfg map[string]any) error {
	t.Helper()
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return testRegistry(t).Check("notifier", "webhook", raw)
}

// TestValidateRefusesTheEndpointAndTheMethod: the endpoint and the mechanics are
// what the deployment states once, and a URL no client can post to or a method
// no endpoint takes is refused before the first night.
func TestValidateRefusesTheEndpointAndTheMethod(t *testing.T) {
	cases := []struct {
		name  string
		patch func(cfg map[string]any)
		want  string
	}{
		{
			name:  "a url that is not http",
			patch: func(cfg map[string]any) { cfg["url"] = "ftp://notify.example.com/backup" },
			want:  "must start with http:// or https://",
		},
		{
			name:  "no url at all",
			patch: func(cfg map[string]any) { cfg["url"] = "" },
			want:  "url is empty",
		},
		{
			name:  "a method no endpoint takes",
			patch: func(cfg map[string]any) { cfg["method"] = "DELETE" },
			want:  "is not one of GET, POST, PUT, PATCH",
		},
		{
			name:  "a timeout with no budget",
			patch: func(cfg map[string]any) { cfg["timeout"] = "0s" },
			want:  "timeout must be positive",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := baseConfig("http://127.0.0.1:1/never")
			tc.patch(cfg)
			err := check(t, cfg)
			if err == nil {
				t.Fatalf("a configuration with %s passed validate", tc.name)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("the refusal does not name %q: %v", tc.want, err)
			}
		})
	}
}

// TestValidateRefusesASpanMarkerArrivingFromTheTemplate: the two bytes the plugin
// marks a code span with may never appear in the template's own text, or the
// message would be torn in two by the author's own words.
func TestValidateRefusesASpanMarkerArrivingFromTheTemplate(t *testing.T) {
	cfg := baseConfig("http://127.0.0.1:1/never")
	cfg["events"] = map[string]string{"run_finished": "Backup \x01 {{ .Run.RunID }}"}
	err := check(t, cfg)
	if err == nil {
		t.Fatal("a message template carrying a span marker passed validate")
	}
	if !strings.Contains(err.Error(), "control character this plugin marks a code span with") {
		t.Errorf("the refusal does not name the control character: %v", err)
	}
}

// TestBuildRefusesACancelledContext: constructing the notifier is IO-free, but a
// context already cancelled is still the caller's answer.
func TestBuildRefusesACancelledContext(t *testing.T) {
	raw, err := json.Marshal(baseConfig("http://127.0.0.1:1/never"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = instances.Build(ctx, testRegistry(t), notifier.Kind, "webhook", raw,
		instances.Deps{Instance: "webhook", Log: slog.New(slog.DiscardHandler)})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("building with a cancelled context returned %v, want context.Canceled", err)
	}
}

// TestARedirectIsNotFollowed: a receiver answering 3xx is a misconfigured
// endpoint, so the body is never re-posted wherever a Location header points.
func TestARedirectIsNotFollowed(t *testing.T) {
	var followed atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		followed.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(target.Close)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusFound)
	}))
	t.Cleanup(srv.Close)

	err := build(t, baseConfig(srv.URL)).Notify(context.Background(), notifiertest.Event())
	if err == nil {
		t.Fatal("Notify accepted a 302")
	}
	if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassPermanent {
		t.Errorf("a 302 classified %q, want permanent", class)
	}
	if n := followed.Load(); n != 0 {
		t.Errorf("the body was posted to the redirect target %d times; a redirect is never followed", n)
	}
}

// TestASpanThatDoesNotPairUpInTheRenderIsPermanent: a message template renders
// one night's data, and a code span whose opening marker the data kept off the
// line leaves a close with no open — a message the model cannot write.
func TestASpanThatDoesNotPairUpInTheRenderIsPermanent(t *testing.T) {
	cfg := baseConfig("http://127.0.0.1:1/never")
	cfg["events"] = map[string]string{
		"run_finished": "{{ if .Run.Previous.RunID }}`{{ end }}Backup`",
	}
	n := build(t, cfg)
	// Every sample the configuration is validated against has a previous run,
	// so the span pairs up offline and only the first-ever night breaks it.
	err := n.Notify(context.Background(), notifiertest.Event())
	if err == nil {
		t.Fatal("Notify delivered a message whose code span never opened")
	}
	if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassPermanent {
		t.Errorf("an unrenderable message classified %q, want permanent", class)
	}
	if !strings.Contains(err.Error(), "code span") {
		t.Errorf("the refusal does not name the code span: %v", err)
	}
}

// TestTextAfterACodeSpanIsPermanent: a span ends its line, so a night whose data
// puts more on that line after the closing marker is refused rather than
// delivered with its monospace part torn in two.
func TestTextAfterACodeSpanIsPermanent(t *testing.T) {
	cfg := baseConfig("http://127.0.0.1:1/never")
	cfg["events"] = map[string]string{
		"run_finished": "Backup finished\n`{{ .Run.RunID }}`\n{{- if not .Run.Previous.RunID }}first run{{ end }}",
	}
	err := build(t, cfg).Notify(context.Background(), notifiertest.Event())
	if err == nil {
		t.Fatal("Notify delivered a line carrying text after its code span")
	}
	if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassPermanent {
		t.Errorf("an unrenderable message classified %q, want permanent", class)
	}
	if !strings.Contains(err.Error(), "follows the code span") {
		t.Errorf("the refusal does not name what follows the span: %v", err)
	}
}

// TestAPayloadThatFailsToRenderIsPermanent: the body is the config's own
// template, and a night whose event it cannot render is a permanent failure —
// retrying would render exactly the same way.
func TestAPayloadThatFailsToRenderIsPermanent(t *testing.T) {
	cfg := baseConfig("http://127.0.0.1:1/never")
	// Every sample carries one of the two kinds, so the config validates; the
	// conformance event carries none, so this body renders nowhere.
	cfg["payload"] = `title: {{ pick .Event.Run.Kind "scheduled" "night" "manual" "manual" }}`
	err := build(t, cfg).Notify(context.Background(), notifiertest.Event())
	if err == nil {
		t.Fatal("Notify delivered a body the payload could not render")
	}
	if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassPermanent {
		t.Errorf("an unrenderable body classified %q, want permanent", class)
	}
	if !strings.Contains(err.Error(), "rendering the payload") {
		t.Errorf("the refusal does not name the payload: %v", err)
	}
}

// TestARequestThatCannotBeBuiltIsPermanent: the URL passed validation, so a
// request the client still refuses is a config defect, not a receiver's outage.
func TestARequestThatCannotBeBuiltIsPermanent(t *testing.T) {
	err := build(t, baseConfig("http://exa\x7fmple.com/backup")).
		Notify(context.Background(), notifiertest.Event())
	if err == nil {
		t.Fatal("Notify built a request from a URL the client refuses")
	}
	if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassPermanent {
		t.Errorf("an unbuildable request classified %q, want permanent", class)
	}
	if !strings.Contains(err.Error(), "building the run_finished request") {
		t.Errorf("the refusal does not name the request: %v", err)
	}
}

// TestACancelledRequestReturnsTheContextError: the run's own context is the
// answer when the caller gives up while the request is in flight, not a
// transient transport failure it would retry.
func TestACancelledRequestReturnsTheContextError(t *testing.T) {
	// The receiver holds the request open until the test lets it go, so the
	// caller's context is the only thing that can end the call.
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		close(entered)
		<-release
		_ = conn.Close()
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(unblock)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	n := build(t, baseConfig(srv.URL))
	done := make(chan error, 1)
	go func() { done <- n.Notify(ctx, notifiertest.Event()) }()
	<-entered
	cancel()
	err := <-done
	unblock()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Notify under a cancelled context returned %v, want context.Canceled", err)
	}
}

// TestATruncatedResponseIsTransient: a receiver that dies mid-body is an outage
// execution may try again, not a permanent refusal.
func TestATruncatedResponseIsTransient(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		// A body shorter than the length it announces, then a close.
		_, _ = conn.Write([]byte("HTTP/1.1 500 Internal Server Error\r\nContent-Length: 4096\r\n\r\nshort"))
	}))
	t.Cleanup(srv.Close)

	err := build(t, baseConfig(srv.URL)).Notify(context.Background(), notifiertest.Event())
	if err == nil {
		t.Fatal("Notify accepted a response body that ended early")
	}
	if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassTransient {
		t.Errorf("a truncated response classified %q, want transient", class)
	}
	if !strings.Contains(err.Error(), "reading the run_finished response") {
		t.Errorf("the refusal does not name the response: %v", err)
	}
}

// TestATransportFailureDoesNotCarryTheEndpoint: a *url.Error spells out the
// request URL, and a bot endpoint's path is its credential
// (https://host/bot123:SECRET/sendMessage). The run stores the error text in
// the run report and sends it to every other notifier, so the cause never
// reaches it at all.
func TestATransportFailureDoesNotCarryTheEndpoint(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	endpoint := srv.URL + "/bot123:SECRET/sendMessage"
	srv.Close()

	err := build(t, baseConfig(endpoint)).Notify(context.Background(), notifiertest.Event())
	if err == nil {
		t.Fatal("Notify to a closed receiver returned success")
	}
	if strings.Contains(err.Error(), "SECRET") {
		t.Fatalf("the error carries the endpoint's own URL: %v", err)
	}
	if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassTransient {
		t.Errorf("a refused connection classified %q, want transient", class)
	}
	if !strings.Contains(err.Error(), "posting run_finished") {
		t.Errorf("the refusal does not name the event: %v", err)
	}
}

// TestReviewMalformedEndpointDoesNotLeakSecret: a URL the client refuses to
// build must not report itself — a *url.Error on the construction path spells
// out the endpoint exactly as the transport one does.
func TestReviewMalformedEndpointDoesNotLeakSecret(t *testing.T) {
	err := build(t, baseConfig("https://notify.example.com/botSECRET/%zz")).Notify(context.Background(), notifiertest.Event())
	if err == nil || strings.Contains(err.Error(), "SECRET") {
		t.Fatalf("delivery leaks endpoint: %v", err)
	}
	if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassPermanent {
		t.Errorf("an unbuildable request classified %q, want permanent", class)
	}
}

// TestReviewResponseCannotEchoEndpoint: a receiver that copies the request URI
// into its answer must not be able to put the endpoint's path or query into the
// error text, which the run report stores verbatim — whatever the body's
// length or representation.
func TestReviewResponseCannotEchoEndpoint(t *testing.T) {
	cases := map[string]struct {
		answer           func(uri string) string
		maxResponseBytes int
	}{
		"the whole URI":   {answer: func(uri string) string { return uri }, maxResponseBytes: 4096},
		"a truncated URI": {answer: func(uri string) string { return uri }, maxResponseBytes: 10},
		"an escaped URI":  {answer: func(uri string) string { return url.PathEscape(uri) }, maxResponseBytes: 4096},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, tc.answer(r.URL.RequestURI()), http.StatusForbidden)
			}))
			t.Cleanup(srv.Close)
			cfg := baseConfig(srv.URL + "/botSECRET/sendMessage?token=TOKEN")
			cfg["max_response_bytes"] = tc.maxResponseBytes
			err := build(t, cfg).Notify(context.Background(), notifiertest.Event())
			if err == nil {
				t.Fatal("a 403 was accepted")
			}
			for _, secret := range []string{"SECRET", "TOKEN"} {
				if strings.Contains(err.Error(), secret) {
					t.Fatalf("response diagnostics leaked the endpoint: %v", err)
				}
			}
			if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassPermanent {
				t.Errorf("HTTP 403 classified %q, want permanent", class)
			}
		})
	}
}

// TestReviewTruncatedEndpointEcho: the read bound must not reopen the leak the
// whole-URI case closes — a body cut before the redacted prefix would still
// carry the credential-bearing path into the report.
func TestReviewTruncatedEndpointEcho(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, r.URL.RequestURI(), http.StatusForbidden)
	}))
	t.Cleanup(srv.Close)
	cfg := baseConfig(srv.URL + "/botSECRET/sendMessage")
	cfg["max_response_bytes"] = 10
	err := build(t, cfg).Notify(context.Background(), notifiertest.Event())
	if err == nil || strings.Contains(err.Error(), "SECRET") {
		t.Fatalf("truncated response leaked endpoint: %v", err)
	}
}

// TestReviewMalformedHeadersCannotEchoEndpoint: a response whose header block
// is malformed makes net/http quote the request line back. The delivery error
// carries no cause at all, so the path never reaches it.
func TestReviewMalformedHeadersCannotEchoEndpoint(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		_, _ = conn.Write([]byte("HTTP/1.1 403 Forbidden\r\nno-colon " + r.URL.RequestURI() + "\r\n\r\n"))
	}))
	t.Cleanup(srv.Close)
	err := build(t, baseConfig(srv.URL+"/botSECRET/sendMessage?token=TOKEN")).Notify(context.Background(), notifiertest.Event())
	if err == nil || strings.Contains(err.Error(), "SECRET") || strings.Contains(err.Error(), "TOKEN") {
		t.Fatalf("malformed headers leaked endpoint: %v", err)
	}
	if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassTransient {
		t.Errorf("a malformed response classified %q, want transient", class)
	}
}

// TestReviewMalformedTrailerCannotEchoEndpoint: a chunked body whose trailer is
// malformed fails while the body is read, and the parser's text quotes the
// request URI. The read stage reports itself and nothing else.
func TestReviewMalformedTrailerCannotEchoEndpoint(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		_, _ = conn.Write([]byte("HTTP/1.1 403 Forbidden\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n" + r.URL.RequestURI() + "\r\n\r\n"))
	}))
	t.Cleanup(srv.Close)
	err := build(t, baseConfig(srv.URL+"/botSECRET/sendMessage?token=TOKEN")).Notify(context.Background(), notifiertest.Event())
	if err == nil || strings.Contains(err.Error(), "SECRET") || strings.Contains(err.Error(), "TOKEN") {
		t.Fatalf("malformed trailer leaked endpoint: %v", err)
	}
	if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassTransient {
		t.Errorf("a malformed trailer classified %q, want transient", class)
	}
}

// TestDeliveryDiagnosticsCarryNoCause is the one matrix for every stage: the
// validation, request build, transport, response read and HTTP status branches
// all report the stage, the event, the status when there is one, and the class
// — never a cause, because a transport, parser or receiver text can quote the
// endpoint's credential-bearing path or query.
func TestDeliveryDiagnosticsCarryNoCause(t *testing.T) {
	const suffix = "/botSECRET/sendMessage?token=TOKEN"
	notify := func(t *testing.T, cfg map[string]any) error {
		t.Helper()
		return build(t, cfg).Notify(context.Background(), notifiertest.Event())
	}
	validate := func(t *testing.T, cfg map[string]any) error {
		t.Helper()
		return check(t, cfg)
	}
	serve := func(t *testing.T, handler http.HandlerFunc) map[string]any {
		t.Helper()
		srv := httptest.NewServer(handler)
		t.Cleanup(srv.Close)
		return baseConfig(srv.URL + suffix)
	}
	withLimit := func(t *testing.T, handler http.HandlerFunc, limit any) map[string]any {
		t.Helper()
		cfg := serve(t, handler)
		cfg["max_response_bytes"] = limit
		return cfg
	}
	status := func(code int) http.HandlerFunc {
		return func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "x", code) }
	}
	echo := func(answer func(string) string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, answer(r.URL.RequestURI()), http.StatusForbidden)
		}
	}
	cases := map[string]struct {
		cfg  func(*testing.T) map[string]any
		run  func(*testing.T, map[string]any) error
		want pluginerrors.Class
	}{
		"validation refuses a scheme": {
			cfg:  func(*testing.T) map[string]any { return baseConfig("ftp://notify.example.com" + suffix) },
			run:  validate,
			want: "",
		},
		"the request cannot be built": {
			cfg:  func(*testing.T) map[string]any { return baseConfig("https://notify.example.com" + suffix + "\x7f") },
			run:  notify,
			want: pluginerrors.ClassPermanent,
		},
		"the transport has no answer": {
			cfg: func(t *testing.T) map[string]any {
				t.Helper()
				srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
				cfg := baseConfig(srv.URL + suffix)
				srv.Close()
				return cfg
			},
			run:  notify,
			want: pluginerrors.ClassTransient,
		},
		"the transport times out": {
			cfg: func(t *testing.T) map[string]any {
				t.Helper()
				// The receiver holds the request until the test releases it, so
				// the configured budget is the only thing that can end the call.
				release := make(chan struct{})
				srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { <-release }))
				t.Cleanup(srv.Close)
				t.Cleanup(func() { close(release) })
				cfg := baseConfig(srv.URL + suffix)
				cfg["timeout"] = "200ms"
				return cfg
			},
			run:  notify,
			want: pluginerrors.ClassTransient,
		},
		"the response body is truncated": {
			cfg: func(t *testing.T) map[string]any {
				t.Helper()
				return serve(t, func(w http.ResponseWriter, _ *http.Request) {
					conn, _, err := w.(http.Hijacker).Hijack()
					if err != nil {
						return
					}
					defer func() { _ = conn.Close() }()
					_, _ = conn.Write([]byte("HTTP/1.1 500 Internal Server Error\r\nContent-Length: 4096\r\n\r\nshort"))
				})
			},
			run:  notify,
			want: pluginerrors.ClassTransient,
		},
		"the response headers are malformed": {
			cfg: func(t *testing.T) map[string]any {
				t.Helper()
				return serve(t, func(w http.ResponseWriter, r *http.Request) {
					conn, _, err := w.(http.Hijacker).Hijack()
					if err != nil {
						return
					}
					defer func() { _ = conn.Close() }()
					_, _ = conn.Write([]byte("HTTP/1.1 403 Forbidden\r\nno-colon " + r.URL.RequestURI() + "\r\n\r\n"))
				})
			},
			run:  notify,
			want: pluginerrors.ClassTransient,
		},
		"the response trailer is malformed": {
			cfg: func(t *testing.T) map[string]any {
				t.Helper()
				return serve(t, func(w http.ResponseWriter, r *http.Request) {
					conn, _, err := w.(http.Hijacker).Hijack()
					if err != nil {
						return
					}
					defer func() { _ = conn.Close() }()
					_, _ = conn.Write([]byte("HTTP/1.1 403 Forbidden\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n" + r.URL.RequestURI() + "\r\n\r\n"))
				})
			},
			run:  notify,
			want: pluginerrors.ClassTransient,
		},
		"the status echoes the whole URI": {
			cfg: func(t *testing.T) map[string]any {
				return withLimit(t, echo(func(uri string) string { return uri }), "4KiB")
			},
			run:  notify,
			want: pluginerrors.ClassPermanent,
		},
		"the status echoes a truncated URI": {
			cfg: func(t *testing.T) map[string]any {
				return withLimit(t, echo(func(uri string) string { return uri }), 10)
			},
			run:  notify,
			want: pluginerrors.ClassPermanent,
		},
		"the status echoes an escaped URI": {
			cfg:  func(t *testing.T) map[string]any { return withLimit(t, echo(url.PathEscape), "4KiB") },
			run:  notify,
			want: pluginerrors.ClassPermanent,
		},
		"a retryable status": {
			cfg:  func(t *testing.T) map[string]any { return serve(t, status(http.StatusServiceUnavailable)) },
			run:  notify,
			want: pluginerrors.ClassTransient,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			err := tc.run(t, tc.cfg(t))
			if err == nil {
				t.Fatal("the delivery was accepted")
			}
			for _, secret := range []string{"SECRET", "TOKEN"} {
				if strings.Contains(err.Error(), secret) {
					t.Fatalf("the diagnosis leaked the endpoint: %v", err)
				}
			}
			if class, _ := pluginerrors.ClassOf(err); class != tc.want {
				t.Fatalf("class = %q, want %q (%v)", class, tc.want, err)
			}
		})
	}
}

// TestACancelledResponseBodyReturnsTheContextError: a caller that gives up
// while the body is being read gets its own context error, not a delivery
// diagnosis built from a truncated stream.
func TestACancelledResponseBodyReturnsTheContextError(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte("x"))
		w.(http.Flusher).Flush()
		close(entered)
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(release) })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- build(t, baseConfig(srv.URL)).Notify(ctx, notifiertest.Event()) }()
	<-entered
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Notify under a cancelled body read returned %v, want context.Canceled", err)
	}
}

// TestReviewResponseReadUsesConfiguredBound: the read stops at
// max_response_bytes, observed without the error text — a receiver sends a
// prefix, flushes and holds the connection; a bound below the prefix ends the
// attempt at once with the status, a bound above it waits for bytes that never
// come until the configured timeout. A plain io.ReadAll never finishes the
// former and only the timeout ends it.
func TestReviewResponseReadUsesConfiguredBound(t *testing.T) {
	run := func(t *testing.T, limit any) error {
		t.Helper()
		// The receiver sends a prefix, flushes it and holds the tail until the
		// test releases it: only the read bound or the configured budget can
		// end the attempt.
		release := make(chan struct{})
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(strings.Repeat("x", 1024)))
			w.(http.Flusher).Flush()
			<-release
		}))
		t.Cleanup(srv.Close)
		t.Cleanup(func() { close(release) })
		cfg := baseConfig(srv.URL)
		cfg["max_response_bytes"] = limit
		cfg["timeout"] = "300ms"
		return build(t, cfg).Notify(context.Background(), notifiertest.Event())
	}
	if err := run(t, 10); err == nil {
		t.Fatal("no error below the held prefix")
	} else if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassPermanent {
		t.Fatalf("the read passed the configured bound: %v", err)
	}
	if err := run(t, "2KiB"); err == nil {
		t.Fatal("no error above the held prefix")
	} else if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassTransient {
		t.Fatalf("a read waiting for the held tail classified %q, want transient (%v)", class, err)
	}
}

// TestPayloadThatDoesNotEncodeAsJSONIsRefusedOffline: a body read as a YAML
// document must also be encodable as the JSON the receiver is sent, and a
// document it cannot hold is a config defect found before the first night.
func TestPayloadThatDoesNotEncodeAsJSONIsRefusedOffline(t *testing.T) {
	cfg := baseConfig("http://127.0.0.1:1/never")
	cfg["payload"] = ".nan"
	err := check(t, cfg)
	if err == nil {
		t.Fatal("a payload that cannot encode as JSON passed validate")
	}
	if !strings.Contains(err.Error(), "does not encode as JSON") {
		t.Errorf("the refusal does not name the encoding: %v", err)
	}
}

// TestMessageTemplatesUseThePluginsOwnFunctions: the functions this plugin adds
// to the core set are the ones a message walking a report is written with, and
// the blank line one of them asks for survives the fold.
func TestMessageTemplatesUseThePluginsOwnFunctions(t *testing.T) {
	const message = "{{ pickdef .Run.Outcome \"Backup\" \"delivered\" \"Backup completed\" \"failed\" \"Backup FAILED\" }}\n" +
		"{{ sumBytes .Run.Artifacts }} bytes\n" +
		"{{ range byGroup .Run.Artifacts }}{{ .Count }}×`{{ .Name }}`\n{{ end }}" +
		"{{ if failed .Run.Artifacts }}⚠️ failed: {{ len (failed .Run.Artifacts) }}\n{{ end }}" +
		"{{ blank }}\n" +
		"first {{ clip 4 .Run.RunID }}\n"

	cfg := baseConfig("http://127.0.0.1:1/never")
	cfg["events"] = map[string]string{"run_finished": message}

	ev := notifiertest.Event()
	ev.Run.RunID = "20261012T042000Z"
	ev.Run.Artifacts = []model.ArtifactResult{
		{Artifact: model.Artifact{Group: "databases", Name: "example/postgres"}, Outcome: model.ArtifactOK, Bytes: 1 << 20},
		{Artifact: model.Artifact{Group: "databases", Name: "example/mysql"}, Outcome: model.ArtifactFailed},
	}

	got := posted(t, cfg, ev)
	if got.Title != "Backup completed" {
		t.Errorf("the title is %q, want the text pickdef chose for a delivered run", got.Title)
	}
	want := []string{"1048576 bytes", "2×databases", "⚠️ failed: 1", "", "first 2026…"}
	lines := got.lines()
	if len(lines) != len(want) {
		t.Fatalf("the message has %d lines, want %d: %q", len(lines), len(want), lines)
	}
	for i, w := range want {
		if lines[i] != w {
			t.Errorf("line %d is %q, want %q", i, lines[i], w)
		}
	}
}
