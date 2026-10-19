package webhook_test

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	json "encoding/json/v2"

	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/notifier"
	"github.com/vi-latyshev/go-cluster-backup/internal/notifier/notifiertest"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
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
	_, err = plugin.Build(ctx, testRegistry(t), notifier.Kind, "webhook", raw,
		plugin.Deps{Instance: "webhook", Log: slog.New(slog.DiscardHandler)})
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
	if class, _ := plugin.ClassOf(err); class != plugin.ClassPermanent {
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
	if class, _ := plugin.ClassOf(err); class != plugin.ClassPermanent {
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
	if class, _ := plugin.ClassOf(err); class != plugin.ClassPermanent {
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
	cfg["payload"] = "{{ if not .Event.Run.Previous.RunID }}{{ .Event.Run.Lease.Holder }}{{ end }}title: {{ json .Title }}"
	err := build(t, cfg).Notify(context.Background(), notifiertest.Event())
	if err == nil {
		t.Fatal("Notify delivered a body the payload could not render")
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassPermanent {
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
	if class, _ := plugin.ClassOf(err); class != plugin.ClassPermanent {
		t.Errorf("an unbuildable request classified %q, want permanent", class)
	}
	if !strings.Contains(err.Error(), "building the run_finished request") {
		t.Errorf("the refusal does not name the request: %v", err)
	}
}

// TestACancelledRequestReturnsTheContextError: the engine's own context is the
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
// the engine may try again, not a permanent refusal.
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
	if class, _ := plugin.ClassOf(err); class != plugin.ClassTransient {
		t.Errorf("a truncated response classified %q, want transient", class)
	}
	if !strings.Contains(err.Error(), "reading the run_finished response") {
		t.Errorf("the refusal does not name the response: %v", err)
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
