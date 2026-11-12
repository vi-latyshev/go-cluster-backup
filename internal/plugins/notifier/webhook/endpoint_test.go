package webhook_test

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	json "encoding/json/v2"

	pluginerrors "github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/execution"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/notifier"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/notifier/notifiertest"
)

// endpointSecret writes one mounted secret file and returns its path.
func endpointSecret(t *testing.T, value string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "endpoint-secret")
	if err := os.WriteFile(path, []byte(value), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// endpointConfig is the baseline with endpoint templating on.
func endpointConfig(t *testing.T, url string) map[string]any {
	t.Helper()
	cfg := baseConfig(url)
	cfg["endpoint_templates"] = true
	return cfg
}

// TestEndpointTemplatesDefaultToLiteral: endpoint templating is opt-in. With the
// default false an old endpoint and old header value carrying the braces a
// template would use are posted exactly as written — the config is never
// silently rewritten into a template and never reads a file it never named.
func TestEndpointTemplatesDefaultToLiteral(t *testing.T) {
	var gotPath, gotHeader string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotHeader = r.URL.Path, r.Header.Get("X-Token")
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	cfg := baseConfig(srv.URL + "/{{ literal }}")
	cfg["headers"] = map[string]string{"X-Token": `{{ secret "/never/read" }}`}
	if err := build(t, cfg).Notify(context.Background(), notifiertest.Event()); err != nil {
		t.Fatal(err)
	}
	if gotPath != "/{{ literal }}" {
		t.Errorf("url path = %q, want the literal braces", gotPath)
	}
	if gotHeader != `{{ secret "/never/read" }}` {
		t.Errorf("X-Token = %q, want the literal braces", gotHeader)
	}
}

// TestEndpointSecretIsRenderedPerCheckAndPerNotify: with templating on, Check
// renders the real secret files so a bad mount fails the instance at preflight,
// and every Notify renders again — a rotated secret is picked up without a
// rebuild, and a trailing CR/LF is not part of the value.
func TestEndpointSecretIsRenderedPerCheckAndPerNotify(t *testing.T) {
	path := endpointSecret(t, "TOKEN-ONE\n")
	var gotPath, gotHeader string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotHeader = r.URL.EscapedPath(), r.Header.Get("X-Token")
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	cfg := endpointConfig(t, srv.URL+"/hook/{{ secret "+quotePath(t, path)+" }}")
	cfg["headers"] = map[string]string{"X-Token": "{{ secret " + quotePath(t, path) + " }}"}
	n := build(t, cfg)
	checker, ok := n.(instances.Checker)
	if !ok {
		t.Fatal("the webhook notifier does not implement instances.Checker")
	}
	if err := checker.Check(context.Background()); err != nil {
		t.Fatalf("Check with a readable secret: %v", err)
	}
	if err := n.Notify(context.Background(), notifiertest.Event()); err != nil {
		t.Fatal(err)
	}
	if gotPath != "/hook/TOKEN-ONE" {
		t.Errorf("rendered url path = %q, want the trimmed secret", gotPath)
	}
	if gotHeader != "TOKEN-ONE" {
		t.Errorf("rendered X-Token = %q, want the trimmed secret", gotHeader)
	}
	if err := os.WriteFile(path, []byte("TOKEN-TWO\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := n.Notify(context.Background(), notifiertest.Event()); err != nil {
		t.Fatal(err)
	}
	if gotHeader != "TOKEN-TWO" {
		t.Errorf("X-Token after rotation = %q, want the reread secret", gotHeader)
	}
}

// quotePath quotes one path the way a template string literal is quoted.
func quotePath(t *testing.T, path string) string {
	t.Helper()
	b, err := json.Marshal(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// shippedRetry is the policy the defaults document ships for a run: four
// attempts, 5s doubling to 40s, 20% jitter, and an unclassified error retried as
// transient. Preflight runs every instance's Check under it.
var shippedRetry = execution.Options{
	Attempts: 4, Initial: 5 * time.Second, Max: 40 * time.Second, Jitter: 0.2,
	Unclassified: pluginerrors.ClassTransient,
}

// TestAMissingEndpointSecretFailsCheckOnTheFirstAttempt: a secret file that is
// missing, unreadable or oversized is a configuration fault, so Check answers a
// permanent error and the real retry ladder spends no wait on it — one attempt,
// zero virtual time. Before this classification an unreadable secret cost the
// shipped policy's four attempts and ~40 s of backoff (the docs dry run hit it).
func TestAMissingEndpointSecretFailsCheckOnTheFirstAttempt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := build(t, endpointConfig(t, `http://127.0.0.1:1/hook/{{ secret "/nonexistent/endpoint-secret" }}`))
		checker, ok := n.(instances.Checker)
		if !ok {
			t.Fatal("the webhook notifier does not implement instances.Checker")
		}
		attempts := 0
		start := time.Now()
		err := execution.AttemptErr(context.Background(), shippedRetry, "check notifier webhook",
			func(ctx context.Context, _ int) error {
				attempts++
				return checker.Check(ctx)
			})
		if err == nil {
			t.Fatal("Check accepted a missing secret file")
		}
		if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassPermanent {
			t.Errorf("class = %v, want permanent", class)
		}
		if attempts != 1 {
			t.Errorf("%d attempts, want one: a missing secret file is not a transient failure", attempts)
		}
		if elapsed := time.Since(start); elapsed != 0 {
			t.Errorf("the retry ladder waited %s for a permanent configuration fault", elapsed)
		}
	})
}

// TestAPlainEndpointChecksWithoutIO: a notifier with templating off is checked
// by preflight without touching a file or the network, exactly as before.
func TestAPlainEndpointChecksWithoutIO(t *testing.T) {
	n := build(t, baseConfig("http://127.0.0.1:1/never"))
	checker, ok := n.(instances.Checker)
	if !ok {
		t.Fatal("the webhook notifier does not implement instances.Checker")
	}
	if err := checker.Check(context.Background()); err != nil {
		t.Fatalf("Check of a literal endpoint: %v", err)
	}
}

// TestReviewNestedSecretNeverAppearsInError: the reviewer's counterexample. A
// nested call — secret (secret "/run/token") — hands the first file's contents
// back as the second call's path, and a refusal that quotes that path prints a
// secret into preflight and run diagnostics. The argument must be one literal
// absolute path: the config's own text is the only thing an error may name.
func TestReviewNestedSecretNeverAppearsInError(t *testing.T) {
	file := endpointSecret(t, "SECRET-SENTINEL")
	nested := func() string { return `(secret ` + quotePath(t, file) + `)` }
	for _, tc := range []struct {
		name  string
		apply func(cfg map[string]any)
	}{
		{"url", func(cfg map[string]any) { cfg["url"] = "https://notify.example.com/{{ secret " + nested() + " }}" }},
		{"header", func(cfg map[string]any) {
			cfg["headers"] = map[string]string{"X-Token": "{{ secret " + nested() + " }}"}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := endpointConfig(t, "https://notify.example.com/hook/")
			tc.apply(cfg)
			raw, err := json.Marshal(cfg)
			if err != nil {
				t.Fatal(err)
			}
			n, err := instances.Build(context.Background(), testRegistry(t), notifier.Kind, "webhook", raw,
				instances.Deps{Instance: "webhook", Log: slog.New(slog.DiscardHandler)})
			if err != nil {
				if strings.Contains(err.Error(), "SECRET-SENTINEL") {
					t.Fatalf("secret bytes reached diagnostics: %v", err)
				}
				return // refused offline, before any file is read
			}
			checker, ok := n.(instances.Checker)
			if !ok {
				t.Fatal("the webhook notifier does not implement instances.Checker")
			}
			if err := checker.Check(context.Background()); err == nil {
				t.Fatal("Check accepted a computed secret path")
			} else if strings.Contains(err.Error(), "SECRET-SENTINEL") {
				t.Fatalf("secret bytes reached diagnostics: %v", err)
			}
			if err := n.Notify(context.Background(), notifiertest.Event()); err == nil {
				t.Fatal("Notify accepted a computed secret path")
			} else if strings.Contains(err.Error(), "SECRET-SENTINEL") {
				t.Fatalf("secret bytes reached diagnostics: %v", err)
			}
		})
	}
}

// TestReviewPipelineInputCannotFeedSecret: a pipeline passes its previous
// command's output as one more argument of the next command, so
// `{{ "x" | secret "/run/token" }}` calls secret with two arguments. The literal
// argument looks valid on its own, so the walk must refuse a secret command
// that follows another command — offline, in the url and in a header, instead
// of accepting the config and failing every Check and Notify later.
func TestReviewPipelineInputCannotFeedSecret(t *testing.T) {
	expression := `{{ "x" | secret "/run/token" }}`
	for _, field := range []string{"url", "header"} {
		t.Run(field, func(t *testing.T) {
			cfg := endpointConfig(t, "https://notify.example.com/")
			if field == "url" {
				cfg["url"] = "https://notify.example.com/" + expression
			} else {
				cfg["headers"] = map[string]string{"X-Token": expression}
			}
			raw, err := json.Marshal(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := instances.Build(context.Background(), testRegistry(t), notifier.Kind, "webhook", raw,
				instances.Deps{Instance: "webhook", Log: slog.New(slog.DiscardHandler)}); err == nil {
				t.Fatal("a secret command fed by a pipeline was accepted; it can only fail at Check")
			}
		})
	}
}

// TestReviewChainNodeCannotHideComputedSecret: the reviewer's second
// counterexample. A chain node — (secret (printf "/%s" (secret "/run/token"))).Missing
// — used to hide both calls from the walk, so runtime read the first file,
// used its bytes as an absolute path and printed them in the second read's
// error. The allow-list walk follows ChainNode.Node, and every form below is
// refused offline, in the url and in a header, with no sentinel in diagnostics.
func TestReviewChainNodeCannotHideComputedSecret(t *testing.T) {
	file := endpointSecret(t, "SECRET-SENTINEL")
	forms := map[string]string{
		"chain":    `{{ (secret (printf "/%s" (secret ` + quotePath(t, file) + `))).Missing }}`,
		"nested":   `{{ secret (secret ` + quotePath(t, file) + `) }}`,
		"variable": `{{ $p := secret ` + quotePath(t, file) + ` }}{{ secret $p }}`,
	}
	for form, expression := range forms {
		for _, field := range []string{"url", "header"} {
			t.Run(form+"/"+field, func(t *testing.T) {
				cfg := endpointConfig(t, "https://notify.example.com/")
				if field == "url" {
					cfg["url"] = "https://notify.example.com/" + expression
				} else {
					cfg["headers"] = map[string]string{"X-Token": expression}
				}
				raw, err := json.Marshal(cfg)
				if err != nil {
					t.Fatal(err)
				}
				n, err := instances.Build(context.Background(), testRegistry(t), notifier.Kind, "webhook", raw,
					instances.Deps{Instance: "webhook", Log: slog.New(slog.DiscardHandler)})
				if err != nil {
					if strings.Contains(err.Error(), "SECRET-SENTINEL") {
						t.Fatalf("secret bytes reached diagnostics: %v", err)
					}
					return // refused offline, before any file is read
				}
				// The allow-list walk must refuse this offline; the runtime set
				// is the second line, never the only one.
				checker, ok := n.(instances.Checker)
				if !ok {
					t.Fatal("the webhook notifier does not implement instances.Checker")
				}
				runs := map[string]func() error{
					"Check":  func() error { return checker.Check(context.Background()) },
					"Notify": func() error { return n.Notify(context.Background(), notifiertest.Event()) },
				}
				for name, run := range runs {
					err := run()
					if err == nil {
						t.Errorf("%s accepted a computed secret path", name)
						continue
					}
					if strings.Contains(err.Error(), "SECRET-SENTINEL") {
						t.Errorf("%s leaked the computed path: %v", name, err)
					}
				}
				t.Error("a computed secret path was built; the walk must refuse it offline")
			})
		}
	}
}

// TestARelativeSecretPathIsRefusedOffline: an endpoint template may take only
// an absolute literal path. A literal relative one is the config's own text, so
// naming it is safe — and refusing it offline keeps the runtime reader's
// computed-path branch unreachable from a config file.
func TestARelativeSecretPathIsRefusedOffline(t *testing.T) {
	for _, tc := range []struct {
		name  string
		apply func(cfg map[string]any)
	}{
		{"url", func(cfg map[string]any) { cfg["url"] = `https://notify.example.com/hook/{{ secret "relative/token" }}` }},
		{"header", func(cfg map[string]any) {
			cfg["headers"] = map[string]string{"X-Token": `{{ secret "relative/token" }}`}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := endpointConfig(t, "https://notify.example.com/hook/")
			tc.apply(cfg)
			err := check(t, cfg)
			if err == nil || !strings.Contains(err.Error(), `"relative/token"`) || !strings.Contains(err.Error(), "absolute") {
				t.Fatalf("a literal relative secret path = %v, want an offline refusal naming it", err)
			}
		})
	}
}

// TestEndpointSecretFailuresNameTheFileAndNeverItsBytes: a missing or oversized
// secret is a permanent failure that names the file — never a byte of what a
// file held — and no HTTP request is made.
func TestEndpointSecretFailuresNameTheFileAndNeverItsBytes(t *testing.T) {
	tooBig := endpointSecret(t, "SECRET-SENTINEL"+strings.Repeat("x", 4096))
	cases := []struct {
		name  string
		expr  string
		names string
	}{
		{"missing file", `{{ secret "/nonexistent/endpoint-secret" }}`, "/nonexistent/endpoint-secret"},
		{"oversized file", `{{ secret "` + tooBig + `" }}`, tooBig},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var requests atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				requests.Add(1)
				w.WriteHeader(http.StatusOK)
			}))
			t.Cleanup(srv.Close)
			n := build(t, endpointConfig(t, srv.URL+"/hook/"+tc.expr))
			checker, ok := n.(instances.Checker)
			if !ok {
				t.Fatal("the webhook notifier does not implement instances.Checker")
			}
			err := checker.Check(context.Background())
			if err == nil {
				t.Fatal("Check accepted a secret it cannot read")
			}
			assertSecretFree(t, tc.names, err)
			err = n.Notify(context.Background(), notifiertest.Event())
			if err == nil {
				t.Fatal("Notify accepted a secret it cannot read")
			}
			if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassPermanent {
				t.Errorf("a missing secret is classified %v, want permanent", class)
			}
			if !strings.Contains(err.Error(), "run_finished") || !strings.Contains(err.Error(), tc.names) {
				t.Errorf("the refusal names neither the event nor the file: %v", err)
			}
			assertSecretFree(t, tc.names, err)
			if got := requests.Load(); got != 0 {
				t.Errorf("%d requests were made for an unreadable secret", got)
			}
		})
	}
}

// assertSecretFree proves the sentinel a test file carried never reaches an
// error's text — only its path may.
func assertSecretFree(t *testing.T, path string, err error) {
	t.Helper()
	if strings.Contains(err.Error(), "SECRET-SENTINEL") {
		t.Errorf("the error quotes the secret file's bytes: %v", err)
	}
	if !strings.Contains(err.Error(), path) {
		t.Errorf("the error does not name the file %q: %v", path, err)
	}
}

// TestAHeaderSecretFailureFailsCheckAndNotify: the header set is rendered like
// the url, so a missing header secret is a preflight failure and a permanent
// delivery failure naming the header and the file.
func TestAHeaderSecretFailureFailsCheckAndNotify(t *testing.T) {
	cfg := endpointConfig(t, "http://127.0.0.1:1/hook/")
	cfg["headers"] = map[string]string{"X-Token": `{{ secret "/nonexistent/header-secret" }}`}
	n := build(t, cfg)
	checker, ok := n.(instances.Checker)
	if !ok {
		t.Fatal("the webhook notifier does not implement instances.Checker")
	}
	err := checker.Check(context.Background())
	if err == nil || !strings.Contains(err.Error(), "/nonexistent/header-secret") {
		t.Fatalf("Check with a missing header secret = %v", err)
	}
	err = n.Notify(context.Background(), notifiertest.Event())
	if err == nil || !strings.Contains(err.Error(), "headers/X-Token") || !strings.Contains(err.Error(), "/nonexistent/header-secret") {
		t.Fatalf("Notify with a missing header secret = %v, want the header and the file", err)
	}
}

// TestPlainCheckReturnsTheCancelledContext: preflight is IO-free for a literal
// endpoint, but a context already cancelled is still the caller's answer.
func TestPlainCheckReturnsTheCancelledContext(t *testing.T) {
	n := build(t, baseConfig("http://127.0.0.1:1/never"))
	checker, ok := n.(instances.Checker)
	if !ok {
		t.Fatal("the webhook notifier does not implement instances.Checker")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := checker.Check(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Check with a cancelled context = %v, want context.Canceled", err)
	}
}

// TestTheBodyCannotReadASecret: the secret function belongs to the endpoint
// fields alone. Enabling endpoint templates never widens the payload's or a
// message's function set, so a body cannot turn the run's report into an
// exfiltration of a mounted file.
func TestTheBodyCannotReadASecret(t *testing.T) {
	cfg := endpointConfig(t, "https://notify.example.com/hook/")
	cfg["payload"] = `{{ secret "/etc/shadow" }}`
	if err := check(t, cfg); err == nil || !strings.Contains(err.Error(), `function "secret" not defined`) {
		t.Fatalf("a payload calling secret = %v, want the function to be undefined there", err)
	}
}

// TestEndpointValidationIsOffline: parsing an endpoint template reads no file —
// `validate` stays offline and a secret mounted only in the run's pod does not
// fail it. Rendering, and so the file read, starts at Check.
func TestEndpointValidationIsOffline(t *testing.T) {
	cfg := endpointConfig(t, `https://notify.example.com/hook/{{ secret "/not/mounted/here" }}`)
	cfg["headers"] = map[string]string{"X-Token": `{{ secret "/not/mounted/here" }}`}
	if err := check(t, cfg); err != nil {
		t.Fatalf("validating an endpoint template read the filesystem or refused the reference: %v", err)
	}
	cfg["url"] = `https://notify.example.com/hook/{{ broken`
	cfg["headers"] = map[string]string{}
	if err := check(t, cfg); err == nil || !strings.Contains(err.Error(), "endpoint template is not valid") {
		t.Fatalf("a syntactically broken endpoint template = %v, want an offline refusal", err)
	}
	cfg["url"] = `https://notify.example.com/hook/`
	cfg["headers"] = map[string]string{"X-Token": "{{ broken"}
	if err := check(t, cfg); err == nil || !strings.Contains(err.Error(), "headers/X-Token") {
		t.Fatalf("a broken header template = %v, want a refusal naming the header", err)
	}
}

// TestEndpointAuthorityIsLiteral: an action may only reach the path, query or
// fragment. The exact suffix and userinfo injections are refused at validate,
// before the file is read and before any request: otherwise a secret file would
// choose the host the run's report is posted to.
func TestEndpointAuthorityIsLiteral(t *testing.T) {
	cases := []struct {
		name string
		url  func(file string) string
		want string
	}{
		{
			name: "a suffix injection needs no slash to move the host",
			url:  func(file string) string { return "https://hooks.example.com{{ secret " + quotePath(t, file) + " }}" },
			want: "must end with",
		},
		{
			name: "a userinfo injection needs no slash to move the host",
			url:  func(file string) string { return "https://hooks.example.com{{ secret " + quotePath(t, file) + " }}" },
			want: "must end with",
		},
		{
			name: "userinfo in the literal url",
			url:  func(string) string { return "https://user:pass@hooks.example.com/{{ secret \"/x\" }}" },
			want: "userinfo",
		},
		{
			name: "a literal prefix that is not a URL",
			url:  func(string) string { return "https://hooks.example.com/%zz/{{ secret \"/x\" }}" },
			want: "not a URL",
		},
		{
			name: "no literal host at all",
			url:  func(string) string { return "relative/{{ secret \"/x\" }}" },
			want: "must name a literal host",
		},
	}
	contents := []string{".evil.tld/x", "@169.254.169.254/"}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			file := endpointSecret(t, contents[i%len(contents)])
			var requests atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				requests.Add(1)
				w.WriteHeader(http.StatusOK)
			}))
			t.Cleanup(srv.Close)
			cfg := endpointConfig(t, tc.url(file))
			err := check(t, cfg)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("injected endpoint = %v, want a refusal naming %q", err, tc.want)
			}
			raw, err := json.Marshal(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := instances.Build(context.Background(), testRegistry(t), notifier.Kind, "webhook", raw,
				instances.Deps{Instance: "webhook", Log: slog.New(slog.DiscardHandler)}); err == nil {
				t.Fatal("the injected endpoint was built")
			}
			if got := requests.Load(); got != 0 {
				t.Errorf("%d requests were made for a refused endpoint", got)
			}
		})
	}
}

// TestEndpointActionStaysInThePath: once the authority is literal and
// terminated, the same secret value lands in the path and cannot move the
// request — the receiver on the configured host is the one asked.
func TestEndpointActionStaysInThePath(t *testing.T) {
	file := endpointSecret(t, "@169.254.169.254/")
	var gotPath, gotHost string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotHost = r.URL.Path, r.Host
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	if err := build(t, endpointConfig(t, srv.URL+"/{{ secret "+quotePath(t, file)+" }}")).
		Notify(context.Background(), notifiertest.Event()); err != nil {
		t.Fatal(err)
	}
	if gotHost != strings.TrimPrefix(srv.URL, "http://") {
		t.Errorf("host = %q, want the configured receiver", gotHost)
	}
	if gotPath != "/@169.254.169.254/" {
		t.Errorf("path = %q, want the secret's text as a path segment", gotPath)
	}
}

// TestEndpointHeaderSecretIsRenderedAndRefusedWhenItBreaksTheHeader: a rendered
// header value may not carry a CR, LF or NUL — a secret file is exactly where
// such a byte comes from, and a split header is a header the receiver never
// sees as one.
func TestEndpointHeaderSecretIsRenderedAndRefusedWhenItBreaksTheHeader(t *testing.T) {
	good := endpointSecret(t, "plain-token\r\n")
	var gotHeader string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeader = r.Header.Get("X-Token")
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	cfg := endpointConfig(t, srv.URL+"/")
	cfg["headers"] = map[string]string{"X-Token": "{{ secret " + quotePath(t, good) + " }}"}
	if err := build(t, cfg).Notify(context.Background(), notifiertest.Event()); err != nil {
		t.Fatal(err)
	}
	if gotHeader != "plain-token" {
		t.Errorf("X-Token = %q, want the trimmed secret", gotHeader)
	}

	bad := endpointSecret(t, "split\nheader")
	cfg["headers"] = map[string]string{"X-Token": "{{ secret " + quotePath(t, bad) + " }}"}
	var requests atomic.Int32
	broken := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(broken.Close)
	cfg["url"] = broken.URL + "/"
	n := build(t, cfg)
	err := n.Notify(context.Background(), notifiertest.Event())
	if err == nil || !strings.Contains(err.Error(), "headers/X-Token") {
		t.Fatalf("a header value with a newline = %v, want a refusal naming the header", err)
	}
	if got := requests.Load(); got != 0 {
		t.Errorf("%d requests were made with a broken header", got)
	}
}

// TestEndpointTemplateThatCannotBeRenderedIsPermanent: a template that names
// data the endpoint set does not carry fails at delivery, naming the stage and
// never the executor's own text.
func TestEndpointTemplateThatCannotBeRenderedIsPermanent(t *testing.T) {
	n := build(t, endpointConfig(t, "http://127.0.0.1:1/{{ .NoSuchField }}"))
	err := n.Notify(context.Background(), notifiertest.Event())
	if err == nil {
		t.Fatal("an endpoint template referencing absent data was delivered")
	}
	if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassPermanent {
		t.Errorf("class = %v, want permanent", class)
	}
	if !strings.Contains(err.Error(), "url: the endpoint template cannot be rendered") {
		t.Errorf("the refusal does not name the stage: %v", err)
	}
}

// TestEndpointRenderedURLMustParse: a secret that is not a URL fragment is
// refused before the request, naming the stage.
func TestEndpointRenderedURLMustParse(t *testing.T) {
	file := endpointSecret(t, "%zz")
	n := build(t, endpointConfig(t, "http://127.0.0.1:1/{{ secret "+quotePath(t, file)+" }}"))
	err := n.Notify(context.Background(), notifiertest.Event())
	if err == nil || !strings.Contains(err.Error(), "url: the rendered endpoint is not a URL") {
		t.Fatalf("a rendered URL that does not parse = %v", err)
	}
}
