package webhook_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"encoding/json/jsontext"
	json "encoding/json/v2"

	"go.yaml.in/yaml/v3"

	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/notifier"
	"github.com/vi-latyshev/go-cluster-backup/internal/notifier/notifiertest"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin/plugintest"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/notifier/webhook"
)

func testRegistry(t *testing.T) *plugin.Registry {
	t.Helper()
	r := plugin.NewRegistry()
	if err := webhook.Register(r); err != nil {
		t.Fatal(err)
	}
	return r
}

func build(t *testing.T, cfg map[string]any) notifier.Notifier {
	t.Helper()
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	n, err := plugin.Build(context.Background(), testRegistry(t), notifier.Kind, "webhook", raw,
		plugin.Deps{Instance: "webhook", Log: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// modelPayload writes the plugin's own message model out as YAML, so a test
// reads back exactly what the plugin rendered and sent.
const modelPayload = "title: {{ json .Title }}\nlines:\n{{ range .Lines }}\n" +
	"  - {text: {{ json .Text }}, code: {{ json .Code }}}\n{{ end }}"

// baseConfig is the shape every test starts from: one server, one event and the
// mechanics a deployment states once.
func baseConfig(url string) map[string]any {
	return map[string]any{
		"url": url, "method": "POST",
		"headers":     map[string]string{"Content-Type": "application/json", "X-Instance": "backup"},
		"body_format": "yaml", "timeout": "5s",
		"success_status": []int{200, 202}, "max_response_bytes": 64,
		"events":  map[string]string{"run_finished": "Backup {{ .Run.Outcome }}\n📦 {{ .Run.RunID }}"},
		"payload": modelPayload,
	}
}

// renderedMessage is the message model as the payload wrote it out.
type renderedMessage struct {
	Title string `yaml:"title"`
	Lines []struct {
		Text string `yaml:"text"`
		Code string `yaml:"code"`
	} `yaml:"lines"`
}

// lines is the rendered message as plain strings, code part in place.
func (m renderedMessage) lines() []string {
	out := make([]string, 0, len(m.Lines))
	for _, l := range m.Lines {
		out = append(out, l.Text+l.Code)
	}
	return out
}

// renderOne posts one event through one message template and returns the
// message the plugin built from it: the payload writes the model out as YAML
// and the test reads the body the receiver got.
func renderOne(t *testing.T, message string, ev model.Event) renderedMessage {
	t.Helper()
	cfg := baseConfig("http://127.0.0.1:1/never")
	cfg["body_format"] = "text"
	cfg["events"] = map[string]string{ev.Name: message}
	return posted(t, cfg, ev)
}

// posted runs one event through one configuration against a recording server
// and returns the message the payload wrote out.
func posted(t *testing.T, cfg map[string]any, ev model.Event) renderedMessage {
	t.Helper()
	var body string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, _ := io.ReadAll(r.Body)
		body = string(got)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	cfg["url"] = srv.URL
	if err := build(t, cfg).Notify(context.Background(), ev); err != nil {
		t.Fatalf("Notify with the message template: %v", err)
	}
	var out renderedMessage
	if err := yaml.Unmarshal([]byte(body), &out); err != nil {
		t.Fatalf("the body is not the message model: %v\n%s", err, body)
	}
	return out
}

func TestGeneralConformance(t *testing.T) {
	plugintest.CheckSpec(t, testRegistry(t))
}

// TestNotifierConformance points the plugin's own example at a live test
// server: the example's URL names a receiver a conformance run cannot post to.
func TestNotifierConformance(t *testing.T) {
	notifiertest.Run(t, func(t *testing.T) notifier.Notifier {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		t.Cleanup(srv.Close)
		return build(t, baseConfig(srv.URL))
	})
}

// TestRequestBodyHeadersAndMethod is the request contract: the body is the JSON
// the payload template describes, and the configured headers and method arrive.
func TestRequestBodyHeadersAndMethod(t *testing.T) {
	var gotBody, gotHeader, gotMethod string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		gotBody = string(body)
		gotHeader = r.Header.Get("X-Instance")
		gotMethod = r.Method
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	n := build(t, baseConfig(srv.URL))

	ev := notifiertest.Event()
	ev.Run.Outcome = model.OutcomeDegraded
	if err := n.Notify(context.Background(), ev); err != nil {
		t.Fatal(err)
	}
	var got struct {
		Title string `json:"title"`
		Lines []struct {
			Text string `json:"text"`
		} `json:"lines"`
	}
	if err := json.Unmarshal([]byte(gotBody), &got); err != nil {
		t.Fatalf("the body of a yaml payload is not JSON: %v\n%s", err, gotBody)
	}
	if got.Title != "Backup degraded" {
		t.Errorf("title = %q, want %q", got.Title, "Backup degraded")
	}
	if len(got.Lines) != 1 || got.Lines[0].Text != "📦 20261012T042000Z" {
		t.Errorf("lines = %+v, want one line naming the run", got.Lines)
	}
	if gotHeader != "backup" {
		t.Errorf("X-Instance header = %q, want %q", gotHeader, "backup")
	}
	if gotMethod != "POST" {
		t.Errorf("method = %q, want POST", gotMethod)
	}
}

// TestMaxResponseBytesTakesAUnit pins the field's type: a byte count accepts the
// unit form a deployment writes for a size.
func TestMaxResponseBytesTakesAUnit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, strings.Repeat("x", 4096), http.StatusBadRequest)
	}))
	t.Cleanup(srv.Close)
	cfg := baseConfig(srv.URL)
	cfg["max_response_bytes"] = "1KiB"
	err := build(t, cfg).Notify(context.Background(), notifiertest.Event())
	if err == nil {
		t.Fatal("a 400 was accepted")
	}
	if len(err.Error()) < 1000 {
		t.Errorf("the error carries %d bytes of the response; 1KiB was the limit", len(err.Error()))
	}
}

// TestUnsubscribedEventIsSkipped settles the question the brief left open: an
// event absent from `events` is "not subscribed" and returns success without a
// request, because a notifier is handed every engine event and refusing the
// ones it was never told about would fail runs for no reason.
func TestUnsubscribedEventIsSkipped(t *testing.T) {
	var requests int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests++
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	n := build(t, baseConfig(srv.URL))

	ev := notifiertest.Event()
	ev.Name = "run_started"
	if err := n.Notify(context.Background(), ev); err != nil {
		t.Fatalf("an unsubscribed event returned %v, want nil", err)
	}
	if requests != 0 {
		t.Errorf("an unsubscribed event made %d requests, want 0", requests)
	}
}

// TestStatusClasses pins the classification the engine's retry loop reads.
func TestStatusClasses(t *testing.T) {
	cases := map[string]struct {
		status int
		want   plugin.Class
	}{
		"accepted 200":     {http.StatusOK, ""},
		"accepted 202":     {http.StatusAccepted, ""},
		"rate limited 429": {http.StatusTooManyRequests, plugin.ClassTransient},
		"server 500":       {http.StatusInternalServerError, plugin.ClassTransient},
		"server 503":       {http.StatusServiceUnavailable, plugin.ClassTransient},
		"bad request 400":  {http.StatusBadRequest, plugin.ClassPermanent},
		"unauthorized 401": {http.StatusUnauthorized, plugin.ClassPermanent},
		"not found 404":    {http.StatusNotFound, plugin.ClassPermanent},
		"redirect 302":     {http.StatusFound, plugin.ClassPermanent},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				http.Error(w, "receiver says "+http.StatusText(tc.status), tc.status)
			}))
			t.Cleanup(srv.Close)
			err := build(t, baseConfig(srv.URL)).Notify(context.Background(), notifiertest.Event())
			if tc.want == "" {
				if err != nil {
					t.Fatalf("Notify: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("Notify accepted a status outside success_status")
			}
			if class, _ := plugin.ClassOf(err); class != tc.want {
				t.Errorf("HTTP %d classified %q, want %q", tc.status, class, tc.want)
			}
		})
	}
}

// TestResponseBodyIsBounded checks the two properties of a failing receiver's
// body: a bounded amount of it may reach the error, and none of it reaches the log.
func TestResponseBodyIsBounded(t *testing.T) {
	const filler = "SECRET-RECEIVER-INTERNAL-DUMP"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, strings.Repeat(filler, 1000))
	}))
	t.Cleanup(srv.Close)
	var logged strings.Builder
	raw, err := json.Marshal(baseConfig(srv.URL))
	if err != nil {
		t.Fatal(err)
	}
	n, err := plugin.Build(context.Background(), testRegistry(t), notifier.Kind, "webhook", raw,
		plugin.Deps{Instance: "webhook", Log: slog.New(slog.NewTextHandler(&logged, nil))})
	if err != nil {
		t.Fatal(err)
	}
	err = n.Notify(context.Background(), notifiertest.Event())
	if err == nil {
		t.Fatal("a 400 was accepted")
	}
	// max_response_bytes is 64; the receiver sent 32000 bytes, so the error
	// may carry 64 of them and no more.
	const maxResponseBytes = 64
	if got := strings.Count(err.Error(), filler) * len(filler); got > maxResponseBytes {
		t.Errorf("the error carries %d bytes of the response body, want at most %d", got, maxResponseBytes)
	}
	if len(err.Error()) > 512 {
		t.Errorf("the error is %d bytes long; a failing receiver must not be able to fill the log", len(err.Error()))
	}
	if logged.String() != "" {
		t.Errorf("Notify wrote to the logger on a failure: %s", logged.String())
	}
}

// TestTimeoutIsTransient is the timeout path: a receiver slower than the
// configured budget fails the attempt, and the engine may try again.
func TestTimeoutIsTransient(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-release
	}))
	t.Cleanup(func() { close(release); srv.Close() })
	cfg := baseConfig(srv.URL)
	cfg["timeout"] = "200ms"
	start := time.Now()
	err := build(t, cfg).Notify(context.Background(), notifiertest.Event())
	if err == nil {
		t.Fatal("Notify against a receiver past the timeout returned success")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("Notify took %s, so the configured timeout did not apply", elapsed)
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassTransient {
		t.Errorf("timeout classified %q, want transient", class)
	}
}

// TestMessageThatRendersForOneRunOnlyIsRefused: the matrix is what makes a
// message written for one night safe for the others. This title exists only
// when the previous run never reported — a real night, and the one the single
// old sample happened to be — while every delivered, degraded or failed night
// would render no title at all and lose the message.
func TestMessageThatRendersForOneRunOnlyIsRefused(t *testing.T) {
	r := testRegistry(t)
	raw, err := json.Marshal(map[string]any{
		"url": "http://127.0.0.1:1/x", "method": "POST", "headers": map[string]string{},
		"body_format": "text", "timeout": "1s", "success_status": []int{200}, "max_response_bytes": 16,
		"events": map[string]string{
			"run_finished": "{{ if .Run.Previous.Unfinished }}The previous run never reported{{ end }}",
		},
		"payload": "{{ .Text }}",
	})
	if err != nil {
		t.Fatal(err)
	}
	err = r.Check("notifier", "webhook", raw)
	if err == nil {
		t.Fatal("a message that renders for one run only passed validate")
	}
	for _, want := range []string{"run_finished", "a delivered run"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not name %q: %v", want, err)
		}
	}
}

// TestMissingFieldAndKeyFailOffline is the missingkey=error rule, and the whole
// point of rendering every message while validating: a name the report does not
// have is refused before the first night, not on it.
func TestMissingFieldAndKeyFailOffline(t *testing.T) {
	r := testRegistry(t)
	cases := map[string]string{
		"a field":             "{{ .Run.NoSuchField }}",
		"a key of a map":      `{{ index .Run "no_such_key" }}`,
		"a field of an entry": "{{ range .Run.Artifacts }}{{ .NoSuchField }}{{ end }}",
	}
	for name, message := range cases {
		t.Run(name, func(t *testing.T) {
			raw, err := json.Marshal(map[string]any{
				"url": "http://127.0.0.1:1/x", "method": "POST", "headers": map[string]string{},
				"body_format": "text", "timeout": "1s", "success_status": []int{200}, "max_response_bytes": 16,
				"events": map[string]string{"run_finished": message}, "payload": "{{ .Text }}",
			})
			if err != nil {
				t.Fatal(err)
			}
			err = r.Check("notifier", "webhook", raw)
			if err == nil {
				t.Fatal("a message naming something the report does not have passed validate")
			}
			if !strings.Contains(err.Error(), "run_finished") {
				t.Errorf("the refusal does not name the event: %v", err)
			}
		})
	}
}

// TestBadTemplateIsRejectedAtDecodeTime: syntax is checked by the framework at
// decode time, so `validate` never lets a broken template reach the network.
func TestBadTemplateIsRejectedAtDecodeTime(t *testing.T) {
	raw := jsontext.Value(`{"url": "http://127.0.0.1:1/x", "method": "POST", "headers": {}, "body_format": "text", ` +
		`"timeout": "1s", "success_status": [200], "max_response_bytes": 16, ` +
		`"events": {"run_finished": "{{ .Run"}, "payload": "x"}`)
	if err := testRegistry(t).Check("notifier", "webhook", raw); err == nil {
		t.Fatal("a template with a syntax error passed validate")
	}
}

// runStarted and runFinished are the message templates of the research report's
// §1 configuration: plain text, one message line per template line, a code span
// ending its line, and action-only lines that render empty — which the message
// model drops, so the author writes no trim markers at all.
const runStarted = `🚀 Backup started
📦 {{ .Run.RunID }}
{{ if .Run.Previous.Unfinished }}
⚠️ Previous run did not report
🕐 Last success: {{ date .Run.Previous.LastGoodAt }}
{{ end }}`

const runFinished = `{{ pick .Run.Outcome "delivered" "✅ Backup completed" "degraded" "⚠️ Backup DEGRADED" "failed" "❌ Backup FAILED" }}
📦 {{ .Run.RunID }}
⏱ Duration: ` + "`{{ duration .Run.Duration }}`" + `
{{ range .Run.Artifacts }}
{{ pick .Outcome "ok" "✓" "failed" "✗" }} {{ .Artifact.Name }}: ` + "`{{ bytes .Bytes }} in {{ duration .Duration }}`" + `
{{ if or .Stats.SkippedEntries .Stats.PaddedFiles }}
   ↳ skipped {{ .Stats.SkippedEntries }}, padded {{ .Stats.PaddedFiles }}
{{ end }}
{{ end }}
{{ range .Run.Storages }}
{{ pick .Outcome "verified" "☁️" "failed" "⚠️" "skipped" "⚠️" }} {{ .Storage }}: ` + "`{{ .Outcome }}, {{ bytes .Bytes }}, kept {{ .Kept }}, deleted {{ len .Deleted }}`" + `
{{ end }}
{{ range .Run.Excluded }}{{ if eq .Kind "ignored" }}
🚫 ignored {{ .Name }}: {{ .Reason }}
{{ end }}{{ end }}`

// TestMessageModelRendersThePlansMessages renders both §1 message templates
// against a real finished run and pins the whole model: the title is the first
// line, the lines that rendered empty are gone, a kept line keeps its leading
// spaces, and a markup span is the line's code part whatever the line's text.
func TestMessageModelRendersThePlansMessages(t *testing.T) {
	ev := notifiertest.Event()
	ev.Run.Outcome = model.OutcomeDegraded
	ev.Run.Duration = model.Duration(5 * time.Minute)
	ev.Run.Artifacts = []model.ArtifactResult{
		{
			Artifact: model.Artifact{Name: "databases/prod/postgres", Group: "databases"},
			Outcome:  model.ArtifactOK, Bytes: 1 << 20, Duration: model.Duration(10 * time.Second),
			Stats: model.DumpStats{SkippedEntries: 2, PaddedFiles: 1},
		},
		{
			Artifact: model.Artifact{Name: "volumes/prod/data", Group: "volumes"},
			Outcome:  model.ArtifactFailed, Duration: model.Duration(time.Second),
		},
	}
	ev.Run.Storages = []model.StorageResult{
		{Storage: "yandex", Outcome: model.StorageVerified, Bytes: 3 << 20, Kept: 7, Deleted: []string{"a", "b"}},
	}
	ev.Run.Previous = model.PreviousRun{
		RunID: "20261011T042000Z", Unfinished: true,
		LastGoodAt: time.Date(2026, 10, 10, 4, 20, 0, 0, time.UTC),
	}

	t.Run("run_started", func(t *testing.T) {
		got := renderOne(t, runStarted, ev)
		if got.Title != "🚀 Backup started" {
			t.Errorf("title = %q, want the first line", got.Title)
		}
		// The unfinished previous run makes the guarded block render; the blank
		// line it opens with is dropped.
		if !slices.Contains(got.lines(), "📦 20261012T042000Z") {
			t.Errorf("the run id is missing: %q", got.lines())
		}
		if !slices.Contains(got.lines(), "⚠️ Previous run did not report") {
			t.Errorf("the unfinished previous run is not mentioned: %q", got.lines())
		}
		if !slices.Contains(got.lines(), "🕐 Last success: 2026-10-10T04:20:00Z") {
			t.Errorf("the last good run is not mentioned: %q", got.lines())
		}
		for _, line := range got.lines() {
			if strings.TrimSpace(line) == "" {
				t.Errorf("a rendered-empty line reached the message: %q", got.lines())
			}
		}
	})

	t.Run("run_finished", func(t *testing.T) {
		got := renderOne(t, runFinished, ev)
		if got.Title != "⚠️ Backup DEGRADED" {
			t.Errorf("title = %q, want the degraded wording", got.Title)
		}
		if got.Lines[0].Text != "📦 20261012T042000Z" {
			t.Errorf("first line = %q", got.Lines[0].Text)
		}
		if got.Lines[1].Text != "⏱ Duration: " || got.Lines[1].Code != "5m0s" {
			t.Errorf("duration line = %+v, want its text and its code part", got.Lines[1])
		}
		if got.Lines[2].Text != "✓ databases/prod/postgres: " || got.Lines[2].Code != "1.0 MiB in 10s" {
			t.Errorf("artifact line = %+v, want the ok mark and the code part", got.Lines[2])
		}
		// The artifact that reported nothing gets no indented line; the one
		// that did keeps the indentation of its own template line.
		if got.Lines[3].Text != "   ↳ skipped 2, padded 1" {
			t.Errorf("stats line = %+v, want the indentation kept", got.Lines[3])
		}
		if got.Lines[4].Text != "✗ volumes/prod/data: " {
			t.Errorf("failed artifact line = %+v, want the failed mark", got.Lines[4])
		}
		if !strings.HasPrefix(got.Lines[5].Text, "☁️ yandex: ") || !strings.Contains(got.Lines[5].Code, "kept 7, deleted 2") {
			t.Errorf("storage line = %+v, want the retention numbers", got.Lines[5])
		}
		if got.Lines[6].Text != "🚫 ignored "+ev.Run.Excluded[0].Name+": "+ev.Run.Excluded[0].Reason {
			t.Errorf("exclusion line = %+v, want the ignored object named", got.Lines[6])
		}
		if len(got.Lines) != 7 {
			t.Errorf("lines = %q, want the covered exclusion to leave no line and no line empty", got.lines())
		}
	})
}

// TestBackticksFromDataAreNotMarkup is the rule that keeps a name or a reason
// verbatim: a backtick arriving from data is a character of that data, and only
// the template's own markup backticks split a line.
func TestBackticksFromDataAreNotMarkup(t *testing.T) {
	ev := notifiertest.Event()
	ev.Run.Artifacts[0].Artifact.Name = "databases/we`ird"
	ev.Run.Excluded[0].Reason = "the marker `backup.example/ignore` is set"
	const message = "Artifact: {{ range .Run.Artifacts }}{{ .Artifact.Name }}{{ end }}\n" +
		"Why: {{ range .Run.Excluded }}{{ .Kind }} {{ .Reason }}\n{{ end }}"
	got := renderOne(t, message, ev)
	if got.Title != "Artifact: databases/we`ird" {
		t.Errorf("a backtick from a name became markup: %q", got.Title)
	}
	if got.Lines[0].Text != "Why: ignored the marker `backup.example/ignore` is set" || got.Lines[0].Code != "" {
		t.Errorf("a backtick from a reason became markup: %+v", got.Lines[0])
	}
	if got.Lines[1].Code != "" {
		t.Errorf("the second exclusion's line carries a code part: %+v", got.Lines[1])
	}
}

// TestSpanRuleIsRefusedOffline pins the three ways a template breaks the line
// model, and that the refusal names the event and the line.
func TestSpanRuleIsRefusedOffline(t *testing.T) {
	cases := map[string]struct {
		message string
		want    []string
	}{
		"one backtick": {
			message: "Title\none ` backtick\n",
			want:    []string{"events/run_finished", "line 2", "one"},
		},
		"text after the span": {
			message: "Title\n`code` and more\n",
			want:    []string{"events/run_finished", "line 2", "and more"},
		},
		"two spans on a line": {
			message: "Title\n`a` `b`\n",
			want:    []string{"events/run_finished", "line 2", "4 backticks"},
		},
	}
	r := testRegistry(t)
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			raw, err := json.Marshal(map[string]any{
				"url": "http://127.0.0.1:1/x", "method": "POST", "headers": map[string]string{},
				"body_format": "text", "timeout": "1s", "success_status": []int{200}, "max_response_bytes": 16,
				"events":  map[string]string{"run_finished": tc.message},
				"payload": "{{ .Text }}",
			})
			if err != nil {
				t.Fatal(err)
			}
			err = r.Check("notifier", "webhook", raw)
			if err == nil {
				t.Fatal("a template breaking the line model passed validate")
			}
			for _, want := range tc.want {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("the refusal does not name %q: %v", want, err)
				}
			}
			if name == "one backtick" && strings.Contains(err.Error(), "line 1") {
				t.Errorf("the refusal names the wrong line: %v", err)
			}
		})
	}
}

// TestSpanOverALineBreakInDataRendersOneLine: the model has one code part per
// line, so a line break arriving INSIDE data — an artifact's error, which
// errors.Join writes as several sentences, one per line — is folded to a space
// rather than tearing the span in two. Tearing it would refuse the whole render
// and lose the night's run_finished message, which is the worst outcome
// available; a folded space is what the reader would have seen anyway.
func TestSpanOverALineBreakInDataRendersOneLine(t *testing.T) {
	var bodies []string
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, string(b))
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	cfg := baseConfig(srv.URL)
	cfg["events"] = map[string]string{"run_finished": "📦 `{{ .Run.RunID }}`\ndone"}
	ev := notifiertest.Event()
	ev.Run.RunID = "20261012T042000Z\ninjected"
	if err := build(t, cfg).Notify(context.Background(), ev); err != nil {
		t.Fatalf("a line break in data must not lose the message: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(bodies) != 1 {
		t.Fatalf("made %d requests, want 1", len(bodies))
	}
	if strings.Contains(bodies[0], "injected\n") || strings.Contains(bodies[0], "\\n") {
		t.Errorf("the line break survived into the body: %s", bodies[0])
	}
	if !strings.Contains(bodies[0], "injected") {
		t.Errorf("the data past the break was dropped: %s", bodies[0])
	}
}

// TestSpanAcrossTemplateLinesIsRefused is the other half: a break the TEMPLATE
// itself puts inside a span is an author's error, not data, and the config is
// refused at decode — before any night runs on it.
func TestSpanAcrossTemplateLinesIsRefused(t *testing.T) {
	cfg := baseConfig("http://127.0.0.1:1/never")
	cfg["events"] = map[string]string{"run_finished": "📦 `{{ .Run.RunID }}\ninjected`\ndone"}
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	_, err = plugin.Build(context.Background(), testRegistry(t), notifier.Kind, "webhook", raw,
		plugin.Deps{Instance: "webhook", Log: slog.New(slog.DiscardHandler)})
	if err == nil {
		t.Fatal("a span the template itself splits across lines was accepted")
	}
	if !strings.Contains(err.Error(), "code span") {
		t.Errorf("the refusal does not name the code span: %v", err)
	}
}

// TestPayloadSerializingTheEventSurvivesDirtyData: a payload may serialize the
// whole event (`event: {{ json .Event }}`), and the event's strings come from
// the cluster — an artifact error can carry a byte that is not valid UTF-8. One
// cleaned copy must reach BOTH templates: cleaning only inside the message
// render left `.Event` dirty, the JSON encoder refused it, and the notification
// failed permanently instead of being sent with the byte repaired.
func TestPayloadSerializingTheEventSurvivesDirtyData(t *testing.T) {
	var body string
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		body = string(b)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	cfg := baseConfig(srv.URL)
	cfg["payload"] = "title: {{ json .Title }}\nevent: {{ json .Event }}\n"
	ev := notifiertest.Event()
	ev.Run.Artifacts[0].Error = "bad\xffbyte"
	if err := build(t, cfg).Notify(context.Background(), ev); err != nil {
		t.Fatalf("a byte the cluster sent lost the notification: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	var doc map[string]any
	if err := json.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatalf("the body is not the document its template promised: %v\n%s", err, body)
	}
	if _, ok := doc["event"]; !ok {
		t.Errorf("the payload did not carry the event: %s", body)
	}
}

// TestStatsLineRendersSkippedAndPadded: what an artifact is NOT a copy of —
// entries the source left out, files it padded — stays visible in the
// notification, not only in the log. Zero says nothing and renders no line.
func TestStatsLineRendersSkippedAndPadded(t *testing.T) {
	var body string
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		body = string(b)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	cfg := baseConfig(srv.URL)
	cfg["events"] = map[string]string{"run_finished": "backup\n" +
		"{{ range .Run.Artifacts }}{{ .Artifact.Name }}{{ if or .Stats.SkippedEntries .Stats.PaddedFiles }} ↳ skipped {{ .Stats.SkippedEntries }}, padded {{ .Stats.PaddedFiles }}{{ end }}\n{{ end }}"}
	ev := notifiertest.Event()
	ev.Run.Artifacts[0].Stats = model.DumpStats{SkippedEntries: 2, PaddedFiles: 1}
	if err := build(t, cfg).Notify(context.Background(), ev); err != nil {
		t.Fatalf("notify: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if !strings.Contains(body, "skipped 2, padded 1") {
		t.Errorf("the body does not state what the artifact left out: %s", body)
	}
}

// TestPayloadSeesTitleLinesTextAndEvent is the payload contract: the four
// values a body can be built from, with the whole message as one string and the
// engine event as it arrived.
func TestPayloadSeesTitleLinesTextAndEvent(t *testing.T) {
	cfg := baseConfig("http://127.0.0.1:1/never")
	cfg["body_format"] = "text"
	cfg["payload"] = "title={{ .Title }}\nlines={{ len .Lines }}\ntext={{ .Text }}\nevent={{ .Event.Name }}/{{ .Event.Run.RunID }}"
	ev := notifiertest.Event()
	var body string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, _ := io.ReadAll(r.Body)
		body = string(got)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	cfg["url"] = srv.URL
	if err := build(t, cfg).Notify(context.Background(), ev); err != nil {
		t.Fatal(err)
	}
	want := "title=Backup delivered\n" +
		"lines=1\n" +
		"text=Backup delivered\n📦 20261012T042000Z\n" +
		"event=run_finished/20261012T042000Z"
	if body != want {
		t.Errorf("body =\n%s\nwant\n%s", body, want)
	}
}

// TestAnyReceiverIsServed keeps the promise in the package doc: a receiver that
// takes one string is served by a one-line payload, and a receiver that wants a
// document of its own is served by building it from the event.
func TestAnyReceiverIsServed(t *testing.T) {
	cases := map[string]struct {
		format  string
		payload string
		want    string
	}{
		"a receiver taking one string": {
			format:  "text",
			payload: `text: {{ json .Text }}`,
			want:    `text: "Backup delivered\n📦 20261012T042000Z"`,
		},
		"a body built from the event": {
			format:  "text",
			payload: `{"outcome":{{ json .Event.Run.Outcome }},"artifacts":{{ len .Event.Run.Artifacts }}}`,
			want:    `{"outcome":"delivered","artifacts":1}`,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			var body string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				got, _ := io.ReadAll(r.Body)
				body = string(got)
				w.WriteHeader(http.StatusOK)
			}))
			t.Cleanup(srv.Close)
			cfg := baseConfig(srv.URL)
			cfg["body_format"] = tc.format
			cfg["payload"] = tc.payload
			if err := build(t, cfg).Notify(context.Background(), notifiertest.Event()); err != nil {
				t.Fatal(err)
			}
			if body != tc.want {
				t.Errorf("body = %q, want %q", body, tc.want)
			}
		})
	}
}

// TestYAMLBodyDropsEmptyLinesAndSendsJSON pins both halves of the yaml format:
// the rendered payload is read as a YAML document — the lines that rendered
// empty never reach it, which is what a literal block scalar of the body sees —
// and what the receiver gets is that document as JSON.
func TestYAMLBodyDropsEmptyLinesAndSendsJSON(t *testing.T) {
	var body string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, _ := io.ReadAll(r.Body)
		body = string(got)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	cfg := baseConfig(srv.URL)
	cfg["body_format"] = "yaml"
	// The template as a YAML block scalar writes it: the action-only lines are
	// flush with the document, so they render empty and the author needs no
	// trim markers to keep them out of the body.
	cfg["payload"] = "title: {{ json .Title }}\nnote: |\n  first\n{{ if .Lines }}\n  second\n{{ end }}\n" +
		"lines:\n{{ range .Lines }}\n  - {text: {{ json .Text }}, code: {{ json .Code }}}\n{{ end }}"
	if err := build(t, cfg).Notify(context.Background(), notifiertest.Event()); err != nil {
		t.Fatal(err)
	}
	var got struct {
		Title string `json:"title"`
		Note  string `json:"note"`
		Lines []struct {
			Text string `json:"text"`
			Code string `json:"code"`
		} `json:"lines"`
	}
	if err := json.Unmarshal([]byte(body), &got); err != nil {
		t.Fatalf("the yaml body is not sent as JSON: %v\n%s", err, body)
	}
	if got.Note != "first\nsecond\n" {
		t.Errorf("the block scalar = %q, want the empty rendered lines dropped", got.Note)
	}
	if got.Title != "Backup delivered" || len(got.Lines) != 1 {
		t.Errorf("body = %+v", got)
	}
}

// TestValidateRendersEveryEventOffline is the defect the plain-text model
// closes: a body that only ever renders at run time is checked here instead,
// against the built-in sample — an event whose title renders empty, and one
// naming a field the report does not have, both fail `validate --config` with
// no network at all.
func TestValidateRendersEveryEventOffline(t *testing.T) {
	r := testRegistry(t)
	cases := map[string]struct {
		events map[string]string
		want   string
	}{
		"a title that renders empty": {
			events: map[string]string{"run_started": "{{ if .Run.Previous.Unfinished }}{{ end }}"},
			want:   "renders to no line",
		},
		"a field the report does not have": {
			events: map[string]string{"run_finished": "Backup {{ .Run.NoSuchField }}"},
			want:   "NoSuchField",
		},
		"an event the engine never sends": {
			events: map[string]string{"run_finshed": "Backup {{ .Run.Outcome }}"},
			want:   "run_finshed",
		},
		"a pick missing an outcome": {
			events: map[string]string{"run_finished": `{{ pick .Run.Outcome "degraded" "⚠️ DEGRADED" "failed" "❌ FAILED" }}`},
			want:   "a delivered run",
		},
		"a second event is rendered too": {
			events: map[string]string{
				"run_started":  "🚀 Backup started",
				"run_finished": "{{ .Run.Artifact }}",
			},
			want: "run_finished",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			raw, err := json.Marshal(map[string]any{
				"url": "http://127.0.0.1:1/x", "method": "POST", "headers": map[string]string{},
				"body_format": "text", "timeout": "1s", "success_status": []int{200}, "max_response_bytes": 16,
				"events": tc.events, "payload": "{{ .Text }}",
			})
			if err != nil {
				t.Fatal(err)
			}
			err = r.Check("notifier", "webhook", raw)
			if err == nil {
				t.Fatal("the config passed validate")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("the refusal does not name %q: %v", tc.want, err)
			}
		})
	}
}

// TestValidateRefusesABodyThatIsNotItsFormat: the payload is rendered offline
// like every message, so a body that is not the document its format promises
// fails `validate --config` rather than a night, and a body that renders to
// nothing is refused in both formats.
func TestValidateRefusesABodyThatIsNotItsFormat(t *testing.T) {
	cases := map[string]struct {
		format  string
		payload string
		want    string
	}{
		"yaml that does not parse": {
			format:  "yaml",
			payload: "text: [unclosed\n",
			want:    "not a YAML document",
		},
		"yaml that renders to nothing": {
			format:  "yaml",
			payload: "{{ if .Event.Name }}{{ end }}",
			want:    "renders to nothing",
		},
		"text that renders to nothing": {
			format:  "text",
			payload: "  ",
			want:    "renders to nothing",
		},
	}
	r := testRegistry(t)
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			raw, err := json.Marshal(map[string]any{
				"url": "http://127.0.0.1:1/x", "method": "POST", "headers": map[string]string{},
				"body_format": tc.format, "timeout": "1s", "success_status": []int{200}, "max_response_bytes": 16,
				"events": map[string]string{"run_finished": "Backup {{ .Run.Outcome }}"}, "payload": tc.payload,
			})
			if err != nil {
				t.Fatal(err)
			}
			err = r.Check("notifier", "webhook", raw)
			if err == nil {
				t.Fatal("the config passed validate")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("the refusal does not name %q: %v", tc.want, err)
			}
		})
	}
}

// TestConfigValidation pins what `validate` rejects, so no default can creep in:
// the deployment states the endpoint, the message of every event and the body, and
// every mechanic is checked for what a request needs.
func TestConfigValidation(t *testing.T) {
	valid := func() string {
		raw, err := json.Marshal(baseConfig("http://127.0.0.1:1/x"))
		if err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}
	const rest = `"method": "POST", "headers": {}, "body_format": "text", "timeout": "1s", ` +
		`"success_status": [200], "max_response_bytes": 16, "payload": "{{ .Text }}"`
	cases := map[string]string{
		"no url":                  `{` + rest + `, "events": {}}`,
		"non-http url":            `{"url": "ftp://x/", ` + rest + `, "events": {}}`,
		"bad method":              `{"url": "http://x/", "method": "TRACE", ` + `"headers": {}, "body_format": "text", "timeout": "1s", "success_status": [200], "max_response_bytes": 16, "payload": "{{ .Text }}", "events": {}}`,
		"bad body format":         `{"url": "http://x/", "method": "POST", "headers": {}, "body_format": "json", "timeout": "1s", "success_status": [200], "max_response_bytes": 16, "payload": "{{ .Text }}", "events": {}}`,
		"no timeout":              `{"url": "http://x/", "method": "POST", "headers": {}, "body_format": "text", "success_status": [200], "max_response_bytes": 16, "payload": "{{ .Text }}", "events": {}}`,
		"empty success_status":    `{"url": "http://x/", "method": "POST", "headers": {}, "body_format": "text", "timeout": "1s", "success_status": [], "max_response_bytes": 16, "payload": "{{ .Text }}", "events": {}}`,
		"success status not http": `{"url": "http://x/", "method": "POST", "headers": {}, "body_format": "text", "timeout": "1s", "success_status": [999], "max_response_bytes": 16, "payload": "{{ .Text }}", "events": {}}`,
		"success status twice":    `{"url": "http://x/", "method": "POST", "headers": {}, "body_format": "text", "timeout": "1s", "success_status": [200, 200], "max_response_bytes": 16, "payload": "{{ .Text }}", "events": {}}`,
		"zero max response":       `{"url": "http://x/", "method": "POST", "headers": {}, "body_format": "text", "timeout": "1s", "success_status": [200], "max_response_bytes": 0, "payload": "{{ .Text }}", "events": {}}`,
		"max response too large":  `{"url": "http://x/", "method": "POST", "headers": {}, "body_format": "text", "timeout": "1s", "success_status": [200], "max_response_bytes": "2MiB", "payload": "{{ .Text }}", "events": {}}`,
		"no events":               `{"url": "http://x/", "method": "POST", "headers": {}, "body_format": "text", "timeout": "1s", "success_status": [200], "max_response_bytes": 16, "payload": "{{ .Text }}"}`,
		"an empty message":        `{"url": "http://x/", "method": "POST", "headers": {}, "body_format": "text", "timeout": "1s", "success_status": [200], "max_response_bytes": 16, "payload": "{{ .Text }}", "events": {"run_started": ""}}`,
	}
	r := testRegistry(t)
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			if err := r.Check("notifier", "webhook", jsontext.Value(raw)); err == nil {
				t.Fatal("config was accepted")
			}
		})
	}
	t.Run("the baseline config is valid", func(t *testing.T) {
		if err := r.Check("notifier", "webhook", jsontext.Value(valid())); err != nil {
			t.Fatalf("the config every other case is derived from does not validate: %v", err)
		}
	})
}

// TestReferenceConfigFinishedMessageRenders: the reference configuration carries
// a run_finished message the docs point at, and
// `validate` only renders it against a sample. Rendering it against a real
// report is what proves the message it promises — the artifact lines, the
// skipped/padded counts, the objects the run deliberately did not back up.
func TestReferenceConfigFinishedMessageRenders(t *testing.T) {
	data, err := os.ReadFile("../../../../cmd/go-cluster-backup/testdata/config.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Notifiers map[string]struct {
			Config struct {
				Events  map[string]string `yaml:"events"`
				Payload string            `yaml:"payload"`
			} `yaml:"config"`
		} `yaml:"notifiers"`
	}
	if err := yaml.Unmarshal(data, &doc); err != nil {
		t.Fatal(err)
	}
	var message, payload string
	for _, n := range doc.Notifiers {
		message, payload = n.Config.Events["run_finished"], n.Config.Payload
	}
	if message == "" {
		t.Fatal("the reference config carries no run_finished message")
	}
	if payload == "" {
		t.Fatal("the reference config carries no payload")
	}
	ev := notifiertest.Event()
	ev.Run.Artifacts[0].Stats = model.DumpStats{SkippedEntries: 2, PaddedFiles: 1}
	var body string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, _ := io.ReadAll(r.Body)
		body = string(got)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	cfg := baseConfig(srv.URL)
	cfg["events"] = map[string]string{"run_finished": message}
	cfg["payload"] = payload
	if err := build(t, cfg).Notify(context.Background(), ev); err != nil {
		t.Fatalf("the reference config's message does not render: %v", err)
	}
	var got struct {
		Title string `json:"title"`
		Lines []struct {
			Text string `json:"text"`
			Code string `json:"code"`
		} `json:"lines"`
	}
	if err := json.Unmarshal([]byte(body), &got); err != nil {
		t.Fatalf("the reference message is not JSON: %v\n%s", err, body)
	}
	all := got.Title + "\n" + strings.Join(renderedLines(got.Lines), "\n")
	for _, want := range []string{
		ev.Run.Artifacts[0].Artifact.Name, // the artifact's own line
		"2 left out, 1 padded",            // what the source reported
		ev.Run.Excluded[0].Name,           // an object the run did not back up
	} {
		if !strings.Contains(all, want) {
			t.Errorf("the reference message does not mention %q:\n%s", want, all)
		}
	}
}

func renderedLines(lines []struct {
	Text string `json:"text"`
	Code string `json:"code"`
}) []string {
	out := make([]string, 0, len(lines))
	for _, l := range lines {
		out = append(out, l.Text+l.Code)
	}
	return out
}

// TestDataCannotBreakTheMessage: every byte a cluster name or a reason can
// carry reaches the message as text a receiver can read. The marker bytes this
// plugin splits a code span with are the sharp case — read as markup they tear
// the line in two — and an invalid byte or a control character is what makes the
// body's YAML document unwritable, which would cost the whole night's message.
func TestDataCannotBreakTheMessage(t *testing.T) {
	ev := notifiertest.Event()
	ev.Run.Artifacts[0].Artifact.Name = "databases/br\x01oken\x02n\x00\xff"
	ev.Run.Excluded = ev.Run.Excluded[:1]
	ev.Run.Excluded[0].Reason = "the marker \x07 is set"
	got := renderOne(t, "Artifact: `{{ range .Run.Artifacts }}{{ .Artifact.Name }}{{ end }}`\n"+
		"Why: {{ range .Run.Excluded }}{{ .Reason }}{{ end }}\ndone", ev)

	if got.Title != "Artifact: databases/br�oken�n��" {
		t.Errorf("title = %q, want the data cleaned and the span intact", got.Title)
	}
	if strings.ContainsAny(got.Title+got.Lines[0].Text+got.Lines[0].Code, "\x01\x02\x00\x07") {
		t.Errorf("a marker or control byte reached the message: %q %+v", got.Title, got.Lines)
	}
	if got.Lines[0].Text != "Why: the marker � is set" {
		t.Errorf("reason line = %+v, want the control byte replaced", got.Lines[0])
	}
	if got.Lines[1].Text != "done" {
		t.Errorf("the message lost its last line: %+v", got.Lines)
	}
}

// TestBodyIsDeterministic: two deliveries of one night are one message. The
// body is a JSON encoding of a parsed document, and a receiver that dedups — or
// an operator comparing two nights — must not see the keys move.
func TestBodyIsDeterministic(t *testing.T) {
	var bodies []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, _ := io.ReadAll(r.Body)
		bodies = append(bodies, string(got))
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	cfg := baseConfig(srv.URL)
	cfg["payload"] = "one: {{ json .Title }}\ntwo: 2\nthree: 3\nfour: 4\nfive: 5\nsix: 6\nseven: 7\neight: 8\nnine: 9\nten: 10\n"
	n := build(t, cfg)
	for i := 0; i < 5; i++ {
		if err := n.Notify(context.Background(), notifiertest.Event()); err != nil {
			t.Fatal(err)
		}
	}
	for i, body := range bodies {
		if body != bodies[0] {
			t.Fatalf("delivery %d differs from the first:\n%s\n%s", i, body, bodies[0])
		}
	}
}

// TestNullHeaderIsRefused: `X-Instance:` with its value left empty is a null,
// and the null walk covers map values of every element type, not only struct
// fields — a header the file wrote as an empty value must be refused, never
// read as an absent member and sent as something the deployment never stated.
func TestNullHeaderIsRefused(t *testing.T) {
	cfg := baseConfig("http://127.0.0.1:1/never")
	cfg["headers"] = map[string]any{"Content-Type": "application/json", "X-Instance": nil}
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	err = testRegistry(t).Check("notifier", "webhook", raw)
	if err == nil {
		t.Fatal("a header written as null passed the check")
	}
	if want := "/headers/X-Instance is null"; !strings.Contains(err.Error(), want) {
		t.Errorf("the refusal does not name the null %q: %v", want, err)
	}
}

// TestTheBodySurvivesEveryCharacterClass: the cleaning is the grammar's own
// set, not a list of the classes seen so far. A class a cluster name can carry
// either renders as itself — a character the document holds — or as the
// replacement rune; it never reaches the body as a byte that costs the whole
// message to "control characters are not allowed". Every case posts through the
// real notifier with body_format yaml and parses the body back, so a class the
// parser refuses fails here.
func TestTheBodySurvivesEveryCharacterClass(t *testing.T) {
	replacement := "\uFFFD"
	cases := map[string]struct{ in, want string }{
		"a plain letter":                {"e", "e"},
		"a multi-byte letter":           {"\u00e9", "\u00e9"},
		"a C0 control":                  {"\x1f", replacement},
		"the marker that opens a span":  {"\x01", replacement},
		"the marker that closes a span": {"\x02", replacement},
		"DEL":                           {"\x7f", replacement},
		"a C1 control":                  {"\u0090", replacement},
		// YAML's lexer folds a NEL away inside a scalar: keeping it would
		// silently rewrite the data, so it is cleaned like any other control.
		"the C1 the lexer folds away": {"\u0085", replacement},
		"a noncharacter":              {"\ufffe", replacement},
		"the last noncharacter":       {"\uffff", replacement},
		"a surrogate's bytes":         {"\xed\xa0\x80", replacement + replacement + replacement},
		"a BOM":                       {"\ufeff", "\ufeff"},
		"a tab":                       {"\t", "\t"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			ev := notifiertest.Event()
			ev.Run.Artifacts[0].Artifact.Name = "db" + tc.in + "name"
			got := renderOne(t, "Name: {{ range .Run.Artifacts }}{{ .Artifact.Name }}{{ end }}\ndone", ev)
			if want := "Name: db" + tc.want + "name"; got.Title != want {
				t.Errorf("title = %q, want %q", got.Title, want)
			}
			if len(got.Lines) != 1 || got.Lines[0].Text != "done" {
				t.Errorf("the message lost its last line: %+v", got.Lines)
			}
		})
	}
}
