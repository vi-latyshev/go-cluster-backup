// Package webhook is the notifier plugin that delivers an HTTP request per
// engine event. Both texts it sends are the config file's own: a plain-text
// message template per event (see message.go) and one payload template for
// every event, which turns that message into whatever body a receiver takes.
// This package knows no receiver and ships no message, so an endpoint taking a
// title and a list of lines, one taking a single string and one taking a
// document built from the event are all served by the configuration.
package webhook

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"slices"
	"strings"

	json "encoding/json/v2"

	"go.yaml.in/yaml/v3"

	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/must"
	"github.com/vi-latyshev/go-cluster-backup/internal/notifier"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
)

const maxBodyBytes = 1 << 20

// The two body formats: how a rendered payload becomes the request body.
const (
	// formatYAML reads the rendered payload as a YAML document — the empty
	// lines of the render are dropped first — and sends its JSON encoding, so
	// the config writes a body a human can read and the receiver gets JSON.
	formatYAML = "yaml"
	// formatText sends the rendered payload exactly as it came out.
	formatText = "text"
)

// Config is one webhook notifier. The endpoint and the mechanics are what the
// deployment states once; the messages and the body are what it writes itself, in the
// shape of its own receiver.
type Config struct {
	URL string `json:"url" required:"true" doc:"endpoint every delivered event is posted to, e.g. https://notify.example.com/backup"`

	Method     string            `json:"method" doc:"HTTP method: GET, POST, PUT or PATCH"`
	Headers    map[string]string `json:"headers" doc:"request headers; a credential belongs in a mounted secret, not here, because every value of this file is plain config"`
	BodyFormat string            `json:"body_format" doc:"how a rendered payload becomes the request body: yaml drops the empty rendered lines, reads the rest as a YAML document and sends its JSON encoding; text sends the render as it is"`

	// Timeout bounds one attempt, response body included.
	Timeout model.Duration `json:"timeout" doc:"whole-request time budget including the response body"`
	// SuccessStatus is the accepted code list; a code outside it fails the
	// attempt, so there is no "accept anything" mode.
	SuccessStatus []int `json:"success_status" doc:"HTTP status codes that count as delivered; every other code fails the attempt"`
	// MaxResponseBytes caps how much of a failing receiver's response body may
	// reach an error message.
	MaxResponseBytes model.ByteSize `json:"max_response_bytes" doc:"most bytes of the response body that may be echoed into an error, so a failing receiver cannot fill the log with a dump"`

	// Events is the message of every event this notifier delivers, keyed by
	// event name. An event with no entry here is "not subscribed": Notify
	// returns nil without a request, because a notifier handed every engine
	// event and refusing the ones it was never told about would fail runs for
	// no reason.
	Events map[string]plugin.Template `json:"events" required:"true" doc:"the message of every event this notifier delivers, keyed by event name; an event with no entry here is not subscribed. The template is plain text: its first rendered line is the title, a line that renders empty is dropped, and a pair of markup backticks marks a line's monospace part"`
	// Payload is one body template for every event, executed with .Title, .Lines
	// (each .Text and .Code), .Text (the whole message) and .Event (the event as
	// the engine sent it). The receiver's own key names live here and nowhere
	// else.
	Payload plugin.Template `json:"payload" required:"true" doc:"one request body template for every event, executed with .Title, .Lines (each .Text and .Code), .Text (the whole message, title included) and .Event (the engine event); whatever keys the receiver takes are written here"`
}

// eventNames is every event the engine delivers, in the order a refusal names
// them: a key of `events` outside this list is a message the notifier would
// wait for forever.
var eventNames = []string{model.EventStarted, model.EventFinished}

var _ plugin.Validator = Config{}

// Validate checks what a configuration can be refused for offline: the
// endpoint, the mechanics, and every template rendered against the built-in
// sample — a message whose title renders empty or whose fields do not exist,
// and a payload that is not the document its format promises, are errors here
// rather than a night delivered nowhere. The plugin is delivered to a receiver
// whose answer nobody watches: with notifier_failure_is_fatal false a body that
// never renders costs the night silently.
func (c Config) Validate() error {
	var errs []error
	switch {
	case c.URL == "":
		errs = append(errs, errors.New("url is empty"))
	case !strings.HasPrefix(c.URL, "http://") && !strings.HasPrefix(c.URL, "https://"):
		errs = append(errs, fmt.Errorf("url %q must start with http:// or https://", c.URL))
	}
	switch c.Method {
	case http.MethodGet, http.MethodPost, http.MethodPut, http.MethodPatch:
	default:
		errs = append(errs, fmt.Errorf("method %q is not one of GET, POST, PUT, PATCH", c.Method))
	}
	switch c.BodyFormat {
	case formatYAML, formatText:
	default:
		errs = append(errs, fmt.Errorf("body_format %q is not %s or %s", c.BodyFormat, formatYAML, formatText))
	}
	if c.Timeout.Std() <= 0 {
		errs = append(errs, fmt.Errorf("timeout must be positive, got %s", c.Timeout))
	}
	if len(c.SuccessStatus) == 0 {
		errs = append(errs, errors.New("success_status is empty: with no accepted code every response would fail"))
	}
	seenStatus := map[int]bool{}
	for _, code := range c.SuccessStatus {
		if code < 100 || code > 599 {
			errs = append(errs, fmt.Errorf("success_status %d is not an HTTP status code", code))
			continue
		}
		if seenStatus[code] {
			errs = append(errs, fmt.Errorf("success_status lists %d twice", code))
		}
		seenStatus[code] = true
	}
	if c.MaxResponseBytes < 1 || c.MaxResponseBytes > maxBodyBytes {
		errs = append(errs, fmt.Errorf("max_response_bytes must be between 1 and %d, got %d", maxBodyBytes, c.MaxResponseBytes))
	}
	for _, name := range slices.Sorted(maps.Keys(c.Events)) {
		if !slices.Contains(eventNames, name) {
			errs = append(errs, fmt.Errorf("events/%s: the engine delivers %s, and a key naming no event it sends is a message nobody ever receives",
				name, strings.Join(eventNames, " and ")))
		}
		event, err := newMessage(name, c.Events[name])
		if err != nil {
			errs = append(errs, err)
			continue
		}
		for _, s := range samples() {
			if s.ev.Name != name {
				// A key subscribes to one event: the night a run_finished
				// message describes never carries the outcome a started run
				// still has.
				continue
			}
			if _, err := event.render(cleanEvent(s.ev)); err != nil {
				errs = append(errs, fmt.Errorf("%w (rendered against %s)", err, s.name))
			}
		}
	}
	if c.BodyFormat == formatYAML || c.BodyFormat == formatText {
		for _, s := range samples() {
			if _, err := c.body(sampleMessage(), cleanEvent(s.ev)); err != nil {
				errs = append(errs, fmt.Errorf("payload: rendered against %s: %w", s.name, err))
			}
		}
	}
	return errors.Join(errs...)
}

// body renders the payload template for one message and returns the bytes the
// request carries. In yaml the empty lines of the render are dropped, the rest
// is read as a YAML document and sent as its JSON encoding; in text the render
// is sent as it came out. A body that renders to nothing is refused: it would
// be a request that tells the receiver nothing at all.
func (c Config) body(msg Message, ev model.Event) ([]byte, error) {
	rendered, err := c.Payload.Render(payloadData{
		Title: msg.Title, Lines: msg.Lines, Text: msg.Text(), Event: ev,
	})
	if err != nil {
		return nil, fmt.Errorf("rendering the payload: %w", err)
	}
	switch c.BodyFormat {
	case formatYAML:
		// What a receiver takes as a document is cleaned first: a byte the
		// YAML document cannot carry would cost the whole message, and the
		// event a payload may pull from is arbitrary text from the cluster.
		text := sanitize(strings.Join(nonEmptyLines(rendered), "\n"))
		if strings.TrimSpace(text) == "" {
			return nil, errors.New("the payload renders to nothing")
		}
		var doc any
		if err := yaml.Unmarshal([]byte(text), &doc); err != nil {
			return nil, fmt.Errorf("the rendered payload is not a YAML document: %w", err)
		}
		// Deterministic: two deliveries of one night are one message, and a
		// receiver that dedups or a test that compares bytes depends on it.
		out, err := json.Marshal(doc, json.Deterministic(true))
		if err != nil {
			return nil, fmt.Errorf("the rendered payload does not encode as JSON: %w", err)
		}
		return out, nil
	case formatText:
		if strings.TrimSpace(rendered) == "" {
			return nil, errors.New("the payload renders to nothing")
		}
		return []byte(sanitize(rendered)), nil
	}
	return nil, fmt.Errorf("body_format %q is not %s or %s", c.BodyFormat, formatYAML, formatText)
}

// Register adds the webhook notifier to a registry.
func Register(r *plugin.Registry) error {
	return plugin.Register(r, notifier.Kind, plugin.Spec[Config, notifier.Notifier]{
		Type:      "webhook",
		Summary:   "posts an event to an HTTP endpoint: the message text and the request body are the config file's own templates",
		Stability: plugin.Stable,
		Example: []byte(`{"url": "https://notify.example.com/backup", ` +
			`"method": "POST", "headers": {"Content-Type": "application/json"}, "body_format": "yaml", ` +
			`"timeout": "10s", "success_status": [200, 202], "max_response_bytes": 512, ` +
			`"events": {"run_finished": "{{ pick .Run.Outcome \"delivered\" \"Backup completed\" \"degraded\" \"Backup DEGRADED\" \"failed\" \"Backup FAILED\" }}\n` +
			"⏱ `{{ .Run.RunID }}`\"}, " +
			`"payload": "title: {{ json .Title }}\nlines:\n{{ range .Lines }}\n  - {text: {{ json .Text }}, mono: {{ json .Code }}}\n{{ end }}"}`),
		New: func(ctx context.Context, cfg Config, deps plugin.Deps) (notifier.Notifier, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			// The config was validated once already: plugin.Decode runs
			// Validate before this constructor is reached, and a config it
			// refuses never gets here.
			accepted := make(map[int]bool, len(cfg.SuccessStatus))
			for _, code := range cfg.SuccessStatus {
				accepted[code] = true
			}
			events := make(map[string]*eventMessage, len(cfg.Events))
			for name, tmpl := range cfg.Events {
				// Validate parsed every subscribed message already, with the
				// same function set, so parsing it here cannot fail.
				events[name] = must.Do(newMessage(name, tmpl))
			}
			client := &http.Client{
				Timeout: cfg.Timeout.Std(),
				// A receiver answering 3xx is a misconfigured endpoint: the
				// body is not re-posted to wherever a Location header points.
				// ErrUseLastResponse hands the 3xx back as an ordinary
				// response, so it falls into the status classification below
				// instead of looking like a transport failure.
				CheckRedirect: func(*http.Request, []*http.Request) error {
					return http.ErrUseLastResponse
				},
			}
			return &Notifier{cfg: cfg, accepted: accepted, events: events, client: client, log: deps.Log}, nil
		},
	})
}

type Notifier struct {
	cfg      Config
	accepted map[int]bool
	events   map[string]*eventMessage
	client   *http.Client
	log      *slog.Logger
}

// Close releases idle connections. It is idempotent and safe after a failed New.
func (n *Notifier) Close() error {
	if n.client != nil {
		n.client.CloseIdleConnections()
	}
	return nil
}

// Notify renders the message of one event and the body built from it, and posts
// it once. An event with no entry in `events` is not subscribed: it is skipped
// and returns nil. Errors are classified for the engine's retry loop: 429 and
// every 5xx are transient; any other unexpected status, an unrenderable message
// and a body its format refuses are permanent.
func (n *Notifier) Notify(ctx context.Context, ev model.Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	event, subscribed := n.events[ev.Name]
	if !subscribed {
		n.log.Debug("event is not subscribed by this notifier", "event", ev.Name)
		return nil
	}
	// ONE cleaned copy, passed to both templates. Cleaning inside render alone
	// left the payload's `.Event` unclean, so a payload that serializes the
	// event — `event: {{ json .Event }}` — carried a byte the body's YAML
	// document cannot hold and the whole notification failed permanently.
	ev = cleanEvent(ev)
	msg, err := event.render(ev)
	if err != nil {
		return plugin.Permanent(err)
	}
	body, err := n.cfg.body(msg, ev)
	if err != nil {
		return plugin.Permanent(fmt.Errorf("events/%s: %w", ev.Name, err))
	}
	req, err := http.NewRequestWithContext(ctx, n.cfg.Method, n.cfg.URL, bytes.NewReader(body))
	if err != nil {
		return plugin.Permanent(fmt.Errorf("building the %s request: %w", ev.Name, err))
	}
	for name, value := range n.cfg.Headers {
		req.Header.Set(name, value)
	}
	resp, err := n.client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return plugin.Transient(fmt.Errorf("posting %s: %w", ev.Name, err))
	}
	defer func() { _ = resp.Body.Close() }()
	notice, err := io.ReadAll(io.LimitReader(resp.Body, n.cfg.MaxResponseBytes.Int64()))
	if err != nil {
		return plugin.Transient(fmt.Errorf("reading the %s response: %w", ev.Name, err))
	}
	if n.accepted[resp.StatusCode] {
		n.log.Debug("event delivered", "event", ev.Name, "status", resp.StatusCode)
		return nil
	}
	err = fmt.Errorf("posted %s: HTTP %d %s", ev.Name, resp.StatusCode, strings.TrimSpace(string(notice)))
	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
		return plugin.Transient(err)
	}
	return plugin.Permanent(err)
}
