package webhook

import (
	"context"
	"net/http"

	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/must"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/notifier"
)

// Spec is the compiled plugin identity of this factory: kind and type decide
// it, never a literal of its own.
var Spec = loader.Spec{
	ID:      instances.ID(notifier.Kind.Name(), "webhook"),
	Summary: "posts an event to an HTTP endpoint: the message text and the request body are the config file's own templates",
}

// Apply contributes the webhook notifier factory and its descriptor to the
// compiled schema in one call.
func Apply(lr *loader.Registry, factories *instances.Registry) error {
	return instances.AddFactory(lr, factories, notifier.Kind, instances.FactorySpec[Config, notifier.Notifier]{
		Type:      "webhook",
		Summary:   Spec.Summary,
		Stability: instances.Stable,
		Example: []byte(`{"url": "https://notify.example.com/backup", ` +
			`"method": "POST", "headers": {"Content-Type": "application/json"}, "body_format": "yaml", ` +
			`"timeout": "10s", "success_status": [200, 202], "max_response_bytes": 512, ` +
			`"events": {"run_finished": "{{ pick .Run.Outcome \"delivered\" \"Backup completed\" \"degraded\" \"Backup DEGRADED\" \"failed\" \"Backup FAILED\" }}\n` +
			"⏱ `{{ .Run.RunID }}`\"}, " +
			`"payload": "title: {{ json .Title }}\nlines:\n{{ range .Lines }}\n  - {text: {{ json .Text }}, mono: {{ json .Code }}}\n{{ end }}"}`),
		New: func(ctx context.Context, cfg Config, deps instances.Deps) (notifier.Notifier, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			// The config was validated once already: configuration.Decode runs
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
