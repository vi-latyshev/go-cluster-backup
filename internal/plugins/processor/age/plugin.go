package age

import (
	"context"

	"filippo.io/age"

	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/must"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/processor"
)

// Spec is the compiled plugin identity of this factory: kind and type decide
// it, never a literal of its own.
var Spec = loader.Spec{
	ID:      instances.ID(processor.Kind.Name(), "age"),
	Summary: "age encryption, streaming; sealing is what makes a secret artifact uploadable",
}

// Apply contributes the age processor factory and its descriptor to the
// compiled schema in one call.
func Apply(lr *loader.Registry, factories *instances.Registry) error {
	return instances.AddFactory(lr, factories, processor.Kind, instances.FactorySpec[Config, processor.Processor]{
		Type:      "age",
		Summary:   Spec.Summary,
		Stability: instances.Stable,
		Example: []byte(`{"recipients": ["age1ql3z7hjy54pw3hyww5ayyfg7zqgvc7w3j2elw8zmrj2kg5sfn9aqmcac8p"], ` +
			`"identities": [{"file": "/run/secrets/age/identity.txt"}]}`),
		New: func(ctx context.Context, cfg Config, _ instances.Deps) (processor.Processor, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			// Decode ran Config.Validate before New, and Validate parsed every
			// recipient with this same parser: neither call can fail on a
			// config this process was handed, and must.Do is what a breach of
			// that claim looks like.
			must.Do(cfg, cfg.Validate())
			recipients := make([]age.Recipient, 0, len(cfg.Recipients))
			for _, text := range cfg.Recipients {
				recipients = append(recipients, must.Do(age.ParseX25519Recipient(text)))
			}
			// The identity files are not read here: construction must not
			// touch the filesystem (the conformance kit builds every plugin
			// from its example), and a restore's key file is a runtime fact.
			// Check verifies them at preflight; Decode reads them.
			return &Processor{recipients: recipients, identityFiles: identityFiles(cfg.Identities)}, nil
		},
	})
}
