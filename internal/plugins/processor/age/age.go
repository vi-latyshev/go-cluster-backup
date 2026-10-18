// Package age is the processor plugin that seals a stream with age. An
// instance may hold recipients, identities or both: a backup instance carries
// recipients and no identity, a restore instance the other way round, and one
// instance with both serves the conformance round trip.
package age

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"filippo.io/age"
	"github.com/vi-latyshev/go-cluster-backup/internal/must"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/processor"
)

// Config carries both halves of the key material. Neither list has a default:
// the file states exactly which keys an instance holds, and an instance
// holding none of a direction refuses that direction instead of failing on
// the first byte.
type Config struct {
	// Recipients are age public keys ("age1..."), written inline: a public key
	// is not a secret, and the plan's config quotes it in the file.
	Recipients []string `json:"recipients" doc:"age public keys (age1...) the stream is encrypted to; empty means this instance cannot encode"`
	// Identities are mounted private-key files, in the order they are tried.
	// The CronJob's config carries an explicit empty list: the private key is
	// never mounted where the backup runs.
	Identities []plugin.Secret `json:"identities" doc:"mounted age private-key files, one identity per file; empty means this instance cannot decode"`
}

var _ plugin.Validator = Config{}

func (c Config) Validate() error {
	if len(c.Recipients) == 0 && len(c.Identities) == 0 {
		return errors.New("neither recipients nor identities: an instance with no key material can neither encode nor decode")
	}
	for i, r := range c.Recipients {
		if strings.TrimSpace(r) == "" {
			return fmt.Errorf("recipients/%d is empty", i)
		}
		// A public key, so naming the offending value leaks nothing, and
		// `validate` catches a typo without constructing anything.
		if _, err := age.ParseX25519Recipient(r); err != nil {
			return fmt.Errorf("recipients/%d: %w", i, err)
		}
	}
	for i, id := range c.Identities {
		if strings.TrimSpace(id.File) == "" {
			return fmt.Errorf("identities/%d: file is empty", i)
		}
	}
	return nil
}

// Register adds the age processor to a registry.
func Register(r *plugin.Registry) error {
	return plugin.Register(r, processor.Kind, plugin.Spec[Config, processor.Processor]{
		Type:      "age",
		Summary:   "age encryption, streaming; sealing is what makes a secret artifact uploadable",
		Stability: plugin.Stable,
		Example: []byte(`{"recipients": ["age1ql3z7hjy54pw3hyww5ayyfg7zqgvc7w3j2elw8zmrj2kg5sfn9aqmcac8p"], ` +
			`"identities": [{"file": "/run/secrets/age/identity.txt"}]}`),
		New: func(ctx context.Context, cfg Config, _ plugin.Deps) (processor.Processor, error) {
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

func identityFiles(identities []plugin.Secret) []string {
	files := make([]string, 0, len(identities))
	for _, id := range identities {
		files = append(files, id.File)
	}
	return files
}

// readIdentities loads one identity file. age's own parser deliberately omits
// the offending line from its errors, which is what keeps private key material
// out of a log line.
func readIdentities(file string) ([]age.Identity, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, fmt.Errorf("identities file %s: %w", file, err)
	}
	defer func() { _ = f.Close() }()
	identities, err := age.ParseIdentities(f)
	if err != nil {
		return nil, fmt.Errorf("identities file %s: %w", file, err)
	}
	return identities, nil
}

type Processor struct {
	recipients    []age.Recipient
	identityFiles []string
}

var (
	_ processor.DecodeChecker = (*Processor)(nil)
	_ processor.EncodeChecker = (*Processor)(nil)
)

// CheckDecode is the restore side's preflight: every identity file must be
// readable and parse before any byte is fetched. It is not a plugin.Checker —
// the run and the stage only encode, and a backup pod that never mounts the
// private key must seal regardless. An instance with no identities has nothing
// to check here: its Encode side is what its recipients are for.
func (p *Processor) CheckDecode(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, file := range p.identityFiles {
		if _, err := readIdentities(file); err != nil {
			// A key file that is unreadable or malformed will not become
			// readable on a retry: the preflight error is permanent.
			return plugin.Permanent(err)
		}
	}
	return nil
}

// CheckEncode is the encode side's preflight: an instance with no recipients
// cannot seal anything, which is a property of its configuration and not of the
// night it runs. The engine asks every pipeline's processors before a run or a
// stage starts, so this is a `validate` failure rather than four retries of an
// artifact that can never be written.
func (p *Processor) CheckEncode(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(p.recipients) == 0 {
		return plugin.Permanent(errors.New("this instance has no recipients, so nothing routed through it can be sealed"))
	}
	return nil
}

// Describe declares Seals: an artifact passing through Encode is what the
// engine's secret-artifact rule looks for.
func (p *Processor) Describe(ctx context.Context) (processor.ProcessorTraits, error) {
	if err := ctx.Err(); err != nil {
		return processor.ProcessorTraits{}, err
	}
	return processor.ProcessorTraits{Ext: ".age", Seals: true}, nil
}

// Encode seals src onto dst. An instance without recipients cannot encode and
// says so with errors.ErrUnsupported — a capability, not a failure.
func (p *Processor) Encode(ctx context.Context, dst io.Writer, src io.Reader) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(p.recipients) == 0 {
		// A configuration fault, not a capability of this run: the engine's
		// retry loop must not spend its budget on it.
		return plugin.Permanent(fmt.Errorf("this instance has no recipients, so nothing routed through it can be sealed: %w", errors.ErrUnsupported))
	}
	w, err := age.Encrypt(dst, p.recipients...)
	if err != nil {
		return plugin.Permanent(fmt.Errorf("age encrypt: %w", err))
	}
	if _, err := plugin.CopyContext(ctx, w, src); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("age encrypt: %w", err)
	}
	// Close writes the final authentication tag; without it the ciphertext is
	// truncated and no identity can open it.
	if err := w.Close(); err != nil {
		return plugin.Permanent(fmt.Errorf("age encrypt: %w", err))
	}
	return nil
}

// Decode opens a sealed stream. An instance without identities cannot decode
// and reports errors.ErrUnsupported, which the engine reads as a refusal, not
// as a corrupt artifact.
func (p *Processor) Decode(ctx context.Context, dst io.Writer, src io.Reader) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(p.identityFiles) == 0 {
		return fmt.Errorf("this instance has no identities: %w", errors.ErrUnsupported)
	}
	var identities []age.Identity
	for _, file := range p.identityFiles {
		parsed, err := readIdentities(file)
		if err != nil {
			return plugin.Permanent(err)
		}
		identities = append(identities, parsed...)
	}
	r, err := age.Decrypt(src, identities...)
	if err != nil {
		return plugin.Permanent(fmt.Errorf("age decrypt: %w", err))
	}
	if _, err := plugin.CopyContext(ctx, dst, r); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// A truncated ciphertext fails here, mid-stream: retrying the same
		// bytes cannot succeed, so the class is permanent either way.
		return plugin.Permanent(fmt.Errorf("age decrypt: %w", err))
	}
	return nil
}

// copyCtx copies until EOF, checking cancellation between reads.
