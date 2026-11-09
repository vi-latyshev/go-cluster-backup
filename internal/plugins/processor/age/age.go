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
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/configuration"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/io"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/processor"
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
	Identities []configuration.Secret `json:"identities" doc:"mounted age private-key files, one identity per file; empty means this instance cannot decode"`
}

var _ configuration.Validator = Config{}

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

func identityFiles(identities []configuration.Secret) []string {
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
// readable and parse before any byte is fetched. It is not an instances.Checker —
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
			return pluginerrors.Permanent(err)
		}
	}
	return nil
}

// CheckEncode is the encode side's preflight: an instance with no recipients
// cannot seal anything, which is a property of its configuration and not of the
// night it runs. The assembly asks every pipeline's processors before a run or a
// stage starts, so this is a `validate` failure rather than four retries of an
// artifact that can never be written.
func (p *Processor) CheckEncode(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(p.recipients) == 0 {
		return pluginerrors.Permanent(errors.New("this instance has no recipients, so nothing routed through it can be sealed"))
	}
	return nil
}

// Describe declares Seals: an artifact passing through Encode is what the
// secret-artifact rule looks for.
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
		// A configuration fault, not a capability of this run: execution's
		// retry loop must not spend its budget on it.
		return pluginerrors.Permanent(fmt.Errorf("this instance has no recipients, so nothing routed through it can be sealed: %w", errors.ErrUnsupported))
	}
	w, err := age.Encrypt(dst, p.recipients...)
	if err != nil {
		return pluginerrors.Permanent(fmt.Errorf("age encrypt: %w", err))
	}
	if _, err := pluginio.CopyContext(ctx, w, src); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("age encrypt: %w", err)
	}
	// Close writes the final authentication tag; without it the ciphertext is
	// truncated and no identity can open it.
	if err := w.Close(); err != nil {
		return pluginerrors.Permanent(fmt.Errorf("age encrypt: %w", err))
	}
	return nil
}

// Decode opens a sealed stream. An instance without identities cannot decode
// and reports errors.ErrUnsupported, which the restore workflow reads as a refusal, not
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
			return pluginerrors.Permanent(err)
		}
		identities = append(identities, parsed...)
	}
	r, err := age.Decrypt(src, identities...)
	if err != nil {
		return pluginerrors.Permanent(fmt.Errorf("age decrypt: %w", err))
	}
	if _, err := pluginio.CopyContext(ctx, dst, r); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// A truncated ciphertext fails here, mid-stream: retrying the same
		// bytes cannot succeed, so the class is permanent either way.
		return pluginerrors.Permanent(fmt.Errorf("age decrypt: %w", err))
	}
	return nil
}
