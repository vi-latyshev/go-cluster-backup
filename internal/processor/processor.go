// Package processor is the seam that wraps a stream: compression and encryption
// are one kind, so a pipeline applies processors to an artifact one at a time.
// The interface obeys the seam rules documented in package plugin.
package processor

import (
	"context"
	"io"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
)

// ProcessorTraits are facts about how a processor transforms a stream.
type ProcessorTraits struct {
	// Ext is appended to the artifact extension, e.g. ".zst", ".age"; may be "".
	Ext string
	// Seals is true when Encode output is safe for a "secret" artifact to leave
	// the process — the only thing that makes a secret artifact uploadable.
	Seals bool
}

// DecodeChecker is the restore-side preflight of a processor: what has to be
// readable — a private key, a helper binary — before a byte is decoded. It is a
// capability of its own, never part of Check: the run and the stage encode, and
// a backup instance must not fail its preflight because a key that only a
// restore needs is not mounted in the backup pod. `validate --preflight` and
// every restore path call it; the run and the stage do not.
type DecodeChecker interface {
	CheckDecode(ctx context.Context) error
}

// EncodeChecker is the encode side of DecodeChecker: a processor that cannot
// encode at all — a sealing instance with no recipient — says so here, and the
// engine asks every processor of every pipeline while the configuration is
// read. Without it such a pipeline is refused on the night, one artifact at a
// time, after the retry budget was spent on something that could never work.
type EncodeChecker interface {
	CheckEncode(ctx context.Context) error
}

type Processor interface {
	Describe(ctx context.Context) (ProcessorTraits, error)
	// Encode reads the plaintext stream and writes the transformed one. One
	// attempt per call; a cancelled context is an error.
	Encode(ctx context.Context, dst io.Writer, src io.Reader) error
	// Decode reverses Encode. Restores call it in reverse chain order. An
	// instance built from a config that cannot decode — an age with recipients
	// but no identity — returns an error wrapping errors.ErrUnsupported, which
	// the engine reports as a refusal, never as a decode failure.
	Decode(ctx context.Context, dst io.Writer, src io.Reader) error
}

var Kind = plugin.NewKind[Processor]("processor")
