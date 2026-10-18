// Package processortest is the conformance suite every processor implementation
// runs. The fixture carries a built processor and the plaintext it round-trips:
//
//	processortest.Run(t, func(t *testing.T) processortest.Fixture {
//		return processortest.Fixture{Proc: p, Plain: []byte("...")}
//	})
package processortest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugin/plugintest"
	"github.com/vi-latyshev/go-cluster-backup/internal/processor"
)

// Fixture is a built processor and non-empty plaintext it accepts. Proc must be
// built from the plugin's own Spec.Example: the suite encodes AND decodes, and an
// Example carries whatever both directions need (an age example carries both
// recipients and an identity). A config that cannot decode is a legitimate
// runtime state — Decode returns errors.ErrUnsupported — but it is not a fixture
// this suite can exercise, and every check says so instead of passing vacuously.
type Fixture struct {
	Proc  processor.Processor
	Plain []byte
}

type Check struct {
	Name string
	Run  func(t *testing.T, f Fixture) error
}

var Checks = []Check{
	{"describe", checkDescribe},
	{"round trip", checkRoundTrip},
	{"sealing changes the stream", checkSeals},
	{"returns on cancellation", checkCancel},
	{"propagates a write error", checkWriterError},
	{"propagates a read error", checkReaderError},
}

func Run(t *testing.T, newFn func(t *testing.T) Fixture) {
	t.Helper()
	for _, c := range Checks {
		t.Run(c.Name, func(t *testing.T) {
			failCheck(t, c.Run(t, newFn(t)))
		})
	}
}

// fatalReporter is what reporting a failed check needs from a test handle:
// *testing.T satisfies it, and the suite's own test drives the failure path
// with a recorder — a live T cannot survive asserting its own failure path.
type fatalReporter interface {
	Helper()
	Fatalf(format string, args ...any)
}

// failCheck ends the test on a check error: a conformance failure is never a
// warning.
func failCheck(t fatalReporter, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("processortest: %v", err)
	}
}

func checkDescribe(_ *testing.T, f Fixture) error {
	tr, err := f.Proc.Describe(context.Background())
	if err != nil {
		return fmt.Errorf("Describe: %w", err)
	}
	if tr.Ext != "" && !strings.HasPrefix(tr.Ext, ".") {
		return fmt.Errorf("Describe: extension %q must be empty or start with a dot", tr.Ext)
	}
	return nil
}

// encode runs one Encode over the fixture; an unsupported direction is a fixture
// error, never a silent pass.
func encode(t *testing.T, f Fixture) ([]byte, error) {
	t.Helper()
	var out bytes.Buffer
	if err := f.Proc.Encode(context.Background(), &out, bytes.NewReader(f.Plain)); err != nil {
		if errors.Is(err, errors.ErrUnsupported) {
			return nil, fmt.Errorf("Encode is unsupported for this fixture; build it from Spec.Example: %w", err)
		}
		return nil, fmt.Errorf("Encode: %w", err)
	}
	return out.Bytes(), nil
}

func checkRoundTrip(t *testing.T, f Fixture) error {
	if len(f.Plain) == 0 {
		return fmt.Errorf("fixture Plain must be non-empty")
	}
	encoded, err := encode(t, f)
	if err != nil {
		return err
	}
	var back bytes.Buffer
	if err := f.Proc.Decode(context.Background(), &back, bytes.NewReader(encoded)); err != nil {
		if errors.Is(err, errors.ErrUnsupported) {
			return fmt.Errorf("Decode is unsupported for this fixture; build it from Spec.Example, whose example configuration carries what a restore needs: %w", err)
		}
		return fmt.Errorf("Decode: %w", err)
	}
	if !bytes.Equal(back.Bytes(), f.Plain) {
		return fmt.Errorf("round trip returned %d bytes, want the original %d", back.Len(), len(f.Plain))
	}
	return nil
}

func checkSeals(t *testing.T, f Fixture) error {
	tr, err := f.Proc.Describe(context.Background())
	if err != nil {
		return fmt.Errorf("Describe: %w", err)
	}
	if !tr.Seals {
		return nil
	}
	encoded, err := encode(t, f)
	if err != nil {
		return err
	}
	if bytes.Equal(encoded, f.Plain) {
		return fmt.Errorf("the Seals trait is true but the Encode output equals the plaintext")
	}
	return nil
}

func checkCancel(t *testing.T, f Fixture) error {
	if err := plugintest.MustReturnWithError(t, 15*time.Second, "Encode", func(ctx context.Context) error {
		return f.Proc.Encode(ctx, io.Discard, bytes.NewReader(f.Plain))
	}); err != nil {
		return err
	}
	return plugintest.MustReturnWithError(t, 15*time.Second, "Decode", func(ctx context.Context) error {
		return f.Proc.Decode(ctx, io.Discard, bytes.NewReader(f.Plain))
	})
}

func checkWriterError(_ *testing.T, f Fixture) error {
	fw := &plugintest.ErrWriter{}
	err := f.Proc.Encode(context.Background(), fw, bytes.NewReader(f.Plain))
	if fw.Wrote > 0 && err == nil {
		return fmt.Errorf("Encode wrote %d bytes into a failing writer and reported success", fw.Wrote)
	}
	return nil
}

func checkReaderError(_ *testing.T, f Fixture) error {
	if err := f.Proc.Encode(context.Background(), io.Discard, plugintest.NewErrReader()); err == nil {
		return fmt.Errorf("Encode read from a failing reader and reported success")
	}
	return nil
}
