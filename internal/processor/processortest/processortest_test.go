package processortest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/processor"
)

// xorProc is a reversible, streaming transform; mask 0 keeps it an identity.
type xorProc struct {
	mask  byte
	seals bool
}

func (p xorProc) Describe(context.Context) (processor.ProcessorTraits, error) {
	return processor.ProcessorTraits{Ext: ".xor", Seals: p.seals}, nil
}

func (p xorProc) Encode(ctx context.Context, dst io.Writer, src io.Reader) error {
	return transform(ctx, dst, src, p.mask)
}

func (p xorProc) Decode(ctx context.Context, dst io.Writer, src io.Reader) error {
	return transform(ctx, dst, src, p.mask)
}

func transform(ctx context.Context, dst io.Writer, src io.Reader, mask byte) error {
	buf := make([]byte, 64)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, err := src.Read(buf)
		if n > 0 {
			for i := range buf[:n] {
				buf[i] ^= mask
			}
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return werr
			}
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func goodFixture() Fixture {
	return Fixture{Proc: xorProc{mask: 0x5a}, Plain: []byte("plain text artifact content")}
}

type brokenDecode struct{ xorProc }

func (brokenDecode) Decode(ctx context.Context, dst io.Writer, src io.Reader) error {
	return transform(ctx, dst, src, 0)
}

type swallowWriteError struct{ xorProc }

func (swallowWriteError) Encode(ctx context.Context, dst io.Writer, src io.Reader) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	buf := make([]byte, 64)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			_, _ = dst.Write(buf[:n])
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

type swallowReadError struct{ xorProc }

func (swallowReadError) Encode(ctx context.Context, dst io.Writer, src io.Reader) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	buf := make([]byte, 64)
	for {
		n, _ := src.Read(buf) // read errors swallowed
		if n == 0 {
			return nil
		}
		if _, werr := dst.Write(buf[:n]); werr != nil {
			return werr
		}
	}
}

type nilOnCancel struct{ xorProc }

func (nilOnCancel) Encode(ctx context.Context, dst io.Writer, src io.Reader) error {
	if ctx.Err() != nil {
		return nil
	}
	return transform(ctx, dst, src, 0x5a)
}

type lyingSealer struct{ xorProc }

func (lyingSealer) Describe(context.Context) (processor.ProcessorTraits, error) {
	return processor.ProcessorTraits{Ext: ".sealed", Seals: true}, nil
}

func (lyingSealer) Encode(ctx context.Context, dst io.Writer, src io.Reader) error {
	return transform(ctx, dst, src, 0)
}

func TestSuitePassesGoodProcessor(t *testing.T) {
	Run(t, func(*testing.T) Fixture { return goodFixture() })
}

func TestSealingProcessorPasses(t *testing.T) {
	Run(t, func(*testing.T) Fixture {
		return Fixture{Proc: xorProc{mask: 0x5a, seals: true}, Plain: goodFixture().Plain}
	})
}

func TestChecksCatchBrokenProcessors(t *testing.T) {
	plain := goodFixture().Plain
	cases := []struct {
		check string
		fix   Fixture
	}{
		{"round trip", Fixture{Proc: brokenDecode{xorProc{mask: 0x5a}}, Plain: plain}},
		{"propagates a write error", Fixture{Proc: swallowWriteError{}, Plain: plain}},
		{"propagates a read error", Fixture{Proc: swallowReadError{}, Plain: plain}},
		{"returns on cancellation", Fixture{Proc: nilOnCancel{}, Plain: plain}},
		{"sealing changes the stream", Fixture{Proc: lyingSealer{}, Plain: plain}},
	}
	for _, tc := range cases {
		t.Run(tc.check, func(t *testing.T) {
			for _, c := range Checks {
				if c.Name != tc.check {
					continue
				}
				if err := c.Run(t, tc.fix); err == nil {
					t.Fatalf("check %q passed a broken processor", c.Name)
				}
				return
			}
			t.Fatalf("check %q not found", tc.check)
		})
	}
}

type unsupportedProc struct{ xorProc }

func (unsupportedProc) Encode(ctx context.Context, dst io.Writer, src io.Reader) error {
	return fmt.Errorf("no recipients configured: %w", errors.ErrUnsupported)
}

func (unsupportedProc) Decode(ctx context.Context, dst io.Writer, src io.Reader) error {
	return fmt.Errorf("no identity configured: %w", errors.ErrUnsupported)
}

type badExtProc struct{ xorProc }

func (badExtProc) Describe(context.Context) (processor.ProcessorTraits, error) {
	return processor.ProcessorTraits{Ext: "no-dot"}, nil
}

// An unsupported fixture is never a silent pass: the suite names the Example as
// the fix.
func TestUnsupportedFixtureIsAFixtureError(t *testing.T) {
	fix := Fixture{Proc: unsupportedProc{}, Plain: []byte("plain")}
	for _, c := range Checks {
		if c.Name != "round trip" {
			continue
		}
		err := c.Run(t, fix)
		if err == nil || !strings.Contains(err.Error(), "Example") {
			t.Fatalf("round trip on an unsupported fixture: %v", err)
		}
	}
}

func TestDescribeCatchesABadExtension(t *testing.T) {
	fix := Fixture{Proc: badExtProc{}, Plain: []byte("plain")}
	for _, c := range Checks {
		if c.Name != "describe" {
			continue
		}
		if err := c.Run(t, fix); err == nil {
			t.Fatal("a dotless extension passed")
		}
	}
}

type failingDescribe struct{ xorProc }

func (failingDescribe) Describe(context.Context) (processor.ProcessorTraits, error) {
	return processor.ProcessorTraits{}, errors.New("describe failed")
}

type failingEncode struct {
	xorProc
	seals bool
}

func (f failingEncode) Describe(context.Context) (processor.ProcessorTraits, error) {
	return processor.ProcessorTraits{Ext: ".fail", Seals: f.seals}, nil
}

func (failingEncode) Encode(context.Context, io.Writer, io.Reader) error {
	return errors.New("encode failed")
}

type unsupportedDecode struct{ xorProc }

func (unsupportedDecode) Decode(context.Context, io.Writer, io.Reader) error {
	return fmt.Errorf("no identity configured: %w", errors.ErrUnsupported)
}

type failingDecode struct{ xorProc }

func (failingDecode) Decode(context.Context, io.Writer, io.Reader) error {
	return errors.New("decode failed")
}

// TestChecksCatchEveryFailureMode drives each check through the failure it
// exists for, so a check that stopped detecting it would pass a broken
// processor silently.
func TestChecksCatchEveryFailureMode(t *testing.T) {
	plain := goodFixture().Plain
	cases := []struct {
		check string
		fix   Fixture
	}{
		{"describe", Fixture{Proc: failingDescribe{}, Plain: plain}},
		{"round trip", Fixture{Proc: unsupportedDecode{xorProc{mask: 0x5a}}, Plain: plain}},
		{"round trip", Fixture{Proc: failingDecode{xorProc{mask: 0x5a}}, Plain: plain}},
		{"sealing changes the stream", Fixture{Proc: failingDescribe{}, Plain: plain}},
		{"sealing changes the stream", Fixture{Proc: failingEncode{xorProc: xorProc{mask: 0x5a}, seals: true}, Plain: plain}},
	}
	for _, tc := range cases {
		t.Run(tc.check, func(t *testing.T) {
			for _, c := range Checks {
				if c.Name != tc.check {
					continue
				}
				if err := c.Run(t, tc.fix); err == nil {
					t.Fatalf("check %q passed a broken processor", c.Name)
				}
				return
			}
			t.Fatalf("check %q not found", tc.check)
		})
	}
}

// recorder stands in for *testing.T where the suite's own failure path is what
// is under test.
type recorder struct{ failures []string }

func (r *recorder) Helper() {}

func (r *recorder) Fatalf(format string, args ...any) {
	r.failures = append(r.failures, fmt.Sprintf(format, args...))
}

func TestFailCheckReportsOnlyFailures(t *testing.T) {
	rec := &recorder{}
	failCheck(rec, errors.New("the check failed"))
	if len(rec.failures) != 1 || !strings.Contains(rec.failures[0], "the check failed") {
		t.Fatalf("a failed check was not reported: %v", rec.failures)
	}
	rec = &recorder{}
	failCheck(rec, nil)
	if len(rec.failures) != 0 {
		t.Fatalf("a passing check was reported as a failure: %v", rec.failures)
	}
}

func TestRoundTripRejectsAnEmptyFixture(t *testing.T) {
	fix := Fixture{Proc: xorProc{mask: 0x5a}, Plain: nil}
	for _, c := range Checks {
		if c.Name != "round trip" {
			continue
		}
		if err := c.Run(t, fix); err == nil {
			t.Fatal("an empty fixture passed the round trip")
		}
	}
}
