package pipeline

import (
	"context"
	"errors"
	"io"
	"testing"
	"testing/synctest"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/processor"
)

// sinkProcessor reads its whole input and writes nothing: a pipeline whose
// output is far smaller than the raw stream. The limiter must still govern the
// raw bytes, so this is how "compressed-output size is irrelevant" is proved.
type sinkProcessor struct{}

func (sinkProcessor) Describe(context.Context) (processor.ProcessorTraits, error) {
	return processor.ProcessorTraits{Ext: ".zst"}, nil
}

func (sinkProcessor) Encode(ctx context.Context, dst io.Writer, src io.Reader) error {
	_, err := io.Copy(io.Discard, src)
	return err
}

func (sinkProcessor) Decode(context.Context, io.Writer, io.Reader) error {
	return errors.ErrUnsupported
}

// TestRateLimitSpreadsRawBytesOverVirtualTime: 10 MiB at 1 MiB/s admits the
// fixed 1 MiB burst at once and then waits a second per MiB — at least nine
// seconds of virtual time, whatever the pipeline writes downstream.
func TestRateLimitSpreadsRawBytesOverVirtualTime(t *testing.T) {
	const total = 10 << 20
	for _, tc := range []struct {
		name  string
		steps []Step
	}{
		{"no processors", nil},
		{"a processor whose output is empty", []Step{{Instance: "sink", Type: "zstd", Processor: sinkProcessor{}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				p := New(testOptions())
				plan := testPlan(&fakeSource{data: make([]byte, total)}, tc.steps...)
				plan.RateLimit = 1 << 20
				start := time.Now()
				n, err := p.Dump(context.Background(), io.Discard, plan, Resolved{})
				if err != nil {
					t.Fatal(err)
				}
				if n != total {
					t.Fatalf("raw bytes = %d, want %d", n, total)
				}
				if elapsed := time.Since(start); elapsed < 9*time.Second {
					t.Fatalf("10 MiB at 1 MiB/s took %s, want at least 9s", elapsed)
				}
			})
		})
	}
}

// TestRateLimitWaitStopsWithTheContext: a cancelled run stops inside the
// limiter instead of finishing a slow stream, and the error is the context's.
func TestRateLimitWaitStopsWithTheContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		p := New(testOptions())
		plan := testPlan(&fakeSource{data: make([]byte, 3<<20)})
		plan.RateLimit = 1 << 20
		done := make(chan error, 1)
		go func() {
			_, err := p.Dump(ctx, io.Discard, plan, Resolved{})
			done <- err
		}()
		// The first MiB is the burst, the second completes at one second, and
		// the third is still waiting when the context ends.
		time.Sleep(1500 * time.Millisecond)
		cancel()
		synctest.Wait()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("Dump = %v, want the context's cancellation", err)
		}
	})
}

// TestUnlimitedRateWritesEverythingAtOnce: 0 is unlimited, and a run without a
// configured rate pays nothing for the limiter.
func TestUnlimitedRateWritesEverythingAtOnce(t *testing.T) {
	p := New(testOptions())
	plan := testPlan(&fakeSource{data: make([]byte, 1<<20)})
	n, err := p.Dump(context.Background(), io.Discard, plan, Resolved{})
	if err != nil {
		t.Fatal(err)
	}
	if n != 1<<20 {
		t.Fatalf("raw bytes = %d, want %d", n, 1<<20)
	}
}

// TestRateLimitIsEnvelopeNotChain: the limit never becomes a processor or a
// recorded chain step, so a restore config without it decodes the artifact.
func TestRateLimitIsEnvelopeNotChain(t *testing.T) {
	plan := testPlan(&fakeSource{}, Step{Instance: "zstd3", Type: "zstd", Processor: &fakeProcessor{}})
	plan.RateLimit = 5 << 20
	res, err := New(testOptions()).Resolve(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Chain) != 1 || res.Chain[0].Instance != "zstd3" {
		t.Fatalf("chain = %+v, want only the declared step", res.Chain)
	}
}

// errWriter refuses every write: the limiter must hand the destination's error
// back unchanged, with the bytes it already accepted.
type errWriter struct{ err error }

func (w errWriter) Write([]byte) (int, error) { return 0, w.err }

// TestRateWriterBranches covers the limiter's edges directly: the fixed burst
// clamp, a destination error, a context already ended, the token ceiling, and
// every credit branch — including a limit so high that a one-byte wait rounds
// down to the nanosecond floor.
func TestRateWriterBranches(t *testing.T) {
	ctx := context.Background()
	w := newRateWriter(ctx, io.Discard, 2<<20)
	rw, ok := w.(*rateWriter)
	if !ok || rw.burst != rateBurst {
		t.Fatalf("a rate above the fixed burst = %+v, want burst %d", w, rateBurst)
	}

	refused := errors.New("destination refused")
	failing := &rateWriter{ctx: ctx, w: errWriter{err: refused}, rate: 1 << 20, burst: 1 << 20, tokens: 1 << 20, last: time.Now()}
	if _, err := failing.Write([]byte("x")); !errors.Is(err, refused) {
		t.Fatalf("Write = %v, want the destination's error", err)
	}

	ended, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := newRateWriter(ended, io.Discard, 1<<20).Write([]byte("x")); !errors.Is(err, context.Canceled) {
		t.Fatalf("Write on an ended context = %v", err)
	}

	now := time.Now()
	r := &rateWriter{rate: 1 << 20, burst: 1 << 20, last: now}
	if r.credit(0) != 0 || r.credit(-time.Second) != 0 {
		t.Fatal("credit of no elapsed time was not zero")
	}
	if (&rateWriter{rate: 0, burst: 1 << 20}).credit(time.Second) != 0 {
		t.Fatal("credit of a zero rate was not zero")
	}
	if r.credit(time.Hour) != r.burst {
		t.Fatal("credit of an hour did not saturate at the burst")
	}
	if got := r.credit(500 * time.Millisecond); got != 512<<10 {
		t.Fatalf("credit of half a second = %d, want %d", got, 512<<10)
	}

	clamp := &rateWriter{ctx: ctx, w: io.Discard, rate: 1 << 20, burst: 1 << 20, tokens: 1 << 20, last: now.Add(-time.Hour)}
	if err := clamp.wait(1); err != nil || clamp.tokens != clamp.burst-1 {
		t.Fatalf("wait = %v, tokens = %d, want %d", err, clamp.tokens, clamp.burst-1)
	}

	// A last stamp in the future credits nothing, so a high rate's one-byte wait
	// rounds down to zero and takes the nanosecond floor.
	tiny := &rateWriter{ctx: ctx, w: io.Discard, rate: 1 << 40, burst: 1 << 20, last: now.Add(time.Hour)}
	if err := tiny.wait(1); err != nil {
		t.Fatalf("a sub-nanosecond wait = %v", err)
	}
}
