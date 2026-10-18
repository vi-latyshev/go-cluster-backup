package engine

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
)

func TestRunPrefixLiteral(t *testing.T) {
	cases := []struct {
		text string
		want string
		bad  bool
	}{
		{"runs/{{ .Run.ID }}", "runs/", false},
		{"backups-{{ .Run.ID }}", "backups-", false},
		{"{{ .Run.ID }}", "", true},
		{"runs/{{ .Run.ID }}/x", "", true},
		{"runs/{{ .Other }}", "", true},
		{"runs/{{ .Run.ID", "", true},
		{"", "", true},
	}
	for _, tc := range cases {
		got, err := runPrefixLiteral(tc.text)
		if tc.bad {
			if err == nil {
				t.Errorf("%q accepted", tc.text)
			}
			continue
		}
		if err != nil || got != tc.want {
			t.Errorf("%q -> %q, %v", tc.text, got, err)
		}
	}
}

func TestRenderKeyRefusesUnsafeKeys(t *testing.T) {
	e := newWorld(t).newEngine(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}))
	e.cfg.Layout.ArtifactKey = mustTemplate(t, `{{ .Artifact.Name }}`)
	for _, name := range []string{"../escape", "/abs", ""} {
		_, err := e.artifactKey("20261012T042000Z", modelArtifact(name))
		if err == nil {
			t.Errorf("artifact key %q rendered without error", name)
		}
	}
}

func TestAttemptRetryPolicy(t *testing.T) {
	p := retryPolicy{attempts: 3, initial: time.Millisecond, max: 2 * time.Millisecond, unclassified: plugin.ClassTransient}
	ctx := context.Background()

	// A permanent error is not retried.
	calls := 0
	_, err := attempt(ctx, p, "t", func(context.Context, int) (struct{}, error) {
		calls++
		return struct{}{}, plugin.Permanent(errors.New("nope"))
	})
	if err == nil || calls != 1 {
		t.Fatalf("permanent: calls=%d err=%v", calls, err)
	}

	// A transient error is retried up to the budget.
	calls = 0
	_, err = attempt(ctx, p, "t", func(context.Context, int) (struct{}, error) {
		calls++
		return struct{}{}, plugin.Transient(errors.New("later"))
	})
	if err == nil || calls != 3 {
		t.Fatalf("transient: calls=%d err=%v", calls, err)
	}

	// not_found is terminal, like permanent.
	calls = 0
	_, _ = attempt(ctx, p, "t", func(context.Context, int) (struct{}, error) {
		calls++
		return struct{}{}, plugin.NotFound(errors.New("absent"))
	})
	if calls != 1 {
		t.Fatalf("not_found retried %d times", calls)
	}

	// An unclassified error obeys run.unclassified_error.
	p2 := p
	p2.unclassified = plugin.ClassPermanent
	calls = 0
	_, _ = attempt(ctx, p2, "t", func(context.Context, int) (struct{}, error) {
		calls++
		return struct{}{}, errors.New("unclassified")
	})
	if calls != 1 {
		t.Fatalf("unclassified-as-permanent retried %d times", calls)
	}

	// A RetryAfter hint is honoured verbatim.
	start := time.Now()
	_, _ = attempt(ctx, p, "t", func(_ context.Context, n int) (struct{}, error) {
		if n == 1 {
			return struct{}{}, plugin.RetryAfter(errors.New("slow down"), 20*time.Millisecond)
		}
		return struct{}{}, nil
	})
	if elapsed := time.Since(start); elapsed < 20*time.Millisecond {
		t.Fatalf("RetryAfter slept %v, want at least 20ms", elapsed)
	}

	// Cancellation is terminal.
	cctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls = 0
	_, _ = attempt(cctx, p, "t", func(ctx context.Context, _ int) (struct{}, error) {
		calls++
		return struct{}{}, ctx.Err()
	})
	if calls != 1 {
		t.Fatalf("cancelled context retried %d times", calls)
	}
}

func TestBackoffGrowsAndCaps(t *testing.T) {
	p := retryPolicy{initial: 10 * time.Millisecond, max: 40 * time.Millisecond}
	for n, want := range map[int]time.Duration{
		1: 10 * time.Millisecond,
		2: 20 * time.Millisecond,
		3: 40 * time.Millisecond,
		4: 40 * time.Millisecond,
		9: 40 * time.Millisecond,
	} {
		if got := p.backoff(n); got != want {
			t.Errorf("backoff(%d) = %v, want %v", n, got, want)
		}
	}
	p.jitter = 0.5
	for range 50 {
		got := p.backoff(1)
		if got < 5*time.Millisecond || got > 15*time.Millisecond {
			t.Fatalf("jittered backoff %v outside [5ms, 15ms]", got)
		}
	}
}

// chainStage labels its input, making the stage order visible in the output.
func chainStage(want string) func(ctx context.Context, dst io.Writer, src io.Reader) error {
	return func(ctx context.Context, dst io.Writer, src io.Reader) error {
		body, err := io.ReadAll(src)
		if err != nil {
			return err
		}
		_, err = dst.Write([]byte(want + ":" + string(body) + "|"))
		return err
	}
}

func TestChainOrderAndPropagation(t *testing.T) {
	ctx := context.Background()
	var out strings.Builder
	src := func(_ context.Context, w io.Writer) error {
		_, err := w.Write([]byte("in"))
		return err
	}
	err := chain(ctx, &out, src, []func(ctx context.Context, dst io.Writer, src io.Reader) error{
		chainStage("one"),
		chainStage("two"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if out.String() != "two:one:in||" {
		t.Fatalf("chain output %q", out.String())
	}

	// A failing stage propagates upstream: the source's write must fail, not block.
	blocked := errors.New("stage failed")
	err = chain(ctx, io.Discard, func(ctx context.Context, w io.Writer) error {
		_, werr := w.Write([]byte(strings.Repeat("x", 1<<20)))
		return werr
	}, []func(ctx context.Context, dst io.Writer, src io.Reader) error{
		func(ctx context.Context, dst io.Writer, src io.Reader) error { return blocked },
	})
	if err == nil || !strings.Contains(err.Error(), "stage failed") {
		t.Fatalf("chain error %v", err)
	}

	// A panicking stage becomes a permanent error and does not hang the source.
	err = chain(ctx, io.Discard, func(ctx context.Context, w io.Writer) error {
		_, werr := w.Write([]byte(strings.Repeat("x", 1<<20)))
		return werr
	}, []func(ctx context.Context, dst io.Writer, src io.Reader) error{
		func(ctx context.Context, dst io.Writer, src io.Reader) error { panic("boom") },
	})
	if err == nil || !strings.Contains(err.Error(), "panic") {
		t.Fatalf("panic not recovered: %v", err)
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassPermanent {
		t.Fatalf("panics must be permanent, got %q", class)
	}
}

func TestChainWithoutStages(t *testing.T) {
	var out strings.Builder
	err := chain(context.Background(), &out, func(_ context.Context, w io.Writer) error {
		_, werr := w.Write([]byte("direct"))
		return werr
	}, nil)
	if err != nil || out.String() != "direct" {
		t.Fatalf("direct chain: %q, %v", out.String(), err)
	}
}

func mustTemplate(t *testing.T, text string) plugin.Template {
	t.Helper()
	raw, err := jsonMarshal(map[string]any{"t": text})
	if err != nil {
		t.Fatal(err)
	}
	var cfg struct {
		T plugin.Template `json:"t"`
	}
	if err := plugin.Decode(raw, &cfg); err != nil {
		t.Fatal(err)
	}
	return cfg.T
}

func modelArtifact(name string) model.Artifact { return model.Artifact{Name: name} }
