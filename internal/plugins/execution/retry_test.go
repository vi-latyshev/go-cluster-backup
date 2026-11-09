package execution

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	pluginerrors "github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
)

func TestBackoffCapsAnOvershootingDoubling(t *testing.T) {
	o := Options{Initial: 10 * time.Millisecond, Max: 15 * time.Millisecond}
	// 10ms doubles to 20ms, past the 15ms cap: the wait is clamped, never the
	// overshoot.
	if got := o.backoff(3); got != 15*time.Millisecond {
		t.Fatalf("backoff(3) = %v, want the 15ms cap", got)
	}
}

func TestBackoffGrowsAndCaps(t *testing.T) {
	o := Options{Initial: 10 * time.Millisecond, Max: 40 * time.Millisecond}
	for n, want := range map[int]time.Duration{
		1: 10 * time.Millisecond,
		2: 20 * time.Millisecond,
		3: 40 * time.Millisecond,
		4: 40 * time.Millisecond,
		9: 40 * time.Millisecond,
	} {
		if got := o.backoff(n); got != want {
			t.Errorf("backoff(%d) = %v, want %v", n, got, want)
		}
	}
	o.Jitter = 0.5
	for range 50 {
		got := o.backoff(1)
		if got < 5*time.Millisecond || got > 15*time.Millisecond {
			t.Fatalf("jittered backoff %v outside [5ms, 15ms]", got)
		}
	}
}

func TestAttemptRetryPolicy(t *testing.T) {
	o := Options{Attempts: 3, Initial: time.Millisecond, Max: 2 * time.Millisecond, Unclassified: pluginerrors.ClassTransient}
	ctx := context.Background()

	// A permanent error is not retried.
	calls := 0
	_, err := Attempt(ctx, o, "t", func(context.Context, int) (struct{}, error) {
		calls++
		return struct{}{}, pluginerrors.Permanent(errors.New("nope"))
	})
	if err == nil || calls != 1 {
		t.Fatalf("permanent: calls=%d err=%v", calls, err)
	}

	// A transient error is retried up to the budget.
	calls = 0
	_, err = Attempt(ctx, o, "t", func(context.Context, int) (struct{}, error) {
		calls++
		return struct{}{}, pluginerrors.Transient(errors.New("later"))
	})
	if err == nil || calls != 3 {
		t.Fatalf("transient: calls=%d err=%v", calls, err)
	}

	// not_found is terminal, like permanent.
	calls = 0
	_, _ = Attempt(ctx, o, "t", func(context.Context, int) (struct{}, error) {
		calls++
		return struct{}{}, pluginerrors.NotFound(errors.New("absent"))
	})
	if calls != 1 {
		t.Fatalf("not_found retried %d times", calls)
	}

	// An unclassified error obeys the configured policy.
	permanent := o
	permanent.Unclassified = pluginerrors.ClassPermanent
	calls = 0
	_, _ = Attempt(ctx, permanent, "t", func(context.Context, int) (struct{}, error) {
		calls++
		return struct{}{}, errors.New("unclassified")
	})
	if calls != 1 {
		t.Fatalf("unclassified-as-permanent retried %d times", calls)
	}

	// A RetryAfter hint is never shortened: the wait is the hint plus jitter.
	start := time.Now()
	_, _ = Attempt(ctx, o, "t", func(_ context.Context, n int) (struct{}, error) {
		if n == 1 {
			return struct{}{}, pluginerrors.RetryAfter(errors.New("slow down"), 20*time.Millisecond)
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
	_, _ = Attempt(cctx, o, "t", func(ctx context.Context, _ int) (struct{}, error) {
		calls++
		return struct{}{}, ctx.Err()
	})
	if calls != 1 {
		t.Fatalf("cancelled context retried %d times", calls)
	}
}

// TestRetryAfterHintIsNeverShortened (A9, R1): a RetryAfter hint is a floor,
// not the whole wait — the configured jitter scatters it upward, so parallel
// retries against one throttled service do not return in lockstep — and with
// jitter 0 the wait is exactly the hint.
func TestRetryAfterHintIsNeverShortened(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		o := Options{Attempts: 11, Initial: time.Millisecond, Max: 2 * time.Millisecond,
			Jitter: 0.5, Unclassified: pluginerrors.ClassTransient}
		var calls []time.Time
		_, err := Attempt(context.Background(), o, "t", func(context.Context, int) (struct{}, error) {
			calls = append(calls, time.Now())
			if len(calls) <= 10 {
				return struct{}{}, pluginerrors.RetryAfter(errors.New("slow down"), time.Second)
			}
			return struct{}{}, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(calls) != 11 {
			t.Fatalf("%d calls, want 11", len(calls))
		}
		waits := map[time.Duration]bool{}
		for i := 1; i < len(calls); i++ {
			wait := calls[i].Sub(calls[i-1])
			if wait < time.Second || wait > 1500*time.Millisecond {
				t.Fatalf("wait %d = %s, want the one-second hint plus at most its half of jitter", i, wait)
			}
			waits[wait] = true
		}
		if len(waits) == 1 {
			t.Fatalf("every wait was %s: the hint is used verbatim and parallel retries stay in lockstep", calls[1].Sub(calls[0]))
		}

		exact := o
		exact.Jitter = 0
		calls = calls[:0]
		if _, err := Attempt(context.Background(), exact, "t", func(context.Context, int) (struct{}, error) {
			calls = append(calls, time.Now())
			if len(calls) == 1 {
				return struct{}{}, pluginerrors.RetryAfter(errors.New("slow down"), time.Second)
			}
			return struct{}{}, nil
		}); err != nil {
			t.Fatal(err)
		}
		if got := calls[1].Sub(calls[0]); got != time.Second {
			t.Fatalf("the wait with jitter 0 = %s, want exactly the one-second hint", got)
		}
	})
}

// TestAttemptStopsOnCancellationDuringTheWait: the context can end between the
// failed attempt and the wait, and the loop returns the context's error instead
// of sleeping out a retry of a cancelled operation.
func TestAttemptStopsOnCancellationDuringTheWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		o := Options{Attempts: 3, Initial: time.Hour, Max: time.Hour, Unclassified: pluginerrors.ClassTransient}
		done := make(chan error, 1)
		calls := 0
		go func() {
			_, err := Attempt(ctx, o, "t", func(context.Context, int) (struct{}, error) {
				calls++
				return struct{}{}, pluginerrors.Transient(errors.New("later"))
			})
			done <- err
		}()
		time.Sleep(30 * time.Minute)
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want the cancelled context", err)
		}
		if calls != 1 {
			t.Fatalf("%d calls, want one", calls)
		}
	})
}

// TestAttemptLogsTheRetry: a retry is part of the log's story — the event names
// what is being retried and why the previous attempt failed.
func TestAttemptLogsTheRetry(t *testing.T) {
	var logged bytes.Buffer
	o := Options{Attempts: 2, Initial: time.Millisecond, Max: time.Millisecond,
		Unclassified: pluginerrors.ClassTransient,
		Log:          slog.New(slog.NewTextHandler(&logged, nil))}
	calls := 0
	_, err := Attempt(context.Background(), o, "put artifact", func(context.Context, int) (struct{}, error) {
		calls++
		if calls == 1 {
			return struct{}{}, pluginerrors.Transient(errors.New("slow down"))
		}
		return struct{}{}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("%d calls, want 2", calls)
	}
	for _, want := range []string{"retry", "put artifact", "slow down"} {
		if !strings.Contains(logged.String(), want) {
			t.Fatalf("log %q does not carry %q", logged.String(), want)
		}
	}
}

func TestAttemptErrReturnsTheError(t *testing.T) {
	o := Options{Attempts: 2, Initial: time.Millisecond, Max: time.Millisecond, Unclassified: pluginerrors.ClassTransient}
	calls := 0
	err := AttemptErr(context.Background(), o, "t", func(context.Context, int) error {
		calls++
		if calls == 1 {
			return pluginerrors.Transient(errors.New("later"))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("%d calls, want 2", calls)
	}
}

func TestSafeCallRecoversAPanic(t *testing.T) {
	if err := SafeCall(func() error { return nil }); err != nil {
		t.Fatalf("a successful call returned %v", err)
	}
	err := SafeCall(func() error { panic("call boom") })
	if err == nil || !strings.Contains(err.Error(), "call boom") {
		t.Fatalf("panic not recovered: %v", err)
	}
	if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassPermanent {
		t.Fatalf("a recovered panic must be permanent, got %q", class)
	}
}

func TestSafeValueRecoversAPanic(t *testing.T) {
	_, err := SafeValue(func() (int, error) { panic("value boom") })
	if err == nil || !strings.Contains(err.Error(), "value boom") {
		t.Fatalf("panic not recovered: %v", err)
	}
	if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassPermanent {
		t.Fatalf("a recovered panic must be permanent, got %q", class)
	}
}
