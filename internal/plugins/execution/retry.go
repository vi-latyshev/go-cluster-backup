package execution

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	pluginerrors "github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/human"
)

// Options is the typed operation policy an assembly projects from configuration:
// plugins make one attempt per call, and every wait and attempt count comes from
// here. An error that carries no class takes Unclassified, and a nil Log
// disables the retry event.
type Options struct {
	Attempts     int
	Initial      time.Duration
	Max          time.Duration
	Jitter       float64
	Unclassified pluginerrors.Class
	Log          *slog.Logger
}

// backoff returns the wait after a failed attempt (1-based), doubling from the
// initial delay up to the cap, with the configured jitter fraction.
func (o Options) backoff(attempt int) time.Duration {
	d := o.Initial
	for i := 1; i < attempt && d < o.Max; i++ {
		d *= 2
	}
	if d > o.Max {
		d = o.Max
	}
	if o.Jitter > 0 {
		d = time.Duration(float64(d) * (1 + o.Jitter*(2*rand.Float64()-1)))
	}
	return d
}

// Attempt runs fn until it succeeds, the error is permanent, or attempts run
// out. Cancellation is terminal: nothing is retried after the context ends.
func Attempt[T any](ctx context.Context, o Options, what string, fn func(ctx context.Context, attempt int) (T, error)) (T, error) {
	return attempt(ctx, o, what, -1, fn)
}

// AttemptWithin is Attempt for a caller with a deadline it must not run past,
// such as the finished notification's window: a retry starts only while its
// wait plus bound — the upper bound of one call — still ends before the
// context's deadline. When the next retry no longer fits, the wait is skipped
// and the last attempt's error is returned at once. A bound of 0 fits any call
// that starts before the deadline; a negative bound is no check at all.
func AttemptWithin[T any](ctx context.Context, o Options, what string, bound time.Duration, fn func(ctx context.Context, attempt int) (T, error)) (T, error) {
	return attempt(ctx, o, what, bound, fn)
}

// attempt is the one retry loop both entry points share: bound < 0 skips the
// fit check, a bound of zero checks the wait alone.
func attempt[T any](ctx context.Context, o Options, what string, bound time.Duration, fn func(ctx context.Context, attempt int) (T, error)) (T, error) {
	var zero T
	for n := 1; ; n++ {
		v, err := fn(ctx, n)
		if err == nil {
			return v, nil
		}
		if ctx.Err() != nil {
			return zero, err
		}
		class, after := pluginerrors.ClassOf(err)
		if class == "" {
			class = o.Unclassified
		}
		if class == pluginerrors.ClassPermanent || class == pluginerrors.ClassNotFound {
			return zero, err
		}
		if n >= o.Attempts {
			return zero, err
		}
		if after > 0 {
			// A service's own RetryAfter hint is a floor, never the whole wait:
			// the configured jitter scatters it upward so parallel retries
			// against one throttled service do not return in lockstep. It is
			// never shortened below the hint.
			if o.Jitter > 0 {
				after += time.Duration(float64(after) * o.Jitter * rand.Float64())
			}
		} else {
			after = o.backoff(n)
		}
		// The fit check comes before the wait is announced or slept: a retry
		// whose wait plus its own bound would cross the caller's deadline is
		// not started, and the deadline never cuts a call the caller budgeted
		// for.
		if dl, ok := ctx.Deadline(); ok && bound >= 0 && time.Until(dl) < after+bound {
			if o.Log != nil {
				// The stop is part of the story, for the same reason the retry
				// is: an attempt count short of run.retry.attempts must have a
				// reason in the log.
				o.Log.Info("retry.skipped", slog.Any("event", human.Warning("retry.skipped",
					human.Text("what", what), human.Count("attempt", int64(n)),
					human.Duration("wait", after), human.Duration("left", time.Until(dl)))))
			}
			return zero, err
		}
		if o.Log != nil {
			// A retry is part of the story the log tells, not a debug detail:
			// what is being retried, which attempt, how long the wait is and
			// why the previous attempt failed.
			o.Log.Info("retry", slog.Any("event", human.New("retry",
				human.Text("what", what), human.Count("attempt", int64(n)),
				human.Duration("wait", after), human.Text("reason", err.Error()))))
		}
		select {
		case <-ctx.Done():
			return zero, ctx.Err()
		case <-time.After(after):
		}
	}
}

// AttemptErr is Attempt for a call with no value: the error is the whole result.
func AttemptErr(ctx context.Context, o Options, what string, fn func(ctx context.Context, attempt int) error) error {
	_, err := Attempt(ctx, o, what, func(ctx context.Context, attempt int) (struct{}, error) {
		return struct{}{}, fn(ctx, attempt)
	})
	return err
}

// SafeCall runs fn recovering a plugin panic into a permanent error: a crashing
// plugin fails its step, never the process, and the steps after it still run.
func SafeCall(fn func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = pluginerrors.Permanent(fmt.Errorf("plugin panic: %v", r))
		}
	}()
	return fn()
}

// SafeValue is SafeCall for a value-returning plugin call.
func SafeValue[T any](fn func() (T, error)) (v T, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = pluginerrors.Permanent(fmt.Errorf("plugin panic: %v", r))
		}
	}()
	return fn()
}
