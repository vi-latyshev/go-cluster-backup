package engine

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/human"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
)

// retryPolicy is the engine's one retry loop: plugins make one attempt per call,
// every wait, budget and attempt count comes from the config.
type retryPolicy struct {
	attempts     int
	initial      time.Duration
	max          time.Duration
	jitter       float64
	unclassified plugin.Class
	log          *slog.Logger
}

// backoff returns the wait after a failed attempt (1-based), doubling from the
// initial delay up to the cap, with the configured jitter fraction.
func (p retryPolicy) backoff(attempt int) time.Duration {
	d := p.initial
	for i := 1; i < attempt && d < p.max; i++ {
		d *= 2
	}
	if d > p.max {
		d = p.max
	}
	if p.jitter > 0 {
		d = time.Duration(float64(d) * (1 + p.jitter*(2*rand.Float64()-1)))
	}
	return d
}

// attempt runs fn until it succeeds, the error is permanent, or attempts run
// out. Cancellation is terminal: nothing is retried after the context ends.
func attempt[T any](ctx context.Context, p retryPolicy, what string, fn func(ctx context.Context, attempt int) (T, error)) (T, error) {
	var zero T
	for n := 1; ; n++ {
		v, err := fn(ctx, n)
		if err == nil {
			return v, nil
		}
		if ctx.Err() != nil {
			return zero, err
		}
		class, after := plugin.ClassOf(err)
		if class == "" {
			class = p.unclassified
		}
		if class == plugin.ClassPermanent || class == plugin.ClassNotFound {
			return zero, err
		}
		if n >= p.attempts {
			return zero, err
		}
		if after <= 0 {
			after = p.backoff(n)
		}
		if p.log != nil {
			// A retry is part of the story the log tells, not a debug detail:
			// what is being retried, which attempt, how long the wait is and
			// why the previous attempt failed.
			p.log.Info("retry", slog.Any("event", human.New("retry",
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

// safeCall runs fn recovering a plugin panic into a permanent error: a crashing
// plugin fails its step, never the process, and the steps after it still run.
func safeCall(fn func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = plugin.Permanent(fmt.Errorf("plugin panic: %v", r))
		}
	}()
	return fn()
}

// safeValue is safeCall for a value-returning plugin call.
func safeValue[T any](fn func() (T, error)) (v T, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = plugin.Permanent(fmt.Errorf("plugin panic: %v", r))
		}
	}()
	return fn()
}
