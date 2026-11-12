package pipeline

import (
	"context"
	"io"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
)

// rateBurst is the most raw bytes a limited source may write before any time
// passes: a fixed 1 MiB, documented beside rate_limit. It bounds the burst a
// short dump sees; a rate below it gets a proportionally smaller burst, so a
// slow limit is never front-loaded with a whole MiB.
const rateBurst = 1 << 20

// newRateWriter wraps the raw source's destination with the configured rate.
// rate <= 0 is unlimited and returns w itself, so an unconfigured run pays
// nothing for the limiter. The limiter sits before the first processor: it
// spreads the source's reads over time and never changes the bytes, the chain
// or the manifest.
func newRateWriter(ctx context.Context, w io.Writer, rate model.ByteSize) io.Writer {
	if rate <= 0 {
		return w
	}
	burst := int64(rate)
	if burst > rateBurst {
		burst = rateBurst
	}
	return &rateWriter{ctx: ctx, w: w, rate: int64(rate), burst: burst, tokens: burst, last: time.Now()}
}

// rateWriter hands writes to w at no more than rate bytes per second, from a
// bucket that starts full and holds at most burst tokens. It waits on the
// caller's context, so a cancelled or expired run stops inside the limiter:
// the source's Dump returns the context error and the chain joins, while the
// source's own subprocess is killed by the same context.
//
// Cumulative admitted bytes never exceed burst + rate*elapsed for one attempt:
// a fresh writer per attempt is what "per attempt" means.
type rateWriter struct {
	ctx    context.Context
	w      io.Writer
	rate   int64
	burst  int64
	tokens int64
	last   time.Time
}

func (r *rateWriter) Write(p []byte) (int, error) {
	written := 0
	for written < len(p) {
		chunk := len(p) - written
		if int64(chunk) > r.burst {
			chunk = int(r.burst)
		}
		if err := r.wait(int64(chunk)); err != nil {
			return written, err
		}
		n, err := r.w.Write(p[written : written+chunk])
		written += n
		if err != nil {
			return written, err
		}
	}
	return written, nil
}

// wait blocks until n tokens are available and spends them. Credit is whole
// bytes only, so the arithmetic never rounds a byte in: sub-nanosecond
// remainders are carried by last and re-added on the next pass.
func (r *rateWriter) wait(n int64) error {
	for {
		if err := r.ctx.Err(); err != nil {
			return err
		}
		now := time.Now()
		r.tokens += r.credit(now.Sub(r.last))
		if r.tokens > r.burst {
			r.tokens = r.burst
		}
		r.last = now
		if r.tokens >= n {
			r.tokens -= n
			return nil
		}
		delay := time.Duration((n - r.tokens) * int64(time.Second) / r.rate)
		if delay <= 0 {
			delay = time.Nanosecond
		}
		timer := time.NewTimer(delay)
		select {
		case <-r.ctx.Done():
			timer.Stop()
			return r.ctx.Err()
		case <-timer.C:
		}
	}
}

// credit is how many whole bytes one elapsed span adds, saturating at the burst
// a bucket can hold. A span long enough to fill the bucket never multiplies the
// two large numbers, so no configured rate can overflow the arithmetic.
func (r *rateWriter) credit(elapsed time.Duration) int64 {
	if elapsed <= 0 || r.rate <= 0 {
		return 0
	}
	if elapsed >= time.Duration(r.burst)*time.Second/time.Duration(r.rate) {
		return r.burst
	}
	return int64(elapsed) * r.rate / int64(time.Second)
}
