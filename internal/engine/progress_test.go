package engine

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/model"
)

// blockingHandler holds one log record inside Handle until the test releases it,
// which is what "a tick already selected and a write already in flight" looks
// like from the outside.
type blockingHandler struct {
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (h *blockingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *blockingHandler) Handle(context.Context, slog.Record) error {
	h.once.Do(func() { close(h.entered) })
	<-h.release
	return nil
}

func (h *blockingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *blockingHandler) WithGroup(string) slog.Handler      { return h }

// TestProgressWatcherStopWaitsForTheRecord: stop must not return while a tick's
// record is still being written. Without the acknowledgement a progress line
// could land after the artifact's own "done" line — or after the run ended,
// pointing at a spool file that is already gone.
func TestProgressWatcherStopWaitsForTheRecord(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entered := make(chan struct{})
		release := make(chan struct{})
		log := slog.New(&blockingHandler{entered: entered, release: release})
		e := &Engine{log: log, cfg: &Config{Run: &RunConfig{ProgressInterval: model.Duration(time.Millisecond)}}}

		raw := &countWriter{}
		raw.n.Store(16)
		stop := e.watchProgress(context.Background(), artifactPlan{instance: "thing", srcType: "synthetic"}, raw)

		<-entered
		stopped := make(chan struct{})
		go func() { stop(); close(stopped) }()
		// Every goroutine is durably blocked: stop is inside the write it is
		// waiting for, and the tick's own goroutine can make no progress.
		synctest.Wait()
		select {
		case <-stopped:
			t.Fatal("stop returned while a progress record was still being written")
		default:
		}
		close(release)
		<-stopped
	})
}
