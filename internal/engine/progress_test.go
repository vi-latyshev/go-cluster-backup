package engine

import (
	"context"
	"log/slog"
	"sync"
	"testing"
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
	entered := make(chan struct{})
	release := make(chan struct{})
	log := slog.New(&blockingHandler{entered: entered, release: release})
	e := &Engine{log: log, cfg: &Config{Run: &RunConfig{ProgressInterval: model.Duration(time.Millisecond)}}}

	raw := &countWriter{}
	raw.n.Store(16)
	stop := e.watchProgress(context.Background(), artifactPlan{instance: "thing", srcType: "synthetic"}, raw)

	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("no progress record was written at all")
	}
	stopped := make(chan struct{})
	go func() { stop(); close(stopped) }()
	select {
	case <-stopped:
		t.Fatal("stop returned while a progress record was still being written")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("stop did not return after the record was written")
	}
}
