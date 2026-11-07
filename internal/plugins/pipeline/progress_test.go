package pipeline

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/human"
)

// recordedEvent is one log record: the event it named and the instance prefix
// it was written under.
type recordedEvent struct {
	event human.Event
	attrs map[string]string
}

// recorder collects the events a producer emits.
type recorder struct {
	mu     sync.Mutex
	events []recordedEvent
}

func (r *recorder) add(ev recordedEvent) {
	r.mu.Lock()
	r.events = append(r.events, ev)
	r.mu.Unlock()
}

func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.events)
}

// named counts the events with that name.
func (r *recorder) named(name string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, ev := range r.events {
		if ev.event.Name == name {
			n++
		}
	}
	return n
}

// find returns the first event with that name.
func (r *recorder) find(name string) (recordedEvent, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, ev := range r.events {
		if ev.event.Name == name {
			return ev, true
		}
	}
	return recordedEvent{}, false
}

// recHandler is the slog handler behind a recorder: it keeps the prefix
// attributes WithAttrs carried and the human.Event of each record.
type recHandler struct {
	r     *recorder
	attrs []slog.Attr
}

func (h *recHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *recHandler) Handle(_ context.Context, rec slog.Record) error {
	ev := recordedEvent{attrs: map[string]string{}}
	for _, a := range h.attrs {
		ev.attrs[a.Key] = a.Value.String()
	}
	rec.Attrs(func(a slog.Attr) bool {
		if a.Key == "event" {
			if e, ok := a.Value.Any().(human.Event); ok {
				ev.event = e
			}
		}
		return true
	})
	h.r.add(ev)
	return nil
}

func (h *recHandler) WithAttrs(as []slog.Attr) slog.Handler {
	out := &recHandler{r: h.r, attrs: make([]slog.Attr, 0, len(h.attrs)+len(as))}
	out.attrs = append(append(out.attrs, h.attrs...), as...)
	return out
}

func (h *recHandler) WithGroup(string) slog.Handler { return h }

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
		p := New(Options{ProgressInterval: time.Millisecond, Log: slog.New(&blockingHandler{entered: entered, release: release})})

		raw := &countWriter{}
		raw.n.Store(16)
		stop := p.watchProgress(context.Background(), Plan{SourceInstance: "thing", SourceType: "synthetic"}, raw)

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

// TestWatchProgressSkipsAnUnchangedCount: a tick that finds the same total as
// the last one logs nothing, and the next real change logs exactly once. The
// clock is virtual, so the ticks are deterministic.
func TestWatchProgressSkipsAnUnchangedCount(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rec := &recorder{}
		p := New(Options{ProgressInterval: time.Second, Log: slog.New(&recHandler{r: rec})})
		raw := &countWriter{} // never advances on its own: three ticks see the same total
		stop := p.watchProgress(context.Background(), Plan{SourceInstance: "x", SourceType: "synthetic"}, raw)
		time.Sleep(3 * time.Second)
		synctest.Wait()
		if got := rec.count(); got != 0 {
			t.Fatalf("%d progress records for a count that never moved", got)
		}
		raw.n.Store(16)
		time.Sleep(time.Second)
		synctest.Wait()
		stop()
		if got := rec.count(); got != 1 {
			t.Fatalf("%d progress records, want exactly 1 for the one change", got)
		}
	})
}

// TestWatchProgressWithoutAnInterval: with the interval at 0 the watcher is a
// no-op and the returned stop is a no-op that returns at once.
func TestWatchProgressWithoutAnInterval(t *testing.T) {
	p := New(Options{Log: slog.New(slog.DiscardHandler)})
	stop := p.watchProgress(context.Background(), Plan{SourceInstance: "thing", SourceType: "synthetic"}, &countWriter{})
	done := make(chan struct{})
	go func() { stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the no-op stop did not return")
	}
}

// TestWatchProgressStopsWhenTheContextEnds: a dump's context ending stops the
// watcher on its own, before the caller's deferred stop runs.
func TestWatchProgressStopsWhenTheContextEnds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		p := New(Options{ProgressInterval: time.Second, Log: slog.New(slog.DiscardHandler)})
		stop := p.watchProgress(ctx, Plan{SourceInstance: "x", SourceType: "synthetic"}, &countWriter{})
		cancel()
		synctest.Wait() // the watcher's goroutine exits on the context
		stop()
	})
}
