package pipeline

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	pluginerrors "github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/execution"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/human"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/processor"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/spool"
)

// Options are the typed production policy the assembly projects from the run
// section: the retry policy of every plugin call, the spool reserve kept free,
// the per-artifact cap and how often a long dump reports its running total. Log
// receives the source and processor events, already carrying each instance's
// prefix.
type Options struct {
	Policy           execution.Options
	ReserveBytes     int64
	MaxArtifactBytes int64
	ProgressInterval time.Duration
	Log              *slog.Logger
}

// Producer binds one run's production policy. It is immutable: every call reads
// the projected options and holds no state between artifacts.
type Producer struct {
	o Options
}

// New returns the producer of one run.
func New(o Options) *Producer { return &Producer{o: o} }

// Step is one processor of an artifact's pipeline, in config order: the order is
// the operator's, never reordered by the plugin graph.
type Step struct {
	// Instance is the config instance name, recorded in the manifest.
	Instance string
	// Type is the factory type, recorded only, so a manifest stays readable
	// after a rename.
	Type      string
	Processor processor.Processor
}

// Plan is one artifact as the planning owner resolved it: the source that
// produces its bytes, the artifact envelope and the processors to apply, in
// order.
type Plan struct {
	Source         source.Source
	SourceInstance string
	SourceType     string
	Artifact       model.Artifact
	Steps          []Step
}

// Resolved is what an artifact's facts say before the first byte: the extension
// and sealing of its bytes, the chain the manifest records and the non-secret
// source identity.
type Resolved struct {
	Artifact model.Artifact
	Chain    []model.ProcessorStep
	Source   model.SourceRef
}

// Artifact is one artifact's bytes sitting in the spool.
type Artifact struct {
	Artifact model.Artifact
	Chain    []model.ProcessorStep
	Source   model.SourceRef
	File     string
	Bytes    int64
	Raw      int64
	SHA256   string
	// Stats is what the source reported about the bytes beyond their validity:
	// entries it left out, files it padded (see source.DumpStatsReporter).
	Stats model.DumpStats
	Took  time.Duration
}

// Resolve fills the artifact's extension, sealing flag and recorded chain, and
// enforces the secret invariant: a secret artifact whose bytes would leave the
// process unsealed is refused before the first byte.
func (p *Producer) Resolve(ctx context.Context, plan Plan) (Resolved, error) {
	srcTr, err := execution.Attempt(ctx, p.o.Policy, "describe source "+plan.SourceInstance, func(ctx context.Context, _ int) (source.SourceTraits, error) {
		return execution.SafeValue(func() (source.SourceTraits, error) { return plan.Source.Describe(ctx) })
	})
	if err != nil {
		return Resolved{}, fmt.Errorf("source %s describe: %w", plan.SourceInstance, err)
	}
	artifact := plan.Artifact
	artifact.Ext = srcTr.Ext
	resolved := Resolved{Artifact: artifact,
		Source: model.SourceRef{Instance: plan.SourceInstance, Type: plan.SourceType, Origin: srcTr.Origin}}
	chain, err := p.chain(ctx, plan)
	if err != nil {
		return Resolved{}, err
	}
	if _, recorded := plan.Source.(source.RecordedChain); !recorded {
		// A recorded chain already named the file: the staged artifact's
		// extension carries every processor's suffix. A run of its own appends
		// them as it describes the steps.
		for _, step := range chain {
			resolved.Artifact.Ext += step.Ext
		}
	}
	sealed := srcTr.Sealed
	for _, step := range chain {
		sealed = sealed || step.Seals
	}
	resolved.Chain = chain
	resolved.Artifact.Sealed = sealed
	if resolved.Artifact.Sensitivity == model.SensitivitySecret && !sealed {
		return Resolved{}, pluginerrors.Permanent(fmt.Errorf("artifact %s is secret but its pipeline has no sealing processor: refusing before the first byte", resolved.Artifact.Name))
	}
	return resolved, nil
}

// chain is where the artifact's chain comes from: the process that produced the
// bytes, when the source carries a recorded chain (the stage's, read back by the
// staged source) — then the artifact's extension already carries every
// processor's suffix, because the file was named with it — or this run's own
// pipeline, whose processors are described for their extension and their sealing
// fact.
func (p *Producer) chain(ctx context.Context, plan Plan) ([]model.ProcessorStep, error) {
	if rc, ok := plan.Source.(source.RecordedChain); ok {
		chain, err := execution.Attempt(ctx, p.o.Policy, "recorded chain "+plan.SourceInstance, func(ctx context.Context, _ int) ([]model.ProcessorStep, error) {
			return execution.SafeValue(func() ([]model.ProcessorStep, error) { return rc.RecordedChain(ctx) })
		})
		if errors.Is(err, errors.ErrUnsupported) {
			return nil, nil
		}
		if err != nil {
			return nil, fmt.Errorf("source %s recorded chain: %w", plan.SourceInstance, err)
		}
		return chain, nil
	}
	chain := make([]model.ProcessorStep, 0, len(plan.Steps))
	for i, step := range plan.Steps {
		tr, err := execution.Attempt(ctx, p.o.Policy, "describe processor", func(ctx context.Context, _ int) (processor.ProcessorTraits, error) {
			return execution.SafeValue(func() (processor.ProcessorTraits, error) { return step.Processor.Describe(ctx) })
		})
		if err != nil {
			return nil, fmt.Errorf("processor %d describe: %w", i, err)
		}
		chain = append(chain, model.ProcessorStep{Instance: step.Instance, Type: step.Type, Ext: tr.Ext, Seals: tr.Seals})
	}
	return chain, nil
}

// Produce checks the spool reserve and streams the artifact into a fresh spool
// file per attempt. g follows the first attempt, so a multipart storage can send
// parts before the producer is done; g may be nil when nothing follows.
func (p *Producer) Produce(ctx context.Context, sp *spool.Spool, plan Plan, res Resolved, g *Growth) (Artifact, error) {
	free, err := sp.FreeBytes()
	if err != nil {
		if g != nil {
			g.fail(err)
		}
		return Artifact{}, err
	}
	if free < p.o.ReserveBytes {
		err := pluginerrors.Permanent(fmt.Errorf("spool has %d free bytes, reserve is %d", free, p.o.ReserveBytes))
		if g != nil {
			g.fail(err)
		}
		return Artifact{}, err
	}
	p.sourceEvent(plan, human.New("source.dump.start", human.Text("origin", res.Source.Origin)))
	start := time.Now()
	pa, err := execution.Attempt(ctx, p.o.Policy, "produce "+res.Artifact.Name, func(ctx context.Context, attempt int) (Artifact, error) {
		w, err := sp.Create(res.Artifact.Group + "-" + res.Artifact.Name + res.Artifact.Ext)
		if err != nil {
			return Artifact{}, pluginerrors.Permanent(err)
		}
		first := attempt == 1 && g != nil
		var sink io.Writer = w
		if first {
			g.begin(w.Name())
			sink = g.writer(w)
		}
		// The cap is the spool's own bound: an artifact past it fails itself —
		// permanently, because the next attempt produces the same bytes — while
		// every other artifact, and the manifest, still fit.
		limit := &limitWriter{w: sink, limit: p.o.MaxArtifactBytes}
		raw, cerr := p.dump(ctx, limit, plan)
		if closeErr := w.Close(); cerr == nil {
			cerr = closeErr
		}
		if cerr != nil {
			if first {
				g.fail(cerr)
			}
			_ = sp.Remove(w.Name())
			return Artifact{}, cerr
		}
		stats, err := p.dumpStats(ctx, plan)
		if err != nil {
			if first {
				g.fail(err)
			}
			_ = sp.Remove(w.Name())
			return Artifact{}, err
		}
		if first {
			g.succeed()
		}
		return Artifact{Artifact: res.Artifact, Chain: res.Chain, Source: res.Source,
			File: w.Name(), Bytes: w.Size(), Raw: raw, SHA256: w.SHA256(), Stats: stats}, nil
	})
	if err != nil {
		// Every terminal exit publishes the end of the first attempt: a failure
		// before the spool file exists never reached the attempt body's own
		// signalling, and a retry that ends the loop must not leave a follower
		// waiting for a change that cannot come. An already-published cause — the
		// first attempt's own failure — stays: fail is a no-op once ended.
		if g != nil {
			g.fail(err)
		}
		return Artifact{}, fmt.Errorf("artifact %s: %w", res.Artifact.Name, err)
	}
	pa.Took = time.Since(start)
	return pa, nil
}

// Dump streams the source through the plan's steps into dst, counting the raw
// bytes the source produced, and announces the dump under the source's own
// instance — the same start event, with the same fields, that Produce emits.
// Every step is wrapped so the log shows what it did, and the dump reports its
// running total while it runs. The resolved facts supply the source identity the
// start event names.
func (p *Producer) Dump(ctx context.Context, dst io.Writer, plan Plan, res Resolved) (int64, error) {
	p.sourceEvent(plan, human.New("source.dump.start", human.Text("origin", res.Source.Origin)))
	return p.dump(ctx, dst, plan)
}

// dump is the streaming body both entry points share: the start event belongs to
// the caller, because Produce announces it once before its first attempt and a
// stage announces it once per dump.
func (p *Producer) dump(ctx context.Context, dst io.Writer, plan Plan) (int64, error) {
	raw := &countWriter{}
	src := func(ctx context.Context, w io.Writer) error {
		raw.w = w
		return plan.Source.Dump(ctx, raw)
	}
	stop := p.watchProgress(ctx, plan, raw)
	defer stop()
	err := Chain(ctx, dst, src, p.stages(plan))
	return raw.n.Load(), err
}

// dumpStats asks the source what it left out of the stream it just wrote. A
// source without the capability — or answering errors.ErrUnsupported — leaves
// out nothing the run has to report. A source that cannot answer fails its own
// artifact: the counts are how a tree that was read while it lived stays
// visible, and an artifact whose report cannot be produced is not a copy anyone
// can lean on.
func (p *Producer) dumpStats(ctx context.Context, plan Plan) (model.DumpStats, error) {
	reporter, ok := plan.Source.(source.DumpStatsReporter)
	if !ok {
		return model.DumpStats{}, nil
	}
	stats, err := execution.Attempt(ctx, p.o.Policy, "dump stats "+plan.SourceInstance, func(ctx context.Context, _ int) (model.DumpStats, error) {
		return execution.SafeValue(func() (model.DumpStats, error) { return reporter.DumpStats(ctx) })
	})
	if errors.Is(err, errors.ErrUnsupported) {
		return model.DumpStats{}, nil
	}
	if err != nil {
		return model.DumpStats{}, fmt.Errorf("reading what the source left out: %w", err)
	}
	return stats, nil
}

// stages wraps every processor step so the log shows the bytes in, the bytes out
// and the time it took, under the processor instance's own name. A failing step
// reports its error here too, where the sizes are known.
func (p *Producer) stages(plan Plan) []Stage {
	out := make([]Stage, len(plan.Steps))
	for i, step := range plan.Steps {
		out[i] = func(ctx context.Context, dst io.Writer, src io.Reader) error {
			in := &countReader{r: src}
			w := &countWriter{w: dst}
			start := time.Now()
			err := step.Processor.Encode(ctx, w, in)
			p.processorEvent(i, step, in.n.Load(), w.n.Load(), time.Since(start), err)
			return err
		}
	}
	return out
}

// processorEvent names every step's outcome under the instance that ran it.
func (p *Producer) processorEvent(i int, step Step, in, out int64, took time.Duration, err error) {
	name := step.Instance
	if name == "" {
		name = fmt.Sprintf("step%d", i+1)
	}
	if err != nil {
		p.at("processor", step.Type, name, human.Failure("processor.failed",
			human.Bytes("in", in), human.Bytes("out", out), human.Duration("took", took), human.Text("reason", err.Error())))
		return
	}
	p.at("processor", step.Type, name, human.New("processor.done",
		human.Bytes("in", in), human.Bytes("out", out), human.Duration("took", took)))
}

// watchProgress reports the bytes a dump has produced, every configured
// interval, so a long dump is visibly alive rather than one line minutes later.
// It returns a stop function the caller defers; with the interval at 0 it does
// nothing at all.
func (p *Producer) watchProgress(ctx context.Context, plan Plan, raw *countWriter) func() {
	interval := p.o.ProgressInterval
	if interval <= 0 {
		return func() {}
	}
	done := make(chan struct{})
	// stopped is the acknowledgement: stop() waits for it, so no tick already
	// selected — and no log write already in flight — can outlive the dump it
	// describes. Without it a progress line could land after the artifact's own
	// "done" line, or after the run ended.
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		t := time.NewTicker(interval)
		defer t.Stop()
		var last int64
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-t.C:
				n := raw.n.Load()
				if n == last {
					continue
				}
				last = n
				p.sourceEvent(plan, human.New("source.progress", human.Bytes("dumped", n)))
			}
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() { close(done) })
		<-stopped
	}
}

// sourceEvent is one event on the source instance's own logger.
func (p *Producer) sourceEvent(plan Plan, ev human.Event) {
	p.at("source", plan.SourceType, plan.SourceInstance, ev)
}

// at emits an event on one instance's own logger, so the line carries that
// instance's prefix: "<kind> <type>/<instance>: ".
func (p *Producer) at(kind, typ, name string, ev human.Event) {
	p.o.Log.With("kind", kind, "type", typ, "instance", name).
		Log(context.Background(), ev.Severity, ev.Name, slog.Any("event", ev))
}

// limitWriter refuses the write that would cross the run's artifact cap. It sits
// between the pipeline's last stage and the spool, so the refusal stops the
// source itself — the bytes of a too-large volume never fill the spool the pod
// runs on.
type limitWriter struct {
	w       io.Writer
	limit   int64
	written int64
}

func (l *limitWriter) Write(p []byte) (int, error) {
	if l.written+int64(len(p)) > l.limit {
		return 0, pluginerrors.Permanent(fmt.Errorf("artifact exceeds run.max_artifact_bytes: %d bytes written, cap is %d",
			l.written+int64(len(p)), l.limit))
	}
	n, err := l.w.Write(p)
	l.written += int64(n)
	return n, err
}

// countWriter counts what flows through it. The counter is atomic because a
// dump's progress watcher reads it from another goroutine while the source
// writes.
type countWriter struct {
	w io.Writer
	n atomic.Int64
}

func (c *countWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n.Add(int64(n))
	return n, err
}

// countReader counts what flows out of it, atomically for the same reason.
type countReader struct {
	r io.Reader
	n atomic.Int64
}

func (c *countReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n.Add(int64(n))
	return n, err
}

// Growth is the first produce attempt as its storages see it: the spool file it
// is writing, the bytes of it that are final, and how the attempt ended. A
// multipart storage follows it while it grows and sends only bytes that cannot
// change any more; a storage that cannot overlap reads the finished file.
type Growth struct {
	mu      sync.Mutex
	file    string
	written int64
	done    bool
	err     error
	changed chan struct{}
}

// NewGrowth returns an empty follower view; the producer feeds it.
func NewGrowth() *Growth { return &Growth{changed: make(chan struct{})} }

// notify wakes every follower; it replaces the channel so the next change is
// awaited too.
func (g *Growth) notify() {
	g.mu.Lock()
	close(g.changed)
	g.changed = make(chan struct{})
	g.mu.Unlock()
}

// begin names the file the first attempt writes.
func (g *Growth) begin(file string) {
	g.mu.Lock()
	g.file = file
	g.mu.Unlock()
	g.notify()
}

// advance makes n more bytes final.
func (g *Growth) advance(n int64) {
	g.mu.Lock()
	g.written += n
	g.mu.Unlock()
	g.notify()
}

// fail ends the attempt with its error. Later changes are ignored: an attempt
// that failed is over, and a retry produces its own file.
func (g *Growth) fail(err error) {
	g.mu.Lock()
	if !g.done {
		g.done, g.err = true, err
	}
	g.mu.Unlock()
	g.notify()
}

// succeed ends the attempt with the object whole; the last write already
// announced every byte.
func (g *Growth) succeed() {
	g.mu.Lock()
	g.done = true
	g.mu.Unlock()
	g.notify()
}

// Snapshot is a follower's view; changed closes on the next change.
func (g *Growth) Snapshot() (file string, written int64, done bool, changed <-chan struct{}, err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.file, g.written, g.done, g.changed, g.err
}

// writer wraps the spool file the first attempt writes: the followers see each
// byte only after the spool accepted it.
func (g *Growth) writer(w io.Writer) io.Writer { return &growthWriter{g: g, w: w} }

// growthWriter makes a write's bytes final for the followers — a byte is
// announced only after the spool accepted it.
type growthWriter struct {
	g *Growth
	w io.Writer
}

func (gw *growthWriter) Write(p []byte) (int, error) {
	n, err := gw.w.Write(p)
	if n > 0 {
		gw.g.advance(int64(n))
	}
	return n, err
}
