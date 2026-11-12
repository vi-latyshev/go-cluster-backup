package pipeline

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	pluginerrors "github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/execution"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/human"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/processor"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/spool"
)

// fakeSource is a source whose traits, bytes and report a test configures, with
// every Dump counted so a retry is visible.
type fakeSource struct {
	data        []byte
	traits      source.SourceTraits
	describeErr error
	dumpErr     error
	dumpOnce    error
	stats       model.DumpStats
	statsErr    error
	dumps       int
}

func (f *fakeSource) Describe(ctx context.Context) (source.SourceTraits, error) {
	if err := ctx.Err(); err != nil {
		return source.SourceTraits{}, err
	}
	if f.describeErr != nil {
		return source.SourceTraits{}, f.describeErr
	}
	tr := f.traits
	if tr.Ext == "" && !tr.Sealed {
		tr.Ext = ".dat"
	}
	if tr.Origin == "" {
		tr.Origin = "test://fake/source"
	}
	return tr, nil
}

func (f *fakeSource) Dump(ctx context.Context, dst io.Writer) error {
	f.dumps++
	if err := ctx.Err(); err != nil {
		return err
	}
	if f.dumpErr != nil {
		return f.dumpErr
	}
	if f.dumps == 1 && f.dumpOnce != nil {
		return f.dumpOnce
	}
	_, err := dst.Write(f.data)
	return err
}

func (f *fakeSource) DumpStats(ctx context.Context) (model.DumpStats, error) {
	if err := ctx.Err(); err != nil {
		return model.DumpStats{}, err
	}
	if f.statsErr != nil {
		return model.DumpStats{}, f.statsErr
	}
	return f.stats, nil
}

// recordedSource is a source whose bytes another process produced: it reports
// the recorded chain through the optional capability.
type recordedSource struct {
	fakeSource
	chain            []model.ProcessorStep
	chainErr         error
	chainUnsupported bool
}

func (r *recordedSource) RecordedChain(ctx context.Context) ([]model.ProcessorStep, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r.chainUnsupported {
		return nil, errors.ErrUnsupported
	}
	if r.chainErr != nil {
		return nil, r.chainErr
	}
	return r.chain, nil
}

// credentialSource carries credentials and answers the optional policy with the
// verdict a test configures: the one way a source type keeps a published
// plaintext configuration working without making plaintext the rule.
type credentialSource struct {
	fakeSource
	verdict source.CredentialVerdict
	err     error
	calls   int
}

func (c *credentialSource) CredentialPolicy(ctx context.Context) (source.CredentialVerdict, error) {
	c.calls++
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return c.verdict, c.err
}

// fakeProcessor is a processor whose traits and failures a test configures; its
// Encode copies the stream and counts the calls.
type fakeProcessor struct {
	traits      processor.ProcessorTraits
	describeErr error
	encodeErr   error
	encodes     int
}

func (p *fakeProcessor) Describe(ctx context.Context) (processor.ProcessorTraits, error) {
	if err := ctx.Err(); err != nil {
		return processor.ProcessorTraits{}, err
	}
	if p.describeErr != nil {
		return processor.ProcessorTraits{}, p.describeErr
	}
	tr := p.traits
	if tr.Ext == "" && !tr.Seals {
		tr.Ext = ".zst"
	}
	return tr, nil
}

func (p *fakeProcessor) Encode(ctx context.Context, dst io.Writer, src io.Reader) error {
	p.encodes++
	if p.encodeErr != nil {
		return p.encodeErr
	}
	_, err := io.Copy(dst, src)
	return err
}

func (p *fakeProcessor) Decode(ctx context.Context, dst io.Writer, src io.Reader) error {
	_, err := io.Copy(dst, src)
	return err
}

// testOptions is the production policy every test starts from.
func testOptions() Options {
	return Options{
		Policy: execution.Options{Attempts: 3, Initial: time.Millisecond, Max: time.Millisecond,
			Unclassified: pluginerrors.ClassTransient},
		MaxArtifactBytes: 1 << 20,
		Log:              slog.New(slog.DiscardHandler),
	}
}

// testPlan is one plain artifact produced from the given source and steps.
func testPlan(src source.Source, steps ...Step) Plan {
	return Plan{Source: src, SourceInstance: "vpn", SourceType: "fake",
		Artifact: model.Artifact{Group: "g", Name: "n", Sensitivity: model.SensitivityPlain},
		Steps:    steps}
}

// produced resolves and produces the plan in one go for the tests that only
// care about the result.
func produced(t *testing.T, p *Producer, sp *spool.Spool, plan Plan, g *Growth) (Artifact, error) {
	t.Helper()
	res, err := p.Resolve(context.Background(), plan)
	if err != nil {
		return Artifact{}, err
	}
	return p.Produce(context.Background(), sp, plan, res, g)
}

func TestResolveRecordsTheChainAndSealing(t *testing.T) {
	p := New(testOptions())
	sealing := &fakeProcessor{traits: processor.ProcessorTraits{Ext: ".age", Seals: true}}
	plan := testPlan(&fakeSource{traits: source.SourceTraits{Ext: ".dump", Origin: "test://db"}},
		Step{Instance: "zstd3", Type: "zstd", Processor: &fakeProcessor{}},
		Step{Instance: "crypt", Type: "age", Processor: sealing})
	plan.Artifact.Sensitivity = model.SensitivitySecret

	res, err := p.Resolve(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	if res.Artifact.Ext != ".dump.zst.age" {
		t.Fatalf("ext %q, want the source's and every processor's suffix", res.Artifact.Ext)
	}
	if !res.Artifact.Sealed {
		t.Fatal("a sealing processor must seal the artifact")
	}
	want := []model.ProcessorStep{
		{Instance: "zstd3", Type: "zstd", Ext: ".zst"},
		{Instance: "crypt", Type: "age", Ext: ".age", Seals: true},
	}
	if !slices.Equal(res.Chain, want) {
		t.Fatalf("chain %+v, want %+v", res.Chain, want)
	}
	if res.Source != (model.SourceRef{Instance: "vpn", Type: "fake", Origin: "test://db"}) {
		t.Fatalf("source ref %+v", res.Source)
	}
}

func TestResolveRefusesAnUnsealedSecretArtifact(t *testing.T) {
	p := New(testOptions())
	plan := testPlan(&fakeSource{}, Step{Instance: "zstd3", Type: "zstd", Processor: &fakeProcessor{}})
	plan.Artifact.Sensitivity = model.SensitivitySecret

	_, err := p.Resolve(context.Background(), plan)
	if err == nil || !strings.Contains(err.Error(), "has no sealing processor") {
		t.Fatalf("err = %v, want the sealing refusal", err)
	}
	if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassPermanent {
		t.Fatalf("class %q, want permanent", class)
	}
}

func TestResolveSourceDescribeFailure(t *testing.T) {
	p := New(testOptions())
	plan := testPlan(&fakeSource{describeErr: errors.New("no describe")})

	_, err := p.Resolve(context.Background(), plan)
	if err == nil || !strings.Contains(err.Error(), "source vpn describe") {
		t.Fatalf("err = %v, want the source describe failure", err)
	}
}

func TestResolveProcessorDescribeFailure(t *testing.T) {
	p := New(testOptions())
	plan := testPlan(&fakeSource{},
		Step{Instance: "ok", Type: "zstd", Processor: &fakeProcessor{}},
		Step{Instance: "bad", Type: "age", Processor: &fakeProcessor{describeErr: errors.New("no traits")}})

	_, err := p.Resolve(context.Background(), plan)
	if err == nil || !strings.Contains(err.Error(), "processor 1 describe") {
		t.Fatalf("err = %v, want the processor describe failure", err)
	}
}

func TestResolveUsesTheRecordedChain(t *testing.T) {
	p := New(testOptions())
	unused := &fakeProcessor{describeErr: errors.New("a recorded chain must not describe processors")}
	recorded := []model.ProcessorStep{{Instance: "staged", Type: "zstd", Ext: ".zst", Seals: true}}
	src := &recordedSource{fakeSource: fakeSource{traits: source.SourceTraits{Ext: ".zst"}}, chain: recorded}
	plan := testPlan(src, Step{Instance: "unused", Type: "zstd", Processor: unused})

	res, err := p.Resolve(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(res.Chain, recorded) {
		t.Fatalf("chain %+v, want the recorded one", res.Chain)
	}
	if res.Artifact.Ext != ".zst" || !res.Artifact.Sealed {
		t.Fatalf("artifact %+v, want the recorded file's extension and sealing", res.Artifact)
	}
}

func TestResolveRecordedChainUnsupportedIsNoChain(t *testing.T) {
	p := New(testOptions())
	src := &recordedSource{fakeSource: fakeSource{}, chainUnsupported: true}

	res, err := p.Resolve(context.Background(), testPlan(src))
	if err != nil {
		t.Fatal(err)
	}
	if res.Chain != nil {
		t.Fatalf("chain %+v, want none", res.Chain)
	}
}

func TestResolveRecordedChainFailure(t *testing.T) {
	p := New(testOptions())
	src := &recordedSource{fakeSource: fakeSource{}, chainErr: errors.New("record is gone")}

	_, err := p.Resolve(context.Background(), testPlan(src))
	if err == nil || !strings.Contains(err.Error(), "recorded chain") {
		t.Fatalf("err = %v, want the recorded-chain failure", err)
	}
}

func TestProduceSpoolFreeBytesFails(t *testing.T) {
	p := New(testOptions())
	dir := filepath.Join(t.TempDir(), "gone")
	sp, err := spool.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	plan := testPlan(&fakeSource{data: []byte("z")})
	res, err := p.Resolve(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	g := NewGrowth()
	_, err = p.Produce(context.Background(), sp, plan, res, g)
	if err == nil || !strings.Contains(err.Error(), "statfs") {
		t.Fatalf("err = %v, want the statfs failure", err)
	}
	if _, _, done, _, gerr := g.Snapshot(); !done || gerr == nil {
		t.Fatalf("the growth was not ended for the followers: done=%v err=%v", done, gerr)
	}
}

func TestProduceReserveFails(t *testing.T) {
	o := testOptions()
	o.ReserveBytes = 1 << 62
	p := New(o)
	sp, err := spool.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	src := &fakeSource{data: []byte("z")}
	plan := testPlan(src)
	res, err := p.Resolve(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	g := NewGrowth()
	_, err = p.Produce(context.Background(), sp, plan, res, g)
	if err == nil || !strings.Contains(err.Error(), "reserve") {
		t.Fatalf("err = %v, want the reserve failure", err)
	}
	if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassPermanent {
		t.Fatalf("class %q, want permanent", class)
	}
	if src.dumps != 0 {
		t.Fatalf("the source ran %d times, want none", src.dumps)
	}
	if _, _, done, _, gerr := g.Snapshot(); !done || gerr == nil {
		t.Fatalf("the growth was not ended for the followers: done=%v err=%v", done, gerr)
	}
}

func TestProduceSpoolCreateFails(t *testing.T) {
	p := New(testOptions())
	dir := t.TempDir()
	sp, err := spool.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	// A spool directory replaced by a regular file makes Create fail: statfs
	// still answers, and the CreateTemp inside it cannot.
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dir, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = produced(t, p, sp, testPlan(&fakeSource{data: []byte("z")}), nil)
	if err == nil || !strings.Contains(err.Error(), "spool") {
		t.Fatalf("err = %v, want the spool create failure", err)
	}
	if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassPermanent {
		t.Fatalf("class %q, want permanent", class)
	}
}

func TestProduceCapFailsPermanently(t *testing.T) {
	o := testOptions()
	o.MaxArtifactBytes = 5
	p := New(o)
	dir := t.TempDir()
	sp, err := spool.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	src := &fakeSource{data: []byte("0123456789")}
	_, err = produced(t, p, sp, testPlan(src), nil)
	if err == nil || !strings.Contains(err.Error(), "max_artifact_bytes") {
		t.Fatalf("err = %v, want the cap failure", err)
	}
	if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassPermanent {
		t.Fatalf("class %q, want permanent", class)
	}
	// Permanent, so the cap costs one attempt: the same bytes would cross it
	// again, and the retry budget is for faults that pass.
	if src.dumps != 1 {
		t.Fatalf("the oversized artifact was attempted %d times, want 1", src.dumps)
	}
	if names := spoolNames(t, dir); len(names) != 0 {
		t.Fatalf("a spool file survived the refused artifact: %v", names)
	}
}

func TestProduceRetriesWithAFreshFile(t *testing.T) {
	p := New(testOptions())
	dir := t.TempDir()
	sp, err := spool.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	src := &fakeSource{data: []byte("bytes"), dumpOnce: errors.New("transient dump")}
	g := NewGrowth()
	pa, err := produced(t, p, sp, testPlan(src), g)
	if err != nil {
		t.Fatal(err)
	}
	if src.dumps != 2 {
		t.Fatalf("dumps = %d, want the first attempt and its retry", src.dumps)
	}
	names := spoolNames(t, dir)
	if len(names) != 1 || names[0] != pa.File {
		t.Fatalf("spool holds %v, want only the retry's file %q", names, pa.File)
	}
	// The growth follows the FIRST attempt only: it ended with the transient
	// failure the retry replaced, and the retry's file is not announced as the
	// first one's.
	file, written, done, changed, gerr := g.Snapshot()
	if !done || gerr == nil || !strings.Contains(gerr.Error(), "transient dump") || file == pa.File || written != 0 || changed == nil {
		t.Fatalf("growth = (%q, %d, %v, %v, %v), want the failed first attempt", file, written, done, changed, gerr)
	}
}

func TestProduceStatsRideIntoTheArtifact(t *testing.T) {
	p := New(testOptions())
	sp, err := spool.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	want := model.DumpStats{SkippedEntries: 3, PaddedFiles: 1}
	src := &fakeSource{data: []byte("bytes"), stats: want}
	pa, err := produced(t, p, sp, testPlan(src), nil)
	if err != nil {
		t.Fatal(err)
	}
	if pa.Stats != want {
		t.Fatalf("stats %+v, want %+v", pa.Stats, want)
	}
	if pa.Bytes != int64(len("bytes")) || pa.Raw != int64(len("bytes")) {
		t.Fatalf("sizes bytes=%d raw=%d", pa.Bytes, pa.Raw)
	}
	if pa.SHA256 == "" || pa.File == "" {
		t.Fatalf("artifact identity missing: %+v", pa)
	}
}

func TestProduceStatsFailureFailsTheArtifact(t *testing.T) {
	p := New(testOptions())
	dir := t.TempDir()
	sp, err := spool.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	src := &fakeSource{data: []byte("bytes"), statsErr: errors.New("the tree listing is gone")}
	g := NewGrowth()
	_, err = produced(t, p, sp, testPlan(src), g)
	if err == nil || !strings.Contains(err.Error(), "the tree listing is gone") {
		t.Fatalf("err = %v, want the report failure", err)
	}
	if names := spoolNames(t, dir); len(names) != 0 {
		t.Fatalf("a file survived the failed artifact: %v", names)
	}
	if _, _, done, _, gerr := g.Snapshot(); !done || gerr == nil {
		t.Fatalf("the growth was not ended with the failure: done=%v err=%v", done, gerr)
	}
}

// bareSource is a source with no optional capability at all.
type bareSource struct{ data []byte }

func (b *bareSource) Describe(context.Context) (source.SourceTraits, error) {
	return source.SourceTraits{Ext: ".dat", Origin: "test://bare"}, nil
}

func (b *bareSource) Dump(_ context.Context, dst io.Writer) error {
	_, err := dst.Write(b.data)
	return err
}

func TestProduceWithoutAStatsReporterLeavesZeroStats(t *testing.T) {
	p := New(testOptions())
	sp, err := spool.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	pa, err := produced(t, p, sp, testPlan(&bareSource{data: []byte("bytes")}), nil)
	if err != nil {
		t.Fatal(err)
	}
	if pa.Stats != (model.DumpStats{}) {
		t.Fatalf("stats %+v, want nothing to report", pa.Stats)
	}
}

func TestGrowthEndsSuccessfullyOnTheFirstAttempt(t *testing.T) {
	p := New(testOptions())
	sp, err := spool.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	g := NewGrowth()
	pa, err := produced(t, p, sp, testPlan(&fakeSource{data: []byte("bytes")}), g)
	if err != nil {
		t.Fatal(err)
	}
	file, written, done, changed, gerr := g.Snapshot()
	if !done || gerr != nil || file != pa.File || written != pa.Bytes || changed == nil {
		t.Fatalf("growth = (%q, %d, %v, %v, %v), want the whole first attempt", file, written, done, changed, gerr)
	}
}

func TestProduceStatsUnsupportedIsEmpty(t *testing.T) {
	p := New(testOptions())
	sp, err := spool.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	src := &fakeSource{data: []byte("bytes"), statsErr: errors.ErrUnsupported}
	pa, err := produced(t, p, sp, testPlan(src), nil)
	if err != nil {
		t.Fatal(err)
	}
	if pa.Stats != (model.DumpStats{}) {
		t.Fatalf("stats %+v, want the zero report", pa.Stats)
	}
}

func TestProduceDumpFailureEndsTheGrowth(t *testing.T) {
	p := New(testOptions())
	sp, err := spool.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	src := &fakeSource{data: []byte("bytes"), dumpErr: pluginerrors.Permanent(errors.New("dump refused"))}
	g := NewGrowth()
	_, err = produced(t, p, sp, testPlan(src), g)
	if err == nil || !strings.Contains(err.Error(), "dump refused") {
		t.Fatalf("err = %v, want the dump failure", err)
	}
	if src.dumps != 1 {
		t.Fatalf("dumps = %d, want one attempt for a permanent failure", src.dumps)
	}
	if _, _, done, _, gerr := g.Snapshot(); !done || gerr == nil || !strings.Contains(gerr.Error(), "dump refused") {
		t.Fatalf("growth err = %v, want the dump failure", gerr)
	}
}

func TestGrowthWriterAnnouncesOnlyAcceptedBytes(t *testing.T) {
	g := NewGrowth()
	gw := g.writer(refusingWriter{})
	n, err := gw.Write([]byte("x"))
	if n != 0 || err == nil {
		t.Fatalf("write = (%d, %v), want the writer's refusal", n, err)
	}
	if _, written, _, _, _ := g.Snapshot(); written != 0 {
		t.Fatalf("written = %d, want a refused write to announce nothing", written)
	}
}

// refusingWriter accepts no byte.
type refusingWriter struct{}

func (refusingWriter) Write([]byte) (int, error) { return 0, errors.New("refused") }

func TestLimitWriterRefusesTheCrossingWrite(t *testing.T) {
	var buf bytes.Buffer
	l := &limitWriter{w: &buf, limit: 5}
	if _, err := l.Write([]byte("abc")); err != nil {
		t.Fatal(err)
	}
	_, err := l.Write([]byte("def"))
	if err == nil || !strings.Contains(err.Error(), "cap is 5") {
		t.Fatalf("err = %v, want the cap refusal", err)
	}
	if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassPermanent {
		t.Fatalf("class %q, want permanent", class)
	}
	if buf.String() != "abc" {
		t.Fatalf("the refused bytes reached the spool: %q", buf.String())
	}
}

func TestProcessorsAreReportedUnderTheirInstance(t *testing.T) {
	rec := &recorder{}
	o := testOptions()
	o.Log = slog.New(&recHandler{r: rec})
	p := New(o)
	plan := testPlan(&fakeSource{},
		Step{Instance: "zstd3", Type: "zstd", Processor: &fakeProcessor{}})
	var out strings.Builder
	err := Chain(context.Background(), &out, func(_ context.Context, w io.Writer) error {
		_, werr := w.Write([]byte("bytes"))
		return werr
	}, p.stages(plan))
	if err != nil {
		t.Fatal(err)
	}
	done, ok := rec.find("processor.done")
	if !ok || done.attrs["instance"] != "zstd3" || done.attrs["type"] != "zstd" {
		t.Fatalf("done event %+v (%d events)", done, rec.count())
	}
}

func TestAFailingProcessorIsReportedUnderItsInstance(t *testing.T) {
	rec := &recorder{}
	o := testOptions()
	o.Log = slog.New(&recHandler{r: rec})
	p := New(o)
	plan := testPlan(&fakeSource{},
		Step{Instance: "crypt", Type: "age", Processor: &fakeProcessor{encodeErr: errors.New("encode refused")}})
	var out strings.Builder
	err := Chain(context.Background(), &out, func(_ context.Context, w io.Writer) error {
		_, werr := w.Write([]byte("bytes"))
		return werr
	}, p.stages(plan))
	if err == nil || !strings.Contains(err.Error(), "encode refused") {
		t.Fatalf("err = %v, want the step failure", err)
	}
	failed, ok := rec.find("processor.failed")
	if !ok || failed.attrs["instance"] != "crypt" || failed.attrs["type"] != "age" {
		t.Fatalf("failed event %+v (%d events)", failed, rec.count())
	}
}

// TestDumpEmitsTheSourceStartEvent: a stage dumps through Dump, and the operator
// must see the same start line a run's produce writes — under the source's own
// instance and naming where the bytes came from.
func TestDumpEmitsTheSourceStartEvent(t *testing.T) {
	rec := &recorder{}
	o := testOptions()
	o.Log = slog.New(&recHandler{r: rec})
	p := New(o)
	plan := testPlan(&fakeSource{data: []byte("bytes"),
		traits: source.SourceTraits{Origin: "test://stage/source"}})
	res, err := p.Resolve(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	var out strings.Builder
	raw, err := p.Dump(context.Background(), &out, plan, res)
	if err != nil {
		t.Fatal(err)
	}
	if out.String() != "bytes" || raw != int64(len("bytes")) {
		t.Fatalf("dump wrote %q and counted %d raw bytes", out.String(), raw)
	}
	start, ok := rec.find("source.dump.start")
	if !ok || start.attrs["instance"] != "vpn" || start.attrs["type"] != "fake" {
		t.Fatalf("start event %+v (%d events)", start, rec.count())
	}
	if got := attrText(start.event, "origin"); got != "test://stage/source" {
		t.Fatalf("origin = %q, want the source's own", got)
	}
	if got := rec.named("source.dump.start"); got != 1 {
		t.Fatalf("%d start events, want exactly one per dump", got)
	}
}

// TestProduceAnnouncesTheDumpOnceAcrossRetries: the produce path keeps its
// single start event per artifact, however many attempts the retry policy spends.
func TestProduceAnnouncesTheDumpOnceAcrossRetries(t *testing.T) {
	rec := &recorder{}
	o := testOptions()
	o.Log = slog.New(&recHandler{r: rec})
	p := New(o)
	sp, err := spool.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	src := &fakeSource{data: []byte("bytes"), dumpOnce: errors.New("transient dump")}
	if _, err := produced(t, p, sp, testPlan(src), nil); err != nil {
		t.Fatal(err)
	}
	if src.dumps != 2 {
		t.Fatalf("dumps = %d, want a retry", src.dumps)
	}
	if got := rec.named("source.dump.start"); got != 1 {
		t.Fatalf("%d start events, want exactly one per artifact", got)
	}
}

func TestTheSourceEventNamesTheOrigin(t *testing.T) {
	rec := &recorder{}
	o := testOptions()
	o.Log = slog.New(&recHandler{r: rec})
	p := New(o)
	sp, err := spool.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := produced(t, p, sp, testPlan(&fakeSource{data: []byte("x")}), nil); err != nil {
		t.Fatal(err)
	}
	start, ok := rec.find("source.dump.start")
	if !ok || start.attrs["instance"] != "vpn" || start.attrs["type"] != "fake" {
		t.Fatalf("source event %+v (%d events)", start, rec.count())
	}
	if got := attrText(start.event, "origin"); got != "test://fake/source" {
		t.Fatalf("origin = %q, want the source's own", got)
	}
}

// attrText returns the text of one event attribute.
func attrText(ev human.Event, key string) string {
	for _, a := range ev.Attrs {
		if a.Key == key {
			return a.Text
		}
	}
	return ""
}

func TestProcessorEventNamesAnAnonymousStep(t *testing.T) {
	p := New(testOptions())
	plan := testPlan(&fakeSource{data: []byte("bytes")},
		Step{Processor: &fakeProcessor{encodeErr: errors.New("encode refused")}})
	var out strings.Builder
	err := Chain(context.Background(), &out, func(_ context.Context, w io.Writer) error {
		_, werr := w.Write([]byte("x"))
		return werr
	}, p.stages(plan))
	if err == nil || !strings.Contains(err.Error(), "encode refused") {
		t.Fatalf("err = %v, want the step failure", err)
	}
}

// spoolNames lists the files currently in the spool directory.
func spoolNames(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

// TestResolveRefusesPlaintextCredentialsBeforeTheFirstByte: bytes a source
// names as credentials are refused unless something seals them — before a byte
// is produced, not after the artifact is uploaded readable.
func TestResolveRefusesPlaintextCredentialsBeforeTheFirstByte(t *testing.T) {
	p := New(testOptions())
	src := &fakeSource{traits: source.SourceTraits{Credentials: true}}
	_, err := p.Resolve(context.Background(), testPlan(src))
	if err == nil {
		t.Fatal("unsealed credential bytes were accepted")
	}
	if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassPermanent {
		t.Fatalf("class = %q, want permanent", class)
	}
	if src.dumps != 0 {
		t.Fatalf("the source was dumped %d times before the refusal", src.dumps)
	}
}

// TestResolveWarnsForThePublishedCredentialException: a source type that answers
// the warning verdict keeps its published plaintext configuration working and
// the run records the coded warning on the artifact, not on a log line only.
func TestResolveWarnsForThePublishedCredentialException(t *testing.T) {
	p := New(testOptions())
	src := &credentialSource{verdict: source.CredentialWarn}
	src.traits = source.SourceTraits{Credentials: true}
	sp, err := spool.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	plan := testPlan(src)
	res, err := p.Resolve(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	if src.calls != 1 {
		t.Fatalf("the credential policy was asked %d times", src.calls)
	}
	if len(res.Warnings) != 1 || res.Warnings[0].Code != source.CredentialWarningCode {
		t.Fatalf("resolved warnings = %+v", res.Warnings)
	}
	artifact, err := p.Produce(context.Background(), sp, plan, res, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(artifact.Warnings) != 1 || artifact.Warnings[0] != res.Warnings[0] {
		t.Fatalf("artifact warnings = %+v, want %+v", artifact.Warnings, res.Warnings)
	}
}

// TestResolveCredentialVerdictsThatDoNotWarn: a policy error fails the artifact,
// an unsupported policy is the zero refuse verdict, and sealed bytes are never
// asked about at all.
func TestResolveCredentialVerdictsThatDoNotWarn(t *testing.T) {
	p := New(testOptions())
	broken := &credentialSource{err: errors.New("policy unavailable")}
	broken.traits = source.SourceTraits{Credentials: true}
	if _, err := p.Resolve(context.Background(), testPlan(broken)); err == nil {
		t.Fatal("a failing credential policy was accepted")
	}

	unsupported := &credentialSource{err: errors.ErrUnsupported}
	unsupported.traits = source.SourceTraits{Credentials: true}
	if _, err := p.Resolve(context.Background(), testPlan(unsupported)); err == nil {
		t.Fatal("an unsupported credential policy was accepted")
	}

	sealed := &credentialSource{verdict: source.CredentialWarn}
	sealed.traits = source.SourceTraits{Credentials: true, Sealed: true}
	res, err := p.Resolve(context.Background(), testPlan(sealed))
	if err != nil {
		t.Fatal(err)
	}
	if sealed.calls != 0 {
		t.Fatalf("sealed bytes asked for a credential verdict %d times", sealed.calls)
	}
	if len(res.Warnings) != 0 {
		t.Fatalf("sealed bytes carried %+v", res.Warnings)
	}

	sealingStep := &fakeSource{traits: source.SourceTraits{Credentials: true}}
	res, err = p.Resolve(context.Background(), testPlan(sealingStep,
		Step{Instance: "seal", Type: "age", Processor: &fakeProcessor{traits: processor.ProcessorTraits{Ext: ".age", Seals: true}}}))
	if err != nil {
		t.Fatal(err)
	}
	if !res.Artifact.Sealed || len(res.Warnings) != 0 {
		t.Fatalf("a sealing step = sealed %v, warnings %+v", res.Artifact.Sealed, res.Warnings)
	}
}

// TestResolveCarriesTheSourcesOwnWarnings: a staged record's findings reach the
// resolved artifact and the manifest beside the credential decision, and they
// never fail the artifact.
func TestResolveCarriesTheSourcesOwnWarnings(t *testing.T) {
	src := &fakeSource{traits: source.SourceTraits{
		Warnings: []model.Warning{{Code: "stage.carried", Message: "from the stage record"}},
	}}
	res, err := New(testOptions()).Resolve(context.Background(), testPlan(src))
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Warnings) != 1 || res.Warnings[0].Code != "stage.carried" {
		t.Fatalf("resolved warnings = %+v", res.Warnings)
	}
}
