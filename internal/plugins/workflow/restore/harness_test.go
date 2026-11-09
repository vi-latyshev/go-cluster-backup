package restore

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/configuration"
	pluginerrors "github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/execution"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/inventory"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/layout"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/processor"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage/guard"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage/storagetest"
)

// testWorld is the workflow harness: the fakes a test seeds and the typed
// options the workflow is built from.
type testWorld struct {
	t          *testing.T
	names      []string
	fakes      map[string]*storagetest.Fake
	readers    map[string]storage.Reader
	processors map[string]processor.Processor
	types      map[string]string
	targets    Targets
	order      []string
	configured bool
	spoolDir   string
	reserve    int64
	layoutCfg  layout.Config
	apply      map[string]json.RawMessage
}

func testLayoutConfig(t *testing.T) layout.Config {
	t.Helper()
	return layout.Config{
		IDLayout:       "20060102T150405Z",
		RunPrefix:      mustTemplate(t, "runs/{{ .Run.ID }}"),
		ArtifactKey:    mustTemplate(t, "{{ .Artifact.Group }}/{{ .Artifact.Name }}{{ .Artifact.Ext }}"),
		ManifestKey:    mustTemplate(t, "manifest.json"),
		ManualIDSuffix: "-manual",
	}
}

func mustTemplate(t *testing.T, text string) configuration.Template {
	t.Helper()
	raw, err := json.Marshal(map[string]any{"t": text})
	if err != nil {
		t.Fatal(err)
	}
	var cfg struct {
		T configuration.Template `json:"t"`
	}
	if err := configuration.Decode(raw, &cfg); err != nil {
		t.Fatal(err)
	}
	return cfg.T
}

func newWorld(t *testing.T) *testWorld {
	t.Helper()
	w := &testWorld{
		t:          t,
		fakes:      map[string]*storagetest.Fake{},
		readers:    map[string]storage.Reader{},
		processors: map[string]processor.Processor{},
		types:      map[string]string{},
		configured: true,
		spoolDir:   t.TempDir(),
		layoutCfg:  testLayoutConfig(t),
	}
	w.processor("zstd3")
	return w
}

// addStorage declares one storage and returns its fake.
func (w *testWorld) addStorage(name string) *storagetest.Fake {
	if _, ok := w.fakes[name]; ok {
		return w.fakes[name]
	}
	f := storagetest.NewFake()
	w.fakes[name] = f
	w.names = append(w.names, name)
	w.order = append(w.order, name)
	return f
}

// replace swaps the reader one storage name hands the workflow: the fault
// injection point for a reader that must fail outside the fake's own knobs.
func (w *testWorld) replace(name string, r storage.Reader) { w.readers[name] = r }

// storage returns the fake declared under one name.
func (w *testWorld) storage(name string) *storagetest.Fake {
	w.t.Helper()
	f, ok := w.fakes[name]
	if !ok {
		w.t.Fatalf("storage %q is not declared", name)
	}
	return f
}

func (w *testWorld) processor(name string) *fakeProcessor {
	p, ok := w.processors[name].(*fakeProcessor)
	if !ok {
		p = &fakeProcessor{}
		w.processors[name] = p
		w.types[name] = "fake"
	}
	return p
}

// flow builds the workflow from the world's options, applying the overrides.
func (w *testWorld) flow(overrides ...func(*Options)) *Workflow {
	w.t.Helper()
	storages := make([]Storage, 0, len(w.names))
	for _, name := range w.names {
		var r storage.Reader = w.fakes[name]
		if custom, ok := w.readers[name]; ok {
			r = custom
		}
		storages = append(storages, Storage{
			Name: name, Port: guard.Read(name, r), CallParallelism: 2,
		})
	}
	o := Options{
		Layout:         layout.New(w.layoutCfg),
		Policy:         execution.Options{Attempts: 1, Initial: time.Millisecond, Max: time.Millisecond, Unclassified: pluginerrors.ClassTransient},
		Configured:     w.configured,
		SpoolDir:       w.spoolDir,
		Reserve:        w.reserve,
		Order:          w.order,
		Storages:       storages,
		Processors:     w.processors,
		ProcessorTypes: w.types,
		Targets:        w.targets,
		ApplyOptions:   w.apply,
	}
	for _, f := range overrides {
		f(&o)
	}
	return New(o)
}

// covManifest is one valid run manifest: one ok artifact with the recorded key
// and the bytes' own size and hash.
func covManifest(runID, artifact, key string, data []byte) *model.RunReport {
	return &model.RunReport{
		Schema: model.ManifestSchema, RunID: runID, Kind: model.RunKindScheduled, StartedAt: time.Unix(1700000000, 0).UTC(),
		Outcome: model.OutcomeDelivered,
		Artifacts: []model.ArtifactResult{{
			Artifact: model.Artifact{Name: artifact, Group: "vpn", Sensitivity: model.SensitivityPlain},
			Outcome:  model.ArtifactOK,
			Key:      key,
			Bytes:    int64(len(data)),
			RawBytes: int64(len(data)),
			SHA256:   model.SHA256Hex(data),
			Chain:    []model.ProcessorStep{{Instance: "zstd3", Type: "fake"}},
			Source:   model.SourceRef{Instance: "vpn", Type: "fake", Origin: "test://fake/source"},
		}},
	}
}

func failedManifest(runID string) *model.RunReport {
	rep := covManifest(runID, "wireguard", "k", []byte("x"))
	rep.Artifacts[0].Outcome = model.ArtifactFailed
	return rep
}

// seed writes one run's manifest, and the artifact object when data is given.
func seed(t *testing.T, f *storagetest.Fake, rep *model.RunReport, key string, data []byte) {
	t.Helper()
	f.PutObject("runs/"+rep.RunID+"/manifest.json", mustMarshal(t, rep), time.Unix(1700000000, 0).UTC())
	if key != "" {
		f.PutObject(key, data, time.Unix(1700000000, 0).UTC())
	}
}

// covSurvey is one storage's survey answer over the same manifest shapes the
// inventory would produce from a listing.
func covSurvey(runs map[string]*model.RunReport) inventory.Survey {
	out := inventory.Survey{Storage: "primary", OK: true, Runs: map[string]inventory.Run{}}
	for id, rep := range runs {
		r := inventory.Run{Manifest: rep}
		if rep != nil {
			r.Stored = model.StoredRun{RunID: id, Kind: rep.Kind, At: rep.StartedAt, Manifest: true}
			if rep.Schema != model.ManifestSchema {
				r.ManifestProblem = "manifest schema is not what this build reads"
			}
			if rep.Kind != model.RunKindScheduled && rep.Kind != model.RunKindManual {
				r.ManifestProblem = "the manifest records no kind"
			}
		}
		out.Runs[id] = r
	}
	return out
}

func mustMarshal(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func readDirNames(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names, nil
}

// --- fakes -------------------------------------------------------------------

// fakeProcessor is a processor whose every optional behaviour a test scripts.
type fakeProcessor struct {
	decodeErr      error
	checkDecodeErr error
	panicNow       bool
	ext            string
	seals          bool
}

func (p *fakeProcessor) Check(context.Context) error { return nil }

func (p *fakeProcessor) Describe(context.Context) (processor.ProcessorTraits, error) {
	ext := p.ext
	if ext == "" {
		ext = ".zst"
	}
	return processor.ProcessorTraits{Ext: ext, Seals: p.seals}, nil
}

func (p *fakeProcessor) Encode(_ context.Context, dst io.Writer, src io.Reader) error {
	if p.panicNow {
		panic("test processor panic")
	}
	_, err := io.Copy(dst, src)
	return err
}

func (p *fakeProcessor) Decode(_ context.Context, dst io.Writer, src io.Reader) error {
	if p.panicNow {
		panic("test processor panic")
	}
	if p.decodeErr != nil {
		return p.decodeErr
	}
	_, err := io.Copy(dst, src)
	return err
}

func (p *fakeProcessor) CheckDecode(context.Context) error { return p.checkDecodeErr }

// fakeSource is a source with neither the Restorer nor the Prober capability.
type fakeSource struct {
	traits      source.SourceTraits
	describeErr error
	closed      int
}

func (s *fakeSource) Describe(context.Context) (source.SourceTraits, error) {
	if s.describeErr != nil {
		return source.SourceTraits{}, s.describeErr
	}
	tr := s.traits
	if tr.Origin == "" {
		tr.Origin = "test://fake/source"
	}
	return tr, nil
}

func (s *fakeSource) Dump(context.Context, io.Writer) error { return errors.New("not a dump source") }

func (s *fakeSource) Close() error { s.closed++; return nil }

// restorerSource is a target that can restore, without the Prober capability.
type restorerSource struct {
	*fakeSource
	dump io.Writer
	rep  source.ApplyReport
	err  error
}

func (s *restorerSource) Apply(_ context.Context, _ source.ApplyRequest, src io.Reader) (source.ApplyReport, error) {
	if s.dump != nil {
		_, _ = io.Copy(s.dump, src)
	}
	return s.rep, s.err
}

// probeSource is a target that can probe and restore, remembering the probe.
type probeSource struct {
	*fakeSource
	dump       io.Writer
	probes     int
	probeRep   source.ProbeReport
	probeErr   error
	probePanic bool
}

func (s *probeSource) Probe(_ context.Context, _ io.Reader) (source.ProbeReport, error) {
	s.probes++
	if s.probePanic {
		panic("test prober panic")
	}
	return s.probeRep, s.probeErr
}

func (s *probeSource) Apply(_ context.Context, _ source.ApplyRequest, src io.Reader) (source.ApplyReport, error) {
	if s.dump != nil {
		_, _ = io.Copy(s.dump, src)
	}
	return source.ApplyReport{}, nil
}

// fakeTargets resolves one static target by name, or reports its own error.
type fakeTargets struct {
	target Target
	err    error
	closer io.Closer
	asked  []string
}

func (f *fakeTargets) ResolveTarget(_ context.Context, name, artifact string) (Target, io.Closer, error) {
	f.asked = append(f.asked, name+"|"+artifact)
	return f.target, f.closer, f.err
}

// covStorage is the fake with one operation's answer replaced.
type covStorage struct {
	*storagetest.Fake
	statInfo model.ObjectInfo
	statErr  error
	getData  []byte
	getErr   error
	closeW   bool
	onGet    func()
}

func (s *covStorage) Stat(ctx context.Context, key string) (model.ObjectInfo, error) {
	if s.statErr == nil && s.statInfo.Size == 0 {
		return s.Fake.Stat(ctx, key)
	}
	return s.statInfo, s.statErr
}

func (s *covStorage) Get(ctx context.Context, key string, dst io.Writer) error {
	if s.onGet != nil {
		s.onGet()
	}
	if s.getErr != nil {
		return s.getErr
	}
	if s.getData != nil {
		if _, err := dst.Write(s.getData); err != nil {
			return err
		}
		if s.closeW {
			if c, ok := dst.(io.Closer); ok {
				return c.Close()
			}
		}
		return nil
	}
	return s.Fake.Get(ctx, key, dst)
}
