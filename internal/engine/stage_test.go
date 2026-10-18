package engine

import (
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/processor"
	"github.com/vi-latyshev/go-cluster-backup/internal/source"
)

func stageConfig(t *testing.T, dir string) map[string]any {
	t.Helper()
	return map[string]any{
		"version": 1,
		"layout":  configLayout(),
		"stage": map[string]any{
			"dir": dir, "degraded_exit_code": 5, "failed_exit_code": 9,
			"sources": map[string]any{
				"snapshot": sourceInstance("fake", "control-plane", "k3s-state", []string{"seal"}, "required", "secret"),
			},
		},
		"processors": map[string]any{"seal": instance("fake")},
	}
}

func TestStageWritesSealedArtifact(t *testing.T) {
	w := newWorld(t)
	w.processors["seal"] = &fakeProcessor{seals: true, ext: ".age"}
	w.sources["snapshot"] = &fakeSource{data: []byte("sealed bytes")}
	dir := t.TempDir()
	e := w.newEngine(t, stageConfig(t, dir))
	rep, err := e.Stage(context.Background(), StageRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeDelivered || rep.ExitCode != 0 {
		t.Fatalf("report %+v", rep)
	}
	body, err := os.ReadFile(filepath.Join(dir, "k3s-state.dat.age"))
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "sealed bytes" {
		t.Fatalf("staged bytes %q", body)
	}
	if reps, err := readDirNames(dir); err != nil || len(reps) != 2 {
		t.Fatalf("stage dir holds %v (%v), want the artifact and its chain record", reps, err)
	}
	// The record is the half the run side reads back: the artifact's exact file
	// name tail and the processor chain in encode order, each step naming the
	// instance and type a restore resolves. Instance and type come from the
	// plan's refs — a stage that drops them publishes a record nobody can decode
	// with.
	var rec source.StagedRecord
	record, err := os.ReadFile(source.StagedChainFile(dir, "k3s-state"))
	if err != nil {
		t.Fatalf("the chain record: %v", err)
	}
	if err := json.Unmarshal(record, &rec); err != nil {
		t.Fatalf("the chain record does not decode: %v", err)
	}
	if rec.Ext != ".dat.age" {
		t.Fatalf("the record's ext = %q, want the artifact's own .dat then .age", rec.Ext)
	}
	want := model.ProcessorStep{Instance: "seal", Type: "fake", Ext: ".age", Seals: true}
	if len(rec.Chain) != 1 || rec.Chain[0] != want {
		t.Fatalf("the chain record = %+v, want [%+v]", rec.Chain, want)
	}
	if got := source.StagedArtifactName("k3s-state", rec); got != "k3s-state.dat.age" {
		t.Fatalf("the record names %q, want the published file k3s-state.dat.age", got)
	}
	if rep.Artifacts[0].Bytes != int64(len("sealed bytes")) || rep.Artifacts[0].SHA256 == "" {
		t.Fatalf("artifact result %+v", rep.Artifacts[0])
	}
}

func TestStageRefusesUnsealedSecret(t *testing.T) {
	w := newWorld(t)
	src := &fakeSource{data: []byte("plaintext")}
	w.sources["snapshot"] = src
	dir := t.TempDir()
	// The pipeline seals nothing: the refusal is a configuration fact, before
	// the run exists and before the first byte.
	cfg := stageConfig(t, dir)
	cfg["processors"] = map[string]any{"zstd3": instance("fake")}
	cfg["stage"].(map[string]any)["sources"].(map[string]any)["snapshot"].(map[string]any)["pipeline"] = []string{"zstd3"}
	e, err := w.newEngineErr(t, cfg)
	if e != nil {
		t.Fatalf("engine.New built an engine for a refused stage config: %v", err)
	}
	if err == nil {
		t.Fatal("a secret stage artifact without a sealing pipeline was accepted")
	}
	if !strings.Contains(err.Error(), "/stage.sources/snapshot: a secret artifact's pipeline [zstd3] has no sealing processor") {
		t.Fatalf("the refusal does not name the stage instance and its pipeline: %v", err)
	}
	if src.dumpCount() != 0 {
		t.Fatal("the source produced bytes for a refused artifact")
	}
	if entries, err := readDirNames(dir); err == nil && len(entries) != 0 {
		t.Fatalf("refused stage left %v", entries)
	}
}

func TestStageNeedsItsConfig(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	e := w.newEngine(t, cfg)
	if _, err := e.Stage(context.Background(), StageRequest{}); err == nil {
		t.Fatal("Stage ran without a stage section")
	}
}

// TestStageClearsWhatAPreviousStageLeft: a stage directory that survives the
// container — any deployment whose dir is not this cluster's emptyDir — must
// never hand the run yesterday's artifact and record as tonight's after a stage
// that failed: the run cannot tell the two apart. Mutant: without the clear, the
// failed second night leaves the first night's pair in place.
func TestStageClearsWhatAPreviousStageLeft(t *testing.T) {
	w := newWorld(t)
	w.processors["seal"] = &fakeProcessor{seals: true, ext: ".age"}
	src := &fakeSource{data: []byte("sealed bytes")}
	w.sources["snapshot"] = src
	dir := t.TempDir()
	e := w.newEngine(t, stageConfig(t, dir))

	rep, err := e.Stage(context.Background(), StageRequest{})
	if err != nil || rep.Outcome != model.OutcomeDelivered {
		t.Fatalf("first night: %+v, %v", rep, err)
	}
	if names, err := readDirNames(dir); err != nil || len(names) != 2 {
		t.Fatalf("first night left %v (%v), want the artifact and its record", names, err)
	}

	// Second night: the source fails before a byte is written.
	src.dumpErr = plugin.Permanent(errors.New("datastore vanished"))
	rep, err = e.Stage(context.Background(), StageRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeFailed || rep.ExitCode != 9 {
		t.Fatalf("second night: %+v", rep)
	}
	if names, err := readDirNames(dir); err != nil || len(names) != 0 {
		t.Fatalf("a failed stage left %v (%v); the run would upload them as tonight's", names, err)
	}
}

// TestStageKeepsTheArtifactsOfPrefixNamedSources: clearing what a previous run
// left for one artifact must never reach another whose name merely starts with
// it — "k3s" against "k3s-state" and against "k3s.v2". The instances are named
// so the prefix artifact is produced last: with a bare name prefix as the
// pattern, its clear would delete the artifacts and records this same run had
// just published for its neighbours.
func TestStageKeepsTheArtifactsOfPrefixNamedSources(t *testing.T) {
	w := newWorld(t)
	dir := t.TempDir()
	cfg := map[string]any{
		"version": 1,
		"layout":  configLayout(),
		"stage": map[string]any{"dir": dir, "degraded_exit_code": 5, "failed_exit_code": 9,
			"sources": map[string]any{
				"aa": sourceInstance("fake", "control-plane", "k3s-state", []string{}, "required", "plain"),
				"bb": sourceInstance("fake", "control-plane", "k3s.v2", []string{}, "required", "plain"),
				"cc": sourceInstance("fake", "control-plane", "k3s", []string{}, "required", "plain"),
			}},
		"processors": map[string]any{},
	}
	e := w.newEngine(t, cfg)
	rep, err := e.Stage(context.Background(), StageRequest{})
	if err != nil || rep.Outcome != model.OutcomeDelivered {
		t.Fatalf("stage: %+v, %v", rep, err)
	}
	for _, want := range []string{
		"k3s-state.dat", ".chain-k3s-state.json",
		"k3s.v2.dat", ".chain-k3s.v2.json",
		"k3s.dat", ".chain-k3s.json",
	} {
		if _, err := os.Stat(filepath.Join(dir, want)); err != nil {
			names, _ := readDirNames(dir)
			t.Fatalf("%s is missing after the stage (%v): a neighbouring artifact's clear took it", want, names)
		}
	}
}

func TestRunSavesTheCatalog(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["catalog"] = instance("fake")
	e := w.newEngine(t, cfg)
	if _, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled}); err != nil {
		t.Fatal(err)
	}
	if len(w.catalog.runs) == 0 {
		t.Fatal("catalog is empty after a run")
	}
}

func TestDescriptorsComeFromTheRegistry(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	e := w.newEngine(t, cfg)
	descs, err := e.Descriptors(context.Background())
	if err != nil || len(descs) == 0 {
		t.Fatalf("descriptors %v, %v", descs, err)
	}
}

// TestStagePublishesAReadableArtifact: the stage container writes the file and
// the run container reads it under another uid, so the published mode is a
// deployment contract — 0644, not the 0600 CreateTemp leaves behind.
func TestStagePublishesAReadableArtifact(t *testing.T) {
	w := newWorld(t)
	dir := t.TempDir()
	cfg := map[string]any{
		"version": 1,
		"layout":  configLayout(),
		"stage": map[string]any{"dir": dir, "degraded_exit_code": 3, "failed_exit_code": 7,
			"sources": map[string]any{"vpn": sourceInstance("fake", "vpn", "wireguard", []string{}, "required", "plain")}},
		"processors": map[string]any{},
	}
	e := w.newEngine(t, cfg)
	rep, err := e.Stage(context.Background(), StageRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeDelivered {
		t.Fatalf("stage report %+v", rep)
	}
	matches, err := filepath.Glob(filepath.Join(dir, "wireguard*"))
	if err != nil || len(matches) != 1 {
		t.Fatalf("staged files %v, %v", matches, err)
	}
	st, err := os.Stat(matches[0])
	if err != nil {
		t.Fatal(err)
	}
	if got := st.Mode().Perm(); got != 0o644 {
		t.Fatalf("staged artifact mode %v, want 0644: the reading container may run as the image's unprivileged default %v", got, got)
	}
	body, err := os.ReadFile(matches[0])
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != string(w.sources["vpn"].data) {
		t.Fatalf("staged bytes %q", body)
	}
}

// TestStageRetriesATransientDumpFailure: a stage config that carries a retry
// policy gets its attempts — the engine's loop, not the plugin's.
func TestStageRetriesATransientDumpFailure(t *testing.T) {
	w := newWorld(t)
	dir := t.TempDir()
	cfg := map[string]any{
		"version": 1,
		"run":     configRun(t.TempDir()),
		"layout":  configLayout(),
		"stage": map[string]any{"dir": dir, "degraded_exit_code": 3, "failed_exit_code": 7,
			"sources": map[string]any{"vpn": sourceInstance("fake", "vpn", "wireguard", []string{}, "required", "plain")}},
		"processors": map[string]any{},
	}
	e := w.newEngine(t, cfg)
	w.sources["vpn"].dumpOnce = plugin.Transient(errors.New("signal: killed"))
	rep, err := e.Stage(context.Background(), StageRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeDelivered {
		t.Fatalf("stage report %+v: one transient failure cost the snapshot", rep)
	}
}

// TestStageRefusesAMultiUnitSource: a stage writes one sealed file per artifact
// and the run side reads it back by that one name. An instance whose domain
// holds several artifacts has no such name, and staging only the first of them
// would stage a fraction of the data under a name that reads as all of it.
func TestStageRefusesAMultiUnitSource(t *testing.T) {
	w := newWorld(t)
	w.processors["seal"] = &fakeProcessor{seals: true, ext: ".age"}
	src := &fakeSource{data: []byte("parent"), units: []source.Unit{
		{Name: "one", Config: jsontext.Value(`{}`)},
		{Name: "two", Config: jsontext.Value(`{}`)},
	}}
	w.extraSources["snapshot"] = src
	dir := t.TempDir()

	e := w.newEngine(t, stageConfig(t, dir))
	rep, err := e.Stage(context.Background(), StageRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeFailed || rep.ExitCode != 9 {
		t.Fatalf("report %+v", rep)
	}
	if len(rep.Artifacts) != 1 || !strings.Contains(rep.Artifacts[0].Error, "stage does not expand a multi-unit source") {
		t.Fatalf("artifacts %+v", rep.Artifacts)
	}
	if src.dumpCount() != 0 {
		t.Fatal("a multi-unit source was staged anyway")
	}
	if names, err := readDirNames(dir); err != nil || len(names) != 0 {
		t.Fatalf("the stage directory holds %v, %v", names, err)
	}
}

func registerStageProcessor(t *testing.T, w *world, typ string, mk func() processor.Processor) {
	t.Helper()
	err := plugin.Register(w.registry, processor.Kind, plugin.Spec[map[string]jsontext.Value, processor.Processor]{
		Type: typ, Example: []byte(`{}`),
		New: func(context.Context, map[string]jsontext.Value, plugin.Deps) (processor.Processor, error) {
			return mk(), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
}

func registerStageProcessorRequiring(t *testing.T, w *world, typ, bin string) {
	t.Helper()
	err := plugin.Register(w.registry, processor.Kind, plugin.Spec[map[string]jsontext.Value, processor.Processor]{
		Type: typ, Example: []byte(`{}`), Requires: []string{bin},
		New: func(context.Context, map[string]jsontext.Value, plugin.Deps) (processor.Processor, error) {
			return &fakeProcessor{}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
}

// --- stage.go ----------------------------------------------------------------

// TestStageNeedsItsSources: a stage section with a dir but no sources has
// nothing to produce — a configuration fact, refused before any plugin runs.
func TestStageNeedsItsSources(t *testing.T) {
	w := newWorld(t)
	cfg := map[string]any{
		"version": 1, "layout": configLayout(),
		"stage":      map[string]any{"dir": t.TempDir(), "degraded_exit_code": 3, "failed_exit_code": 7},
		"processors": map[string]any{},
	}
	e := w.newEngine(t, cfg)
	_, err := e.Stage(context.Background(), StageRequest{})
	if err == nil || !strings.Contains(err.Error(), "no stage.sources") {
		t.Fatalf("Stage with an empty stage.sources = %v, want the missing-sources refusal", err)
	}
}

// TestStageRefusesAMissingBinary: a stage processor whose plugin type
// requires a binary PATH does not have stops the stage before any plugin work.
func TestStageRefusesAMissingBinary(t *testing.T) {
	w := newWorld(t)
	registerStageProcessorRequiring(t, w, "zzneedsbin", "zz-definitely-not-a-real-binary")
	dir := t.TempDir()
	cfg := map[string]any{
		"version": 1, "layout": configLayout(),
		"stage": map[string]any{"dir": dir, "degraded_exit_code": 3, "failed_exit_code": 7,
			"sources": map[string]any{"snapshot": sourceInstance("fake", "g", "n", []string{"seal"}, "required", "plain")}},
		"processors": map[string]any{"seal": instance("zzneedsbin")},
	}
	e := w.newEngine(t, cfg)
	_, err := e.Stage(context.Background(), StageRequest{})
	if err == nil || !strings.Contains(err.Error(), "required binaries are not on PATH") {
		t.Fatalf("Stage with a missing required binary = %v, want the preflight refusal", err)
	}
	if entries, _ := readDirNames(dir); len(entries) != 0 {
		t.Fatalf("a refused stage left %v", entries)
	}
}

// TestStageBestEffortFailureDegrades: a best-effort artifact whose stage
// failed degrades the stage — the configured degraded exit code, not the failed
// one — while the required case is proven by TestStageClearsWhatAPreviousStageLeft.
func TestStageBestEffortFailureDegrades(t *testing.T) {
	w := newWorld(t)
	src := &fakeSource{data: []byte("bytes"), dumpErr: plugin.Permanent(errors.New("dump vanished"))}
	w.extraSources["snapshot"] = src
	dir := t.TempDir()
	cfg := map[string]any{
		"version": 1, "layout": configLayout(),
		"stage": map[string]any{"dir": dir, "degraded_exit_code": 5, "failed_exit_code": 9,
			"sources": map[string]any{"snapshot": sourceInstance("fake", "g", "n", []string{}, "best_effort", "plain")}},
		"processors": map[string]any{},
	}
	e := w.newEngine(t, cfg)
	rep, err := e.Stage(context.Background(), StageRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeDegraded || rep.ExitCode != 5 {
		t.Fatalf("a best-effort stage failure = outcome %q exit %d, want degraded and 5", rep.Outcome, rep.ExitCode)
	}
	if len(rep.Artifacts) != 1 || rep.Artifacts[0].Outcome != model.ArtifactFailed {
		t.Fatalf("artifacts %+v", rep.Artifacts)
	}
}

// TestStageOneProcessorCheckFails: a processor that failed its own Check
// fails every artifact routed through it, before any byte is produced.
func TestStageOneProcessorCheckFails(t *testing.T) {
	w := newWorld(t)
	w.processors["seal"] = &fakeProcessor{checkErr: errors.New("no key material")}
	src := &fakeSource{data: []byte("bytes")}
	w.sources["snapshot"] = src
	dir := t.TempDir()
	cfg := map[string]any{
		"version": 1, "layout": configLayout(),
		"stage": map[string]any{"dir": dir, "degraded_exit_code": 3, "failed_exit_code": 7,
			"sources": map[string]any{"snapshot": sourceInstance("fake", "g", "n", []string{"seal"}, "required", "plain")}},
		"processors": map[string]any{"seal": instance("fake")},
	}
	e := w.newEngine(t, cfg)
	rep, err := e.Stage(context.Background(), StageRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeFailed || len(rep.Artifacts) != 1 {
		t.Fatalf("report %+v", rep)
	}
	if !strings.Contains(rep.Artifacts[0].Error, "processor seal check") {
		t.Fatalf("artifact error %q, want the processor check failure", rep.Artifacts[0].Error)
	}
	if src.dumpCount() != 0 {
		t.Fatal("bytes were produced for an artifact whose processor failed its check")
	}
}

// TestStageOneRefusesANonPlainName: a stage writes one file per artifact and
// the run side reads it back by that name, so a name carrying a separator or a
// leading dot is refused.
func TestStageOneRefusesANonPlainName(t *testing.T) {
	for _, name := range []string{"bad/name", ".hidden"} {
		t.Run(name, func(t *testing.T) {
			w := newWorld(t)
			src := &fakeSource{data: []byte("bytes")}
			w.extraSources["snapshot"] = src
			dir := t.TempDir()
			cfg := map[string]any{
				"version": 1, "layout": configLayout(),
				"stage": map[string]any{"dir": dir, "degraded_exit_code": 3, "failed_exit_code": 7,
					"sources": map[string]any{"snapshot": sourceInstance("fake", "g", name, []string{}, "required", "plain")}},
				"processors": map[string]any{},
			}
			e := w.newEngine(t, cfg)
			rep, err := e.Stage(context.Background(), StageRequest{})
			if err != nil {
				t.Fatal(err)
			}
			if len(rep.Artifacts) != 1 || !strings.Contains(rep.Artifacts[0].Error, "must be a plain file name") {
				t.Fatalf("artifacts %+v, want the plain-name refusal", rep.Artifacts)
			}
			if src.dumpCount() != 0 {
				t.Fatal("a source produced bytes for a refused artifact name")
			}
		})
	}
}

// TestStageOneEnumerateError: an enumerating source whose Units fails is a
// failed artifact of its own — a stage never expands a multi-unit source.
func TestStageOneEnumerateError(t *testing.T) {
	w := newWorld(t)
	w.processors["seal"] = &fakeProcessor{seals: true, ext: ".age"}
	src := &fakeSource{data: []byte("bytes"), unitsErr: errors.New("cannot enumerate")}
	w.extraSources["snapshot"] = src
	dir := t.TempDir()
	e := w.newEngine(t, stageConfig(t, dir))
	rep, err := e.Stage(context.Background(), StageRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Artifacts) != 1 || !strings.Contains(rep.Artifacts[0].Error, "enumerating snapshot") {
		t.Fatalf("artifacts %+v, want the enumeration failure", rep.Artifacts)
	}
	if src.dumpCount() != 0 {
		t.Fatal("a source that failed to enumerate was dumped anyway")
	}
}

// TestStageOneDescribeError: a source whose Describe fails fails its artifact
// before the dir is even created.
func TestStageOneDescribeError(t *testing.T) {
	w := newWorld(t)
	w.processors["seal"] = &fakeProcessor{seals: true, ext: ".age"}
	src := &fakeSource{data: []byte("bytes"), describeErr: errors.New("describe boom")}
	w.extraSources["snapshot"] = src
	dir := t.TempDir()
	e := w.newEngine(t, stageConfig(t, dir))
	rep, err := e.Stage(context.Background(), StageRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Artifacts) != 1 || !strings.Contains(rep.Artifacts[0].Error, "describe") {
		t.Fatalf("artifacts %+v, want the describe failure", rep.Artifacts)
	}
	if src.dumpCount() != 0 {
		t.Fatal("a source whose Describe failed was dumped")
	}
}

// TestStageOneMkdirFails: a stage dir that cannot be created fails the
// artifact before anything is written.
func TestStageOneMkdirFails(t *testing.T) {
	w := newWorld(t)
	w.processors["seal"] = &fakeProcessor{seals: true, ext: ".age"}
	blocker := filepath.Join(t.TempDir(), "a-file")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(blocker, "stage") // a path under a regular file
	e := w.newEngine(t, stageConfig(t, dir))
	rep, err := e.Stage(context.Background(), StageRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Artifacts) != 1 || rep.Artifacts[0].Error == "" {
		t.Fatalf("artifacts %+v, want a failed artifact for the uncreatable dir", rep.Artifacts)
	}
}

// TestStageOneClearStagedFails: a chain record that cannot be read is a
// permanent refusal — guessing the old file's name would be the collision the
// whole staged-record contract exists to prevent.
func TestStageOneClearStagedFails(t *testing.T) {
	w := newWorld(t)
	w.processors["seal"] = &fakeProcessor{seals: true, ext: ".age"}
	src := &fakeSource{data: []byte("bytes")}
	w.sources["snapshot"] = src
	dir := t.TempDir()
	if err := os.WriteFile(source.StagedChainFile(dir, "k3s-state"), []byte("not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	e := w.newEngine(t, stageConfig(t, dir))
	rep, err := e.Stage(context.Background(), StageRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Artifacts) != 1 || !strings.Contains(rep.Artifacts[0].Error, "staged chain record") {
		t.Fatalf("artifacts %+v, want the unreadable-record refusal", rep.Artifacts)
	}
	if src.dumpCount() != 0 {
		t.Fatal("a source was dumped for an artifact whose old record could not be read")
	}
}

// TestWriteStagedRecordRefusesABadExt: a record that cannot be encoded (an
// invalid-UTF-8 extension) is a permanent failure and writes no file.
func TestWriteStagedRecordRefusesABadExt(t *testing.T) {
	dir := t.TempDir()
	path := source.StagedChainFile(dir, "n")
	err := writeStagedRecord(source.StagedRecord{Ext: "bad\xff"}, path)
	if err == nil {
		t.Fatal("writeStagedRecord accepted an unencodable record")
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassPermanent {
		t.Fatalf("class = %q, want permanent", class)
	}
	if _, serr := os.Stat(path); !errors.Is(serr, fs.ErrNotExist) {
		t.Fatalf("the failed record left a file: %v", serr)
	}
}

// TestPublishFileFailurePaths walks every failure of the atomic publish: a
// temp file that cannot be created, a caller write that fails, a Close, Chmod
// and Rename that fail. Each path removes its temp file and leaves no artifact.
func TestPublishFileFailurePaths(t *testing.T) {
	dir := t.TempDir()
	final := func(name string) string { return filepath.Join(dir, name) }

	// CreateTemp fails: the directory does not exist.
	missing := filepath.Join(dir, "missing")
	if err := publishFile(missing, filepath.Join(missing, "f"), func(*os.File) error { return nil }); err == nil {
		t.Fatal("publishFile created a temp file in a directory that does not exist")
	}

	// The caller's write fails: the error comes back and the temp file is gone.
	werr := errors.New("write failed")
	if err := publishFile(dir, final("w"), func(*os.File) error { return werr }); !errors.Is(err, werr) {
		t.Fatalf("write failure = %v, want %v", err, werr)
	}
	if names, _ := readDirNames(dir); len(names) != 0 {
		t.Fatalf("a failed write left %v", names)
	}

	// Close fails: the caller closed the handle itself.
	if err := publishFile(dir, final("c"), func(f *os.File) error { return f.Close() }); err == nil {
		t.Fatal("publishFile accepted a Close failure")
	}

	// Chmod fails: the temp file is renamed away under the writer's feet.
	if err := publishFile(dir, final("m"), func(f *os.File) error {
		return os.Rename(f.Name(), f.Name()+".moved")
	}); err == nil {
		t.Fatal("publishFile accepted a Chmod failure")
	}

	// Rename fails: the final name sits under a directory that does not exist.
	if err := publishFile(dir, filepath.Join(dir, "nodir", "r"), func(f *os.File) error {
		_, err := f.Write([]byte("x"))
		return err
	}); err == nil {
		t.Fatal("publishFile accepted a Rename failure")
	}
}

// TestClearStagedAndRemoveIfPresent drives the removal failures the stage's
// clear exists for: a non-empty directory where an artifact or the record must
// go is a real error, never a silent skip.
func TestClearStagedAndRemoveIfPresent(t *testing.T) {
	// The old record names an artifact that is a non-empty directory.
	dir1 := t.TempDir()
	old := source.StagedRecord{Ext: ".old"}
	if err := os.WriteFile(source.StagedChainFile(dir1, "n"), mustMarshal(t, old), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir1, "n.old"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir1, "n.old", "x"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := clearStaged(dir1, "n", source.StagedRecord{}); err == nil {
		t.Fatal("clearStaged ignored an old artifact it could not remove")
	}

	// No record; the new artifact's own name is a non-empty directory.
	dir2 := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir2, "n.new"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir2, "n.new", "x"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := clearStaged(dir2, "n", source.StagedRecord{Ext: ".new"}); err == nil {
		t.Fatal("clearStaged ignored a new artifact it could not remove")
	}

	// removeIfPresent itself: a non-empty directory is a transient failure, a
	// missing path is done.
	dir3 := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir3, "full"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir3, "full", "x"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := removeIfPresent(filepath.Join(dir3, "full")); err == nil {
		t.Fatal("removeIfPresent ignored a non-empty directory")
	}
	if err := removeIfPresent(filepath.Join(dir3, "gone")); err != nil {
		t.Fatalf("removeIfPresent on a missing path = %v, want nil", err)
	}
}

// --- multipart.go ------------------------------------------------------------
