package assembly_test

import (
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/assembly"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/workflow/backup"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/processor"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source"
)

func stageConfig(t *testing.T, dir string) map[string]any {
	t.Helper()
	return map[string]any{
		"layout": assembly.ConfigLayout(),
		"stage": map[string]any{
			"dir": dir, "degraded_exit_code": 5, "failed_exit_code": 9,
			"sources": map[string]any{
				"snapshot": assembly.SourceInstance("fake", "control-plane", "k3s-state", []string{"seal"}, "required", "secret"),
			},
		},
		"processors": map[string]any{"seal": assembly.Instance("fake")},
	}
}

func TestStageWritesSealedArtifact(t *testing.T) {
	w := assembly.NewWorld(t)
	w.Processors["seal"] = assembly.NewFakeProcessor(".age", true, nil)
	w.Sources["snapshot"] = assembly.NewFakeSource([]byte("sealed bytes"))
	dir := t.TempDir()
	e := w.NewAssembly(t, stageConfig(t, dir))
	rep, err := e.Stage(context.Background(), assembly.StageRequest{})
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
	if reps, err := assembly.ReadDirNames(dir); err != nil || len(reps) != 2 {
		t.Fatalf("stage dir holds %v (%v), want the artifact and its chain record", reps, err)
	}
	// The record is the half the run side reads back: the artifact's exact file
	// name tail and the processor chain in encode order, each step naming the
	// instance and type a restore resolves. configuration.Instance and type come from the
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
	if got, err := source.StagedArtifactName("k3s-state", rec); err != nil || got != "k3s-state.dat.age" {
		t.Fatalf("the record names %q (%v), want the published file k3s-state.dat.age", got, err)
	}
	if rep.Artifacts[0].Bytes != int64(len("sealed bytes")) || rep.Artifacts[0].SHA256 == "" {
		t.Fatalf("artifact result %+v", rep.Artifacts[0])
	}
}

func TestStageRefusesUnsealedSecret(t *testing.T) {
	w := assembly.NewWorld(t)
	src := assembly.NewFakeSource([]byte("plaintext"))
	w.Sources["snapshot"] = src
	dir := t.TempDir()
	// The pipeline seals nothing: the refusal is a configuration fact, before
	// the run exists and before the first byte.
	cfg := stageConfig(t, dir)
	cfg["processors"] = map[string]any{"zstd3": assembly.Instance("fake")}
	cfg["stage"].(map[string]any)["sources"].(map[string]any)["snapshot"].(map[string]any)["pipeline"] = []string{"zstd3"}
	e, err := w.NewAssemblyErr(t, cfg)
	if e != nil {
		t.Fatalf("assembly.New built an assembly for a refused stage config: %v", err)
	}
	if err == nil {
		t.Fatal("a secret stage artifact without a sealing pipeline was accepted")
	}
	if !strings.Contains(err.Error(), "/stage.sources/snapshot: a secret artifact's pipeline [zstd3] has no sealing processor") {
		t.Fatalf("the refusal does not name the stage instance and its pipeline: %v", err)
	}
	if src.DumpCount() != 0 {
		t.Fatal("the source produced bytes for a refused artifact")
	}
	if entries, err := assembly.ReadDirNames(dir); err == nil && len(entries) != 0 {
		t.Fatalf("refused stage left %v", entries)
	}
}

func TestStageNeedsItsConfig(t *testing.T) {
	w := assembly.NewWorld(t)
	cfg := assembly.BaseConfig(t, map[string]any{"primary": assembly.StorageInstance("fake", "keep_last")})
	e := w.NewAssembly(t, cfg)
	if _, err := e.Stage(context.Background(), assembly.StageRequest{}); err == nil {
		t.Fatal("Stage ran without a stage section")
	}
}

// stageArtifact is one artifact a stage case declares: the instance that
// produces it, the artifact it names, and what the case needs from the source.
type stageArtifact struct {
	inst, name, importance, ext string
	pipeline                    []string
	prepare                     func(w *assembly.World, src *assembly.FakeSource)
}

// TestStagePublishesOnlyThisStagesPairs is the one proof of the stage's
// admission contract: a stage publishes into an empty directory it owns, never
// deletes, and refuses to start on any other directory; within one stage, an
// artifact whose record or file name another artifact already claimed fails
// before it produces bytes, so one artifact's file can never answer another's
// record — not after a failed check, and not after a failed publish. Every
// refusal leaves the directory byte-identical and runs no plugin Check or Dump.
func TestStagePublishesOnlyThisStagesPairs(t *testing.T) {
	seedPair := func(t *testing.T, dir, name, ext, body string) {
		t.Helper()
		if err := os.WriteFile(source.StagedChainFile(dir, name), assembly.MustMarshal(t, source.StagedRecord{Ext: ext}), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, name+ext), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	healthy := func(inst string) stageArtifact {
		return stageArtifact{inst: inst, name: "ok", importance: "required"}
	}
	cases := []struct {
		name string
		// setup builds the start state of stage.dir and returns the stage dir.
		setup func(t *testing.T) (dir string)
		// check runs after a refusal instead of the snapshot comparison, for a
		// stage.dir that is not a directory.
		check        func(t *testing.T, dir string)
		artifacts    []stageArtifact
		prepareWorld func(w *assembly.World)
		degraded     int
		failed       int
		wantErr      string
		wantOutcome  model.Outcome
		wantExit     int
		// wantFailed maps an instance to the error its artifact must carry.
		wantFailed map[string]string
		// dumps overrides the expected Dump count for a failed instance.
		dumps map[string]int
	}{
		{
			name: "refuses a previous pair of the configured name",
			setup: func(t *testing.T) string {
				dir := t.TempDir()
				seedPair(t, dir, "k3s-state", ".dat", "last night")
				return dir
			},
			artifacts: []stageArtifact{{inst: "snap", name: "k3s-state", importance: "best_effort"}},
			degraded:  0, failed: 0,
			wantErr: "must be an empty directory",
		},
		{
			name: "refuses the C2b sequence on a persistent directory",
			setup: func(t *testing.T) string {
				dir := t.TempDir()
				seedPair(t, dir, "state", ".old", "night one state")
				seedPair(t, dir, "state.dat", ".old", "night one state.dat")
				return dir
			},
			artifacts: []stageArtifact{
				{inst: "aa", name: "state", importance: "required", ext: ".dat.old"},
				{inst: "bb", name: "state.dat", importance: "required", ext: ".new"},
			},
			degraded: 0, failed: 0,
			wantErr: "must be an empty directory",
		},
		{
			name: "refuses an orphan pair of an unconfigured name",
			setup: func(t *testing.T) string {
				dir := t.TempDir()
				seedPair(t, dir, "legacy", ".dat", "orphan")
				return dir
			},
			artifacts: []stageArtifact{{inst: "snap", name: "k3s-state", importance: "best_effort"}},
			degraded:  0, failed: 0,
			wantErr: "must be an empty directory",
		},
		{
			name: "refuses a lone record",
			setup: func(t *testing.T) string {
				dir := t.TempDir()
				if err := os.WriteFile(source.StagedChainFile(dir, "k3s-state"), assembly.MustMarshal(t, source.StagedRecord{Ext: ".dat"}), 0o644); err != nil {
					t.Fatal(err)
				}
				return dir
			},
			artifacts: []stageArtifact{{inst: "snap", name: "k3s-state", importance: "best_effort"}},
			degraded:  0, failed: 0,
			wantErr: "must be an empty directory",
		},
		{
			name: "refuses a lone artifact",
			setup: func(t *testing.T) string {
				dir := t.TempDir()
				if err := os.WriteFile(filepath.Join(dir, "k3s-state.dat"), []byte("lone"), 0o644); err != nil {
					t.Fatal(err)
				}
				return dir
			},
			artifacts: []stageArtifact{{inst: "snap", name: "k3s-state", importance: "best_effort"}},
			degraded:  0, failed: 0,
			wantErr: "must be an empty directory",
		},
		{
			name: "refuses a garbage record",
			setup: func(t *testing.T) string {
				dir := t.TempDir()
				if err := os.WriteFile(source.StagedChainFile(dir, "k3s-state"), []byte("not json"), 0o644); err != nil {
					t.Fatal(err)
				}
				return dir
			},
			artifacts: []stageArtifact{{inst: "snap", name: "k3s-state", importance: "best_effort"}},
			degraded:  0, failed: 0,
			wantErr: "must be an empty directory",
		},
		{
			name: "refuses a stale temp",
			setup: func(t *testing.T) string {
				dir := t.TempDir()
				if err := os.WriteFile(filepath.Join(dir, ".tmp-123"), []byte("x"), 0o644); err != nil {
					t.Fatal(err)
				}
				return dir
			},
			artifacts: []stageArtifact{{inst: "snap", name: "k3s-state", importance: "best_effort"}},
			degraded:  0, failed: 0,
			wantErr: "must be an empty directory",
		},
		{
			name: "refuses an unrelated file",
			setup: func(t *testing.T) string {
				dir := t.TempDir()
				if err := os.WriteFile(filepath.Join(dir, "keep.me"), []byte("x"), 0o644); err != nil {
					t.Fatal(err)
				}
				return dir
			},
			artifacts: []stageArtifact{{inst: "snap", name: "k3s-state", importance: "best_effort"}},
			degraded:  0, failed: 0,
			wantErr: "must be an empty directory",
		},
		{
			name: "refuses a non-empty subdirectory",
			setup: func(t *testing.T) string {
				dir := t.TempDir()
				if err := os.MkdirAll(filepath.Join(dir, "sub"), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, "sub", "x"), []byte("x"), 0o644); err != nil {
					t.Fatal(err)
				}
				return dir
			},
			artifacts: []stageArtifact{{inst: "snap", name: "k3s-state", importance: "best_effort"}},
			degraded:  0, failed: 0,
			wantErr: "must be an empty directory",
		},
		{
			name: "refuses a stage dir under a regular file",
			setup: func(t *testing.T) string {
				blocker := filepath.Join(t.TempDir(), "file")
				if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
					t.Fatal(err)
				}
				return filepath.Join(blocker, "stage")
			},
			check: func(t *testing.T, dir string) {
				t.Helper()
				blocker := filepath.Dir(dir)
				if body, err := os.ReadFile(blocker); err != nil || string(body) != "x" {
					t.Fatalf("the refused stage touched its blocker: %q, %v", body, err)
				}
			},
			artifacts: []stageArtifact{{inst: "snap", name: "k3s-state", importance: "best_effort"}},
			degraded:  0, failed: 0,
			wantErr: "stage:",
		},
		{
			name: "creates an absent stage dir",
			setup: func(t *testing.T) string {
				return filepath.Join(t.TempDir(), "staged")
			},
			artifacts: []stageArtifact{{inst: "snap", name: "state", importance: "required"}},
			degraded:  5, failed: 9,
			wantOutcome: model.OutcomeDelivered, wantExit: 0,
		},
		{
			name:  "keeps prefix-named neighbours",
			setup: func(t *testing.T) string { return t.TempDir() },
			artifacts: []stageArtifact{
				{inst: "aa", name: "k3s-state", importance: "required"},
				{inst: "bb", name: "k3s.v2", importance: "required"},
				{inst: "cc", name: "k3s", importance: "required"},
			},
			degraded: 5, failed: 9, wantOutcome: model.OutcomeDelivered, wantExit: 0,
		},
		{
			name:  "keeps an extension-named sibling, name first",
			setup: func(t *testing.T) string { return t.TempDir() },
			artifacts: []stageArtifact{
				{inst: "a-state", name: "state", importance: "required"},
				{inst: "b-state.dat", name: "state.dat", importance: "required"},
			},
			degraded: 5, failed: 9, wantOutcome: model.OutcomeDelivered, wantExit: 0,
		},
		{
			name:  "keeps an extension-named sibling, longer name first",
			setup: func(t *testing.T) string { return t.TempDir() },
			artifacts: []stageArtifact{
				{inst: "a-state.dat", name: "state.dat", importance: "required"},
				{inst: "b-state", name: "state", importance: "required"},
			},
			degraded: 5, failed: 9, wantOutcome: model.OutcomeDelivered, wantExit: 0,
		},
		{
			name:  "keeps a changed-ext overlap, name first",
			setup: func(t *testing.T) string { return t.TempDir() },
			artifacts: []stageArtifact{
				{inst: "a-state", name: "state", importance: "required", ext: ".dat.old"},
				{inst: "b-state.dat", name: "state.dat", importance: "required", ext: ".new"},
			},
			degraded: 5, failed: 9, wantOutcome: model.OutcomeDelivered, wantExit: 0,
		},
		{
			name:  "keeps a changed-ext overlap, longer name first",
			setup: func(t *testing.T) string { return t.TempDir() },
			artifacts: []stageArtifact{
				{inst: "a-state.dat", name: "state.dat", importance: "required", ext: ".new"},
				{inst: "b-state", name: "state", importance: "required", ext: ".dat.old"},
			},
			degraded: 5, failed: 9, wantOutcome: model.OutcomeDelivered, wantExit: 0,
		},
		{
			name:  "refuses a colliding file, name first",
			setup: func(t *testing.T) string { return t.TempDir() },
			artifacts: []stageArtifact{
				{inst: "a-state", name: "state", importance: "required", ext: ".dat.old"},
				{inst: "b-state.dat", name: "state.dat", importance: "required", ext: ".old"},
			},
			degraded: 5, failed: 9, wantOutcome: model.OutcomeFailed, wantExit: 9,
			wantFailed: map[string]string{"b-state.dat": "would replace state.dat.old"},
		},
		{
			name:  "refuses a colliding file, longer name first",
			setup: func(t *testing.T) string { return t.TempDir() },
			artifacts: []stageArtifact{
				{inst: "a-state.dat", name: "state.dat", importance: "required", ext: ".old"},
				{inst: "b-state", name: "state", importance: "required", ext: ".dat.old"},
			},
			degraded: 5, failed: 9, wantOutcome: model.OutcomeFailed, wantExit: 9,
			wantFailed: map[string]string{"b-state": "would replace state.dat.old"},
		},
		{
			name:  "refuses a duplicate name after the owner failed its check",
			setup: func(t *testing.T) string { return t.TempDir() },
			artifacts: []stageArtifact{
				{inst: "a-first", name: "state", importance: "required", prepare: func(_ *assembly.World, src *assembly.FakeSource) {
					src.CheckErr = errors.New("datastore vanished")
				}},
				{inst: "b-second", name: "state", importance: "required"},
			},
			degraded: 5, failed: 9, wantOutcome: model.OutcomeFailed, wantExit: 9,
			wantFailed: map[string]string{"a-first": "source check", "b-second": "already claimed"},
		},
		{
			name:  "refuses a duplicate name with different extensions",
			setup: func(t *testing.T) string { return t.TempDir() },
			artifacts: []stageArtifact{
				{inst: "a-first", name: "state", importance: "required", ext: ".dat"},
				{inst: "b-second", name: "state", importance: "required", ext: ".old"},
			},
			degraded: 5, failed: 9, wantOutcome: model.OutcomeFailed, wantExit: 9,
			wantFailed: map[string]string{"b-second": "already claimed"},
		},
		{
			name:  "refuses a duplicate name after the owner published",
			setup: func(t *testing.T) string { return t.TempDir() },
			artifacts: []stageArtifact{
				{inst: "a-first", name: "state", importance: "required"},
				{inst: "b-second", name: "state", importance: "required"},
			},
			degraded: 5, failed: 9, wantOutcome: model.OutcomeFailed, wantExit: 9,
			wantFailed: map[string]string{"b-second": "already claimed"},
		},
		{
			name:  "refuses a failed owner's file, name first",
			setup: func(t *testing.T) string { return t.TempDir() },
			artifacts: []stageArtifact{
				{inst: "a-state", name: "state", importance: "required", ext: ".dat.old", prepare: func(_ *assembly.World, src *assembly.FakeSource) {
					src.DumpErr = pluginerrors.Permanent(errors.New("dump vanished"))
				}},
				{inst: "b-state.dat", name: "state.dat", importance: "required", ext: ".old"},
			},
			degraded: 5, failed: 9, wantOutcome: model.OutcomeFailed, wantExit: 9,
			wantFailed: map[string]string{"a-state": "dump vanished", "b-state.dat": "would replace state.dat.old"},
			dumps:      map[string]int{"a-state": 1},
		},
		{
			name:  "refuses a failed owner's file, longer name first",
			setup: func(t *testing.T) string { return t.TempDir() },
			artifacts: []stageArtifact{
				{inst: "a-state.dat", name: "state.dat", importance: "required", ext: ".old", prepare: func(_ *assembly.World, src *assembly.FakeSource) {
					src.DumpErr = pluginerrors.Permanent(errors.New("dump vanished"))
				}},
				{inst: "b-state", name: "state", importance: "required", ext: ".dat.old"},
			},
			degraded: 5, failed: 9, wantOutcome: model.OutcomeFailed, wantExit: 9,
			wantFailed: map[string]string{"a-state.dat": "dump vanished", "b-state": "would replace state.dat.old"},
			dumps:      map[string]int{"a-state.dat": 1},
		},
		{
			name:  "a source check failure leaves the sibling intact",
			setup: func(t *testing.T) string { return t.TempDir() },
			artifacts: []stageArtifact{
				{inst: "a-bad", name: "bad", importance: "required", prepare: func(_ *assembly.World, src *assembly.FakeSource) {
					src.CheckErr = errors.New("datastore vanished")
				}},
				healthy("b-ok"),
			},
			degraded: 5, failed: 9, wantOutcome: model.OutcomeFailed, wantExit: 9,
			wantFailed: map[string]string{"a-bad": "source check"},
		},
		{
			name:  "a processor check failure leaves the sibling intact",
			setup: func(t *testing.T) string { return t.TempDir() },
			artifacts: []stageArtifact{
				{inst: "a-bad", name: "bad", importance: "required", pipeline: []string{"seal"}},
				healthy("b-ok"),
			},
			prepareWorld: func(w *assembly.World) {
				w.Processors["seal"] = assembly.NewFakeProcessor(".age", true, errors.New("no key material"))
			},
			degraded: 5, failed: 9, wantOutcome: model.OutcomeFailed, wantExit: 9,
			wantFailed: map[string]string{"a-bad": "processor seal check"},
		},
		{
			name:  "refuses a name that starts with a dot",
			setup: func(t *testing.T) string { return t.TempDir() },
			artifacts: []stageArtifact{
				{inst: "a-bad", name: ".hidden", importance: "required"},
				healthy("b-ok"),
			},
			degraded: 5, failed: 9, wantOutcome: model.OutcomeFailed, wantExit: 9,
			wantFailed: map[string]string{"a-bad": "must be a plain file name"},
		},
		{
			name:  "refuses a name with a separator",
			setup: func(t *testing.T) string { return t.TempDir() },
			artifacts: []stageArtifact{
				{inst: "a-bad", name: "bad/name", importance: "required"},
				healthy("b-ok"),
			},
			degraded: 5, failed: 9, wantOutcome: model.OutcomeFailed, wantExit: 9,
			wantFailed: map[string]string{"a-bad": "must be a plain file name"},
		},
		{
			name:  "an enumeration failure leaves the sibling intact",
			setup: func(t *testing.T) string { return t.TempDir() },
			artifacts: []stageArtifact{
				{inst: "a-bad", name: "bad", importance: "required", prepare: func(_ *assembly.World, src *assembly.FakeSource) {
					src.UnitsErr = errors.New("cannot enumerate")
				}},
				healthy("b-ok"),
			},
			degraded: 5, failed: 9, wantOutcome: model.OutcomeFailed, wantExit: 9,
			wantFailed: map[string]string{"a-bad": "enumerating a-bad"},
		},
		{
			name:  "a multi-unit refusal leaves the sibling intact",
			setup: func(t *testing.T) string { return t.TempDir() },
			artifacts: []stageArtifact{
				{inst: "a-bad", name: "bad", importance: "required", prepare: func(_ *assembly.World, src *assembly.FakeSource) {
					src.UnitList = []source.Unit{{Name: "one", Config: jsontext.Value(`{}`)}, {Name: "two", Config: jsontext.Value(`{}`)}}
				}},
				healthy("b-ok"),
			},
			degraded: 5, failed: 9, wantOutcome: model.OutcomeFailed, wantExit: 9,
			wantFailed: map[string]string{"a-bad": "stage does not expand a multi-unit source"},
		},
		{
			name:  "a describe failure leaves the sibling intact",
			setup: func(t *testing.T) string { return t.TempDir() },
			artifacts: []stageArtifact{
				{inst: "a-bad", name: "bad", importance: "required", prepare: func(_ *assembly.World, src *assembly.FakeSource) {
					src.DescribeErr = errors.New("describe boom")
				}},
				healthy("b-ok"),
			},
			degraded: 5, failed: 9, wantOutcome: model.OutcomeFailed, wantExit: 9,
			wantFailed: map[string]string{"a-bad": "describe"},
		},
		{
			name:  "a dump failure leaves the sibling intact",
			setup: func(t *testing.T) string { return t.TempDir() },
			artifacts: []stageArtifact{
				{inst: "a-bad", name: "bad", importance: "required", prepare: func(_ *assembly.World, src *assembly.FakeSource) {
					src.DumpErr = pluginerrors.Permanent(errors.New("dump vanished"))
				}},
				healthy("b-ok"),
			},
			degraded: 5, failed: 9, wantOutcome: model.OutcomeFailed, wantExit: 9,
			wantFailed: map[string]string{"a-bad": "dump vanished"},
			dumps:      map[string]int{"a-bad": 1},
		},
		{
			name:  "a best-effort guard failure degrades",
			setup: func(t *testing.T) string { return t.TempDir() },
			artifacts: []stageArtifact{
				{inst: "a-bad", name: "bad", importance: "best_effort", prepare: func(_ *assembly.World, src *assembly.FakeSource) {
					src.CheckErr = errors.New("datastore vanished")
				}},
				healthy("b-ok"),
			},
			degraded: 5, failed: 9, wantOutcome: model.OutcomeDegraded, wantExit: 5,
			wantFailed: map[string]string{"a-bad": "source check"},
		},
		{
			name:  "reports a filesystem error that is not absence",
			setup: func(t *testing.T) string { return t.TempDir() },
			artifacts: []stageArtifact{
				{inst: "a-bad", name: strings.Repeat("x", 300), importance: "required"},
				healthy("b-ok"),
			},
			degraded: 5, failed: 9, wantOutcome: model.OutcomeFailed, wantExit: 9,
			wantFailed: map[string]string{"a-bad": "file name too long"},
		},
		{
			name:  "an invalid name claims no record alias, bad first",
			setup: func(t *testing.T) string { return t.TempDir() },
			artifacts: []stageArtifact{
				{inst: "aa-bad", name: "x/../.chain-ok", importance: "best_effort"},
				{inst: "bb-good", name: "ok", importance: "required"},
			},
			degraded: 0, failed: 9, wantOutcome: model.OutcomeDegraded, wantExit: 0,
			wantFailed: map[string]string{"aa-bad": "must be a plain file name"},
		},
		{
			name:  "an invalid name claims no record alias, good first",
			setup: func(t *testing.T) string { return t.TempDir() },
			artifacts: []stageArtifact{
				{inst: "aa-good", name: "ok", importance: "required"},
				{inst: "bb-bad", name: "x/../.chain-ok", importance: "best_effort"},
			},
			degraded: 0, failed: 9, wantOutcome: model.OutcomeDegraded, wantExit: 0,
			wantFailed: map[string]string{"bb-bad": "must be a plain file name"},
		},
		{
			name:  "an invalid name claims no file alias, bad first",
			setup: func(t *testing.T) string { return t.TempDir() },
			artifacts: []stageArtifact{
				{inst: "aa-bad", name: "x/../ok.dat", importance: "best_effort"},
				{inst: "bb-good", name: "ok", importance: "required", ext: ".dat.json"},
			},
			degraded: 0, failed: 9, wantOutcome: model.OutcomeDegraded, wantExit: 0,
			wantFailed: map[string]string{"aa-bad": "must be a plain file name"},
		},
		{
			name:  "an invalid name claims no file alias, good first",
			setup: func(t *testing.T) string { return t.TempDir() },
			artifacts: []stageArtifact{
				{inst: "aa-good", name: "ok", importance: "required", ext: ".dat.json"},
				{inst: "bb-bad", name: "x/../ok.dat", importance: "best_effort"},
			},
			degraded: 0, failed: 9, wantOutcome: model.OutcomeDegraded, wantExit: 0,
			wantFailed: map[string]string{"bb-bad": "must be a plain file name"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := assembly.NewWorld(t)
			if tc.prepareWorld != nil {
				tc.prepareWorld(w)
			}
			sources := map[string]any{}
			specs := append([]stageArtifact(nil), tc.artifacts...)
			for _, a := range specs {
				src := assembly.NewFakeSource([]byte("bytes of " + a.inst))
				if a.ext != "" {
					src.Traits.Ext = a.ext
				}
				w.Sources[a.inst] = src
				sources[a.inst] = assembly.SourceInstance("fake", "control-plane", a.name, a.pipeline, a.importance, "plain")
			}
			for _, a := range specs {
				if a.prepare != nil {
					a.prepare(w, w.Sources[a.inst])
				}
			}
			// The stage workflow stages sorted instance names.
			sort.Slice(specs, func(i, j int) bool { return specs[i].inst < specs[j].inst })
			dir := tc.setup(t)
			before := snapshotTree(t, dir)
			cfg := map[string]any{
				"layout":     assembly.ConfigLayout(),
				"stage":      map[string]any{"dir": dir, "degraded_exit_code": tc.degraded, "failed_exit_code": tc.failed, "sources": sources},
				"processors": map[string]any{"seal": assembly.Instance("fake")},
			}
			rep, err := w.NewAssembly(t, cfg).Stage(context.Background(), assembly.StageRequest{})
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("stage = %v, want a refusal naming %q", err, tc.wantErr)
				}
				for _, a := range specs {
					if n := w.Sources[a.inst].CheckCount(); n != 0 {
						t.Fatalf("%s was checked %d times before the refusal", a.inst, n)
					}
					if n := w.Sources[a.inst].DumpCount(); n != 0 {
						t.Fatalf("%s was dumped %d times before the refusal", a.inst, n)
					}
				}
				if tc.check != nil {
					tc.check(t, dir)
				} else if after := snapshotTree(t, dir); !maps.Equal(before, after) {
					t.Fatalf("the refusal changed the directory: before %v, after %v", before, after)
				}
				return
			}
			if err != nil {
				t.Fatalf("stage: %v", err)
			}
			if rep.Outcome != tc.wantOutcome || rep.ExitCode != tc.wantExit {
				t.Fatalf("outcome %q exit %d, want %q/%d (artifacts %+v)", rep.Outcome, rep.ExitCode, tc.wantOutcome, tc.wantExit, rep.Artifacts)
			}
			if len(rep.Artifacts) != len(specs) {
				t.Fatalf("report holds %d artifacts, want %d", len(rep.Artifacts), len(specs))
			}
			var want []string
			for i, a := range specs {
				got := rep.Artifacts[i]
				if wantErr, failed := tc.wantFailed[a.inst]; failed {
					if got.Outcome != model.ArtifactFailed || !strings.Contains(got.Error, wantErr) {
						t.Fatalf("%s: outcome %q error %q, want failed naming %q", a.inst, got.Outcome, got.Error, wantErr)
					}
					if n := w.Sources[a.inst].DumpCount(); n != tc.dumps[a.inst] {
						t.Fatalf("%s was dumped %d times, want %d", a.inst, n, tc.dumps[a.inst])
					}
					continue
				}
				if got.Outcome != model.ArtifactOK {
					t.Fatalf("%s: outcome %q error %q, want ok", a.inst, got.Outcome, got.Error)
				}
				ext := a.ext
				if ext == "" {
					ext = ".dat"
				}
				for range a.pipeline {
					ext += ".age"
				}
				rec, err := source.ReadStagedRecord(dir, a.name)
				if err != nil {
					t.Fatalf("%s: no record: %v", a.inst, err)
				}
				if rec.Ext != ext {
					t.Fatalf("%s: record ext %q, want %q", a.inst, rec.Ext, ext)
				}
				body, err := os.ReadFile(filepath.Join(dir, a.name+ext))
				if err != nil {
					t.Fatalf("%s: no artifact: %v", a.inst, err)
				}
				if string(body) != "bytes of "+a.inst {
					t.Fatalf("%s: artifact holds %q, another artifact's bytes", a.inst, body)
				}
				want = append(want, a.name+ext, filepath.Base(source.StagedChainFile(dir, a.name)))
				if n := w.Sources[a.inst].DumpCount(); n != 1 {
					t.Fatalf("%s was dumped %d times, want 1", a.inst, n)
				}
			}
			names, err := assembly.ReadDirNames(dir)
			if err != nil {
				t.Fatal(err)
			}
			sort.Strings(names)
			sort.Strings(want)
			if strings.Join(names, ",") != strings.Join(want, ",") {
				t.Fatalf("stage dir holds %v, want exactly %v", names, want)
			}
		})
	}
}

// snapshotTree maps every entry under dir to its bytes, a directory to "dir",
// and returns nil when dir cannot be walked.
func snapshotTree(t *testing.T, dir string) map[string]string {
	t.Helper()
	out := map[string]string{}
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		if d.IsDir() {
			out[rel] = "dir"
			return nil
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		out[rel] = string(body)
		return nil
	})
	if err != nil {
		return nil
	}
	return out
}

// TestStageRefusesAnExtThatLeavesTheDir: an extension reaches the writer from a
// processor's traits. A separator or a parent step in it must be refused before
// the writer joins it, so no trait can steer a write outside stage.dir. (The
// reader refuses the same shape in a record; the staged package proves it.)
func TestStageRefusesAnExtThatLeavesTheDir(t *testing.T) {
	w := assembly.NewWorld(t)
	w.Processors["seal"] = assembly.NewFakeProcessor("/../../evil", true, nil)
	w.Sources["snapshot"] = assembly.NewFakeSource([]byte("bytes"))
	dir := t.TempDir()
	evil := filepath.Join(filepath.Dir(dir), "evil")
	e := w.NewAssembly(t, stageConfig(t, dir))
	rep, err := e.Stage(context.Background(), assembly.StageRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if _, statErr := os.Stat(evil); statErr == nil {
		t.Fatalf("the writer reached outside the stage dir: %s", evil)
	}
	if len(rep.Artifacts) != 1 || !strings.Contains(rep.Artifacts[0].Error, "ext") {
		t.Fatalf("artifacts %+v, want the refusal of the trait's ext", rep.Artifacts)
	}
}

// TestRunTakesTheWriterLease: the catalog holds only the writer lease, so the
// proof a run reached it is the claim it took and gave back — there is no run
// history to save.
func TestRunTakesTheWriterLease(t *testing.T) {
	w := assembly.NewWorld(t)
	cfg := assembly.BaseConfig(t, map[string]any{"primary": assembly.StorageInstance("fake", "keep_last")})
	cfg["catalog"] = assembly.Instance("fake")
	e := w.NewAssembly(t, cfg)
	if _, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled}); err != nil {
		t.Fatal(err)
	}
	if w.Catalog.AcquireCount() == 0 {
		t.Fatal("the run never took the writer lease")
	}
	if _, held := w.Catalog.Held(); held {
		t.Fatal("the run returned still holding the writer lease")
	}
}

func TestDescriptorsComeFromTheRegistry(t *testing.T) {
	w := assembly.NewWorld(t)
	cfg := assembly.BaseConfig(t, map[string]any{"primary": assembly.StorageInstance("fake", "keep_last")})
	e := w.NewAssembly(t, cfg)
	descs, err := e.Descriptors(context.Background())
	if err != nil || len(descs) == 0 {
		t.Fatalf("descriptors %v, %v", descs, err)
	}
}

// TestStagePublishesAReadableArtifact: the stage container writes the file and
// the run container reads it under another uid, so the published mode is a
// deployment contract — 0644, not the 0600 CreateTemp leaves behind.
func TestStagePublishesAReadableArtifact(t *testing.T) {
	w := assembly.NewWorld(t)
	dir := t.TempDir()
	cfg := map[string]any{
		"layout": assembly.ConfigLayout(),
		"stage": map[string]any{"dir": dir, "degraded_exit_code": 3, "failed_exit_code": 7,
			"sources": map[string]any{"vpn": assembly.SourceInstance("fake", "vpn", "wireguard", []string{}, "required", "plain")}},
		"processors": map[string]any{},
	}
	e := w.NewAssembly(t, cfg)
	rep, err := e.Stage(context.Background(), assembly.StageRequest{})
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
	if string(body) != string(w.Sources["vpn"].Data) {
		t.Fatalf("staged bytes %q", body)
	}
}

// TestStageRetriesATransientDumpFailure: a stage config that carries a retry
// policy gets its attempts — execution's loop, not the plugin's.
func TestStageRetriesATransientDumpFailure(t *testing.T) {
	w := assembly.NewWorld(t)
	dir := t.TempDir()
	cfg := map[string]any{
		"run":    assembly.ConfigRun(t.TempDir()),
		"layout": assembly.ConfigLayout(),
		"stage": map[string]any{"dir": dir, "degraded_exit_code": 3, "failed_exit_code": 7,
			"sources": map[string]any{"vpn": assembly.SourceInstance("fake", "vpn", "wireguard", []string{}, "required", "plain")}},
		"processors": map[string]any{},
	}
	e := w.NewAssembly(t, cfg)
	w.Sources["vpn"].DumpOnce = pluginerrors.Transient(errors.New("signal: killed"))
	rep, err := e.Stage(context.Background(), assembly.StageRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeDelivered {
		t.Fatalf("stage report %+v: one transient failure cost the snapshot", rep)
	}
}

func registerStageProcessorRequiring(t *testing.T, w *assembly.World, typ, bin string) {
	t.Helper()
	err := instances.Register(w.Registry, processor.Kind, instances.FactorySpec[map[string]jsontext.Value, processor.Processor]{
		Type: typ, Example: []byte(`{}`), Binaries: []string{bin},
		New: func(context.Context, map[string]jsontext.Value, instances.Deps) (processor.Processor, error) {
			return assembly.NewFakeProcessor("", false, nil), nil
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
	w := assembly.NewWorld(t)
	cfg := map[string]any{
		"layout":     assembly.ConfigLayout(),
		"stage":      map[string]any{"dir": t.TempDir(), "degraded_exit_code": 3, "failed_exit_code": 7},
		"processors": map[string]any{},
	}
	e := w.NewAssembly(t, cfg)
	_, err := e.Stage(context.Background(), assembly.StageRequest{})
	if err == nil || !strings.Contains(err.Error(), "no stage.sources") {
		t.Fatalf("Stage with an empty stage.sources = %v, want the missing-sources refusal", err)
	}
}

// TestStageRefusesAMissingBinary: a stage processor whose plugin type
// requires a binary PATH does not have stops the stage before any plugin work.
func TestStageRefusesAMissingBinary(t *testing.T) {
	w := assembly.NewWorld(t)
	registerStageProcessorRequiring(t, w, "zzneedsbin", "zz-definitely-not-a-real-binary")
	dir := t.TempDir()
	cfg := map[string]any{
		"layout": assembly.ConfigLayout(),
		"stage": map[string]any{"dir": dir, "degraded_exit_code": 3, "failed_exit_code": 7,
			"sources": map[string]any{"snapshot": assembly.SourceInstance("fake", "g", "n", []string{"seal"}, "required", "plain")}},
		"processors": map[string]any{"seal": assembly.Instance("zzneedsbin")},
	}
	e := w.NewAssembly(t, cfg)
	_, err := e.Stage(context.Background(), assembly.StageRequest{})
	if err == nil || !strings.Contains(err.Error(), "required binaries are not on PATH") {
		t.Fatalf("Stage with a missing required binary = %v, want the preflight refusal", err)
	}
	if entries, _ := assembly.ReadDirNames(dir); len(entries) != 0 {
		t.Fatalf("a refused stage left %v", entries)
	}
}

// --- multipart.go ------------------------------------------------------------
