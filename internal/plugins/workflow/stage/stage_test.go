package stage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"encoding/json/jsontext"
	"fmt"

	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
	pluginerrors "github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/execution"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/pipeline"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source/staged"
)

// plainSource is a stage source the workflow never dumps through: the pipeline
// owns the bytes, and the stage only asks the source for its units.
type plainSource struct{}

func (plainSource) Describe(context.Context) (source.SourceTraits, error) {
	return source.SourceTraits{}, nil
}

func (plainSource) Dump(context.Context, io.Writer) error { return nil }

// unitSource adds the enumeration capability with a scripted answer.
type unitSource struct {
	plainSource
	units []source.Unit
	err   error
}

func (u *unitSource) Units(context.Context) ([]source.Unit, error) { return u.units, u.err }

// production is one source's scripted pipeline answer.
type production struct {
	ext     string
	chain   []model.ProcessorStep
	body    string
	dumpErr error
	// transient is returned by the first Dump and cleared, for the retry proofs.
	transient error
}

// fakePipeline is the producer seam: it answers each source by instance name and
// counts what the stage asked of it.
type fakePipeline struct {
	productions map[string]*production
	resolveErr  map[string]error
	resolves    int
	dumps       map[string]int
}

func (p *fakePipeline) Resolve(_ context.Context, plan pipeline.Plan) (pipeline.Resolved, error) {
	p.resolves++
	if err := p.resolveErr[plan.SourceInstance]; err != nil {
		return pipeline.Resolved{}, err
	}
	prod := p.productions[plan.SourceInstance]
	artifact := plan.Artifact
	artifact.Ext = prod.ext
	return pipeline.Resolved{Artifact: artifact, Chain: prod.chain}, nil
}

func (p *fakePipeline) Dump(_ context.Context, dst io.Writer, plan pipeline.Plan, _ pipeline.Resolved) (int64, error) {
	if p.dumps == nil {
		p.dumps = map[string]int{}
	}
	p.dumps[plan.SourceInstance]++
	prod := p.productions[plan.SourceInstance]
	if prod.transient != nil {
		err := prod.transient
		prod.transient = nil
		return 0, err
	}
	if prod.dumpErr != nil {
		return 0, prod.dumpErr
	}
	n, err := io.WriteString(dst, prod.body)
	return int64(n), err
}

func newPipeline(productions map[string]*production) *fakePipeline {
	return &fakePipeline{productions: productions, resolveErr: map[string]error{}}
}

// one is a source with its scripted production: the one-artifact case.
func one(name string, prod *production) (Source, *production) {
	return Source{
		Name: name, CheckKind: "stage_source", Type: "fake", Instance: plainSource{},
		Artifact: model.Artifact{Name: name, Group: "control-plane", Importance: model.ImportanceRequired,
			Sensitivity: model.SensitivityPlain},
	}, prod
}

func options(dir string, pipeline Producer, sources ...Source) Options {
	return Options{Dir: dir, ConfigPath: "/etc/config.yaml", DegradedExitCode: 5, FailedExitCode: 9,
		Sources: sources, Retry: execution.Options{Attempts: 1}, Pipeline: pipeline}
}

func TestRunRefusesAnIncompleteConfiguration(t *testing.T) {
	cases := []struct {
		name string
		opts Options
		want string
	}{
		{"a stage dir is required", Options{Sources: []Source{{Name: "a"}}}, "needs the stage section with a dir"},
		{"a stage source is required", Options{Dir: t.TempDir()}, "no stage.sources"},
		{"a pipeline is required", Options{Dir: t.TempDir(), Sources: []Source{{Name: "a"}}}, "no pipeline"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Run(context.Background(), tc.opts)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Run = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestRunStagesEverySource(t *testing.T) {
	dir := t.TempDir()
	sealed := &production{ext: ".dat.age", body: "sealed bytes",
		chain: []model.ProcessorStep{{Instance: "seal", Type: "age", Ext: ".age", Seals: true}}}
	plain := &production{ext: ".dat", body: "plain bytes"}
	a, _ := one("control_plane", sealed)
	b, _ := one("files", plain)
	b.Name, b.Artifact.Name = "files", "home"
	b.Steps = []pipeline.Step{{Instance: "seal", Type: "age"}}
	p := newPipeline(map[string]*production{"control_plane": sealed, "files": plain})

	rep, err := Run(context.Background(), options(dir, p, a, b))
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeDelivered || rep.ExitCode != 0 {
		t.Fatalf("report %+v", rep)
	}
	if len(rep.Artifacts) != 2 {
		t.Fatalf("report holds %d artifacts", len(rep.Artifacts))
	}
	want := map[string]struct {
		ext  string
		body string
	}{
		"control_plane": {".dat.age", "sealed bytes"},
		"home":          {".dat", "plain bytes"},
	}
	for _, ar := range rep.Artifacts {
		w := want[ar.Artifact.Name]
		if ar.Outcome != model.ArtifactOK || ar.Artifact.Ext != w.ext {
			t.Fatalf("artifact %+v, want ok with ext %q", ar, w.ext)
		}
	}
	// The pair the run side reads: <name><ext> and its record, nothing else.
	assertPair(t, dir, "control_plane", ".dat.age", "sealed bytes", sealed.chain)
	assertPair(t, dir, "home", ".dat", "plain bytes", nil)
	names, err := readNames(dir)
	if err != nil {
		t.Fatal(err)
	}
	wantNames := []string{".chain-control_plane.json", ".chain-home.json", "control_plane.dat.age", "home.dat"}
	sort.Strings(wantNames)
	if strings.Join(names, ",") != strings.Join(wantNames, ",") {
		t.Fatalf("stage dir holds %v, want %v", names, wantNames)
	}
}

// assertPair proves the published artifact and record: the exact name, the exact
// bytes, the recorded chain and the 0644 both cross-container readers rely on.
func assertPair(t *testing.T, dir, name, ext, body string, chain []model.ProcessorStep) {
	t.Helper()
	path := filepath.Join(dir, name+ext)
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != body {
		t.Fatalf("%s holds %q, want %q", path, got, body)
	}
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if mode := st.Mode().Perm(); mode != 0o644 {
		t.Fatalf("%s mode %v, want 0644", path, mode)
	}
	rec, err := source.ReadStagedRecord(dir, name)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Ext != ext || len(rec.Chain) != len(chain) {
		t.Fatalf("record %+v, want ext %q and chain %+v", rec, ext, chain)
	}
	// The record is 0644 too: the container that reads it runs as another uid.
	st, err = os.Stat(source.StagedChainFile(dir, name))
	if err != nil {
		t.Fatal(err)
	}
	if mode := st.Mode().Perm(); mode != 0o644 {
		t.Fatalf("record mode %v, want 0644", mode)
	}
}

func readNames(dir string) ([]string, error) {
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

func TestRunRefusesANonEmptyStageDir(t *testing.T) {
	seed := func(name, body string) func(t *testing.T) string {
		return func(t *testing.T) string {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
				t.Fatal(err)
			}
			return dir
		}
	}
	cases := []struct {
		name  string
		dir   func(t *testing.T) string
		want  string
		check func(t *testing.T, dir string)
	}{
		{"a previous pair", seed(".chain-k3s-state.json", `{"ext":".dat"}`), "must be an empty directory", nil},
		{"a lone record", seed(".chain-legacy.json", `{"ext":".old"}`), "must be an empty directory", nil},
		{"a lone artifact", seed("k3s-state.dat", "lone"), "must be an empty directory", nil},
		{"a garbage record", seed(".chain-k3s-state.json", "not json"), "must be an empty directory", nil},
		{"a stale temp", seed(".tmp-123", "x"), "must be an empty directory", nil},
		{"an unrelated file", seed("keep.me", "x"), "must be an empty directory", nil},
		{"a subdirectory", func(t *testing.T) string {
			dir := t.TempDir()
			if err := os.MkdirAll(filepath.Join(dir, "sub"), 0o755); err != nil {
				t.Fatal(err)
			}
			return dir
		}, "must be an empty directory", nil},
		{"a stage dir under a regular file", func(t *testing.T) string {
			blocker := filepath.Join(t.TempDir(), "file")
			if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
				t.Fatal(err)
			}
			return filepath.Join(blocker, "stage")
		}, "stage:", func(t *testing.T, dir string) {
			body, err := os.ReadFile(filepath.Dir(dir))
			if err != nil || string(body) != "x" {
				t.Fatalf("the refused stage touched its blocker: %q, %v", body, err)
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := tc.dir(t)
			prod := &production{ext: ".dat"}
			src, _ := one("snap", prod)
			p := newPipeline(map[string]*production{"snap": prod})
			opts := options(dir, p, src)
			opts.Checks = func() map[Check]error {
				t.Fatal("a refused stage asked for its check set before accepting the directory")
				return nil
			}
			_, err := Run(context.Background(), opts)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Run = %v, want %q", err, tc.want)
			}
			if p.resolves != 0 {
				t.Fatalf("the refusal resolved %d artifacts before any source ran", p.resolves)
			}
			if tc.check != nil {
				tc.check(t, dir)
			}
		})
	}
}

// TestRunReportsAnUnlistableStageDir proves the one start state a process
// cannot provoke: a directory it created and owns that ReadDir still fails.
func TestRunReportsAnUnlistableStageDir(t *testing.T) {
	readDir = func(string) ([]fs.DirEntry, error) { return nil, errors.New("listing denied") }
	t.Cleanup(func() { readDir = os.ReadDir })
	_, err := Run(context.Background(), Options{Dir: t.TempDir(), Sources: []Source{{Name: "a"}}, Pipeline: newPipeline(nil)})
	if err == nil || !strings.Contains(err.Error(), "listing denied") {
		t.Fatalf("Run = %v, want the listing failure", err)
	}
}

func TestRunRefusesANameOutsideTheStageDir(t *testing.T) {
	cases := []struct {
		name     string
		artifact string
		ext      string
		want     string
	}{
		{"a separator", "bad/name", ".dat", "must be a plain file name"},
		{"a leading dot", ".hidden", ".dat", "must be a plain file name"},
		{"an ext with a separator", "bad", "/escape", "must not contain a path separator or a parent step"},
		{"an ext with a parent step", "bad", "..", "must not contain a path separator or a parent step"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			src, prod := one("bad", &production{ext: tc.ext})
			src.Artifact.Name = tc.artifact
			p := newPipeline(map[string]*production{"bad": prod})
			rep, err := Run(context.Background(), options(dir, p, src))
			if err != nil {
				t.Fatal(err)
			}
			if rep.Outcome != model.OutcomeFailed || rep.ExitCode != 9 {
				t.Fatalf("report %+v, want failed/9", rep)
			}
			if rep.Artifacts[0].Error == "" || !strings.Contains(rep.Artifacts[0].Error, tc.want) {
				t.Fatalf("artifact error %q, want %q", rep.Artifacts[0].Error, tc.want)
			}
			if names, err := readNames(dir); err != nil || len(names) != 0 {
				t.Fatalf("a refused artifact left %v (%v)", names, err)
			}
		})
	}
}

func TestRunRefusesCollisions(t *testing.T) {
	required := func(name, inst string) Source {
		return Source{Name: inst, CheckKind: "stage_source", Type: "fake", Instance: plainSource{},
			Artifact: model.Artifact{Name: name, Group: "g", Importance: model.ImportanceRequired,
				Sensitivity: model.SensitivityPlain}}
	}
	cases := []struct {
		name    string
		sources []Source
		exts    map[string]string
		want    map[string]string
	}{
		{
			name:    "one name twice",
			sources: []Source{required("state", "a-first"), required("state", "b-second")},
			exts:    map[string]string{"a-first": ".dat", "b-second": ".dat"},
			want:    map[string]string{"b-second": "already claimed"},
		},
		{
			name:    "a duplicate name with different extensions",
			sources: []Source{required("state", "a-first"), required("state", "b-second")},
			exts:    map[string]string{"a-first": ".dat", "b-second": ".old"},
			want:    map[string]string{"b-second": "already claimed"},
		},
		{
			name:    "a colliding file",
			sources: []Source{required("state", "a-state"), required("state.dat", "b-state.dat")},
			exts:    map[string]string{"a-state": ".dat.old", "b-state.dat": ".old"},
			want:    map[string]string{"b-state.dat": "would replace state.dat.old"},
		},
		{
			name:    "a failed owner's file stays claimed",
			sources: []Source{required("state", "a-state"), required("state.dat", "b-state.dat")},
			exts:    map[string]string{"a-state": ".dat.old", "b-state.dat": ".old"},
			want:    map[string]string{"a-state": "dump vanished", "b-state.dat": "would replace state.dat.old"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			p := newPipeline(map[string]*production{})
			for _, src := range tc.sources {
				p.productions[src.Name] = &production{ext: tc.exts[src.Name], body: "bytes of " + src.Name}
			}
			if tc.want["a-state"] == "dump vanished" {
				p.productions["a-state"].dumpErr = pluginerrors.Permanent(errors.New("dump vanished"))
			}
			rep, err := Run(context.Background(), options(dir, p, tc.sources...))
			if err != nil {
				t.Fatal(err)
			}
			if rep.Outcome != model.OutcomeFailed || rep.ExitCode != 9 {
				t.Fatalf("report %+v, want failed/9", rep)
			}
			for i, ar := range rep.Artifacts {
				want, ok := tc.want[tc.sources[i].Name]
				if !ok {
					if ar.Outcome != model.ArtifactOK {
						t.Fatalf("%s: outcome %q error %q, want ok", ar.Artifact.Name, ar.Outcome, ar.Error)
					}
					continue
				}
				if ar.Outcome != model.ArtifactFailed || !strings.Contains(ar.Error, want) {
					t.Fatalf("%s: outcome %q error %q, want failed naming %q", ar.Artifact.Name, ar.Outcome, ar.Error, want)
				}
			}
		})
	}
}

func TestRunRefusesAFailedCheck(t *testing.T) {
	// A failed source check fails only that source's artifact; a best-effort one
	// degrades the stage, a required one fails it.
	cases := []struct {
		name        string
		importance  model.Importance
		checks      map[Check]error
		wantOutcome model.Outcome
		wantExit    int
		wantErr     string
	}{
		{"a required source check", model.ImportanceRequired, map[Check]error{{Kind: "stage_source", Name: "a-bad"}: errors.New("datastore vanished")},
			model.OutcomeFailed, 9, "source check"},
		{"a best-effort source check", model.ImportanceBestEffort, map[Check]error{{Kind: "stage_source", Name: "a-bad"}: errors.New("datastore vanished")},
			model.OutcomeDegraded, 5, "source check"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			bad, _ := one("a-bad", &production{ext: ".dat", body: "bytes"})
			bad.Artifact.Importance = tc.importance
			ok, _ := one("b-ok", &production{ext: ".dat", body: "bytes"})
			ok.Artifact.Name = "ok"
			p := newPipeline(map[string]*production{"a-bad": {ext: ".dat", body: "bytes"}, "b-ok": {ext: ".dat", body: "bytes"}})
			opts := options(dir, p, bad, ok)
			opts.Checks = func() map[Check]error { return tc.checks }
			rep, err := Run(context.Background(), opts)
			if err != nil {
				t.Fatal(err)
			}
			if rep.Outcome != tc.wantOutcome || rep.ExitCode != tc.wantExit {
				t.Fatalf("report %+v, want %q/%d", rep, tc.wantOutcome, tc.wantExit)
			}
			if got := rep.Artifacts[0]; got.Artifact.Name != "a-bad" || !strings.Contains(got.Error, tc.wantErr) {
				t.Fatalf("artifact %+v, want the check refusal", got)
			}
			if got := rep.Artifacts[1]; got.Outcome != model.ArtifactOK {
				t.Fatalf("the sibling artifact %+v, want ok", got)
			}
			if p.dumps["a-bad"] != 0 {
				t.Fatalf("a checked-out source was dumped %d times", p.dumps["a-bad"])
			}
		})
	}

	t.Run("a processor check", func(t *testing.T) {
		dir := t.TempDir()
		src, _ := one("a-bad", &production{ext: ".dat.age", body: "bytes"})
		src.Steps = []pipeline.Step{{Instance: "seal", Type: "age"}}
		p := newPipeline(map[string]*production{"a-bad": {ext: ".dat.age", body: "bytes"}})
		opts := options(dir, p, src)
		opts.Checks = func() map[Check]error {
			return map[Check]error{{Kind: "processor", Name: "seal"}: errors.New("no key material")}
		}
		rep, err := Run(context.Background(), opts)
		if err != nil {
			t.Fatal(err)
		}
		if got := rep.Artifacts[0]; !strings.Contains(got.Error, "processor seal check") {
			t.Fatalf("artifact %+v, want the processor check refusal", got)
		}
		if p.dumps["a-bad"] != 0 {
			t.Fatalf("a refused artifact was dumped %d times", p.dumps["a-bad"])
		}
	})
}

func TestRunRefusesAMultiUnitSource(t *testing.T) {
	newSource := func(units *unitSource) Source {
		src, _ := one("multi", &production{ext: ".dat", body: "bytes"})
		src.Instance = units
		return src
	}
	cases := []struct {
		name    string
		units   *unitSource
		wantErr string
	}{
		{"an enumeration failure", &unitSource{err: errors.New("cannot enumerate")}, "enumerating multi"},
		{"two units", &unitSource{units: []source.Unit{{Name: "one"}, {Name: "two"}}}, "holds 2 artifacts"},
		{"one unit", &unitSource{units: []source.Unit{{Name: "one"}}}, "holds 1 artifacts"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			p := newPipeline(map[string]*production{"multi": {ext: ".dat", body: "bytes"}})
			rep, err := Run(context.Background(), options(dir, p, newSource(tc.units)))
			if err != nil {
				t.Fatal(err)
			}
			if rep.Outcome != model.OutcomeFailed || rep.ExitCode != 9 {
				t.Fatalf("report %+v, want failed/9", rep)
			}
			if !strings.Contains(rep.Artifacts[0].Error, tc.wantErr) {
				t.Fatalf("artifact error %q, want %q", rep.Artifacts[0].Error, tc.wantErr)
			}
			if p.dumps["multi"] != 0 {
				t.Fatalf("a multi-unit source was dumped %d times", p.dumps["multi"])
			}
		})
	}

	t.Run("a source that answers unsupported", func(t *testing.T) {
		dir := t.TempDir()
		src := newSource(&unitSource{err: errors.ErrUnsupported})
		p := newPipeline(map[string]*production{"multi": {ext: ".dat", body: "bytes"}})
		rep, err := Run(context.Background(), options(dir, p, src))
		if err != nil {
			t.Fatal(err)
		}
		if rep.Outcome != model.OutcomeDelivered {
			t.Fatalf("report %+v, want delivered", rep)
		}
	})
}

func TestRunRetriesATransientDumpFailure(t *testing.T) {
	dir := t.TempDir()
	src, prod := one("snap", &production{ext: ".dat", body: "bytes"})
	prod.transient = pluginerrors.Transient(errors.New("signal: killed"))
	p := newPipeline(map[string]*production{"snap": prod})
	opts := options(dir, p, src)
	opts.Retry = execution.Options{Attempts: 2}
	rep, err := Run(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeDelivered {
		t.Fatalf("report %+v: one transient failure cost the snapshot", rep)
	}
	if p.dumps["snap"] != 2 {
		t.Fatalf("the source was dumped %d times, want 2", p.dumps["snap"])
	}

	t.Run("a permanent failure is not retried", func(t *testing.T) {
		dir := t.TempDir()
		src, prod := one("snap", &production{ext: ".dat", body: "bytes", dumpErr: pluginerrors.Permanent(errors.New("dump vanished"))})
		p := newPipeline(map[string]*production{"snap": prod})
		opts := options(dir, p, src)
		opts.Retry = execution.Options{Attempts: 3}
		rep, err := Run(context.Background(), opts)
		if err != nil {
			t.Fatal(err)
		}
		if rep.Outcome != model.OutcomeFailed || !strings.Contains(rep.Artifacts[0].Error, "dump vanished") {
			t.Fatalf("report %+v, want the permanent dump refusal", rep)
		}
		if p.dumps["snap"] != 1 {
			t.Fatalf("the source was dumped %d times, want 1", p.dumps["snap"])
		}
	})
}

func TestRunReportsAPrepareFailure(t *testing.T) {
	dir := t.TempDir()
	src, _ := one("snap", &production{ext: ".dat", body: "bytes"})
	p := newPipeline(map[string]*production{"snap": {ext: ".dat"}})
	p.resolveErr["snap"] = errors.New("describe boom")
	rep, err := Run(context.Background(), options(dir, p, src))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(rep.Artifacts[0].Error, "describe boom") {
		t.Fatalf("artifact %+v, want the resolve refusal", rep.Artifacts[0])
	}
	if names, _ := readNames(dir); len(names) != 0 {
		t.Fatalf("a refused resolve left %v", names)
	}
}

func TestRunReportsAFilesystemErrorThatIsNotAbsence(t *testing.T) {
	dir := t.TempDir()
	src, _ := one("a-bad", &production{ext: ".dat", body: "bytes"})
	src.Artifact.Name = strings.Repeat("x", 300)
	ok, _ := one("b-ok", &production{ext: ".dat", body: "bytes"})
	ok.Artifact.Name = "ok"
	p := newPipeline(map[string]*production{"a-bad": {ext: ".dat", body: "bytes"}, "b-ok": {ext: ".dat", body: "bytes"}})
	rep, err := Run(context.Background(), options(dir, p, src, ok))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(rep.Artifacts[0].Error, "file name too long") {
		t.Fatalf("artifact %+v, want the filesystem error", rep.Artifacts[0])
	}
	if rep.Artifacts[1].Outcome != model.ArtifactOK {
		t.Fatalf("the sibling artifact %+v, want ok", rep.Artifacts[1])
	}
}

func TestRunEmitsTheStageLines(t *testing.T) {
	dir := t.TempDir()
	var buf bytes.Buffer
	ok, _ := one("ok", &production{ext: ".dat", body: "bytes"})
	ok.Artifact.Name = "ok"
	bad, _ := one("bad", &production{ext: ".dat"})
	bad.Artifact.Name = "bad"
	p := newPipeline(map[string]*production{"ok": {ext: ".dat", body: "bytes"}, "bad": {ext: ".dat"}})
	opts := options(dir, p, ok, bad)
	opts.Checks = func() map[Check]error {
		return map[Check]error{{Kind: "stage_source", Name: "bad"}: errors.New("gone")}
	}
	opts.Log = slog.New(slog.NewTextHandler(&buf, nil))
	if _, err := Run(context.Background(), opts); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"stage.start", "artifact.start", "artifact.done", "artifact.failed"} {
		if !strings.Contains(buf.String(), want) {
			t.Fatalf("log holds %q, want %q", buf.String(), want)
		}
	}
}

func TestWriteStagedRecordRefusesABadExt(t *testing.T) {
	dir := t.TempDir()
	path := source.StagedChainFile(dir, "n")
	err := writeStagedRecord(source.StagedRecord{Ext: "bad\xff"}, path)
	if err == nil {
		t.Fatal("writeStagedRecord accepted an unencodable record")
	}
	if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassPermanent {
		t.Fatalf("class = %q, want permanent", class)
	}
	if _, serr := os.Stat(path); !errors.Is(serr, fs.ErrNotExist) {
		t.Fatalf("the failed record left a file: %v", serr)
	}
}

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
	if names, _ := readNames(dir); len(names) != 0 {
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

// TestStagedSourceReadsBackThePair is the round trip the whole stage exists for:
// a run-side staged source opens exactly the pair this workflow published and
// reports the recorded chain.
func TestStagedSourceReadsBackThePair(t *testing.T) {
	dir := t.TempDir()
	chain := []model.ProcessorStep{{Instance: "seal", Type: "age", Ext: ".age", Seals: true}}
	src, prod := one("snap", &production{ext: ".dat.age", body: "sealed bytes", chain: chain})
	src.Artifact.Sensitivity = model.SensitivitySecret
	p := newPipeline(map[string]*production{"snap": prod})
	if _, err := Run(context.Background(), options(dir, p, src)); err != nil {
		t.Fatal(err)
	}

	lr := loader.New()
	factories := instances.NewRegistry()
	if err := loader.Add(lr, staged.Spec, func(l *loader.Registry) error { return staged.Apply(l, factories) }); err != nil {
		t.Fatal(err)
	}
	raw := jsontext.Value(fmt.Sprintf(`{"dir": %q, "name": "snap"}`, dir))
	reader, err := instances.Build(context.Background(), factories, source.Kind, "staged", raw, instances.Deps{Instance: "reader"})
	if err != nil {
		t.Fatal(err)
	}
	tr, err := reader.Describe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if tr.Ext != ".dat.age" {
		t.Fatalf("described ext %q, want the recorded .dat.age", tr.Ext)
	}
	var body bytes.Buffer
	if err := reader.Dump(context.Background(), &body); err != nil {
		t.Fatal(err)
	}
	if body.String() != "sealed bytes" {
		t.Fatalf("read back %q, want the staged bytes", body.String())
	}
	got, err := reader.(source.RecordedChain).RecordedChain(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != chain[0] {
		t.Fatalf("recorded chain %+v, want %+v", got, chain)
	}
}

// quote renders a path as a JSON string for a factory config.
