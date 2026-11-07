package inventory

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	json "encoding/json/v2"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/configuration"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/execution"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/human"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/layout"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage/storagetest"
)

// testLayout is the reference layout: the defaults document's own values.
func testLayout(t *testing.T) layout.Layout {
	t.Helper()
	return layout.New(layout.Config{
		IDLayout:       "20060102T150405Z",
		RunPrefix:      mustTemplate(t, "runs/{{ .Run.ID }}"),
		ArtifactKey:    mustTemplate(t, "{{ .Artifact.Group }}/{{ .Artifact.Name }}{{ .Artifact.Ext }}"),
		ManifestKey:    mustTemplate(t, "manifest.json"),
		ManualIDSuffix: "-manual",
	})
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

// testPolicy is the execution policy of every test: three attempts, no waits.
func testPolicy() execution.Options {
	return execution.Options{Attempts: 3, Unclassified: pluginerrors.ClassTransient}
}

func testOptions(t *testing.T) Inventory {
	t.Helper()
	return New(Options{Layout: testLayout(t), Policy: testPolicy()})
}

func testStorage(name string, reader storage.Reader, parallelism int) Storage {
	return Storage{Name: name, Reader: reader, CallParallelism: parallelism, Log: slog.New(slog.DiscardHandler)}
}

func testStorageWithLog(name string, reader storage.Reader, log *slog.Logger) Storage {
	return Storage{Name: name, Reader: reader, CallParallelism: 1, Log: log}
}

func mustMarshal(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func putRun(t *testing.T, f *storagetest.Fake, storageName, runID string, at time.Time, verified bool) {
	t.Helper()
	f.PutObject("runs/"+runID+"/artifact.bin", []byte(runID), at)
	if !verified {
		return
	}
	rep := model.RunReport{
		Schema: model.ManifestSchema, RunID: runID, Kind: model.RunKindScheduled, StartedAt: at, Outcome: model.OutcomeDelivered,
		Storages: []model.StorageResult{{Storage: storageName, Outcome: model.StorageVerified}},
	}
	f.PutObject("runs/"+runID+"/manifest.json", mustMarshal(t, rep), at)
}

// TestSurveyStoragePaginates: a listing that hands back a token is followed
// until it stops, every page's objects landing in the survey.
func TestSurveyStoragePaginates(t *testing.T) {
	f := storagetest.NewFake()
	f.PageSize = 1
	now := time.Now().UTC()
	f.PutObject("runs/r1/a.bin", []byte("x"), now)
	f.PutObject("runs/r1/b.bin", []byte("y"), now)
	sur := testOptions(t).SurveyStorage(context.Background(), "runs/", testStorage("primary", f, 1))
	if !sur.OK {
		t.Fatalf("survey failed: %v", sur.Err)
	}
	if got := len(sur.Runs["r1"].Objects); got != 2 {
		t.Fatalf("surveyed %d objects, want 2 across the pages", got)
	}
}

// TestSurveyStorageStopsARunawayListing: a listing that never terminates is
// refused rather than looped forever.
func TestSurveyStorageStopsARunawayListing(t *testing.T) {
	f := storagetest.NewFake()
	f.PageSize = 1
	now := time.Now().UTC()
	for i := 0; i < 502; i++ {
		f.PutObject(fmt.Sprintf("runs/r1/o%04d", i), []byte("x"), now)
	}
	sur := testOptions(t).SurveyStorage(context.Background(), "runs/", testStorage("primary", f, 1))
	if sur.OK || sur.Err == nil || !strings.Contains(sur.Err.Error(), "did not terminate") {
		t.Fatalf("survey = ok=%v err=%v, want the runaway-listing refusal", sur.OK, sur.Err)
	}
}

// TestSurveyStorageIgnoresObjectsOutsideARun: an object the prefix matches
// but that names no run directory is not the survey's to reason about.
func TestSurveyStorageIgnoresObjectsOutsideARun(t *testing.T) {
	f := storagetest.NewFake()
	f.PutObject("runs/toplevel.bin", []byte("x"), time.Now().UTC())
	sur := testOptions(t).SurveyStorage(context.Background(), "runs/", testStorage("primary", f, 1))
	if !sur.OK {
		t.Fatalf("survey failed: %v", sur.Err)
	}
	if len(sur.Runs) != 0 {
		t.Fatalf("survey kept %d runs from an object outside any run dir", len(sur.Runs))
	}
}

// TestSurveyStorageManifestKeyFailure: a run id the manifest key cannot be
// rendered from leaves the run's manifest unknown, never "absent" — so it is
// refused deletion for the sweep.
func TestSurveyStorageManifestKeyFailure(t *testing.T) {
	f := storagetest.NewFake()
	f.PutObject("runs/../x.bin", []byte("x"), time.Now().UTC())
	sur := testOptions(t).SurveyStorage(context.Background(), "runs/", testStorage("primary", f, 1))
	if !sur.OK {
		t.Fatalf("survey failed: %v", sur.Err)
	}
	r := sur.Runs[".."]
	if r.ManifestProblem == "" || !strings.Contains(r.ManifestProblem, "does not render") {
		t.Fatalf("run %+v, want a manifest-key problem", r)
	}
}

// TestSurveyStorageKindDisagreement: a manifest whose kind disagrees with the
// run id is a problem run, never permission to delete.
func TestSurveyStorageKindDisagreement(t *testing.T) {
	f := storagetest.NewFake()
	now := time.Now().UTC()
	id := "20261001T040000Z"
	rep := model.RunReport{
		Schema: model.ManifestSchema, RunID: id, Kind: "weekly", StartedAt: now, Outcome: model.OutcomeDelivered,
		Storages: []model.StorageResult{{Storage: "primary", Outcome: model.StorageVerified}},
	}
	f.PutObject("runs/"+id+"/a.bin", []byte("x"), now)
	f.PutObject("runs/"+id+"/manifest.json", mustMarshal(t, rep), now)
	sur := testOptions(t).SurveyStorage(context.Background(), "runs/", testStorage("primary", f, 1))
	r := sur.Runs[id]
	if !strings.Contains(r.ManifestProblem, "the run id says") {
		t.Fatalf("run %+v, want the kind disagreement recorded", r)
	}
}

// TestSurveyRecordsWhereTheManifestSits: every run whose manifest the listing
// holds carries its ManifestKey — deletion orders the manifest first by this
// exact key, so an empty one would remove artifacts before the record that
// promises them. A run without a manifest carries none.
func TestSurveyRecordsWhereTheManifestSits(t *testing.T) {
	f := storagetest.NewFake()
	now := time.Now().UTC()
	putRun(t, f, "primary", "20261001T040000Z", now, true)
	putRun(t, f, "primary", "20261002T040000Z", now, true)
	f.PutObject("runs/20261003T040000Z/a.bin", []byte("x"), now)

	sur := testOptions(t).SurveyStorage(context.Background(), "runs/", testStorage("primary", f, 1))
	if !sur.OK {
		t.Fatalf("survey failed: %v", sur.Err)
	}
	for id, want := range map[string]string{
		"20261001T040000Z": "runs/20261001T040000Z/manifest.json",
		"20261002T040000Z": "runs/20261002T040000Z/manifest.json",
		"20261003T040000Z": "",
	} {
		r := sur.Runs[id]
		if r.ManifestKey != want {
			t.Fatalf("run %s manifest key = %q, want %q", id, r.ManifestKey, want)
		}
		if want != "" && !slices.ContainsFunc(r.Objects, func(o model.ObjectInfo) bool { return o.Key == want }) {
			t.Fatalf("run %s manifest key %q is not among its objects", id, want)
		}
	}
}

// TestSurveyStorageVerifiesOnlyItsOwnDeliveredRun: a run counts as verified
// only when its manifest says delivered and calls THIS storage verified.
func TestSurveyStorageVerifiesOnlyItsOwnDeliveredRun(t *testing.T) {
	f := storagetest.NewFake()
	now := time.Now().UTC()
	putRun(t, f, "primary", "20261001T040000Z", now, true)
	failed := model.RunReport{
		Schema: model.ManifestSchema, RunID: "20261002T040000Z", Kind: model.RunKindScheduled, StartedAt: now, Outcome: model.OutcomeFailed,
		Storages: []model.StorageResult{{Storage: "primary", Outcome: model.StorageVerified}},
	}
	f.PutObject("runs/20261002T040000Z/a.bin", []byte("x"), now)
	f.PutObject("runs/20261002T040000Z/manifest.json", mustMarshal(t, failed), now)
	elsewhere := model.RunReport{
		Schema: model.ManifestSchema, RunID: "20261003T040000Z", Kind: model.RunKindScheduled, StartedAt: now, Outcome: model.OutcomeDelivered,
		Storages: []model.StorageResult{{Storage: "offsite", Outcome: model.StorageVerified}},
	}
	f.PutObject("runs/20261003T040000Z/a.bin", []byte("x"), now)
	f.PutObject("runs/20261003T040000Z/manifest.json", mustMarshal(t, elsewhere), now)

	sur := testOptions(t).SurveyStorage(context.Background(), "runs/", testStorage("primary", f, 1))
	for id, want := range map[string]bool{"20261001T040000Z": true, "20261002T040000Z": false, "20261003T040000Z": false} {
		r := sur.Runs[id]
		if r.Stored.Verified != want || !r.Stored.Manifest {
			t.Fatalf("run %s = verified %v manifest %v, want verified %v", id, r.Stored.Verified, r.Stored.Manifest, want)
		}
	}
}

// TestSurveyReadsManifestsWithBoundedParallelism (A5, S1): the manifest reads
// of one survey run with at most call_parallelism in flight — twenty-five
// one-second reads finish in thirteen seconds, not twenty-five — and every run
// still gets its own manifest.
func TestSurveyReadsManifestsWithBoundedParallelism(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := &slowGetStorage{Fake: storagetest.NewFake(), delay: time.Second}
		now := time.Now().UTC()
		for i := range 25 {
			putRun(t, f.Fake, "primary", fmt.Sprintf("202610%02dT040000Z", i+1), now.Add(-time.Duration(25-i)*time.Hour), true)
		}
		opts := testOptions(t)

		start := time.Now()
		sur := opts.SurveyStorage(context.Background(), "runs/", testStorage("primary", f, 2))
		if elapsed := time.Since(start); elapsed > 14*time.Second {
			t.Fatalf("the survey took %s for 25 one-second reads, want at most 14s with two in flight", elapsed)
		}
		if !sur.OK || len(sur.Runs) != 25 {
			t.Fatalf("survey ok=%v runs=%d", sur.OK, len(sur.Runs))
		}
		if got := f.peakInFlight(); got != 2 {
			t.Fatalf("the survey held %d manifest reads in flight, want the configured 2", got)
		}
		for id, r := range sur.Runs {
			if r.Manifest == nil || r.ManifestProblem != "" {
				t.Fatalf("run %s = manifest %v problem %q, want its own manifest", id, r.Manifest, r.ManifestProblem)
			}
		}
	})
}

// TestSurveyParallelMatchesSequential (F7, S1): the parallel read changes no
// field of the result — the same runs, objects, manifests and statuses come
// back whatever order the reads finish in.
func TestSurveyParallelMatchesSequential(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := storagetest.NewFake()
		now := time.Now().UTC()
		for i := range 3 {
			putRun(t, f, "primary", fmt.Sprintf("r%d", i+1), now.Add(-time.Duration(3-i)*time.Hour), true)
		}
		// The first manifest reads slowest, so the reads finish in reverse order.
		f.Fault = func(op storagetest.Op, key string, _ int) error {
			if op == storagetest.OpGet {
				switch {
				case strings.HasPrefix(key, "runs/r1/"):
					time.Sleep(30 * time.Millisecond)
				case strings.HasPrefix(key, "runs/r2/"):
					time.Sleep(20 * time.Millisecond)
				}
			}
			return nil
		}
		opts := testOptions(t)

		parallel := opts.SurveyStorage(context.Background(), "runs/", testStorage("primary", f, 3))
		sequential := opts.SurveyStorage(context.Background(), "runs/", testStorage("primary", f, 1))
		if !reflect.DeepEqual(parallel, sequential) {
			t.Fatalf("the parallel survey differs from the sequential one:\nparallel:   %+v\nsequential: %+v", parallel, sequential)
		}
	})
}

// slowGetStorage is a fake whose Get takes the given time, counting how many
// reads are in flight — the witness for the manifest-read ceiling.
type slowGetStorage struct {
	*storagetest.Fake
	delay time.Duration
	mu    sync.Mutex
	now   int
	peak  int
}

func (s *slowGetStorage) Get(ctx context.Context, key string, dst io.Writer) error {
	s.mu.Lock()
	s.now++
	if s.now > s.peak {
		s.peak = s.now
	}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.now--
		s.mu.Unlock()
	}()
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	return s.Fake.Get(ctx, key, dst)
}

func (s *slowGetStorage) peakInFlight() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.peak
}

// TestSurveyEmitsOneStorageSurveyLine (A4, S1): a finished survey tells the log
// what it saw — the runs, objects and manifests the storage holds and how long
// listing and reading took.
func TestSurveyEmitsOneStorageSurveyLine(t *testing.T) {
	f := storagetest.NewFake()
	now := time.Now().UTC()
	putRun(t, f, "primary", "20261001T040000Z", now.Add(-48*time.Hour), true)
	putRun(t, f, "primary", "20261002T040000Z", now.Add(-24*time.Hour), true)
	var buf bytes.Buffer
	opts := testOptions(t)

	sur := opts.SurveyStorage(context.Background(), "runs/",
		testStorageWithLog("primary", f, slog.New(human.NewHandler(&buf, slog.LevelInfo))))
	if !sur.OK {
		t.Fatalf("survey failed: %v", sur.Err)
	}
	line := buf.String()
	for _, want := range []string{"survey: 2 runs", "2 manifests"} {
		if !strings.Contains(line, want) {
			t.Errorf("the survey line does not carry %q:\n%s", want, line)
		}
	}
}

// TestSurveyFailureLeavesOnlyTheFailureLine (F3, S1): a survey that could not
// list emits storage.survey.failed and no storage.survey line of its own — the
// storage that did list still reports what it saw, so a reader never takes a
// missing line for a survey that found nothing.
func TestSurveyFailureLeavesOnlyTheFailureLine(t *testing.T) {
	var buf bytes.Buffer
	opts := testOptions(t)
	logger := slog.New(human.NewHandler(&buf, slog.LevelInfo))
	good := storagetest.NewFake()
	putRun(t, good, "good", "20261001T040000Z", time.Now().UTC().Add(-24*time.Hour), true)
	bad := storagetest.NewFake()
	bad.Fault = func(op storagetest.Op, _ string, _ int) error {
		if op == storagetest.OpList {
			return pluginerrors.Permanent(errors.New("list refused"))
		}
		return nil
	}
	if sur := opts.SurveyStorage(context.Background(), "runs/", testStorageWithLog("good", good, logger)); !sur.OK {
		t.Fatalf("the good storage failed: %v", sur.Err)
	}
	if sur := opts.SurveyStorage(context.Background(), "runs/", testStorageWithLog("bad", bad, logger)); sur.OK {
		t.Fatal("the bad storage listed")
	}
	if !strings.Contains(buf.String(), "listing what the storage holds: list refused") {
		t.Fatalf("the failure line is missing:\n%s", buf.String())
	}
	if got := surveyLogLines(buf.String()); len(got) != 1 {
		t.Fatalf("the log carries %d survey lines, want only the storage that listed:\n%s", len(got), buf.String())
	}
	if !strings.Contains(buf.String(), "survey: 1 runs, 2 objects, 1 manifests") {
		t.Fatalf("the successful storage's survey line is missing:\n%s", buf.String())
	}
}

// TestM9_RunInventoryOrderIsDeterministic: the survey walks the runs in
// ascending id order on every pass — the stdlib map-key sort is the one
// ordering the job list and its per-run events rely on.
func TestM9_RunInventoryOrderIsDeterministic(t *testing.T) {
	f := storagetest.NewFake()
	now := time.Now().UTC()
	ids := []string{"20261002T040000Z", "20261001T040000Z", "20261003T040000Z"}
	for _, id := range ids {
		f.PutObject("runs/"+id+"/a.bin", []byte("x"), now)
		f.PutObject("runs/"+id+"/manifest.json", []byte("{"), now)
	}
	want := slices.Sorted(slices.Values(ids))
	for pass := range 2 {
		var buf bytes.Buffer
		sur := testOptions(t).SurveyStorage(context.Background(), "runs/",
			testStorageWithLog("primary", f, slog.New(human.NewHandler(&buf, slog.LevelInfo))))
		if !sur.OK {
			t.Fatalf("pass %d: survey failed: %v", pass, sur.Err)
		}
		var got []string
		for _, line := range strings.Split(buf.String(), "\n") {
			if !strings.Contains(line, "storage.manifest-unreadable") {
				continue
			}
			for _, id := range ids {
				if strings.Contains(line, "run="+id) {
					got = append(got, id)
				}
			}
		}
		if !slices.Equal(got, want) {
			t.Fatalf("pass %d: unreadable-manifest order = %v, want %v", pass, got, want)
		}
	}
}

// surveyLogLines is the successful survey lines of a log: a survey line is the
// one carrying the survey's own counts, not the failure that names its reason.
func surveyLogLines(log string) []string {
	var out []string
	for _, line := range strings.Split(log, "\n") {
		if strings.Contains(line, "survey: ") {
			out = append(out, line)
		}
	}
	return out
}

// TestSurveyManifestReadProblems (A8, S2): a listed manifest that reads back
// absent, does not parse or carries a foreign schema is a problem — never the
// verdict that the run has no manifest, which a deletion decision would act on.
func TestSurveyManifestReadProblems(t *testing.T) {
	now := time.Now().UTC()
	for name, tc := range map[string]struct {
		body  string
		fault error
		want  string
	}{
		"absent":     {fault: pluginerrors.NotFound(errors.New("gone")), want: "reads back absent"},
		"unparsable": {body: "{", want: "parsing the manifest"},
		"foreign schema": {
			body: string(mustMarshal(t, model.RunReport{
				Schema: model.ManifestSchema + 1, RunID: "r1", Kind: model.RunKindScheduled, StartedAt: now,
			})),
			want: "manifest schema",
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := storagetest.NewFake()
			f.PutObject("runs/r1/a.bin", []byte("x"), now)
			f.PutObject("runs/r1/manifest.json", []byte(tc.body), now)
			f.Fault = func(op storagetest.Op, key string, _ int) error {
				if op == storagetest.OpGet && key == "runs/r1/manifest.json" && tc.fault != nil {
					return tc.fault
				}
				return nil
			}
			sur := testOptions(t).SurveyStorage(context.Background(), "runs/", testStorage("primary", f, 1))
			r := sur.Runs["r1"]
			if !strings.Contains(r.ManifestProblem, tc.want) {
				t.Fatalf("manifest problem %q, want %q", r.ManifestProblem, tc.want)
			}
		})
	}
}

// panicStorage fails its operation by panicking, the crash a survey must turn
// into a classified error rather than take the process down.
type panicStorage struct {
	*storagetest.Fake
	list bool
	get  bool
}

func (p *panicStorage) List(ctx context.Context, req storage.ListRequest) (storage.ListPage, error) {
	if p.list {
		panic("list boom")
	}
	return p.Fake.List(ctx, req)
}

func (p *panicStorage) Get(ctx context.Context, key string, dst io.Writer) error {
	if p.get {
		panic("get boom")
	}
	return p.Fake.Get(ctx, key, dst)
}

// TestSurveyRecoversAPluginPanic: a panicking storage fails the listing or the
// manifest read with a permanent cause, never the process.
func TestSurveyRecoversAPluginPanic(t *testing.T) {
	opts := testOptions(t)
	f := &panicStorage{Fake: storagetest.NewFake(), list: true}
	sur := opts.SurveyStorage(context.Background(), "runs/", testStorage("primary", f, 1))
	if sur.OK || sur.Err == nil || !strings.Contains(sur.Err.Error(), "plugin panic: list boom") {
		t.Fatalf("list panic = ok=%v err=%v, want a classified failure", sur.OK, sur.Err)
	}

	f.list, f.get = false, true
	f.PutObject("runs/r1/a.bin", []byte("x"), time.Now().UTC())
	f.PutObject("runs/r1/manifest.json", []byte("{}"), time.Now().UTC())
	sur = opts.SurveyStorage(context.Background(), "runs/", testStorage("primary", f, 1))
	if r := sur.Runs["r1"]; !strings.Contains(r.ManifestProblem, "plugin panic: get boom") {
		t.Fatalf("get panic = %+v, want a classified manifest problem", r)
	}
}
