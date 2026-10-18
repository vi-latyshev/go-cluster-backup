package engine

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/spool"
	"github.com/vi-latyshev/go-cluster-backup/internal/storage/storagetest"
)

// TestSourceDescribeFailureFailsTheArtifact: a source that cannot describe
// itself fails its artifact before any byte.
func TestSourceDescribeFailureFailsTheArtifact(t *testing.T) {
	w := newWorld(t)
	w.sources["vpn"] = &fakeSource{data: []byte("x"), describeErr: errors.New("no describe")}
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	e := w.newEngine(t, cfg)

	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	got := artifactByName(rep, "wireguard")
	if got.Outcome != model.ArtifactFailed || !strings.Contains(got.Error, "no describe") {
		t.Fatalf("artifact %+v", got)
	}
}

// TestProcessorDescribeFailureFailsTheArtifact: a processor whose traits cannot
// be read fails the artifact's chain.
func TestProcessorDescribeFailureFailsTheArtifact(t *testing.T) {
	w := newWorld(t)
	registerProcessorType(t, w, "errd", &errDescribeProcessor{})
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["processors"] = map[string]any{"p": instance("errd")}
	cfg["sources"] = map[string]any{
		"vpn": sourceInstance("fake", "vpn", "wireguard", []string{"p"}, "required", "plain"),
	}
	e := w.newEngine(t, cfg)

	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	got := artifactByName(rep, "wireguard")
	if got.Outcome != model.ArtifactFailed || !strings.Contains(got.Error, "processor 0 describe") {
		t.Fatalf("artifact %+v", got)
	}
}

// --- produce: spool failures -------------------------------------------------

// TestProduceSpoolFreeBytesFails: a spool whose directory is gone cannot report
// its free space, and the artifact fails before anything is written.
func TestProduceSpoolFreeBytesFails(t *testing.T) {
	w := newWorld(t)
	e := w.newEngine(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}))
	dir := filepath.Join(t.TempDir(), "gone")
	sp, err := spool.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	plan := artifactPlan{instance: "x", srcType: "fake", src: &fakeSource{data: []byte("z")},
		artifact: model.Artifact{Group: "g", Name: "n"}}

	_, err = e.produce(context.Background(), sp, plan)
	if err == nil || !strings.Contains(err.Error(), "statfs") {
		t.Fatalf("err = %v, want the statfs failure", err)
	}
}

// TestProduceSpoolCreateFails: a spool file that cannot be created is permanent —
// the next attempt would fail the same way.
func TestProduceSpoolCreateFails(t *testing.T) {
	w := newWorld(t)
	e := w.newEngine(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}))
	dir := t.TempDir()
	sp, err := spool.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	// The source reports ".dat", so the first artifact's spool file is
	// "001-g-n.dat"; a directory of that name makes Create fail.
	if err := os.MkdirAll(filepath.Join(dir, "001-g-n.dat"), 0o755); err != nil {
		t.Fatal(err)
	}
	plan := artifactPlan{instance: "x", srcType: "fake", src: &fakeSource{data: []byte("z")},
		artifact: model.Artifact{Group: "g", Name: "n"}}

	_, err = e.produce(context.Background(), sp, plan)
	if err == nil || !strings.Contains(err.Error(), "spool") {
		t.Fatalf("err = %v, want the spool create failure", err)
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassPermanent {
		t.Fatalf("class %q, want permanent: %v", class, err)
	}
}

// --- dumpStats: ErrUnsupported is no report ----------------------------------

// TestDumpStatsUnsupportedIsEmpty: a source that answers ErrUnsupported leaves
// out nothing the run has to report, and its artifact still runs.
func TestDumpStatsUnsupportedIsEmpty(t *testing.T) {
	w := newWorld(t)
	w.sources["vpn"] = &fakeSource{data: []byte("x"), statsErr: errors.ErrUnsupported}
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	e := w.newEngine(t, cfg)

	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	got := artifactByName(rep, "wireguard")
	if got.Outcome != model.ArtifactOK {
		t.Fatalf("artifact %+v", got)
	}
	if got.Stats != (model.DumpStats{}) {
		t.Fatalf("stats %+v, want the zero report", got.Stats)
	}
}

// --- watchProgress: no interval means no watcher ------------------------------

// TestWatchProgressSkipsAnUnchangedCount: a tick that finds the same total as
// the last one logs nothing, and the next real change logs exactly once. The
// clock is virtual, so the ticks are deterministic.
func TestWatchProgressSkipsAnUnchangedCount(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := &countingHandler{}
		e := &Engine{log: slog.New(h), cfg: &Config{Run: &RunConfig{ProgressInterval: model.Duration(time.Second)}}}
		raw := &countWriter{} // never advances on its own: three ticks see the same total
		stop := e.watchProgress(context.Background(), artifactPlan{instance: "x", srcType: "synthetic"}, raw)
		time.Sleep(3 * time.Second)
		synctest.Wait()
		if got := h.count(); got != 0 {
			t.Fatalf("%d progress records for a count that never moved", got)
		}
		raw.n.Store(16)
		time.Sleep(time.Second)
		synctest.Wait()
		stop()
		if got := h.count(); got != 1 {
			t.Fatalf("%d progress records, want exactly 1 for the one change", got)
		}
	})
}

// TestWatchProgressWithoutARunSection: a config without a run section has no
// interval to watch at, and the returned stop is a no-op that returns at once.
func TestWatchProgressWithoutARunSection(t *testing.T) {
	e := &Engine{cfg: &Config{}}
	stop := e.watchProgress(context.Background(), artifactPlan{instance: "thing", srcType: "synthetic"}, &countWriter{})
	done := make(chan struct{})
	go func() { stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the no-op stop did not return")
	}
}

// TestProgressIntervalZeroStillRuns: with the interval at 0 the watcher is a
// no-op and the artifact is produced exactly as before.
func TestProgressIntervalZeroStillRuns(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["run"].(map[string]any)["progress_interval"] = "0s"
	e := w.newEngine(t, cfg)

	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeDelivered || artifactByName(rep, "wireguard").Outcome != model.ArtifactOK {
		t.Fatalf("report %+v", rep)
	}
}

// --- processArtifact: the artifact key can fail after the bytes --------------

// TestArtifactKeyFailureFailsTheArtifact: the object was produced but its key
// cannot be rendered, so nothing is uploaded and the artifact fails.
func TestArtifactKeyFailureFailsTheArtifact(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["layout"].(map[string]any)["artifact_key"] = "{{ .Nope }}"
	e := w.newEngine(t, cfg)

	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	got := artifactByName(rep, "wireguard")
	if got.Outcome != model.ArtifactFailed || !strings.Contains(got.Error, "layout.artifact_key") {
		t.Fatalf("artifact %+v", got)
	}
	if keys := w.storage(t, "primary").Keys(); len(keys) != 0 {
		t.Fatalf("a keyless artifact was uploaded: %v", keys)
	}
}

// --- putWhole / verifyObject -------------------------------------------------

// TestPutWholeSpoolOpenFails: an upload attempt whose spool file cannot be
// re-opened is permanent — a retry would read the same missing file.
func TestPutWholeSpoolOpenFails(t *testing.T) {
	w := newWorld(t)
	e := w.newEngine(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}))
	sp, err := spool.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	sb := &storageBinding{name: "primary", inst: storagetest.NewFake(), cfg: StorageInstance{Type: "fake"}}
	st := &storageRunState{name: "primary", typ: "fake"}
	pa := producedArtifact{artifact: model.Artifact{Group: "g", Name: "n"}, file: "missing", bytes: 3}

	err = e.putWhole(context.Background(), sb, st, sp, pa, "k")
	if err == nil || !strings.Contains(err.Error(), "spool") {
		t.Fatalf("err = %v, want the spool open failure", err)
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassPermanent {
		t.Fatalf("class %q, want permanent: %v", class, err)
	}
	if st.result.Attempts != 1 {
		t.Fatalf("attempts = %d, want exactly 1 (permanent is not retried)", st.result.Attempts)
	}
}

// TestVerifyObjectSizeMismatch: an object visible at the wrong size is not a
// committed upload.
func TestVerifyObjectSizeMismatch(t *testing.T) {
	w := newWorld(t)
	e := w.newEngine(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}))
	f := storagetest.NewFake()
	f.PutObject("k", []byte("abc"), time.Now().UTC())
	sb := &storageBinding{name: "primary", inst: f, cfg: StorageInstance{Type: "fake"}}

	err := e.verifyObject(context.Background(), sb, "k", "artifact x", 99)
	if err == nil || !strings.Contains(err.Error(), "size after upload is 3, want 99") {
		t.Fatalf("err = %v, want the size mismatch", err)
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassTransient {
		t.Fatalf("class %q, want transient: %v", class, err)
	}
}

// --- writeManifests ----------------------------------------------------------

// TestWriteManifestsMarshalFailureMarksEveryStorageIncomplete: a report that
// cannot be marshalled cannot be stored, so every storage that took objects is
// marked incomplete and no manifest is written.
func TestWriteManifestsMarshalFailureMarksEveryStorageIncomplete(t *testing.T) {
	w := newWorld(t)
	e := w.newEngine(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}))
	f := storagetest.NewFake()
	sb := &storageBinding{name: "primary", inst: f, cfg: StorageInstance{Type: "fake"}}
	st := newStorageRunState(sb)
	st.tried = true
	st.result.Objects = 1
	st.result.Bytes = 10
	results := map[string]*storageRunState{"primary": st}
	// A start time no manifest can render: json/v2 refuses a year past 9999.
	rep := &model.RunReport{Schema: model.ManifestSchema, RunID: "R",
		StartedAt: time.Date(12000, 1, 1, 0, 0, 0, 0, time.UTC), Outcome: model.OutcomeDelivered}

	e.writeManifests(context.Background(), model.RunKindScheduled, []*storageBinding{sb}, results,
		model.OutcomeDelivered, rep, map[string]*capState{})

	if st.uploadErr == nil || !strings.Contains(st.uploadErr.Error(), "manifest marshal") {
		t.Fatalf("uploadErr = %v, want the marshal failure", st.uploadErr)
	}
	if st.fatal == nil {
		t.Fatal("a storage that took objects was not marked fatal")
	}
	if keys := f.Keys(); len(keys) != 0 {
		t.Fatalf("a manifest was written despite the marshal failure: %v", keys)
	}
}

// TestManifestKeyFailureFailsTheStorage: the objects are there but the record of
// them cannot be named, so the copy is incomplete and the run fails.
func TestManifestKeyFailureFailsTheStorage(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["layout"].(map[string]any)["manifest_key"] = "{{ .Nope }}"
	e := w.newEngine(t, cfg)

	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeFailed {
		t.Fatalf("outcome %q", rep.Outcome)
	}
	got := storageResult(rep, "primary")
	if got.Outcome != model.StorageFailed || !strings.Contains(got.Error, "layout.manifest_key") {
		t.Fatalf("storage %+v", got)
	}
}

// --- catalog: load and save failures -----------------------------------------
