package engine

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/discovery"
	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/processor"
	"github.com/vi-latyshev/go-cluster-backup/internal/source"
	"github.com/vi-latyshev/go-cluster-backup/internal/spool"
	"github.com/vi-latyshev/go-cluster-backup/internal/storage"
	"github.com/vi-latyshev/go-cluster-backup/internal/storage/storagetest"
)

func testTime() time.Time { return time.Now().UTC() }

// withRestore attaches a restore section to a config.
func withRestore(t *testing.T, cfg map[string]any, order ...string) map[string]any {
	t.Helper()
	cfg["restore"] = map[string]any{
		"spool_dir":           t.TempDir(),
		"spool_reserve_bytes": 0,
		"storage_order":       order,
		"unclassified_error":  "transient",
		"retry":               map[string]any{"attempts": 3, "initial_delay": "1ms", "max_delay": "5ms", "jitter": 0},
	}
	return cfg
}

// runOnce performs a delivered run so the storages hold a real manifest.
func runOnce(t *testing.T, w *world, cfg map[string]any) *Engine {
	t.Helper()
	e := w.newEngine(t, cfg)
	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeDelivered {
		t.Fatalf("fixture run: %+v", rep)
	}
	return e
}

func TestFetchRoundTrip(t *testing.T) {
	w := newWorld(t)
	cfg := withRestore(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}), "primary")
	e := runOnce(t, w, cfg)
	runs, err := e.StoredRuns(context.Background(), StoredRunsRequest{})
	if err != nil || len(runs) != 1 {
		t.Fatalf("stored runs %v, %v", runs, err)
	}
	runID := runs[0].RunID
	src := w.sources["vpn"]

	var raw bytes.Buffer
	rep, err := e.Fetch(context.Background(), FetchRequest{RunID: runID, Artifact: "wireguard"}, &raw)
	if err != nil {
		t.Fatal(err)
	}
	if !rep.Verified || rep.Storage != "primary" || rep.Bytes != int64(len(src.data)) {
		t.Fatalf("fetch report %+v", rep)
	}
	if !bytes.Equal(raw.Bytes(), src.data) {
		t.Fatalf("raw fetch returned %q", raw.Bytes())
	}

	var decoded bytes.Buffer
	rep, err = e.Fetch(context.Background(), FetchRequest{RunID: runID, Artifact: "wireguard", Decode: true}, &decoded)
	if err != nil {
		t.Fatal(err)
	}
	if !rep.Verified || !bytes.Equal(decoded.Bytes(), src.data) {
		t.Fatalf("decoded fetch: %+v, %q", rep, decoded.Bytes())
	}
}

func TestFetchRawByKey(t *testing.T) {
	w := newWorld(t)
	cfg := withRestore(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}), "primary")
	e := runOnce(t, w, cfg)
	runs, _ := e.StoredRuns(context.Background(), StoredRunsRequest{})
	key := "runs/" + runs[0].RunID + "/vpn/wireguard.dat.zst"
	var out bytes.Buffer
	rep, err := e.Fetch(context.Background(), FetchRequest{RawStorage: "primary", RawKey: key}, &out)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Verified {
		t.Fatal("a raw by-key fetch must be reported as unverified")
	}
	if rep.Key != key || !bytes.Equal(out.Bytes(), w.sources["vpn"].data) {
		t.Fatalf("raw fetch %+v, %q", rep, out.Bytes())
	}
	// Request shapes.
	if _, err := e.Fetch(context.Background(), FetchRequest{RawStorage: "primary"}, &out); err == nil {
		t.Fatal("RawStorage without RawKey accepted")
	}
	if _, err := e.Fetch(context.Background(), FetchRequest{RawStorage: "primary", RawKey: key, RunID: "x"}, &out); err == nil {
		t.Fatal("raw fetch with RunID accepted")
	}
	if _, err := e.Fetch(context.Background(), FetchRequest{}, &out); err == nil {
		t.Fatal("fetch without RunID/Newest accepted")
	}
	if _, err := e.Fetch(context.Background(), FetchRequest{RunID: "x", Artifact: "a", Newest: true}, &out); err == nil {
		t.Fatal("RunID and Newest together accepted")
	}
}

// TestFetchFallsBackToTheSecondStorage covers the three ways the first storage
// lets a fetch down: not found, wrong size, wrong hash.
func TestFetchFallsBackToTheSecondStorage(t *testing.T) {
	cases := []struct {
		name      string
		corrupt   func(t *testing.T, f *storagetest.Fake, key string, body []byte)
		reasonSub string
	}{
		{"not_found", func(_ *testing.T, f *storagetest.Fake, key string, _ []byte) {
			_ = f.Delete(context.Background(), key)
		}, "not found"},
		{"wrong size", func(_ *testing.T, f *storagetest.Fake, key string, _ []byte) {
			f.PutObject(key, []byte("short"), testTime())
		}, "manifest says"},
		{"wrong hash", func(_ *testing.T, f *storagetest.Fake, key string, body []byte) {
			other := bytes.Repeat([]byte{'z'}, len(body))
			f.PutObject(key, other, testTime())
		}, "sha256"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := newWorld(t)
			cfg := withRestore(t, baseConfig(t, twoStorages()), "primary", "offsite")
			e := runOnce(t, w, cfg)
			runs, _ := e.StoredRuns(context.Background(), StoredRunsRequest{})
			runID := runs[0].RunID
			key := "runs/" + runID + "/vpn/wireguard.dat.zst"
			body := w.sources["vpn"].data
			tc.corrupt(t, w.storage(t, "primary"), key, body)

			var out bytes.Buffer
			rep, err := e.Fetch(context.Background(), FetchRequest{RunID: runID, Artifact: "wireguard"}, &out)
			if err != nil {
				t.Fatal(err)
			}
			if rep.Storage != "offsite" {
				t.Fatalf("served from %q, want offsite", rep.Storage)
			}
			if len(rep.Skipped) != 1 || rep.Skipped[0].Storage != "primary" ||
				!strings.Contains(rep.Skipped[0].Reason, tc.reasonSub) {
				t.Fatalf("skip reasons %+v", rep.Skipped)
			}
			if !bytes.Equal(out.Bytes(), body) {
				t.Fatal("the second storage returned wrong bytes")
			}
		})
	}
}

// TestFetchRetryDoesNotCorruptDestination: a first attempt that fails mid-read
// must not leave half an object in dst; the destination sees exactly one copy.
func TestFetchRetryDoesNotCorruptDestination(t *testing.T) {
	w := newWorld(t)
	f := storagetest.NewFake()
	faults := 0
	w.storages["primary"] = f
	cfg := withRestore(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}), "primary")
	e := runOnce(t, w, cfg)
	runs, _ := e.StoredRuns(context.Background(), StoredRunsRequest{})
	runID := runs[0].RunID
	f.Fault = func(op storagetest.Op, key string, call int) error {
		if op == storagetest.OpGet && faults == 0 {
			faults++
			return plugin.Transient(errors.New("connection reset"))
		}
		return nil
	}
	var out bytes.Buffer
	rep, err := e.Fetch(context.Background(), FetchRequest{RunID: runID, Artifact: "wireguard"}, &out)
	if err != nil {
		t.Fatal(err)
	}
	if !rep.Verified || out.Len() != int(rep.Bytes) {
		t.Fatalf("destination holds %d bytes, report %+v", out.Len(), rep)
	}
}

// TestFetchWithRenamedLayoutAndPipeline: the manifest carries the key and the
// chain, so a config whose own layout and pipeline differ from the run's still
// restores that run — a restore never reads the current config's pipeline.
// (The run prefix keeps its literal part — that is what a survey lists.)
func TestFetchWithRenamedLayoutAndPipeline(t *testing.T) {
	w := newWorld(t)
	cfg := withRestore(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}), "primary")
	e := runOnce(t, w, cfg)
	runs, _ := e.StoredRuns(context.Background(), StoredRunsRequest{})
	runID := runs[0].RunID

	renamed := withRestore(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}), "primary")
	// The artifact key template and the pipeline names change; the run prefix's
	// literal part and the manifest key are the survey's anchors and must stay.
	renamed["layout"] = map[string]any{
		"run_prefix":       "runs/{{ .Run.ID }}",
		"artifact_key":     "by-run/{{ .Artifact.Name }}{{ .Artifact.Ext }}",
		"manifest_key":     "manifest.json",
		"manual_id_suffix": "-manual",
	}
	// The source's own pipeline names a second processor this run never used: the
	// fetch still decodes with the chain the manifest recorded.
	renamed["processors"] = map[string]any{"zstd3": instance("fake"), "pack2": instance("fake")}
	renamed["sources"] = map[string]any{
		"vpn": sourceInstance("fake", "vpn", "wireguard", []string{"zstd3", "pack2"}, "required", "plain"),
	}
	w2 := newWorld(t)
	w2.storages["primary"] = w.storage(t, "primary")
	e2 := w2.newEngine(t, renamed)

	var out bytes.Buffer
	rep, err := e2.Fetch(context.Background(), FetchRequest{RunID: runID, Artifact: "wireguard", Decode: true}, &out)
	if err != nil {
		t.Fatal(err)
	}
	if !rep.Verified || rep.Key != "runs/"+runID+"/vpn/wireguard.dat.zst" {
		t.Fatalf("report %+v", rep)
	}
	if !bytes.Equal(out.Bytes(), w.sources["vpn"].data) {
		t.Fatal("decoded bytes differ from the source")
	}
}

func TestFetchNewestOK(t *testing.T) {
	w := newWorld(t)
	cfg := withRestore(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}), "primary")
	e := runOnce(t, w, cfg)
	runs, _ := e.StoredRuns(context.Background(), StoredRunsRequest{})
	oldRun := runs[0].RunID

	// A newer run where the artifact failed must be passed over for the older ok one.
	manifest := mustMarshal(t, model.RunReport{
		Schema: model.ManifestSchema, RunID: "20990101T000000Z", StartedAt: testTime().AddDate(1, 0, 0),
		Outcome: model.OutcomeFailed,
		Artifacts: []model.ArtifactResult{{
			Artifact: model.Artifact{Name: "wireguard"}, Outcome: model.ArtifactFailed, Error: "boom",
		}},
	})
	w.storage(t, "primary").PutObject("runs/20990101T000000Z/manifest.json", manifest, testTime().AddDate(1, 0, 0))

	var out bytes.Buffer
	rep, err := e.Fetch(context.Background(), FetchRequest{Newest: true, Artifact: "wireguard"}, &out)
	if err != nil {
		t.Fatal(err)
	}
	if rep.RunID != oldRun {
		t.Fatalf("newest-ok served run %q, want %q", rep.RunID, oldRun)
	}
}

func TestFetchRefusesFailedArtifactAndNamesNearestOK(t *testing.T) {
	w := newWorld(t)
	cfg := withRestore(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}), "primary")
	e := runOnce(t, w, cfg)
	runs, _ := e.StoredRuns(context.Background(), StoredRunsRequest{})
	okRun := runs[0].RunID
	f := w.storage(t, "primary")

	failed := mustMarshal(t, model.RunReport{
		Schema: model.ManifestSchema, RunID: "20990101T000000Z", StartedAt: testTime().AddDate(1, 0, 0),
		Artifacts: []model.ArtifactResult{{Artifact: model.Artifact{Name: "wireguard"}, Outcome: model.ArtifactFailed}},
	})
	f.PutObject("runs/20990101T000000Z/manifest.json", failed, testTime().AddDate(1, 0, 0))

	var out bytes.Buffer
	_, err := e.Fetch(context.Background(), FetchRequest{RunID: "20990101T000000Z", Artifact: "wireguard"}, &out)
	if err == nil {
		t.Fatal("a failed artifact was fetched")
	}
	if !strings.Contains(err.Error(), okRun) {
		t.Fatalf("error does not name the nearest ok run %q: %v", okRun, err)
	}
}

func TestFetchRefusesUnknownSchema(t *testing.T) {
	w := newWorld(t)
	cfg := withRestore(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}), "primary")
	e := runOnce(t, w, cfg)
	runs, _ := e.StoredRuns(context.Background(), StoredRunsRequest{})
	runID := runs[0].RunID
	f := w.storage(t, "primary")
	body, _ := f.Bytes("runs/" + runID + "/manifest.json")
	var rep model.RunReport
	if err := jsonUnmarshal(body, &rep); err != nil {
		t.Fatal(err)
	}
	rep.Schema = 7
	f.PutObject("runs/"+runID+"/manifest.json", mustMarshal(t, rep), testTime())

	var out bytes.Buffer
	_, err := e.Fetch(context.Background(), FetchRequest{RunID: runID, Artifact: "wireguard"}, &out)
	if err == nil || !strings.Contains(err.Error(), "schema") {
		t.Fatalf("unknown schema accepted: %v", err)
	}
}

// TestRestorePathsAreReadOnly: a fetch never writes to a storage, and the
// wrapper refuses writes by construction.
func TestRestorePathsAreReadOnly(t *testing.T) {
	w := newWorld(t)
	cfg := withRestore(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}), "primary")
	e := runOnce(t, w, cfg)
	f := w.storage(t, "primary")
	runs, _ := e.StoredRuns(context.Background(), StoredRunsRequest{})
	before := len(f.Events())
	var out bytes.Buffer
	if _, err := e.Fetch(context.Background(), FetchRequest{RunID: runs[0].RunID, Artifact: "wireguard"}, &out); err != nil {
		t.Fatal(err)
	}
	for _, ev := range f.Events()[before:] {
		if ev.Op == storagetest.OpPut || ev.Op == storagetest.OpDelete {
			t.Fatalf("a restore path wrote to the storage: %v", ev)
		}
	}
	ro := readOnlyStorage{f}
	if _, err := ro.Put(context.Background(), storage.PutRequest{Key: "k"}, bytes.NewReader(nil)); !errors.Is(err, errors.ErrUnsupported) {
		t.Fatalf("read-only Put: %v", err)
	}
	if err := ro.Delete(context.Background(), "k"); !errors.Is(err, errors.ErrUnsupported) {
		t.Fatalf("read-only Delete: %v", err)
	}
}

func TestStoredRunsListsRunsWithoutManifest(t *testing.T) {
	w := newWorld(t)
	cfg := withRestore(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}), "primary")
	e := runOnce(t, w, cfg)
	w.storage(t, "primary").PutObject("runs/20990101T000000Z/leftover.bin", []byte("x"), testTime())
	runs, err := e.StoredRuns(context.Background(), StoredRunsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 2 {
		t.Fatalf("stored runs %+v", runs)
	}
	if runs[0].RunID != "20990101T000000Z" || runs[0].HasManifest {
		t.Fatalf("newest entry %+v", runs[0])
	}
	if runs[1].HasManifest != true || runs[1].Report.RunID == "" {
		t.Fatalf("manifest entry %+v", runs[1])
	}
}

func TestVerifyDepths(t *testing.T) {
	w := newWorld(t)
	cfg := withRestore(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}), "primary")
	e := runOnce(t, w, cfg)
	runs, _ := e.StoredRuns(context.Background(), StoredRunsRequest{})
	runID := runs[0].RunID

	res, err := e.Verify(context.Background(), VerifyRequest{Fetch: FetchRequest{RunID: runID}, Depth: "stored"})
	if err != nil || len(res) != 1 || !res[0].OK || res[0].Artifact != "wireguard" {
		t.Fatalf("stored verify: %+v, %v", res, err)
	}
	res, err = e.Verify(context.Background(), VerifyRequest{Fetch: FetchRequest{RunID: runID, Artifact: "wireguard"}, Depth: "decoded"})
	if err != nil || len(res) != 1 || !res[0].OK {
		t.Fatalf("decoded verify: %+v, %v", res, err)
	}
	if _, err := e.Verify(context.Background(), VerifyRequest{Fetch: FetchRequest{RunID: runID}, Depth: "nope"}); err == nil {
		t.Fatal("unknown depth accepted")
	}
	if _, err := e.Verify(context.Background(), VerifyRequest{Fetch: FetchRequest{RunID: runID}, Depth: "probed"}); err == nil {
		t.Fatal("probed without a target accepted")
	}

	// A corrupted stored object fails the stored depth.
	f := w.storage(t, "primary")
	key := "runs/" + runID + "/vpn/wireguard.dat.zst"
	f.PutObject(key, []byte("short"), testTime())
	res, err = e.Verify(context.Background(), VerifyRequest{Fetch: FetchRequest{RunID: runID, Artifact: "wireguard"}, Depth: "stored"})
	if err != nil {
		t.Fatal(err)
	}
	if res[0].OK {
		t.Fatalf("a size mismatch verified as ok: %+v", res[0])
	}
}

// fakeRestorer is a target source that can be applied to and probed.
type fakeRestorer struct {
	fakeSource
	applied    int
	probed     int
	probeFirst bool
	failProbe  error
	applyErr   error
	gotMode    source.ApplyMode
	gotOptions string
	gotBytes   []byte
}

func (f *fakeRestorer) Apply(ctx context.Context, req source.ApplyRequest, src io.Reader) (source.ApplyReport, error) {
	if err := ctx.Err(); err != nil {
		return source.ApplyReport{}, err
	}
	if f.probeFirst {
		f.applied++
	}
	body, err := io.ReadAll(src)
	if err != nil {
		return source.ApplyReport{}, err
	}
	f.applied++
	f.gotMode = req.Mode
	f.gotOptions = string(req.Options)
	f.gotBytes = body
	if f.applyErr != nil {
		return source.ApplyReport{}, f.applyErr
	}
	return source.ApplyReport{Target: "test-target", Bytes: int64(len(body))}, nil
}

func (f *fakeRestorer) Probe(ctx context.Context, src io.Reader) (source.ProbeReport, error) {
	if err := ctx.Err(); err != nil {
		return source.ProbeReport{}, err
	}
	if f.failProbe != nil {
		return source.ProbeReport{}, f.failProbe
	}
	body, err := io.ReadAll(src)
	if err != nil {
		return source.ProbeReport{}, err
	}
	f.probed++
	return source.ProbeReport{Entries: 1, Bytes: int64(len(body))}, nil
}

func applyConfig(t *testing.T, w *world, target string, extra func(map[string]any)) map[string]any {
	t.Helper()
	cfg := withRestore(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}), "primary")
	cfg["sources"] = map[string]any{
		"vpn": sourceInstance("fake", "vpn", "wireguard", []string{"zstd3"}, "required", "plain"),
		// The target is a separate source instance; its own artifact name stays
		// out of the way of the one being restored.
		"target": sourceInstance("fake", "targets", "restore-target", []string{"zstd3"}, "required", "plain"),
	}
	cfg["restore"].(map[string]any)["apply"] = map[string]any{
		"fake": map[string]any{"note": "target"},
	}
	if extra != nil {
		extra(cfg)
	}
	return cfg
}

func TestApplyHappySideBySide(t *testing.T) {
	w := newWorld(t)
	target := &fakeRestorer{}
	w.extraSources["target"] = target
	cfg := applyConfig(t, w, "target", nil)
	e := runOnce(t, w, cfg)
	runs, _ := e.StoredRuns(context.Background(), StoredRunsRequest{})
	runID := runs[0].RunID

	rep, err := e.Apply(context.Background(), ApplyRunRequest{
		Fetch:  FetchRequest{RunID: runID, Artifact: "wireguard"},
		Target: "target",
		Mode:   source.ApplySideBySide,
	})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Target != "test-target" {
		t.Fatalf("apply report %+v", rep)
	}
	if target.gotMode != source.ApplySideBySide {
		t.Fatalf("mode %q", target.gotMode)
	}
	if target.gotOptions != `{"note":"target"}` {
		t.Fatalf("options %q", target.gotOptions)
	}
	if !bytes.Equal(target.gotBytes, w.sources["vpn"].data) {
		t.Fatal("the restorer received wrong bytes")
	}
	if target.probed != 0 {
		t.Fatal("side-by-side must not probe")
	}
}

func TestApplyRefusals(t *testing.T) {
	w := newWorld(t)
	target := &fakeRestorer{}
	w.extraSources["target"] = target
	cfg := applyConfig(t, w, "target", nil)
	e := runOnce(t, w, cfg)
	runs, _ := e.StoredRuns(context.Background(), StoredRunsRequest{})
	runID := runs[0].RunID
	fetch := FetchRequest{RunID: runID, Artifact: "wireguard"}

	cases := []struct {
		name string
		req  ApplyRunRequest
		want string
	}{
		{"mode is required", ApplyRunRequest{Fetch: fetch, Target: "target"}, "Mode is required"},
		{"unknown mode", ApplyRunRequest{Fetch: fetch, Target: "target", Mode: "overwrite"}, "Mode is required"},
		{"target is required", ApplyRunRequest{Fetch: fetch, Mode: source.ApplySideBySide}, "Target is required"},
		{"unknown target", ApplyRunRequest{Fetch: fetch, Target: "ghost", Mode: source.ApplySideBySide}, "unknown target"},
		{"in_place needs the exact confirm", ApplyRunRequest{Fetch: fetch, Target: "target", Mode: source.ApplyInPlace}, "Confirm"},
		{"wrong confirm", ApplyRunRequest{Fetch: fetch, Target: "target", Mode: source.ApplyInPlace, Confirm: "somewhere-else"}, "Confirm"},
		{"raw fetch", ApplyRunRequest{Fetch: FetchRequest{RawStorage: "primary", RawKey: "k"}, Target: "target", Mode: source.ApplySideBySide}, "raw"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := e.Apply(context.Background(), tc.req)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %v, want %q", err, tc.want)
			}
		})
	}
}

func TestApplyInPlaceRequiresProbeAndExactConfirm(t *testing.T) {
	w := newWorld(t)
	target := &fakeRestorer{}
	target.traits.Origin = "test://target/origin"
	w.extraSources["target"] = target
	cfg := applyConfig(t, w, "target", nil)
	e := runOnce(t, w, cfg)
	runs, _ := e.StoredRuns(context.Background(), StoredRunsRequest{})
	runID := runs[0].RunID
	fetch := FetchRequest{RunID: runID, Artifact: "wireguard"}

	// The manifest's origin is the source's; the target's differs — refused
	// without the explicit flag.
	_, err := e.Apply(context.Background(), ApplyRunRequest{Fetch: fetch, Target: "target",
		Mode: source.ApplyInPlace, Confirm: "test://target/origin"})
	if err == nil || !strings.Contains(err.Error(), "AllowForeignOrigin") {
		t.Fatalf("foreign origin accepted silently: %v", err)
	}
	// Wrong confirm refused even with the flag.
	_, err = e.Apply(context.Background(), ApplyRunRequest{Fetch: fetch, Target: "target",
		Mode: source.ApplyInPlace, Confirm: "test://fake/source", AllowForeignOrigin: true})
	if err == nil || !strings.Contains(err.Error(), "Confirm") {
		t.Fatalf("wrong confirm accepted: %v", err)
	}
	if target.applied != 0 || target.probed != 0 {
		t.Fatal("a refused apply ran")
	}
	// The exact confirm and the flag: probe first, then one write.
	rep, err := e.Apply(context.Background(), ApplyRunRequest{Fetch: fetch, Target: "target",
		Mode: source.ApplyInPlace, Confirm: "test://target/origin", AllowForeignOrigin: true})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Bytes == 0 || target.probed != 1 || target.applied != 1 {
		t.Fatalf("probe/apply counts: probe=%d apply=%d report=%+v", target.probed, target.applied, rep)
	}
	if !bytes.Equal(target.gotBytes, w.sources["vpn"].data) {
		t.Fatal("the restorer received wrong bytes")
	}
}

func TestApplyInPlaceFailingProbeRefusesTheWrite(t *testing.T) {
	w := newWorld(t)
	target := &fakeRestorer{failProbe: errors.New("not a valid archive")}
	w.extraSources["target"] = target
	cfg := applyConfig(t, w, "target", nil)
	e := runOnce(t, w, cfg)
	runs, _ := e.StoredRuns(context.Background(), StoredRunsRequest{})
	_, err := e.Apply(context.Background(), ApplyRunRequest{
		Fetch:  FetchRequest{RunID: runs[0].RunID, Artifact: "wireguard"},
		Target: "target", Mode: source.ApplyInPlace, Confirm: "test://fake/source"})
	if err == nil || !strings.Contains(err.Error(), "probe") {
		t.Fatalf("failed probe did not refuse: %v", err)
	}
	if target.applied != 0 {
		t.Fatal("an apply ran after a failed probe")
	}
}

func TestApplySourceTypeMismatch(t *testing.T) {
	w := newWorld(t)
	target := &fakeRestorer{}
	w.extraSources["target"] = target
	w.registerType(t, "source", "other")
	cfg := applyConfig(t, w, "target", func(cfg map[string]any) {
		cfg["sources"].(map[string]any)["target"] = sourceInstance("other", "targets", "restore-target", []string{"zstd3"}, "required", "plain")
	})
	e := runOnce(t, w, cfg)
	runs, _ := e.StoredRuns(context.Background(), StoredRunsRequest{})
	_, err := e.Apply(context.Background(), ApplyRunRequest{
		Fetch:  FetchRequest{RunID: runs[0].RunID, Artifact: "wireguard"},
		Target: "target", Mode: source.ApplySideBySide})
	if err == nil || !strings.Contains(err.Error(), "type") {
		t.Fatalf("a source-type mismatch was accepted: %v", err)
	}
}

func TestApplyDiscoversTheTarget(t *testing.T) {
	w := newWorld(t)
	target := &fakeRestorer{}
	w.extraSources["found"] = target
	// The run happens without discovery; the restore config adds it, and the
	// target is found by the same rule and the same artifact name.
	cfg := applyConfig(t, w, "target", nil)
	e1 := runOnce(t, w, cfg)
	runs, _ := e1.StoredRuns(context.Background(), StoredRunsRequest{})

	cfg2 := applyConfig(t, w, "target", func(cfg map[string]any) {
		cfg["discoveries"] = map[string]any{"workloads": instance("fake")}
	})
	w.discoveries["workloads"] = &fakeDiscovery{found: []discovery.Found{{
		Name: "found", Type: "fake", Config: []byte(`{}`),
		Artifact: model.Artifact{Group: "vpn", Name: "wireguard", Importance: model.ImportanceRequired, Sensitivity: model.SensitivityPlain},
		Pipeline: []string{"zstd3"},
	}}}
	e2 := w.newEngine(t, cfg2)
	rep, err := e2.Apply(context.Background(), ApplyRunRequest{
		Fetch:  FetchRequest{RunID: runs[0].RunID, Artifact: "wireguard"},
		Target: "discovered", Mode: source.ApplySideBySide})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Target == "" || target.applied != 1 {
		t.Fatalf("discovered target not applied to: %+v", rep)
	}
}

// TestRestoreOnlyConfig: a config with no run section fetches fine — that is
// what the restore section exists for.
func TestRestoreOnlyConfig(t *testing.T) {
	w := newWorld(t)
	full := withRestore(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}), "primary")
	e := runOnce(t, w, full)
	runs, _ := e.StoredRuns(context.Background(), StoredRunsRequest{})
	runID := runs[0].RunID

	restoreOnly := map[string]any{
		"version": 1,
		"layout":  configLayout(),
		"restore": full["restore"],
		"storages": map[string]any{
			// No retention: a restore-only storage keeps no such section.
			"primary": map[string]any{
				"type":        "fake",
				"time_budget": "10s", "config": map[string]any{},
			},
		},
		"processors": map[string]any{"zstd3": instance("fake")},
	}
	e2 := w.newEngine(t, restoreOnly)
	var out bytes.Buffer
	rep, err := e2.Fetch(context.Background(), FetchRequest{RunID: runID, Artifact: "wireguard", Decode: true}, &out)
	if err != nil {
		t.Fatal(err)
	}
	if !rep.Verified || !bytes.Equal(out.Bytes(), w.sources["vpn"].data) {
		t.Fatalf("restore-only fetch: %+v", rep)
	}
}

func TestRestoreConfigValidation(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["restore"] = map[string]any{
		"spool_dir":           "",
		"spool_reserve_bytes": 0,
		"storage_order":       []string{"ghost"},
		"unclassified_error":  "sometimes",
		"retry":               map[string]any{"attempts": 0, "initial_delay": "1ms", "max_delay": "2ms", "jitter": 0},
	}
	raw, err := jsonMarshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	_, err = newEngineRaw(t, w, raw)
	if err == nil {
		t.Fatal("a broken restore section was accepted")
	}
	msg := err.Error()
	for _, want := range []string{"spool_dir", "storage_order", "ghost", "unclassified_error", "attempts"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error lacks %q:\n%s", want, msg)
		}
	}
}

// closingRestorer counts Close calls: the engine builds it per facade call and
// must close it when that call returns.
type closingRestorer struct {
	fakeRestorer
	mu     sync.Mutex
	closes int
}

func (c *closingRestorer) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closes++
	return nil
}

func (c *closingRestorer) closeCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closes
}

// TestApplyClosesTheTargetsItBuilds: a discovered target belongs to the call
// that resolved it. Several applies leave the Engine's lifetime closer list
// unchanged, every built target is closed exactly once, and a call that fails
// after the target was built — a refusal, never a nil Close — still closes it.
func TestApplyClosesTheTargetsItBuilds(t *testing.T) {
	w := newWorld(t)
	target := &closingRestorer{}
	w.extraSources["found"] = target
	cfg := applyConfig(t, w, "target", nil)
	e1 := runOnce(t, w, cfg)
	runs, err := e1.StoredRuns(context.Background(), StoredRunsRequest{})
	if err != nil || len(runs) != 1 {
		t.Fatalf("stored runs %v, %v", runs, err)
	}
	runID := runs[0].RunID

	cfg2 := applyConfig(t, w, "target", func(cfg map[string]any) {
		cfg["discoveries"] = map[string]any{"workloads": instance("fake")}
	})
	w.discoveries["workloads"] = &fakeDiscovery{found: []discovery.Found{{
		Name: "found", Type: "fake", Config: []byte(`{}`),
		Artifact: model.Artifact{Group: "vpn", Name: "wireguard", Importance: model.ImportanceRequired, Sensitivity: model.SensitivityPlain},
		Pipeline: []string{"zstd3"},
	}}}
	e2 := w.newEngine(t, cfg2)
	before := len(e2.closers)
	fetch := FetchRequest{RunID: runID, Artifact: "wireguard"}

	for i := 1; i <= 2; i++ {
		if _, err := e2.Apply(context.Background(), ApplyRunRequest{
			Fetch: fetch, Target: "discovered", Mode: source.ApplySideBySide,
		}); err != nil {
			t.Fatalf("apply %d: %v", i, err)
		}
		if got := target.closeCount(); got != i {
			t.Fatalf("after apply %d Close ran %d times, want once per call", i, got)
		}
	}

	// An in-place apply without the verbatim confirmation is refused after the
	// target has been built; the refusal must still close it.
	if _, err := e2.Apply(context.Background(), ApplyRunRequest{
		Fetch: fetch, Target: "discovered", Mode: source.ApplyInPlace,
	}); err == nil {
		t.Fatal("an in-place apply without Confirm was accepted")
	}
	if got := target.closeCount(); got != 3 {
		t.Fatalf("a refused apply left its target unclosed: Close ran %d times, want 3", got)
	}

	// The probed verify path resolves a target the same way.
	res, err := e2.Verify(context.Background(), VerifyRequest{Fetch: fetch, Depth: "probed", Target: "discovered"})
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 1 || !res[0].OK {
		t.Fatalf("verify result %+v", res)
	}
	if got := target.closeCount(); got != 4 {
		t.Fatalf("probed verify left its target unclosed: Close ran %d times, want 4", got)
	}

	if got := len(e2.closers); got != before {
		t.Fatalf("the engine's lifetime closer list grew by %d over the calls", got-before)
	}
}

// TestApplyStaysOnTheRunItValidated: the origin and type are checked against
// the run the plan picked; when that run's copy then fails verification, the
// apply refuses, naming the validated run — it never falls back to an older
// run whose recorded origin was checked by nobody.
func TestApplyStaysOnTheRunItValidated(t *testing.T) {
	w := newWorld(t)
	target := &fakeRestorer{}
	w.extraSources["target"] = target
	cfg := applyConfig(t, w, "target", nil)
	e := w.newEngine(t, cfg)

	w.sources["vpn"].traits.Origin = "test://vpn/older"
	repOld, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil || repOld.Outcome != model.OutcomeDelivered {
		t.Fatalf("older run: %+v, %v", repOld, err)
	}
	// Run ids have second resolution: let the next second start so the newer
	// run keeps its own manifest instead of overwriting the older one.
	time.Sleep(time.Until(repOld.StartedAt.Truncate(time.Second).Add(time.Second)) + 20*time.Millisecond)
	w.sources["vpn"].traits.Origin = "test://vpn/newest"
	repNew, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil || repNew.Outcome != model.OutcomeDelivered || repNew.RunID == repOld.RunID {
		t.Fatalf("newer run: %+v, %v", repNew, err)
	}
	// The target matches the NEWEST run's origin: the plan validates it.
	target.traits.Origin = "test://vpn/newest"

	// The newest run's object is corrupted in place — same length, different
	// bytes — so the storage listing still matches the manifest (the plan
	// picks the newest run) while the download's SHA-256 re-check fails. The
	// older run still holds a verified copy: the fallback a loose fetch would
	// take, on a run whose recorded origin differs from the target's and was
	// never validated.
	ar := artifactByName(repNew, "wireguard")
	if ar.Key == "" {
		t.Fatalf("the newer run records no key: %+v", ar)
	}
	f := w.storage(t, "primary")
	var body bytes.Buffer
	if err := f.Get(context.Background(), ar.Key, &body); err != nil {
		t.Fatal(err)
	}
	corrupted := append([]byte(nil), body.Bytes()...)
	if len(corrupted) == 0 {
		t.Fatal("the stored object is empty")
	}
	corrupted[0] ^= 0xff
	if _, err := f.Put(context.Background(), storage.PutRequest{
		Key: ar.Key, Size: int64(len(corrupted)), SHA256: model.SHA256Hex(corrupted),
	}, bytes.NewReader(corrupted)); err != nil {
		t.Fatal(err)
	}

	_, err = e.Apply(context.Background(), ApplyRunRequest{
		Fetch:  FetchRequest{Newest: true, Artifact: "wireguard"},
		Target: "target", Mode: source.ApplySideBySide,
	})
	if err == nil {
		t.Fatal("the apply fell back to the older run whose origin was never validated")
	}
	if !strings.Contains(err.Error(), repNew.RunID) {
		t.Fatalf("the refusal does not name the validated run %s: %v", repNew.RunID, err)
	}
	if target.applied != 0 {
		t.Fatalf("the target was applied to %d times", target.applied)
	}
}

// covStorage is a storage whose Stat and Get are scripted: the fetch paths that
// must see one specific listing answer, one specific byte stream, or a
// destination closed under them.
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
	if s.statErr != nil {
		return model.ObjectInfo{}, s.statErr
	}
	if s.statInfo != (model.ObjectInfo{}) {
		return s.statInfo, nil
	}
	return s.Fake.Stat(ctx, key)
}

func (s *covStorage) Get(ctx context.Context, key string, dst io.Writer) error {
	scripted := s.getData != nil || s.getErr != nil || s.closeW || s.onGet != nil
	if !scripted {
		return s.Fake.Get(ctx, key, dst)
	}
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
	}
	if s.closeW {
		if c, ok := dst.(io.Closer); ok {
			_ = c.Close()
		}
	}
	return nil
}

// hookStorage delegates to the in-memory fake and runs one hook after a
// successful Get — what proves a spool file the download left behind is what a
// later step reads.
type hookStorage struct {
	*storagetest.Fake
	onGet func()
}

func (h *hookStorage) Get(ctx context.Context, key string, dst io.Writer) error {
	if err := h.Fake.Get(ctx, key, dst); err != nil {
		return err
	}
	if h.onGet != nil {
		h.onGet()
	}
	return nil
}

// covProcessor is a processor whose Decode and DecodeChecker are scripted.
type covProcessor struct {
	decodeErr      error
	checkDecodeErr error
	// decodeAfter runs once the decoded bytes have been written: what lets a
	// test remove the spool file a later step reads.
	decodeAfter func()
}

func (p *covProcessor) Describe(context.Context) (processor.ProcessorTraits, error) {
	return processor.ProcessorTraits{Ext: ".zst"}, nil
}

func (p *covProcessor) Encode(_ context.Context, dst io.Writer, src io.Reader) error {
	_, err := io.Copy(dst, src)
	return err
}

func (p *covProcessor) Decode(_ context.Context, dst io.Writer, src io.Reader) error {
	if p.decodeErr != nil {
		return p.decodeErr
	}
	if _, err := io.Copy(dst, src); err != nil {
		return err
	}
	if p.decodeAfter != nil {
		p.decodeAfter()
	}
	return nil
}

func (p *covProcessor) CheckDecode(context.Context) error { return p.checkDecodeErr }

// errWriter refuses every write: the destination side of a fetch that fails.
type errWriter struct{}

func (errWriter) Write([]byte) (int, error) { return 0, errors.New("destination refused the write") }

// applyOnlySource is a target that can be applied to but never probed.
type applyOnlySource struct{ fakeSource }

func (a *applyOnlySource) Apply(context.Context, source.ApplyRequest, io.Reader) (source.ApplyReport, error) {
	return source.ApplyReport{Target: "apply-only"}, nil
}

// probeCloseRestorer closes the reader it is handed before answering: the
// probe-side bug a later seek must survive.
type probeCloseRestorer struct{ fakeRestorer }

func (p *probeCloseRestorer) Probe(_ context.Context, src io.Reader) (source.ProbeReport, error) {
	if c, ok := src.(io.Closer); ok {
		_ = c.Close()
	}
	return source.ProbeReport{Entries: 1, Bytes: 1}, nil
}

// errCloseRestorer is a discovered target whose Close fails.
type errCloseRestorer struct{ fakeRestorer }

func (e *errCloseRestorer) Close() error { return errors.New("target close failed") }

// --- helpers -----------------------------------------------------------------

func covManifest(runID, artifact, key string, data []byte) *model.RunReport {
	return &model.RunReport{
		Schema: model.ManifestSchema, RunID: runID, StartedAt: time.Unix(1700000000, 0).UTC(),
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

// covRemoveSpoolFiles is a hook that empties a spool directory: what models a
// file another step removed between the download and the read.
func covRemoveSpoolFiles(dir string) func() {
	return func() {
		entries, _ := os.ReadDir(dir)
		for _, en := range entries {
			_ = os.Remove(filepath.Join(dir, en.Name()))
		}
	}
}

// covSurvey builds a one-run survey.
func covSurvey(runID string, rep *model.RunReport, objects []model.ObjectInfo) survey {
	return survey{storage: "primary", ok: true, runs: map[string]*surveyRun{
		runID: {objects: objects, manifest: rep},
	}}
}

// covEngine builds the smallest engine that has the storages and the processor
// the fetch paths reach for, without a restore section unless asked.
func covEngine(t *testing.T, storages map[string]any) (*world, *Engine) {
	t.Helper()
	w := newWorld(t)
	e := w.newEngine(t, baseConfig(t, storages))
	return w, e
}

func covRestoreEngine(t *testing.T, storages map[string]any, order ...string) (*world, *Engine) {
	t.Helper()
	w := newWorld(t)
	e := w.newEngine(t, withRestore(t, baseConfig(t, storages), order...))
	return w, e
}

// covSpool is a fresh spool directory.
func covSpool(t *testing.T) (*spool.Spool, string) {
	t.Helper()
	dir := t.TempDir()
	sp, err := spool.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	return sp, dir
}

// covSpoolFile writes one file into a spool and returns its name.
func covSpoolFile(t *testing.T, sp *spool.Spool, data []byte) string {
	t.Helper()
	name := sp.NextName("src")
	w, err := sp.Create(name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return name
}

// --- pure functions ----------------------------------------------------------
