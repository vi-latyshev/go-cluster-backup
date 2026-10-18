package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/spool"
	"github.com/vi-latyshev/go-cluster-backup/internal/storage"
	"github.com/vi-latyshev/go-cluster-backup/internal/storage/storagetest"
)

func TestDecodeTo(t *testing.T) {
	_, e := covEngine(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	data := []byte("plaintext")
	ar := model.ArtifactResult{Key: "k", Chain: []model.ProcessorStep{{Instance: "zstd3", Type: "fake"}}}

	// decodeStages fails: the chain names an instance the config lacks.
	sp, _ := covSpool(t)
	file := covSpoolFile(t, sp, data)
	bad := ar
	bad.Chain = []model.ProcessorStep{{Instance: "ghost", Type: "fake"}}
	if err := e.decodeTo(context.Background(), sp, file, bad, io.Discard); err == nil ||
		!strings.Contains(err.Error(), "does not have") {
		t.Fatalf("decodeStages failure not propagated: %v", err)
	}

	// sp.Open fails: the spool file is gone.
	if err := e.decodeTo(context.Background(), sp, "missing", ar, io.Discard); err == nil {
		t.Fatal("decodeTo opened a missing spool file")
	}

	// The chain's Decode reports the artifact undecodable here.
	e.processors["zstd3"] = &covProcessor{decodeErr: fmt.Errorf("age: %w", errors.ErrUnsupported)}
	if err := e.decodeTo(context.Background(), sp, file, ar, io.Discard); err == nil ||
		!strings.Contains(err.Error(), "cannot decode the artifact") {
		t.Fatalf("unsupported decode not reported: %v", err)
	}

	// A generic decode failure names the artifact's key.
	e.processors["zstd3"] = &covProcessor{decodeErr: errors.New("corrupt stream")}
	if err := e.decodeTo(context.Background(), sp, file, ar, io.Discard); err == nil ||
		!strings.Contains(err.Error(), "decoding k") {
		t.Fatalf("decode failure not named: %v", err)
	}
}

// --- fetchToFile -------------------------------------------------------------

func TestFetchToFileRefusals(t *testing.T) {
	// No restore section.
	_, e := covEngine(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	if _, _, _, _, err := e.fetchToFile(context.Background(), FetchRequest{RunID: "r", Artifact: "a"}); err == nil ||
		!strings.Contains(err.Error(), "restore section") {
		t.Fatalf("no restore section: %v", err)
	}

	// A raw fetch asking for a decode is refused before anything is read.
	w, e2 := covRestoreEngine(t, map[string]any{"primary": storageInstance("fake", "keep_last")}, "primary")
	_ = w
	if _, _, _, _, err := e2.fetchToFile(context.Background(), FetchRequest{RawStorage: "primary", RawKey: "k", Decode: true}); err == nil ||
		!strings.Contains(err.Error(), "never decodes") {
		t.Fatalf("raw decode accepted: %v", err)
	}

	// An unusable spool directory.
	dir := e2.restoreSpoolDir
	e2.restoreSpoolDir = ""
	if _, _, _, _, err := e2.fetchToFile(context.Background(), FetchRequest{RunID: "r", Artifact: "a"}); err == nil {
		t.Fatal("an empty spool directory was accepted")
	}
	e2.restoreSpoolDir = dir

	// The reserve is larger than the free space.
	e2.restoreReserve = 1 << 62
	if _, _, _, _, err := e2.fetchToFile(context.Background(), FetchRequest{RunID: "r", Artifact: "a"}); err == nil ||
		!strings.Contains(err.Error(), "reserve") {
		t.Fatalf("reserve not enforced: %v", err)
	}
}

// --- fetchRaw ----------------------------------------------------------------

func TestFetchRaw(t *testing.T) {
	_, e := covEngine(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	sp, _ := covSpool(t)
	ctx := context.Background()

	// Unknown storage.
	if _, _, err := e.fetchRaw(ctx, FetchRequest{RawStorage: "ghost", RawKey: "k"}, sp); err == nil {
		t.Fatal("fetchRaw accepted an unknown storage")
	}

	// The spool directory is gone: Create fails permanently.
	gone, goneDir := covSpool(t)
	if err := os.RemoveAll(goneDir); err != nil {
		t.Fatal(err)
	}
	if _, _, err := e.fetchRaw(ctx, FetchRequest{RawStorage: "primary", RawKey: "k"}, gone); err == nil {
		t.Fatal("fetchRaw wrote into a missing spool directory")
	}

	// The storage's Get fails.
	e.storages[0].inst = &covStorage{Fake: storagetest.NewFake(), getErr: plugin.Transient(errors.New("connection reset"))}
	if _, _, err := e.fetchRaw(ctx, FetchRequest{RawStorage: "primary", RawKey: "k"}, sp); err == nil {
		t.Fatal("fetchRaw accepted a failing Get")
	}

	// The storage closes the spool writer under the engine: the close error is
	// what the attempt sees.
	e.storages[0].inst = &covStorage{Fake: storagetest.NewFake(), getData: []byte("x"), closeW: true}
	if _, _, err := e.fetchRaw(ctx, FetchRequest{RawStorage: "primary", RawKey: "k"}, sp); err == nil {
		t.Fatal("fetchRaw accepted a closed writer")
	}
}

// --- fetchArtifact -----------------------------------------------------------

func TestFetchArtifactRefusals(t *testing.T) {
	ctx := context.Background()

	// Unknown storage in the walk order.
	_, e := covRestoreEngine(t, map[string]any{"primary": storageInstance("fake", "keep_last")}, "primary")
	sp, _ := covSpool(t)
	if _, _, _, _, err := e.fetchArtifact(ctx, FetchRequest{RunID: "r", Artifact: "a", Storages: []string{"ghost"}}, sp); err == nil {
		t.Fatal("fetchArtifact accepted an unknown storage")
	}

	// No layout section.
	w := newWorld(t)
	noLayout := withRestore(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}), "primary")
	noLayout["layout"] = map[string]any{"run_prefix": "", "artifact_key": "a", "manifest_key": "manifest.json", "manual_id_suffix": "-manual"}
	e2 := w.newEngine(t, noLayout)
	if _, _, _, _, err := e2.fetchArtifact(ctx, FetchRequest{RunID: "r", Artifact: "a"}, sp); err == nil ||
		!strings.Contains(err.Error(), "layout") {
		t.Fatalf("no layout section: %v", err)
	}

	// A run prefix that is not a literal plus {{ .Run.ID }}.
	badPrefix := withRestore(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}), "primary")
	badPrefix["layout"] = map[string]any{
		"run_prefix": "{{ .Run.ID }}", "artifact_key": "a", "manifest_key": "manifest.json", "manual_id_suffix": "-manual",
	}
	e3 := w.newEngine(t, badPrefix)
	if _, _, _, _, err := e3.fetchArtifact(ctx, FetchRequest{RunID: "r", Artifact: "a"}, sp); err == nil ||
		!strings.Contains(err.Error(), "run_prefix") {
		t.Fatalf("bad run prefix: %v", err)
	}

	// No run holds the artifact.
	if _, _, _, _, err := e.fetchArtifact(ctx, FetchRequest{Newest: true, Artifact: "nope"}, sp); err == nil ||
		!strings.Contains(err.Error(), "no run on any storage") {
		t.Fatalf("no candidate run: %v", err)
	}

	// A run whose chain cannot be decoded is a hard error, not a skip.
	w2 := newWorld(t)
	cfg := withRestore(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}), "primary")
	e4 := runOnce(t, w2, cfg)
	runs, err := e4.StoredRuns(ctx, StoredRunsRequest{})
	if err != nil || len(runs) != 1 {
		t.Fatalf("stored runs %v, %v", runs, err)
	}
	e4.processors["zstd3"] = &covProcessor{decodeErr: errors.New("cannot decode")}
	if _, _, _, _, err := e4.fetchArtifact(ctx, FetchRequest{RunID: runs[0].RunID, Artifact: "wireguard", Decode: true}, sp); err == nil ||
		!strings.Contains(err.Error(), "decoding") {
		t.Fatalf("undecodable chain not a hard error: %v", err)
	}
}

// --- fetchCandidate ----------------------------------------------------------

func TestFetchCandidate(t *testing.T) {
	ctx := context.Background()
	data := []byte("artifact bytes")
	key := "runs/r/vpn/wireguard.dat.zst"
	rep := covManifest("r", "wireguard", key, data)

	newEngineAndBinding := func(t *testing.T, st storage.Storage) (*Engine, *storageBinding) {
		t.Helper()
		_, e := covEngine(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
		return e, &storageBinding{name: "primary", inst: st}
	}

	t.Run("survey failed", func(t *testing.T) {
		e, sb := newEngineAndBinding(t, storagetest.NewFake())
		sp, _ := covSpool(t)
		_, _, skip, err := e.fetchCandidate(ctx, sb, func(*storageBinding) (survey, bool) { return survey{}, false },
			"r", "wireguard", false, sp)
		if err != nil || skip == nil || !strings.Contains(skip.Reason, "survey failed") {
			t.Fatalf("skip %+v, err %v", skip, err)
		}
	})

	t.Run("artifact refused by the manifest", func(t *testing.T) {
		e, sb := newEngineAndBinding(t, storagetest.NewFake())
		sp, _ := covSpool(t)
		sur := covSurvey("r", failedManifest("r"), nil)
		_, _, skip, err := e.fetchCandidate(ctx, sb, func(*storageBinding) (survey, bool) { return sur, true },
			"r", "wireguard", false, sp)
		if err != nil || skip == nil || !strings.Contains(skip.Reason, "failed in run") {
			t.Fatalf("skip %+v, err %v", skip, err)
		}
	})

	t.Run("object not found", func(t *testing.T) {
		e, sb := newEngineAndBinding(t, &covStorage{Fake: storagetest.NewFake(), statErr: plugin.NotFound(errors.New("gone"))})
		sp, _ := covSpool(t)
		sur := covSurvey("r", rep, nil)
		_, _, skip, err := e.fetchCandidate(ctx, sb, func(*storageBinding) (survey, bool) { return sur, true },
			"r", "wireguard", false, sp)
		if err != nil || skip == nil || !strings.Contains(skip.Reason, "not found") {
			t.Fatalf("skip %+v, err %v", skip, err)
		}
	})

	t.Run("stat fails", func(t *testing.T) {
		e, sb := newEngineAndBinding(t, &covStorage{Fake: storagetest.NewFake(), statErr: plugin.Transient(errors.New("timeout"))})
		sp, _ := covSpool(t)
		sur := covSurvey("r", rep, nil)
		_, _, skip, err := e.fetchCandidate(ctx, sb, func(*storageBinding) (survey, bool) { return sur, true },
			"r", "wireguard", false, sp)
		if err != nil || skip == nil || !strings.Contains(skip.Reason, "stat:") {
			t.Fatalf("skip %+v, err %v", skip, err)
		}
	})

	t.Run("size mismatch before download", func(t *testing.T) {
		e, sb := newEngineAndBinding(t, &covStorage{Fake: storagetest.NewFake(), statInfo: model.ObjectInfo{Size: 1}})
		sp, _ := covSpool(t)
		sur := covSurvey("r", rep, nil)
		_, _, skip, err := e.fetchCandidate(ctx, sb, func(*storageBinding) (survey, bool) { return sur, true },
			"r", "wireguard", false, sp)
		if err != nil || skip == nil || !strings.Contains(skip.Reason, "manifest says") {
			t.Fatalf("skip %+v, err %v", skip, err)
		}
	})

	t.Run("create in a missing spool dir", func(t *testing.T) {
		e, sb := newEngineAndBinding(t, &covStorage{Fake: storagetest.NewFake(), statInfo: model.ObjectInfo{Size: int64(len(data))}})
		gone, dir := covSpool(t)
		if err := os.RemoveAll(dir); err != nil {
			t.Fatal(err)
		}
		sur := covSurvey("r", rep, nil)
		_, _, skip, err := e.fetchCandidate(ctx, sb, func(*storageBinding) (survey, bool) { return sur, true },
			"r", "wireguard", false, gone)
		if err != nil || skip == nil || !strings.Contains(skip.Reason, "download:") {
			t.Fatalf("skip %+v, err %v", skip, err)
		}
	})

	t.Run("get fails", func(t *testing.T) {
		e, sb := newEngineAndBinding(t, &covStorage{Fake: storagetest.NewFake(), statInfo: model.ObjectInfo{Size: int64(len(data))},
			getErr: plugin.Transient(errors.New("reset"))})
		sp, _ := covSpool(t)
		sur := covSurvey("r", rep, nil)
		_, _, skip, err := e.fetchCandidate(ctx, sb, func(*storageBinding) (survey, bool) { return sur, true },
			"r", "wireguard", false, sp)
		if err != nil || skip == nil || !strings.Contains(skip.Reason, "download:") {
			t.Fatalf("skip %+v, err %v", skip, err)
		}
	})

	t.Run("writer closed under the engine", func(t *testing.T) {
		e, sb := newEngineAndBinding(t, &covStorage{Fake: storagetest.NewFake(), statInfo: model.ObjectInfo{Size: int64(len(data))},
			getData: data, closeW: true})
		sp, _ := covSpool(t)
		sur := covSurvey("r", rep, nil)
		_, _, skip, err := e.fetchCandidate(ctx, sb, func(*storageBinding) (survey, bool) { return sur, true },
			"r", "wireguard", false, sp)
		if err != nil || skip == nil || !strings.Contains(skip.Reason, "download:") {
			t.Fatalf("skip %+v, err %v", skip, err)
		}
	})

	t.Run("downloaded size mismatch", func(t *testing.T) {
		e, sb := newEngineAndBinding(t, &covStorage{Fake: storagetest.NewFake(), statInfo: model.ObjectInfo{Size: int64(len(data))},
			getData: data[:1]})
		sp, _ := covSpool(t)
		sur := covSurvey("r", rep, nil)
		_, _, skip, err := e.fetchCandidate(ctx, sb, func(*storageBinding) (survey, bool) { return sur, true },
			"r", "wireguard", false, sp)
		if err != nil || skip == nil || !strings.Contains(skip.Reason, "downloaded") {
			t.Fatalf("skip %+v, err %v", skip, err)
		}
	})

	t.Run("sha256 mismatch", func(t *testing.T) {
		e, sb := newEngineAndBinding(t, &covStorage{Fake: storagetest.NewFake(), statInfo: model.ObjectInfo{Size: int64(len(data))},
			getData: bytes.Repeat([]byte{'z'}, len(data))})
		sp, _ := covSpool(t)
		sur := covSurvey("r", rep, nil)
		_, _, skip, err := e.fetchCandidate(ctx, sb, func(*storageBinding) (survey, bool) { return sur, true },
			"r", "wireguard", false, sp)
		if err != nil || skip == nil || !strings.Contains(skip.Reason, "sha256") {
			t.Fatalf("skip %+v, err %v", skip, err)
		}
	})

	t.Run("happy download", func(t *testing.T) {
		e, sb := newEngineAndBinding(t, &covStorage{Fake: storagetest.NewFake(), statInfo: model.ObjectInfo{Size: int64(len(data))},
			getData: data})
		sp, _ := covSpool(t)
		sur := covSurvey("r", rep, nil)
		name, ar, skip, err := e.fetchCandidate(ctx, sb, func(*storageBinding) (survey, bool) { return sur, true },
			"r", "wireguard", false, sp)
		if err != nil || skip != nil || name == "" || ar.Key != key {
			t.Fatalf("name %q, ar %+v, skip %+v, err %v", name, ar, skip, err)
		}
	})

	t.Run("decode fails on the chain", func(t *testing.T) {
		e, sb := newEngineAndBinding(t, &covStorage{Fake: storagetest.NewFake(), statInfo: model.ObjectInfo{Size: int64(len(data))},
			getData: data})
		sp, _ := covSpool(t)
		sur := covSurvey("r", rep, nil)
		e.processors["zstd3"] = &covProcessor{decodeErr: fmt.Errorf("sealed: %w", errors.ErrUnsupported)}
		_, _, _, err := e.fetchCandidate(ctx, sb, func(*storageBinding) (survey, bool) { return sur, true },
			"r", "wireguard", true, sp)
		if err == nil || !strings.Contains(err.Error(), "cannot decode the artifact") {
			t.Fatalf("unsupported decode not a hard error: %v", err)
		}
	})

	t.Run("decode fails generically", func(t *testing.T) {
		e, sb := newEngineAndBinding(t, &covStorage{Fake: storagetest.NewFake(), statInfo: model.ObjectInfo{Size: int64(len(data))},
			getData: data})
		sp, _ := covSpool(t)
		sur := covSurvey("r", rep, nil)
		e.processors["zstd3"] = &covProcessor{decodeErr: errors.New("corrupt")}
		_, _, _, err := e.fetchCandidate(ctx, sb, func(*storageBinding) (survey, bool) { return sur, true },
			"r", "wireguard", true, sp)
		if err == nil || !strings.Contains(err.Error(), "decoding") {
			t.Fatalf("decode failure not reported: %v", err)
		}
	})

	t.Run("decodeStages fails", func(t *testing.T) {
		e, sb := newEngineAndBinding(t, &covStorage{Fake: storagetest.NewFake(), statInfo: model.ObjectInfo{Size: int64(len(data))},
			getData: data})
		sp, _ := covSpool(t)
		badRep := covManifest("r", "wireguard", key, data)
		badRep.Artifacts[0].Chain = []model.ProcessorStep{{Instance: "ghost", Type: "fake"}}
		sur := covSurvey("r", badRep, nil)
		_, _, _, err := e.fetchCandidate(ctx, sb, func(*storageBinding) (survey, bool) { return sur, true },
			"r", "wireguard", true, sp)
		if err == nil || !strings.Contains(err.Error(), "does not have") {
			t.Fatalf("decodeStages failure not propagated: %v", err)
		}
	})

	t.Run("decoded spool file cannot be created", func(t *testing.T) {
		sp, dir := covSpool(t)
		hook := func() { _ = os.RemoveAll(dir) }
		e, sb := newEngineAndBinding(t, &covStorage{Fake: storagetest.NewFake(), statInfo: model.ObjectInfo{Size: int64(len(data))},
			getData: data, onGet: hook})
		sur := covSurvey("r", rep, nil)
		_, _, _, err := e.fetchCandidate(ctx, sb, func(*storageBinding) (survey, bool) { return sur, true },
			"r", "wireguard", true, sp)
		if err == nil {
			t.Fatal("a decode into a missing spool dir succeeded")
		}
	})

	t.Run("downloaded file cannot be reopened", func(t *testing.T) {
		sp, dir := covSpool(t)
		hook := func() {
			entries, _ := os.ReadDir(dir)
			for _, en := range entries {
				_ = os.Remove(filepath.Join(dir, en.Name()))
			}
		}
		e, sb := newEngineAndBinding(t, &covStorage{Fake: storagetest.NewFake(), statInfo: model.ObjectInfo{Size: int64(len(data))},
			getData: data, onGet: hook})
		sur := covSurvey("r", rep, nil)
		_, _, _, err := e.fetchCandidate(ctx, sb, func(*storageBinding) (survey, bool) { return sur, true },
			"r", "wireguard", true, sp)
		if err == nil {
			t.Fatal("a decode of a vanished spool file succeeded")
		}
	})
}

// --- Fetch -------------------------------------------------------------------

func TestFetchDestinationError(t *testing.T) {
	w := newWorld(t)
	cfg := withRestore(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}), "primary")
	e := runOnce(t, w, cfg)
	runs, _ := e.StoredRuns(context.Background(), StoredRunsRequest{})
	if _, err := e.Fetch(context.Background(), FetchRequest{RunID: runs[0].RunID, Artifact: "wireguard"}, errWriter{}); err == nil ||
		!strings.Contains(err.Error(), "destination") {
		t.Fatalf("a failing destination was accepted: %v", err)
	}
}

func TestFetchDecodeError(t *testing.T) {
	w := newWorld(t)
	cfg := withRestore(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}), "primary")
	e := runOnce(t, w, cfg)
	runs, _ := e.StoredRuns(context.Background(), StoredRunsRequest{})
	// The manifest's chain names a processor this engine does not have.
	delete(e.processors, "zstd3")
	var out bytes.Buffer
	if _, err := e.Fetch(context.Background(), FetchRequest{RunID: runs[0].RunID, Artifact: "wireguard", Decode: true}, &out); err == nil ||
		!strings.Contains(err.Error(), "does not have") {
		t.Fatalf("undecodable fetch accepted: %v", err)
	}
}

func TestFetchSpoolFileVanished(t *testing.T) {
	w := newWorld(t)
	cfg := withRestore(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}), "primary")
	e := runOnce(t, w, cfg)
	runs, _ := e.StoredRuns(context.Background(), StoredRunsRequest{})
	spoolDir := e.restoreSpoolDir
	e.storages[0].inst = &hookStorage{Fake: w.storage(t, "primary"), onGet: func() {
		entries, _ := os.ReadDir(spoolDir)
		for _, en := range entries {
			_ = os.Remove(filepath.Join(spoolDir, en.Name()))
		}
	}}
	var out bytes.Buffer
	if _, err := e.Fetch(context.Background(), FetchRequest{RunID: runs[0].RunID, Artifact: "wireguard"}, &out); err == nil {
		t.Fatal("a fetch whose spool file vanished succeeded")
	}
}

// --- ResolveFetch / resolveRaw ----------------------------------------------

// TestFetchToFileReportsAFreeBytesFailure: the spool's free space cannot be read
// — a statfs error no configuration can produce — so the fetch stops there
// rather than measuring a spool it does not have.
func TestFetchToFileReportsAFreeBytesFailure(t *testing.T) {
	orig := spoolFreeBytes
	spoolFreeBytes = func(*spool.Spool) (int64, error) { return 0, errors.New("statfs: no space left on device") }
	t.Cleanup(func() { spoolFreeBytes = orig })

	_, e := covRestoreEngine(t, map[string]any{"primary": storageInstance("fake", "keep_last")}, "primary")
	_, _, _, _, err := e.fetchToFile(context.Background(), FetchRequest{RunID: "r", Artifact: "a"})
	if err == nil || !strings.Contains(err.Error(), "no space left") {
		t.Fatalf("fetchToFile with an unreadable spool = %v, want the statfs error", err)
	}
}
