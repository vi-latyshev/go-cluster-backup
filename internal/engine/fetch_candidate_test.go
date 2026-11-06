package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/source"
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
	if _, _, _, _, err := e.fetchToFile(context.Background(), FetchRequest{RunID: "r", Artifact: "a"}, 0); err == nil ||
		!strings.Contains(err.Error(), "restore section") {
		t.Fatalf("no restore section: %v", err)
	}

	// A raw fetch asking for a decode is refused before anything is read.
	_, e2 := covRestoreEngine(t, map[string]any{"primary": storageInstance("fake", "keep_last")}, "primary")
	if _, _, _, _, err := e2.fetchToFile(context.Background(), FetchRequest{RawStorage: "primary", RawKey: "k", Decode: true}, 0); err == nil ||
		!strings.Contains(err.Error(), "never decodes") {
		t.Fatalf("raw decode accepted: %v", err)
	}

	// An unusable spool directory.
	dir := e2.restoreSpoolDir
	e2.restoreSpoolDir = ""
	if _, _, _, _, err := e2.fetchToFile(context.Background(), FetchRequest{RunID: "r", Artifact: "a"}, 0); err == nil {
		t.Fatal("an empty spool directory was accepted")
	}
	e2.restoreSpoolDir = dir

	// The reserve is larger than the free space.
	e2.restoreReserve = 1 << 62
	if _, _, _, _, err := e2.fetchToFile(context.Background(), FetchRequest{RunID: "r", Artifact: "a"}, 0); err == nil ||
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

	// A space refusal during the raw write is the guard's own verdict, whether
	// the storage reports the write error, swallows it, or closes the sink.
	e.restoreReserve = 1 << 62
	e.storages[0].inst = &covStorage{Fake: storagetest.NewFake(), getData: []byte("x")}
	if _, _, err := e.fetchRaw(ctx, FetchRequest{RawStorage: "primary", RawKey: "k"}, sp); err == nil ||
		!strings.Contains(err.Error(), "reserve") {
		t.Fatalf("a raw space refusal was not reported: %v", err)
	}
	e.storages[0].inst = &carelessStorage{Fake: storagetest.NewFake()}
	if _, _, err := e.fetchRaw(ctx, FetchRequest{RawStorage: "primary", RawKey: "k"}, sp); err == nil ||
		!strings.Contains(err.Error(), "reserve") {
		t.Fatalf("a swallowed raw space refusal was not reported: %v", err)
	}
	e.storages[0].inst = &carelessCloseStorage{Fake: storagetest.NewFake()}
	if _, _, err := e.fetchRaw(ctx, FetchRequest{RawStorage: "primary", RawKey: "k"}, sp); err == nil ||
		!strings.Contains(err.Error(), "reserve") {
		t.Fatalf("a closed raw sink lost its space verdict: %v", err)
	}
}

// carelessStorage ignores the write error of the guard it was handed.
type carelessStorage struct{ *storagetest.Fake }

func (s *carelessStorage) Get(_ context.Context, _ string, dst io.Writer) error {
	_, _ = dst.Write([]byte("x"))
	return nil
}

// carelessCloseStorage ignores the write error and closes the sink anyway.
type carelessCloseStorage struct{ *storagetest.Fake }

func (s *carelessCloseStorage) Get(_ context.Context, _ string, dst io.Writer) error {
	_, _ = dst.Write([]byte("x"))
	if c, ok := dst.(io.Closer); ok {
		_ = c.Close()
	}
	return nil
}

// --- fetchArtifact -----------------------------------------------------------

func TestFetchArtifactRefusals(t *testing.T) {
	ctx := context.Background()

	// Unknown storage in the walk order.
	_, e := covRestoreEngine(t, map[string]any{"primary": storageInstance("fake", "keep_last")}, "primary")
	sp, _ := covSpool(t)
	if _, _, _, _, err := e.fetchArtifact(ctx, FetchRequest{RunID: "r", Artifact: "a", Storages: []string{"ghost"}}, sp, 0); err == nil {
		t.Fatal("fetchArtifact accepted an unknown storage")
	}

	// No layout section.
	w := newWorld(t)
	noLayout := withRestore(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}), "primary")
	noLayout["layout"] = map[string]any{"run_prefix": "", "artifact_key": "a", "manifest_key": "manifest.json", "manual_id_suffix": "-manual"}
	e2 := w.newEngine(t, noLayout)
	if _, _, _, _, err := e2.fetchArtifact(ctx, FetchRequest{RunID: "r", Artifact: "a"}, sp, 0); err == nil ||
		!strings.Contains(err.Error(), "layout") {
		t.Fatalf("no layout section: %v", err)
	}

	// A run prefix that is not a literal plus {{ .Run.ID }}.
	badPrefix := withRestore(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}), "primary")
	badPrefix["layout"] = map[string]any{
		"run_prefix": "{{ .Run.ID }}", "artifact_key": "a", "manifest_key": "manifest.json", "manual_id_suffix": "-manual",
	}
	e3 := w.newEngine(t, badPrefix)
	if _, _, _, _, err := e3.fetchArtifact(ctx, FetchRequest{RunID: "r", Artifact: "a"}, sp, 0); err == nil ||
		!strings.Contains(err.Error(), "run_prefix") {
		t.Fatalf("bad run prefix: %v", err)
	}

	// No run holds the artifact.
	if _, _, _, _, err := e.fetchArtifact(ctx, FetchRequest{Newest: true, Artifact: "nope"}, sp, 0); err == nil ||
		!strings.Contains(err.Error(), "no run on any storage") {
		t.Fatalf("no candidate run: %v", err)
	}
}

// --- walkDownload ------------------------------------------------------------

// TestWalkDownloadPreflight covers the walk's refusals before a byte moves.
func TestWalkDownloadPreflight(t *testing.T) {
	ctx := context.Background()
	data := []byte("artifact bytes")
	key := "runs/r/vpn/wireguard.dat.zst"
	rep := covManifest("r", "wireguard", key, data)
	req := FetchRequest{RunID: "r", Artifact: "wireguard"}
	takeOK := func(_ context.Context, _ *storageBinding, ar *model.ArtifactResult) (copyResult, *StorageSkip, error) {
		return copyResult{ar: *ar}, nil, nil
	}

	cases := []struct {
		name      string
		survey    func() (survey, bool)
		storage   storage.Storage
		want      string
		wantSkips int
	}{
		{
			name:      "survey failed",
			survey:    func() (survey, bool) { return survey{}, false },
			storage:   storagetest.NewFake(),
			want:      "survey failed",
			wantSkips: 1,
		},
		{
			name:      "artifact refused by the manifest",
			survey:    func() (survey, bool) { return covSurvey("r", failedManifest("r"), nil), true },
			storage:   storagetest.NewFake(),
			want:      "failed in run",
			wantSkips: 1,
		},
		{
			name:      "object not found",
			survey:    func() (survey, bool) { return covSurvey("r", rep, nil), true },
			storage:   &covStorage{Fake: storagetest.NewFake(), statErr: plugin.NotFound(errors.New("gone"))},
			want:      "not found",
			wantSkips: 1,
		},
		{
			name:      "stat fails",
			survey:    func() (survey, bool) { return covSurvey("r", rep, nil), true },
			storage:   &covStorage{Fake: storagetest.NewFake(), statErr: plugin.Transient(errors.New("timeout"))},
			want:      "stat:",
			wantSkips: 1,
		},
		{
			name:      "size mismatch before download",
			survey:    func() (survey, bool) { return covSurvey("r", rep, nil), true },
			storage:   &covStorage{Fake: storagetest.NewFake(), statInfo: model.ObjectInfo{Size: 1}},
			want:      "manifest says",
			wantSkips: 1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, e := covEngine(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
			sb := &storageBinding{name: "primary", inst: tc.storage}
			_, _, _, skips, err := walkDownload(ctx, e, req, []*storageBinding{sb},
				func(*storageBinding) (survey, bool) { return tc.survey() }, takeOK)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %v, want %q", err, tc.want)
			}
			if len(skips) != tc.wantSkips {
				t.Fatalf("skips %+v", skips)
			}
		})
	}

	// A take that skips, and a take that fails hard.
	_, e := covEngine(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	okStorage := &covStorage{Fake: storagetest.NewFake(), statInfo: model.ObjectInfo{Size: int64(len(data))}}
	sb := &storageBinding{name: "primary", inst: okStorage}
	getSurvey := func(*storageBinding) (survey, bool) { return covSurvey("r", rep, nil), true }
	_, _, _, _, err := walkDownload(ctx, e, req, []*storageBinding{sb}, getSurvey,
		func(context.Context, *storageBinding, *model.ArtifactResult) (copyResult, *StorageSkip, error) {
			return copyResult{}, &StorageSkip{Storage: "primary", Reason: "passed over"}, nil
		})
	if err == nil || !strings.Contains(err.Error(), "passed over") {
		t.Fatalf("skip not reported: %v", err)
	}
	hard := errors.New("hard stop")
	if _, _, _, _, err := walkDownload(ctx, e, req, []*storageBinding{sb}, getSurvey,
		func(context.Context, *storageBinding, *model.ArtifactResult) (copyResult, *StorageSkip, error) {
			return copyResult{}, nil, hard
		}); !errors.Is(err, hard) {
		t.Fatalf("hard error not propagated: %v", err)
	}
}

// TestWalkDownloadNewestFallsThrough: a newest run whose copy is skipped is
// passed over for an older one, and the walk names the skip.
func TestWalkDownloadNewestFallsThrough(t *testing.T) {
	ctx := context.Background()
	_, e := covEngine(t, map[string]any{"primary": storageInstance("fake", "keep_last")})

	oldRep := covManifest("r-old", "wireguard", "old", []byte("x"))
	oldRep.StartedAt = time.Unix(1600000000, 0).UTC()
	sur := survey{ok: true, runs: map[string]*surveyRun{
		"r-new": {manifest: covManifest("r-new", "wireguard", "new", []byte("x"))},
		"r-old": {manifest: oldRep},
	}}
	sb := &storageBinding{name: "primary", inst: &covStorage{Fake: storagetest.NewFake(), statInfo: model.ObjectInfo{Size: 1}}}
	runID, _, _, _, err := walkDownload(ctx, e, FetchRequest{Newest: true, Artifact: "wireguard"},
		[]*storageBinding{sb},
		func(*storageBinding) (survey, bool) { return sur, true },
		func(context.Context, *storageBinding, *model.ArtifactResult) (copyResult, *StorageSkip, error) {
			return copyResult{}, &StorageSkip{Storage: "primary", Reason: "corrupt"}, nil
		})
	if err == nil || !strings.Contains(err.Error(), "corrupt") || runID != "r-new" {
		t.Fatalf("walk = %q, %v", runID, err)
	}
}

// --- downloadSpool / spoolCopy ----------------------------------------------

func TestDownloadSpool(t *testing.T) {
	ctx := context.Background()
	data := []byte("artifact bytes")
	key := "runs/r/vpn/wireguard.dat.zst"
	rep := covManifest("r", "wireguard", key, data)

	newEngine := func(t *testing.T, st storage.Storage) (*Engine, *storageBinding) {
		t.Helper()
		_, e := covRestoreEngine(t, map[string]any{"primary": storageInstance("fake", "keep_last")}, "primary")
		return e, &storageBinding{name: "primary", inst: st}
	}

	t.Run("missing spool dir", func(t *testing.T) {
		sp, dir := covSpool(t)
		if err := os.RemoveAll(dir); err != nil {
			t.Fatal(err)
		}
		_, err := spoolCopy(ctx, storagetest.NewFake(), &rep.Artifacts[0], sp, 0)
		if ce, ok := errors.AsType[*copyError](err); !ok || !ce.skip || !strings.Contains(ce.reason, "download:") {
			t.Fatalf("a missing spool dir = %v", err)
		}
	})

	t.Run("get fails", func(t *testing.T) {
		e, sb := newEngine(t, &covStorage{Fake: storagetest.NewFake(), getErr: plugin.Transient(errors.New("reset"))})
		sp, _ := covSpool(t)
		_, skip, err := e.downloadSpool(ctx, sb, &rep.Artifacts[0], sp, 0)
		if err != nil || skip == nil || !strings.Contains(skip.Reason, "download:") {
			t.Fatalf("skip %+v, err %v", skip, err)
		}
	})

	t.Run("writer closed under the engine", func(t *testing.T) {
		e, sb := newEngine(t, &covStorage{Fake: storagetest.NewFake(), getData: data, closeW: true})
		sp, _ := covSpool(t)
		_, skip, err := e.downloadSpool(ctx, sb, &rep.Artifacts[0], sp, 0)
		if err != nil || skip == nil || !strings.Contains(skip.Reason, "download:") {
			t.Fatalf("skip %+v, err %v", skip, err)
		}
	})

	t.Run("short download", func(t *testing.T) {
		e, sb := newEngine(t, &covStorage{Fake: storagetest.NewFake(), getData: data[:1]})
		sp, _ := covSpool(t)
		_, skip, err := e.downloadSpool(ctx, sb, &rep.Artifacts[0], sp, 0)
		if err != nil || skip == nil || !strings.Contains(skip.Reason, "downloaded") {
			t.Fatalf("skip %+v, err %v", skip, err)
		}
	})

	t.Run("sha256 mismatch", func(t *testing.T) {
		e, sb := newEngine(t, &covStorage{Fake: storagetest.NewFake(), getData: bytes.Repeat([]byte{'z'}, len(data))})
		sp, _ := covSpool(t)
		_, skip, err := e.downloadSpool(ctx, sb, &rep.Artifacts[0], sp, 0)
		if err != nil || skip == nil || !strings.Contains(skip.Reason, "sha256") {
			t.Fatalf("skip %+v, err %v", skip, err)
		}
	})

	t.Run("long download stops at the manifest size", func(t *testing.T) {
		long := append(append([]byte(nil), data...), 'x')
		gets := 0
		store := &covStorage{Fake: storagetest.NewFake(), getData: long, onGet: func() { gets++ }}
		e, sb := newEngine(t, store)
		sp, _ := covSpool(t)
		_, skip, err := e.downloadSpool(ctx, sb, &rep.Artifacts[0], sp, 0)
		if err != nil || skip == nil || !strings.Contains(skip.Reason, "downloaded") {
			t.Fatalf("skip %+v, err %v", skip, err)
		}
		if gets != 1 {
			t.Fatalf("an overflow was downloaded %d times, want one attempt", gets)
		}
	})

	t.Run("happy download", func(t *testing.T) {
		e, sb := newEngine(t, &covStorage{Fake: storagetest.NewFake(), getData: data})
		sp, _ := covSpool(t)
		name, skip, err := e.downloadSpool(ctx, sb, &rep.Artifacts[0], sp, 0)
		if err != nil || skip != nil || name == "" {
			t.Fatalf("name %q, skip %+v, err %v", name, skip, err)
		}
	})
}

// TestDownloadSpoolReserve: the space a copy needs is checked before its first
// Get, and a refusal is hard — it never falls through to another storage or an
// older run.
func TestDownloadSpoolReserve(t *testing.T) {
	ctx := context.Background()
	data := []byte("artifact bytes")
	rep := covManifest("r", "wireguard", "runs/r/vpn/wireguard.dat.zst", data)
	store := &covStorage{Fake: storagetest.NewFake(), getData: data}

	orig := spoolFreeBytes
	t.Cleanup(func() { spoolFreeBytes = orig })
	calls := 0
	spoolFreeBytes = func(*spool.Spool) (int64, error) {
		calls++
		return int64(len(data)) - 1, nil
	}
	_, e := covRestoreEngine(t, map[string]any{"primary": storageInstance("fake", "keep_last")}, "primary")
	sb := &storageBinding{name: "primary", inst: store}
	_, skip, err := e.downloadSpool(ctx, sb, &rep.Artifacts[0], mustSpool(t, e), 0)
	if skip != nil || err == nil || !strings.Contains(err.Error(), "need") {
		t.Fatalf("skip %+v, err %v", skip, err)
	}
	if calls != 1 {
		t.Fatalf("statfs ran %d times before the refusal", calls)
	}
	if n := store.CallCount(storagetest.OpGet, rep.Artifacts[0].Key); n != 0 {
		t.Fatalf("a space refusal ran %d Gets", n)
	}

	// A statfs that cannot answer is hard, never a skip.
	spoolFreeBytes = func(*spool.Spool) (int64, error) { return 0, errors.New("statfs: io error") }
	if _, _, err := e.downloadSpool(ctx, sb, &rep.Artifacts[0], mustSpool(t, e), 0); err == nil ||
		!strings.Contains(err.Error(), "statfs") {
		t.Fatalf("an unreadable spool was accepted: %v", err)
	}

	// The pre-check passes but the guard refuses during the write: the copy is
	// hard, and the refusal carries the guard's numbers.
	calls = 0
	spoolFreeBytes = func(*spool.Spool) (int64, error) {
		calls++
		if calls == 1 {
			return 1 << 30, nil
		}
		return 0, nil
	}
	e.restoreReserve = 0
	if _, _, err := e.downloadSpool(ctx, sb, &rep.Artifacts[0], mustSpool(t, e), 0); err == nil ||
		!strings.Contains(err.Error(), "restore spool") {
		t.Fatalf("a guard refusal during the write was not hard: %v", err)
	}
	if entries, err := readDirNames(e.restoreSpoolDir); err != nil || len(entries) != 0 {
		t.Fatalf("a refused copy left %v, %v", entries, err)
	}
}

func mustSpool(t *testing.T, e *Engine) *spool.Spool {
	t.Helper()
	sp, err := spool.New(e.restoreSpoolDir)
	if err != nil {
		t.Fatal(err)
	}
	return sp
}

// --- spaceGuard / meter ------------------------------------------------------

// scriptWriter is the file underneath a guard: it answers a scripted Write and
// Close error.
type scriptWriter struct {
	writeErr error
	closeErr error
	writes   int
}

func (s *scriptWriter) Write(p []byte) (int, error) {
	s.writes++
	if s.writeErr != nil {
		return 0, s.writeErr
	}
	return len(p), nil
}

func (s *scriptWriter) Close() error { return s.closeErr }

// TestSpaceGuard covers the guard directly: its own share of the decision, its
// measurement schedule and the errors it keeps.
func TestSpaceGuard(t *testing.T) {
	sp, _ := covSpool(t)
	orig := spoolFreeBytes
	t.Cleanup(func() { spoolFreeBytes = orig })

	// A statfs failure is terminal and permanent: no retry, no skip.
	spoolFreeBytes = func(*spool.Spool) (int64, error) { return 0, errors.New("statfs: io error") }
	g := &spaceGuard{w: &scriptWriter{}, sp: sp, reserve: 0}
	if _, err := g.Write([]byte("x")); err == nil || g.err == nil ||
		!strings.Contains(err.Error(), "statfs") {
		t.Fatalf("statfs failure not reported: %v", err)
	}
	// Once terminal, every later write says the same thing.
	if _, err := g.Write([]byte("x")); !errors.Is(err, g.err) {
		t.Fatalf("a refused guard wrote again: %v", err)
	}

	// Less headroom than the write needs: a refusal naming all the numbers.
	spoolFreeBytes = func(*spool.Spool) (int64, error) { return 3, nil }
	g = &spaceGuard{w: &scriptWriter{}, sp: sp, reserve: 2}
	_, err := g.Write([]byte("xy"))
	if err == nil || !strings.Contains(err.Error(), "free 3 bytes, need 2, reserve 2") {
		t.Fatalf("headroom refusal %v", err)
	}

	// Headroom measured once, then only after half of it is spent.
	calls := 0
	spoolFreeBytes = func(*spool.Spool) (int64, error) { calls++; return 100, nil }
	w := &scriptWriter{}
	g = &spaceGuard{w: w, sp: sp, reserve: 0}
	for range 2 {
		if _, err := g.Write([]byte("0123456789")); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 1 {
		t.Fatalf("statfs ran %d times for two small writes, want one measurement", calls)
	}
	if g.budget != 30 {
		t.Fatalf("budget after two writes = %d, want 30", g.budget)
	}

	// A filesystem shortage from the write and from the close is one space
	// verdict; any other write error keeps its own class.
	spoolFreeBytes = func(*spool.Spool) (int64, error) { return 100, nil }
	g = &spaceGuard{w: &scriptWriter{writeErr: syscall.ENOSPC}, sp: sp}
	if _, err := g.Write([]byte("x")); err == nil || g.err == nil {
		t.Fatal("ENOSPC not recorded")
	}
	g = &spaceGuard{w: &scriptWriter{writeErr: syscall.EDQUOT}, sp: sp}
	if _, err := g.Write([]byte("x")); err == nil || g.err == nil {
		t.Fatal("EDQUOT not recorded")
	}
	g = &spaceGuard{w: &scriptWriter{writeErr: errors.New("io error")}, sp: sp}
	_, err = g.Write([]byte("x"))
	if err == nil || g.err != nil || !strings.Contains(err.Error(), "io error") {
		t.Fatalf("a plain write error was misread: %v, guard %v", err, g.err)
	}
	g = &spaceGuard{w: &scriptWriter{closeErr: syscall.ENOSPC}, sp: sp}
	_ = g.Close()
	if g.err == nil {
		t.Fatal("a close-time ENOSPC was not a space verdict")
	}
	g = &spaceGuard{w: &scriptWriter{}, sp: sp}
	if err := g.Close(); err != nil {
		t.Fatalf("clean close: %v", err)
	}

	// A short headroom still allows the write it measured, and the space error
	// names its cause.
	spoolFreeBytes = func(*spool.Spool) (int64, error) { return 100, nil }
	g = &spaceGuard{w: &scriptWriter{}, sp: sp}
	if _, err := g.Write(bytes.Repeat([]byte("x"), 60)); err != nil {
		t.Fatalf("a write the headroom allows: %v", err)
	}
	if g.budget != 0 {
		t.Fatalf("budget after a clamped write = %d", g.budget)
	}
	spoolFreeBytes = func(*spool.Spool) (int64, error) { return 100, nil }
	g = &spaceGuard{w: &scriptWriter{writeErr: syscall.ENOSPC}, sp: sp}
	_, err = g.Write([]byte("x"))
	if err == nil || !strings.Contains(err.Error(), "no space left on device") {
		t.Fatalf("a space error lost its cause: %v", err)
	}
}

// TestMeter covers the size cap: a byte past the manifest is refused before any
// sink sees it, and the stream's own numbers are kept.
func TestMeter(t *testing.T) {
	var out bytes.Buffer
	m := &meter{w: &out, hash: sha256.New(), limit: 4}
	if _, err := m.Write([]byte("abcd")); err != nil || out.Len() != 4 {
		t.Fatalf("write %v, %q", err, out.Bytes())
	}
	if _, err := m.Write([]byte("e")); !errors.Is(err, errOverflow) || !m.overflow || m.over != 5 {
		t.Fatalf("overflow not raised: %v, %+v", err, m)
	}
	if _, err := m.Write([]byte("e")); !errors.Is(err, errOverflow) {
		t.Fatalf("a crossing meter kept writing: %v", err)
	}
	if m.sum() != model.SHA256Hex([]byte("abcd")) {
		t.Fatalf("meter hash %q", m.sum())
	}
	// A sink that is not closable has nothing to close.
	if err := (&meter{w: &bytes.Buffer{}, hash: sha256.New()}).Close(); err != nil {
		t.Fatalf("meter over a plain writer: %v", err)
	}
}

// --- streamCopy / downloadStream --------------------------------------------

// oneByteProber reads one byte and answers with a scripted error.
type oneByteProber struct {
	err      error
	attempts int
}

func (p *oneByteProber) probe(ctx context.Context, src io.Reader) (source.ProbeReport, error) {
	p.attempts++
	var b [1]byte
	n, err := src.Read(b[:])
	if err != nil && !errors.Is(err, io.EOF) {
		return source.ProbeReport{}, err
	}
	if p.attempts == 1 {
		return source.ProbeReport{Bytes: int64(n)}, p.err
	}
	return source.ProbeReport{Bytes: int64(n)}, nil
}

// tailErrProcessor copies its input and then fails, so a consumer that stops
// early still leaves a decode error on the tail.
type tailErrProcessor struct {
	covProcessor
	tail error
}

func (p *tailErrProcessor) Decode(_ context.Context, dst io.Writer, src io.Reader) error {
	if _, err := io.Copy(dst, src); err != nil {
		return err
	}
	return p.tail
}

// fmtWriteProcessor reports a failed write as a formatted string, losing both
// the class and the error chain — the processor bug the drain has to absorb.
type fmtWriteProcessor struct{ covProcessor }

func (p *fmtWriteProcessor) Decode(_ context.Context, dst io.Writer, src io.Reader) error {
	buf := make([]byte, 8)
	for {
		n, rerr := src.Read(buf)
		if n > 0 {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				// Deliberately drops the class and the chain: the processor bug
				// the drain has to make unreachable.
				return errors.New(werr.Error())
			}
		}
		if rerr == io.EOF {
			return nil
		}
		if rerr != nil {
			return rerr
		}
	}
}

// TestStreamCopy covers one attempt of the streaming path: the download's
// verdict, the decoder's, and the consumer's are told apart in order.
func TestStreamCopy(t *testing.T) {
	ctx := context.Background()
	data := []byte("artifact bytes")
	key := "runs/r/vpn/wireguard.dat.zst"
	rep := covManifest("r", "wireguard", key, data)
	ar := rep.Artifacts[0]
	ar.Chain = nil

	newEngine := func(t *testing.T, st storage.Storage) *Engine {
		t.Helper()
		_, e := covRestoreEngine(t, map[string]any{"primary": storageInstance("fake", "keep_last")}, "primary")
		e.storages[0].inst = st
		return e
	}

	// A good stream.
	e := newEngine(t, &covStorage{Fake: storagetest.NewFake(), getData: data})
	probe, err := streamCopy(ctx, readOnlyStorage{e.storages[0].inst}, &ar, nil, discardProbe)
	if err != nil || probe.Bytes != int64(len(data)) {
		t.Fatalf("good stream: %+v, %v", probe, err)
	}

	// A consumer's own error is retried, never mistaken for a decode failure.
	prober := &oneByteProber{err: plugin.Transient(errors.New("probe hiccup"))}
	_, err = streamCopy(ctx, readOnlyStorage{e.storages[0].inst}, &ar, nil, prober.probe)
	if ce, ok := errors.AsType[*copyError](err); !ok || ce.skip {
		t.Fatalf("consumer error verdict %v", err)
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassTransient {
		t.Fatalf("consumer error class %q", class)
	}

	// A consumer that refuses with ErrUnsupported stops the download at once.
	unsupported := func(context.Context, io.Reader) (source.ProbeReport, error) {
		return source.ProbeReport{}, errors.ErrUnsupported
	}
	blocking := &blockingGetStorage{Fake: storagetest.NewFake()}
	e = newEngine(t, blocking)
	_, err = streamCopy(ctx, readOnlyStorage{e.storages[0].inst}, &ar, nil, unsupported)
	if ce, ok := errors.AsType[*copyError](err); !ok || ce.skip {
		t.Fatalf("unsupported verdict %v", err)
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassPermanent {
		t.Fatalf("unsupported class %q", class)
	}
	if blocking.gets != 1 {
		t.Fatalf("an unsupported consumer was retried %d times, want one attempt", blocking.gets-1)
	}

	// A consumer that reads one byte and answers OK must not hide a decode
	// failure on the tail: the stream's verdict is still not ok.
	e = newEngine(t, &covStorage{Fake: storagetest.NewFake(), getData: data})
	tail := &tailErrProcessor{tail: errors.New("tail boom")}
	_, err = streamCopy(ctx, readOnlyStorage{e.storages[0].inst}, &ar,
		[]func(context.Context, io.Writer, io.Reader) error{tail.Decode}, (&oneByteProber{}).probe)
	if err == nil || !strings.Contains(err.Error(), "decoding") {
		t.Fatalf("a one-byte consumer hid the tail error: %v", err)
	}

	// A consumer that panics is one permanent error, not a hang.
	panicking := func(context.Context, io.Reader) (source.ProbeReport, error) { panic("probe panic") }
	e = newEngine(t, &covStorage{Fake: storagetest.NewFake(), getData: data})
	_, err = streamCopy(ctx, readOnlyStorage{e.storages[0].inst}, &ar, nil, panicking)
	if ce, ok := errors.AsType[*copyError](err); !ok || ce.skip || !strings.Contains(err.Error(), "plugin panic") {
		t.Fatalf("a panicking consumer = %v", err)
	}

	// A processor that formats its own write error with %v never gets one while
	// the engine drains after an early consumer: the consumer's transient error
	// is what the retry follows, never a fabricated decode failure.
	e = newEngine(t, &covStorage{Fake: storagetest.NewFake(), getData: data})
	fw := &fmtWriteProcessor{}
	prober = &oneByteProber{err: plugin.Transient(errors.New("probe hiccup"))}
	_, err = streamCopy(ctx, readOnlyStorage{e.storages[0].inst}, &ar,
		[]func(context.Context, io.Writer, io.Reader) error{fw.Decode}, prober.probe)
	if class, _ := plugin.ClassOf(err); class != plugin.ClassTransient {
		t.Fatalf("the consumer's transient error became class %q: %v", class, err)
	}
	if strings.Contains(err.Error(), "decoding") {
		t.Fatalf("a processor's write error was mistaken for a decode failure: %v", err)
	}

	// A decoder that fails before reading its input stops the download; the
	// bytes the download still delivers are accounted for and discarded.
	big := bytes.Repeat([]byte("x"), 1<<20)
	bigAr := covManifest("r", "wireguard", key, big).Artifacts[0]
	bigAr.Chain = nil
	e = newEngine(t, &covStorage{Fake: storagetest.NewFake(), getData: big})
	stages := []func(context.Context, io.Writer, io.Reader) error{
		func(context.Context, io.Writer, io.Reader) error { return errors.New("decode boom") },
	}
	if _, err := streamCopy(ctx, readOnlyStorage{e.storages[0].inst}, &bigAr, stages, discardProbe); err == nil ||
		!strings.Contains(err.Error(), "decoding") {
		t.Fatalf("a decoder that stopped early: %v", err)
	}
}

// blockingGetStorage holds its Get until the context ends: what proves a
// refused stream cancels the download instead of draining it.
type blockingGetStorage struct {
	*storagetest.Fake
	gets int
}

func (s *blockingGetStorage) Get(ctx context.Context, _ string, _ io.Writer) error {
	s.gets++
	<-ctx.Done()
	return ctx.Err()
}

// TestDownloadStreamVerdicts walks the stream verdicts through the retry loop:
// a transient Get is retried, a short or corrupt stream is skipped, a decode
// failure is hard, and a retried consumer downloads again.
func TestDownloadStreamVerdicts(t *testing.T) {
	ctx := context.Background()
	data := []byte("artifact bytes")
	key := "runs/r/vpn/wireguard.dat.zst"
	rep := covManifest("r", "wireguard", key, data)
	ar := rep.Artifacts[0]
	ar.Chain = nil

	bind := func(t *testing.T, st storage.Storage) (*Engine, *storageBinding) {
		t.Helper()
		_, e := covRestoreEngine(t, map[string]any{"primary": storageInstance("fake", "keep_last")}, "primary")
		return e, &storageBinding{name: "primary", inst: st}
	}

	t.Run("get retried then skipped", func(t *testing.T) {
		store := &covStorage{Fake: storagetest.NewFake(), getData: data, getErr: plugin.Transient(errors.New("reset"))}
		e, sb := bind(t, store)
		_, skip, err := e.downloadStream(ctx, sb, &ar, nil, discardProbe)
		if err != nil || skip == nil || !strings.Contains(skip.Reason, "download:") {
			t.Fatalf("skip %+v, err %v", skip, err)
		}
	})

	t.Run("short stream", func(t *testing.T) {
		e, sb := bind(t, &covStorage{Fake: storagetest.NewFake(), getData: data[:1]})
		_, skip, err := e.downloadStream(ctx, sb, &ar, nil, discardProbe)
		if err != nil || skip == nil || !strings.Contains(skip.Reason, "downloaded") {
			t.Fatalf("skip %+v, err %v", skip, err)
		}
	})

	t.Run("corrupt stream", func(t *testing.T) {
		gets := 0
		e, sb := bind(t, &covStorage{Fake: storagetest.NewFake(), getData: bytes.Repeat([]byte{'z'}, len(data)), onGet: func() { gets++ }})
		_, skip, err := e.downloadStream(ctx, sb, &ar, nil, discardProbe)
		if err != nil || skip == nil || !strings.Contains(skip.Reason, "sha256") {
			t.Fatalf("skip %+v, err %v", skip, err)
		}
		if gets != 1 {
			t.Fatalf("a corrupt copy was downloaded %d times, want one", gets)
		}
	})

	t.Run("long stream", func(t *testing.T) {
		long := append(append([]byte(nil), data...), 'x')
		e, sb := bind(t, &covStorage{Fake: storagetest.NewFake(), getData: long})
		_, skip, err := e.downloadStream(ctx, sb, &ar, nil, discardProbe)
		if err != nil || skip == nil || !strings.Contains(skip.Reason, "downloaded") {
			t.Fatalf("skip %+v, err %v", skip, err)
		}
	})

	t.Run("decode failure", func(t *testing.T) {
		gets := 0
		e, sb := bind(t, &covStorage{Fake: storagetest.NewFake(), getData: data, onGet: func() { gets++ }})
		e.processors["zstd3"] = &covProcessor{decodeErr: errors.New("corrupt stream")}
		stages, err := e.decodeStages(ctx, []model.ProcessorStep{{Instance: "zstd3", Type: "fake"}})
		if err != nil {
			t.Fatal(err)
		}
		_, skip, err := e.downloadStream(ctx, sb, &ar, stages, discardProbe)
		if err == nil || skip != nil || !strings.Contains(err.Error(), "decoding") {
			t.Fatalf("skip %+v, err %v", skip, err)
		}
		// A decode failure is permanent: the same copy decodes the same way, so a
		// retry only costs a second download. A non-permanent error would trip this.
		if class, _ := plugin.ClassOf(err); class != plugin.ClassPermanent {
			t.Fatalf("a decode failure is class %q, want permanent", class)
		}
		if gets != 1 {
			t.Fatalf("a decode failure downloaded the copy %d times, want one", gets)
		}
	})

	t.Run("retried consumer downloads again", func(t *testing.T) {
		gets := 0
		store := &covStorage{Fake: storagetest.NewFake(), getData: data, onGet: func() { gets++ }}
		e, sb := bind(t, store)
		prober := &oneByteProber{err: plugin.Transient(errors.New("probe hiccup"))}
		if _, _, err := e.downloadStream(ctx, sb, &ar, nil, prober.probe); err != nil {
			t.Fatal(err)
		}
		if prober.attempts != 2 {
			t.Fatalf("the consumer ran %d times, want a failed attempt and its retry", prober.attempts)
		}
		if gets != 2 {
			t.Fatalf("a retried consumer downloaded %d times, want 2", gets)
		}
	})
}

// TestDecodeSpool: the apply-side decode writes its own guarded spool file and
// refuses a space refusal with the guard's own error.
func TestDecodeSpool(t *testing.T) {
	ctx := context.Background()
	data := []byte("plaintext")
	ar := model.ArtifactResult{Key: "k", Chain: []model.ProcessorStep{{Instance: "zstd3", Type: "fake"}}}

	t.Run("decodeStages fails", func(t *testing.T) {
		_, e := covRestoreEngine(t, map[string]any{"primary": storageInstance("fake", "keep_last")}, "primary")
		sp, _ := covSpool(t)
		file := covSpoolFile(t, sp, data)
		bad := ar
		bad.Chain = []model.ProcessorStep{{Instance: "ghost", Type: "fake"}}
		if _, err := e.decodeSpool(ctx, sp, file, bad); err == nil || !strings.Contains(err.Error(), "does not have") {
			t.Fatalf("decodeStages failure: %v", err)
		}
	})

	t.Run("create fails", func(t *testing.T) {
		_, e := covRestoreEngine(t, map[string]any{"primary": storageInstance("fake", "keep_last")}, "primary")
		sp, dir := covSpool(t)
		file := covSpoolFile(t, sp, data)
		if err := os.RemoveAll(dir); err != nil {
			t.Fatal(err)
		}
		if _, err := e.decodeSpool(ctx, sp, file, ar); err == nil {
			t.Fatal("a decode into a missing spool dir succeeded")
		}
	})

	t.Run("source vanished", func(t *testing.T) {
		_, e := covRestoreEngine(t, map[string]any{"primary": storageInstance("fake", "keep_last")}, "primary")
		sp, _ := covSpool(t)
		if _, err := e.decodeSpool(ctx, sp, "missing", ar); err == nil {
			t.Fatal("a decode of a vanished file succeeded")
		}
	})

	t.Run("decode fails", func(t *testing.T) {
		_, e := covRestoreEngine(t, map[string]any{"primary": storageInstance("fake", "keep_last")}, "primary")
		e.processors["zstd3"] = &covProcessor{decodeErr: errors.New("corrupt")}
		sp, _ := covSpool(t)
		file := covSpoolFile(t, sp, data)
		if _, err := e.decodeSpool(ctx, sp, file, ar); err == nil || !strings.Contains(err.Error(), "decoding") {
			t.Fatalf("decode failure: %v", err)
		}
	})

	t.Run("close fails", func(t *testing.T) {
		_, e := covRestoreEngine(t, map[string]any{"primary": storageInstance("fake", "keep_last")}, "primary")
		e.processors["zstd3"] = &closeDstProcessor{}
		sp, _ := covSpool(t)
		file := covSpoolFile(t, sp, data)
		if _, err := e.decodeSpool(ctx, sp, file, ar); err == nil {
			t.Fatal("a closed decoded file was accepted")
		}
	})

	t.Run("space during decode", func(t *testing.T) {
		_, e := covRestoreEngine(t, map[string]any{"primary": storageInstance("fake", "keep_last")}, "primary")
		sp, _ := covSpool(t)
		file := covSpoolFile(t, sp, data)
		e.restoreReserve = 1 << 62
		if _, err := e.decodeSpool(ctx, sp, file, ar); err == nil || !strings.Contains(err.Error(), "reserve") {
			t.Fatalf("space refusal: %v", err)
		}
	})

	t.Run("happy", func(t *testing.T) {
		_, e := covRestoreEngine(t, map[string]any{"primary": storageInstance("fake", "keep_last")}, "primary")
		sp, _ := covSpool(t)
		file := covSpoolFile(t, sp, data)
		name, err := e.decodeSpool(ctx, sp, file, ar)
		if err != nil || name == "" {
			t.Fatalf("name %q, err %v", name, err)
		}
	})
}

// closeDstProcessor closes the destination it was handed before answering.
type closeDstProcessor struct{ covProcessor }

func (p *closeDstProcessor) Decode(_ context.Context, dst io.Writer, src io.Reader) error {
	if _, err := io.Copy(dst, src); err != nil {
		return err
	}
	if c, ok := dst.(io.Closer); ok {
		_ = c.Close()
	}
	return nil
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
	_, _, _, _, err := e.fetchToFile(context.Background(), FetchRequest{RunID: "r", Artifact: "a"}, 0)
	if err == nil || !strings.Contains(err.Error(), "no space left") {
		t.Fatalf("fetchToFile with an unreadable spool = %v, want the statfs error", err)
	}
}
