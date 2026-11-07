package restore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	pluginerrors "github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/inventory"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/spool"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage/storagetest"
)

// --- request shape -----------------------------------------------------------

func TestCheckFetchRequestShapes(t *testing.T) {
	cases := []struct {
		name string
		req  FetchRequest
		want string
	}{
		{"neither selector", FetchRequest{Artifact: "a"}, "RunID or Newest"},
		{"both selectors", FetchRequest{RunID: "r", Newest: true, Artifact: "a"}, "exclude each other"},
		{"raw storage only", FetchRequest{RawStorage: "primary"}, "go together"},
		{"raw key only", FetchRequest{RawKey: "k"}, "go together"},
		{"raw with run", FetchRequest{RawStorage: "p", RawKey: "k", RunID: "r"}, "excludes RunID"},
		{"raw with decode", FetchRequest{RawStorage: "p", RawKey: "k", Decode: true}, "never decodes"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := checkFetchRequest(tc.req); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %v, want %q", err, tc.want)
			}
		})
	}
	if err := checkFetchRequest(FetchRequest{RunID: "r", Artifact: "a"}); err != nil {
		t.Fatalf("a valid artifact request was refused: %v", err)
	}
	if err := checkFetchRequest(FetchRequest{RawStorage: "p", RawKey: "k"}); err != nil {
		t.Fatalf("a valid raw request was refused: %v", err)
	}
}

// --- ResolveFetch ------------------------------------------------------------

func TestResolveFetchRefusals(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t)
	w.addStorage("primary")
	e := w.flow()

	if _, err := e.ResolveFetch(ctx, FetchRequest{Artifact: "a"}); err == nil {
		t.Fatal("ResolveFetch accepted a request with neither RunID nor Newest")
	}

	// No restore section.
	w.configured = false
	if _, err := w.flow().ResolveFetch(ctx, FetchRequest{RunID: "r", Artifact: "a"}); err == nil ||
		!strings.Contains(err.Error(), "restore section") {
		t.Fatalf("no restore section: %v", err)
	}
	w.configured = true

	// Unknown storage in the walk order.
	if _, err := e.ResolveFetch(ctx, FetchRequest{RunID: "r", Artifact: "a", Storages: []string{"ghost"}}); err == nil {
		t.Fatal("ResolveFetch accepted an unknown storage")
	}

	// No run holds the artifact.
	if _, err := e.ResolveFetch(ctx, FetchRequest{Newest: true, Artifact: "nope"}); err == nil ||
		!strings.Contains(err.Error(), "no run on any storage") {
		t.Fatalf("no candidate run: %v", err)
	}

	// Raw by-key with an unknown storage.
	if _, err := e.ResolveFetch(ctx, FetchRequest{RawStorage: "ghost", RawKey: "k"}); err == nil {
		t.Fatal("ResolveFetch accepted an unknown raw storage")
	}
}

func TestResolveFetchArtifactAndRaw(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t)
	f := w.addStorage("primary")
	e := w.flow()
	data := []byte("artifact bytes")
	key := "runs/r/vpn/wireguard.dat.zst"
	seed(t, f, covManifest("r", "wireguard", key, data), key, data)

	plan, err := e.ResolveFetch(ctx, FetchRequest{RunID: "r", Artifact: "wireguard"})
	if err != nil {
		t.Fatal(err)
	}
	if plan.RunID != "r" || plan.Key != key || plan.Bytes != int64(len(data)) ||
		len(plan.Storages) != 1 || !plan.Storages[0].Holds {
		t.Fatalf("plan = %+v", plan)
	}

	raw, err := e.ResolveFetch(ctx, FetchRequest{RawStorage: "primary", RawKey: key})
	if err != nil {
		t.Fatal(err)
	}
	if raw.Key != key || !raw.Storages[0].Holds || raw.RunID != "" {
		t.Fatalf("raw plan = %+v", raw)
	}
	absent, err := e.ResolveFetch(ctx, FetchRequest{RawStorage: "primary", RawKey: "absent"})
	if err != nil {
		t.Fatal(err)
	}
	if absent.Storages[0].Holds || absent.Storages[0].Reason == "" {
		t.Fatalf("an absent raw key holds: %+v", absent)
	}
}

// TestResolveFetchFailedSurvey: one storage answers, another cannot be surveyed
// at all — the plan reports the failed storage as a skip.
func TestResolveFetchFailedSurvey(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t)
	f := w.addStorage("primary")
	off := w.addStorage("offsite")
	e := w.flow()
	data := []byte("artifact bytes")
	key := "runs/r/vpn/wireguard.dat.zst"
	seed(t, f, covManifest("r", "wireguard", key, data), key, data)
	off.Fault = func(op storagetest.Op, _ string, _ int) error {
		if op == storagetest.OpList {
			return pluginerrors.Permanent(errors.New("listing refused"))
		}
		return nil
	}
	plan, err := e.ResolveFetch(ctx, FetchRequest{RunID: "r", Artifact: "wireguard"})
	if err != nil {
		t.Fatal(err)
	}
	var failed bool
	for _, h := range plan.Storages {
		if h.Storage == "offsite" && !h.Holds && strings.Contains(h.Reason, "survey failed") {
			failed = true
		}
	}
	if !failed {
		t.Fatalf("the failed survey is not reported: %+v", plan.Storages)
	}
}

func TestResolveFetchListsSizeMismatch(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t)
	f := w.addStorage("primary")
	rep := covManifest("r", "wireguard", "runs/r/vpn/wireguard.dat.zst", []byte("artifact bytes"))
	// A manifest claiming eight bytes while the listing records three.
	rep.Artifacts[0].Bytes = 8
	f.PutObject("runs/r/manifest.json", mustMarshal(t, rep), time.Unix(1700000000, 0).UTC())
	f.PutObject(rep.Artifacts[0].Key, []byte("abc"), time.Unix(1700000000, 0).UTC())
	e := w.flow()
	if _, err := e.ResolveFetch(ctx, FetchRequest{RunID: "r", Artifact: "wireguard"}); err == nil ||
		!strings.Contains(err.Error(), "in the listing") {
		t.Fatalf("listing size mismatch not reported: %v", err)
	}

	// An artifact the manifest records but the listing does not hold.
	f2 := w.addStorage("second")
	seed(t, f2, covManifest("r2", "wireguard", "nowhere", []byte("x")), "", nil)
	if _, err := w.flow().ResolveFetch(ctx, FetchRequest{RunID: "r2", Artifact: "wireguard"}); err == nil ||
		!strings.Contains(err.Error(), "not in the storage listing") {
		t.Fatalf("a listing without the object not reported: %v", err)
	}
}

// --- Fetch -------------------------------------------------------------------

func TestFetchRawAndArtifact(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t)
	f := w.addStorage("primary")
	e := w.flow()
	data := []byte("artifact bytes")
	key := "runs/r/vpn/wireguard.dat.zst"
	seed(t, f, covManifest("r", "wireguard", key, data), key, data)

	var raw bytes.Buffer
	rep, err := e.Fetch(ctx, FetchRequest{RawStorage: "primary", RawKey: key}, &raw)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw.Bytes(), data) || rep.Verified || rep.SHA256 != model.SHA256Hex(data) {
		t.Fatalf("raw fetch = %+v, %q", rep, raw.Bytes())
	}

	var out bytes.Buffer
	rep, err = e.Fetch(ctx, FetchRequest{RunID: "r", Artifact: "wireguard"}, &out)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out.Bytes(), data) || !rep.Verified || rep.RunID != "r" || rep.Storage != "primary" {
		t.Fatalf("artifact fetch = %+v, %q", rep, out.Bytes())
	}

	// Decode reverses the chain over the verified spool file.
	out.Reset()
	rep, err = e.Fetch(ctx, FetchRequest{RunID: "r", Artifact: "wireguard", Decode: true}, &out)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out.Bytes(), data) || !rep.Verified {
		t.Fatalf("decoded fetch = %+v, %q", rep, out.Bytes())
	}
}

func TestFetchFallsBackToTheSecondStorage(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t)
	first := w.addStorage("primary")
	second := w.addStorage("offsite")
	e := w.flow()
	data := []byte("artifact bytes")
	key := "runs/r/vpn/wireguard.dat.zst"
	seed(t, second, covManifest("r", "wireguard", key, data), key, data)
	first.Fault = func(op storagetest.Op, _ string, _ int) error {
		if op == storagetest.OpList {
			return nil
		}
		return pluginerrors.Transient(errors.New("reset"))
	}
	var out bytes.Buffer
	rep, err := e.Fetch(ctx, FetchRequest{RunID: "r", Artifact: "wireguard"}, &out)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Storage != "offsite" || !bytes.Equal(out.Bytes(), data) {
		t.Fatalf("fallback fetch = %+v", rep)
	}
}

func TestFetchNewestOK(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t)
	f := w.addStorage("primary")
	e := w.flow()
	data := []byte("artifact bytes")
	oldKey := "runs/20990101T000000Z/vpn/wireguard.dat.zst"
	seed(t, f, covManifest("20990101T000000Z", "wireguard", oldKey, data), oldKey, data)
	newer := covManifest("20990101T000100Z", "wireguard", "runs/20990101T000100Z/vpn/wireguard.dat.zst", data)
	newer.StartedAt = newer.StartedAt.Add(time.Hour)
	newer.Artifacts[0].Outcome = model.ArtifactFailed
	f.PutObject("runs/20990101T000100Z/manifest.json", mustMarshal(t, newer), time.Unix(1700000000, 0).UTC())

	var out bytes.Buffer
	rep, err := e.Fetch(ctx, FetchRequest{Newest: true, Artifact: "wireguard"}, &out)
	if err != nil {
		t.Fatal(err)
	}
	if rep.RunID != "20990101T000000Z" {
		t.Fatalf("newest-ok served run %q", rep.RunID)
	}
}

func TestFetchRefusals(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t)
	w.addStorage("primary")
	e := w.flow()
	if _, err := e.Fetch(ctx, FetchRequest{}, io.Discard); err == nil {
		t.Fatal("Fetch accepted an empty request")
	}
	// A raw request asking for a decode.
	if _, err := e.Fetch(ctx, FetchRequest{RawStorage: "primary", RawKey: "k", Decode: true}, io.Discard); err == nil ||
		!strings.Contains(err.Error(), "never decodes") {
		t.Fatalf("raw decode accepted: %v", err)
	}
	// No restore section.
	w.configured = false
	if _, err := w.flow().Fetch(ctx, FetchRequest{RunID: "r", Artifact: "a"}, io.Discard); err == nil ||
		!strings.Contains(err.Error(), "restore section") {
		t.Fatalf("no restore section: %v", err)
	}
}

func TestFetchToFileRefusals(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t)
	w.addStorage("primary")
	e := w.flow()

	// An unusable spool directory.
	w.spoolDir = ""
	if _, _, _, _, err := w.flow().fetchToFile(ctx, FetchRequest{RunID: "r", Artifact: "a"}, 0); err == nil {
		t.Fatal("an empty spool directory was accepted")
	}
	_ = e
	w.spoolDir = t.TempDir()

	// The reserve is larger than the free space.
	w.reserve = 1 << 62
	if _, _, _, _, err := w.flow().fetchToFile(ctx, FetchRequest{RunID: "r", Artifact: "a"}, 0); err == nil ||
		!strings.Contains(err.Error(), "reserve") {
		t.Fatalf("reserve not enforced: %v", err)
	}
}

// --- fetchRaw ----------------------------------------------------------------

func TestFetchRawRefusals(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t)
	w.addStorage("primary")
	e := w.flow()
	sp, _ := testSpool(t)

	// Unknown storage.
	if _, _, err := e.fetchRaw(ctx, FetchRequest{RawStorage: "ghost", RawKey: "k"}, sp); err == nil {
		t.Fatal("fetchRaw accepted an unknown storage")
	}

	// The spool directory is gone: Create fails permanently.
	gone, goneDir := testSpool(t)
	if err := os.RemoveAll(goneDir); err != nil {
		t.Fatal(err)
	}
	if _, _, err := e.fetchRaw(ctx, FetchRequest{RawStorage: "primary", RawKey: "k"}, gone); err == nil {
		t.Fatal("fetchRaw wrote into a missing spool directory")
	}

	// The storage's Get fails.
	w.replace("primary", &covStorage{Fake: storagetest.NewFake(), getErr: pluginerrors.Transient(errors.New("connection reset"))})
	if _, _, err := w.flow().fetchRaw(ctx, FetchRequest{RawStorage: "primary", RawKey: "k"}, sp); err == nil {
		t.Fatal("fetchRaw accepted a failing Get")
	}

	// The storage closes the spool writer under the workflow: the close error is
	// what the attempt sees.
	w.replace("primary", &covStorage{Fake: storagetest.NewFake(), getData: []byte("x"), closeW: true})
	if _, _, err := w.flow().fetchRaw(ctx, FetchRequest{RawStorage: "primary", RawKey: "k"}, sp); err == nil {
		t.Fatal("fetchRaw accepted a closed writer")
	}

	// A space refusal during the raw write is the guard's own verdict, whether
	// the storage reports the write error, swallows it, or closes the sink.
	w.reserve = 1 << 62
	w.replace("primary", &covStorage{Fake: storagetest.NewFake(), getData: []byte("x")})
	if _, _, err := w.flow().fetchRaw(ctx, FetchRequest{RawStorage: "primary", RawKey: "k"}, sp); err == nil ||
		!strings.Contains(err.Error(), "reserve") {
		t.Fatalf("a raw space refusal was not reported: %v", err)
	}
	w.replace("primary", &carelessStorage{Fake: storagetest.NewFake()})
	if _, _, err := w.flow().fetchRaw(ctx, FetchRequest{RawStorage: "primary", RawKey: "k"}, sp); err == nil ||
		!strings.Contains(err.Error(), "reserve") {
		t.Fatalf("a swallowed raw space refusal was not reported: %v", err)
	}
	w.replace("primary", &carelessCloseStorage{Fake: storagetest.NewFake()})
	if _, _, err := w.flow().fetchRaw(ctx, FetchRequest{RawStorage: "primary", RawKey: "k"}, sp); err == nil ||
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

// --- fetchArtifact / walkDownload --------------------------------------------

func TestFetchArtifactRefusals(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t)
	w.addStorage("primary")
	e := w.flow()
	sp, _ := testSpool(t)

	if _, _, _, _, err := e.fetchArtifact(ctx, FetchRequest{RunID: "r", Artifact: "a", Storages: []string{"ghost"}}, sp, 0); err == nil {
		t.Fatal("fetchArtifact accepted an unknown storage")
	}
	if _, _, _, _, err := e.fetchArtifact(ctx, FetchRequest{Newest: true, Artifact: "nope"}, sp, 0); err == nil ||
		!strings.Contains(err.Error(), "no run on any storage") {
		t.Fatalf("no candidate run: %v", err)
	}
}

// TestWalkDownloadPreflight covers the walk's refusals before a byte moves.
func TestWalkDownloadPreflight(t *testing.T) {
	ctx := context.Background()
	data := []byte("artifact bytes")
	key := "runs/r/vpn/wireguard.dat.zst"
	rep := covManifest("r", "wireguard", key, data)
	req := FetchRequest{RunID: "r", Artifact: "wireguard"}
	takeOK := func(_ context.Context, _ *binding, ar *model.ArtifactResult) (copyResult, *StorageSkip, error) {
		return copyResult{ar: *ar}, nil, nil
	}

	cases := []struct {
		name      string
		survey    func() (inventory.Survey, bool)
		storage   storage.Reader
		want      string
		wantSkips int
	}{
		{
			name:      "survey failed",
			survey:    func() (inventory.Survey, bool) { return inventory.Survey{}, false },
			storage:   storagetest.NewFake(),
			want:      "survey failed",
			wantSkips: 1,
		},
		{
			name: "artifact refused by the manifest",
			survey: func() (inventory.Survey, bool) {
				return covSurvey(map[string]*model.RunReport{"r": failedManifest("r")}), true
			},
			storage:   storagetest.NewFake(),
			want:      "failed in run",
			wantSkips: 1,
		},
		{
			name:      "object not found",
			survey:    func() (inventory.Survey, bool) { return covSurvey(map[string]*model.RunReport{"r": rep}), true },
			storage:   &covStorage{Fake: storagetest.NewFake(), statErr: pluginerrors.NotFound(errors.New("gone"))},
			want:      "not found",
			wantSkips: 1,
		},
		{
			name:      "stat fails",
			survey:    func() (inventory.Survey, bool) { return covSurvey(map[string]*model.RunReport{"r": rep}), true },
			storage:   &covStorage{Fake: storagetest.NewFake(), statErr: pluginerrors.Transient(errors.New("timeout"))},
			want:      "stat:",
			wantSkips: 1,
		},
		{
			name:      "size mismatch before download",
			survey:    func() (inventory.Survey, bool) { return covSurvey(map[string]*model.RunReport{"r": rep}), true },
			storage:   &covStorage{Fake: storagetest.NewFake(), statInfo: model.ObjectInfo{Size: 1}},
			want:      "manifest says",
			wantSkips: 1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := newWorld(t)
			w.addStorage("primary")
			w.replace("primary", tc.storage)
			e := w.flow()
			sb := &binding{name: "primary", port: w.flow().storageByName("primary").port}
			_, _, _, skips, err := walkDownload(ctx, e, req, []*binding{sb},
				func(*binding) (inventory.Survey, bool) { return tc.survey() }, takeOK)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %v, want %q", err, tc.want)
			}
			if len(skips) != tc.wantSkips {
				t.Fatalf("skips %+v", skips)
			}
		})
	}

	// A take that skips, and a take that fails hard.
	w := newWorld(t)
	w.addStorage("primary")
	okStorage := &covStorage{Fake: storagetest.NewFake(), statInfo: model.ObjectInfo{Size: int64(len(data))}}
	w.replace("primary", okStorage)
	e := w.flow()
	sb := e.storageByName("primary")
	getSurvey := func(*binding) (inventory.Survey, bool) { return covSurvey(map[string]*model.RunReport{"r": rep}), true }
	_, _, _, _, err := walkDownload(ctx, e, req, []*binding{sb}, getSurvey,
		func(context.Context, *binding, *model.ArtifactResult) (copyResult, *StorageSkip, error) {
			return copyResult{}, &StorageSkip{Storage: "primary", Reason: "passed over"}, nil
		})
	if err == nil || !strings.Contains(err.Error(), "passed over") {
		t.Fatalf("skip not reported: %v", err)
	}
	hard := errors.New("hard stop")
	if _, _, _, _, err := walkDownload(ctx, e, req, []*binding{sb}, getSurvey,
		func(context.Context, *binding, *model.ArtifactResult) (copyResult, *StorageSkip, error) {
			return copyResult{}, nil, hard
		}); !errors.Is(err, hard) {
		t.Fatalf("hard error not propagated: %v", err)
	}
}

// TestWalkDownloadNewestFallsThrough: a newest run whose copy is skipped is
// passed over for an older one, and the walk names the skip.
func TestWalkDownloadNewestFallsThrough(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t)
	w.addStorage("primary")
	oldRep := covManifest("r-old", "wireguard", "old", []byte("x"))
	oldRep.StartedAt = time.Unix(1600000000, 0).UTC()
	sur := inventory.Survey{OK: true, Runs: map[string]inventory.Run{
		"r-new": {Manifest: covManifest("r-new", "wireguard", "new", []byte("x")), Stored: model.StoredRun{RunID: "r-new", Kind: model.RunKindScheduled}},
		"r-old": {Manifest: oldRep, Stored: model.StoredRun{RunID: "r-old", Kind: model.RunKindScheduled}},
	}}
	w.replace("primary", &covStorage{Fake: storagetest.NewFake(), statInfo: model.ObjectInfo{Size: 1}})
	e := w.flow()
	sb := e.storageByName("primary")
	runID, _, _, _, err := walkDownload(ctx, e, FetchRequest{Newest: true, Artifact: "wireguard"},
		[]*binding{sb},
		func(*binding) (inventory.Survey, bool) { return sur, true },
		func(context.Context, *binding, *model.ArtifactResult) (copyResult, *StorageSkip, error) {
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

	newFlow := func(t *testing.T, st storage.Reader) (*Workflow, *binding) {
		t.Helper()
		w := newWorld(t)
		w.addStorage("primary")
		w.replace("primary", st)
		e := w.flow()
		return e, e.storageByName("primary")
	}

	t.Run("missing spool dir", func(t *testing.T) {
		sp, dir := testSpool(t)
		if err := os.RemoveAll(dir); err != nil {
			t.Fatal(err)
		}
		_, err := spoolCopy(ctx, storagetest.NewFake(), &rep.Artifacts[0], sp, 0)
		if ce, ok := errors.AsType[*copyError](err); !ok || !ce.skip || !strings.Contains(ce.reason, "download:") {
			t.Fatalf("a missing spool dir = %v", err)
		}
	})

	t.Run("get fails", func(t *testing.T) {
		e, sb := newFlow(t, &covStorage{Fake: storagetest.NewFake(), getErr: pluginerrors.Transient(errors.New("reset"))})
		sp := mustSpool(t, e)
		_, skip, err := e.downloadSpool(ctx, sb, &rep.Artifacts[0], sp, 0)
		if err != nil || skip == nil || !strings.Contains(skip.Reason, "download:") {
			t.Fatalf("skip %+v, err %v", skip, err)
		}
	})

	t.Run("writer closed under the workflow", func(t *testing.T) {
		e, sb := newFlow(t, &covStorage{Fake: storagetest.NewFake(), getData: data, closeW: true})
		sp := mustSpool(t, e)
		_, skip, err := e.downloadSpool(ctx, sb, &rep.Artifacts[0], sp, 0)
		if err != nil || skip == nil || !strings.Contains(skip.Reason, "download:") {
			t.Fatalf("skip %+v, err %v", skip, err)
		}
	})

	t.Run("short download", func(t *testing.T) {
		e, sb := newFlow(t, &covStorage{Fake: storagetest.NewFake(), getData: data[:1]})
		sp := mustSpool(t, e)
		_, skip, err := e.downloadSpool(ctx, sb, &rep.Artifacts[0], sp, 0)
		if err != nil || skip == nil || !strings.Contains(skip.Reason, "downloaded") {
			t.Fatalf("skip %+v, err %v", skip, err)
		}
	})

	t.Run("sha256 mismatch", func(t *testing.T) {
		e, sb := newFlow(t, &covStorage{Fake: storagetest.NewFake(), getData: bytes.Repeat([]byte{'z'}, len(data))})
		sp := mustSpool(t, e)
		_, skip, err := e.downloadSpool(ctx, sb, &rep.Artifacts[0], sp, 0)
		if err != nil || skip == nil || !strings.Contains(skip.Reason, "sha256") {
			t.Fatalf("skip %+v, err %v", skip, err)
		}
	})

	t.Run("long download stops at the manifest size", func(t *testing.T) {
		long := append(append([]byte(nil), data...), 'x')
		gets := 0
		store := &covStorage{Fake: storagetest.NewFake(), getData: long, onGet: func() { gets++ }}
		e, sb := newFlow(t, store)
		sp := mustSpool(t, e)
		_, skip, err := e.downloadSpool(ctx, sb, &rep.Artifacts[0], sp, 0)
		if err != nil || skip == nil || !strings.Contains(skip.Reason, "downloaded") {
			t.Fatalf("skip %+v, err %v", skip, err)
		}
		if gets != 1 {
			t.Fatalf("an overflow was downloaded %d times, want one attempt", gets)
		}
	})

	t.Run("happy download", func(t *testing.T) {
		e, sb := newFlow(t, &covStorage{Fake: storagetest.NewFake(), getData: data})
		sp := mustSpool(t, e)
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
	w := newWorld(t)
	w.addStorage("primary")
	w.replace("primary", store)
	e := w.flow()

	orig := spoolFreeBytes
	t.Cleanup(func() { spoolFreeBytes = orig })
	calls := 0
	spoolFreeBytes = func(*spool.Spool) (int64, error) {
		calls++
		return int64(len(data)) - 1, nil
	}
	sb := e.storageByName("primary")
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
	if _, _, err := e.downloadSpool(ctx, sb, &rep.Artifacts[0], mustSpool(t, e), 0); err == nil ||
		!strings.Contains(err.Error(), "restore spool") {
		t.Fatalf("a guard refusal during the write was not hard: %v", err)
	}
	if entries, err := readDirNames(e.o.SpoolDir); err != nil || len(entries) != 0 {
		t.Fatalf("a refused copy left %v, %v", entries, err)
	}
}

// --- streamCopy --------------------------------------------------------------

func TestStreamCopyVerdicts(t *testing.T) {
	ctx := context.Background()
	data := []byte("decoded artifact")
	ar := model.ArtifactResult{Key: "k", Bytes: int64(len(data)), RawBytes: int64(len(data)),
		SHA256: model.SHA256Hex(data), Chain: []model.ProcessorStep{{Instance: "zstd3", Type: "fake"}}}

	t.Run("happy stream", func(t *testing.T) {
		w := newWorld(t)
		w.addStorage("primary")
		w.replace("primary", &covStorage{Fake: storagetest.NewFake(), getData: data})
		e := w.flow()
		probe, err := streamCopy(ctx, e.storageByName("primary").port, &ar, nil, func(_ context.Context, src io.Reader) (source.ProbeReport, error) {
			n, err := io.Copy(io.Discard, src)
			return source.ProbeReport{Bytes: n}, err
		})
		if err != nil || probe.Bytes != int64(len(data)) {
			t.Fatalf("probe %+v, err %v", probe, err)
		}
	})

	t.Run("consumer panics", func(t *testing.T) {
		w := newWorld(t)
		w.addStorage("primary")
		w.replace("primary", &covStorage{Fake: storagetest.NewFake(), getData: data})
		e := w.flow()
		_, err := streamCopy(ctx, e.storageByName("primary").port, &ar, nil, func(context.Context, io.Reader) (source.ProbeReport, error) {
			panic("consumer panic")
		})
		if err == nil {
			t.Fatal("a panicking consumer was accepted")
		}
	})

	t.Run("consumer refuses as unsupported", func(t *testing.T) {
		w := newWorld(t)
		w.addStorage("primary")
		w.replace("primary", &covStorage{Fake: storagetest.NewFake(), getData: data})
		e := w.flow()
		_, err := streamCopy(ctx, e.storageByName("primary").port, &ar, nil, func(context.Context, io.Reader) (source.ProbeReport, error) {
			return source.ProbeReport{}, errors.ErrUnsupported
		})
		if err == nil || !errors.Is(err, errors.ErrUnsupported) {
			t.Fatalf("unsupported consumer = %v", err)
		}
	})

	t.Run("decode fails after the stream verified", func(t *testing.T) {
		w := newWorld(t)
		w.addStorage("primary")
		w.replace("primary", &covStorage{Fake: storagetest.NewFake(), getData: data})
		w.processor("zstd3").decodeErr = errors.New("corrupt stream")
		e := w.flow()
		stages, err := e.decodeStages(ctx, ar.Chain)
		if err != nil {
			t.Fatal(err)
		}
		_, err = streamCopy(ctx, e.storageByName("primary").port, &ar, stages, discardProbe)
		var ce *copyError
		if !errors.As(err, &ce) || ce.skip || !strings.Contains(ce.err.Error(), "corrupt stream") {
			t.Fatalf("decode failure = %v", err)
		}
	})

	t.Run("consumer error after a good stream", func(t *testing.T) {
		w := newWorld(t)
		w.addStorage("primary")
		w.replace("primary", &covStorage{Fake: storagetest.NewFake(), getData: data})
		e := w.flow()
		_, err := streamCopy(ctx, e.storageByName("primary").port, &ar, nil, func(context.Context, io.Reader) (source.ProbeReport, error) {
			return source.ProbeReport{}, pluginerrors.Transient(errors.New("consumer failed"))
		})
		var ce *copyError
		if !errors.As(err, &ce) || ce.skip || !strings.Contains(ce.err.Error(), "consumer failed") {
			t.Fatalf("consumer failure = %v", err)
		}
	})
}

// TestDecodeTo covers the spool decode helper's refusals.
func TestDecodeTo(t *testing.T) {
	ctx := context.Background()
	ar := model.ArtifactResult{Key: "k", Chain: []model.ProcessorStep{{Instance: "zstd3", Type: "fake"}}}
	data := []byte("plaintext")

	w := newWorld(t)
	w.addStorage("primary")
	e := w.flow()
	sp, _ := testSpool(t)
	file := spoolFile(t, sp, data)

	bad := ar
	bad.Chain = []model.ProcessorStep{{Instance: "ghost", Type: "fake"}}
	if err := e.decodeTo(ctx, sp, file, bad, io.Discard); err == nil ||
		!strings.Contains(err.Error(), "does not have") {
		t.Fatalf("decodeStages failure not propagated: %v", err)
	}
	if err := e.decodeTo(ctx, sp, "missing", ar, io.Discard); err == nil {
		t.Fatal("decodeTo opened a missing spool file")
	}
	w.processor("zstd3").decodeErr = fmt.Errorf("age: %w", errors.ErrUnsupported)
	if err := e.decodeTo(ctx, sp, file, ar, io.Discard); err == nil ||
		!strings.Contains(err.Error(), "cannot decode the artifact") {
		t.Fatalf("unsupported decode not reported: %v", err)
	}
	w.processor("zstd3").decodeErr = errors.New("corrupt stream")
	if err := e.decodeTo(ctx, sp, file, ar, io.Discard); err == nil ||
		!strings.Contains(err.Error(), "decoding k") {
		t.Fatalf("decode failure not named: %v", err)
	}
}

// --- primitives --------------------------------------------------------------

func TestMeterAndDetachWriter(t *testing.T) {
	var sink bytes.Buffer
	m := &meter{w: &sink, hash: sha256.New(), limit: 4}
	if n, err := m.Write([]byte("abcd")); err != nil || n != 4 {
		t.Fatalf("write = %d, %v", n, err)
	}
	if _, err := m.Write([]byte("e")); !errors.Is(err, errOverflow) || !m.overflow || m.over != 5 {
		t.Fatalf("overflow = %v, over %d", err, m.over)
	}
	if _, err := m.Write([]byte("f")); !errors.Is(err, errOverflow) {
		t.Fatalf("second overflow = %v", err)
	}
	if m.sum() != model.SHA256Hex([]byte("abcd")) {
		t.Fatalf("sum = %s", m.sum())
	}

	d := &detachWriter{w: failingWriter{}}
	for i := 0; i < 3; i++ {
		if n, err := d.Write([]byte("xx")); err != nil || n != 2 {
			t.Fatalf("detached write = %d, %v", n, err)
		}
	}

	if err := (&meter{w: io.Discard, hash: sha256.New(), limit: 1}).Close(); err != nil {
		t.Fatalf("close without a closer = %v", err)
	}
	closerMeter := &meter{w: &countingCloser{}, hash: sha256.New(), limit: 1}
	if err := closerMeter.Close(); err != nil {
		t.Fatalf("close = %v", err)
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("write failed") }

type countingCloser struct{ closed int }

func (c *countingCloser) Write(p []byte) (int, error) { return len(p), nil }
func (c *countingCloser) Close() error                { c.closed++; return nil }

func TestSpoolSpaceErrorMessages(t *testing.T) {
	e := &spoolSpaceError{free: 1, need: 2, reserve: 3, written: 4}
	if !strings.Contains(e.Error(), "free 1 bytes, need 2, reserve 3, written 4") {
		t.Fatalf("message = %q", e.Error())
	}
	cause := errors.New("no space")
	e.cause = cause
	if !strings.Contains(e.Error(), "no space") || !errors.Is(e, cause) {
		t.Fatalf("caused message = %q", e.Error())
	}
	var nilCause *spoolSpaceError
	_ = nilCause
	if !isNoSpace(syscall.ENOSPC) || !isNoSpace(syscall.EDQUOT) || isNoSpace(errors.New("other")) {
		t.Fatal("isNoSpace classification is wrong")
	}
}

func TestSpaceGuardBudgetAndErrors(t *testing.T) {
	orig := spoolFreeBytes
	t.Cleanup(func() { spoolFreeBytes = orig })

	// A statfs failure is the guard's terminal verdict.
	spoolFreeBytes = func(*spool.Spool) (int64, error) { return 0, errors.New("statfs: io error") }
	sp := testSpoolOnly(t)
	g := &spaceGuard{w: &countingCloser{}, sp: sp, reserve: 0}
	if _, err := g.Write([]byte("x")); err == nil || !strings.Contains(err.Error(), "statfs") {
		t.Fatalf("statfs failure = %v", err)
	}
	if _, err := g.Write([]byte("x")); err == nil {
		t.Fatal("a terminal guard verdict was not sticky")
	}

	// The reserve refusal.
	spoolFreeBytes = func(*spool.Spool) (int64, error) { return 0, nil }
	g = &spaceGuard{w: &countingCloser{}, sp: sp, reserve: 1}
	if _, err := g.Write([]byte("x")); err == nil || !strings.Contains(err.Error(), "reserve") {
		t.Fatalf("reserve refusal = %v", err)
	}

	// An ENOSPC from the sink is a space refusal with the guard's numbers.
	spoolFreeBytes = func(*spool.Spool) (int64, error) { return 1 << 30, nil }
	g = &spaceGuard{w: &errCloser{err: syscall.ENOSPC}, sp: sp}
	if _, err := g.Write([]byte("x")); err == nil || !strings.Contains(err.Error(), "reserve") {
		t.Fatalf("ENOSPC = %v", err)
	}

	// A non-space sink error passes through.
	g = &spaceGuard{w: &errCloser{err: errors.New("disk broke")}, sp: sp}
	if _, err := g.Write([]byte("x")); err == nil || !strings.Contains(err.Error(), "disk broke") {
		t.Fatalf("sink error = %v", err)
	}

	// Close reports a shortage the filesystem only reports then.
	g = &spaceGuard{w: &errCloser{err: syscall.ENOSPC, closeErr: syscall.ENOSPC}, sp: sp}
	if err := g.Close(); err == nil || g.err == nil || !strings.Contains(g.err.Error(), "reserve") {
		t.Fatalf("close shortage = %v, %v", err, g.err)
	}
	if err := (&spaceGuard{w: &countingCloser{}, sp: sp}).Close(); err != nil {
		t.Fatalf("clean close = %v", err)
	}
}

type errCloser struct {
	err      error
	closeErr error
}

func (e *errCloser) Write([]byte) (int, error) { return 0, e.err }
func (e *errCloser) Close() error              { return e.closeErr }

// --- helpers -----------------------------------------------------------------

func testSpool(t *testing.T) (*spool.Spool, string) {
	t.Helper()
	dir := t.TempDir()
	sp, err := spool.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	return sp, dir
}

func testSpoolOnly(t *testing.T) *spool.Spool {
	t.Helper()
	sp, _ := testSpool(t)
	return sp
}

func spoolFile(t *testing.T, sp *spool.Spool, data []byte) string {
	t.Helper()
	w, err := sp.Create("test")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return w.Name()
}

func mustSpool(t *testing.T, e *Workflow) *spool.Spool {
	t.Helper()
	sp, err := spool.New(e.o.SpoolDir)
	if err != nil {
		t.Fatal(err)
	}
	return sp
}
