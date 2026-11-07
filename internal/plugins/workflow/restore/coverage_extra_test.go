package restore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"
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

// seeded is one workflow over one storage that holds one ok run.
func seeded(t *testing.T) (*testWorld, *Workflow, []byte, string) {
	t.Helper()
	w := newWorld(t)
	f := w.addStorage("primary")
	data := []byte("artifact bytes")
	key := "runs/r/vpn/wireguard.dat.zst"
	seed(t, f, covManifest("r", "wireguard", key, data), key, data)
	return w, w.flow(), data, key
}

func TestApplyResolveAndSpoolFailures(t *testing.T) {
	ctx := context.Background()

	// A request ResolveFetch refuses fails the apply before the target.
	w, _, _, _ := seeded(t)
	req := applyReq()
	req.Fetch = FetchRequest{Artifact: "wireguard"}
	if _, err := w.flow().Apply(ctx, req); err == nil {
		t.Fatal("an unresolved request reached the target")
	}

	// A spool that cannot serve the copy fails the validated run's fetch.
	w, _, _, _ = seeded(t)
	w.targets = &fakeTargets{target: Target{Type: "fake", Source: &restorerSource{fakeSource: &fakeSource{}}}}
	bad := w.flow(func(o *Options) { o.SpoolDir = "" })
	if _, err := bad.Apply(ctx, applyReq()); err == nil || !strings.Contains(err.Error(), "not restorable") {
		t.Fatalf("spool failure: %v", err)
	}

	// The chain the manifest records is not decodable here.
	w, _, _, key := seeded(t)
	f := w.storage("primary")
	rep := covManifest("r", "wireguard", key, []byte("artifact bytes"))
	rep.Artifacts[0].Chain = []model.ProcessorStep{{Instance: "ghost", Type: "fake"}}
	f.PutObject("runs/r/manifest.json", mustMarshal(t, rep), time.Unix(1700000000, 0).UTC())
	w.targets = &fakeTargets{target: Target{Type: "fake", Source: &restorerSource{fakeSource: &fakeSource{}}}}
	if _, err := w.flow().Apply(ctx, applyReq()); err == nil || !strings.Contains(err.Error(), "does not have") {
		t.Fatalf("decode failure: %v", err)
	}
}

func TestApplyDecodeSpoolFileFailures(t *testing.T) {
	ctx := context.Background()
	open, create, free, closeFn := spoolOpen, spoolCreate, spoolFreeBytes, closeSpoolFile
	reset := func() { spoolOpen, spoolCreate, spoolFreeBytes, closeSpoolFile = open, create, free, closeFn }
	t.Cleanup(reset)

	// The decode spool file cannot be created.
	w, _, _, _ := seeded(t)
	w.targets = &fakeTargets{target: Target{Type: "fake", Source: &restorerSource{fakeSource: &fakeSource{}}}}
	restoreSpoolSeams(t)
	spoolCreate = func(*spool.Spool, string) (*spool.Writer, error) { return nil, errors.New("create refused") }
	if _, err := w.flow().Apply(ctx, applyReq()); err == nil || !strings.Contains(err.Error(), "create refused") {
		t.Fatalf("create failure: %v", err)
	}

	// The verified spool file cannot be reopened for the decode.
	reset()
	w, _, _, _ = seeded(t)
	w.targets = &fakeTargets{target: Target{Type: "fake", Source: &restorerSource{fakeSource: &fakeSource{}}}}
	spoolOpen = func(*spool.Spool, string) (*os.File, error) { return nil, errors.New("open refused") }
	if _, err := w.flow().Apply(ctx, applyReq()); err == nil || !strings.Contains(err.Error(), "open refused") {
		t.Fatalf("open failure: %v", err)
	}

	// A space refusal during the decode write is the guard's verdict.
	reset()
	w, _, _, _ = seeded(t)
	w.targets = &fakeTargets{target: Target{Type: "fake", Source: &restorerSource{fakeSource: &fakeSource{}}}}
	calls := 0
	spoolFreeBytes = func(*spool.Spool) (int64, error) {
		calls++
		if calls == 1 { // the copy's own pre-check
			return 1 << 30, nil
		}
		return 0, nil
	}
	if _, err := w.flow().Apply(ctx, applyReq()); err == nil || !strings.Contains(err.Error(), "restore spool") {
		t.Fatalf("decode space refusal: %v", err)
	}

	// A close the filesystem reports only on close is the decode's verdict.
	reset()
	w, _, _, _ = seeded(t)
	w.targets = &fakeTargets{target: Target{Type: "fake", Source: &restorerSource{fakeSource: &fakeSource{}}}}
	spoolFreeBytes = func(*spool.Spool) (int64, error) { return 1 << 30, nil }
	closes := 0
	closeSpoolFile = func(c io.Closer) error {
		closes++
		if closes > 1 { // the copy's guard closes first
			return errors.New("close refused")
		}
		return c.Close()
	}
	if _, err := w.flow().Apply(ctx, applyReq()); err == nil || !strings.Contains(err.Error(), "close refused") {
		t.Fatalf("close failure: %v", err)
	}
}

// restoreSpoolSeams saves and restores the spool seams a test replaces.
func restoreSpoolSeams(t *testing.T) {
	t.Helper()
	open, create, free, closeFn := spoolOpen, spoolCreate, spoolFreeBytes, closeSpoolFile
	t.Cleanup(func() {
		spoolOpen, spoolCreate, spoolFreeBytes, closeSpoolFile = open, create, free, closeFn
	})
}

func TestVerifyRowFailures(t *testing.T) {
	ctx := context.Background()

	// Enumerating artifacts without a layout fails the call.
	w, _, _, _ := seeded(t)
	w.layoutCfg.RunPrefix = mustTemplate(t, "")
	if _, err := w.flow().Verify(ctx, VerifyRequest{Depth: "stored", Fetch: FetchRequest{RunID: "r"}}); err == nil ||
		!strings.Contains(err.Error(), "layout") {
		t.Fatalf("no layout: %v", err)
	}

	// A storage whose survey fails is skipped while enumerating.
	w, _, _, _ = seeded(t)
	off := w.addStorage("offsite")
	off.Fault = func(op storagetest.Op, _ string, _ int) error {
		if op == storagetest.OpList {
			return pluginerrors.Permanent(errors.New("listing refused"))
		}
		return nil
	}
	if _, err := w.flow().Verify(ctx, VerifyRequest{Depth: "stored", Fetch: FetchRequest{RunID: "r"}}); err != nil {
		t.Fatalf("enumerating past a failed survey: %v", err)
	}

	// A probed verification with an unreadable layout is a row failure.
	w, _, _, _ = seeded(t)
	w.layoutCfg.RunPrefix = mustTemplate(t, "")
	probe := &probeSource{fakeSource: &fakeSource{}}
	w.targets = &fakeTargets{target: Target{Type: "fake", Source: probe}}
	res, err := w.flow().Verify(ctx, VerifyRequest{Depth: "probed", Target: "target", Fetch: FetchRequest{RunID: "r", Artifact: "wireguard"}})
	if err != nil {
		t.Fatal(err)
	}
	if res[0].OK || !strings.Contains(res[0].Error, "layout") {
		t.Fatalf("bad layout row = %+v", res[0])
	}

	// The target's type disagrees with the manifest's record.
	w, _, _, _ = seeded(t)
	w.targets = &fakeTargets{target: Target{Type: "other", Source: probe}}
	res, err = w.flow().Verify(ctx, VerifyRequest{Depth: "probed", Target: "target", Fetch: FetchRequest{RunID: "r", Artifact: "wireguard"}})
	if err != nil {
		t.Fatal(err)
	}
	if res[0].OK || !strings.Contains(res[0].Error, "manifest's source") {
		t.Fatalf("type mismatch row = %+v", res[0])
	}

	// The recorded chain cannot be decoded here.
	w, _, _, key := seeded(t)
	rep := covManifest("r", "wireguard", key, []byte("artifact bytes"))
	rep.Artifacts[0].Chain = []model.ProcessorStep{{Instance: "ghost", Type: "fake"}}
	w.storage("primary").PutObject("runs/r/manifest.json", mustMarshal(t, rep), time.Unix(1700000000, 0).UTC())
	w.targets = &fakeTargets{target: Target{Type: "fake", Source: probe}}
	res, err = w.flow().Verify(ctx, VerifyRequest{Depth: "probed", Target: "target", Fetch: FetchRequest{RunID: "r", Artifact: "wireguard"}})
	if err != nil {
		t.Fatal(err)
	}
	if res[0].OK || !strings.Contains(res[0].Error, "does not have") {
		t.Fatalf("chain row = %+v", res[0])
	}

	// A probed verification that closes its target reports a close failure.
	w, _, _, _ = seeded(t)
	var buf strings.Builder
	w.targets = &fakeTargets{target: Target{Type: "fake", Source: probe}, closer: errCloser2{}}
	_, err = w.flow(func(o *Options) { o.Log = slog.New(slog.NewTextHandler(&buf, nil)) }).Verify(ctx,
		VerifyRequest{Depth: "probed", Target: "target", Fetch: FetchRequest{RunID: "r", Artifact: "wireguard"}})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "target.close.failed") {
		t.Fatalf("close failure not logged: %s", buf.String())
	}
}

func TestVerifyStoredRowReasons(t *testing.T) {
	ctx := context.Background()

	// The artifact is in no run at all.
	w, _, _, _ := seeded(t)
	res, err := w.flow().Verify(ctx, VerifyRequest{Depth: "stored", Fetch: FetchRequest{Newest: true, Artifact: "absent"}})
	if err != nil {
		t.Fatal(err)
	}
	if res[0].OK || !strings.Contains(res[0].Error, "no run on any storage") {
		t.Fatalf("absent artifact row = %+v", res[0])
	}

	// One storage's survey fails while another resolves the run.
	w, _, _, _ = seeded(t)
	off := w.addStorage("offsite")
	off.Fault = func(op storagetest.Op, _ string, _ int) error {
		if op == storagetest.OpList {
			return pluginerrors.Permanent(errors.New("listing refused"))
		}
		return nil
	}
	res, err = w.flow().Verify(ctx, VerifyRequest{Depth: "stored", Fetch: FetchRequest{Newest: true, Artifact: "wireguard"}})
	if err != nil {
		t.Fatal(err)
	}
	if !res[0].OK || res[0].Storage != "primary" {
		t.Fatalf("stored row = %+v", res[0])
	}

	// A run one storage lacks is that storage's reason, not a silent pass.
	w, _, _, _ = seeded(t)
	w.addStorage("offsite")
	res, err = w.flow().Verify(ctx, VerifyRequest{Depth: "stored", Fetch: FetchRequest{RunID: "r", Artifact: "wireguard"}})
	if err != nil {
		t.Fatal(err)
	}
	if !res[0].OK {
		t.Fatalf("a healthy copy did not verify: %+v", res[0])
	}
	res, err = w.flow().Verify(ctx, VerifyRequest{Depth: "stored", Fetch: FetchRequest{RunID: "absent", Artifact: "wireguard"}})
	if err != nil {
		t.Fatal(err)
	}
	if res[0].OK || !strings.Contains(res[0].Error, "not present") {
		t.Fatalf("missing run row = %+v", res[0])
	}

	// A same-size object whose bytes differ is a sha256 mismatch.
	w, _, _, key := seeded(t)
	w.storage("primary").PutObject(key, bytes.Repeat([]byte("z"), len([]byte("artifact bytes"))), time.Unix(1700000000, 0).UTC())
	res, err = w.flow().Verify(ctx, VerifyRequest{Depth: "stored", Fetch: FetchRequest{RunID: "r", Artifact: "wireguard"}})
	if err != nil {
		t.Fatal(err)
	}
	if res[0].OK || !strings.Contains(res[0].Error, "sha256") {
		t.Fatalf("sha mismatch row = %+v", res[0])
	}

	// A stat failure is a row reason.
	w, _, _, _ = seeded(t)
	w.replace("primary", &covStorage{Fake: w.storage("primary"), statErr: pluginerrors.Transient(errors.New("stat timeout"))})
	res, err = w.flow().Verify(ctx, VerifyRequest{Depth: "stored", Fetch: FetchRequest{RunID: "r", Artifact: "wireguard"}})
	if err != nil {
		t.Fatal(err)
	}
	if res[0].OK || !strings.Contains(res[0].Error, "stat timeout") {
		t.Fatalf("stat row = %+v", res[0])
	}
}

func TestFetchDestinationAndDecodeFailures(t *testing.T) {
	ctx := context.Background()

	// Writing the verified copy to the caller's destination fails.
	_, flow, _, _ := seeded(t)
	if _, err := flow.Fetch(ctx, FetchRequest{RunID: "r", Artifact: "wireguard"}, failingWriter{}); err == nil ||
		!strings.Contains(err.Error(), "destination") {
		t.Fatalf("destination failure: %v", err)
	}

	// A decode over the spool file fails.
	w, _, _, key := seeded(t)
	f := w.storage("primary")
	rep := covManifest("r", "wireguard", key, []byte("artifact bytes"))
	rep.Artifacts[0].Chain = []model.ProcessorStep{{Instance: "ghost", Type: "fake"}}
	f.PutObject("runs/r/manifest.json", mustMarshal(t, rep), time.Unix(1700000000, 0).UTC())
	if _, err := w.flow().Fetch(ctx, FetchRequest{RunID: "r", Artifact: "wireguard", Decode: true}, io.Discard); err == nil {
		t.Fatal("a chain this config cannot decode was accepted")
	}

	// The verified spool file cannot be reopened for the copy.
	w, _, _, _ = seeded(t)
	restoreSpoolSeams(t)
	spoolOpen = func(*spool.Spool, string) (*os.File, error) { return nil, errors.New("open refused") }
	if _, err := w.flow().Fetch(ctx, FetchRequest{RunID: "r", Artifact: "wireguard"}, io.Discard); err == nil ||
		!strings.Contains(err.Error(), "open refused") {
		t.Fatalf("open failure: %v", err)
	}
}

func TestFetchToFileDirectRefusals(t *testing.T) {
	ctx := context.Background()
	w, flow, _, _ := seeded(t)
	// A raw request asking for a decode is refused even here.
	if _, _, _, _, err := flow.fetchToFile(ctx, FetchRequest{RawStorage: "primary", RawKey: "k", Decode: true}, 0); err == nil ||
		!strings.Contains(err.Error(), "never decodes") {
		t.Fatalf("raw decode: %v", err)
	}
	// A spool that cannot be measured fails before the copy.
	restoreSpoolSeams(t)
	spoolFreeBytes = func(*spool.Spool) (int64, error) { return 0, errors.New("statfs failed") }
	if _, _, _, _, err := flow.fetchToFile(ctx, FetchRequest{RunID: "r", Artifact: "wireguard"}, 0); err == nil ||
		!strings.Contains(err.Error(), "statfs") {
		t.Fatalf("statfs failure: %v", err)
	}
	_ = w
}

func TestM9_FetchCandidatesDeterministicOrder(t *testing.T) {
	ctx := context.Background()

	// A storage whose survey failed is skipped, not fatal.
	w, flow, _, _ := seeded(t)
	off := w.addStorage("offsite")
	off.Fault = func(op storagetest.Op, _ string, _ int) error {
		if op == storagetest.OpList {
			return pluginerrors.Permanent(errors.New("listing refused"))
		}
		return nil
	}
	var out bytes.Buffer
	if _, err := w.flow().Fetch(ctx, FetchRequest{Newest: true, Artifact: "wireguard"}, &out); err != nil {
		t.Fatalf("newest with a failed storage: %v", err)
	}

	// Equal start times break the tie on the run id, newest first.
	sur := inventorySurveyForTie()
	got, err := flow.fetchCandidates(ctx, FetchRequest{Newest: true, Artifact: "wireguard"},
		[]*binding{flow.storageByName("primary")},
		func(*binding) (inventory.Survey, bool) { return sur, true })
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != "run-b" || got[1] != "run-a" {
		t.Fatalf("candidates %v", got)
	}
}

func TestStreamCopyDownloadVerdicts(t *testing.T) {
	ctx := context.Background()
	data := []byte("decoded artifact")
	ar := model.ArtifactResult{Key: "k", Bytes: int64(len(data)), RawBytes: int64(len(data)),
		SHA256: model.SHA256Hex(data), Chain: []model.ProcessorStep{{Instance: "zstd3", Type: "fake"}}}

	cases := []struct {
		name    string
		storage storage.Reader
		want    string
	}{
		{"get fails", &covStorage{Fake: storagetest.NewFake(), getErr: pluginerrors.Transient(errors.New("reset"))}, "download:"},
		{"short stream", &covStorage{Fake: storagetest.NewFake(), getData: data[:2]}, "downloaded"},
		{"long stream", &covStorage{Fake: storagetest.NewFake(), getData: append(append([]byte(nil), data...), 'x')}, "downloaded"},
		{"sha mismatch", &covStorage{Fake: storagetest.NewFake(), getData: bytes.Repeat([]byte("z"), len(data))}, "sha256"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := newWorld(t)
			w.addStorage("primary")
			w.replace("primary", tc.storage)
			flow := w.flow()
			_, err := streamCopy(ctx, flow.storageByName("primary").port, &ar, nil, discardProbe)
			var ce *copyError
			if !errors.As(err, &ce) || !ce.skip || !strings.Contains(ce.reason, tc.want) {
				t.Fatalf("verdict = %v", err)
			}
			if ce.Error() == "" {
				t.Fatal("copyError.Error() is empty")
			}
		})
	}

	// The same verdicts surface as a skip the walk passes over.
	w := newWorld(t)
	f := w.addStorage("primary")
	key := "runs/r/vpn/wireguard.dat.zst"
	seed(t, f, covManifest("r", "wireguard", key, data), key, data)
	f.Fault = func(op storagetest.Op, _ string, _ int) error {
		if op == storagetest.OpGet {
			return nil
		}
		return nil
	}
	// A decoded verification of a corrupt copy is that row's error.
	w.storage("primary").PutObject(key, bytes.Repeat([]byte("z"), len(data)), time.Unix(1700000000, 0).UTC())
	res, err := w.flow().Verify(ctx, VerifyRequest{Depth: "decoded", Fetch: FetchRequest{RunID: "r", Artifact: "wireguard"}})
	if err != nil {
		t.Fatal(err)
	}
	if res[0].OK || !strings.Contains(res[0].Error, "sha256") {
		t.Fatalf("corrupt decoded row = %+v", res[0])
	}
}

func TestSpaceGuardTinyHeadroom(t *testing.T) {
	restoreSpoolSeams(t)
	sp := testSpoolOnly(t)
	// Headroom too small for the half-budget rule: the next chunk sets the
	// budget to the chunk itself.
	spoolFreeBytes = func(*spool.Spool) (int64, error) { return 2, nil }
	g := &spaceGuard{w: &countingCloser{}, sp: sp, reserve: 0}
	if _, err := g.Write([]byte("abcd")); err == nil {
		t.Fatal("a four-byte write into two bytes of headroom was accepted")
	}
}

func inventorySurveyForTie() inventory.Survey {
	same := time.Unix(1700000000, 0).UTC()
	repA := covManifest("run-a", "wireguard", "ka", []byte("x"))
	repB := covManifest("run-b", "wireguard", "kb", []byte("x"))
	repA.StartedAt, repB.StartedAt = same, same
	return inventory.Survey{OK: true, Runs: map[string]inventory.Run{
		"run-a": {Manifest: repA, Stored: model.StoredRun{RunID: "run-a", Kind: model.RunKindScheduled}},
		"run-b": {Manifest: repB, Stored: model.StoredRun{RunID: "run-b", Kind: model.RunKindScheduled}},
	}}
}

func TestApplyOpenSeekAndDecodeRuntimeFailures(t *testing.T) {
	ctx := context.Background()
	open, create, free, closeFn, seek := spoolOpen, spoolCreate, spoolFreeBytes, closeSpoolFile, fileSeek
	reset := func() {
		spoolOpen, spoolCreate, spoolFreeBytes, closeSpoolFile, fileSeek = open, create, free, closeFn, seek
	}
	t.Cleanup(reset)

	// The decoded spool file cannot be reopened for the restorer.
	w, _, _, _ := seeded(t)
	w.targets = &fakeTargets{target: Target{Type: "fake", Source: &restorerSource{fakeSource: &fakeSource{}}}}
	calls := 0
	spoolOpen = func(sp *spool.Spool, name string) (*os.File, error) {
		calls++
		if calls > 1 {
			return nil, errors.New("open refused")
		}
		return open(sp, name)
	}
	if _, err := w.flow().Apply(ctx, applyReq()); err == nil || !strings.Contains(err.Error(), "open refused") {
		t.Fatalf("decoded open failure: %v", err)
	}

	// An in-place apply whose rewind fails ends before the write.
	reset()
	w, _, _, _ = seeded(t)
	probe := &probeSource{fakeSource: &fakeSource{}}
	w.targets = &fakeTargets{target: Target{Type: "fake", Source: probe}}
	fileSeek = func(*os.File, int64, int) (int64, error) { return 0, errors.New("seek refused") }
	req := applyReq()
	req.Mode = source.ApplyInPlace
	req.Confirm = "test://fake/source"
	if _, err := w.flow().Apply(ctx, req); err == nil || !strings.Contains(err.Error(), "seek refused") {
		t.Fatalf("seek failure: %v", err)
	}

	// A decode that fails at runtime is the apply's refusal.
	reset()
	w, _, _, _ = seeded(t)
	w.processor("zstd3").decodeErr = errors.New("corrupt stream")
	w.targets = &fakeTargets{target: Target{Type: "fake", Source: &restorerSource{fakeSource: &fakeSource{}}}}
	if _, err := w.flow().Apply(ctx, applyReq()); err == nil || !strings.Contains(err.Error(), "corrupt stream") {
		t.Fatalf("runtime decode failure: %v", err)
	}

	// A guard refusal during the decode write is the decode's verdict.
	reset()
	w, _, _, _ = seeded(t)
	w.targets = &fakeTargets{target: Target{Type: "fake", Source: &restorerSource{fakeSource: &fakeSource{}}}}
	n := 0
	spoolFreeBytes = func(*spool.Spool) (int64, error) {
		n++
		if n <= 3 { // fetch pre-check, copy pre-check, copy guard
			return 1 << 30, nil
		}
		return 0, nil
	}
	if _, err := w.flow().Apply(ctx, applyReq()); err == nil || !strings.Contains(err.Error(), "restore spool") {
		t.Fatalf("decode guard refusal: %v", err)
	}
}

func TestPrefixFailuresOnEveryFetchPath(t *testing.T) {
	ctx := context.Background()
	w, _, _, _ := seeded(t)
	w.layoutCfg.RunPrefix = mustTemplate(t, "")
	flow := w.flow()

	if _, _, _, _, err := flow.fetchArtifact(ctx, FetchRequest{RunID: "r", Artifact: "wireguard"}, testSpoolOnly(t), 0); err == nil ||
		!strings.Contains(err.Error(), "layout") {
		t.Fatalf("fetchArtifact prefix: %v", err)
	}
	if _, err := flow.ResolveFetch(ctx, FetchRequest{RunID: "r", Artifact: "wireguard"}); err == nil ||
		!strings.Contains(err.Error(), "layout") {
		t.Fatalf("ResolveFetch prefix: %v", err)
	}
	res, err := flow.Verify(ctx, VerifyRequest{Depth: "stored", Fetch: FetchRequest{RunID: "r", Artifact: "wireguard"}})
	if err != nil {
		t.Fatal(err)
	}
	if res[0].OK || !strings.Contains(res[0].Error, "layout") {
		t.Fatalf("verifyStored prefix row = %+v", res[0])
	}
}

func TestVerifyEnumerationSkipsAFailedSurvey(t *testing.T) {
	ctx := context.Background()
	w, _, _, _ := seeded(t)
	off := w.addStorage("offsite")
	off.Fault = func(op storagetest.Op, _ string, _ int) error {
		if op == storagetest.OpList {
			return pluginerrors.Permanent(errors.New("listing refused"))
		}
		return nil
	}
	w.order = []string{"offsite", "primary"}
	if _, err := w.flow().Verify(ctx, VerifyRequest{Depth: "stored", Fetch: FetchRequest{RunID: "r"}}); err != nil {
		t.Fatalf("enumeration past a failed survey: %v", err)
	}
	// The same order reaches the stored row's own "survey failed" reason while
	// the healthy storage still answers.
	res, err := w.flow().Verify(ctx, VerifyRequest{Depth: "stored", Fetch: FetchRequest{Newest: true, Artifact: "wireguard"}})
	if err != nil {
		t.Fatal(err)
	}
	if !res[0].OK || res[0].Storage != "primary" {
		t.Fatalf("stored row with a failed first storage = %+v", res[0])
	}
}

func TestSpaceGuardHalfBudgetClamp(t *testing.T) {
	restoreSpoolSeams(t)
	sp := testSpoolOnly(t)
	spoolFreeBytes = func(*spool.Spool) (int64, error) { return 3, nil }
	g := &spaceGuard{w: &countingCloser{}, sp: sp, reserve: 0}
	if n, err := g.Write([]byte("ab")); err != nil || n != 2 {
		t.Fatalf("clamped write = %d, %v", n, err)
	}
}
