package restore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/source"
)

// applyWorld seeds one ok run on a storage and a workflow whose target
// resolver answers with the given source.
func applyWorld(t *testing.T, res source.Source, closer io.Closer, resolveErr error) (*testWorld, *Workflow, []byte) {
	t.Helper()
	w := newWorld(t)
	f := w.addStorage("primary")
	data := []byte("artifact bytes")
	key := "runs/r/vpn/wireguard.dat.zst"
	seed(t, f, covManifest("r", "wireguard", key, data), key, data)
	w.targets = &fakeTargets{target: Target{Type: "fake", Source: res}, closer: closer, err: resolveErr}
	return w, w.flow(), data
}

func applyReq() ApplyRunRequest {
	return ApplyRunRequest{
		Fetch:  FetchRequest{RunID: "r", Artifact: "wireguard"},
		Target: "target",
		Mode:   source.ApplySideBySide,
	}
}

func TestApplyRefusals(t *testing.T) {
	ctx := context.Background()

	// A mode is required and must be one of the two.
	_, flow, _ := applyWorld(t, &fakeSource{}, nil, nil)
	if _, err := flow.Apply(ctx, ApplyRunRequest{}); err == nil || !strings.Contains(err.Error(), "Mode") {
		t.Fatalf("missing mode: %v", err)
	}
	raw := applyReq()
	raw.Fetch = FetchRequest{RawStorage: "primary", RawKey: "k"}
	if _, err := flow.Apply(ctx, raw); err == nil || !strings.Contains(err.Error(), "raw by-key") {
		t.Fatalf("raw apply: %v", err)
	}
	noTarget := applyReq()
	noTarget.Target = ""
	if _, err := flow.Apply(ctx, noTarget); err == nil || !strings.Contains(err.Error(), "Target is required") {
		t.Fatalf("missing target: %v", err)
	}

	// The configuration declares no targets.
	w := newWorld(t)
	nf := w.addStorage("primary")
	ndata := []byte("artifact bytes")
	nkey := "runs/r/vpn/wireguard.dat.zst"
	seed(t, nf, covManifest("r", "wireguard", nkey, ndata), nkey, ndata)
	if _, err := w.flow().Apply(ctx, applyReq()); err == nil || !strings.Contains(err.Error(), "no targets") {
		t.Fatalf("no targets: %v", err)
	}
	_ = flow

	// The resolver's own error.
	_, flow, _ = applyWorld(t, nil, nil, errors.New("no discovered source"))
	if _, err := flow.Apply(ctx, applyReq()); err == nil || !strings.Contains(err.Error(), "no discovered") {
		t.Fatalf("resolver error: %v", err)
	}

	// Describing the target fails.
	_, flow, _ = applyWorld(t, &fakeSource{describeErr: errors.New("unreachable")}, nil, nil)
	if _, err := flow.Apply(ctx, applyReq()); err == nil || !strings.Contains(err.Error(), "describing the target") {
		t.Fatalf("describe error: %v", err)
	}

	// The target's type disagrees with the manifest's record.
	otherType := &fakeTargets{target: Target{Type: "other", Source: &fakeSource{}}}
	w2 := newWorld(t)
	f2 := w2.addStorage("primary")
	data := []byte("x")
	key := "runs/r/vpn/wireguard.dat.zst"
	seed(t, f2, covManifest("r", "wireguard", key, data), key, data)
	w2.targets = otherType
	if _, err := w2.flow().Apply(ctx, applyReq()); err == nil || !strings.Contains(err.Error(), "manifest's source") {
		t.Fatalf("type mismatch: %v", err)
	}

	// A foreign origin is refused unless explicitly allowed.
	foreign := &fakeSource{traits: source.SourceTraits{Origin: "test://other/origin"}}
	_, flow, _ = applyWorld(t, foreign, nil, nil)
	if _, err := flow.Apply(ctx, applyReq()); err == nil || !strings.Contains(err.Error(), "differs from the target's") {
		t.Fatalf("foreign origin: %v", err)
	}

	// A target without a restorer is fetch-only.
	restorerless := &fakeSource{}
	_, flow, _ = applyWorld(t, restorerless, nil, nil)
	if _, err := flow.Apply(ctx, applyReq()); err == nil || !strings.Contains(err.Error(), "no restorer") {
		t.Fatalf("restorerless: %v", err)
	}
}

func TestApplyInPlaceChecks(t *testing.T) {
	ctx := context.Background()
	req := applyReq()
	req.Mode = source.ApplyInPlace

	// An in-place apply needs the origin verbatim as confirmation.
	proberless := &restorerSource{fakeSource: &fakeSource{}}
	_, flow, _ := applyWorld(t, proberless, nil, nil)
	if _, err := flow.Apply(ctx, req); err == nil || !strings.Contains(err.Error(), "Confirm") {
		t.Fatalf("missing confirmation: %v", err)
	}
	req.Confirm = "test://other/origin"
	if _, err := flow.Apply(ctx, req); err == nil || !strings.Contains(err.Error(), "Confirm") {
		t.Fatalf("wrong confirmation: %v", err)
	}

	// A confirm that matches still needs a Prober for the pre-write check.
	req.Confirm = "test://fake/source"
	if _, err := flow.Apply(ctx, req); err == nil || !strings.Contains(err.Error(), "no Prober") {
		t.Fatalf("in-place without a prober: %v", err)
	}
}

func TestApplySuccessPaths(t *testing.T) {
	ctx := context.Background()

	// Side by side: the decoded bytes reach the restorer and its report returns.
	var seen bytes.Buffer
	rest := &restorerSource{fakeSource: &fakeSource{}, rep: source.ApplyReport{Target: "t", Previous: "p", Bytes: 7}, dump: &seen}
	_, flow, data := applyWorld(t, rest, nil, nil)
	rep, err := flow.Apply(ctx, applyReq())
	if err != nil {
		t.Fatal(err)
	}
	if rep.Target != "t" || !bytes.Equal(seen.Bytes(), data) {
		t.Fatalf("apply = %+v, %q", rep, seen.Bytes())
	}

	// In place: the probe runs first and the stream is rewound for the write.
	probe := &probeSource{fakeSource: &fakeSource{}, probeRep: source.ProbeReport{Entries: 2, Bytes: 3}, dump: &seen}
	seen.Reset()
	req := applyReq()
	req.Mode = source.ApplyInPlace
	req.Confirm = "test://fake/source"
	_, flow, _ = applyWorld(t, probe, nil, nil)
	if _, err := flow.Apply(ctx, req); err != nil {
		t.Fatal(err)
	}
	if probe.probes != 1 || !bytes.Equal(seen.Bytes(), []byte("artifact bytes")) {
		t.Fatalf("in-place probe %d, bytes %q", probe.probes, seen.Bytes())
	}
}

func TestApplyProbeFailure(t *testing.T) {
	ctx := context.Background()
	probe := &probeSource{fakeSource: &fakeSource{}, probeErr: errors.New("probe refused")}
	_, flow, _ := applyWorld(t, probe, nil, nil)
	req := applyReq()
	req.Mode = source.ApplyInPlace
	req.Confirm = "test://fake/source"
	if _, err := flow.Apply(ctx, req); err == nil || !strings.Contains(err.Error(), "probe before an in-place write") {
		t.Fatalf("probe failure: %v", err)
	}
}

func TestApplyTargetCloserErrorIsLogged(t *testing.T) {
	ctx := context.Background()
	var buf strings.Builder
	w := newWorld(t)
	f := w.addStorage("primary")
	data := []byte("x")
	key := "runs/r/vpn/wireguard.dat.zst"
	seed(t, f, covManifest("r", "wireguard", key, data), key, data)
	w.targets = &fakeTargets{target: Target{Type: "fake", Source: &restorerSource{fakeSource: &fakeSource{}}},
		closer: errCloser2{}}
	flow := w.flow(func(o *Options) { o.Log = slog.New(slog.NewTextHandler(&buf, nil)) })
	if _, err := flow.Apply(ctx, applyReq()); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "target.close.failed") {
		t.Fatalf("close failure not logged: %s", buf.String())
	}
}

type errCloser2 struct{}

func (errCloser2) Close() error { return errors.New("close failed") }

func TestApplyDecodeFailureBeforeTheTarget(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t)
	f := w.addStorage("primary")
	data := []byte("x")
	key := "runs/r/vpn/wireguard.dat.zst"
	seed(t, f, covManifest("r", "wireguard", key, data), key, data)
	// The manifest records a chain step this configuration lacks.
	rep := covManifest("r", "wireguard", key, data)
	rep.Artifacts[0].Chain = []model.ProcessorStep{{Instance: "ghost", Type: "fake"}}
	f.PutObject("runs/r/manifest.json", mustMarshal(t, rep), time.Unix(1700000000, 0).UTC())
	w.targets = &fakeTargets{target: Target{Type: "fake", Source: &restorerSource{fakeSource: &fakeSource{}}}}
	if _, err := w.flow().Apply(ctx, applyReq()); err == nil || !strings.Contains(err.Error(), "does not have") {
		t.Fatalf("decode failure: %v", err)
	}
}

// --- Verify ------------------------------------------------------------------

func TestVerifyRequestRefusals(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t)
	w.addStorage("primary")
	flow := w.flow()
	if _, err := flow.Verify(ctx, VerifyRequest{Depth: "deep"}); err == nil || !strings.Contains(err.Error(), "Depth") {
		t.Fatalf("bad depth: %v", err)
	}
	if _, err := flow.Verify(ctx, VerifyRequest{Depth: "probed"}); err == nil || !strings.Contains(err.Error(), "needs a Target") {
		t.Fatalf("probed without a target: %v", err)
	}
	if _, err := flow.Verify(ctx, VerifyRequest{Depth: "stored", Fetch: FetchRequest{Storages: []string{"ghost"}}}); err == nil {
		t.Fatal("unknown storage accepted")
	}
	// Enumerating needs an exact run.
	if _, err := flow.Verify(ctx, VerifyRequest{Depth: "stored", Fetch: FetchRequest{Newest: true}}); err == nil ||
		!strings.Contains(err.Error(), "cannot enumerate") {
		t.Fatalf("newest enumeration: %v", err)
	}
	// A run no storage has a manifest for.
	if _, err := flow.Verify(ctx, VerifyRequest{Depth: "stored", Fetch: FetchRequest{RunID: "absent"}}); err == nil ||
		!strings.Contains(err.Error(), "no storage has a manifest") {
		t.Fatalf("manifest-less run: %v", err)
	}
}

func TestVerifyEnumeratesArtifacts(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t)
	f := w.addStorage("primary")
	data := []byte("x")
	key := "runs/r/vpn/wireguard.dat.zst"
	seed(t, f, covManifest("r", "wireguard", key, data), key, data)
	res, err := w.flow().Verify(ctx, VerifyRequest{Depth: "stored", Fetch: FetchRequest{RunID: "r"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 1 || !res[0].OK || res[0].Artifact != "wireguard" {
		t.Fatalf("verify = %+v", res)
	}
}

func TestVerifyStoredNewestFallsThrough(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t)
	f := w.addStorage("primary")
	data := []byte("artifact bytes")
	oldKey := "runs/20990101T000000Z/vpn/wireguard.dat.zst"
	seed(t, f, covManifest("20990101T000000Z", "wireguard", oldKey, data), oldKey, data)
	newer := covManifest("20990101T000100Z", "wireguard", "runs/20990101T000100Z/vpn/wireguard.dat.zst", data)
	newer.StartedAt = newer.StartedAt.Add(time.Hour)
	f.PutObject("runs/20990101T000100Z/manifest.json", mustMarshal(t, newer), time.Unix(1700000000, 0).UTC())

	res, err := w.flow().Verify(ctx, VerifyRequest{Depth: "stored", Fetch: FetchRequest{Newest: true, Artifact: "wireguard"}})
	if err != nil {
		t.Fatal(err)
	}
	if !res[0].OK || res[0].RunID != "20990101T000000Z" {
		t.Fatalf("stored newest = %+v", res[0])
	}

	// A size disagreement is a row failure, never a silent pass.
	stale := covManifest("20990101T000200Z", "wireguard", "runs/20990101T000200Z/vpn/wireguard.dat.zst", data)
	stale.StartedAt = stale.StartedAt.Add(2 * time.Hour)
	f.PutObject("runs/20990101T000200Z/manifest.json", mustMarshal(t, stale), time.Unix(1700000000, 0).UTC())
	f.PutObject(stale.Artifacts[0].Key, []byte("short"), time.Unix(1700000000, 0).UTC())
	res, err = w.flow().Verify(ctx, VerifyRequest{Depth: "stored", Fetch: FetchRequest{RunID: "20990101T000200Z", Artifact: "wireguard"}})
	if err != nil {
		t.Fatal(err)
	}
	if res[0].OK || !strings.Contains(res[0].Error, "size") {
		t.Fatalf("missing object row = %+v", res[0])
	}
}

func TestVerifyStreamDepths(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t)
	f := w.addStorage("primary")
	data := []byte("artifact bytes")
	key := "runs/r/vpn/wireguard.dat.zst"
	seed(t, f, covManifest("r", "wireguard", key, data), key, data)
	flow := w.flow()

	res, err := flow.Verify(ctx, VerifyRequest{Depth: "decoded", Fetch: FetchRequest{RunID: "r", Artifact: "wireguard"}})
	if err != nil {
		t.Fatal(err)
	}
	if !res[0].OK || res[0].Bytes != int64(len(data)) || res[0].ApplySpoolBytes != int64(2*len(data)) {
		t.Fatalf("decoded verify = %+v", res[0])
	}

	// Probed asks the target's Prober and reports what it read.
	probe := &probeSource{fakeSource: &fakeSource{}, probeRep: source.ProbeReport{Entries: 1, Bytes: 4}}
	w.targets = &fakeTargets{target: Target{Type: "fake", Source: probe}}
	res, err = w.flow().Verify(ctx, VerifyRequest{Depth: "probed", Target: "target", Fetch: FetchRequest{RunID: "r", Artifact: "wireguard"}})
	if err != nil {
		t.Fatal(err)
	}
	if !res[0].OK || res[0].Bytes != 4 || probe.probes != 1 {
		t.Fatalf("probed verify = %+v", res[0])
	}

	// A target without a Prober is that row's failure.
	restorer := &restorerSource{fakeSource: &fakeSource{}}
	w.targets = &fakeTargets{target: Target{Type: "fake", Source: restorer}}
	res, err = w.flow().Verify(ctx, VerifyRequest{Depth: "probed", Target: "target", Fetch: FetchRequest{RunID: "r", Artifact: "wireguard"}})
	if err != nil {
		t.Fatal(err)
	}
	if res[0].OK || !strings.Contains(res[0].Error, "no Prober") {
		t.Fatalf("proberless row = %+v", res[0])
	}

	// The resolver's failure is that row's failure too.
	w.targets = &fakeTargets{err: errors.New("no target")}
	res, err = w.flow().Verify(ctx, VerifyRequest{Depth: "probed", Target: "target", Fetch: FetchRequest{RunID: "r", Artifact: "wireguard"}})
	if err != nil {
		t.Fatal(err)
	}
	if res[0].OK || !strings.Contains(res[0].Error, "no target") {
		t.Fatalf("resolver row = %+v", res[0])
	}

	// A probe that panics is a failure, not a process crash.
	panicky := &probeSource{fakeSource: &fakeSource{}, probePanic: true}
	w.targets = &fakeTargets{target: Target{Type: "fake", Source: panicky}}
	res, err = w.flow().Verify(ctx, VerifyRequest{Depth: "probed", Target: "target", Fetch: FetchRequest{RunID: "r", Artifact: "wireguard"}})
	if err != nil {
		t.Fatal(err)
	}
	if res[0].OK || !strings.Contains(res[0].Error, "plugin panic") {
		t.Fatalf("panicking probe row = %+v", res[0])
	}
}
