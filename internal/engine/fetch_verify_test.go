package engine

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/discovery"
	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/source"
	"github.com/vi-latyshev/go-cluster-backup/internal/spool"
	"github.com/vi-latyshev/go-cluster-backup/internal/storage/storagetest"
)

func TestVerifyRefusals(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t)
	e := w.newEngine(t, withRestore(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}), "primary"))

	if _, err := e.Verify(ctx, VerifyRequest{Fetch: FetchRequest{RunID: "r", Artifact: "a", Storages: []string{"ghost"}}, Depth: "stored"}); err == nil {
		t.Fatal("Verify accepted an unknown storage")
	}
	// Neither an artifact nor a run id to enumerate.
	if _, err := e.Verify(ctx, VerifyRequest{Fetch: FetchRequest{}, Depth: "stored"}); err == nil ||
		!strings.Contains(err.Error(), "Artifact or RunID") {
		t.Fatalf("Verify without an artifact or run id: %v", err)
	}
}

func TestVerifyArtifactsRefusals(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t)
	e := w.newEngine(t, withRestore(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}), "primary"))

	// No layout section.
	noLayout := withRestore(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}), "primary")
	noLayout["layout"] = map[string]any{"run_prefix": "", "artifact_key": "a", "manifest_key": "manifest.json", "manual_id_suffix": "-manual"}
	el := w.newEngine(t, noLayout)
	if _, err := el.Verify(ctx, VerifyRequest{Fetch: FetchRequest{RunID: "r"}, Depth: "stored"}); err == nil ||
		!strings.Contains(err.Error(), "layout") {
		t.Fatalf("no layout section: %v", err)
	}

	// A run prefix that is not a literal plus {{ .Run.ID }}.
	badPrefix := withRestore(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}), "primary")
	badPrefix["layout"] = map[string]any{"run_prefix": "{{ .Run.ID }}", "artifact_key": "a", "manifest_key": "manifest.json", "manual_id_suffix": "-manual"}
	ep := w.newEngine(t, badPrefix)
	if _, err := ep.Verify(ctx, VerifyRequest{Fetch: FetchRequest{RunID: "r"}, Depth: "stored"}); err == nil ||
		!strings.Contains(err.Error(), "run_prefix") {
		t.Fatalf("bad run prefix: %v", err)
	}

	// No storage holds a manifest for the run.
	if _, err := e.Verify(ctx, VerifyRequest{Fetch: FetchRequest{RunID: "20990101T000000Z"}, Depth: "stored"}); err == nil ||
		!strings.Contains(err.Error(), "no storage has a manifest") {
		t.Fatalf("a run no storage holds: %v", err)
	}

	// A storage that cannot be surveyed is skipped, and the search continues.
	w.storage(t, "primary").Fault = func(op storagetest.Op, _ string, _ int) error {
		if op == storagetest.OpList {
			return plugin.Permanent(errors.New("listing refused"))
		}
		return nil
	}
	if _, err := e.Verify(ctx, VerifyRequest{Fetch: FetchRequest{RunID: "20990101T000000Z"}, Depth: "stored"}); err == nil {
		t.Fatal("a survey failure was not surfaced as a missing manifest")
	}
}

func TestVerifyOneErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("decoded fetch fails", func(t *testing.T) {
		w := newWorld(t)
		cfg := withRestore(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}), "primary")
		e := runOnce(t, w, cfg)
		res, err := e.Verify(ctx, VerifyRequest{Fetch: FetchRequest{RunID: "20990101T000000Z", Artifact: "wireguard"}, Depth: "decoded"})
		if err != nil {
			t.Fatal(err)
		}
		if len(res) != 1 || res[0].OK || res[0].Error == "" {
			t.Fatalf("decoded verify %+v", res)
		}
	})

	t.Run("decoded chain fails", func(t *testing.T) {
		w := newWorld(t)
		cfg := withRestore(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}), "primary")
		e := runOnce(t, w, cfg)
		runs, _ := e.StoredRuns(ctx, StoredRunsRequest{})
		delete(e.processors, "zstd3")
		res, err := e.Verify(ctx, VerifyRequest{Fetch: FetchRequest{RunID: runs[0].RunID, Artifact: "wireguard"}, Depth: "decoded"})
		if err != nil {
			t.Fatal(err)
		}
		if len(res) != 1 || res[0].OK || !strings.Contains(res[0].Error, "does not have") {
			t.Fatalf("decoded verify %+v", res)
		}
	})

	t.Run("probed fetch fails", func(t *testing.T) {
		w := newWorld(t)
		target := &fakeRestorer{}
		w.extraSources["target"] = target
		cfg := applyConfig(t, w, "target", nil)
		e := runOnce(t, w, cfg)
		res, err := e.Verify(ctx, VerifyRequest{
			Fetch: FetchRequest{RunID: "20990101T000000Z", Artifact: "wireguard"}, Depth: "probed", Target: "target"})
		if err != nil {
			t.Fatal(err)
		}
		if len(res) != 1 || res[0].OK || res[0].Error == "" {
			t.Fatalf("probed verify %+v", res)
		}
	})

	t.Run("probed target unresolved", func(t *testing.T) {
		w := newWorld(t)
		target := &fakeRestorer{}
		w.extraSources["target"] = target
		cfg := applyConfig(t, w, "target", nil)
		e := runOnce(t, w, cfg)
		runs, _ := e.StoredRuns(ctx, StoredRunsRequest{})
		res, err := e.Verify(ctx, VerifyRequest{
			Fetch: FetchRequest{RunID: runs[0].RunID, Artifact: "wireguard"}, Depth: "probed", Target: "ghost"})
		if err != nil {
			t.Fatal(err)
		}
		if len(res) != 1 || res[0].OK || !strings.Contains(res[0].Error, "unknown target") {
			t.Fatalf("probed verify %+v", res)
		}
	})

	t.Run("probed target type mismatch", func(t *testing.T) {
		w := newWorld(t)
		target := &fakeRestorer{}
		w.extraSources["target"] = target
		w.registerType(t, "source", "other")
		cfg := applyConfig(t, w, "target", func(cfg map[string]any) {
			cfg["sources"].(map[string]any)["target"] = sourceInstance("other", "targets", "restore-target", []string{"zstd3"}, "required", "plain")
		})
		e := runOnce(t, w, cfg)
		runs, _ := e.StoredRuns(ctx, StoredRunsRequest{})
		res, err := e.Verify(ctx, VerifyRequest{
			Fetch: FetchRequest{RunID: runs[0].RunID, Artifact: "wireguard"}, Depth: "probed", Target: "target"})
		if err != nil {
			t.Fatal(err)
		}
		if len(res) != 1 || res[0].OK || !strings.Contains(res[0].Error, "type") {
			t.Fatalf("probed verify %+v", res)
		}
	})

	t.Run("probed target has no prober", func(t *testing.T) {
		w := newWorld(t)
		w.extraSources["target"] = &fakeSource{}
		cfg := applyConfig(t, w, "target", nil)
		e := runOnce(t, w, cfg)
		runs, _ := e.StoredRuns(ctx, StoredRunsRequest{})
		res, err := e.Verify(ctx, VerifyRequest{
			Fetch: FetchRequest{RunID: runs[0].RunID, Artifact: "wireguard"}, Depth: "probed", Target: "target"})
		if err != nil {
			t.Fatal(err)
		}
		if len(res) != 1 || res[0].OK || !strings.Contains(res[0].Error, "Prober") {
			t.Fatalf("probed verify %+v", res)
		}
	})

	t.Run("probe fails", func(t *testing.T) {
		w := newWorld(t)
		target := &fakeRestorer{failProbe: errors.New("not a valid archive")}
		w.extraSources["target"] = target
		cfg := applyConfig(t, w, "target", nil)
		e := runOnce(t, w, cfg)
		runs, _ := e.StoredRuns(ctx, StoredRunsRequest{})
		res, err := e.Verify(ctx, VerifyRequest{
			Fetch: FetchRequest{RunID: runs[0].RunID, Artifact: "wireguard"}, Depth: "probed", Target: "target"})
		if err != nil {
			t.Fatal(err)
		}
		if len(res) != 1 || res[0].OK || !strings.Contains(res[0].Error, "archive") {
			t.Fatalf("probed verify %+v", res)
		}
	})

}

// TestVerifyProbedClosesTheFailedTarget: a probed verify's discovered target
// that fails to close is reported.
func TestVerifyProbedClosesTheFailedTarget(t *testing.T) {
	w := newWorld(t)
	target := &errCloseRestorer{}
	w.extraSources["found"] = target
	cfg := applyConfig(t, w, "target", func(cfg map[string]any) {
		cfg["discoveries"] = map[string]any{"workloads": instance("fake")}
	})
	e1 := runOnce(t, w, cfg)
	runs, err := e1.StoredRuns(context.Background(), StoredRunsRequest{})
	if err != nil || len(runs) != 1 {
		t.Fatalf("stored runs %v, %v", runs, err)
	}
	w.discoveries["workloads"] = &fakeDiscovery{found: []discovery.Found{{
		Name: "found", Type: "fake", Config: []byte(`{}`),
		Artifact: model.Artifact{Group: "vpn", Name: "wireguard", Importance: model.ImportanceRequired, Sensitivity: model.SensitivityPlain},
		Pipeline: []string{"zstd3"},
	}}}
	e2 := w.newEngine(t, cfg)
	res, err := e2.Verify(context.Background(), VerifyRequest{
		Fetch: FetchRequest{RunID: runs[0].RunID, Artifact: "wireguard"}, Depth: "probed", Target: "discovered"})
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 1 || !res[0].OK {
		t.Fatalf("probed verify %+v", res)
	}
}

// headerProber refuses a stream whose first byte is a broken header, before it
// has read the whole copy: the consumer error a corrupt object can raise early.
type headerProber struct{ fakeSource }

func (p *headerProber) Probe(ctx context.Context, src io.Reader) (source.ProbeReport, error) {
	if err := ctx.Err(); err != nil {
		return source.ProbeReport{}, err
	}
	var b [1]byte
	n, err := src.Read(b[:])
	if err != nil && !errors.Is(err, io.EOF) {
		return source.ProbeReport{}, err
	}
	if n > 0 && b[0] == 'z' {
		return source.ProbeReport{}, errors.New("invalid archive header")
	}
	rest, err := io.ReadAll(src)
	if err != nil {
		return source.ProbeReport{}, err
	}
	return source.ProbeReport{Entries: 1, Bytes: int64(n + len(rest))}, nil
}

// TestVerifyProbedFallsThroughACorruptCopy: a copy whose bytes are wrong is a
// skip, even when the consumer refused it before EOF and with an error of its
// own. The stream's verdict comes first; a healthy copy on another storage
// still answers the check.
func TestVerifyProbedFallsThroughACorruptCopy(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t)
	w.extraSources["target"] = &headerProber{}
	// No processor chain: the stored bytes are the probed bytes, so a corrupt
	// copy reaches the Prober's header check.
	cfg := withRestore(t, baseConfig(t, twoStorages()), "primary", "offsite")
	cfg["sources"] = map[string]any{
		"vpn":    sourceInstance("fake", "vpn", "wireguard", nil, "required", "plain"),
		"target": sourceInstance("fake", "targets", "restore-target", nil, "required", "plain"),
	}
	e := runOnce(t, w, cfg)
	runs, err := e.StoredRuns(ctx, StoredRunsRequest{})
	if err != nil || len(runs) == 0 {
		t.Fatalf("stored runs %v, %v", runs, err)
	}
	ar := findArtifact(&runs[0].Report, "wireguard")
	if ar == nil {
		t.Fatalf("the manifest does not record wireguard: %+v", runs[0].Report.Artifacts)
	}
	w.storage(t, "primary").PutObject(ar.Key, bytes.Repeat([]byte{'z'}, int(ar.Bytes)), testTime())

	got, err := e.Verify(ctx, VerifyRequest{
		Fetch: FetchRequest{RunID: runs[0].RunID, Artifact: "wireguard"}, Depth: "probed", Target: "target",
	})
	if err != nil || len(got) != 1 || !got[0].OK || got[0].Storage != "offsite" {
		t.Fatalf("a corrupt primary prevented the healthy offsite copy: %+v, %v", got, err)
	}
}

// TestVerifyStreamNeedsNoSpool: decoded and probed verify stream from the
// storage, so an unreadable, full or absent restore spool cannot affect them —
// and the row says what an apply would need.
func TestVerifyStreamNeedsNoSpool(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t)
	w.extraSources["target"] = &fakeRestorer{}
	cfg := applyConfig(t, w, "target", nil)
	e := runOnce(t, w, cfg)
	runs, err := e.StoredRuns(ctx, StoredRunsRequest{})
	if err != nil || len(runs) != 1 {
		t.Fatalf("stored runs %v, %v", runs, err)
	}
	ar := findArtifact(&runs[0].Report, "wireguard")
	if ar == nil {
		t.Fatalf("the manifest does not record wireguard: %+v", runs[0].Report.Artifacts)
	}
	spoolDir := e.restoreSpoolDir
	e.restoreSpoolDir = ""
	orig := spoolFreeBytes
	spoolFreeBytes = func(*spool.Spool) (int64, error) { return 0, errors.New("statfs: io error") }
	t.Cleanup(func() { spoolFreeBytes = orig })

	fetch := FetchRequest{RunID: runs[0].RunID, Artifact: "wireguard"}
	for _, req := range []VerifyRequest{
		{Fetch: fetch, Depth: "decoded"},
		{Fetch: fetch, Depth: "probed", Target: "target"},
	} {
		res, err := e.Verify(ctx, req)
		if err != nil || len(res) != 1 || !res[0].OK {
			t.Fatalf("verify %s without a spool = %+v, %v", req.Depth, res, err)
		}
		if want := ar.Bytes + ar.RawBytes; res[0].ApplySpoolBytes != want {
			t.Fatalf("verify %s reports apply spool %d, want %d", req.Depth, res[0].ApplySpoolBytes, want)
		}
	}
	if entries, err := readDirNames(spoolDir); err != nil || len(entries) != 0 {
		t.Fatalf("the restore spool holds %v, %v", entries, err)
	}
}

// TestVerifyStreamLayout: a streaming check needs the same layout a survey does
// and refuses it the same way, per row rather than as a command error.
func TestVerifyStreamLayout(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t)

	noLayout := withRestore(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}), "primary")
	noLayout["layout"] = map[string]any{"run_prefix": "", "artifact_key": "a", "manifest_key": "manifest.json", "manual_id_suffix": "-manual"}
	res, err := w.newEngine(t, noLayout).Verify(ctx, VerifyRequest{
		Fetch: FetchRequest{RunID: "r", Artifact: "a"}, Depth: "decoded"})
	if err != nil || len(res) != 1 || res[0].OK || !strings.Contains(res[0].Error, "layout") {
		t.Fatalf("no layout section: %+v, %v", res, err)
	}

	badPrefix := withRestore(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}), "primary")
	badPrefix["layout"] = map[string]any{"run_prefix": "{{ .Run.ID }}", "artifact_key": "a", "manifest_key": "manifest.json", "manual_id_suffix": "-manual"}
	res, err = w.newEngine(t, badPrefix).Verify(ctx, VerifyRequest{
		Fetch: FetchRequest{RunID: "r", Artifact: "a"}, Depth: "decoded"})
	if err != nil || len(res) != 1 || res[0].OK || !strings.Contains(res[0].Error, "run_prefix") {
		t.Fatalf("bad run prefix: %+v, %v", res, err)
	}
}

// --- verifyStored ------------------------------------------------------------

func TestVerifyStoredRefusals(t *testing.T) {
	ctx := context.Background()

	// No layout section.
	w := newWorld(t)
	noLayout := withRestore(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}), "primary")
	noLayout["layout"] = map[string]any{"run_prefix": "", "artifact_key": "a", "manifest_key": "manifest.json", "manual_id_suffix": "-manual"}
	el := w.newEngine(t, noLayout)
	res, err := el.Verify(ctx, VerifyRequest{Fetch: FetchRequest{RunID: "r", Artifact: "a"}, Depth: "stored"})
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 1 || res[0].OK || !strings.Contains(res[0].Error, "layout") {
		t.Fatalf("no layout section: %+v", res)
	}

	// A run prefix that is not a literal plus {{ .Run.ID }}.
	badPrefix := withRestore(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}), "primary")
	badPrefix["layout"] = map[string]any{"run_prefix": "{{ .Run.ID }}", "artifact_key": "a", "manifest_key": "manifest.json", "manual_id_suffix": "-manual"}
	ep := w.newEngine(t, badPrefix)
	res, err = ep.Verify(ctx, VerifyRequest{Fetch: FetchRequest{RunID: "r", Artifact: "a"}, Depth: "stored"})
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 1 || res[0].OK || !strings.Contains(res[0].Error, "run_prefix") {
		t.Fatalf("bad run prefix: %+v", res)
	}

	// A storage that cannot be surveyed.
	cfg := withRestore(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}), "primary")
	e := runOnce(t, w, cfg)
	runs, _ := e.StoredRuns(ctx, StoredRunsRequest{})
	w.storage(t, "primary").Fault = func(op storagetest.Op, _ string, _ int) error {
		if op == storagetest.OpList {
			return plugin.Permanent(errors.New("listing refused"))
		}
		return nil
	}
	res, err = e.Verify(ctx, VerifyRequest{Fetch: FetchRequest{RunID: runs[0].RunID, Artifact: "wireguard"}, Depth: "stored"})
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 1 || res[0].OK || !strings.Contains(res[0].Error, "survey failed") {
		t.Fatalf("a failed survey: %+v", res)
	}
}

// TestVerifyStoredSHA256Mismatch: the storage exposes a hash that disagrees
// with the manifest's, at the right size.
func TestVerifyStoredSHA256Mismatch(t *testing.T) {
	w := newWorld(t)
	cfg := withRestore(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}), "primary")
	e := runOnce(t, w, cfg)
	runs, _ := e.StoredRuns(context.Background(), StoredRunsRequest{})
	data := w.sources["vpn"].data
	e.storages[0].inst = &covStorage{Fake: w.storage(t, "primary"), statInfo: model.ObjectInfo{
		Size: int64(len(data)), SHA256: strings.Repeat("0", 64),
	}}
	res, err := e.Verify(context.Background(), VerifyRequest{
		Fetch: FetchRequest{RunID: runs[0].RunID, Artifact: "wireguard"}, Depth: "stored"})
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 1 || res[0].OK || !strings.Contains(res[0].Error, "sha256 mismatch") {
		t.Fatalf("sha mismatch verify %+v", res)
	}
}

// probeRetryRestorer consumes the whole stream on its first attempt and fails
// transiently; a correct retry sees the file from its start again, not EOF.
type probeRetryRestorer struct {
	fakeSource
	attempts  int
	firstLen  int
	secondLen int
}

func (p *probeRetryRestorer) Probe(ctx context.Context, src io.Reader) (source.ProbeReport, error) {
	if err := ctx.Err(); err != nil {
		return source.ProbeReport{}, err
	}
	body, err := io.ReadAll(src)
	if err != nil {
		return source.ProbeReport{}, err
	}
	p.attempts++
	if p.attempts == 1 {
		p.firstLen = len(body)
		return source.ProbeReport{}, plugin.Transient(errors.New("probe: a transient read fault"))
	}
	p.secondLen = len(body)
	return source.ProbeReport{Entries: 1, Bytes: int64(len(body))}, nil
}

// TestVerifyProbedRetriesFromTheStart: a Prober that reads the whole input and
// then fails transiently is retried against the same spool file. The retry must
// see the full content from offset zero — a retried probe that starts at EOF
// would take an empty read for a sound archive and report a corrupt artifact OK.
func TestVerifyProbedRetriesFromTheStart(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t)
	prober := &probeRetryRestorer{}
	w.extraSources["target"] = prober
	cfg := applyConfig(t, w, "target", nil)
	e := runOnce(t, w, cfg)
	runs, err := e.StoredRuns(ctx, StoredRunsRequest{})
	if err != nil || len(runs) != 1 {
		t.Fatalf("stored runs %v, %v", runs, err)
	}

	res, err := e.Verify(ctx, VerifyRequest{
		Fetch: FetchRequest{RunID: runs[0].RunID, Artifact: "wireguard"}, Depth: "probed", Target: "target"})
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 1 || !res[0].OK {
		t.Fatalf("probed verify %+v", res)
	}
	if prober.attempts != 2 {
		t.Fatalf("the probe ran %d times, want one failed attempt and one retry", prober.attempts)
	}
	if prober.firstLen == 0 {
		t.Fatal("the first attempt read nothing, so the fixture proves nothing")
	}
	if prober.secondLen != prober.firstLen {
		t.Fatalf("the retried probe read %d bytes, want the whole %d from the start", prober.secondLen, prober.firstLen)
	}
	// A stream cannot be rewound: the retry downloads the copy again.
	ar := findArtifact(&runs[0].Report, "wireguard")
	if ar == nil {
		t.Fatalf("the manifest does not record wireguard: %+v", runs[0].Report.Artifacts)
	}
	if n := w.storage(t, "primary").CallCount(storagetest.OpGet, ar.Key); n != 2 {
		t.Fatalf("the artifact was downloaded %d times, want one per probe attempt", n)
	}
}
