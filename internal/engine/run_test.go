package engine

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/catalog"
	"github.com/vi-latyshev/go-cluster-backup/internal/discovery"
	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/processor"
	"github.com/vi-latyshev/go-cluster-backup/internal/storage/storagetest"
)

func twoStorages() map[string]any {
	return map[string]any{
		"primary": storageInstance("fake", "keep_last"),
		"offsite": storageInstance("fake", "keep_last"),
	}
}

// keepLast1 drops every run but the newest — the shape the retention tests need.
func keepLast1() map[string]any { return keepLastCfg(1) }

func TestRunHappyPath(t *testing.T) {
	w := newWorld(t)
	e := w.newEngine(t, baseConfig(t, twoStorages()))
	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeDelivered || rep.ExitCode != 0 {
		t.Fatalf("outcome %q exit %d\nartifacts: %+v\nstorages: %+v", rep.Outcome, rep.ExitCode, rep.Artifacts, rep.Storages)
	}
	if len(rep.Artifacts) != 1 || rep.Artifacts[0].Outcome != model.ArtifactOK {
		t.Fatalf("artifacts %+v", rep.Artifacts)
	}
	if rep.Artifacts[0].Artifact.Ext != ".dat.zst" || rep.Artifacts[0].Artifact.Sealed {
		t.Fatalf("artifact %+v", rep.Artifacts[0])
	}
	for _, name := range []string{"primary", "offsite"} {
		s := w.storage(t, name)
		wantObj := "runs/" + rep.RunID + "/vpn/wireguard.dat.zst"
		if _, ok := s.Bytes(wantObj); !ok {
			t.Fatalf("%s misses %s (has %v)", name, wantObj, s.Keys())
		}
		if _, ok := s.Bytes("runs/" + rep.RunID + "/manifest.json"); !ok {
			t.Fatalf("%s misses the manifest", name)
		}
		if last := lastPutKey(t, s); last != "runs/"+rep.RunID+"/manifest.json" {
			t.Fatalf("manifest is not the last write on %s: last=%s", name, last)
		}
	}
	for _, s := range rep.Storages {
		if s.Outcome != model.StorageVerified {
			t.Fatalf("storage result %+v", s)
		}
	}
}

func TestRunManifestBytesCarryTheReport(t *testing.T) {
	w := newWorld(t)
	e := w.newEngine(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}))
	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	body, ok := w.storage(t, "primary").Bytes("runs/" + rep.RunID + "/manifest.json")
	if !ok {
		t.Fatal("manifest missing")
	}
	var got model.RunReport
	if err := jsonUnmarshal(body, &got); err != nil {
		t.Fatalf("manifest does not parse: %v", err)
	}
	if got.RunID != rep.RunID || got.Outcome != model.OutcomeDelivered || len(got.Artifacts) != 1 {
		t.Fatalf("manifest content %+v", got)
	}
}

// TestOneStorageDownIsDegraded: the destination is down for the whole run; the
// other holds every object.
func TestOneStorageDownIsDegraded(t *testing.T) {
	w := newWorld(t)
	down := storagetest.NewFake()
	down.Fault = func(op storagetest.Op, key string, call int) error {
		return plugin.Transient(errors.New("destination unreachable"))
	}
	w.storages["primary"] = down
	e := w.newEngine(t, baseConfig(t, twoStorages()))
	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeDegraded || rep.ExitCode != 3 {
		t.Fatalf("outcome %q exit %d", rep.Outcome, rep.ExitCode)
	}
	healthy := w.storage(t, "offsite")
	if _, ok := healthy.Bytes("runs/" + rep.RunID + "/vpn/wireguard.dat.zst"); !ok {
		t.Fatal("healthy storage misses the object")
	}
	if _, ok := healthy.Bytes("runs/" + rep.RunID + "/manifest.json"); !ok {
		t.Fatal("healthy storage misses the manifest")
	}
	if len(healthy.Keys()) != 2 {
		t.Fatalf("healthy storage holds %v", healthy.Keys())
	}
	for _, s := range rep.Storages {
		if s.Storage == "primary" && s.Outcome != model.StorageFailed {
			t.Fatalf("down storage reported %+v", s)
		}
	}
}

// TestFirstCallFailsThenSucceeds: the 2026-10-11 shape — the very first call of
// the run answers 500; the retry must absorb it, and the size-and-hash check the
// Fake runs on every Put proves the spool file is re-opened per attempt.
func TestFirstCallFailsThenSucceeds(t *testing.T) {
	w := newWorld(t)
	f := storagetest.NewFake()
	f.Fault = func(op storagetest.Op, key string, call int) error {
		if op == storagetest.OpPut && call == 1 {
			return plugin.Transient(errors.New("HTTP 500"))
		}
		return nil
	}
	w.storages["primary"] = f
	e := w.newEngine(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}))
	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeDelivered {
		t.Fatalf("outcome %q", rep.Outcome)
	}
	key := "runs/" + rep.RunID + "/vpn/wireguard.dat.zst"
	if n := f.CallCount(storagetest.OpPut, key); n != 2 {
		t.Fatalf("put calls = %d, want 2 (one attempt retried)", n)
	}
}

// TestQuotaFullIsPermanent: a permanent failure stops that storage without
// burning attempts, and later artifacts skip it while the other storage takes
// everything.
func TestQuotaFullIsPermanent(t *testing.T) {
	w := newWorld(t)
	f := storagetest.NewFake()
	f.Fault = func(op storagetest.Op, key string, call int) error {
		if op == storagetest.OpPut {
			return plugin.Permanent(errors.New("quota exceeded"))
		}
		return nil
	}
	w.storages["primary"] = f
	cfg := baseConfig(t, twoStorages())
	cfg["sources"] = map[string]any{
		"vpn": sourceInstance("fake", "vpn", "wireguard", []string{"zstd3"}, "required", "plain"),
		"db":  sourceInstance("fake", "databases", "pg", []string{"zstd3"}, "required", "plain"),
	}
	e := w.newEngine(t, cfg)
	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeDegraded || rep.ExitCode != 3 {
		t.Fatalf("outcome %q exit %d", rep.Outcome, rep.ExitCode)
	}
	// Sources are processed in name order: db first (one failing attempt), vpn
	// after — and the vpn upload must have been skipped entirely.
	if n := f.CallCount(storagetest.OpPut, "runs/"+rep.RunID+"/databases/pg.dat.zst"); n != 1 {
		t.Fatalf("the failing artifact got %d attempts, want exactly 1 (permanent is not retried)", n)
	}
	if n := f.CallCount(storagetest.OpPut, "runs/"+rep.RunID+"/vpn/wireguard.dat.zst"); n != 0 {
		t.Fatalf("a permanently failed storage was tried again (%d puts)", n)
	}
	healthy := w.storage(t, "offsite")
	for _, want := range []string{"/vpn/wireguard.dat.zst", "/databases/pg.dat.zst", "/manifest.json"} {
		if _, ok := healthy.Bytes("runs/" + rep.RunID + want); !ok {
			t.Fatalf("healthy storage misses %s", want)
		}
	}
}

// TestAcceptedButNotCommitted: Put answers success, nothing becomes visible —
// success must never be declared on an unverifiable size.
func TestAcceptedButNotCommitted(t *testing.T) {
	w := newWorld(t)
	f := storagetest.NewFake()
	f.DiscardPuts = true
	w.storages["primary"] = f
	e := w.newEngine(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}))
	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeFailed || rep.ExitCode != 7 {
		t.Fatalf("outcome %q exit %d", rep.Outcome, rep.ExitCode)
	}
	key := "runs/" + rep.RunID + "/vpn/wireguard.dat.zst"
	if n := f.CallCount(storagetest.OpPut, key); n != 3 {
		t.Fatalf("put attempts = %d, want the full retry budget; artifacts: %+v; storages: %+v", n, rep.Artifacts, rep.Storages)
	}
	if len(f.Keys()) != 0 {
		t.Fatalf("storage holds %v", f.Keys())
	}
}

// TestDontKnowIsNotAbsent: a transient Stat answer is retried like any failure,
// never accepted as "the object is absent".
func TestDontKnowIsNotAbsent(t *testing.T) {
	w := newWorld(t)
	f := storagetest.NewFake()
	f.Fault = func(op storagetest.Op, key string, call int) error {
		if op == storagetest.OpStat && call <= 2 {
			return plugin.Transient(errors.New("storage busy"))
		}
		return nil
	}
	w.storages["primary"] = f
	e := w.newEngine(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}))
	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeDelivered {
		t.Fatalf("outcome %q", rep.Outcome)
	}
}

// TestAllStoragesFail: everything down — failed, with the configured exit code.
func TestAllStoragesFail(t *testing.T) {
	w := newWorld(t)
	for _, name := range []string{"primary", "offsite"} {
		f := storagetest.NewFake()
		f.Fault = func(storagetest.Op, string, int) error { return plugin.Transient(errors.New("down")) }
		w.storages[name] = f
	}
	e := w.newEngine(t, baseConfig(t, twoStorages()))
	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeFailed || rep.ExitCode != 7 {
		t.Fatalf("outcome %q exit %d", rep.Outcome, rep.ExitCode)
	}
}

// TestRetentionRunsWhenUploadsFail: retention is not gated on this run's
// delivery — the two old runs are trimmed to one while every upload fails.
func TestRetentionRunsWhenUploadsFail(t *testing.T) {
	w := newWorld(t)
	f := storagetest.NewFake()
	now := time.Now().UTC()
	f.PutObject("runs/OLD1/artifact.bin", []byte("old one"), now.AddDate(0, 0, -10))
	f.PutObject("runs/OLD2/artifact.bin", []byte("old two"), now.AddDate(0, 0, -20))
	f.Fault = func(op storagetest.Op, key string, call int) error {
		if op == storagetest.OpPut {
			return plugin.Permanent(errors.New("down"))
		}
		return nil
	}
	w.storages["primary"] = f
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	withRetention(cfg, "primary", keepLastCfg(1))
	e := w.newEngine(t, cfg)
	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeFailed {
		t.Fatalf("outcome %q", rep.Outcome)
	}
	if _, ok := f.Bytes("runs/OLD2/artifact.bin"); ok {
		t.Fatal("retention did not run despite the failed upload")
	}
	if _, ok := f.Bytes("runs/OLD1/artifact.bin"); !ok {
		t.Fatal("retention dropped too much")
	}
	for _, s := range rep.Storages {
		if s.Storage == "primary" && (len(s.Deleted) != 1 || s.Deleted[0] != "OLD2") {
			t.Fatalf("storage result %+v", s)
		}
	}
}

// TestLastVerifiedCopyGuard: an old run lives verified on both storages and both
// policies want it gone — exactly one deletion happens.
func TestLastVerifiedCopyGuard(t *testing.T) {
	w := newWorld(t)
	old := time.Now().UTC().AddDate(0, 0, -30)
	manifest := mustMarshal(t, model.RunReport{Schema: model.ManifestSchema,
		RunID: "OLD", StartedAt: old, Outcome: model.OutcomeDelivered,
		Storages: []model.StorageResult{
			{Storage: "primary", Outcome: model.StorageVerified},
			{Storage: "offsite", Outcome: model.StorageVerified},
		},
	})
	for _, name := range []string{"primary", "offsite"} {
		f := storagetest.NewFake()
		f.PutObject("runs/OLD/artifact.bin", []byte("old"), old)
		f.PutObject("runs/OLD/manifest.json", manifest, old)
		w.storages[name] = f
	}
	cfg := baseConfig(t, twoStorages())
	withRetention(cfg, "primary", keepLast1())
	e := w.newEngine(t, cfg)
	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeDelivered {
		t.Fatalf("outcome %q", rep.Outcome)
	}
	survivors := 0
	for _, name := range []string{"primary", "offsite"} {
		if _, ok := w.storage(t, name).Bytes("runs/OLD/artifact.bin"); ok {
			survivors++
		}
	}
	if survivors != 1 {
		t.Fatalf("%d verified copies survived, want exactly 1", survivors)
	}
}

// TestRetentionRunsOnceAfterTheManifest: there is one retention pass per
// storage, after the manifest — the floor may credit this run's copies only
// where the manifest is already saved — and it trims the history there.
func TestRetentionRunsOnceAfterTheManifest(t *testing.T) {
	w := newWorld(t)
	now := time.Now().UTC()
	for _, name := range []string{"primary", "offsite"} {
		f := storagetest.NewFake()
		f.PutObject("runs/OLD1/artifact.bin", []byte("old one"), now.AddDate(0, 0, -10))
		f.PutObject("runs/OLD2/artifact.bin", []byte("old two"), now.AddDate(0, 0, -20))
		w.storages[name] = f
	}
	cfg := baseConfig(t, twoStorages())
	withRetention(cfg, "primary", keepLast1())
	withRetention(cfg, "offsite", keepLast1())
	e := w.newEngine(t, cfg)
	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"primary", "offsite"} {
		f := w.storage(t, name)
		manifest := indexOfEvent(f.Events(), storagetest.OpPut, "runs/"+rep.RunID+"/manifest.json")
		deleted := indexOfEvent(f.Events(), storagetest.OpDelete, "runs/OLD2/artifact.bin")
		if manifest == -1 || deleted == -1 || deleted < manifest {
			t.Fatalf("%s: retention did not run after the manifest: manifest=%d delete=%d", name, manifest, deleted)
		}
		// keep-last 1 keeps this run, which is the newest once its manifest is
		// saved: both old runs go.
		if _, ok := f.Bytes("runs/OLD1/artifact.bin"); ok {
			t.Fatalf("%s: retention kept a run the policy dropped", name)
		}
	}
}

// TestPluginPanicIsContained: a panicking source fails its artifact into a
// permanent error; the run completes with a report.
func TestPluginPanicIsContained(t *testing.T) {
	w := newWorld(t)
	w.sources["vpn"] = &fakeSource{dumpPanic: true}
	e := w.newEngine(t, baseConfig(t, twoStorages()))
	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeFailed {
		t.Fatalf("outcome %q", rep.Outcome)
	}
	if len(rep.Artifacts) != 1 || rep.Artifacts[0].Error == "" {
		t.Fatalf("artifact result %+v", rep.Artifacts)
	}
	for _, name := range []string{"primary", "offsite"} {
		if keys := w.storage(t, name).Keys(); len(keys) != 0 {
			t.Fatalf("%s has unexpected objects %v", name, keys)
		}
	}
}

func TestProcessorPanicIsContained(t *testing.T) {
	w := newWorld(t)
	w.processors["zstd3"] = &fakeProcessor{panicNow: true}
	e := w.newEngine(t, baseConfig(t, twoStorages()))
	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeFailed || len(rep.Artifacts) != 1 {
		t.Fatalf("report %+v", rep)
	}
}

// TestSecretArtifactWithoutSealIsRefused: the refusal is a configuration fact —
// validate refuses it, before the run exists and before the first byte.
func TestSecretArtifactWithoutSealIsRefused(t *testing.T) {
	w := newWorld(t)
	src := &fakeSource{data: []byte("secret data")}
	w.sources["vpn"] = src
	cfg := baseConfig(t, twoStorages())
	cfg["sources"] = map[string]any{
		"vpn": sourceInstance("fake", "vpn", "wireguard", []string{"zstd3"}, "required", "secret"),
	}
	e, err := w.newEngineErr(t, cfg)
	if e != nil {
		t.Fatalf("engine.New built an engine for a refused config: %v", err)
	}
	if err == nil {
		t.Fatalf("a secret artifact without a sealing pipeline was accepted")
	}
	if !strings.Contains(err.Error(), "/sources/vpn: a secret artifact's pipeline [zstd3] has no sealing processor") {
		t.Fatalf("the refusal does not name the instance and its pipeline: %v", err)
	}
	if src.dumpCount() != 0 {
		t.Fatalf("the source produced %d streams for a refused artifact", src.dumpCount())
	}
	if len(w.storage(t, "primary").Keys()) != 0 {
		t.Fatal("a refused artifact left objects")
	}
}

func TestSecretArtifactWithSealFlow(t *testing.T) {
	w := newWorld(t)
	w.processors["seal"] = &fakeProcessor{seals: true, ext: ".age"}
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["processors"] = map[string]any{"zstd3": instance("fake"), "seal": instance("fake")}
	cfg["sources"] = map[string]any{
		"vpn": sourceInstance("fake", "vpn", "wireguard", []string{"zstd3", "seal"}, "required", "secret"),
	}
	e := w.newEngine(t, cfg)
	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeDelivered {
		t.Fatalf("outcome %q", rep.Outcome)
	}
	if !rep.Artifacts[0].Artifact.Sealed || rep.Artifacts[0].Artifact.Ext != ".dat.zst.age" {
		t.Fatalf("artifact %+v", rep.Artifacts[0])
	}
}

// TestSealedSourceNeedsNoSealingProcessor: a source whose bytes another process
// produced declares no pipeline of its own — the config check asks it for none —
// and the chain's sealing step is what the secret invariant reads.
func TestSealedSourceNeedsNoSealingProcessor(t *testing.T) {
	w := newWorld(t)
	w.extraSources["vpn"] = &chainSource{data: []byte("sealed"), ext: ".tar.zst.age",
		chain: []model.ProcessorStep{
			{Instance: "zstd", Type: "zstd", Ext: ".zst"},
			{Instance: "seal", Type: "age", Ext: ".age", Seals: true},
		}}
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["sources"] = map[string]any{
		"vpn": sourceInstance("fake", "vpn", "k3s-state", []string{}, "required", "secret"),
	}
	e := w.newEngine(t, cfg)
	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeDelivered {
		t.Fatalf("outcome %q", rep.Outcome)
	}
	if !rep.Artifacts[0].Artifact.Sealed || rep.Artifacts[0].Artifact.Ext != ".tar.zst.age" {
		t.Fatalf("artifact %+v, want it sealed by its chain", rep.Artifacts[0])
	}
}

// TestSealedSourceWithAnUnsealingChainIsRefused: the record is the only thing
// that knows whether the bytes are sealed, so a chain without a sealing step is
// refused at run time — the config check cannot see the record.
func TestSealedSourceWithAnUnsealingChainIsRefused(t *testing.T) {
	w := newWorld(t)
	src := &chainSource{data: []byte("plaintext"), ext: ".tar.zst",
		chain: []model.ProcessorStep{{Instance: "zstd", Type: "zstd", Ext: ".zst"}}}
	w.extraSources["vpn"] = src
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["sources"] = map[string]any{
		"vpn": sourceInstance("fake", "vpn", "k3s-state", []string{}, "required", "secret"),
	}
	e := w.newEngine(t, cfg)
	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeFailed || len(rep.Artifacts) != 1 {
		t.Fatalf("report %+v", rep)
	}
	if !strings.Contains(rep.Artifacts[0].Error, "has no sealing processor") {
		t.Fatalf("the failure does not name the missing seal: %+v", rep.Artifacts[0])
	}
	if len(w.storage(t, "primary").Keys()) != 0 {
		t.Fatal("a refused artifact left objects")
	}
}

func TestDiscoveryFailureFailsTheRun(t *testing.T) {
	w := newWorld(t)
	w.discoveries["workloads"] = &fakeDiscovery{err: errors.New("api down")}
	cfg := baseConfig(t, twoStorages())
	cfg["discoveries"] = map[string]any{"workloads": instance("fake")}
	e := w.newEngine(t, cfg)
	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeFailed {
		t.Fatalf("outcome %q", rep.Outcome)
	}
}

func TestDiscoveredSourcesFlow(t *testing.T) {
	w := newWorld(t)
	w.discoveries["workloads"] = &fakeDiscovery{found: []discovery.Found{{
		Name:     "pg1",
		Type:     "fake",
		Config:   []byte(`{}`),
		Artifact: model.Artifact{Group: "databases", Name: "ns/postgres", Importance: model.ImportanceRequired, Sensitivity: model.SensitivityPlain},
		Pipeline: []string{"zstd3"},
	}}}
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["discoveries"] = map[string]any{"workloads": instance("fake")}
	e := w.newEngine(t, cfg)
	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeDelivered {
		t.Fatalf("outcome %q", rep.Outcome)
	}
	if _, ok := w.storage(t, "primary").Bytes("runs/" + rep.RunID + "/databases/ns/postgres.dat.zst"); !ok {
		t.Fatalf("discovered artifact missing: %v", w.storage(t, "primary").Keys())
	}
}

func TestDuplicateArtifactNames(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["sources"] = map[string]any{
		"a": sourceInstance("fake", "g", "same-name", []string{"zstd3"}, "required", "plain"),
		"b": sourceInstance("fake", "g", "same-name", []string{"zstd3"}, "required", "plain"),
	}
	e := w.newEngine(t, cfg)
	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeFailed {
		t.Fatalf("outcome %q", rep.Outcome)
	}
	failed := 0
	for _, a := range rep.Artifacts {
		if a.Outcome == model.ArtifactFailed {
			failed++
		}
	}
	if failed != 1 {
		t.Fatalf("results %+v", rep.Artifacts)
	}
}

func TestUnknownPipelineFailsAtBuildTime(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["sources"] = map[string]any{
		"vpn": sourceInstance("fake", "vpn", "wireguard", []string{"nope"}, "required", "plain"),
	}
	raw, err := jsonMarshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := New(context.Background(), raw, nil, w.registry, testLogger()); err == nil {
		t.Fatal("an unknown pipeline was accepted at build time")
	}
}

// TestSpoolReserveBlocksTheArtifact: nothing is produced when the reserve cannot
// be honoured, and nothing was delivered — failed.
func TestSpoolReserveBlocksTheArtifact(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["run"].(map[string]any)["spool_reserve_bytes"] = int64(1) << 62
	src := &fakeSource{data: []byte("x")}
	w.sources["vpn"] = src
	e := w.newEngine(t, cfg)
	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if src.dumpCount() != 0 {
		t.Fatal("the source produced bytes despite the reserve")
	}
	if rep.Artifacts[0].Outcome != model.ArtifactFailed {
		t.Fatalf("artifact %+v", rep.Artifacts[0])
	}
	if rep.Outcome != model.OutcomeFailed {
		t.Fatalf("outcome %q (nothing was delivered)", rep.Outcome)
	}
}

func TestNotifierFatalFlag(t *testing.T) {
	w := newWorld(t)
	w.notifiers["ops"] = &fakeNotifier{fails: 100}
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["notifiers"] = map[string]any{"ops": instance("fake")}
	e := w.newEngine(t, cfg)
	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeDelivered {
		t.Fatalf("not fatal: outcome %q", rep.Outcome)
	}

	w2 := newWorld(t)
	w2.notifiers["ops"] = &fakeNotifier{fails: 100}
	cfg["run"].(map[string]any)["notifier_failure_is_fatal"] = true
	e2 := w2.newEngine(t, cfg)
	rep, err = e2.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeFailed || rep.ExitCode != 7 {
		t.Fatalf("fatal notifier: outcome %q exit %d", rep.Outcome, rep.ExitCode)
	}
}

func TestNotificationsCarryPreviousAndOutcome(t *testing.T) {
	w := newWorld(t)
	n := &fakeNotifier{}
	w.notifiers["ops"] = n
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["notifiers"] = map[string]any{"ops": instance("fake")}
	cfg["catalog"] = instance("fake")
	w.catalog = &fakeCatalog{runs: map[string]model.RunReport{
		"20261010T042000Z": {RunID: "20261010T042000Z", StartedAt: time.Date(2026, 10, 10, 4, 20, 0, 0, time.UTC), Outcome: model.OutcomeRunning},
		"20261009T042000Z": {RunID: "20261009T042000Z", StartedAt: time.Date(2026, 10, 9, 4, 20, 0, 0, time.UTC), Outcome: model.OutcomeDelivered},
	}}
	e := w.newEngine(t, cfg)
	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	names := n.eventNames()
	if len(names) != 2 || names[0] != "run_started" || names[1] != "run_finished" {
		t.Fatalf("events %v", names)
	}
	if !n.events[0].Run.Previous.Unfinished {
		t.Fatalf("previous not marked unfinished: %+v", n.events[0].Run.Previous)
	}
	if !n.events[0].Run.Previous.LastGoodAt.Equal(w.catalog.runs["20261009T042000Z"].StartedAt) {
		t.Fatalf("last good %v", n.events[0].Run.Previous.LastGoodAt)
	}
	if n.events[1].Run.Outcome != model.OutcomeDelivered {
		t.Fatalf("final event %q", n.events[1].Run.Outcome)
	}
	if got := w.catalog.runs[rep.RunID].Outcome; got != model.OutcomeDelivered {
		t.Fatalf("catalog final outcome %q", got)
	}
}

func TestExpiredDeadlineFailsTheRunNotTheProcess(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["run"].(map[string]any)["deadline"] = "1ns"
	e := w.newEngine(t, cfg)
	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatalf("an expired deadline must come back as a report, got error %v", err)
	}
	if rep.Outcome != model.OutcomeFailed {
		t.Fatalf("outcome %q", rep.Outcome)
	}
}

func TestDryRunWritesNothing(t *testing.T) {
	w := newWorld(t)
	w.notifiers["ops"] = &fakeNotifier{}
	cfg := baseConfig(t, twoStorages())
	cfg["notifiers"] = map[string]any{"ops": instance("fake")}
	cfg["catalog"] = instance("fake")
	spoolDir := cfg["run"].(map[string]any)["spool_dir"].(string)
	e := w.newEngine(t, cfg)
	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled, DryRun: true})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeDelivered {
		t.Fatalf("outcome %q", rep.Outcome)
	}
	for _, name := range []string{"primary", "offsite"} {
		if keys := w.storage(t, name).Keys(); len(keys) != 0 {
			t.Fatalf("dry run wrote %v to %s", keys, name)
		}
	}
	if len(w.notifiers["ops"].events) != 0 {
		t.Fatal("dry run notified")
	}
	if w.catalog != nil && len(w.catalog.runs) != 0 {
		t.Fatal("dry run touched the catalog")
	}
	if entries, err := readDirNames(spoolDir); err == nil && len(entries) != 0 {
		t.Fatalf("dry run left spool files: %v", entries)
	}
	for _, s := range rep.Storages {
		if s.Outcome != model.StorageSkipped {
			t.Fatalf("storage %+v", s)
		}
	}
}

func TestPreflightRunsChecksAndRunCallsIt(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	e := w.newEngine(t, cfg)
	src := w.sources["vpn"]
	if src.checks != 0 {
		t.Fatalf("New ran plugin checks (%d)", src.checks)
	}
	for _, r := range e.PreflightResults(context.Background(), PreflightRequest{}) {
		if !r.OK {
			t.Fatalf("preflight %s %s: %s", r.Kind, r.Instance, r.Error)
		}
	}
	if src.checks != 1 {
		t.Fatalf("Preflight ran %d checks", src.checks)
	}
	if _, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled, DryRun: true}); err != nil {
		t.Fatal(err)
	}
	if src.checks != 2 {
		t.Fatalf("Run did not preflight (checks=%d)", src.checks)
	}
}

// TestRunSurvivesATransientStorageCheck: a check blip that outlives the retry
// window costs nothing — the storage is tried anyway, and its uploads verify.
func TestRunSurvivesATransientStorageCheck(t *testing.T) {
	w := newWorld(t)
	fleet := checkingStorages(t, w, "checking")
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("checking", "keep_last")})
	e := serviceCheck(t, w, cfg)
	fleet["primary"].checkErr = plugin.Transient(errors.New("connection reset"))

	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeDelivered {
		t.Fatalf("outcome %s: a transient check blip cost the night's copy", rep.Outcome)
	}
	if got := storageResult(rep, "primary"); got.Outcome != model.StorageVerified {
		t.Fatalf("storage %+v", got)
	}
	if fleet["primary"].checks < 3 {
		t.Fatalf("the check ran %d times, want the retry policy's attempts", fleet["primary"].checks)
	}
}

// TestRunRefusesAnUnusableSpoolBeforeTellingAnyone: the spool comes before the
// first event, so a run that cannot buffer has not started — nobody is told it
// did, and the catalog keeps no entry stuck at running.
func TestRunRefusesAnUnusableSpoolBeforeTellingAnyone(t *testing.T) {
	w := newWorld(t)
	blocker := filepath.Join(t.TempDir(), "spool-is-a-file")
	if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["run"].(map[string]any)["spool_dir"] = blocker
	cfg["catalog"] = instance("fake")
	e := serviceCheck(t, w, cfg)

	if _, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled}); err == nil {
		t.Fatal("a run with an unusable spool reported success")
	}
	if _, ok := w.notifiers["ops"].last(); ok {
		t.Fatal("a run that cannot buffer sent an event")
	}
	if len(w.catalog.runs) != 0 {
		t.Fatalf("the catalog holds %d entries for a run that never started", len(w.catalog.runs))
	}
}

// closingSource counts its Close calls; the run that discovered it must close
// it exactly once, and the Engine must not remember it between runs.
type closingSource struct {
	fakeSource
	mu     sync.Mutex
	closes int
}

func (c *closingSource) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closes++
	return nil
}

func (c *closingSource) closeCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closes
}

// TestRunClosesTheSourcesItDiscovered: a discovered source is built per run and
// closed when that run ends — two runs leave no growth on the Engine's lifetime
// closer list and every run's source is closed once.
func TestRunClosesTheSourcesItDiscovered(t *testing.T) {
	w := newWorld(t)
	discovered := &closingSource{}
	w.extraSources["discovered-db"] = discovered
	cfg := baseConfig(t, twoStorages())
	// Two runs in one test must not share a run id: the engine refuses a
	// collision, and a nanosecond layout is what makes them distinct here.
	cfg["run"].(map[string]any)["id_layout"] = "20060102T150405.000000000"
	cfg["discoveries"] = map[string]any{"workloads": instance("fake")}
	e := serviceCheck(t, w, cfg)
	w.discoveries["workloads"].found = []discovery.Found{{
		Name:   "discovered-db",
		Type:   "fake",
		Config: jsontext.Value(`{}`),
		Artifact: model.Artifact{Group: "databases", Name: "discovered-db",
			Importance: model.ImportanceBestEffort, Sensitivity: model.SensitivityPlain},
		Pipeline: []string{"zstd3"},
	}}

	before := len(e.closers)
	for i := range 2 {
		rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
		if err != nil {
			t.Fatalf("run %d: %v", i+1, err)
		}
		ok := artifactByName(rep, "discovered-db")
		if ok.Outcome != model.ArtifactOK {
			t.Fatalf("run %d: the discovered artifact did not run: %+v", i+1, ok)
		}
		if got := discovered.closeCount(); got != i+1 {
			t.Fatalf("after run %d Close ran %d times, want once per run", i+1, got)
		}
	}
	if got := len(e.closers); got != before {
		t.Fatalf("the engine's lifetime closer list grew by %d over two runs", got-before)
	}
}

// TestRunClosesADiscoveredSourceWhoseCheckFails: a discovered source is built
// and belongs to the run even when its Check fails — the run closes it exactly
// once, and the Engine's lifetime list never sees it.
func TestRunClosesADiscoveredSourceWhoseCheckFails(t *testing.T) {
	w := newWorld(t)
	broken := &closingSource{}
	broken.checkErr = errors.New("api refused")
	w.extraSources["broken-db"] = broken
	cfg := baseConfig(t, twoStorages())
	cfg["discoveries"] = map[string]any{"workloads": instance("fake")}
	e := serviceCheck(t, w, cfg)
	w.discoveries["workloads"].found = []discovery.Found{foundSource("broken-db", "broken-db", model.ImportanceBestEffort)}

	before := len(e.closers)
	rep, err := e.Run(context.Background(), RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if got := artifactByName(rep, "broken-db"); got.Outcome != model.ArtifactFailed || !strings.Contains(got.Error, "api refused") {
		t.Fatalf("the failed artifact: %+v", got)
	}
	if got := broken.closeCount(); got != 1 {
		t.Fatalf("Close ran %d times for a source whose Check failed, want exactly once", got)
	}
	if got := len(e.closers); got != before {
		t.Fatalf("the engine's lifetime closer list grew by %d", got-before)
	}
}

// failingCloseSource is a discovered source whose Close reports a failure: the
// run's scoped close has to surface it without failing the run itself.
type failingCloseSource struct {
	fakeSource
	closeErr error
	mu       sync.Mutex
	closes   int
}

func (f *failingCloseSource) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closes++
	return f.closeErr
}

func (f *failingCloseSource) closeCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closes
}

// unsupportedChainSource carries no recorded chain: the capability is present
// but the answer is errors.ErrUnsupported, which the engine reads as "no chain".
type unsupportedChainSource struct{ fakeSource }

func (u *unsupportedChainSource) RecordedChain(context.Context) ([]model.ProcessorStep, error) {
	return nil, errors.ErrUnsupported
}

// failingChainSource answers the capability with a real failure.
type failingChainSource struct{ fakeSource }

func (f *failingChainSource) RecordedChain(context.Context) ([]model.ProcessorStep, error) {
	return nil, errors.New("chain unreadable")
}

// errDescribeProcessor cannot describe itself: the artifact's chain cannot be
// built, so the artifact fails before any byte.
type errDescribeProcessor struct{}

func (p *errDescribeProcessor) Describe(context.Context) (processor.ProcessorTraits, error) {
	return processor.ProcessorTraits{}, errors.New("processor describe failed")
}

func (p *errDescribeProcessor) Encode(ctx context.Context, dst io.Writer, src io.Reader) error {
	_, err := io.Copy(dst, src)
	return err
}

func (p *errDescribeProcessor) Decode(ctx context.Context, dst io.Writer, src io.Reader) error {
	_, err := io.Copy(dst, src)
	return err
}

// errCatalog is a catalog whose Load and/or Save fail, so the run's
// non-fatal catalog paths can be exercised.
type errCatalog struct {
	loadErr  error
	saveErr  error
	mu       sync.Mutex
	saves    int
	loads    int
	stored   map[string]model.RunReport
	checkErr error
}

func (c *errCatalog) Check(context.Context) error { return c.checkErr }

func (c *errCatalog) Save(ctx context.Context, run model.RunReport) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.saves++
	if c.saveErr != nil {
		return c.saveErr
	}
	if c.stored == nil {
		c.stored = map[string]model.RunReport{}
	}
	c.stored[run.RunID] = run
	return nil
}

func (c *errCatalog) Load(context.Context, catalog.LoadRequest) ([]model.RunReport, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.loads++
	if c.loadErr != nil {
		return nil, c.loadErr
	}
	return nil, nil
}

func (c *errCatalog) Forget(context.Context, []string) error { return nil }

func (c *errCatalog) counts() (loads, saves int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.loads, c.saves
}

// registerProcessorType registers one processor type whose New returns the given
// processor, so a test can hand the engine a processor of its own shape.
func registerProcessorType(t *testing.T, w *world, typ string, p processor.Processor) {
	t.Helper()
	if err := plugin.Register(w.registry, processor.Kind, plugin.Spec[map[string]jsontext.Value, processor.Processor]{
		Type: typ, Example: []byte(`{}`),
		New: func(context.Context, map[string]jsontext.Value, plugin.Deps) (processor.Processor, error) {
			return p, nil
		},
	}); err != nil {
		t.Fatal(err)
	}
}

// registerCatalogType registers one catalog type whose New returns c.
func registerCatalogType(t *testing.T, w *world, typ string, c catalog.Catalog) {
	t.Helper()
	if err := plugin.Register(w.registry, catalog.Kind, plugin.Spec[map[string]jsontext.Value, catalog.Catalog]{
		Type: typ, Example: []byte(`{}`),
		New: func(context.Context, map[string]jsontext.Value, plugin.Deps) (catalog.Catalog, error) {
			return c, nil
		},
	}); err != nil {
		t.Fatal(err)
	}
}

// --- Run: refusals before anything is touched --------------------------------

// countingHandler counts the records it renders.
type countingHandler struct {
	mu sync.Mutex
	n  int
}

func (h *countingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *countingHandler) Handle(context.Context, slog.Record) error {
	h.mu.Lock()
	h.n++
	h.mu.Unlock()
	return nil
}

func (h *countingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h *countingHandler) WithGroup(string) slog.Handler { return h }

func (h *countingHandler) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.n
}
