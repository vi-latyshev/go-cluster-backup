package backup_test

import (
	"context"
	"errors"
	"fmt"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/assembly"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/workflow/backup"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	json "encoding/json/v2"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/catalog"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/catalog/catalogtest"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/discovery"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/planning"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/storage/storagetest"
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
	e := w.newAssembly(t, baseConfig(t, twoStorages()))
	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
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
	e := w.newAssembly(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}))
	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
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
		return pluginerrors.Transient(errors.New("destination unreachable"))
	}
	w.Storages["primary"] = down
	e := w.newAssembly(t, baseConfig(t, twoStorages()))
	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
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
			return pluginerrors.Transient(errors.New("HTTP 500"))
		}
		return nil
	}
	w.Storages["primary"] = f
	e := w.newAssembly(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}))
	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
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
			return pluginerrors.Permanent(errors.New("quota exceeded"))
		}
		return nil
	}
	w.Storages["primary"] = f
	cfg := baseConfig(t, twoStorages())
	cfg["sources"] = map[string]any{
		"vpn": sourceInstance("fake", "vpn", "wireguard", []string{"zstd3"}, "required", "plain"),
		"db":  sourceInstance("fake", "databases", "pg", []string{"zstd3"}, "required", "plain"),
	}
	e := w.newAssembly(t, cfg)
	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
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
	w.Storages["primary"] = f
	e := w.newAssembly(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}))
	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
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
			return pluginerrors.Transient(errors.New("storage busy"))
		}
		return nil
	}
	w.Storages["primary"] = f
	e := w.newAssembly(t, baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")}))
	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
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
		f.Fault = func(storagetest.Op, string, int) error { return pluginerrors.Transient(errors.New("down")) }
		w.Storages[name] = f
	}
	e := w.newAssembly(t, baseConfig(t, twoStorages()))
	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
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
			return pluginerrors.Permanent(errors.New("down"))
		}
		return nil
	}
	w.Storages["primary"] = f
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	withRetention(cfg, "primary", keepLastCfg(1))
	e := w.newAssembly(t, cfg)
	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
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
		RunID: "OLD", Kind: model.RunKindScheduled, StartedAt: old, Outcome: model.OutcomeDelivered,
		Storages: []model.StorageResult{
			{Storage: "primary", Outcome: model.StorageVerified},
			{Storage: "offsite", Outcome: model.StorageVerified},
		},
	})
	for _, name := range []string{"primary", "offsite"} {
		f := storagetest.NewFake()
		f.PutObject("runs/OLD/artifact.bin", []byte("old"), old)
		f.PutObject("runs/OLD/manifest.json", manifest, old)
		w.Storages[name] = f
	}
	cfg := baseConfig(t, twoStorages())
	withRetention(cfg, "primary", keepLast1())
	e := w.newAssembly(t, cfg)
	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
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
		w.Storages[name] = f
	}
	cfg := baseConfig(t, twoStorages())
	withRetention(cfg, "primary", keepLast1())
	withRetention(cfg, "offsite", keepLast1())
	e := w.newAssembly(t, cfg)
	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
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
	w.Sources["vpn"] = &fakeSource{DumpPanic: true}
	e := w.newAssembly(t, baseConfig(t, twoStorages()))
	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
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
	w.Processors["zstd3"] = &fakeProcessor{PanicNow: true}
	e := w.newAssembly(t, baseConfig(t, twoStorages()))
	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
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
	src := &fakeSource{Data: []byte("secret data")}
	w.Sources["vpn"] = src
	cfg := baseConfig(t, twoStorages())
	cfg["sources"] = map[string]any{
		"vpn": sourceInstance("fake", "vpn", "wireguard", []string{"zstd3"}, "required", "secret"),
	}
	e, err := w.newAssemblyErr(t, cfg)
	if e != nil {
		t.Fatalf("assembly.New built an assembly for a refused config: %v", err)
	}
	if err == nil {
		t.Fatalf("a secret artifact without a sealing pipeline was accepted")
	}
	if !strings.Contains(err.Error(), "/sources/vpn: a secret artifact's pipeline [zstd3] has no sealing processor") {
		t.Fatalf("the refusal does not name the instance and its pipeline: %v", err)
	}
	if src.DumpCount() != 0 {
		t.Fatalf("the source produced %d streams for a refused artifact", src.DumpCount())
	}
	if len(w.storage(t, "primary").Keys()) != 0 {
		t.Fatal("a refused artifact left objects")
	}
}

func TestSecretArtifactWithSealFlow(t *testing.T) {
	w := newWorld(t)
	w.Processors["seal"] = &fakeProcessor{Seals: true, Ext: ".age"}
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["processors"] = map[string]any{"zstd3": instance("fake"), "seal": instance("fake")}
	cfg["sources"] = map[string]any{
		"vpn": sourceInstance("fake", "vpn", "wireguard", []string{"zstd3", "seal"}, "required", "secret"),
	}
	e := w.newAssembly(t, cfg)
	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
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
	w.ExtraSources["vpn"] = &chainSource{Data: []byte("sealed"), Ext: ".tar.zst.age",
		Chain: []model.ProcessorStep{
			{Instance: "zstd", Type: "zstd", Ext: ".zst"},
			{Instance: "seal", Type: "age", Ext: ".age", Seals: true},
		}}
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["sources"] = map[string]any{
		"vpn": sourceInstance("fake", "vpn", "k3s-state", []string{}, "required", "secret"),
	}
	e := w.newAssembly(t, cfg)
	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
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
	src := &chainSource{Data: []byte("plaintext"), Ext: ".tar.zst",
		Chain: []model.ProcessorStep{{Instance: "zstd", Type: "zstd", Ext: ".zst"}}}
	w.ExtraSources["vpn"] = src
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["sources"] = map[string]any{
		"vpn": sourceInstance("fake", "vpn", "k3s-state", []string{}, "required", "secret"),
	}
	e := w.newAssembly(t, cfg)
	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
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
	w.Discoveries["workloads"] = &fakeDiscovery{Err: errors.New("api down")}
	cfg := baseConfig(t, twoStorages())
	cfg["discoveries"] = map[string]any{"workloads": instance("fake")}
	e := w.newAssembly(t, cfg)
	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeFailed {
		t.Fatalf("outcome %q", rep.Outcome)
	}
}

func TestDiscoveredSourcesFlow(t *testing.T) {
	w := newWorld(t)
	w.Discoveries["workloads"] = &fakeDiscovery{Found: []discovery.Found{{
		Name:     "pg1",
		Type:     "fake",
		Config:   []byte(`{}`),
		Artifact: model.Artifact{Group: "databases", Name: "ns/postgres", Importance: model.ImportanceRequired, Sensitivity: model.SensitivityPlain},
		Pipeline: []string{"zstd3"},
	}}}
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["discoveries"] = map[string]any{"workloads": instance("fake")}
	e := w.newAssembly(t, cfg)
	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
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
	e := w.newAssembly(t, cfg)
	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
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
	if _, err := assembly.New(context.Background(), raw, nil, assembly.Deps{Factories: w.Registry, Loader: w.Loader, Log: testLogger()}); err == nil {
		t.Fatal("an unknown pipeline was accepted at build time")
	}
}

// TestSpoolReserveBlocksTheArtifact: nothing is produced when the reserve cannot
// be honoured, and nothing was delivered — failed.
func TestSpoolReserveBlocksTheArtifact(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["run"].(map[string]any)["spool_reserve_bytes"] = int64(1) << 62
	src := &fakeSource{Data: []byte("x")}
	w.Sources["vpn"] = src
	e := w.newAssembly(t, cfg)
	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if src.DumpCount() != 0 {
		t.Fatal("the source produced bytes despite the reserve")
	}
	if rep.Artifacts[0].Outcome != model.ArtifactFailed {
		t.Fatalf("artifact %+v", rep.Artifacts[0])
	}
	if rep.Outcome != model.OutcomeFailed {
		t.Fatalf("outcome %q (nothing was delivered)", rep.Outcome)
	}
}

// TestNotifierFailureVerdict pins what run.notifier_failure_is_fatal does to the
// outcome: a failed finished notification no notifier delivered makes the run
// failed, a failed started notification only degrades it, and a finished message
// that was delivered always names the verdict the report, the exit code and the
// catalog carry — so a second notifier that worked cannot contradict a faulty
// one, and a notifier subscribed to nothing cannot stand in for one that did.
func TestNotifierFailureVerdict(t *testing.T) {
	tests := []struct {
		name              string
		notifiers         int
		fault             string // started | finished | both | retry
		backup            string // "" = a healthy fake; webhook-both | webhook-started | webhook-mute
		fatal             bool
		finishedDelivered int
		outcome           model.Outcome
		exit              int
	}{
		// One notifier: its finished message reaches nobody when it fails, so the
		// flag's finished rule decides alone.
		{"one/started/off", 1, "started", "", false, 1, model.OutcomeDelivered, 0},
		{"one/started/on", 1, "started", "", true, 1, model.OutcomeDegraded, 3},
		{"one/finished/off", 1, "finished", "", false, 0, model.OutcomeDelivered, 0},
		{"one/finished/on", 1, "finished", "", true, 0, model.OutcomeFailed, 7},
		{"one/both/off", 1, "both", "", false, 0, model.OutcomeDelivered, 0},
		{"one/both/on", 1, "both", "", true, 0, model.OutcomeFailed, 7},
		// Two notifiers, one faulty: the healthy one still delivers the finished
		// verdict, so its failure is not fatal; the started failure still degrades.
		{"two/started/off", 2, "started", "", false, 2, model.OutcomeDelivered, 0},
		{"two/started/on", 2, "started", "", true, 2, model.OutcomeDegraded, 3},
		{"two/finished/off", 2, "finished", "", false, 1, model.OutcomeDelivered, 0},
		{"two/finished/on", 2, "finished", "", true, 1, model.OutcomeDelivered, 0},
		{"two/both/off", 2, "both", "", false, 1, model.OutcomeDelivered, 0},
		{"two/both/on", 2, "both", "", true, 1, model.OutcomeDegraded, 3},
		// A real webhook as the second notifier: only an event it is subscribed to
		// is a delivery; a started-only or mute notifier skips the finished event
		// and its skip is neutral, so the faulty notifier's finished failure stays
		// fatal. A webhook subscribed to both still suppresses it.
		{"two/finished/on/webhook-both", 2, "finished", "webhook-both", true, 1, model.OutcomeDelivered, 0},
		{"two/finished/on/webhook-started", 2, "finished", "webhook-started", true, 0, model.OutcomeFailed, 7},
		{"two/finished/on/webhook-mute", 2, "finished", "webhook-mute", true, 0, model.OutcomeFailed, 7},
		// A notifier that failed an attempt and delivered on the retry counts as
		// delivered: the flag reads the final answer, never the attempts.
		{"one/retry/on", 1, "retry", "", true, 1, model.OutcomeDelivered, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := newWorld(t)
			faulty := &fakeNotifier{}
			switch tc.fault {
			case "started":
				faulty.FailOn = model.EventStarted
			case "finished":
				faulty.FailOn = model.EventFinished
			case "both":
				faulty.Fails = 100
			case "retry":
				faulty.Fails = 1
			}
			w.Notifiers["ops"] = faulty
			notifiers := map[string]any{"ops": instance("fake")}
			var (
				bodiesMu sync.Mutex
				bodies   []string
			)
			if tc.notifiers == 2 {
				switch tc.backup {
				case "webhook-both", "webhook-started", "webhook-mute":
					// The webhook notifier is part of the compiled distribution the
					// harness registers through its composition root.
					srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						body, _ := io.ReadAll(r.Body)
						bodiesMu.Lock()
						bodies = append(bodies, string(body))
						bodiesMu.Unlock()
						w.WriteHeader(http.StatusOK)
					}))
					t.Cleanup(srv.Close)
					events := map[string]any{}
					switch tc.backup {
					case "webhook-both":
						events = map[string]any{
							model.EventStarted:  "started",
							model.EventFinished: "outcome={{ .Run.Outcome }} exit={{ .Run.ExitCode }}",
						}
					case "webhook-started":
						events = map[string]any{model.EventStarted: "started"}
					}
					notifiers["backup"] = map[string]any{"type": "webhook", "config": map[string]any{
						"url": srv.URL, "method": "POST", "headers": map[string]string{},
						"body_format": "text", "timeout": "5s", "success_status": []int{200},
						"max_response_bytes": 512, "events": events, "payload": "{{ .Text }}",
					}}
				default:
					w.Notifiers["backup"] = &fakeNotifier{}
					notifiers["backup"] = instance("fake")
				}
			}
			cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
			cfg["notifiers"] = notifiers
			cfg["catalog"] = instance("fake")
			cfg["run"].(map[string]any)["notifier_failure_is_fatal"] = tc.fatal
			e := w.newAssembly(t, cfg)
			rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
			if err != nil {
				t.Fatal(err)
			}
			if rep.Outcome != tc.outcome || rep.ExitCode != tc.exit {
				t.Fatalf("outcome %q exit %d, want %q exit %d", rep.Outcome, rep.ExitCode, tc.outcome, tc.exit)
			}
			want := fmt.Sprintf("outcome=%s exit=%d", rep.Outcome, rep.ExitCode)
			delivered := 0
			for name, n := range w.Notifiers {
				ev, ok := n.Finished()
				if !ok {
					continue
				}
				delivered++
				if ev.Run.Outcome != rep.Outcome || ev.Run.ExitCode != rep.ExitCode {
					t.Fatalf("notifier %s delivered %q exit %d, the run ended %q exit %d",
						name, ev.Run.Outcome, ev.Run.ExitCode, rep.Outcome, rep.ExitCode)
				}
			}
			bodiesMu.Lock()
			defer bodiesMu.Unlock()
			for _, body := range bodies {
				if !strings.Contains(body, "outcome=") {
					continue // the started message carries no verdict
				}
				delivered++
				if !strings.Contains(body, want) {
					t.Fatalf("the webhook delivered %q, the run ended %s", body, want)
				}
			}
			if delivered != tc.finishedDelivered {
				t.Fatalf("%d notifiers delivered the finished verdict, want %d", delivered, tc.finishedDelivered)
			}
		})
	}
}

func seedSurveyRun(t *testing.T, f *storagetest.Fake, id string, at time.Time, manifest bool, outcome model.Outcome) {
	t.Helper()
	f.PutObject("runs/"+id+"/databases/postgres.dump", []byte("x"), at)
	if !manifest {
		return
	}
	body, err := json.Marshal(model.RunReport{
		Schema: model.ManifestSchema, RunID: id, Kind: model.RunKindScheduled,
		StartedAt: at, Outcome: outcome,
	})
	if err != nil {
		t.Fatal(err)
	}
	f.PutObject("runs/"+id+"/manifest.json", body, at)
}

func TestNotificationsCarryPreviousAndOutcome(t *testing.T) {
	w := newWorld(t)
	n := &fakeNotifier{}
	w.Notifiers["ops"] = n
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["notifiers"] = map[string]any{"ops": instance("fake")}
	cfg["catalog"] = instance("fake")
	cfg["plugins"] = map[string]any{
		"bundles": []any{}, "disabled": []any{},
		"config": map[string]any{"reporting": map[string]any{
			"api_host_env": "GCB_REPORT_HOST", "api_port_env": "GCB_REPORT_PORT",
			"token_file": "/var/run/secrets/token", "ca_file": "/var/run/secrets/ca.crt",
			"request_timeout": "30s", "namespace_env": "GCB_REPORT_NAMESPACE", "pod_env": "GCB_REPORT_POD",
		}},
	}
	e := w.newAssembly(t, cfg)
	// The previous scheduled night started and never reported; the one before it
	// delivered. The survey is the only record of either.
	lastGood := time.Date(2026, 10, 9, 4, 20, 0, 0, time.UTC)
	f := w.storage(t, "primary")
	seedSurveyRun(t, f, "20261009T042000Z", lastGood, true, model.OutcomeDelivered)
	seedSurveyRun(t, f, "20261010T042000Z", time.Date(2026, 10, 10, 4, 20, 0, 0, time.UTC), false, "")

	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	names := n.EventNames()
	if len(names) != 2 || names[0] != "run_started" || names[1] != "run_finished" {
		t.Fatalf("events %v", names)
	}
	// The started event goes out before the survey: it carries no previous data.
	if got := n.Events[0].Run.Previous; got != (model.PreviousRun{}) {
		t.Fatalf("the started event carries previous Data: %+v", got)
	}
	prev := n.Events[1].Run.Previous
	if prev.RunID != "20261010T042000Z" || !prev.Unfinished {
		t.Fatalf("previous %+v, want the unfinished 20261010 run", prev)
	}
	if !prev.LastGoodAt.Equal(lastGood) {
		t.Fatalf("last good %v, want %v", prev.LastGoodAt, lastGood)
	}
	if n.Events[1].Run.Outcome != model.OutcomeDelivered {
		t.Fatalf("final event %q", n.Events[1].Run.Outcome)
	}
	if rep.Outcome != model.OutcomeDelivered {
		t.Fatalf("run outcome %q", rep.Outcome)
	}
}

func TestExpiredDeadlineFailsTheRunNotTheProcess(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["run"].(map[string]any)["deadline"] = "1ns"
	e := w.newAssembly(t, cfg)
	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatalf("an expired deadline must come back as a report, got error %v", err)
	}
	if rep.Outcome != model.OutcomeFailed {
		t.Fatalf("outcome %q", rep.Outcome)
	}
}

func TestDryRunWritesNothing(t *testing.T) {
	w := newWorld(t)
	w.Notifiers["ops"] = &fakeNotifier{}
	cfg := baseConfig(t, twoStorages())
	cfg["notifiers"] = map[string]any{"ops": instance("fake")}
	cfg["catalog"] = instance("fake")
	spoolDir := cfg["run"].(map[string]any)["spool_dir"].(string)
	e := w.newAssembly(t, cfg)
	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled, DryRun: true})
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
	if len(w.Notifiers["ops"].Events) != 0 {
		t.Fatal("dry run notified")
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
	e := w.newAssembly(t, cfg)
	src := w.Sources["vpn"]
	if src.Checks != 0 {
		t.Fatalf("New ran plugin checks (%d)", src.Checks)
	}
	for _, r := range e.PreflightResults(context.Background(), planning.Request{}) {
		if !r.OK {
			t.Fatalf("preflight %s %s: %s", r.Kind, r.Instance, r.Error)
		}
	}
	if src.Checks != 1 {
		t.Fatalf("Preflight ran %d checks", src.Checks)
	}
	if _, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled, DryRun: true}); err != nil {
		t.Fatal(err)
	}
	if src.Checks != 2 {
		t.Fatalf("Run did not preflight (checks=%d)", src.Checks)
	}
}

// TestRunSurvivesATransientStorageCheck: a check blip that outlives the retry
// window costs nothing — the storage is tried anyway, and its uploads verify.
func TestRunSurvivesATransientStorageCheck(t *testing.T) {
	w := newWorld(t)
	fleet := checkingStorages(t, w, "checking")
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("checking", "keep_last")})
	e := serviceCheck(t, w, cfg)
	fleet["primary"].CheckErr = pluginerrors.Transient(errors.New("connection reset"))

	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeDelivered {
		t.Fatalf("outcome %s: a transient check blip cost the night's copy", rep.Outcome)
	}
	if got := storageResult(rep, "primary"); got.Outcome != model.StorageVerified {
		t.Fatalf("storage %+v", got)
	}
	if fleet["primary"].Checks < 3 {
		t.Fatalf("the check ran %d times, want the retry policy's attempts", fleet["primary"].Checks)
	}
}

// TestRunRefusesAnUnusableSpoolBeforeTellingAnyone: the spool comes before the
// first event, so a run that cannot buffer has not started — nobody is told it
// did.
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

	if _, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled}); err == nil {
		t.Fatal("a run with an unusable spool reported success")
	}
	if _, ok := w.Notifiers["ops"].Last(); ok {
		t.Fatal("a run that cannot buffer sent an event")
	}
}

// closingSource counts its Close calls; the run that discovered it must close
// it exactly once, and the assembly.Assembly must not remember it between runs.
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
// closed when that run ends — two runs leave no growth on the assembly.Assembly's lifetime
// closer list and every run's source is closed once.
func TestProduceFailsWhenTheSpoolDirectoryVanishes(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	spoolDir := filepath.Join(t.TempDir(), "run-spool")
	cfg["run"].(map[string]any)["spool_dir"] = spoolDir
	fake := storagetest.NewFake()
	fake.Fault = func(op storagetest.Op, _ string, call int) error {
		if op == storagetest.OpList && call == 1 {
			_ = os.RemoveAll(spoolDir)
			_ = os.WriteFile(spoolDir, []byte("x"), 0o600)
		}
		return nil
	}
	w.Storages["primary"] = fake
	e := w.newAssembly(t, cfg)

	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatalf("a vanished spool must be a reported artifact failure, not a run error: %v", err)
	}
	if rep.Outcome != model.OutcomeFailed {
		t.Fatalf("outcome %q: %+v", rep.Outcome, rep.Artifacts)
	}
	if len(rep.Artifacts) != 1 || rep.Artifacts[0].Outcome != model.ArtifactFailed || !strings.Contains(rep.Artifacts[0].Error, "spool") {
		t.Fatalf("artifact %+v, want a failed artifact naming the spool", rep.Artifacts)
	}
	if dumps := w.Sources["vpn"].DumpCount(); dumps != 0 {
		t.Fatalf("the source dumped %d times into a spool it could not write", dumps)
	}
	if keys := fake.Keys(); len(keys) != 0 {
		t.Fatalf("an artifact whose spool failed reached a storage: %v", keys)
	}
}

// --- the survey phase ---------------------------------------------------------

// listClockStorage is a fake whose listing takes the given time and records when
// it ended: what proves no write starts while another storage is still surveyed.
type listClockStorage struct {
	*storagetest.Fake
	delay time.Duration
	mu    sync.Mutex
	end   time.Time
}

func (s *listClockStorage) List(ctx context.Context, req storage.ListRequest) (storage.ListPage, error) {
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	page, err := s.Fake.List(ctx, req)
	s.mu.Lock()
	s.end = time.Now()
	s.mu.Unlock()
	return page, err
}

func (s *listClockStorage) listEnd() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.end
}

// TestRunSurveysStoragesAtOnce (A6, S3): the surveys of every storage run
// together — two ten-second listings cost the run ten seconds, not twenty — and
// the first write still waits for all of them.
func TestRunSurveysStoragesAtOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newWorld(t)
		var mu sync.Mutex
		var firstPut time.Time
		for _, name := range []string{"primary", "offsite"} {
			f := storagetest.NewFake()
			f.Fault = func(op storagetest.Op, _ string, _ int) error {
				if op == storagetest.OpList {
					time.Sleep(10 * time.Second)
				}
				return nil
			}
			h := &hookPutStorage{Fake: f}
			h.onPut = func() {
				mu.Lock()
				if firstPut.IsZero() {
					firstPut = time.Now()
				}
				mu.Unlock()
			}
			w.ExtraStorages[name] = h
			w.Storages[name] = f
		}
		cfg := baseConfig(t, twoStorages())
		for _, name := range []string{"primary", "offsite"} {
			cfg["storages"].(map[string]any)[name].(map[string]any)["time_budget"] = "1m"
		}
		start := time.Now()
		rep, err := w.newAssembly(t, cfg).Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
		if err != nil {
			t.Fatal(err)
		}
		if rep.Outcome != model.OutcomeDelivered {
			t.Fatalf("outcome %q: %+v", rep.Outcome, rep.Storages)
		}
		mu.Lock()
		at := firstPut
		mu.Unlock()
		if at.IsZero() {
			t.Fatal("the run uploaded nothing")
		}
		if elapsed := at.Sub(start); elapsed >= 15*time.Second {
			t.Fatalf("the first write waited %s for two ten-second listings, want both surveys at once", elapsed)
		}
	})
}

// TestRunWritesNothingBeforeEverySurveyEnds (F5, S3): no write — not an upload,
// not an abort — starts on one storage while another storage is still being
// surveyed; a broken parallel survey that writes as it goes would start the
// fast storage's upload in the slow one's tenth second.
func TestRunWritesNothingBeforeEverySurveyEnds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newWorld(t)
		slow := &listClockStorage{Fake: storagetest.NewFake(), delay: 10 * time.Second}
		w.ExtraStorages["a_slow"] = slow
		w.Storages["a_slow"] = slow.Fake
		parts := addMultipart(w, "b_parts", 4)
		var mu sync.Mutex
		var firstWrite time.Time
		parts.beginHook = func(req storage.PutRequest) {
			if req.Size <= 4 {
				return
			}
			mu.Lock()
			if firstWrite.IsZero() {
				firstWrite = time.Now()
			}
			mu.Unlock()
		}
		cfg := baseConfig(t, map[string]any{
			"a_slow":  storageInstance("fake", "keep_last"),
			"b_parts": storageInstance("fake", "keep_last"),
		})
		// The multipart storage's accepted write may settle after the run, so the
		// run must durably mark itself before the first one.
		cfg["catalog"] = instance("fake")
		cfg["run"].(map[string]any)["storage_parallelism"] = 2
		for _, name := range []string{"a_slow", "b_parts"} {
			cfg["storages"].(map[string]any)[name].(map[string]any)["time_budget"] = "1m"
		}
		w.Sources["vpn"] = &fakeSource{Data: []byte("0123456789")}
		rep, err := w.newAssembly(t, cfg).Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
		if err != nil {
			t.Fatal(err)
		}
		if rep.Outcome != model.OutcomeDelivered {
			t.Fatalf("outcome %q: %+v", rep.Outcome, rep.Storages)
		}
		mu.Lock()
		at := firstWrite
		mu.Unlock()
		if at.IsZero() {
			t.Fatal("the multipart storage never began its upload")
		}
		if end := slow.listEnd(); at.Before(end) {
			t.Fatalf("a write started %s before the slow storage's survey ended", end.Sub(at))
		}
	})
}

// TestRunCollisionRefusesBeforeWritingToAnyStorage (F5, S3): a run id one
// storage already holds refuses the run after the surveys and before the first
// write on any storage — the collision is decided once, over every survey.
func TestRunCollisionRefusesBeforeWritingToAnyStorage(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{
		"a_slow":  storageInstance("fake", "keep_last"),
		"b_parts": storageInstance("fake", "keep_last"),
	})
	cfg["run"].(map[string]any)["id_layout"] = "fixedrun"
	parts := addMultipart(w, "b_parts", 4)
	e := w.newAssembly(t, cfg)
	putCopiedRun(t, w.storage(t, "a_slow"), "a_slow", "fixedrun", time.Now().Add(-time.Hour),
		model.OutcomeDelivered, [][2]string{{"databases", "prod"}})
	if _, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled}); err == nil {
		t.Fatal("a colliding run id was accepted")
	} else if !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("the refusal does not name the collision: %v", err)
	}
	begins, aborts, completes, _ := parts.stats()
	if begins != 0 || aborts != 0 || completes != 0 {
		t.Fatalf("a refused run wrote to another storage: begins=%d aborts=%d completes=%d", begins, aborts, completes)
	}
}

// TestRunSettlesAnInheritedUncertainty: the record's uncertainty is cleared only
// by two consecutive full surveys of the predecessor's prefix that agree. The
// first run records its observation; the second one, seeing the same inventory,
// clears the record and treats occupancy as known.
func TestRunSettlesAnInheritedUncertainty(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newWorld(t)
		f := storagetest.NewFake()
		f.PutObject("runs/dead/artifact.bin", []byte("x"), time.Now().UTC().Add(-time.Hour))
		w.Storages["alpha"] = f
		cfg := baseConfig(t, map[string]any{"alpha": storageInstance("fake", "keep_last")})
		cfg["catalog"] = instance("fake")
		e := w.newAssembly(t, cfg)
		w.Catalog.SetUncertainty(&catalog.Uncertainty{RunIDs: []string{"dead"}, Since: time.Now().UTC()})

		first, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
		if err != nil {
			t.Fatal(err)
		}
		if got := w.Catalog.Uncertainty(); got == nil || got.Digest == "" {
			t.Fatalf("the first survey was not recorded: %+v", got)
		}
		if first.Outcome != model.OutcomeDelivered {
			t.Fatalf("first outcome %q", first.Outcome)
		}
		// The run id is second-precise: give the second acquisition its own second.
		time.Sleep(1100 * time.Millisecond)
		second, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
		if err != nil {
			t.Fatal(err)
		}
		if got := w.Catalog.Uncertainty(); got != nil {
			t.Fatalf("the record still carries uncertainty: %+v", got)
		}
		if second.Outcome != model.OutcomeDelivered {
			t.Fatalf("second outcome %q", second.Outcome)
		}
	})
}

// observeFailingLease is a catalog whose observation call Fails: the record
// cannot be told what this survey saw, so occupancy stays unknown.
type observeFailingLease struct {
	*catalogtest.Leaser
	err error
}

func (l *observeFailingLease) ObserveUncertainty(context.Context, catalog.Lease, string) error {
	return l.err
}

// markFailingLease is a catalog whose mark call Fails: the run cannot promise a
// successor that its provider writes may still land, so it refuses to write.
type markFailingLease struct {
	*catalogtest.Leaser
	err error
}

func (l *markFailingLease) MarkUncertainty(context.Context, catalog.Lease) error { return l.err }

// TestRunWarnsWhenTheInheritedUncertaintyCannotBeObserved: a catalog that
// cannot record the survey keeps the record conservative; the run says so and
// continues with occupancy unknown.
func TestRunRefusesWhenTheObservationCannotBeDurablyRecorded(t *testing.T) {
	w := newWorld(t)
	f := storagetest.NewFake()
	f.PutObject("runs/dead/artifact.bin", []byte("x"), time.Now().UTC().Add(-time.Hour))
	w.Storages["alpha"] = f
	cfg := baseConfig(t, map[string]any{"alpha": storageInstance("fake", "keep_last")})
	cfg["catalog"] = instance("failing")
	record := catalogtest.NewLeaser()
	registerCatalogType(t, w, "failing", &observeFailingLease{Leaser: record, err: errors.New("record unavailable")})
	e := w.newAssembly(t, cfg)
	// The record carries no observation digest yet: its write-ahead marker
	// cannot be written, so no survey may start.
	record.SetUncertainty(&catalog.Uncertainty{RunIDs: []string{"dead"}, Since: time.Now().UTC()})

	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeFailed || rep.Lease == nil || !strings.Contains(rep.Lease.Error, "record unavailable") {
		t.Fatalf("a run whose marker is not durable was not refused: %+v", rep)
	}
	for _, ev := range f.Events() {
		if ev.Op == storagetest.OpList {
			t.Fatal("a refused run surveyed the storage")
		}
	}
	if record.Uncertainty() == nil {
		t.Fatal("a failed marker cleared the record")
	}
}

// TestRunRefusesBeforeWritingWhenTheMarkFails: a storage that writes in parts
// makes the run promise a successor an uncertain record before the first write;
// when the catalog cannot take that mark, the run writes nothing and reaches
// the common finish as a refusal.
func TestRunRefusesBeforeWritingWhenTheMarkFails(t *testing.T) {
	w := newWorld(t)
	parts := addMultipart(w, "alpha", 4)
	cfg := baseConfig(t, map[string]any{"alpha": storageInstance("fake", "keep_last")})
	cfg["catalog"] = instance("failing")
	record := catalogtest.NewLeaser()
	registerCatalogType(t, w, "failing", &markFailingLease{Leaser: record, err: errors.New("mark unavailable")})
	e := w.newAssembly(t, cfg)

	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Lease == nil || !strings.Contains(rep.Lease.Error, "mark unavailable") {
		t.Fatalf("the refusal does not carry the mark failure: %+v", rep.Lease)
	}
	if rep.Outcome != model.OutcomeFailed {
		t.Fatalf("outcome %q", rep.Outcome)
	}
	begins, aborts, completes, _ := parts.stats()
	if begins != 0 || aborts != 0 || completes != 0 {
		t.Fatalf("a refused run wrote: begins=%d aborts=%d completes=%d", begins, aborts, completes)
	}
	if record.Uncertainty() != nil {
		t.Fatal("a failed mark left an uncertainty behind")
	}
}

// TestRunClearsItsOwnMarkAfterASettledRun: the run that armed the mark clears it
// at the end once every write has a terminal proof.
func TestRunClearsItsOwnMarkAfterASettledRun(t *testing.T) {
	w := newWorld(t)
	addMultipart(w, "alpha", 4)
	cfg := baseConfig(t, map[string]any{"alpha": storageInstance("fake", "keep_last")})
	cfg["catalog"] = instance("record")
	record := catalogtest.NewLeaser()
	registerCatalogType(t, w, "record", record)
	e := w.newAssembly(t, cfg)

	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeDelivered {
		t.Fatalf("outcome %q: %+v", rep.Outcome, rep.Storages)
	}
	if got := record.Uncertainty(); got != nil {
		t.Fatalf("the run kept its own mark: %+v", got)
	}
}

// clearFailingLease is a catalog whose clear call Fails: the record keeps the
// uncertainty this run armed, and the run must not fail over it.
type clearFailingLease struct {
	*catalogtest.Leaser
	err error
}

func (l *clearFailingLease) ClearUncertainty(context.Context, catalog.Lease) error { return l.err }

// TestRunKeepsItsOutcomeWhenTheClearFails: the run's writes are proven, so a
// record that could not be cleared is a warning — the outcome stands and the
// successor inherits the uncertainty.
func TestRunKeepsItsOutcomeWhenTheClearFails(t *testing.T) {
	w := newWorld(t)
	addMultipart(w, "alpha", 4)
	cfg := baseConfig(t, map[string]any{"alpha": storageInstance("fake", "keep_last")})
	cfg["catalog"] = instance("failing")
	record := catalogtest.NewLeaser()
	registerCatalogType(t, w, "failing", &clearFailingLease{Leaser: record, err: errors.New("clear unavailable")})
	e := w.newAssembly(t, cfg)

	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome != model.OutcomeDelivered {
		t.Fatalf("a failed clear lowered the outcome to %q", rep.Outcome)
	}
	if record.Uncertainty() == nil {
		t.Fatal("a failed clear dropped the record's uncertainty")
	}
}

// budgetRunConfig is the run config with a storage budget short enough to
// witness: production longer than the budget must not cost the upload.
func budgetRunConfig(t *testing.T, w *world, budget string) (*assembly.Assembly, map[string]any) {
	t.Helper()
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["storages"].(map[string]any)["primary"].(map[string]any)["time_budget"] = budget
	return w.newAssembly(t, cfg), cfg
}

// storageOutcome finds one storage's result in the report.
func storageOutcome(rep model.RunReport, name string) model.StorageResult {
	for _, s := range rep.Storages {
		if s.Storage == name {
			return s
		}
	}
	return model.StorageResult{}
}

// TestStorageBudgetCountsOnlyStorageTime (A21): the budget covers the time spent
// inside that storage's own calls — a dump slower than the budget still gets a
// full upload budget, and the report carries what the storage's calls took.
func TestStorageBudgetCountsOnlyStorageTime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newWorld(t)
		e, _ := budgetRunConfig(t, w, "1s")
		w.Sources["vpn"].DumpDelay = 1500 * time.Millisecond

		rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
		if err != nil {
			t.Fatal(err)
		}
		if rep.Outcome != model.OutcomeDelivered {
			t.Fatalf("outcome %s: the dump outlasted the storage budget and cost the upload", rep.Outcome)
		}
		got := storageOutcome(rep, "primary")
		if got.Outcome != model.StorageVerified {
			t.Fatalf("storage %+v", got)
		}
		if got.Duration.Std() < 0 || got.Duration.Std() > 1500*time.Millisecond {
			t.Fatalf("the recorded storage duration is %s, which is not the storage's own time", got.Duration)
		}
	})
}

// TestStorageBudgetResetsPerRun (A21): the clock lives in the run, not on the
// assembly.Assembly, so a second Run in one process starts with the full budget.
func TestStorageBudgetResetsPerRun(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newWorld(t)
		e, _ := budgetRunConfig(t, w, "1s")
		w.Sources["vpn"].DumpDelay = 1200 * time.Millisecond

		for i := range 2 {
			rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
			if err != nil {
				t.Fatal(err)
			}
			if rep.Outcome != model.OutcomeDelivered {
				t.Fatalf("run %d: outcome %s — a budget survived from the run before", i+1, rep.Outcome)
			}
		}
	})
}

// TestStorageBudgetExhaustionIsAnError (A21): the budget the config names is
// still a budget — a storage slower than it fails its calls rather than hanging
// the run.
func TestStorageBudgetExhaustionIsAnError(t *testing.T) {
	w := newWorld(t)
	e, cfg := budgetRunConfig(t, w, "1ns")
	rep, err := e.Run(context.Background(), backup.RunRequest{Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcome == model.OutcomeDelivered {
		t.Fatalf("an exhausted storage budget still delivered: %+v", rep.Storages)
	}
	if cfg["run"] == nil {
		t.Fatal("fixture drift")
	}
}
