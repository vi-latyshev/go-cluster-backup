package cli

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	json "encoding/json/v2"

	"github.com/vi-latyshev/go-cluster-backup/internal/discovery"
	"github.com/vi-latyshev/go-cluster-backup/internal/model"
	"github.com/vi-latyshev/go-cluster-backup/internal/storage/storagetest"
)

// TestRunOneStorageDownIsDegraded is the end-to-end run: two storages, one of
// them down, and the run must report the configured degraded outcome while the
// other storage holds every object plus the manifest.
func TestRunOneStorageDownIsDegraded(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	root := filepath.Join(dir, "exchange")
	w.storage("offsite").Fault = down
	cfg := mustWrite(t, dir, "config.yaml",
		configYAML(dir, plainSourceYAML("hello")+zstdYAML+"storages:\n"+exchangeYAML(root)+faultyYAML+""))

	code, stdout, stderr := w.main(context.Background(), "run", "--kind", "scheduled", "--config", cfg)
	if code != 3 {
		t.Fatalf("exit code = %d, want the configured degraded code 3\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}
	if !strings.Contains(stdout, "degraded in ") {
		t.Fatalf("report does not say degraded:\n%s", stdout)
	}
	if !strings.Contains(stdout, "exchange: verified") || !strings.Contains(stdout, "offsite: failed") {
		t.Fatalf("report does not name both storage outcomes:\n%s", stdout)
	}

	// The local storage holds the artifact and the manifest, the manifest last.
	key := "runs/" + testRunID + "/g/n.dat.zst"
	if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(key))); err != nil {
		t.Errorf("artifact %s is not on the exchange storage: %v", key, err)
	}
	manifestPath := filepath.Join(root, "runs", testRunID, "manifest.json")
	var manifest model.RunReport
	if err := json.Unmarshal([]byte(mustRead(t, manifestPath)), &manifest); err != nil {
		t.Fatalf("manifest does not decode: %v", err)
	}
	if manifest.Schema != model.ManifestSchema || manifest.RunID != testRunID {
		t.Errorf("manifest identity = schema %d run %q, want schema %d run %q",
			manifest.Schema, manifest.RunID, model.ManifestSchema, testRunID)
	}
	if len(manifest.Artifacts) != 1 || manifest.Artifacts[0].Outcome != model.ArtifactOK || manifest.Artifacts[0].Key != key {
		t.Errorf("manifest does not record the artifact ok with its key: %+v", manifest.Artifacts)
	}

	// The down storage was tried and holds nothing: an endpoint that cannot
	// even be described fails before a single upload is attempted.
	off := w.storage("offsite")
	if keys := off.Keys(); len(keys) != 0 {
		t.Errorf("the down storage holds %v, want nothing", keys)
	}
	events := off.Events()
	if len(events) == 0 || events[0].Op != storagetest.OpDescribe {
		t.Errorf("the down storage was not tried: events %v", events)
	}
}

// TestRunAllStoragesDownIsFailed: with no verified copy of a required artifact
// the outcome is failed and the exit code is the one the config names.
func TestRunAllStoragesDownIsFailed(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	w.storage("offsite").Fault = down
	w.storage("exchange").Fault = down
	storages := `storages:
  exchange:
    type: faulty
    retention: {scheduled: {type: keep_last, config: {count: 3, min_keep: 1, count_incomplete: true}}}
    max_total_bytes: 1048576
    time_budget: 10s
    config: {}
` + faultyYAML
	cfg := mustWrite(t, dir, "config.yaml", configYAML(dir, plainSourceYAML("hello")+zstdYAML+storages+""))

	code, stdout, stderr := w.main(context.Background(), "run", "--kind", "scheduled", "--config", cfg)
	if code != 7 {
		t.Fatalf("exit code = %d, want the configured failed code 7\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}
	if !strings.Contains(stdout, "failed in ") {
		t.Fatalf("report does not say failed:\n%s", stdout)
	}
	if !strings.Contains(stdout, "no storage verified the artifact") {
		t.Fatalf("report does not name the reason:\n%s", stdout)
	}
}

// TestRunDryRunWritesNothing: --dry-run resolves everything and touches no
// storage.
func TestRunDryRunWritesNothing(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	root := filepath.Join(dir, "exchange")
	cfg := mustWrite(t, dir, "config.yaml",
		configYAML(dir, plainSourceYAML("hello")+zstdYAML+"storages:\n"+exchangeYAML(root)+faultyYAML+""))

	code, stdout, stderr := w.main(context.Background(), "run", "--kind", "scheduled", "--config", cfg, "--dry-run")
	if code != 0 {
		t.Fatalf("exit code = %d, want 0\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}
	if !strings.Contains(stdout, "delivered in ") {
		t.Fatalf("dry run did not resolve every artifact:\n%s", stdout)
	}
	if entries, err := os.ReadDir(root); err == nil && len(entries) != 0 {
		t.Errorf("dry run wrote %v", entries)
	}
	if keys := w.storage("offsite").Keys(); len(keys) != 0 {
		t.Errorf("dry run uploaded %v", keys)
	}
}

// TestStageWritesTheArtifact: the stage command leaves the sealed artifact in
// stage.dir under the name the run-side staged source reads back.
func TestStageWritesTheArtifact(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	staged := filepath.Join(dir, "staged")
	cfg := mustWrite(t, dir, "config.yaml", `version: 1
stage:
  dir: `+staged+`
  degraded_exit_code: 3
  failed_exit_code: 7
  sources:
    control:
      type: synthetic
      artifact: {group: control-plane, name: state, importance: required, sensitivity: plain}
      pipeline: []
      config: {payload: staged-bytes, dir: "", origin: "test:stage"}
`)

	code, stdout, stderr := w.main(context.Background(), "stage", "--config", cfg)
	if code != 0 {
		t.Fatalf("exit code = %d, want 0\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}
	if !strings.Contains(stdout, "stage delivered in ") {
		t.Fatalf("report does not name the stage outcome:\n%s", stdout)
	}
	if got := mustRead(t, filepath.Join(staged, "state.dat")); got != "staged-bytes" {
		t.Errorf("staged file = %q, want %q", got, "staged-bytes")
	}
}

// TestRunCancelledContextExitsNonZero: cancellation returns promptly and is
// never a zero exit.
func TestRunCancelledContextExitsNonZero(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	root := filepath.Join(dir, "exchange")
	cfg := mustWrite(t, dir, "config.yaml",
		configYAML(dir, plainSourceYAML("hello")+zstdYAML+"storages:\n"+exchangeYAML(root)+faultyYAML+""))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	code, _, stderr := w.main(ctx, "run", "--kind", "scheduled", "--config", cfg)
	if code == 0 {
		t.Fatalf("cancelled run exited 0\nstderr: %s", stderr)
	}
	if !strings.Contains(stderr, "context canceled") {
		t.Fatalf("cancellation is not named: %s", stderr)
	}
}

// TestUsageErrorsNameTheMissingFlag: nothing that changes behaviour has a
// default, so a missing flag is a usage error that names it.
func TestUsageErrorsNameTheMissingFlag(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"run without config", []string{"run"}, "--config"},
		{"stage without config", []string{"stage"}, "--config"},
		{"validate without config", []string{"validate"}, "--config"},
		{"unknown command", []string{"frobnicate"}, "unknown command"},
		{"unknown restore subcommand", []string{"restore", "frobnicate"}, "unknown subcommand"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newWorld()
			code, _, stderr := w.main(context.Background(), tc.args...)
			if code != exitUsage {
				t.Fatalf("exit code = %d, want %d", code, exitUsage)
			}
			if !strings.Contains(stderr, tc.want) {
				t.Fatalf("stderr does not name %q: %s", tc.want, stderr)
			}
		})
	}
}

// TestRunReportNamesExcludedObjectsOnlyAtDebug: the nightly report counts what
// the run decided not to back up, and names each object only at --log-level
// debug. A covered or ignored object is a decision, not a problem, so the info
// report must not read as one; an undeclared claim is a failed artifact instead.
func TestRunReportNamesExcludedObjectsOnlyAtDebug(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	root := filepath.Join(dir, "exchange")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	w.found["workloads"] = []discovery.Found{{
		Name:     "excluded_thing",
		Type:     "a-type-no-registry-knows",
		Artifact: model.Artifact{Group: "volumes", Name: "ns/media", Importance: model.ImportanceRequired, Sensitivity: model.SensitivityPlain},
		Excluded: model.ExclusionIgnored, ExcludedReason: "carries the exclusion marker backup.example/ignore=true",
	}}
	body := plainSourceYAML("hello") + "discoveries:\n  workloads:\n    type: fakediscovery\n    config: {}\n" +
		zstdYAML + "storages:\n" + exchangeYAML(root) + faultyYAML
	cfg := mustWrite(t, dir, "config.yaml", configYAML(dir, body))
	uniqueRunIDs(t, cfg) // this test runs the engine twice

	code, stdout, stderr := w.main(context.Background(), "run", "--kind", "scheduled", "--config", cfg)
	if code != 0 {
		t.Fatalf("run: exit %d\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}
	if !strings.Contains(stdout, "artifacts: 1 ok, 0 failed, 1 excluded") {
		t.Errorf("the report does not count the excluded object:\n%s", stdout)
	}
	if strings.Contains(stdout, "excluded ns/media") {
		t.Errorf("the info report names the excluded object:\n%s", stdout)
	}

	code, stdout, stderr = w.main(context.Background(), "run", "--kind", "scheduled", "--config", cfg, "--log-level", "debug")
	if code != 0 {
		t.Fatalf("debug run: exit %d\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}
	if !strings.Contains(stdout, "excluded ns/media (ignored): carries the exclusion marker backup.example/ignore=true") {
		t.Errorf("the debug report does not name the excluded object, so the info check proves nothing:\n%s", stdout)
	}
}

// TestIdentityFileAbsentCostsTheRunNothing: the age identities are the restore
// side's key material, and a backup pod never mounts the private key. Its
// absence must not stop a run from sealing — while `validate --preflight`, the
// command that asks whether the key material is at hand, names the file.
func TestIdentityFileAbsentCostsTheRunNothing(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	root := filepath.Join(dir, "exchange")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	missing := filepath.Join(dir, "no-such-identity.txt")
	body := fmt.Sprintf(`sources:
  secret_thing:
    type: synthetic
    artifact: {group: s, name: vpn, importance: required, sensitivity: secret}
    pipeline: [sealer]
    config: {payload: top-secret, dir: "", origin: "test:secret"}
processors:
  sealer:
    type: age
    config: {recipients: ["age1ql3z7hjy54pw3hyww5ayyfg7zqgvc7w3j2elw8zmrj2kg5sfn9aqmcac8p"], identities: [{file: %s}]}
storages:
  exchange:
    type: local
    retention: %s
    max_total_bytes: 1048576
    time_budget: 10s
    config: {root: %q, page_size: 100}
`+faultyYAML, missing, keepRetention, root)
	cfg := mustWrite(t, dir, "config.yaml", configYAML(dir, body))

	code, stdout, stderr := w.main(context.Background(), "run", "--kind", "scheduled", "--config", cfg)
	if code != 0 {
		t.Fatalf("a run whose identity file is absent: exit %d\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}
	if !strings.Contains(stdout, "delivered in ") {
		t.Fatalf("the run is not delivered:\n%s", stdout)
	}
	// The exchange storage is the real local plugin: what it wrote is on disk,
	// and the artifact's extension is the seal's own.
	var uploaded []string
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && !strings.HasSuffix(path, "/manifest.json") {
			uploaded = append(uploaded, path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(uploaded) != 1 || !strings.HasSuffix(uploaded[0], ".age") {
		t.Fatalf("uploaded %v, want one sealed artifact", uploaded)
	}

	code, stdout, _ = w.main(context.Background(), "validate", "--config", cfg, "--preflight")
	if code == 0 {
		t.Fatalf("preflight accepted a config whose identity file is absent:\n%s", stdout)
	}
	if !strings.Contains(stdout, missing) {
		t.Errorf("the preflight failure does not name the identity file:\n%s", stdout)
	}
}

// TestRunRefusesAManualRunWithNoManualRetention: the refusal happens before any
// plugin work, so the command reports it rather than an outcome.
func TestRunRefusesAManualRunWithNoManualRetention(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	root := filepath.Join(dir, "exchange")
	// A catalog that can hold the lease, and a storage whose only retention
	// partition is the scheduled one: the refusal is the missing partition.
	cfg := mustWrite(t, dir, "config.yaml",
		configYAML(dir, plainSourceYAML("hi")+zstdYAML+"catalog:\n  type: memory\n  config: {}\nstorages:\n"+exchangeYAML(root)+faultyYAML))
	code, _, stderr := w.main(context.Background(), "run", "--kind", "manual", "--config", cfg)
	if code != exitFailure || !strings.Contains(stderr, "retention/manual") {
		t.Fatalf("manual run without a manual partition: exit %d\nstderr: %s", code, stderr)
	}
}

// TestStageRefusesAConfigWithoutAStageSection: stage has no run id and no stage
// config to work from, so the command reports it.
func TestStageRefusesAConfigWithoutAStageSection(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	root := filepath.Join(dir, "exchange")
	cfg := mustWrite(t, dir, "config.yaml", configYAML(dir, plainBody(root)))
	code, _, stderr := w.main(context.Background(), "stage", "--config", cfg)
	if code != exitFailure || !strings.Contains(stderr, "stage") {
		t.Fatalf("stage without a stage section: exit %d\nstderr: %s", code, stderr)
	}
}
