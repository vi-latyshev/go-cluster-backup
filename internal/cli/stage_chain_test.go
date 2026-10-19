package cli

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"filippo.io/age"

	json "encoding/json/v2"

	"github.com/vi-latyshev/go-cluster-backup/internal/model"
)

// TestStageChainRoundTrip pins the control plane's whole path: the stage
// compresses and seals the artifact, the run's staged source records the stage's
// chain — the chain the bytes were actually produced with, not an empty chain of
// its own — and a restore fetch reverses it back to the original bytes.
//
// Mutant: a sidecar that records only the sealing step (the stage's zstd dropped
// from the manifest) leaves the fetch holding zstd-compressed bytes, and the
// byte comparison here fails.
func TestStageChainRoundTrip(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	staged := filepath.Join(dir, "staged")
	root := filepath.Join(dir, "exchange")

	identity, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	idPath := filepath.Join(dir, "identity.txt")
	if err := os.WriteFile(idPath, []byte(identity.String()+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	const payload = "control-plane-bytes"
	cfg := mustWrite(t, dir, "config.yaml", configYAML(dir, `
processors:
  squeeze:
    type: zstd
    config: {level: fastest, concurrency: 1, window_bytes: 1024, decode_window_bytes: 1024}
  seal:
    type: age
    config: {recipients: ["`+identity.Recipient().String()+`"], identities: [{file: `+idPath+`}]}
sources:
  control:
    type: staged
    artifact: {group: control-plane, name: state, sensitivity: secret}
    pipeline: []
    config: {dir: `+staged+`, name: state}
storages:
`+exchangeYAML(root)+faultyYAML+`
stage:
  dir: `+staged+`
  degraded_exit_code: 3
  failed_exit_code: 7
  sources:
    control:
      type: synthetic
      artifact: {group: control-plane, name: state, sensitivity: secret}
      pipeline: [squeeze, seal]
      config: {payload: `+payload+`, dir: "", origin: "test:stage"}
`))

	if code, stdout, stderr := w.main(context.Background(), "stage", "--config", cfg); code != 0 {
		t.Fatalf("stage exit code = %d\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}
	// The stage ran both processors: the file it published says so in its name.
	if _, err := os.Stat(filepath.Join(staged, "state.dat.zst.age")); err != nil {
		t.Fatalf("the stage did not publish the compressed, sealed artifact: %v", err)
	}

	if code, stdout, stderr := w.main(context.Background(), "run", "--kind", "scheduled", "--config", cfg); code != 0 {
		t.Fatalf("run exit code = %d\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}

	// The manifest carries the stage's chain: the run applied nothing itself, so
	// anything there is what the record said.
	var manifest model.RunReport
	if err := json.Unmarshal([]byte(mustRead(t, filepath.Join(root, "runs", testRunID, "manifest.json"))), &manifest); err != nil {
		t.Fatalf("manifest does not decode: %v", err)
	}
	if len(manifest.Artifacts) != 1 {
		t.Fatalf("manifest artifacts = %+v", manifest.Artifacts)
	}
	chain := manifest.Artifacts[0].Chain
	if len(chain) != 2 {
		t.Fatalf("the manifest's chain = %+v, want the stage's two steps", chain)
	}
	if chain[0].Instance != "squeeze" || chain[0].Type != "zstd" || chain[1].Instance != "seal" || chain[1].Type != "age" {
		t.Fatalf("the manifest's chain = %+v, want zstd then age", chain)
	}
	if chain[0].Ext != ".zst" || chain[1].Ext != ".age" || !chain[1].Seals {
		t.Fatalf("the manifest's chain steps = %+v, want .zst then a sealing .age", chain)
	}

	// The restore decodes with exactly that chain: age first, then zstd, back to
	// the bytes the stage's source dumped.
	out := filepath.Join(dir, "fetched")
	code, stdout, stderr := w.main(context.Background(), "restore", "fetch", "--config", cfg,
		"--run", testRunID, "--artifact", "state", "--out", out, "--plaintext-to-file")
	if code != 0 {
		t.Fatalf("fetch exit code = %d\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}
	if got := mustRead(t, out); got != payload {
		t.Fatalf("the fetched bytes = %q, want the staged payload %q", got, payload)
	}

	// The chain the restore used is the one the fetched artifact proves: the
	// stored object is compressed and sealed, and only the chain gets back.
	stored := filepath.Join(root, "runs", testRunID, "control-plane", "state.dat.zst.age")
	data, err := os.ReadFile(stored)
	if err != nil {
		t.Fatalf("the stored artifact: %v", err)
	}
	if strings.Contains(string(data), payload) {
		t.Fatal("the stored object carries the payload in the clear")
	}
}
