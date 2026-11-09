package cli

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
)

// TestValidateRefusesAPipelineOnARecordedChainSource: a source whose bytes were
// produced by another process (stage) applies no pipeline of its own — the
// record it carries is the chain. `validate` refuses the config, so the mistake
// never waits for 04:20 to fail the required artifact.
func TestValidateRefusesAPipelineOnARecordedChainSource(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	cfg := mustWrite(t, dir, "config.yaml", configYAML(dir, `
processors:
  squeeze:
    type: zstd
    config: {level: fastest, concurrency: 1, window_bytes: 1024, decode_window_bytes: 1024}
sources:
  control:
    type: staged
    artifact: {group: control-plane, name: state, sensitivity: secret}
    pipeline: [squeeze]
    config: {dir: `+filepath.Join(dir, "staged")+`, name: state}
storages:
`+exchangeYAML(filepath.Join(dir, "exchange"))+faultyYAML))

	code, _, stderr := w.main(context.Background(), "validate", "--config", cfg)
	if code != exitFailure {
		t.Fatalf("exit %d, want %d: a pipeline on a recorded-chain source was accepted\nstderr: %s", code, exitFailure, stderr)
	}
	if !strings.Contains(stderr, "/sources/control: the source carries the chain of the process that produced its bytes (stage); its pipeline must be empty, not [squeeze]") {
		t.Fatalf("the refusal does not name the source and its pipeline:\n%s", stderr)
	}
}

// TestValidateRefusesASecretStageSourceWithoutASeal: the stage's own source is
// checked the way the run's is — a secret artifact whose pipeline cannot seal
// would otherwise be refused at night, after the init container spent its time.
func TestValidateRefusesASecretStageSourceWithoutASeal(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	cfg := mustWrite(t, dir, "config.yaml", configYAML(dir, `
processors:
  squeeze:
    type: zstd
    config: {level: fastest, concurrency: 1, window_bytes: 1024, decode_window_bytes: 1024}
storages:
`+exchangeYAML(filepath.Join(dir, "exchange"))+faultyYAML+`
stage:
  dir: `+filepath.Join(dir, "staged")+`
  degraded_exit_code: 3
  failed_exit_code: 7
  sources:
    control:
      type: synthetic
      artifact: {group: control-plane, name: state, sensitivity: secret}
      pipeline: [squeeze]
      config: {payload: staged-bytes, dir: "", origin: "test:stage"}
`))

	code, _, stderr := w.main(context.Background(), "validate", "--config", cfg)
	if code != exitFailure {
		t.Fatalf("exit %d, want %d: a secret stage artifact without a sealing pipeline was accepted\nstderr: %s", code, exitFailure, stderr)
	}
	if !strings.Contains(stderr, "/stage.sources/control: a secret artifact's pipeline [squeeze] has no sealing processor") {
		t.Fatalf("the refusal does not name the stage source and its pipeline:\n%s", stderr)
	}
}
