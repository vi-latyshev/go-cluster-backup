package cli

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/configuration/defaults"
)

// TestR3_M4_DoctorClosesConstructedStorage is the reviewer's probe for M4: a
// storage doctor constructs is a closeable the invocation owner must release
// through the guarded destination exactly once.
func TestR3_M4_DoctorClosesConstructedStorage(t *testing.T) {
	w := newWorld()
	closes := 0
	var stdout, stderr bytes.Buffer
	Main(context.Background(),
		Options{Program: "backup", Factories: w.closingFactories(&closes), Defaults: [][]byte{defaults.Defaults, []byte(defaultsYAML)}},
		[]string{"doctor"}, &stdout, &stderr)
	if !strings.Contains(stdout.String(), "storage examples:") {
		t.Fatalf("doctor did not build examples: %s %s", stdout.String(), stderr.String())
	}
	if closes != 1 {
		t.Fatalf("doctor closed constructed storage %d times, want 1", closes)
	}
}

// TestR3_M11_MergeProblemKeepsOtherDiagnostics is the reviewer's probe for M11:
// a semantic merge problem must not short-circuit the decode and selection
// diagnostics the same file carries.
func TestR3_M11_MergeProblemKeepsOtherDiagnostics(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	body := `oops: true
plugins:
  disabled: [source/ghost]
  config:
    unknown_provider: {typo: true}
sources:
  thing:
    type: synthetic
    artifact: {group: g, name: n, importance: required, sensitivity: plain}
    pipeline: [ghost_processor]
    config: {payload: hi, dir: "", origin: "test:thing"}
storages:
` + exchangeYAML(filepath.Join(dir, "exchange")) + faultyYAML
	config := mustWrite(t, dir, "config.yaml", configYAML(dir, body))
	code, _, stderr := w.main(context.Background(), "validate", "--config", config)
	if code == 0 {
		t.Fatal("invalid configuration accepted")
	}
	for _, want := range []string{`unknown_provider`, `unknown object member name "oops"`, `unknown plugin "source/ghost"`} {
		if !strings.Contains(stderr, want) {
			t.Errorf("missing %q: %s", want, stderr)
		}
	}
}

// TestR3_M13_MergeProblemKeepsPureReferences is the reviewer's probe for the
// effective-config half of M11: the candidate keeps printing and the pure
// reference problems join the same answer.
func TestR3_M13_MergeProblemKeepsPureReferences(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	body := `plugins:
  disabled: [source/ghost]
  config:
    unknown_provider: {typo: true}
sources:
  thing:
    type: synthetic
    artifact: {group: g, name: n, importance: required, sensitivity: plain}
    pipeline: [ghost_processor]
    config: {payload: hi, dir: "", origin: "test:thing"}
storages:
` + exchangeYAML(filepath.Join(dir, "exchange")) + faultyYAML
	config := mustWrite(t, dir, "config.yaml", configYAML(dir, body))
	code, _, stderr := w.main(context.Background(), "config", "effective", "--config", config)
	if code == 0 {
		t.Fatal("invalid configuration accepted")
	}
	for _, want := range []string{`unknown_provider`, `unknown plugin "source/ghost"`, `unknown processor instance "ghost_processor"`} {
		if !strings.Contains(stderr, want) {
			t.Errorf("missing %q: %s", want, stderr)
		}
	}
}
