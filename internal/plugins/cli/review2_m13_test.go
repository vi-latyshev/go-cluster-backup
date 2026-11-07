package cli

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
)

// TestR2_M13_SelectionDoesNotHideOfflineReferences is the reviewer's probe for
// M13: a refused selection must not suppress the pure reference problems the
// same file carries; no instance is built to discover them.
func TestR2_M13_SelectionDoesNotHideOfflineReferences(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	body := `plugins:
  disabled: [source/ghost]
sources:
  thing:
    type: synthetic
    artifact: {group: g, name: n, importance: required, sensitivity: plain}
    pipeline: [ghost_processor]
    config: {payload: hi, dir: "", origin: "test:thing"}
storages:
` + exchangeYAML(filepath.Join(dir, "exchange")) + faultyYAML
	config := mustWrite(t, dir, "config.yaml", configYAML(dir, body))
	_, _, stderr := w.main(context.Background(), "validate", "--config", config)
	for _, want := range []string{`unknown plugin "source/ghost"`, `unknown processor instance "ghost_processor"`} {
		if !strings.Contains(stderr, want) {
			t.Errorf("missing %q: %s", want, stderr)
		}
	}
}
