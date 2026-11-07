package cli

import (
	"context"
	"strings"
	"testing"
)

// TestR2_M11_MergeablePartialSettingsErrorKeepsCandidate is the reviewer's probe
// for M11: a partial-settings problem refuses the operation but must not
// withhold the merged candidate a reader needs to repair the file.
func TestR2_M11_MergeablePartialSettingsErrorKeepsCandidate(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	config := mustWrite(t, dir, "config.yaml", configYAML(dir, `plugins:
  config:
    unknown_provider: {typo: true}
`))
	code, stdout, stderr := w.main(context.Background(), "config", "effective", "--config", config, "--output", "json")
	if code == 0 || !strings.Contains(stderr, "unknown plugin") {
		t.Fatalf("expected diagnostic: %d %s", code, stderr)
	}
	if !strings.Contains(stdout, `"config"`) {
		t.Fatal("mergeable document withheld after partial-settings validation error")
	}
}
