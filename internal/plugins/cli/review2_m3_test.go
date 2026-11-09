package cli

import (
	"context"
	"path/filepath"
	"testing"
)

// TestR2_M3_ExplicitZeroGraceCannotBypassChartGate is the reviewer's probe for
// M3: the chart always passes its configured value, so an explicit zero is a
// real promise and must be checked, not read as "the flag was omitted".
func TestR2_M3_ExplicitZeroGraceCannotBypassChartGate(t *testing.T) {
	w := newWorld()
	dir := t.TempDir()
	config := mustWrite(t, dir, "config.yaml", configYAML(dir, plainBody(filepath.Join(dir, "exchange"))))
	code, stdout, stderr := w.main(context.Background(), "validate", "--config", config, "--termination-grace-seconds", "0")
	if code == 0 {
		t.Fatalf("explicit zero grace accepted: %s %s", stdout, stderr)
	}
}
