package assembly

import (
	"strings"
	"testing"
)

// TestLayoutKeyRefusalIsNamed: a key template the checker refuses stops
// construction with the /layout/<field> path, so validate and run refuse the
// same file before any work.
func TestLayoutKeyRefusalIsNamed(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
	cfg["layout"].(map[string]any)["artifact_key"] = "{{ .Artifact.Grup }}/{{ .Artifact.Name }}"
	_, err := w.newAssemblyErr(t, cfg)
	if err == nil || !strings.Contains(err.Error(), "/layout/artifact_key") {
		t.Fatalf("New = %v, want the layout key refusal named", err)
	}
}
