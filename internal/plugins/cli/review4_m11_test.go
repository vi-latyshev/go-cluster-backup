package cli

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
)

// TestR4_M11_TypeErrorKeepsIndependentDiagnostics is the reviewer's probe for
// M11: a type error in one section (`catalog: 7`) must not hide the merge,
// selection and pure reference problems the rest of the document still carries.
func TestR4_M11_TypeErrorKeepsIndependentDiagnostics(t *testing.T) {
	for _, command := range [][]string{{"validate"}, {"config", "effective"}} {
		t.Run(strings.Join(command, "_"), func(t *testing.T) {
			w := newWorld()
			dir := t.TempDir()
			body := `catalog: 7
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
			path := mustWrite(t, dir, "config.yaml", configYAML(dir, body))
			args := append(command, "--config", path)
			code, _, stderr := w.main(context.Background(), args...)
			if code == 0 {
				t.Fatal("invalid config accepted")
			}
			for _, want := range []string{"unknown_provider", `unknown plugin "source/ghost"`, `unknown processor instance "ghost_processor"`} {
				if !strings.Contains(stderr, want) {
					t.Errorf("missing %q: %s", want, stderr)
				}
			}
		})
	}
}
