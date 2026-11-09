package assembly

import (
	"fmt"
	"strings"
	"testing"
)

func TestR8_M11_SelectionIndices(t *testing.T) {
	for _, member := range []string{"bundles", "disabled"} {
		for _, bad := range []any{7, true, map[string]any{}, []any{}} {
			t.Run(fmt.Sprintf("%s/%T", member, bad), func(t *testing.T) {
				w := newWorld(t)
				cfg := baseConfig(t, map[string]any{"alpha": storageInstance("fake", "keep_last")})
				ref, kind := "source/ghost", "plugin"
				if member == "bundles" {
					ref, kind = "ghost_bundle", "bundle"
				}
				plugins := map[string]any{"bundles": []any{}, "disabled": []any{}, "config": map[string]any{}}
				plugins[member] = []any{bad, ref}
				cfg["plugins"] = plugins
				_, err := w.newAssemblyErr(t, cfg)
				if err == nil {
					t.Fatal("malformed config accepted")
				}
				want := fmt.Sprintf("/plugins/%s/1: unknown %s %q", member, kind, ref)
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("selection index not preserved: %v", err)
				}
				if len(w.Configs) != 0 || len(w.Processors) != 0 || len(w.Storages) != 0 {
					t.Fatal("refused config constructed instances")
				}
			})
		}
	}
}

func TestR8_M11_SentinelIsAReadableReference(t *testing.T) {
	for _, section := range []string{"sources", "stage.sources"} {
		t.Run(section, func(t *testing.T) {
			w := newWorld(t)
			cfg := baseConfig(t, map[string]any{"alpha": storageInstance("fake", "keep_last")})
			cfg["plugins"] = map[string]any{"bundles": []any{}, "disabled": []string{"source/ghost"}, "config": map[string]any{}}
			ref := "\x00malformed-pipeline-entry"
			si := sourceInstance("fake", "g", "n", []string{ref}, "required", "plain")
			if section == "sources" {
				cfg["sources"] = map[string]any{"alpha": si}
			} else {
				cfg["stage"] = map[string]any{"dir": t.TempDir(), "degraded_exit_code": 3, "failed_exit_code": 7, "sources": map[string]any{"alpha": si}}
			}
			_, err := w.newAssemblyErr(t, cfg)
			if err == nil {
				t.Fatal("invalid selection accepted")
			}
			want := fmt.Sprintf("/%s/alpha/pipeline/0: unknown processor instance %q", section, ref)
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("readable string reference hidden: %v", err)
			}
			if len(w.Configs) != 0 || len(w.Processors) != 0 || len(w.Storages) != 0 {
				t.Fatal("refused config constructed instances")
			}
		})
	}
}
