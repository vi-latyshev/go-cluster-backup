package assembly

import (
	"strings"
	"testing"
)

func TestR7_M11_PluginMembers(t *testing.T) {
	for _, key := range []string{"bundles", "config"} {
		t.Run(key, func(t *testing.T) {
			w := newWorld(t)
			cfg := baseConfig(t, map[string]any{"alpha": storageInstance("fake", "keep_last")})
			plugins := map[string]any{"bundles": []any{}, "config": map[string]any{}, "disabled": []string{"source/ghost"}}
			plugins[key] = 7
			cfg["plugins"] = plugins
			_, err := w.newAssemblyErr(t, cfg)
			if err == nil {
				t.Fatal("malformed plugins accepted")
			}
			if !strings.Contains(err.Error(), `unknown plugin "source/ghost"`) {
				t.Fatalf("readable disabled reference hidden: %v", err)
			}
			if len(w.Configs) != 0 || len(w.Processors) != 0 || len(w.Storages) != 0 {
				t.Fatal("refused config constructed instances")
			}
		})
	}
}
