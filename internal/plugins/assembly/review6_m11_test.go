package assembly

import (
	"strings"
	"testing"
)

func TestR6_M11_ReadableMembers(t *testing.T) {
	for _, collection := range []string{"sources", "stage.sources", "storages"} {
		t.Run(collection, func(t *testing.T) {
			w := newWorld(t)
			cfg := baseConfig(t, map[string]any{"alpha": storageInstance("fake", "keep_last")})
			var want string
			if collection == "storages" {
				si := storageInstance("fake", "ghost_retention")
				si["time_budget"] = 7
				cfg["storages"] = map[string]any{"alpha": si}
				want = `/storages/alpha/retention/scheduled: unknown retention type "ghost_retention"`
			} else {
				si := sourceInstance("fake", "g", "n", []string{"ghost_processor"}, "required", "plain")
				si["artifact"] = 7
				if collection == "sources" {
					cfg["sources"] = map[string]any{"alpha": si}
				} else {
					cfg["stage"] = map[string]any{"dir": t.TempDir(), "degraded_exit_code": 3, "failed_exit_code": 7, "sources": map[string]any{"alpha": si}}
				}
				want = "/" + collection + `/alpha/pipeline/0: unknown processor instance "ghost_processor"`
			}
			_, err := w.newAssemblyErr(t, cfg)
			if err == nil {
				t.Fatal("malformed config accepted")
			}
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("readable member hidden: %v", err)
			}
			if len(w.Configs) != 0 || len(w.Processors) != 0 || len(w.Storages) != 0 {
				t.Fatal("refused config constructed instances")
			}
		})
	}
}
