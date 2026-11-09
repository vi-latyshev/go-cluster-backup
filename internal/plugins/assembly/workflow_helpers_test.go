package assembly

import (
	"testing"
)

// clearFailingLease is a catalog whose clear call fails: the record keeps the
// uncertainty this run armed, and the run must not fail over it.

// manualStorage is one storage that accepts both run kinds, with the given cap.
func manualStorage(cap int64) map[string]any {
	return map[string]any{
		"type": "fake",
		"retention": map[string]any{
			"scheduled": map[string]any{"type": "keep_last", "config": map[string]any{
				"count": 10, "min_keep": 1, "count_incomplete": true}},
			"manual": map[string]any{"type": "keep_last", "config": map[string]any{
				"count": 5, "min_keep": 1, "count_incomplete": true}},
		},
		"max_total_bytes": cap,
		"time_budget":     "10s", "config": map[string]any{},
		"call_parallelism": 1,
	}
}

func manualConfig(t *testing.T, storages map[string]any) map[string]any {
	t.Helper()
	cfg := baseConfig(t, storages)
	cfg["catalog"] = instance("fake")
	return cfg
}

func manualID(stamp string) string { return stamp + "-manual" }
