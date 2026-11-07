package assembly

import (
	"context"
	"strings"
	"testing"
)

// TestR5_M11_MalformedEntryKeepsSiblingReferences is the reviewer probe: a
// malformed entry before a readable one does not hide the sibling's dangling
// pipeline reference, and nothing is constructed.
func TestR5_M11_MalformedEntryKeepsSiblingReferences(t *testing.T) {
	w := newWorld(t)
	cfg := baseConfig(t, map[string]any{"alpha": storageInstance("fake", "keep_last")})
	cfg["sources"] = map[string]any{
		"aaa": 7,
		"zzz": sourceInstance("fake", "g", "n", []string{"ghost_processor"}, "required", "plain"),
	}
	_, err := w.newAssemblyErr(t, cfg)
	if err == nil {
		t.Fatal("malformed source accepted")
	}
	_ = context.Background()
	if !strings.Contains(err.Error(), `/sources/zzz/pipeline/0: unknown processor instance "ghost_processor"`) {
		t.Fatalf("readable sibling reference hidden: %v", err)
	}
	if len(w.Configs) != 0 || len(w.Processors) != 0 || len(w.Storages) != 0 {
		t.Fatal("refused config constructed instances")
	}
}

// TestR5_M11_MalformedEntriesInEveryCollection: the same isolation holds for
// every reference-bearing collection, before and after the readable sibling,
// including stage sources and a storage whose retention alone is malformed.
func TestR5_M11_MalformedEntriesInEveryCollection(t *testing.T) {
	cases := []struct {
		name  string
		build func(t *testing.T, cfg map[string]any)
		want  string
	}{
		{
			name: "sources after",
			build: func(t *testing.T, cfg map[string]any) {
				cfg["sources"] = map[string]any{
					"zzz": sourceInstance("fake", "g", "n", []string{"ghost_processor"}, "required", "plain"),
					"aaa": 7,
				}
			},
			want: `/sources/zzz/pipeline/0: unknown processor instance "ghost_processor"`,
		},
		{
			name: "processors",
			build: func(t *testing.T, cfg map[string]any) {
				cfg["processors"] = map[string]any{"p": instance("nosuch"), "aaa": 7}
			},
			want: `/processors/p: unknown processor type`,
		},
		{
			name: "discoveries",
			build: func(t *testing.T, cfg map[string]any) {
				cfg["discoveries"] = map[string]any{"d": instance("nosuch"), "aaa": 7}
			},
			want: `/discoveries/d: unknown discovery type`,
		},
		{
			name: "notifiers",
			build: func(t *testing.T, cfg map[string]any) {
				cfg["notifiers"] = map[string]any{"n": instance("nosuch"), "aaa": 7}
			},
			want: `/notifiers/n: unknown notifier type`,
		},
		{
			name: "storages",
			build: func(t *testing.T, cfg map[string]any) {
				cfg["storages"] = map[string]any{
					"st":  storageInstance("nosuch", "keep_last"),
					"aaa": 7,
				}
			},
			want: `/storages/st: unknown storage type`,
		},
		{
			name: "storage retention",
			build: func(t *testing.T, cfg map[string]any) {
				cfg["storages"] = map[string]any{"st": map[string]any{
					"type": "fake", "time_budget": "10s", "max_total_bytes": int64(1) << 20,
					"call_parallelism": 1, "config": map[string]any{},
					"retention": map[string]any{
						"scheduled": map[string]any{"type": "nosuch", "config": map[string]any{}},
						"manual":    7,
					},
				}}
			},
			want: `/storages/st/retention/scheduled: unknown retention type`,
		},
		{
			name: "whole sources malformed",
			build: func(t *testing.T, cfg map[string]any) {
				cfg["sources"] = 7
				cfg["processors"] = map[string]any{"p": instance("nosuch")}
			},
			want: `/processors/p: unknown processor type`,
		},
		{
			name: "whole storages malformed",
			build: func(t *testing.T, cfg map[string]any) {
				cfg["storages"] = 7
				cfg["processors"] = map[string]any{"p": instance("nosuch")}
			},
			want: `/processors/p: unknown processor type`,
		},
		{
			name: "storage with another malformed member",
			build: func(t *testing.T, cfg map[string]any) {
				cfg["storages"] = map[string]any{
					"bad": map[string]any{"type": "fake", "max_total_bytes": "huge", "time_budget": "10s", "call_parallelism": 1, "config": map[string]any{}},
					"st":  storageInstance("nosuch", "keep_last"),
				}
			},
			want: `/storages/st: unknown storage type`,
		},
		{
			name: "whole stage malformed",
			build: func(t *testing.T, cfg map[string]any) {
				cfg["stage"] = 7
				cfg["processors"] = map[string]any{"p": instance("nosuch")}
			},
			want: `/processors/p: unknown processor type`,
		},
		{
			name: "stage sources",
			build: func(t *testing.T, cfg map[string]any) {
				cfg["stage"] = map[string]any{
					"dir": t.TempDir(), "degraded_exit_code": 3, "failed_exit_code": 7,
					"sources": map[string]any{
						"zzz": sourceInstance("fake", "g", "n", []string{"ghost_processor"}, "required", "plain"),
						"aaa": 7,
					},
				}
			},
			want: `/stage.sources/zzz/pipeline/0: unknown processor instance "ghost_processor"`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := newWorld(t)
			cfg := baseConfig(t, map[string]any{"alpha": storageInstance("fake", "keep_last")})
			cfg["plugins"] = map[string]any{
				"bundles": []any{}, "disabled": []any{"source/ghost"}, "config": map[string]any{},
			}
			tc.build(t, cfg)
			_, err := w.newAssemblyErr(t, cfg)
			if err == nil {
				t.Fatal("a malformed configuration was accepted")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("readable sibling reference hidden: %v", err)
			}
			if len(w.Configs) != 0 || len(w.Processors) != 0 || len(w.Storages) != 0 {
				t.Fatalf("refused config constructed instances: %v %v %v", w.Configs, w.Processors, w.Storages)
			}
		})
	}
}
