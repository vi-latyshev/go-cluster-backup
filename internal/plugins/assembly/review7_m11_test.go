package assembly

import (
	"fmt"
	"strings"
	"testing"
)

func TestR7_M11_ReferenceLists(t *testing.T) {
	for _, collection := range []string{"sources", "stage.sources", "disabled", "bundles"} {
		for _, bad := range []any{7, true, map[string]any{}, []any{}} {
			for _, badFirst := range []bool{true, false} {
				t.Run(fmt.Sprintf("%s/%T/badFirst=%t", collection, bad, badFirst), func(t *testing.T) {
					w := newWorld(t)
					cfg := baseConfig(t, map[string]any{"alpha": storageInstance("fake", "keep_last")})
					ref := "ghost_processor"
					want := `unknown processor instance "ghost_processor"`
					if collection == "disabled" {
						ref = "source/ghost"
						want = `unknown plugin "source/ghost"`
					}
					if collection == "bundles" {
						ref = "ghost_bundle"
						want = `unknown bundle "ghost_bundle"`
					}
					refs := []any{ref, bad}
					if badFirst {
						refs = []any{bad, ref}
					}
					if collection == "disabled" || collection == "bundles" {
						cfg["plugins"] = map[string]any{"bundles": []any{}, "disabled": []any{}, "config": map[string]any{}, collection: refs}
					} else {
						si := sourceInstance("fake", "g", "n", []string{}, "required", "plain")
						si["pipeline"] = refs
						if collection == "sources" {
							cfg["sources"] = map[string]any{"alpha": si}
						} else {
							cfg["stage"] = map[string]any{"dir": t.TempDir(), "degraded_exit_code": 3, "failed_exit_code": 7, "sources": map[string]any{"alpha": si}}
						}
					}
					_, err := w.newAssemblyErr(t, cfg)
					if err == nil {
						t.Fatal("malformed config accepted")
					}
					if !strings.Contains(err.Error(), want) {
						t.Fatalf("readable reference hidden: %v", err)
					}
					if len(w.Configs) != 0 || len(w.Processors) != 0 || len(w.Storages) != 0 {
						t.Fatal("refused config constructed instances")
					}
				})
			}
		}
	}
}

// TestR7_M11_OriginalPipelineIndices: a malformed element keeps its JSON slot,
// so a readable reference after it is diagnosed at its original index.
func TestR7_M11_OriginalPipelineIndices(t *testing.T) {
	cases := []struct {
		name  string
		build func(t *testing.T, cfg map[string]any)
		want  string
	}{
		{
			name: "sources after a number",
			build: func(t *testing.T, cfg map[string]any) {
				si := sourceInstance("fake", "g", "n", []string{}, "required", "plain")
				si["pipeline"] = []any{7, "ghost_processor"}
				cfg["sources"] = map[string]any{"alpha": si}
			},
			want: `/sources/alpha/pipeline/1: unknown processor instance "ghost_processor"`,
		},
		{
			name: "stage sources before a bool",
			build: func(t *testing.T, cfg map[string]any) {
				si := sourceInstance("fake", "g", "n", []string{}, "required", "plain")
				si["pipeline"] = []any{"ghost_processor", true}
				cfg["stage"] = map[string]any{"dir": t.TempDir(), "degraded_exit_code": 3, "failed_exit_code": 7,
					"sources": map[string]any{"alpha": si}}
			},
			want: `/stage.sources/alpha/pipeline/0: unknown processor instance "ghost_processor"`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := newWorld(t)
			cfg := baseConfig(t, map[string]any{"alpha": storageInstance("fake", "keep_last")})
			tc.build(t, cfg)
			_, err := w.newAssemblyErr(t, cfg)
			if err == nil {
				t.Fatal("malformed config accepted")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("reference index not preserved: %v", err)
			}
			if len(w.Configs) != 0 || len(w.Processors) != 0 || len(w.Storages) != 0 {
				t.Fatal("refused config constructed instances")
			}
		})
	}
}

// TestR7_M11_SectionShapes: a whole malformed collection, a scalar pipeline and
// a scalar plugins block hide only themselves; the readable sibling sections and
// their references stay diagnosed.
func TestR7_M11_SectionShapes(t *testing.T) {
	cases := []struct {
		name  string
		build func(t *testing.T, cfg map[string]any)
		want  string
	}{
		{
			name: "processors is not a collection",
			build: func(t *testing.T, cfg map[string]any) {
				cfg["processors"] = 7
			},
			want: `/sources/vpn/pipeline/0: unknown processor instance "zstd3"`,
		},
		{
			name: "plugins is not an object",
			build: func(t *testing.T, cfg map[string]any) {
				cfg["plugins"] = 7
				cfg["processors"] = map[string]any{"p": instance("nosuch")}
			},
			want: `/processors/p: unknown processor type`,
		},
		{
			name: "pipeline is not a list",
			build: func(t *testing.T, cfg map[string]any) {
				si := sourceInstance("fake", "g", "n", []string{}, "required", "plain")
				si["pipeline"] = 7
				cfg["sources"] = map[string]any{"alpha": si}
				cfg["processors"] = map[string]any{"p": instance("nosuch")}
			},
			want: `/processors/p: unknown processor type`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := newWorld(t)
			cfg := baseConfig(t, map[string]any{"alpha": storageInstance("fake", "keep_last")})
			tc.build(t, cfg)
			_, err := w.newAssemblyErr(t, cfg)
			if err == nil {
				t.Fatal("malformed config accepted")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("readable reference hidden: %v", err)
			}
			if len(w.Configs) != 0 || len(w.Processors) != 0 || len(w.Storages) != 0 {
				t.Fatal("refused config constructed instances")
			}
		})
	}
}
