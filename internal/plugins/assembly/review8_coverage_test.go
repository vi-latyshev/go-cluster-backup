package assembly

import (
	"strings"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/configuration"
)

// TestR8_M11_KnownSelectionMembers: readable known ids and bundles survive the
// diagnostic filter; a repeat keeps the first entry and reports the second at
// its original index.
func TestR8_M11_KnownSelectionMembers(t *testing.T) {
	t.Run("disabled", func(t *testing.T) {
		w := newWorld(t)
		cfg := baseConfig(t, map[string]any{"alpha": storageInstance("fake", "keep_last")})
		cfg["sources"] = map[string]any{"aaa": 7, "zzz": sourceInstance("fake", "g", "n", nil, "required", "plain")}
		cfg["plugins"] = map[string]any{
			"bundles": []any{}, "config": map[string]any{},
			"disabled": []any{"source/fake", "source/fake"},
		}
		_, err := w.newAssemblyErr(t, cfg)
		if err == nil || !strings.Contains(err.Error(), `/plugins/disabled/1: plugin "source/fake" is listed twice`) {
			t.Fatalf("known disabled duplicate = %v", err)
		}
	})
	t.Run("bundles", func(t *testing.T) {
		w := newWorld(t)
		if err := loader.Add(w.Loader, loader.Spec{ID: "test.bundle"}, func(lr *loader.Registry) error {
			return configuration.AddBundle(lr, configuration.Bundle{Name: "known"})
		}); err != nil {
			t.Fatal(err)
		}
		cfg := baseConfig(t, map[string]any{"alpha": storageInstance("fake", "keep_last")})
		cfg["sources"] = map[string]any{"aaa": 7, "zzz": sourceInstance("fake", "g", "n", nil, "required", "plain")}
		cfg["plugins"] = map[string]any{
			"disabled": []any{}, "config": map[string]any{},
			"bundles": []any{"known", "known"},
		}
		_, err := w.newAssemblyErr(t, cfg)
		if err == nil || !strings.Contains(err.Error(), `/plugins/bundles/1: bundle "known" is listed twice`) {
			t.Fatalf("known bundle duplicate = %v", err)
		}
	})
}

// TestR8_M11_FallbackPipelines: when the raw document holds no indexed list for
// a source, the decoded pipeline is still diagnosed with its slice indices.
func TestR8_M11_FallbackPipelines(t *testing.T) {
	cfg := &configuration.Config{
		Processors: map[string]configuration.Instance{"known": {Type: "fake"}},
		Sources: map[string]configuration.SourceInstance{"s": {
			Type: "fake", Pipeline: []string{"ghost_processor", "known"},
		}},
	}
	cases := []struct {
		name string
		refs *diagnosticReferences
	}{
		{"no view", nil},
		{"no sections", &diagnosticReferences{pipelines: map[string]map[string][]indexedReference{}}},
		{"no entry", &diagnosticReferences{pipelines: map[string]map[string][]indexedReference{
			"sources": {"other": {{index: 0, name: "ghost_processor"}}},
		}}},
		{"empty list", &diagnosticReferences{pipelines: map[string]map[string][]indexedReference{
			"sources": {"s": {}},
		}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			errs := pureReferenceErrors(cfg, newWorld(t).Registry, configuration.ActiveFrom(nil), tc.refs)
			joined := ""
			for _, err := range errs {
				joined += err.Error() + "\n"
			}
			if tc.name == "empty list" {
				if strings.Contains(joined, "unknown processor instance") {
					t.Fatalf("empty indexed list fell back: %s", joined)
				}
				return
			}
			if !strings.Contains(joined, `/sources/s/pipeline/0: unknown processor instance "ghost_processor"`) {
				t.Fatalf("decoded pipeline not diagnosed: %s", joined)
			}
		})
	}
}
