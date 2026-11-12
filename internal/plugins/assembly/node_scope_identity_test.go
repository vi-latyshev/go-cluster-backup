package assembly

import (
	"strings"
	"testing"
)

// nodeScopedDiscovery answers the optional node-scope capability with the value
// a test configures; the plain fake has no such method at all.
type nodeScopedDiscovery struct {
	fakeDiscovery
	scoped bool
}

func (d *nodeScopedDiscovery) NodeScoped() bool { return d.scoped }

// TestNodeScopeRequiresAWriterIdentity: a node-scoped discovery narrows every
// writer to its own node, and protection tells one writer's runs from a peer's
// only by run.writer_identity. The configuration is refused offline when the
// scope is on and the identity is empty — validate and a run share this one
// construction path — and accepted with it.
func TestNodeScopeRequiresAWriterIdentity(t *testing.T) {
	config := func(t *testing.T, w *world, identity string) map[string]any {
		t.Helper()
		cfg := baseConfig(t, map[string]any{"primary": storageInstance("fake", "keep_last")})
		cfg["discoveries"] = map[string]any{"workloads": instance("fake")}
		cfg["run"].(map[string]any)["writer_identity"] = identity
		return cfg
	}

	t.Run("refused without an identity", func(t *testing.T) {
		w := newWorld(t)
		w.ExtraDiscoveries["workloads"] = &nodeScopedDiscovery{scoped: true}
		_, err := w.newAssemblyErr(t, config(t, w, ""))
		if err == nil {
			t.Fatal("a node-scoped discovery without a writer identity was accepted")
		}
		for _, want := range []string{"/discoveries/workloads", "node_scope", "run/writer_identity"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("the refusal does not name %q: %v", want, err)
			}
		}
	})

	t.Run("accepted with an identity", func(t *testing.T) {
		w := newWorld(t)
		w.ExtraDiscoveries["workloads"] = &nodeScopedDiscovery{scoped: true}
		w.newAssembly(t, config(t, w, "node-a"))
	})

	t.Run("a discovery without the scope needs no identity", func(t *testing.T) {
		w := newWorld(t)
		w.ExtraDiscoveries["workloads"] = &nodeScopedDiscovery{scoped: false}
		w.newAssembly(t, config(t, w, ""))
	})

	t.Run("a configuration without a run section has no identity", func(t *testing.T) {
		w := newWorld(t)
		w.ExtraDiscoveries["workloads"] = &nodeScopedDiscovery{scoped: true}
		cfg := config(t, w, "")
		delete(cfg, "run")
		_, err := w.newAssemblyErr(t, cfg)
		if err == nil || !strings.Contains(err.Error(), "run/writer_identity") {
			t.Fatalf("New = %v, want the missing-identity refusal", err)
		}
	})
}
