package kuberneteslabels_test

import (
	"bytes"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"testing"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/discovery"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/human"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/kubeapi/kubeapitest"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
)

// testNodeEnv is the environment variable a node-scoped config names; the
// chart fills the same role with BACKUP_NODE_NAME.
const testNodeEnv = "GCB_TEST_NODE"

// nodeJSON is one Node the fake API answers for: the name, and the labels a
// required node affinity matches.
func nodeJSON(name string, labels map[string]string) string {
	return mustJSON(map[string]any{
		"metadata": map[string]any{"name": name, "labels": labels},
	})
}

func hostnameLabels(node string) map[string]string {
	return map[string]string{"kubernetes.io/hostname": node}
}

// pinnedPV is a volume pinned to one node by required node affinity, the shape
// the local-path provisioner writes.
func pinnedPV(name, claimNamespace, claimName, path, node string) string {
	return fmt.Sprintf(`{"metadata":{"name":%q},"spec":{"local":{"path":%q},`+
		`"nodeAffinity":{"required":{"nodeSelectorTerms":[{"matchExpressions":[{"key":"kubernetes.io/hostname","operator":"In","values":[%q]}]}]}},`+
		`"claimRef":{"namespace":%q,"name":%q,"uid":%q}}}`,
		name, path, node, claimNamespace, claimName, claimUID(claimNamespace, claimName))
}

// unpinnedHostPathPV is a volume with no affinity: the node whose directory it
// is is proven only by the Running pod that mounts it.
func unpinnedHostPathPV(name, claimNamespace, claimName, path string) string {
	return fmt.Sprintf(`{"metadata":{"name":%q},"spec":{"hostPath":{"path":%q},`+
		`"claimRef":{"namespace":%q,"name":%q,"uid":%q}}}`,
		name, path, claimNamespace, claimName, claimUID(claimNamespace, claimName))
}

// scheduledPod is a pod with the node it runs on filled in, mounting the named
// claims; it is how a fixture states where the data lives.
func scheduledPod(namespace, name string, labels map[string]string, phase, node string, claims ...string) string {
	var p objectFixture
	p.Metadata.Name = name
	p.Metadata.Namespace = namespace
	p.Metadata.Labels = labels
	p.Spec.NodeName = node
	p.Status.Phase = phase
	for _, claim := range claims {
		var v volumeFixture
		v.Name = "volume-" + claim
		v.PersistentVolumeClaim.ClaimName = claim
		p.Spec.Volumes = append(p.Spec.Volumes, v)
	}
	return mustJSON(p)
}

// nodeScoped builds a discoverer whose node_scope names the given writers; the
// process is node-a.
func nodeScoped(t *testing.T, srv *kubeapitest.Server, writers []string, rules ...ruleFixture) (*instances.Registry, jsontext.Value) {
	t.Helper()
	opts := srv.Connection(t, testHostEnv, testPortEnv)
	cfg := configFixture{
		APIHostEnv: testHostEnv, APIPortEnv: testPortEnv,
		TokenFile: opts.TokenFile, CAFile: opts.CAFile,
		RequestTimeout: "10s", SecretsDir: t.TempDir(), ListPageSize: 500,
		Exclude:     excludeFixture{Label: testExcludeLabel, Value: testExcludeValue},
		Undeclared:  undeclaredVolume(),
		Rules:       rules,
		NodeEnv:     testNodeEnv,
		NodeScope:   true,
		WriterNodes: writers,
	}
	return registry(t), jsontext.Value(mustJSON(cfg))
}

func buildNodeScoped(t *testing.T, srv *kubeapitest.Server, writers []string, rules ...ruleFixture) discovery.Discoverer {
	t.Helper()
	reg, raw := nodeScoped(t, srv, writers, rules...)
	d, err := instances.Build(t.Context(), reg, discovery.Kind, "kubernetes_labels", raw,
		instances.Deps{Instance: "workloads", Log: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// TestNodeScopedCapability: the optional capability assembly asserts answers
// the config's own opt-in before any listing, so a node-scoped run can be
// required to carry the stable writer identity peer accounting needs.
func TestNodeScopedCapability(t *testing.T) {
	rule := simpleRule("databases", "pods", "backup.example/type=database", "Running")
	plain := newDiscoverer(t, kubeapitest.New(t), rule)
	capability, ok := plain.(discovery.NodeScoped)
	if !ok || capability.NodeScoped() {
		t.Fatalf("plain discovery = (%v, %v), want the capability reported as off", capability, ok)
	}
	scoped := buildNodeScoped(t, kubeapitest.New(t), []string{"node-a", "node-b"}, rule)
	capability, ok = scoped.(discovery.NodeScoped)
	if !ok || !capability.NodeScoped() {
		t.Fatalf("node-scoped discovery = (%v, %v), want the capability reported as on", capability, ok)
	}
}

// TestReviewAmbiguousAffinityRequiresMountingProof is the reviewer's
// counterexample: a volume whose required affinity admits both this writer's
// node and a declared peer's must never read this node's directory on the
// affinity alone — only the Running pod that mounts it can place the volume.
func TestReviewAmbiguousAffinityRequiresMountingProof(t *testing.T) {
	ambiguous := `{"key":"kubernetes.io/hostname","operator":"In","values":["node-a","node-b"]}`
	cases := []struct {
		name     string
		expr     string
		terms    string
		mountOn  string
		verdict  string
		mentions string
	}{
		{name: "no mounting pod proves nothing", expr: ambiguous, verdict: "failed", mentions: "admits nodes"},
		{name: "mounted on this node reads it", expr: ambiguous, mountOn: "node-a", verdict: "ok"},
		{name: "mounted on the peer delegates it", expr: ambiguous, mountOn: "node-b", verdict: "elsewhere"},
		{name: "mounted outside the admitted set conflicts", expr: ambiguous, mountOn: "node-c", verdict: "failed", mentions: "placement conflicts"},
		// A label selector may admit a node nobody declared: it cannot place
		// the volume on its own, however many declared nodes it happens to
		// match.
		{
			name:     "a label selector admitting an undeclared node needs mounting proof",
			expr:     `{"key":"disk","operator":"In","values":["ssd"]}`,
			verdict:  "failed",
			mentions: "narrow its required node affinity to a node in writer_nodes",
		},
		{
			name:    "a label selector mounted on this node reads",
			expr:    `{"key":"disk","operator":"In","values":["ssd"]}`,
			mountOn: "node-a",
			verdict: "ok",
		},
		{
			name:     "hostname values admitting an undeclared node fail without a mounting pod",
			expr:     `{"key":"kubernetes.io/hostname","operator":"In","values":["node-a","node-c"]}`,
			verdict:  "failed",
			mentions: "narrow its required node affinity to a node in writer_nodes",
		},
		// Terms pinning declared writers only are decided among writer_nodes,
		// with no mounting pod needed.
		{name: "a single pin to this node reads it", expr: `{"key":"kubernetes.io/hostname","operator":"In","values":["node-a"]}`, verdict: "ok"},
		{
			name: "a term pinned to an undeclared node needs mounting proof",
			terms: `{"matchExpressions":[{"key":"kubernetes.io/hostname","operator":"In","values":["node-a"]}]},` +
				`{"matchExpressions":[{"key":"kubernetes.io/hostname","operator":"In","values":["node-c"]}]}`,
			verdict:  "failed",
			mentions: "narrow its required node affinity to a node in writer_nodes",
		},
		{
			name: "pins on two declared writers still need mounting proof",
			terms: `{"matchExpressions":[{"key":"kubernetes.io/hostname","operator":"In","values":["node-a"]}]},` +
				`{"matchExpressions":[{"key":"kubernetes.io/hostname","operator":"In","values":["node-b"]}]}`,
			verdict:  "failed",
			mentions: "admits nodes",
		},
		{name: "a single pin to a declared peer delegates it", expr: `{"key":"kubernetes.io/hostname","operator":"In","values":["node-b"]}`, verdict: "elsewhere"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(testNodeEnv, "node-a")
			srv := kubeapitest.New(t)
			srv.AddNode(nodeJSON("node-a", map[string]string{"kubernetes.io/hostname": "node-a", "disk": "ssd"}))
			srv.AddNode(nodeJSON("node-b", hostnameLabels("node-b")))
			files := map[string]string{"backup.example/engine": "files"}
			if tc.mountOn != "" {
				srv.AddPod(scheduledPod("ns", "app-0", nil, "Running", tc.mountOn, "media"))
			}
			srv.AddPVC(boundPVC("ns", "media", files, "Bound", "pv-media"))
			terms := tc.terms
			if terms == "" {
				terms = fmt.Sprintf(`{"matchExpressions":[%s]}`, tc.expr)
			}
			srv.AddPV(fmt.Sprintf(`{"metadata":{"name":"pv-media"},"spec":{"local":{"path":"/mnt/local-data"},`+
				`"nodeAffinity":{"required":{"nodeSelectorTerms":[%s]}},`+
				`"claimRef":{"namespace":"ns","name":"media","uid":%q}}}`, terms, claimUID("ns", "media")))
			rule := simpleRule("volumes", "persistentvolumeclaims", "backup.example/engine=files", "Bound")
			rule.Source.Config = map[string]any{"root": "{{ volumePath }}", "ignore_vanished": true}
			d := buildNodeScoped(t, srv, []string{"node-a", "node-b"}, rule)

			found := discover(t, d)
			if len(found) != 1 {
				t.Fatalf("found %d claims, want 1: %+v", len(found), found)
			}
			f := found[0]
			switch tc.verdict {
			case "ok":
				if f.Problem != "" || f.Excluded != "" || !strings.Contains(string(f.Config), "/mnt/local-data") {
					t.Fatalf("claim = %+v, want the mounting pod's own node to prove the read", f)
				}
			case "elsewhere":
				if f.Excluded != model.ExclusionElsewhere || f.ExcludedNode != "node-b" {
					t.Fatalf("claim = %+v, want the peer's delegation", f)
				}
			default:
				if f.Problem == "" || !strings.Contains(f.Problem, tc.mentions) {
					t.Fatalf("claim = %+v, want a failure containing %q", f, tc.mentions)
				}
			}
		})
	}
}

// TestReviewPendingBindingStillProvesNode is the reviewer's second
// counterexample: a Pending claim that already names a volume must not bypass
// the node proof just because its phase is not the rule's Bound. The volume is
// a peer's, so the template's path is refused.
func TestReviewPendingBindingStillProvesNode(t *testing.T) {
	t.Setenv(testNodeEnv, "node-a")
	srv := kubeapitest.New(t)
	srv.AddNode(nodeJSON("node-a", hostnameLabels("node-a")))
	srv.AddNode(nodeJSON("node-b", hostnameLabels("node-b")))
	labels := map[string]string{"backup.example/engine": "files"}
	srv.AddPVC(boundPVC("ns", "media", labels, "Pending", "pv-media"))
	srv.AddPV(pinnedPV("pv-media", "ns", "media", "/mnt/local-data", "node-b"))
	rule := simpleRule("volumes", "persistentvolumeclaims", "backup.example/engine=files", "Pending")
	rule.Source.Config = map[string]any{"root": "{{ volumePath }}", "ignore_vanished": true}
	d := buildNodeScoped(t, srv, []string{"node-a", "node-b"}, rule)

	found := discover(t, d)
	if len(found) != 1 || (found[0].Problem == "" && found[0].Excluded == "") {
		t.Fatalf("wrong-node Pending binding authorized local read: %+v", found)
	}
	if found[0].Problem == "" || !strings.Contains(found[0].Problem, "declared peer writer's node") {
		t.Fatalf("claim = %+v, want the peer's node named in the refusal", found[0])
	}
}

// TestReviewPendingBindingReadsItsOwnNode: the resolved placement is what a
// Pending claim's path is read by — its own node's volume is still this
// writer's, and the phase no longer decides the verdict.
func TestReviewPendingBindingReadsItsOwnNode(t *testing.T) {
	t.Setenv(testNodeEnv, "node-a")
	srv := kubeapitest.New(t)
	srv.AddNode(nodeJSON("node-a", hostnameLabels("node-a")))
	srv.AddNode(nodeJSON("node-b", hostnameLabels("node-b")))
	labels := map[string]string{"backup.example/engine": "files"}
	srv.AddPVC(boundPVC("ns", "media", labels, "Pending", "pv-media"))
	srv.AddPV(pinnedPV("pv-media", "ns", "media", "/mnt/local-data", "node-a"))
	rule := simpleRule("volumes", "persistentvolumeclaims", "backup.example/engine=files", "Pending")
	rule.Source.Config = map[string]any{"root": "{{ volumePath }}", "ignore_vanished": true}
	d := buildNodeScoped(t, srv, []string{"node-a", "node-b"}, rule)

	found := discover(t, d)
	if len(found) != 1 || found[0].Problem != "" || !strings.Contains(string(found[0].Config), "/mnt/local-data") {
		t.Fatalf("claim = %+v, want its own node's volume read", found)
	}
}

// TestReviewPendingUnboundClaimRefused: a Pending claim that names no volume
// has nothing a template can place, and asking volumePath says so rather than
// rendering a path nobody proved.
func TestReviewPendingUnboundClaimRefused(t *testing.T) {
	t.Setenv(testNodeEnv, "node-a")
	srv := kubeapitest.New(t)
	srv.AddNode(nodeJSON("node-a", hostnameLabels("node-a")))
	labels := map[string]string{"backup.example/engine": "files"}
	srv.AddPVC(pvc("ns", "media", labels, "Pending"))
	rule := simpleRule("volumes", "persistentvolumeclaims", "backup.example/engine=files", "Pending")
	rule.Source.Config = map[string]any{"root": "{{ volumePath }}", "ignore_vanished": true}
	d := buildNodeScoped(t, srv, []string{"node-a", "node-b"}, rule)

	found := discover(t, d)
	if len(found) != 1 || !strings.Contains(found[0].Problem, "bound to no volume") {
		t.Fatalf("claim = %+v, want the unbound Pending claim refused", found)
	}
}

// buildNodeAware is a discoverer that proves this pod's node without scoping:
// no declared writer set delegates anything, and the node still has to be
// readable wherever a local path is read.
func buildNodeAware(t *testing.T, srv *kubeapitest.Server, rules ...ruleFixture) discovery.Discoverer {
	t.Helper()
	opts := srv.Connection(t, testHostEnv, testPortEnv)
	cfg := configFixture{
		APIHostEnv: testHostEnv, APIPortEnv: testPortEnv,
		TokenFile: opts.TokenFile, CAFile: opts.CAFile,
		RequestTimeout: "10s", SecretsDir: t.TempDir(), ListPageSize: 500,
		Exclude:    excludeFixture{Label: testExcludeLabel, Value: testExcludeValue},
		Undeclared: undeclaredVolume(),
		Rules:      rules,
		NodeEnv:    testNodeEnv,
	}
	d, err := instances.Build(t.Context(), registry(t), discovery.Kind, "kubernetes_labels",
		jsontext.Value(mustJSON(cfg)), instances.Deps{Instance: "workloads"})
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// TestReviewAmbiguousAffinityWithoutNodeScope is the coordinator's addendum:
// outside node scope there is no declared set to read, so an affinity that is
// not provably exclusive to this writer's node must not authorize the local
// read on its own — the single Running mounting pod decides, and absent that
// evidence the artifact fails with the deployment's remedy.
func TestReviewAmbiguousAffinityWithoutNodeScope(t *testing.T) {
	cases := []struct {
		name     string
		expr     string
		terms    string
		mountOn  string
		verdict  string
		mentions string
	}{
		{
			name:     "an ambiguous affinity with no mounting pod fails",
			expr:     `{"key":"kubernetes.io/hostname","operator":"In","values":["node-a","node-b"]}`,
			verdict:  "failed",
			mentions: "narrow its required node affinity to one node, or enable node_scope with writer_nodes",
		},
		{
			name:    "an ambiguous affinity mounted here reads",
			expr:    `{"key":"kubernetes.io/hostname","operator":"In","values":["node-a","node-b"]}`,
			mountOn: "node-a",
			verdict: "ok",
		},
		{
			name:     "an ambiguous affinity mounted elsewhere fails naming the node",
			expr:     `{"key":"kubernetes.io/hostname","operator":"In","values":["node-a","node-b"]}`,
			mountOn:  "node-b",
			verdict:  "failed",
			mentions: `"node-b"`,
		},
		{
			name:     "a selector that does not name the node needs mounting proof",
			expr:     `{"key":"cpu","operator":"Gt","values":["4"]}`,
			verdict:  "failed",
			mentions: "enable node_scope with writer_nodes",
		},
		{
			name:    "a single hostname value proves the node alone",
			expr:    `{"key":"kubernetes.io/hostname","operator":"In","values":["node-a"]}`,
			verdict: "ok",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(testNodeEnv, "node-a")
			srv := kubeapitest.New(t)
			srv.AddNode(nodeJSON("node-a", map[string]string{
				"kubernetes.io/hostname": "node-a",
				"cpu":                    "8",
			}))
			srv.AddNode(nodeJSON("node-b", hostnameLabels("node-b")))
			labels := map[string]string{"backup.example/engine": "files"}
			if tc.mountOn != "" {
				srv.AddPod(scheduledPod("ns", "app-0", nil, "Running", tc.mountOn, "media"))
			}
			srv.AddPVC(boundPVC("ns", "media", labels, "Bound", "pv-media"))
			srv.AddPV(fmt.Sprintf(`{"metadata":{"name":"pv-media"},"spec":{"local":{"path":"/mnt/local-data"},`+
				`"nodeAffinity":{"required":{"nodeSelectorTerms":[{"matchExpressions":[%s]}]}},`+
				`"claimRef":{"namespace":"ns","name":"media","uid":%q}}}`, tc.expr, claimUID("ns", "media")))
			rule := simpleRule("volumes", "persistentvolumeclaims", "backup.example/engine=files", "Bound")
			rule.Source.Config = map[string]any{"root": "{{ volumePath }}", "ignore_vanished": true}
			d := buildNodeAware(t, srv, rule)

			found := discover(t, d)
			if len(found) != 1 {
				t.Fatalf("found %d claims, want 1: %+v", len(found), found)
			}
			f := found[0]
			if tc.verdict == "ok" {
				if f.Problem != "" || f.Excluded != "" || !strings.Contains(string(f.Config), "/mnt/local-data") {
					t.Fatalf("claim = %+v, want the placement proven and the path read", f)
				}
				return
			}
			if f.Problem == "" || !strings.Contains(f.Problem, tc.mentions) {
				t.Fatalf("claim = %+v, want a failure containing %q", f, tc.mentions)
			}
			if f.Type != "" {
				t.Fatalf("an unproven volume built a source: %+v", f)
			}
		})
	}
}

// TestNodeScopeDelegatesAPodOnADeclaredPeer: a pod on this writer's node is
// rendered, while a pod a peer writer's node runs is one elsewhere exclusion
// naming that node — a declared delegation, never a silent skip and never a
// second copy.
func TestNodeScopeDelegatesAPodOnADeclaredPeer(t *testing.T) {
	t.Setenv(testNodeEnv, "node-a")
	srv := kubeapitest.New(t)
	srv.AddNode(nodeJSON("node-a", hostnameLabels("node-a")))
	srv.AddNode(nodeJSON("node-b", hostnameLabels("node-b")))
	srv.AddPod(scheduledPod("ns", "db-0", map[string]string{"backup.example/type": "database"}, "Running", "node-a"))
	srv.AddPod(scheduledPod("ns", "db-1", map[string]string{"backup.example/type": "database"}, "Running", "node-b"))
	d := buildNodeScoped(t, srv, []string{"node-a", "node-b"},
		simpleRule("databases", "pods", "backup.example/type=database", "Running"))

	if err := check(t, d); err != nil {
		t.Fatalf("Check = %v, want a readable own node", err)
	}
	found := discover(t, d)
	if len(found) != 2 {
		t.Fatalf("found %d objects, want 2: %+v", len(found), found)
	}
	own := found[0]
	if own.Name != "databases/ns/db-0" || own.Problem != "" || own.Excluded != "" {
		t.Fatalf("this writer's pod = %+v, want the ordinary render", own)
	}
	f := found[1]
	if f.Excluded != model.ExclusionElsewhere {
		t.Fatalf("verdict = %q, want elsewhere: %+v", f.Excluded, f)
	}
	if f.ExcludedNode != "node-b" {
		t.Fatalf("ExcludedNode = %q, want node-b", f.ExcludedNode)
	}
	if f.Problem != "" || !strings.Contains(f.ExcludedReason, "node-b") {
		t.Fatalf("elsewhere record = %+v", f)
	}
}

// TestNodeScopeFailsAPodOutsideWriterNodes: a pod on a node nobody declared is
// one failed artifact with its rule's importance. The global match satisfied
// min_matches; it proves the label exists, not that a declared writer backs the
// data up.
func TestNodeScopeFailsAPodOutsideWriterNodes(t *testing.T) {
	t.Setenv(testNodeEnv, "node-a")
	srv := kubeapitest.New(t)
	srv.AddNode(nodeJSON("node-a", hostnameLabels("node-a")))
	srv.AddNode(nodeJSON("node-b", hostnameLabels("node-b")))
	srv.AddPod(scheduledPod("ns", "db-0", map[string]string{"backup.example/type": "database"}, "Running", "node-c"))
	rule := simpleRule("databases", "pods", "backup.example/type=database", "Running")
	rule.MinMatches = 1
	d := buildNodeScoped(t, srv, []string{"node-a", "node-b"}, rule)

	if err := check(t, d); err != nil {
		t.Fatalf("Check = %v, want the listing count to pass", err)
	}
	found := discover(t, d)
	if len(found) != 1 || found[0].Problem == "" {
		t.Fatalf("found = %+v, want one failed artifact", found)
	}
	f := found[0]
	if f.Excluded != "" {
		t.Fatalf("an out-of-scope object became an exclusion: %+v", f)
	}
	for _, want := range []string{"node-c", "writer_nodes", "node_scope"} {
		if !strings.Contains(f.Problem, want) {
			t.Errorf("Problem %q does not name %q", f.Problem, want)
		}
	}
	if f.Artifact.Importance != model.ImportanceRequired {
		t.Errorf("importance = %q, want the rule's required", f.Artifact.Importance)
	}
}

// TestNodeScopeFailsAnUnscheduledPod: node scope cannot prove whose data a pod
// that is on no node carries, and an empty placement is not evidence of nothing
// to back up.
func TestNodeScopeFailsAnUnscheduledPod(t *testing.T) {
	t.Setenv(testNodeEnv, "node-a")
	srv := kubeapitest.New(t)
	srv.AddNode(nodeJSON("node-a", hostnameLabels("node-a")))
	srv.AddNode(nodeJSON("node-b", hostnameLabels("node-b")))
	srv.AddPod(scheduledPod("ns", "db-0", map[string]string{"backup.example/type": "database"}, "Running", ""))
	d := buildNodeScoped(t, srv, []string{"node-a", "node-b"},
		simpleRule("databases", "pods", "backup.example/type=database", "Running"))

	found := discover(t, d)
	if len(found) != 1 || !strings.Contains(found[0].Problem, "not scheduled on a node") {
		t.Fatalf("found = %+v, want the unscheduled refusal", found)
	}
}

// TestNodeScopeRefusesWhenItsOwnNodeCannotBeProven: the discovery's own node
// must be named, readable and declared. Any of the three missing refuses the
// whole discovery — widening to cluster scope is the silent wrong-node backup
// node scope exists to prevent.
func TestNodeScopeRefusesWhenItsOwnNodeCannotBeProven(t *testing.T) {
	rule := simpleRule("databases", "pods", "backup.example/type=database", "Running")

	t.Run("the environment is unset", func(t *testing.T) {
		srv := kubeapitest.New(t)
		srv.AddNode(nodeJSON("node-a", hostnameLabels("node-a")))
		d := buildNodeScoped(t, srv, []string{"node-a", "node-b"}, rule)
		_, err := d.Discover(t.Context())
		if err == nil || !strings.Contains(err.Error(), testNodeEnv) || !strings.Contains(err.Error(), "node_scope") {
			t.Fatalf("Discover = %v, want the empty environment named", err)
		}
		if err := check(t, d); err == nil {
			t.Fatal("Check = nil, want the same refusal")
		}
	})

	t.Run("the node is not readable", func(t *testing.T) {
		t.Setenv(testNodeEnv, "node-a")
		srv := kubeapitest.New(t)
		srv.Deny("nodes")
		d := buildNodeScoped(t, srv, []string{"node-a", "node-b"}, rule)
		_, err := d.Discover(t.Context())
		if err == nil || !strings.Contains(err.Error(), "cannot prove this pod's node") || !strings.Contains(err.Error(), "nodes") {
			t.Fatalf("Discover = %v, want the unreadable node and the remedy named", err)
		}
	})

	t.Run("the node does not exist", func(t *testing.T) {
		t.Setenv(testNodeEnv, "node-a")
		srv := kubeapitest.New(t)
		d := buildNodeScoped(t, srv, []string{"node-a", "node-b"}, rule)
		_, err := d.Discover(t.Context())
		if err == nil || !strings.Contains(err.Error(), "has no such node") || !strings.Contains(err.Error(), `"node-a"`) {
			t.Fatalf("Discover = %v, want the wrong name told apart from a missing grant", err)
		}
	})

	t.Run("the node is not a declared writer", func(t *testing.T) {
		t.Setenv(testNodeEnv, "node-a")
		srv := kubeapitest.New(t)
		srv.AddNode(nodeJSON("node-a", hostnameLabels("node-a")))
		srv.AddNode(nodeJSON("node-b", hostnameLabels("node-b")))
		d := buildNodeScoped(t, srv, []string{"node-b"}, rule)
		_, err := d.Discover(t.Context())
		if err == nil || !strings.Contains(err.Error(), "not in writer_nodes") || !strings.Contains(err.Error(), "node-a") {
			t.Fatalf("Discover = %v, want the undeclared own node named", err)
		}
	})
}

// TestNodeConfigRefusals: node scope without the environment that names the
// node, without a declared writer set, or writer_nodes that no scope uses are
// config errors, reported before any cluster is touched.
func TestNodeConfigRefusals(t *testing.T) {
	srv := kubeapitest.New(t)
	opts := srv.Connection(t, testHostEnv, testPortEnv)
	base := configFixture{
		APIHostEnv: testHostEnv, APIPortEnv: testPortEnv,
		TokenFile: opts.TokenFile, CAFile: opts.CAFile,
		RequestTimeout: "10s", SecretsDir: t.TempDir(), ListPageSize: 500,
		Exclude:    excludeFixture{Label: testExcludeLabel, Value: testExcludeValue},
		Undeclared: undeclaredVolume(),
		Rules:      []ruleFixture{simpleRule("databases", "pods", "backup.example/type=database", "Running")},
	}
	cases := []struct {
		name  string
		build func(configFixture) configFixture
		want  string
	}{
		{"node_scope without node_env",
			func(c configFixture) configFixture { c.NodeScope = true; c.WriterNodes = []string{"node-a"}; return c },
			"node_env is empty"},
		{"node_scope without writer_nodes",
			func(c configFixture) configFixture { c.NodeScope = true; c.NodeEnv = testNodeEnv; return c },
			"writer_nodes is empty"},
		{"writer_nodes without node_scope",
			func(c configFixture) configFixture { c.WriterNodes = []string{"node-a"}; return c },
			"node_scope is false"},
		{"an empty writer node",
			func(c configFixture) configFixture {
				c.NodeScope = true
				c.NodeEnv = testNodeEnv
				c.WriterNodes = []string{""}
				return c
			},
			"writer_nodes/0 is empty"},
		{"a repeated writer node",
			func(c configFixture) configFixture {
				c.NodeScope = true
				c.NodeEnv = testNodeEnv
				c.WriterNodes = []string{"node-a", "node-a"}
				return c
			},
			"repeats"},
		{"a negative rule rate limit",
			func(c configFixture) configFixture {
				c.Rules[0].RateLimit = -1
				return c
			},
			"rate_limit must not be negative"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw := jsontext.Value(mustJSON(tc.build(base)))
			if _, err := instances.Build(t.Context(), registry(t), discovery.Kind, "kubernetes_labels", raw,
				instances.Deps{Instance: "workloads"}); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Build = %v, want a config error containing %q", err, tc.want)
			}
		})
	}
}

// TestNodeScopeCarriesTheRuleRateLimit: a rule's rate_limit is part of the
// envelope every Found carries, beside the pipeline it never enters.
func TestNodeScopeCarriesTheRuleRateLimit(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddPod(scheduledPod("ns", "db-0", map[string]string{"backup.example/type": "database"}, "Running", ""))
	rule := simpleRule("databases", "pods", "backup.example/type=database", "Running")
	rule.RateLimit = 1 << 20
	d := newDiscoverer(t, srv, rule)

	found := discover(t, d)
	if len(found) != 1 || found[0].RateLimit != 1<<20 {
		t.Fatalf("found = %+v, want the rule's rate limit on the Found", found)
	}
}

// TestPinnedVolumeBelongsToItsOwnNode: a volume pinned to this writer's node is
// read at its path, and the node is proven once however many claims ask.
func TestPinnedVolumeBelongsToItsOwnNode(t *testing.T) {
	t.Setenv(testNodeEnv, "node-a")
	srv := kubeapitest.New(t)
	srv.AddNode(nodeJSON("node-a", hostnameLabels("node-a")))
	srv.AddNode(nodeJSON("node-b", hostnameLabels("node-b")))
	files := map[string]string{"backup.example/engine": "files"}
	for _, name := range []string{"media-a", "media-b"} {
		srv.AddPVC(boundPVC("ns", name, files, "Bound", "pv-"+name))
		srv.AddPV(pinnedPV("pv-"+name, "ns", name, "/var/lib/rancher/k3s/storage/"+name, "node-a"))
	}
	rule := simpleRule("volumes", "persistentvolumeclaims", "backup.example/engine=files", "Bound")
	rule.Source.Config = map[string]any{"root": "{{ volumePath }}", "ignore_vanished": true}
	d := buildNodeScoped(t, srv, []string{"node-a", "node-b"}, rule)

	found := discover(t, d)
	if len(found) != 2 {
		t.Fatalf("found %d claims, want 2: %+v", len(found), found)
	}
	for _, f := range found {
		if f.Problem != "" || !strings.Contains(string(f.Config), "/var/lib/rancher/k3s/storage/") {
			t.Fatalf("claim = %+v, want its own volume path", f)
		}
	}
	if got := srv.Calls("nodes/node-a"); got != 1 {
		t.Fatalf("node-a read %d times, want one proof per collect", got)
	}
}

// TestPinnedVolumeOnAPeerIsElsewhere: a volume whose required affinity admits
// only a declared peer is that peer's data, recorded as an elsewhere with the
// node — even though no pod rule covers the claim.
func TestPinnedVolumeOnAPeerIsElsewhere(t *testing.T) {
	t.Setenv(testNodeEnv, "node-a")
	srv := kubeapitest.New(t)
	srv.AddNode(nodeJSON("node-a", hostnameLabels("node-a")))
	srv.AddNode(nodeJSON("node-b", hostnameLabels("node-b")))
	files := map[string]string{"backup.example/engine": "files"}
	srv.AddPVC(boundPVC("ns", "media", files, "Bound", "pv-media"))
	srv.AddPV(pinnedPV("pv-media", "ns", "media", "/var/lib/rancher/k3s/storage/media", "node-b"))
	rule := simpleRule("volumes", "persistentvolumeclaims", "backup.example/engine=files", "Bound")
	rule.Source.Config = map[string]any{"root": "{{ volumePath }}", "ignore_vanished": true}
	d := buildNodeScoped(t, srv, []string{"node-a", "node-b"}, rule)

	found := discover(t, d)
	if len(found) != 1 || found[0].Excluded != model.ExclusionElsewhere || found[0].ExcludedNode != "node-b" {
		t.Fatalf("found = %+v, want one elsewhere exclusion naming node-b", found)
	}
	if found[0].Type != "" {
		t.Fatalf("a delegated claim built a source: %+v", found[0])
	}
}

// TestPinnedVolumeOutsideWriterNodesFails: a volume pinned to a node nobody
// declared would be backed up by no writer, so it is a failed artifact — never
// a green copy of somebody else's directory and never an elsewhere.
func TestPinnedVolumeOutsideWriterNodesFails(t *testing.T) {
	t.Setenv(testNodeEnv, "node-a")
	srv := kubeapitest.New(t)
	srv.AddNode(nodeJSON("node-a", hostnameLabels("node-a")))
	srv.AddNode(nodeJSON("node-b", hostnameLabels("node-b")))
	files := map[string]string{"backup.example/engine": "files"}
	srv.AddPVC(boundPVC("ns", "media", files, "Bound", "pv-media"))
	srv.AddPV(pinnedPV("pv-media", "ns", "media", "/var/lib/rancher/k3s/storage/media", "node-c"))
	rule := simpleRule("volumes", "persistentvolumeclaims", "backup.example/engine=files", "Bound")
	rule.Source.Config = map[string]any{"root": "{{ volumePath }}", "ignore_vanished": true}
	d := buildNodeScoped(t, srv, []string{"node-a", "node-b"}, rule)

	found := discover(t, d)
	if len(found) != 1 || found[0].Excluded != "" || !strings.Contains(found[0].Problem, "outside writer_nodes") {
		t.Fatalf("found = %+v, want the out-of-set failure", found)
	}
}

// TestNodeGetFailureNeverDisablesTheCheck: without node_scope a missing nodes
// get grant is a warning and the affected pinned volume fails before any source
// is built — never the old debug-only bypass that read the wrong directory.
func TestNodeGetFailureNeverDisablesTheCheck(t *testing.T) {
	t.Setenv(testNodeEnv, "node-a")
	srv := kubeapitest.New(t)
	srv.Deny("nodes")
	files := map[string]string{"backup.example/engine": "files"}
	for _, name := range []string{"media-a", "media-b"} {
		srv.AddPVC(boundPVC("ns", name, files, "Bound", "pv-"+name))
		srv.AddPV(pinnedPV("pv-"+name, "ns", name, "/var/lib/rancher/k3s/storage/"+name, "node-a"))
	}
	rule := simpleRule("volumes", "persistentvolumeclaims", "backup.example/engine=files", "Bound")
	rule.Source.Config = map[string]any{"root": "{{ volumePath }}", "ignore_vanished": true}

	opts := srv.Connection(t, testHostEnv, testPortEnv)
	var logs bytes.Buffer
	cfg := configFixture{
		APIHostEnv: testHostEnv, APIPortEnv: testPortEnv,
		TokenFile: opts.TokenFile, CAFile: opts.CAFile,
		RequestTimeout: "10s", SecretsDir: t.TempDir(), ListPageSize: 500,
		Exclude:    excludeFixture{Label: testExcludeLabel, Value: testExcludeValue},
		Undeclared: undeclaredVolume(),
		Rules:      []ruleFixture{rule},
		NodeEnv:    testNodeEnv,
	}
	d, err := instances.Build(t.Context(), registry(t), discovery.Kind, "kubernetes_labels",
		jsontext.Value(mustJSON(cfg)), instances.Deps{Instance: "workloads", Log: slog.New(human.NewHandler(&logs, slog.LevelDebug))})
	if err != nil {
		t.Fatal(err)
	}
	if err := check(t, d); err != nil {
		t.Fatalf("Check = %v, want only the artifacts to fail", err)
	}
	found := discover(t, d)
	if len(found) != 2 {
		t.Fatalf("found %d claims, want both affected: %+v", len(found), found)
	}
	for _, f := range found {
		if !strings.Contains(f.Problem, "reading node") || !strings.Contains(f.Problem, "grant get on nodes") {
			t.Fatalf("claim = %+v, want the unprovable node refused with its remedy", f)
		}
	}
	// The node is asked once per collect and its failure is one warning per
	// collect: two objects describing one missing grant must not read as two
	// incidents. Check and Discover are two collects, hence two lines.
	if got := strings.Count(logs.String(), "cannot prove this pod's node"); got != 2 {
		t.Fatalf("warning count = %d in %q, want one per collect", got, logs.String())
	}
}

// TestUnpinnedHostPathUsesTheMountingPodsNode: a hostPath volume says nothing
// about its node, so the Running pod that mounts the claim does — a declared
// peer's pod delegates the claim to that peer.
func TestUnpinnedHostPathUsesTheMountingPodsNode(t *testing.T) {
	t.Setenv(testNodeEnv, "node-a")
	srv := kubeapitest.New(t)
	srv.AddNode(nodeJSON("node-a", hostnameLabels("node-a")))
	srv.AddNode(nodeJSON("node-b", hostnameLabels("node-b")))
	files := map[string]string{"backup.example/engine": "files"}
	srv.AddPod(scheduledPod("ns", "app-0", nil, "Running", "node-b", "media"))
	srv.AddPVC(boundPVC("ns", "media", files, "Bound", "pv-media"))
	srv.AddPV(unpinnedHostPathPV("pv-media", "ns", "media", "/mnt/disks/vol1"))
	rule := simpleRule("volumes", "persistentvolumeclaims", "backup.example/engine=files", "Bound")
	rule.Source.Config = map[string]any{"root": "{{ volumePath }}", "ignore_vanished": true}
	d := buildNodeScoped(t, srv, []string{"node-a", "node-b"}, rule)

	found := discover(t, d)
	if len(found) != 1 || found[0].Excluded != model.ExclusionElsewhere || found[0].ExcludedNode != "node-b" {
		t.Fatalf("found = %+v, want the mounting pod's node to delegate the claim", found)
	}
}

// TestUnpinnedHostPathCases fails closed on every placement it cannot prove: a
// pod on an undeclared node, no Running pod at all, and two Running pods on
// different nodes.
func TestUnpinnedHostPathCases(t *testing.T) {
	files := map[string]string{"backup.example/engine": "files"}
	cases := []struct {
		name     string
		seed     func(*kubeapitest.Server)
		scope    bool
		mentions string
	}{
		{
			name: "a mounting pod on an undeclared node",
			seed: func(srv *kubeapitest.Server) {
				srv.AddPod(scheduledPod("ns", "app-0", nil, "Running", "node-c", "media"))
			},
			scope:    true,
			mentions: "not in writer_nodes",
		},
		{
			name:     "no Running pod mounts it",
			seed:     func(*kubeapitest.Server) {},
			scope:    true,
			mentions: "no Running pod mounts it",
		},
		{
			name: "two Running pods on different nodes",
			seed: func(srv *kubeapitest.Server) {
				srv.AddPod(scheduledPod("ns", "app-0", nil, "Running", "node-b", "media"))
				srv.AddPod(scheduledPod("ns", "app-1", nil, "Running", "node-c", "media"))
			},
			scope:    true,
			mentions: "cannot be proven",
		},
		{
			name: "a mounting pod on another node without node_scope",
			seed: func(srv *kubeapitest.Server) {
				srv.AddPod(scheduledPod("ns", "app-0", nil, "Running", "node-b", "media"))
			},
			scope:    false,
			mentions: "another node's directory",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(testNodeEnv, "node-a")
			srv := kubeapitest.New(t)
			srv.AddNode(nodeJSON("node-a", hostnameLabels("node-a")))
			srv.AddNode(nodeJSON("node-b", hostnameLabels("node-b")))
			tc.seed(srv)
			srv.AddPVC(boundPVC("ns", "media", files, "Bound", "pv-media"))
			srv.AddPV(unpinnedHostPathPV("pv-media", "ns", "media", "/mnt/disks/vol1"))
			rule := simpleRule("volumes", "persistentvolumeclaims", "backup.example/engine=files", "Bound")
			rule.Source.Config = map[string]any{"root": "{{ volumePath }}", "ignore_vanished": true}

			var d discovery.Discoverer
			if tc.scope {
				d = buildNodeScoped(t, srv, []string{"node-a", "node-b"}, rule)
			} else {
				opts := srv.Connection(t, testHostEnv, testPortEnv)
				cfg := configFixture{
					APIHostEnv: testHostEnv, APIPortEnv: testPortEnv,
					TokenFile: opts.TokenFile, CAFile: opts.CAFile,
					RequestTimeout: "10s", SecretsDir: t.TempDir(), ListPageSize: 500,
					Exclude:    excludeFixture{Label: testExcludeLabel, Value: testExcludeValue},
					Undeclared: undeclaredVolume(),
					Rules:      []ruleFixture{rule},
					NodeEnv:    testNodeEnv,
				}
				var err error
				d, err = instances.Build(t.Context(), registry(t), discovery.Kind, "kubernetes_labels",
					jsontext.Value(mustJSON(cfg)), instances.Deps{Instance: "workloads"})
				if err != nil {
					t.Fatal(err)
				}
			}
			found := discover(t, d)
			if len(found) != 1 || found[0].Excluded != "" || !strings.Contains(found[0].Problem, tc.mentions) {
				t.Fatalf("found = %+v, want an unproven placement failing with %q", found, tc.mentions)
			}
		})
	}
}

// TestPinnedVolumeConflictsWithItsMountingPod: a volume pinned to this node but
// mounted by a Running pod elsewhere is a contradiction no writer can resolve,
// and the artifact fails instead of choosing one fact over the other.
func TestPinnedVolumeConflictsWithItsMountingPod(t *testing.T) {
	t.Setenv(testNodeEnv, "node-a")
	srv := kubeapitest.New(t)
	srv.AddNode(nodeJSON("node-a", hostnameLabels("node-a")))
	srv.AddNode(nodeJSON("node-b", hostnameLabels("node-b")))
	files := map[string]string{"backup.example/engine": "files"}
	srv.AddPod(scheduledPod("ns", "app-0", nil, "Running", "node-b", "media"))
	srv.AddPVC(boundPVC("ns", "media", files, "Bound", "pv-media"))
	srv.AddPV(pinnedPV("pv-media", "ns", "media", "/var/lib/rancher/k3s/storage/media", "node-a"))
	rule := simpleRule("volumes", "persistentvolumeclaims", "backup.example/engine=files", "Bound")
	rule.Source.Config = map[string]any{"root": "{{ volumePath }}", "ignore_vanished": true}
	d := buildNodeScoped(t, srv, []string{"node-a", "node-b"}, rule)

	found := discover(t, d)
	if len(found) != 1 || !strings.Contains(found[0].Problem, "placement cannot be proven") {
		t.Fatalf("found = %+v, want the conflicting placement refused", found)
	}
}

// TestNodeChecksOffPreservesTheOldRead: an empty node_env with node_scope false
// is the explicit opt-out — no node read at all, and the pinned volume is read
// exactly as it was before node checks existed.
func TestNodeChecksOffPreservesTheOldRead(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.Deny("nodes")
	files := map[string]string{"backup.example/engine": "files"}
	srv.AddPVC(boundPVC("ns", "media", files, "Bound", "pv-media"))
	srv.AddPV(pinnedPV("pv-media", "ns", "media", "/var/lib/rancher/k3s/storage/media", "node-b"))
	rule := simpleRule("volumes", "persistentvolumeclaims", "backup.example/engine=files", "Bound")
	rule.Source.Config = map[string]any{"root": "{{ volumePath }}", "ignore_vanished": true}
	d := newDiscoverer(t, srv, rule)

	found := discover(t, d)
	if len(found) != 1 || found[0].Problem != "" || !strings.Contains(string(found[0].Config), "/var/lib/rancher/k3s/storage/media") {
		t.Fatalf("found = %+v, want the unchecked path of the opt-out", found)
	}
	if srv.Calls("nodes/node-b") != 0 {
		t.Fatal("the opt-out read a node")
	}
}

// TestPinnedVolumeOnAnotherNodeFailsWithoutNodeScope: the wrong-node bugfix.
// A volume pinned to node-b read by the writer on node-a is another node's
// directory, and without node_scope there is no declared peer to delegate it
// to: the artifact fails instead of copying the wrong data.
func TestPinnedVolumeOnAnotherNodeFailsWithoutNodeScope(t *testing.T) {
	t.Setenv(testNodeEnv, "node-a")
	srv := kubeapitest.New(t)
	srv.AddNode(nodeJSON("node-a", hostnameLabels("node-a")))
	srv.AddNode(nodeJSON("node-b", hostnameLabels("node-b")))
	files := map[string]string{"backup.example/engine": "files"}
	srv.AddPVC(boundPVC("ns", "media", files, "Bound", "pv-media"))
	srv.AddPV(pinnedPV("pv-media", "ns", "media", "/var/lib/rancher/k3s/storage/media", "node-b"))
	rule := simpleRule("volumes", "persistentvolumeclaims", "backup.example/engine=files", "Bound")
	rule.Source.Config = map[string]any{"root": "{{ volumePath }}", "ignore_vanished": true}

	opts := srv.Connection(t, testHostEnv, testPortEnv)
	cfg := configFixture{
		APIHostEnv: testHostEnv, APIPortEnv: testPortEnv,
		TokenFile: opts.TokenFile, CAFile: opts.CAFile,
		RequestTimeout: "10s", SecretsDir: t.TempDir(), ListPageSize: 500,
		Exclude:    excludeFixture{Label: testExcludeLabel, Value: testExcludeValue},
		Undeclared: undeclaredVolume(),
		Rules:      []ruleFixture{rule},
		NodeEnv:    testNodeEnv,
	}
	d, err := instances.Build(t.Context(), registry(t), discovery.Kind, "kubernetes_labels",
		jsontext.Value(mustJSON(cfg)), instances.Deps{Instance: "workloads"})
	if err != nil {
		t.Fatal(err)
	}
	found := discover(t, d)
	if len(found) != 1 || found[0].Excluded != "" || !strings.Contains(found[0].Problem, "another node's directory") {
		t.Fatalf("found = %+v, want the pinned-away volume refused", found)
	}
}

// TestLocalVolumeWithoutAffinityIsUnprovable: a local volume with no required
// node affinity cannot say which node holds it, and reading its path anyway is
// the guess this scope exists to refuse.
func TestLocalVolumeWithoutAffinityIsUnprovable(t *testing.T) {
	t.Setenv(testNodeEnv, "node-a")
	srv := kubeapitest.New(t)
	srv.AddNode(nodeJSON("node-a", hostnameLabels("node-a")))
	files := map[string]string{"backup.example/engine": "files"}
	srv.AddPVC(boundPVC("ns", "media", files, "Bound", "pv-media"))
	srv.AddPV(fmt.Sprintf(`{"metadata":{"name":"pv-media"},"spec":{"local":{"path":"/var/lib/rancher/k3s/storage/media"},`+
		`"claimRef":{"namespace":"ns","name":"media","uid":%q}}}`, claimUID("ns", "media")))
	rule := simpleRule("volumes", "persistentvolumeclaims", "backup.example/engine=files", "Bound")
	rule.Source.Config = map[string]any{"root": "{{ volumePath }}", "ignore_vanished": true}

	opts := srv.Connection(t, testHostEnv, testPortEnv)
	cfg := configFixture{
		APIHostEnv: testHostEnv, APIPortEnv: testPortEnv,
		TokenFile: opts.TokenFile, CAFile: opts.CAFile,
		RequestTimeout: "10s", SecretsDir: t.TempDir(), ListPageSize: 500,
		Exclude:    excludeFixture{Label: testExcludeLabel, Value: testExcludeValue},
		Undeclared: undeclaredVolume(),
		Rules:      []ruleFixture{rule},
		NodeEnv:    testNodeEnv,
	}
	d, err := instances.Build(t.Context(), registry(t), discovery.Kind, "kubernetes_labels",
		jsontext.Value(mustJSON(cfg)), instances.Deps{Instance: "workloads"})
	if err != nil {
		t.Fatal(err)
	}
	found := discover(t, d)
	if len(found) != 1 || !strings.Contains(found[0].Problem, "no required node affinity") {
		t.Fatalf("found = %+v, want the unprovable local volume refused", found)
	}
}

// TestUnpinnedHostPathOnThisNode: the mounting Running pod names this writer's
// own node, so the directory is read.
func TestUnpinnedHostPathOnThisNode(t *testing.T) {
	t.Setenv(testNodeEnv, "node-a")
	srv := kubeapitest.New(t)
	srv.AddNode(nodeJSON("node-a", hostnameLabels("node-a")))
	files := map[string]string{"backup.example/engine": "files"}
	srv.AddPod(scheduledPod("ns", "app-0", nil, "Running", "node-a", "media"))
	srv.AddPVC(boundPVC("ns", "media", files, "Bound", "pv-media"))
	srv.AddPV(unpinnedHostPathPV("pv-media", "ns", "media", "/mnt/disks/vol1"))
	rule := simpleRule("volumes", "persistentvolumeclaims", "backup.example/engine=files", "Bound")
	rule.Source.Config = map[string]any{"root": "{{ volumePath }}", "ignore_vanished": true}
	d := buildNodeScoped(t, srv, []string{"node-a", "node-b"}, rule)

	found := discover(t, d)
	if len(found) != 1 || found[0].Problem != "" || !strings.Contains(string(found[0].Config), "/mnt/disks/vol1") {
		t.Fatalf("found = %+v, want the mounting pod's own node read", found)
	}
}

// TestPeerNodeUnreadableFailsTheDelegation: naming a peer is not believing in
// it — the peer's node is read to prove the volume actually belongs there, and
// a node nobody can read proves nothing.
func TestPeerNodeUnreadableFailsTheDelegation(t *testing.T) {
	t.Setenv(testNodeEnv, "node-a")
	srv := kubeapitest.New(t)
	srv.AddNode(nodeJSON("node-a", hostnameLabels("node-a")))
	files := map[string]string{"backup.example/engine": "files"}
	srv.AddPVC(boundPVC("ns", "media", files, "Bound", "pv-media"))
	srv.AddPV(pinnedPV("pv-media", "ns", "media", "/var/lib/rancher/k3s/storage/media", "node-b"))
	rule := simpleRule("volumes", "persistentvolumeclaims", "backup.example/engine=files", "Bound")
	rule.Source.Config = map[string]any{"root": "{{ volumePath }}", "ignore_vanished": true}
	d := buildNodeScoped(t, srv, []string{"node-a", "node-b"}, rule)

	found := discover(t, d)
	if len(found) != 1 || !strings.Contains(found[0].Problem, "cannot be read to prove it is a declared peer") {
		t.Fatalf("found = %+v, want the unreadable peer refused", found)
	}
}

// TestPinnedVolumeMatchingSeveralPeersFails: a volume whose affinity admits two
// declared writers cannot be attributed to one of them, and guessing would
// produce either two copies or none.
func TestPinnedVolumeMatchingSeveralPeersFails(t *testing.T) {
	t.Setenv(testNodeEnv, "node-a")
	srv := kubeapitest.New(t)
	for _, node := range []string{"node-a", "node-b", "node-c"} {
		srv.AddNode(nodeJSON(node, hostnameLabels(node)))
	}
	files := map[string]string{"backup.example/engine": "files"}
	srv.AddPVC(boundPVC("ns", "media", files, "Bound", "pv-media"))
	srv.AddPV(fmt.Sprintf(`{"metadata":{"name":"pv-media"},"spec":{"local":{"path":"/var/lib/rancher/k3s/storage/media"},`+
		`"nodeAffinity":{"required":{"nodeSelectorTerms":[`+
		`{"matchExpressions":[{"key":"kubernetes.io/hostname","operator":"In","values":["node-b"]}]},`+
		`{"matchExpressions":[{"key":"kubernetes.io/hostname","operator":"In","values":["node-c"]}]}]}},`+
		`"claimRef":{"namespace":"ns","name":"media","uid":%q}}}`, claimUID("ns", "media")))
	rule := simpleRule("volumes", "persistentvolumeclaims", "backup.example/engine=files", "Bound")
	rule.Source.Config = map[string]any{"root": "{{ volumePath }}", "ignore_vanished": true}
	d := buildNodeScoped(t, srv, []string{"node-a", "node-b", "node-c"}, rule)

	found := discover(t, d)
	if len(found) != 1 || !strings.Contains(found[0].Problem, "admits nodes") {
		t.Fatalf("found = %+v, want the ambiguous placement refused", found)
	}
}

// TestPinnedVolumeOnAPeerIsOneNodeRead: the peer node is proven once per
// collect, however many of its volumes are delegated.
func TestPinnedVolumeOnAPeerIsOneNodeRead(t *testing.T) {
	t.Setenv(testNodeEnv, "node-a")
	srv := kubeapitest.New(t)
	srv.AddNode(nodeJSON("node-a", hostnameLabels("node-a")))
	srv.AddNode(nodeJSON("node-b", hostnameLabels("node-b")))
	files := map[string]string{"backup.example/engine": "files"}
	for _, name := range []string{"media-a", "media-b"} {
		srv.AddPVC(boundPVC("ns", name, files, "Bound", "pv-"+name))
		srv.AddPV(pinnedPV("pv-"+name, "ns", name, "/var/lib/rancher/k3s/storage/"+name, "node-b"))
	}
	rule := simpleRule("volumes", "persistentvolumeclaims", "backup.example/engine=files", "Bound")
	rule.Source.Config = map[string]any{"root": "{{ volumePath }}", "ignore_vanished": true}
	d := buildNodeScoped(t, srv, []string{"node-a", "node-b"}, rule)

	found := discover(t, d)
	if len(found) != 2 {
		t.Fatalf("found %d claims, want both delegated: %+v", len(found), found)
	}
	for _, f := range found {
		if f.Excluded != model.ExclusionElsewhere || f.ExcludedNode != "node-b" {
			t.Fatalf("claim = %+v, want the peer's delegation", f)
		}
	}
	if got := srv.Calls("nodes/node-b"); got != 1 {
		t.Fatalf("node-b read %d times, want one proof per collect", got)
	}
}

// TestClaimPlacementFailsAnUnboundClaim: under node checks a claim that names
// no volume has no placement to prove, and the artifact fails instead of
// rendering a source for an unknown directory.
func TestClaimPlacementFailsAnUnboundClaim(t *testing.T) {
	t.Setenv(testNodeEnv, "node-a")
	srv := kubeapitest.New(t)
	srv.AddNode(nodeJSON("node-a", hostnameLabels("node-a")))
	files := map[string]string{"backup.example/engine": "files"}
	srv.AddPVC(pvc("ns", "media", files, "Bound"))
	rule := simpleRule("volumes", "persistentvolumeclaims", "backup.example/engine=files", "Bound")
	d := buildNodeScoped(t, srv, []string{"node-a", "node-b"}, rule)

	found := discover(t, d)
	if len(found) != 1 || !strings.Contains(found[0].Problem, "bound to no volume") {
		t.Fatalf("found = %+v, want the unbound claim refused", found)
	}
}

// TestVolumePathRefusesANonNodeVolumeUnderNodeChecks: a volume that is not on a
// node has no placement to prove, and the template that asks for a node path
// still refuses it by name.
func TestVolumePathRefusesANonNodeVolumeUnderNodeChecks(t *testing.T) {
	t.Setenv(testNodeEnv, "node-a")
	srv := kubeapitest.New(t)
	srv.AddNode(nodeJSON("node-a", hostnameLabels("node-a")))
	files := map[string]string{"backup.example/engine": "files"}
	srv.AddPVC(boundPVC("ns", "media", files, "Bound", "pv-media"))
	srv.AddPV(fmt.Sprintf(`{"metadata":{"name":"pv-media"},"spec":{"nfs":{"server":"nfs.example","path":"/export"},`+
		`"claimRef":{"namespace":"ns","name":"media","uid":%q}}}`, claimUID("ns", "media")))
	rule := simpleRule("volumes", "persistentvolumeclaims", "backup.example/engine=files", "Bound")
	rule.Source.Config = map[string]any{"root": "{{ volumePath }}", "ignore_vanished": true}
	d := buildNodeScoped(t, srv, []string{"node-a", "node-b"}, rule)

	found := discover(t, d)
	if len(found) != 1 || !strings.Contains(found[0].Problem, "holds no spec.local.path") {
		t.Fatalf("found = %+v, want the non-node volume refused by the template", found)
	}
}

// TestPeerPodCoversItsClaims: a covering pod on a peer's node delegates the
// claims it mounts, and the claim's own volume can contradict the delegation.
func TestPeerPodCoversItsClaims(t *testing.T) {
	files := map[string]string{"backup.example/engine": "postgres"}
	cases := []struct {
		name     string
		writer   []string
		pv       func() string
		mentions string
	}{
		{
			// The normal delegation: the hostPath volume has no affinity, and
			// the pod mounting it from node-b proves the claim lives there.
			name:   "a declared peer delegates the claim",
			writer: []string{"node-a", "node-b"},
			pv: func() string {
				return unpinnedHostPathPV("pv-media", "ns", "media", "/mnt/disks/vol1")
			},
		},
		{
			name:     "a broken binding fails the claim",
			writer:   []string{"node-a", "node-b"},
			pv:       func() string { return "" },
			mentions: "no persistentvolume named",
		},
		{
			name:   "a volume pinned to two different peers conflicts",
			writer: []string{"node-a", "node-b", "node-c"},
			pv: func() string {
				return pinnedPV("pv-media", "ns", "media", "/var/lib/rancher/k3s/storage/media", "node-c")
			},
			mentions: "its placement cannot be proven",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(testNodeEnv, "node-a")
			srv := kubeapitest.New(t)
			for _, node := range tc.writer {
				srv.AddNode(nodeJSON(node, hostnameLabels(node)))
			}
			srv.AddPod(scheduledPod("ns", "db-0", files, "Running", "node-b", "media"))
			srv.AddPVC(boundPVC("ns", "media", nil, "Bound", "pv-media"))
			if pv := tc.pv(); pv != "" {
				srv.AddPV(pv)
			}
			rule := simpleRule("databases", "pods", "backup.example/engine=postgres", "Running")
			rule.CoversVolumes = true
			d := buildNodeScoped(t, srv, tc.writer, rule)

			found := discover(t, d)
			var claim *discovery.Found
			for i := range found {
				if found[i].Name == "undeclared/ns/media" {
					claim = &found[i]
				}
			}
			if claim == nil {
				t.Fatalf("the claim produced no verdict: %+v", found)
			}
			switch {
			case tc.mentions != "":
				if claim.Problem == "" || !strings.Contains(claim.Problem, tc.mentions) {
					t.Fatalf("claim = %+v, want a failure containing %q", claim, tc.mentions)
				}
			default:
				if claim.Excluded != model.ExclusionElsewhere || claim.ExcludedNode != "node-b" {
					t.Fatalf("claim = %+v, want the pod's peer delegation", claim)
				}
			}
		})
	}
}

// TestReviewMetadataNameLabelIsNotIdentity is the reviewer's counterexample:
// metadata.name as a matchExpression is an ordinary label an undeclared node
// may share, so it cannot be an identity pin — in either scope, in either
// phase, the path is not read without mounting proof.
func TestReviewMetadataNameLabelIsNotIdentity(t *testing.T) {
	for _, scope := range []bool{false, true} {
		for _, phase := range []string{"Bound", "Pending"} {
			t.Run(fmt.Sprintf("scope=%t/%s", scope, phase), func(t *testing.T) {
				t.Setenv(testNodeEnv, "node-a")
				srv := kubeapitest.New(t)
				srv.AddNode(nodeJSON("node-a", map[string]string{"metadata.name": "node-a"}))
				srv.AddNode(nodeJSON("node-b", hostnameLabels("node-b")))
				srv.AddNode(nodeJSON("node-c", map[string]string{"metadata.name": "node-a"}))
				srv.AddPVC(boundPVC("ns", "media", map[string]string{"backup.example/engine": "files"}, phase, "pv-media"))
				srv.AddPV(fmt.Sprintf(`{"metadata":{"name":"pv-media"},"spec":{"local":{"path":"/mnt/local-data"},`+
					`"nodeAffinity":{"required":{"nodeSelectorTerms":[{"matchExpressions":[{"key":"metadata.name","operator":"In","values":["node-a"]}]}]}},`+
					`"claimRef":{"namespace":"ns","name":"media","uid":%q}}}`, claimUID("ns", "media")))
				rule := simpleRule("volumes", "persistentvolumeclaims", "backup.example/engine=files", phase)
				rule.Source.Config = map[string]any{"root": "{{ volumePath }}", "ignore_vanished": true}
				var d discovery.Discoverer
				if scope {
					d = buildNodeScoped(t, srv, []string{"node-a", "node-b"}, rule)
				} else {
					d = buildNodeAware(t, srv, rule)
				}
				found := discover(t, d)
				if len(found) != 1 || (found[0].Problem == "" && found[0].Excluded == "") {
					t.Fatalf("label shared with undeclared node authorized local read: %+v", found)
				}
			})
		}
	}
}

// TestReviewSelectorMatrixNeverReadsUnprovenPlacement is the full matrix the
// finding names: MatchExpressions and MatchFields, identity pins and custom
// labels, targets on this node, a declared peer and an undeclared node, scoped
// and unscoped, Bound and Pending, with and without mounting evidence. A local
// path is read only where the selector or the mounting pod proves this node's
// placement; everything else fails or delegates.
func TestReviewSelectorMatrixNeverReadsUnprovenPlacement(t *testing.T) {
	fieldTerm := func(name string) string {
		return fmt.Sprintf(`{"matchFields":[{"key":"metadata.name","operator":"In","values":[%q]}]}`, name)
	}
	hostnameTerm := func(name string) string {
		return fmt.Sprintf(`{"matchExpressions":[{"key":"kubernetes.io/hostname","operator":"In","values":[%q]}]}`, name)
	}
	exprTerm := func(key string, values ...string) string {
		quoted := make([]string, len(values))
		for i, value := range values {
			quoted[i] = strconv.Quote(value)
		}
		return fmt.Sprintf(`{"matchExpressions":[{"key":%q,"operator":"In","values":[%s]}]}`, key, strings.Join(quoted, ","))
	}
	type selector struct {
		name  string
		kind  string // identity-own, identity-peer, identity-undeclared, label-own, label-peer, label-undeclared
		terms string
	}
	selectors := []selector{
		{"field metadata.name of this node", "identity-own", fieldTerm("node-a")},
		{"field metadata.name of a declared peer", "identity-peer", fieldTerm("node-b")},
		{"field metadata.name of an undeclared node", "identity-undeclared", fieldTerm("node-c")},
		{"hostname label of this node", "identity-own", hostnameTerm("node-a")},
		{"hostname label of a declared peer", "identity-peer", hostnameTerm("node-b")},
		{"hostname label of an undeclared node", "identity-undeclared", hostnameTerm("node-c")},
		{"metadata.name label shared with an undeclared node", "label-own", exprTerm("metadata.name", "node-a")},
		{"custom label shared with an undeclared node", "label-own", exprTerm("disk", "ssd")},
		{"custom label of a declared peer", "label-peer", exprTerm("rack", "r2")},
		{"custom label of an undeclared node", "label-undeclared", exprTerm("zone", "z3")},
	}
	mounts := map[string]string{
		"no mounting pod":     "",
		"this node's pod":     "node-a",
		"a declared peer pod": "node-b",
		"an undeclared pod":   "node-c",
	}
	wantVerdict := func(sel selector, mountOn string, scope bool, phase string) string {
		bound := phase == "Bound"
		switch sel.kind {
		case "identity-own":
			if mountOn == "" || mountOn == "node-a" {
				return "ok"
			}
			return "failed" // a pod on another node contradicts the identity pin
		case "identity-peer":
			if scope && bound && (mountOn == "" || mountOn == "node-b") {
				return "elsewhere"
			}
			return "failed"
		case "identity-undeclared":
			return "failed"
		case "label-own":
			// The selector matches this node, but a shared label may also admit
			// an undeclared node: only this node's own mounting pod proves it.
			if mountOn == "node-a" {
				return "ok"
			}
			return "failed"
		case "label-peer":
			if scope && bound && mountOn == "node-b" {
				return "elsewhere"
			}
			return "failed"
		default:
			return "failed"
		}
	}
	for _, sel := range selectors {
		for mountName, mountOn := range mounts {
			for _, scope := range []bool{false, true} {
				for _, phase := range []string{"Bound", "Pending"} {
					t.Run(fmt.Sprintf("%s/%s/scope=%t/%s", sel.name, mountName, scope, phase), func(t *testing.T) {
						t.Setenv(testNodeEnv, "node-a")
						srv := kubeapitest.New(t)
						srv.AddNode(nodeJSON("node-a", map[string]string{
							"kubernetes.io/hostname": "node-a", "metadata.name": "node-a", "disk": "ssd",
						}))
						srv.AddNode(nodeJSON("node-b", map[string]string{
							"kubernetes.io/hostname": "node-b", "rack": "r2",
						}))
						srv.AddNode(nodeJSON("node-c", map[string]string{
							"metadata.name": "node-a", "disk": "ssd", "zone": "z3",
						}))
						if mountOn != "" {
							srv.AddPod(scheduledPod("ns", "app-0", nil, "Running", mountOn, "media"))
						}
						srv.AddPVC(boundPVC("ns", "media", map[string]string{"backup.example/engine": "files"}, phase, "pv-media"))
						srv.AddPV(fmt.Sprintf(`{"metadata":{"name":"pv-media"},"spec":{"local":{"path":"/mnt/local-data"},`+
							`"nodeAffinity":{"required":{"nodeSelectorTerms":[%s]}},`+
							`"claimRef":{"namespace":"ns","name":"media","uid":%q}}}`, sel.terms, claimUID("ns", "media")))
						rule := simpleRule("volumes", "persistentvolumeclaims", "backup.example/engine=files", phase)
						rule.Source.Config = map[string]any{"root": "{{ volumePath }}", "ignore_vanished": true}
						var d discovery.Discoverer
						if scope {
							d = buildNodeScoped(t, srv, []string{"node-a", "node-b"}, rule)
						} else {
							d = buildNodeAware(t, srv, rule)
						}
						found := discover(t, d)
						if len(found) != 1 {
							t.Fatalf("found %d claims, want 1: %+v", len(found), found)
						}
						f := found[0]
						switch want := wantVerdict(sel, mountOn, scope, phase); want {
						case "ok":
							if f.Problem != "" || f.Excluded != "" || !strings.Contains(string(f.Config), "/mnt/local-data") {
								t.Fatalf("claim = %+v, want the placement proven and the path read", f)
							}
						case "elsewhere":
							if f.Excluded != model.ExclusionElsewhere || f.ExcludedNode != "node-b" || f.Problem != "" {
								t.Fatalf("claim = %+v, want the peer's delegation", f)
							}
						default:
							if f.Problem == "" {
								t.Fatalf("claim = %+v, want an unproven placement refused", f)
							}
						}
					})
				}
			}
		}
	}
}
