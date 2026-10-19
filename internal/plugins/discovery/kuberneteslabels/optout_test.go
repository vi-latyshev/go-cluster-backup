package kuberneteslabels_test

import (
	"strings"
	"testing"

	"github.com/vi-latyshev/go-cluster-backup/internal/discovery"
	"github.com/vi-latyshev/go-cluster-backup/internal/kubeapi/kubeapitest"
	"github.com/vi-latyshev/go-cluster-backup/internal/model"
)

// foundWant is one line of the decision table: the source instance a rule
// produced, the artifact it named, and the verdict.
type foundWant struct {
	instance string
	artifact string
	verdict  string // ok | ignored | covered | failed
	mentions string // a substring the Problem or the exclusion reason must carry
	// importance is asserted when set: the envelope of the rule decides what a
	// failed artifact costs the run.
	importance model.Importance
}

// describeFound is one Found as the table reads it.
func describeFound(f discovery.Found) foundWant {
	w := foundWant{instance: f.Name, artifact: f.Artifact.Name, importance: f.Artifact.Importance}
	switch {
	case f.Excluded != "":
		w.verdict, w.mentions = string(f.Excluded), f.ExcludedReason
	case f.Problem != "":
		w.verdict, w.mentions = "failed", f.Problem
	default:
		w.verdict = "ok"
	}
	return w
}

// TestOptOutCoverageTable is the decision table of the opt-out coverage. Every
// claim the cluster holds is exactly one of: covered, because a pod rule that
// reads volumes discovered the pod mounting it; declared, because a rule's
// selector matches it and renders a source; ignored, because its own labels (or
// the pod's) carry the marker; or reported — one failed artifact naming it.
// A claim in another phase is nothing while it is Pending (WaitForFirstConsumer
// has not provisioned it) and a report of its own once it is Lost.
func TestOptOutCoverageTable(t *testing.T) {
	postgres := func() ruleFixture {
		r := simpleRule("databases", "pods", "backup.example/engine=postgres", "Running")
		r.CoversVolumes = true
		return r
	}
	volumes := func(selector string) ruleFixture {
		return simpleRule("volumes", "persistentvolumeclaims", selector, "Bound")
	}
	// declared reads the claim's own volume instead of a mount made for this
	// pod, which is what a file-tree rule does in a deployment.
	declared := func(selector string) ruleFixture {
		r := volumes(selector)
		r.Source.Config = map[string]any{"root": "{{ volumePath }}", "ignore_vanished": true}
		return r
	}
	databases := map[string]string{"backup.example/engine": "postgres"}
	media := map[string]string{"backup.example/type": "pvc"}
	files := map[string]string{"backup.example/engine": "files"}

	cases := []struct {
		name  string
		seed  func(*kubeapitest.Server)
		rules []ruleFixture
		want  []foundWant
		check func(t *testing.T, found []discovery.Found)
	}{
		{
			name: "an undeclared claim is one failed required artifact",
			seed: func(srv *kubeapitest.Server) {
				srv.AddPVC(pvc("ns", "orphan", media, "Bound"))
			},
			rules: []ruleFixture{volumes("backup.example/engine=files")},
			want: []foundWant{{
				instance: "undeclared/ns/orphan", artifact: "ns/orphan", verdict: "failed",
				mentions: "is backed up by no rule", importance: model.ImportanceRequired,
			}},
			check: func(t *testing.T, found []discovery.Found) {
				t.Helper()
				// The reason must say WHAT TO DO: the selector of the rule that
				// would declare the claim, from the config, and the marker that
				// would exclude it. "A rule has to match" is not an action.
				for _, want := range []string{
					`add backup.example/engine=files (rule "volumes")`,
					"or add backup.example/ignore=true to leave it out on purpose",
				} {
					if !strings.Contains(found[0].Problem, want) {
						t.Errorf("Problem %q does not carry %q", found[0].Problem, want)
					}
				}
				// The short form a notification shows names the labels only.
				if want := "no backup rule: add backup.example/engine=files or backup.example/ignore=true"; found[0].Hint != want {
					t.Errorf("Hint = %q, want %q", found[0].Hint, want)
				}
			},
		},
		{
			// A rule that matches the claim but never renders it — a phase that
			// never meets it — must not make the claim vanish: the accounting
			// reports what no rule would have taken.
			name: "a claim a rule matches but never renders is still reported",
			seed: func(srv *kubeapitest.Server) {
				srv.AddPVC(pvc("ns", "media", media, "Bound"))
			},
			rules: []ruleFixture{volumes("backup.example/type=pvc").withPhase("Pending")},
			want: []foundWant{{
				instance: "undeclared/ns/media", artifact: "ns/media", verdict: "failed",
				mentions: "is backed up by no rule",
			}},
		},
		{
			name: "a declared claim is read at the path of its volume",
			seed: func(srv *kubeapitest.Server) {
				srv.AddPVC(boundPVC("ns", "media", files, "Bound", "pv-media"))
				srv.AddPV(`{"metadata":{"name":"pv-media"},"spec":{"local":{"path":"/var/lib/rancher/k3s/storage/pvc-media"}}}`)
			},
			rules: []ruleFixture{declared("backup.example/engine=files")},
			want:  []foundWant{{instance: "volumes/ns/media", artifact: "ns/media", verdict: "ok"}},
			check: func(t *testing.T, found []discovery.Found) {
				t.Helper()
				if found[0].Type != "files" || !strings.Contains(string(found[0].Config), "/var/lib/rancher/k3s/storage/pvc-media") {
					t.Fatalf("the claim was not declared as a file tree at its volume: %+v", found[0])
				}
			},
		},
		{
			name: "a claim the pod mounts is covered by its dump",
			seed: func(srv *kubeapitest.Server) {
				srv.AddPod(mountingPod("ns", "db-0", databases, "Running", "data-db-0"))
				srv.AddPVC(pvc("ns", "data-db-0", media, "Bound"))
			},
			rules: []ruleFixture{postgres(), volumes("backup.example/type=pvc")},
			want: []foundWant{
				{instance: "databases/ns/db-0", artifact: "ns/db-0", verdict: "ok"},
				{instance: "volumes/ns/data-db-0", artifact: "ns/data-db-0", verdict: "covered",
					mentions: `covered by the dump of pod ns/db-0 (rule "databases")`},
			},
		},
		{
			name: "a claim whose labels carry the marker is ignored",
			seed: func(srv *kubeapitest.Server) {
				srv.AddPVC(pvc("ns", "media", map[string]string{"backup.example/type": "pvc", testExcludeLabel: testExcludeValue}, "Bound"))
			},
			rules: []ruleFixture{volumes("backup.example/type=pvc")},
			want: []foundWant{{
				instance: "volumes/ns/media", artifact: "ns/media", verdict: "ignored",
				mentions: "carries the exclusion marker " + testExcludeLabel + "=" + testExcludeValue,
			}},
		},
		{
			name: "the marker on the pod excludes its dump and the volumes it mounts",
			seed: func(srv *kubeapitest.Server) {
				srv.AddPod(mountingPod("ns", "db-0", map[string]string{
					"backup.example/engine": "postgres", testExcludeLabel: testExcludeValue,
				}, "Running", "data-db-0"))
				srv.AddPVC(pvc("ns", "data-db-0", media, "Bound"))
			},
			rules: []ruleFixture{postgres(), volumes("backup.example/type=pvc")},
			want: []foundWant{
				{instance: "databases/ns/db-0", artifact: "ns/db-0", verdict: "ignored",
					mentions: "carries the exclusion marker " + testExcludeLabel + "=" + testExcludeValue},
				{instance: "volumes/ns/data-db-0", artifact: "ns/data-db-0", verdict: "ignored",
					mentions: "the pod that mounts it, ns/db-0"},
			},
		},
		{
			name: "a declared claim the pod also mounts is still covered once",
			seed: func(srv *kubeapitest.Server) {
				srv.AddPod(mountingPod("ns", "db-0", databases, "Running", "data-db-0"))
				srv.AddPVC(pvc("ns", "data-db-0", files, "Bound"))
			},
			rules: []ruleFixture{postgres(), volumes("backup.example/engine=files")},
			want: []foundWant{
				{instance: "databases/ns/db-0", artifact: "ns/db-0", verdict: "ok"},
				{instance: "volumes/ns/data-db-0", artifact: "ns/data-db-0", verdict: "covered",
					mentions: "covered by the dump of pod ns/db-0"},
			},
		},
		{
			name: "a claim two rules match is backed up by the first and reported by the second",
			seed: func(srv *kubeapitest.Server) {
				srv.AddPVC(pvc("ns", "media", media, "Bound"))
			},
			rules: func() []ruleFixture {
				second := simpleRule("volumes_again", "persistentvolumeclaims", "backup.example/type=pvc", "Bound")
				return []ruleFixture{volumes("backup.example/type=pvc"), second}
			}(),
			want: []foundWant{
				{instance: "volumes/ns/media", artifact: "ns/media", verdict: "ok"},
				{instance: "volumes_again/ns/media", artifact: "ns/media", verdict: "failed",
					mentions: `already backed up by rule "volumes"`},
			},
		},
		{
			name: "a lost claim is reported",
			seed: func(srv *kubeapitest.Server) {
				srv.AddPVC(pvc("ns", "media", media, "Lost"))
			},
			rules: []ruleFixture{volumes("backup.example/type=pvc")},
			want: []foundWant{{
				instance: "volumes/ns/media", artifact: "ns/media", verdict: "failed",
				mentions: "is in phase Lost",
			}},
		},
		{
			name: "a pending claim is nothing yet",
			seed: func(srv *kubeapitest.Server) {
				srv.AddPVC(pvc("ns", "media", media, "Pending"))
			},
			rules: []ruleFixture{volumes("backup.example/type=pvc")},
			want:  nil,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := kubeapitest.New(t)
			tc.seed(srv)
			found := discover(t, newDiscoverer(t, srv, tc.rules...))
			if len(found) != len(tc.want) {
				t.Fatalf("found %d objects, want %d: %+v", len(found), len(tc.want), found)
			}
			for i, want := range tc.want {
				got := describeFound(found[i])
				if got.instance != want.instance || got.artifact != want.artifact || got.verdict != want.verdict {
					t.Fatalf("found[%d] = %+v, want %+v", i, got, want)
				}
				if want.mentions != "" && !strings.Contains(got.mentions, want.mentions) {
					t.Fatalf("found[%d] %s text = %q, want it to mention %q", i, want.verdict, got.mentions, want.mentions)
				}
				if want.importance != "" && got.importance != want.importance {
					t.Fatalf("found[%d] importance = %q, want %q", i, got.importance, want.importance)
				}
			}
			if tc.check != nil {
				tc.check(t, found)
			}
		})
	}
}

// TestVolumePathReadsTheVolumeListingOnce: the path of a claim comes from its
// PersistentVolume — spec.local.path of a node-local volume, spec.hostPath.path
// otherwise — and one collect lists the volumes once however many claims ask.
func TestVolumePathReadsTheVolumeListingOnce(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddPVC(boundPVC("ns", "media", map[string]string{"backup.example/engine": "files"}, "Bound", "pv-media"))
	srv.AddPVC(boundPVC("ns", "other", map[string]string{"backup.example/engine": "files"}, "Bound", "pv-hostpath"))
	srv.AddPV(`{"metadata":{"name":"pv-media"},"spec":{"local":{"path":"/var/lib/rancher/k3s/storage/pvc-media"}}}`)
	srv.AddPV(`{"metadata":{"name":"pv-hostpath"},"spec":{"hostPath":{"path":"/srv/host"}}}`)
	rule := simpleRule("volumes", "persistentvolumeclaims", "backup.example/engine=files", "Bound")
	rule.Source.Config = map[string]any{"root": "{{ volumePath }}", "ignore_vanished": true}

	found := discover(t, newDiscoverer(t, srv, rule))
	if len(found) != 2 {
		t.Fatalf("found %+v", found)
	}
	// One local volume and one hostPath: both answer with the path the node
	// stores the data at, and the claims come back in namespace/name order.
	for i, want := range []string{"/var/lib/rancher/k3s/storage/pvc-media", "/srv/host"} {
		if !strings.Contains(string(found[i].Config), want) {
			t.Fatalf("found[%d] config = %s, want the volume path %q", i, found[i].Config, want)
		}
	}
	if calls := srv.Calls("persistentvolumes"); calls != 1 {
		t.Fatalf("the volumes were listed %d times, want once per collect", calls)
	}
}

// TestVolumePathProblemsStayWithTheClaim: every way a claim can have no node
// path is that claim's own failed artifact. A listing the server refuses is a
// problem of the objects that asked for a path, never of the discovery — the
// rule over pods must still deliver its dump.
func TestVolumePathProblemsStayWithTheClaim(t *testing.T) {
	cases := []struct {
		name      string
		seed      func(*kubeapitest.Server)
		want      string
		retryable bool
		wantPodOK bool
	}{
		{
			name: "no binding",
			seed: func(srv *kubeapitest.Server) {
				srv.AddPVC(pvc("ns", "media", map[string]string{"backup.example/engine": "files"}, "Bound"))
			},
			want: "is bound to no volume",
		},
		{
			name: "no such volume",
			seed: func(srv *kubeapitest.Server) {
				srv.AddPVC(boundPVC("ns", "media", map[string]string{"backup.example/engine": "files"}, "Bound", "pv-gone"))
			},
			want: `the cluster has no persistentvolume named "pv-gone"`,
		},
		{
			name: "not a volume on a node",
			seed: func(srv *kubeapitest.Server) {
				srv.AddPVC(boundPVC("ns", "media", map[string]string{"backup.example/engine": "files"}, "Bound", "pv-csi"))
				srv.AddPV(`{"metadata":{"name":"pv-csi"},"spec":{}}`)
			},
			want: "is not a volume on a node",
		},
		{
			name: "the listing is refused",
			seed: func(srv *kubeapitest.Server) {
				srv.AddPVC(boundPVC("ns", "media", map[string]string{"backup.example/engine": "files"}, "Bound", "pv-media"))
				srv.Fail("persistentvolumes", 403)
			},
			want:      "reading the persistent volumes",
			wantPodOK: true,
		},
		{
			name: "the listing fails transiently",
			seed: func(srv *kubeapitest.Server) {
				srv.AddPVC(boundPVC("ns", "media", map[string]string{"backup.example/engine": "files"}, "Bound", "pv-media"))
				srv.Fail("persistentvolumes", 500)
			},
			want:      "reading the persistent volumes",
			retryable: true,
			wantPodOK: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := kubeapitest.New(t)
			tc.seed(srv)
			srv.AddPod(pod("ns", "db-0", map[string]string{"backup.example/engine": "postgres"}, "Running"))
			rule := simpleRule("volumes", "persistentvolumeclaims", "backup.example/engine=files", "Bound")
			rule.Source.Config = map[string]any{"root": "{{ volumePath }}", "ignore_vanished": true}
			found := discover(t, newDiscoverer(t, srv,
				simpleRule("databases", "pods", "backup.example/engine=postgres", "Running"),
				rule))

			if len(found) != 2 {
				t.Fatalf("found %+v, want the dump and the claim's own problem", found)
			}
			podFound, claimFound := found[0], found[1]
			if tc.wantPodOK && (podFound.Problem != "" || podFound.Excluded != "") {
				t.Fatalf("the pod rule did not deliver while the volume listing failed: %+v", podFound)
			}
			if !strings.Contains(claimFound.Problem, tc.want) {
				t.Fatalf("Problem = %q, want it to mention %q", claimFound.Problem, tc.want)
			}
			if claimFound.ProblemRetryable != tc.retryable {
				t.Fatalf("ProblemRetryable = %v, want %v", claimFound.ProblemRetryable, tc.retryable)
			}
		})
	}
}

// TestVolumePathOnAPodIsRefused: the function reads a claim's volume, and a
// rule that calls it on a pod is told so by name.
func TestVolumePathOnAPodIsRefused(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddPod(pod("ns", "db-0", map[string]string{"backup.example/type": "database"}, "Running"))
	rule := simpleRule("databases", "pods", "backup.example/type=database", "Running")
	rule.Source.Config = map[string]any{"root": "{{ volumePath }}"}
	if p := problemOf(t, newDiscoverer(t, srv, rule)); !strings.Contains(p, "declares no volume path") {
		t.Fatalf("Problem = %q", p)
	}
}

// TestExcludedObjectReadsNoSecret: an excluded object renders nothing, so it
// neither reads a Secret nor writes one under secrets_path — and the same rule
// without the marker does read one, which is what makes the count a check.
func TestExcludedObjectReadsNoSecret(t *testing.T) {
	for _, tc := range []struct {
		name        string
		labels      map[string]string
		wantReads   int
		wantVerdict string
	}{
		{"marked pod reads nothing", map[string]string{"backup.example/type": "database", testExcludeLabel: testExcludeValue}, 0, "ignored"},
		{"unmarked pod reads its secret", map[string]string{"backup.example/type": "database"}, 1, "ok"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := kubeapitest.New(t)
			srv.AddSecret("ns", "pw", map[string]string{"PW": "x"})
			srv.AddPod(pod("ns", "db-0", tc.labels, "Running",
				container{Name: "postgres", Env: []envVar{{Name: "PW", ValueFrom: map[string]any{
					"secretKeyRef": map[string]any{"name": "pw", "key": "PW"},
				}}}}))
			rule := simpleRule("databases", "pods", "backup.example/type=database", "Running")
			rule.Source.Config = map[string]any{"password": `{{ envFile 0 "PW" }}`}
			found := discover(t, newDiscoverer(t, srv, rule))

			if got := describeFound(found[0]).verdict; got != tc.wantVerdict {
				t.Fatalf("verdict = %q, want %q", got, tc.wantVerdict)
			}
			if reads := srv.Calls("secrets/ns/pw"); reads != tc.wantReads {
				t.Fatalf("the secret was read %d times, want %d", reads, tc.wantReads)
			}
			if tc.wantVerdict == "ok" && !strings.Contains(string(found[0].Config), "ns/secret-pw/PW") {
				t.Fatalf("the rendered config does not carry the secret file path: %s", found[0].Config)
			}
		})
	}
}

// TestTheMarkerOnAPodWithoutAnEngineExcludesItsClaims: the marker lives on the
// workload's own manifest, and a workload that declares no engine is listed by
// no rule — a StatefulSet whose volumeClaimTemplates PVCs nobody declared is
// exactly the case the marker is for. Its claims are ignored like any other
// decision, and the run is delivered.
func TestTheMarkerOnAPodWithoutAnEngineExcludesItsClaims(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddPod(mountingPod("ns", "app-0", map[string]string{testExcludeLabel: testExcludeValue}, "Running", "data-app-0"))
	srv.AddPVC(pvc("ns", "data-app-0", map[string]string{"backup.example/kind": "pvc"}, "Bound"))

	// A config whose only rule matches nothing: nothing lists the pod, and the
	// accounting is what must still see the claim and the marker on it.
	unmatched := simpleRule("volumes", "persistentvolumeclaims", "backup.example/nothing=none", "Bound")
	found := discover(t, newDiscoverer(t, srv, unmatched))
	if len(found) != 1 {
		t.Fatalf("found %+v, want the claim's decision and nothing else", found)
	}
	got := describeFound(found[0])
	if got.verdict != "ignored" || !strings.Contains(got.mentions, "the pod that mounts it, ns/app-0, carries the exclusion marker") {
		t.Fatalf("the marked pod's claim = %+v, want it ignored through the pod's own marker", got)
	}
}
