package kuberneteslabels_test

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"encoding/json/jsontext"

	"github.com/vi-latyshev/go-cluster-backup/internal/discovery"
	"github.com/vi-latyshev/go-cluster-backup/internal/kubeapi/kubeapitest"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/discovery/kuberneteslabels"
)

// buildFrom builds the plugin instance from a config fixture, the way a run
// does: through the registry, so the strict decode and Validate are the ones a
// deployment meets.
func buildFrom(t *testing.T, cfg configFixture) discovery.Discoverer {
	t.Helper()
	d, err := plugin.Build(context.Background(), registry(t), discovery.Kind, "kubernetes_labels",
		jsontext.Value(mustJSON(cfg)), plugin.Deps{Instance: "workloads", Log: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// configFor is the config of one instance over a given connection.
func configFor(hostEnv, portEnv, tokenFile, caFile, secretsPath string, rules ...ruleFixture) configFixture {
	return configFixture{
		APIHostEnv: hostEnv, APIPortEnv: portEnv, TokenFile: tokenFile, CAFile: caFile,
		RequestTimeout: "10s", SecretsDir: secretsPath, ListPageSize: 500,
		Exclude:    excludeFixture{Label: testExcludeLabel, Value: testExcludeValue},
		Undeclared: undeclaredVolume(),
		Rules:      rules,
	}
}

// TestNewHonoursACancelledContextDirectly: construction refuses a cancelled
// context before it reads a single field of the config.
func TestNewHonoursACancelledContextDirectly(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := kuberneteslabels.New(ctx, kuberneteslabels.Config{}, plugin.Deps{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("New = %v, want the cancellation", err)
	}
}

// TestBrokenConnectionIsOneDiscoveryFailure: the client is built once, before
// any listing, and a connection the environment cannot supply fails the whole
// discovery permanently — it is config, not a blip to retry.
func TestBrokenConnectionIsOneDiscoveryFailure(t *testing.T) {
	const unsetHost, unsetPort = "GCB_TEST_UNSET_HOST", "GCB_TEST_UNSET_PORT"
	cfg := configFor(unsetHost, unsetPort, "/var/run/token", "/var/run/ca.crt", t.TempDir(),
		simpleRule("databases", "pods", "backup.example/type=database", "Running"))
	d := buildFrom(t, cfg)

	_, err := d.Discover(context.Background())
	if err == nil || !strings.Contains(err.Error(), unsetHost) {
		t.Fatalf("Discover = %v, want the unset environment variable named", err)
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassPermanent {
		t.Fatalf("class = %q, want permanent", class)
	}
	if err := check(t, d); err == nil {
		t.Fatal("Check = nil, want the same connection failure")
	}
}

// TestClaimListingFailureIsADiscoveryFailure: a listing the server refuses is
// the rule's own failure, wrapped with the rule that asked for it.
func TestClaimListingFailureIsADiscoveryFailure(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.Fail("persistentvolumeclaims", 500)
	d := newDiscoverer(t, srv, simpleRule("volumes", "persistentvolumeclaims", "backup.example/type=pvc", "Bound"))

	_, err := d.Discover(context.Background())
	if err == nil || !strings.Contains(err.Error(), `rule "volumes"`) || !strings.Contains(err.Error(), "500") {
		t.Fatalf("Discover = %v, want the failing rule and the status named", err)
	}
}

// TestMarkedPodsListingFailureIsADiscoveryFailure: the pass that decides the
// marked pods lists every pod, and a listing the server refuses fails the
// discovery rather than silently deciding nothing.
func TestMarkedPodsListingFailureIsADiscoveryFailure(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddPod(pod("ns", "db-0", map[string]string{"backup.example/type": "database"}, "Running"))
	rule := simpleRule("databases", "pods", "backup.example/type=database", "Running")
	// The first pods request is the rule's own listing; the second is the pass
	// that looks for the marker on every pod.
	srv.FailNth("pods", 500, 2)
	d := newDiscoverer(t, srv, rule)

	if _, err := d.Discover(context.Background()); err == nil || !strings.Contains(err.Error(), "listing pods") {
		t.Fatalf("Discover = %v, want the marked-pod listing failure", err)
	}
}

// TestCoveringPodProblemsLeaveTheClaimToTheAccounting: a pod rule that reads
// volumes covers nothing when its pod could not be rendered. A permanent
// problem leaves the claim unaccounted — the accounting reports it and names
// the pod — while a retryable one leaves the verdict to another pass, and the
// claim travels as a retryable problem of its own.
func TestCoveringPodProblemsLeaveTheClaimToTheAccounting(t *testing.T) {
	t.Run("permanent problem covers nothing", func(t *testing.T) {
		srv := kubeapitest.New(t)
		srv.AddPod(mountingPod("ns", "db-0", map[string]string{"backup.example/type": "database"}, "Running", "data-db-0"))
		srv.AddPVC(pvc("ns", "data-db-0", map[string]string{"backup.example/kind": "pvc"}, "Bound"))
		rule := withCovers(simpleRule("databases", "pods", "backup.example/type=database", "Running"))
		// The pod's own source cannot be rendered: the pod covers no claim.
		rule.Source.Config = map[string]any{"password": `{{ label "not-a-label" }}`}

		found := discover(t, newDiscoverer(t, srv, rule))
		if len(found) != 2 {
			t.Fatalf("found %+v, want the pod's problem and the claim's report", found)
		}
		if !strings.Contains(found[0].Problem, `has no label "not-a-label"`) {
			t.Fatalf("the pod = %+v, want its own problem", found[0])
		}
		claim := found[1]
		if claim.Artifact.Name != "ns/data-db-0" || !strings.Contains(claim.Problem, "is backed up by no rule") {
			t.Fatalf("the claim = %+v, want the accounting's report", claim)
		}
		if !strings.Contains(claim.Problem, "the pod that mounts it, ns/db-0") {
			t.Fatalf("the report %q does not name the pod that mounts the claim", claim.Problem)
		}
	})

	t.Run("retryable problem leaves the verdict to another pass", func(t *testing.T) {
		srv := kubeapitest.New(t)
		srv.Fail("secrets/ns/cred", 500)
		srv.AddPod(mountingPodWithEnv("ns", "db-0", map[string]string{"backup.example/type": "database"}, "Running", "data-db-0"))
		srv.AddPVC(pvc("ns", "data-db-0", map[string]string{"backup.example/kind": "pvc"}, "Bound"))
		rule := withCovers(simpleRule("databases", "pods", "backup.example/type=database", "Running"))
		rule.Source.Config = map[string]any{"password": `{{ envFile 0 "PW" }}`}

		found := discover(t, newDiscoverer(t, srv, rule))
		if len(found) != 2 {
			t.Fatalf("found %+v, want the pod's problem and the claim's", found)
		}
		if !found[0].ProblemRetryable || !strings.Contains(found[0].Problem, "500") {
			t.Fatalf("the pod = %+v, want a retryable problem", found[0])
		}
		claim := found[1]
		if claim.Artifact.Name != "ns/data-db-0" || !strings.Contains(claim.Problem, "could not be read in this pass") {
			t.Fatalf("the claim = %+v, want the pod's retryable stumble", claim)
		}
		if !claim.ProblemRetryable {
			t.Fatal("the claim's problem is not retryable although the pod's read was")
		}
	})
}

// mountingPodWithEnv is a pod that mounts one claim and carries the secret
// reference a covering rule reads.
func mountingPodWithEnv(namespace, name string, labels map[string]string, phase string, claims ...string) string {
	var p objectFixture
	p.Metadata.Name = name
	p.Metadata.Namespace = namespace
	p.Metadata.Labels = labels
	p.Status.Phase = phase
	for _, claim := range claims {
		var v volumeFixture
		v.Name = "volume-" + claim
		v.PersistentVolumeClaim.ClaimName = claim
		p.Spec.Volumes = append(p.Spec.Volumes, v)
	}
	p.Spec.Containers = []container{{Name: "postgres", Env: []envVar{
		{Name: "PW", ValueFrom: map[string]any{"secretKeyRef": map[string]any{"name": "cred", "key": "PW"}}},
	}}}
	return mustJSON(p)
}

// TestEnvironmentOnAClaimIsRefused: envValue and envFile read a pod's spec, and
// a rule that calls one of them on a claim is told so by name.
func TestEnvironmentOnAClaimIsRefused(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddPVC(pvc("ns", "media", map[string]string{"backup.example/type": "pvc"}, "Bound"))
	rule := simpleRule("volumes", "persistentvolumeclaims", "backup.example/type=pvc", "Bound")
	rule.Source.Config = map[string]any{"password": `{{ envValue 0 "PW" }}`}
	if p := problemOf(t, newDiscoverer(t, srv, rule)); !strings.Contains(p, "declares no containers") {
		t.Fatalf("Problem = %q", p)
	}
}

// TestMalformedEnvironmentRefs: a reference that is not the shape Kubernetes
// defines is named, not guessed at.
func TestMalformedEnvironmentRefs(t *testing.T) {
	cases := []struct {
		name string
		env  envVar
		want string
	}{
		{
			"secret reference that is not an object",
			envVar{Name: "PW", ValueFrom: map[string]any{"secretKeyRef": 5}},
			`environment variable "PW": json: `,
		},
		{
			"secret reference without a key",
			envVar{Name: "PW", ValueFrom: map[string]any{"secretKeyRef": map[string]any{"name": "cred"}}},
			"names an incomplete secretKeyRef",
		},
		{
			"configmap reference that is not an object",
			envVar{Name: "DB", ValueFrom: map[string]any{"configMapKeyRef": 5}},
			`environment variable "DB": json: `,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := kubeapitest.New(t)
			srv.AddPod(pod("ns", "db-0", map[string]string{"backup.example/type": "database"}, "Running",
				container{Name: "postgres", Env: []envVar{tc.env}}))
			rule := simpleRule("databases", "pods", "backup.example/type=database", "Running")
			rule.Source.Config = map[string]any{"password": `{{ envValue 0 "` + tc.env.Name + `" }}`}
			if p := problemOf(t, newDiscoverer(t, srv, rule)); !strings.Contains(p, tc.want) {
				t.Fatalf("Problem = %q, want a message containing %q", p, tc.want)
			}
		})
	}
}

// TestSecretFileEscapingTheSecretsDirectory: the names the file path is built
// from come from the cluster, so a key that would leave secrets_dir is refused
// rather than written outside it.
func TestSecretFileEscapingTheSecretsDirectory(t *testing.T) {
	srv := kubeapitest.New(t)
	const key = "../../etc/passwd"
	srv.AddSecret("ns", "cred", map[string]string{key: "x"})
	srv.AddPod(pod("ns", "db-0", map[string]string{"backup.example/type": "database"}, "Running",
		container{Name: "postgres", Env: []envVar{{Name: "PW", ValueFrom: map[string]any{
			"secretKeyRef": map[string]any{"name": "cred", "key": key},
		}}}}))
	rule := simpleRule("databases", "pods", "backup.example/type=database", "Running")
	rule.Source.Config = map[string]any{"password": `{{ envFile 0 "PW" }}`}
	if p := problemOf(t, newDiscoverer(t, srv, rule)); !strings.Contains(p, "escapes the secrets directory") {
		t.Fatalf("Problem = %q, want the escaping secret key refused", p)
	}
}

// TestSecretFileWriteFailure: a path the file cannot be written to is that
// object's problem, named with the path, and never a half-rendered config.
func TestSecretFileWriteFailure(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddSecret("ns", "cred", map[string]string{"PW": "x"})
	srv.AddPod(pod("ns", "db-0", map[string]string{"backup.example/type": "database"}, "Running",
		container{Name: "postgres", Env: []envVar{{Name: "PW", ValueFrom: map[string]any{
			"secretKeyRef": map[string]any{"name": "cred", "key": "PW"},
		}}}}))
	rule := simpleRule("databases", "pods", "backup.example/type=database", "Running")
	rule.Source.Config = map[string]any{"password": `{{ envFile 0 "PW" }}`}

	secretsDir := t.TempDir()
	// The path envFile resolves to is a directory: the file cannot be written.
	target := filepath.Join(secretsDir, "ns", "secret-cred", "PW")
	if err := os.MkdirAll(target, 0o700); err != nil {
		t.Fatal(err)
	}
	found, err := newDiscovererIn(t, srv, secretsDir, 500, rule).Discover(context.Background())
	if err != nil {
		t.Fatalf("Discover = %v, want the problem scoped to its object", err)
	}
	if len(found) != 1 || !strings.Contains(found[0].Problem, target) {
		t.Fatalf("found = %+v, want a Problem naming %s", found, target)
	}
}

// TestArtifactGroupFallback: the artifact group is a template too, and one that
// cannot be rendered falls back to the rule's name while the Problem says so.
func TestArtifactGroupFallback(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddPod(pod("ns", "db-0", map[string]string{"backup.example/type": "database"}, "Running", container{Name: "postgres"}))
	rule := simpleRule("databases", "pods", "backup.example/type=database", "Running")
	rule.Artifact.Group = `{{ label "not-a-label" }}`
	rule.Artifact.Name = "{{ .Name }}"

	found := discover(t, newDiscoverer(t, srv, rule))
	if len(found) != 1 {
		t.Fatalf("found %+v", found)
	}
	f := found[0]
	if f.Artifact.Group != "databases" {
		t.Fatalf("artifact group = %q, want the rule-name fallback", f.Artifact.Group)
	}
	if !strings.Contains(f.Problem, "artifact group:") || !strings.Contains(f.Problem, "falls back to") {
		t.Fatalf("Problem = %q", f.Problem)
	}
}

// TestPortContainerOutOfRange: a port index past the pod's containers is named
// with the count the pod actually has.
func TestPortContainerOutOfRange(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddPod(pod("ns", "db-0", map[string]string{"backup.example/type": "database"}, "Running", container{Name: "postgres"}))
	rule := simpleRule("databases", "pods", "backup.example/type=database", "Running")
	rule.Source.Config = map[string]any{"address": "db:{{ port 2 0 }}"}
	if p := problemOf(t, newDiscoverer(t, srv, rule)); !strings.Contains(p, "has no container 2 (it has 1)") {
		t.Fatalf("Problem = %q", p)
	}
}

// TestFailingRenderStopsAtTheFirstProblem: a node after the one that failed is
// not rendered into a document that is already discarded.
func TestFailingRenderStopsAtTheFirstProblem(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddPod(pod("ns", "db-0", map[string]string{"backup.example/type": "database"}, "Running", container{Name: "postgres"}))
	rule := simpleRule("databases", "pods", "backup.example/type=database", "Running")
	// The keys are rendered in sorted order: "a" fails and "b" is never reached.
	rule.Source.Config = map[string]any{"a": `{{ label "not-a-label" }}`, "b": `{{ label "also-not-a-label" }}`}

	found := discover(t, newDiscoverer(t, srv, rule))
	if len(found) != 1 {
		t.Fatalf("found %+v", found)
	}
	if !strings.Contains(found[0].Problem, `has no label "not-a-label"`) {
		t.Fatalf("Problem = %q", found[0].Problem)
	}
	if strings.Contains(found[0].Problem, "also-not-a-label") {
		t.Fatalf("Problem = %q, want only the first failure", found[0].Problem)
	}
}

// TestCancellationDuringTheVolumeListingStopsTheDiscovery: the claim's own
// read is cut off by the caller's stop, and the pass ends with the
// cancellation rather than a problem invented for the claim.
func TestCancellationDuringTheVolumeListingStopsTheDiscovery(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.Hold = map[string]time.Duration{"persistentvolumes": 500 * time.Millisecond}
	srv.AddPVC(boundPVC("ns", "media", map[string]string{"backup.example/engine": "files"}, "Bound", "pv-media"))
	rule := simpleRule("volumes", "persistentvolumeclaims", "backup.example/engine=files", "Bound")
	rule.Source.Config = map[string]any{"root": "{{ volumePath }}"}
	d := newDiscoverer(t, srv, rule)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	base := srv.Calls("persistentvolumes")
	go func() {
		for srv.Calls("persistentvolumes") < base+1 {
			time.Sleep(2 * time.Millisecond)
		}
		cancel()
	}()

	found, err := d.Discover(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Discover = %+v, %v, want the cancellation", found, err)
	}
	if found != nil {
		t.Fatalf("found = %+v, want nil: the cancelled pass is not a result", found)
	}
}

// TestRenderedValueThatIsNotUTF8: JSON carries UTF-8 and a Secret may not. A
// value the encoder refuses fails this one object's render — the identity and
// the envelope survive — instead of producing a config no source could be built
// from.
func TestRenderedValueThatIsNotUTF8(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddSecret("ns", "cred", map[string]string{"PW": "\xff\xfe not utf-8"})
	srv.AddPod(pod("ns", "db-0", map[string]string{"backup.example/type": "database"}, "Running",
		container{Name: "postgres", Env: []envVar{{Name: "PW", ValueFrom: map[string]any{
			"secretKeyRef": map[string]any{"name": "cred", "key": "PW"},
		}}}}))
	rule := simpleRule("databases", "pods", "backup.example/type=database", "Running")
	rule.Source.Config = map[string]any{"password": `{{ envValue 0 "PW" }}`}

	if p := problemOf(t, newDiscoverer(t, srv, rule)); !strings.Contains(p, "invalid UTF-8") {
		t.Fatalf("Problem = %q, want the undecodable value refused by the encoder", p)
	}
}

// TestFailingRenderInsideAnArrayStopsTheArray: an element that cannot be
// rendered ends the array's traversal too, so no later element is written into
// a document that is already discarded.
func TestFailingRenderInsideAnArrayStopsTheArray(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddPod(pod("ns", "db-0", map[string]string{"backup.example/type": "database"}, "Running", container{Name: "postgres"}))
	rule := simpleRule("databases", "pods", "backup.example/type=database", "Running")
	rule.Source.Config = map[string]any{"list": []any{`{{ label "not-a-label" }}`, `{{ label "also-not-a-label" }}`}}

	found := discover(t, newDiscoverer(t, srv, rule))
	if len(found) != 1 {
		t.Fatalf("found %+v", found)
	}
	if !strings.Contains(found[0].Problem, `has no label "not-a-label"`) {
		t.Fatalf("Problem = %q", found[0].Problem)
	}
	if strings.Contains(found[0].Problem, "also-not-a-label") {
		t.Fatalf("Problem = %q, want only the first failure", found[0].Problem)
	}
}
