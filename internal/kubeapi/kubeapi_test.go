package kubeapi_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/vi-latyshev/go-cluster-backup/internal/kubeapi"
	"github.com/vi-latyshev/go-cluster-backup/internal/kubeapi/kubeapitest"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugin"
)

const (
	testHostEnv = "GCB_TEST_HOST"
	testPortEnv = "GCB_TEST_PORT"
)

func client(t *testing.T, srv *kubeapitest.Server) *kubeapi.Client {
	t.Helper()
	c, err := kubeapi.New(srv.Connection(t, testHostEnv, testPortEnv))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func podJSON(namespace, name string, labels map[string]string, phase string) string {
	pod := strings.Join([]string{
		`{"metadata":{"name":"` + name + `","namespace":"` + namespace + `","labels":{`,
		labelsJSON(labels),
		`}},"status":{"phase":"` + phase + `"}}`,
	}, "")
	return pod
}

func labelsJSON(labels map[string]string) string {
	var parts []string
	for k, v := range labels {
		parts = append(parts, `"`+k+`":"`+v+`"`)
	}
	return strings.Join(parts, ",")
}

func TestListPodsPaginationSelectorAndPhase(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.PageSize = 2
	srv.AddPod(podJSON("ns-a", "db-0", map[string]string{"type": "database", "engine": "postgres"}, "Running"))
	srv.AddPod(podJSON("ns-a", "db-1", map[string]string{"type": "database", "engine": "postgres"}, "Running"))
	srv.AddPod(podJSON("ns-a", "db-2", map[string]string{"type": "database", "engine": "redis"}, "Running"))
	srv.AddPod(podJSON("ns-b", "pvc-like", map[string]string{"type": "pvc"}, "Running"))
	srv.AddPod(podJSON("ns-b", "db-3", map[string]string{"type": "database", "engine": "postgres"}, "Succeeded"))

	pods, err := client(t, srv).ListPods(context.Background(), kubeapi.ListRequest{Selector: "type=database,engine=postgres", Phase: "Running", PageSize: 500})
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, p := range pods {
		names = append(names, p.Metadata.Namespace+"/"+p.Metadata.Name)
	}
	if strings.Join(names, " ") != "ns-a/db-0 ns-a/db-1" {
		t.Fatalf("paged, filtered listing = %v", names)
	}
	if pods[0].Status.Phase != "Running" || pods[0].Metadata.Labels["engine"] != "postgres" {
		t.Fatalf("pod = %+v", pods[0])
	}
}

// TestPageSizeIsRequired: the page size is the caller's config value; one below
// 1 is a configuration error, never a silent default.
func TestPageSizeIsRequired(t *testing.T) {
	srv := kubeapitest.New(t)
	_, err := client(t, srv).ListPods(context.Background(), kubeapi.ListRequest{PageSize: 0})
	if err == nil {
		t.Fatal("a listing without a page size was accepted")
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassPermanent {
		t.Fatalf("class = %q, want permanent", class)
	}
}

// TestListPVCsCarriesEveryPhase: the API server registers no status.phase field
// selector for claims, so the client sends none — and drops no phase either.
// Pending has no volume yet and Lost has lost its data; a listing that filtered
// one of them away would make the caller read it as an absent object, so the
// decision belongs to the caller and a request that asks for one phase is
// refused instead of answered.
func TestListPVCsCarriesEveryPhase(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddPVC(`{"metadata":{"name":"media","namespace":"media","labels":{"type":"pvc"}},"spec":{"volumeName":"pv-1"},"status":{"phase":"Bound"}}`)
	srv.AddPVC(`{"metadata":{"name":"pending","namespace":"media","labels":{"type":"pvc"}},"status":{"phase":"Pending"}}`)
	srv.AddPVC(`{"metadata":{"name":"lost","namespace":"media","labels":{"type":"pvc"}},"status":{"phase":"Lost"}}`)
	c := client(t, srv)
	pvcs, err := c.ListPVCs(context.Background(), kubeapi.ListRequest{Selector: "type=pvc", PageSize: 500})
	if err != nil {
		t.Fatal(err)
	}
	if len(pvcs) != 3 {
		t.Fatalf("a claim listing returned %d claims, want every phase", len(pvcs))
	}
	if pvcs[0].Spec.VolumeName != "pv-1" {
		t.Fatalf("volumeName = %q, want the bound volume", pvcs[0].Spec.VolumeName)
	}
	_, err = c.ListPVCs(context.Background(), kubeapi.ListRequest{Phase: "Bound", PageSize: 500})
	if err == nil {
		t.Fatal("a phase-filtered claim listing was accepted")
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassPermanent {
		t.Fatalf("class = %q, want permanent", class)
	}
}

// TestListPVs: a volume listing is cluster-scoped and carries the two paths a
// rule can turn a claim into a readable directory with.
func TestListPVs(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddPV(`{"metadata":{"name":"pv-1","labels":{"type":"local"}},"spec":{"local":{"path":"/var/lib/rancher/k3s/storage/pvc-1"}}}`)
	srv.AddPV(`{"metadata":{"name":"pv-2"},"spec":{"hostPath":{"path":"/srv/data"}}}`)
	c := client(t, srv)
	pvs, err := c.ListPVs(context.Background(), kubeapi.ListRequest{Selector: "type=local", PageSize: 500})
	if err != nil {
		t.Fatal(err)
	}
	if len(pvs) != 1 || pvs[0].Spec.Local == nil || pvs[0].Spec.Local.Path != "/var/lib/rancher/k3s/storage/pvc-1" {
		t.Fatalf("pvs = %+v", pvs)
	}
	if _, err := c.ListPVs(context.Background(), kubeapi.ListRequest{PageSize: 0}); err == nil {
		t.Fatal("a listing without a page size was accepted")
	}
}

func TestSecretValueDecodesBase64(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddSecret("ns", "pw", map[string]string{"PASSWORD": "p@ss\nword"})
	secret, err := client(t, srv).GetSecret(context.Background(), "ns", "pw")
	if err != nil {
		t.Fatal(err)
	}
	value, found, err := secret.Value("PASSWORD")
	if err != nil || !found || value != "p@ss\nword" {
		t.Fatalf("Value = %q, %v, %v", value, found, err)
	}
	if _, found, err := secret.Value("OTHER"); err != nil || found {
		t.Fatalf("absent key reported %v, %v", found, err)
	}
}

func TestConfigMapRead(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddConfigMap("ns", "catalog", map[string]string{"catalog.json": "[]"})
	cm, err := client(t, srv).GetConfigMap(context.Background(), "ns", "catalog")
	if err != nil {
		t.Fatal(err)
	}
	if cm.Data["catalog.json"] != "[]" || cm.Metadata.ResourceVersion == "" {
		t.Fatalf("configmap = %+v", cm)
	}
}

// TestPatchConfigMapDataOptimisticConcurrency: a patch carries the version the
// reader saw; a stale one conflicts, and an empty one is refused outright.
func TestPatchConfigMapDataOptimisticConcurrency(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddConfigMap("ns", "catalog", map[string]string{"catalog.json": "[]"})
	c := client(t, srv)
	ctx := context.Background()

	cm, err := c.GetConfigMap(ctx, "ns", "catalog")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.PatchConfigMapData(ctx, "ns", "catalog", map[string]string{"catalog.json": `[{"run_id":"r1"}]`}, cm.Metadata.ResourceVersion); err != nil {
		t.Fatal(err)
	}
	if got := srv.ConfigMapData("ns", "catalog")["catalog.json"]; got != `[{"run_id":"r1"}]` {
		t.Fatalf("stored = %q", got)
	}

	// The version read before the patch is stale now.
	_, err = c.PatchConfigMapData(ctx, "ns", "catalog", map[string]string{"catalog.json": "[]"}, cm.Metadata.ResourceVersion)
	if err == nil || !strings.Contains(err.Error(), "409") {
		t.Fatalf("stale patch = %v, want a 409", err)
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassTransient {
		t.Fatalf("class = %q, want transient", class)
	}

	if _, err := c.PatchConfigMapData(ctx, "ns", "catalog", map[string]string{"catalog.json": "[]"}, ""); err == nil {
		t.Fatal("an unconditional patch was accepted")
	}
}

// TestPatchConfigMapDataKeepsMetadata: the patch names the data and nothing
// else, so the annotations and labels a reconciler owns survive it. A PUT
// (the previous shape) replaced the object and wiped them, which is how a
// `prune: disabled` annotation would disappear on every Save.
func TestPatchConfigMapDataKeepsMetadata(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddConfigMapMeta("ns", "catalog",
		map[string]string{"kustomize.toolkit.fluxcd.io/prune": "disabled", "kustomize.toolkit.fluxcd.io/ssa": "Merge"},
		map[string]string{"kustomize.toolkit.fluxcd.io/name": "cluster-infra"},
		map[string]string{"catalog.json": "[]"})
	c := client(t, srv)
	ctx := context.Background()

	cm, err := c.GetConfigMap(ctx, "ns", "catalog")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.PatchConfigMapData(ctx, "ns", "catalog", map[string]string{"catalog.json": `[{"run_id":"r1"}]`}, cm.Metadata.ResourceVersion); err != nil {
		t.Fatal(err)
	}
	got := srv.ConfigMapAnnotations("ns", "catalog")
	for _, want := range []string{"kustomize.toolkit.fluxcd.io/prune", "kustomize.toolkit.fluxcd.io/ssa"} {
		if got[want] == "" {
			t.Errorf("annotation %q did not survive the patch: %v", want, got)
		}
	}
}

func TestErrorClassification(t *testing.T) {
	cases := []struct {
		name  string
		key   string
		code  int
		class plugin.Class
	}{
		{"not found", "secrets", 404, plugin.ClassNotFound},
		{"forbidden", "secrets", 403, plugin.ClassPermanent},
		{"unauthorized", "secrets", 401, plugin.ClassPermanent},
		{"server error", "secrets", 500, plugin.ClassTransient},
		{"too many requests", "secrets", 429, plugin.ClassTransient},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := kubeapitest.New(t)
			srv.Fail(tc.key, tc.code)
			_, err := client(t, srv).GetSecret(context.Background(), "ns", "pw")
			if err == nil {
				t.Fatal("no error")
			}
			if class, _ := plugin.ClassOf(err); class != tc.class {
				t.Fatalf("class = %q, want %q (error: %v)", class, tc.class, err)
			}
			if !strings.Contains(err.Error(), fmt.Sprintf("HTTP %d", tc.code)) {
				t.Fatalf("error does not carry the status: %v", err)
			}
		})
	}
}

// TestTokenIsRereadPerRequest: a projected token rotates under the process, so
// every request must carry the file's current value.
func TestTokenIsRereadPerRequest(t *testing.T) {
	srv := kubeapitest.New(t)
	opts := srv.Connection(t, testHostEnv, testPortEnv)
	c, err := kubeapi.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := c.ListPods(ctx, kubeapi.ListRequest{PageSize: 500}); err != nil {
		t.Fatal(err)
	}
	kubeapitest.SetToken(t, opts, "token-2")
	if _, err := c.ListPods(ctx, kubeapi.ListRequest{PageSize: 500}); err != nil {
		t.Fatal(err)
	}
	if len(srv.Tokens) != 2 || srv.Tokens[0] != "token-1" || srv.Tokens[1] != "token-2" {
		t.Fatalf("tokens seen = %v", srv.Tokens)
	}
}

func TestEmptyTokenIsPermanent(t *testing.T) {
	srv := kubeapitest.New(t)
	opts := srv.Connection(t, testHostEnv, testPortEnv)
	kubeapitest.SetToken(t, opts, "   \n")
	c, err := kubeapi.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.ListPods(context.Background(), kubeapi.ListRequest{PageSize: 500})
	if err == nil {
		t.Fatal("no error")
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassPermanent {
		t.Fatalf("class = %q, want permanent", class)
	}
}

func TestCancelledContextSendsNothing(t *testing.T) {
	srv := kubeapitest.New(t)
	c := client(t, srv)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.ListPods(ctx, kubeapi.ListRequest{PageSize: 500}); err == nil {
		t.Fatal("no error")
	}
	if len(srv.Tokens) != 0 {
		t.Fatalf("a cancelled call sent %d requests", len(srv.Tokens))
	}
}

func TestTimeoutIsTransient(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.Delay = 200 * time.Millisecond
	opts := srv.Connection(t, testHostEnv, testPortEnv)
	opts.Timeout = 20 * time.Millisecond
	c, err := kubeapi.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.ListPods(context.Background(), kubeapi.ListRequest{PageSize: 500})
	if err == nil {
		t.Fatal("no error")
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassTransient {
		t.Fatalf("class = %q, want transient (%v)", class, err)
	}
}

// TestStuckPaginationFails: a server that repeats the same continue token must
// not loop forever.
func TestStuckPaginationFails(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.PageSize = 1
	srv.StuckContinue = true
	srv.AddPod(podJSON("ns", "a", nil, "Running"))
	srv.AddPod(podJSON("ns", "b", nil, "Running"))
	_, err := client(t, srv).ListPods(context.Background(), kubeapi.ListRequest{PageSize: 500})
	if err == nil || !strings.Contains(err.Error(), "continue token") {
		t.Fatalf("ListPods = %v, want the stuck token reported", err)
	}
	if class, _ := plugin.ClassOf(err); class != plugin.ClassTransient {
		t.Fatalf("class = %q, want transient", class)
	}
}

func TestNewRejectsBadOptions(t *testing.T) {
	good := kubeapitest.New(t).Connection(t, testHostEnv, testPortEnv)
	cases := map[string]func(*kubeapi.Options){
		"no host env name": func(o *kubeapi.Options) { o.HostEnv = "" },
		"no port env name": func(o *kubeapi.Options) { o.PortEnv = "" },
		"no token file":    func(o *kubeapi.Options) { o.TokenFile = "" },
		"no CA file":       func(o *kubeapi.Options) { o.CAFile = "" },
		"no timeout":       func(o *kubeapi.Options) { o.Timeout = 0 },
		"unset env":        func(o *kubeapi.Options) { o.HostEnv = "GCB_TEST_UNSET_HOST" },
		"unset port env":   func(o *kubeapi.Options) { o.PortEnv = "GCB_TEST_UNSET_PORT" },
		"missing CA file":  func(o *kubeapi.Options) { o.CAFile = "/nonexistent/ca.crt" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			opts := good
			mutate(&opts)
			if _, err := kubeapi.New(opts); err == nil {
				t.Fatal("no error")
			}
		})
	}
}

func TestCAFileMustHoldACertificate(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/ca.crt"
	if err := os.WriteFile(path, []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	opts := kubeapitest.New(t).Connection(t, testHostEnv, testPortEnv)
	opts.CAFile = path
	if _, err := kubeapi.New(opts); err == nil {
		t.Fatal("a CA file without a certificate was accepted")
	}
}
