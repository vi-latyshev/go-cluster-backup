package kubernetesconfigmap_test

import (
	"context"
	"encoding/pem"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"encoding/json/jsontext"
	json "encoding/json/v2"

	"github.com/vi-latyshev/go-cluster-backup/internal/loader"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/catalog"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/catalog/catalogtest"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/catalog/kubernetesconfigmap"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/errors"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/instances/plugintest"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/kubeapi/kubeapitest"
	"github.com/vi-latyshev/go-cluster-backup/internal/plugins/model"
)

const (
	testHostEnv = "GCB_TEST_HOST"
	testPortEnv = "GCB_TEST_PORT"

	namespace = "infrastructure"
	name      = "cluster-backup-catalog"
	leaseKey  = "lease.json"
)

type configFixture struct {
	APIHostEnv     string `json:"api_host_env"`
	APIPortEnv     string `json:"api_port_env"`
	TokenFile      string `json:"token_file"`
	CAFile         string `json:"ca_file"`
	RequestTimeout string `json:"request_timeout"`
	Namespace      string `json:"namespace"`
	Name           string `json:"name"`
	LeaseKey       string `json:"lease_key"`
}

func registry(t *testing.T) *instances.Registry {
	t.Helper()
	lr := loader.New()
	factories := instances.NewRegistry()
	if err := loader.Add(lr, kubernetesconfigmap.Spec, func(lr *loader.Registry) error {
		return kubernetesconfigmap.Apply(lr, factories)
	}); err != nil {
		t.Fatal(err)
	}
	return factories
}

func newCatalogIn(t *testing.T, srv *kubeapitest.Server) catalog.Catalog {
	t.Helper()
	opts := srv.Connection(t, testHostEnv, testPortEnv)
	return buildCatalog(t, configFixture{
		APIHostEnv: testHostEnv, APIPortEnv: testPortEnv,
		TokenFile: opts.TokenFile, CAFile: opts.CAFile,
		RequestTimeout: "10s",
		Namespace:      namespace, Name: name, LeaseKey: leaseKey,
	})
}

func buildCatalog(t *testing.T, cfg configFixture) catalog.Catalog {
	t.Helper()
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	c, err := instances.Build(context.Background(), registry(t), catalog.Kind, "kubernetes_configmap",
		jsontext.Value(raw), instances.Deps{Instance: "catalog", Log: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func leaseConfig(hostEnv, portEnv, tokenFile, caFile string) configFixture {
	return configFixture{
		APIHostEnv: hostEnv, APIPortEnv: portEnv, TokenFile: tokenFile, CAFile: caFile,
		RequestTimeout: "10s", Namespace: namespace, Name: name, LeaseKey: leaseKey,
	}
}

func TestGeneralConformance(t *testing.T) {
	plugintest.CheckSpec(t, registry(t))
}

// TestApplyContributesCompiledDescriptor: registration is memory-only and the
// descriptor stays visible whatever the selection, so merge, schema and
// `plugins describe` never depend on the deployment enabling the catalog.
func TestApplyContributesCompiledDescriptor(t *testing.T) {
	lr := loader.New()
	factories := instances.NewRegistry()
	if err := loader.Add(lr, kubernetesconfigmap.Spec, func(lr *loader.Registry) error {
		return kubernetesconfigmap.Apply(lr, factories)
	}); err != nil {
		t.Fatal(err)
	}
	ds := loader.All(lr, instances.Descriptors)
	if len(ds) != 1 {
		t.Fatalf("descriptors %+v", ds)
	}
	if ds[0].ID != "catalog/kubernetes_configmap" || ds[0].ID != kubernetesconfigmap.Spec.ID {
		t.Fatalf("descriptor id %q, spec id %q", ds[0].ID, kubernetesconfigmap.Spec.ID)
	}
	if ds[0].Summary != kubernetesconfigmap.Spec.Summary {
		t.Fatalf("descriptor summary %q, spec summary %q", ds[0].Summary, kubernetesconfigmap.Spec.Summary)
	}
}

// TestWriteTimeoutIsTwoRequests: one record write is a read and a patch, so the
// finish window's reserve for removing this run's pending mark is twice
// request_timeout.
func TestWriteTimeoutIsTwoRequests(t *testing.T) {
	srv := kubeapitest.New(t)
	opts := srv.Connection(t, testHostEnv, testPortEnv)
	c := buildCatalog(t, configFixture{
		APIHostEnv: testHostEnv, APIPortEnv: testPortEnv,
		TokenFile: opts.TokenFile, CAFile: opts.CAFile,
		RequestTimeout: "7s",
		Namespace:      namespace, Name: name, LeaseKey: leaseKey,
	})
	bounded, ok := c.(catalog.WriteBounded)
	if !ok {
		t.Fatal("the catalog cannot bound one record write")
	}
	if got := bounded.WriteTimeout(); got != 14*time.Second {
		t.Fatalf("WriteTimeout() = %s, want twice the 7s request timeout", got)
	}
}

func TestCatalogConformance(t *testing.T) {
	catalogtest.Run(t, func(t *testing.T) catalog.Catalog {
		srv := kubeapitest.New(t)
		// The deployment manifest creates the object; the record inside it
		// starts with no holder.
		srv.AddConfigMap(namespace, name, map[string]string{})
		return newCatalogIn(t, srv)
	})
}

// TestAbsentConfigMap: without the object there is no claim to take, mark or
// clear, and each of those reports not_found; Release is a no-op because no
// claim can exist, and preflight passes because the deployment manifest creates
// the object before the first run.
func TestAbsentConfigMap(t *testing.T) {
	srv := kubeapitest.New(t)
	c := newCatalogIn(t, srv)
	ctx := context.Background()
	lease := catalog.Lease{Holder: "run-a", Token: "t"}

	_, err := c.Acquire(ctx, catalog.LeaseRequest{Holder: "run-a"})
	if err == nil || !strings.Contains(err.Error(), name) || !strings.Contains(err.Error(), "deployment manifest creates it") {
		t.Fatalf("Acquire = %v, want the missing ConfigMap named", err)
	}
	if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassNotFound {
		t.Fatalf("class = %q, want not_found", class)
	}
	if err := c.MarkUncertainty(ctx, lease); err == nil {
		t.Fatal("MarkUncertainty on an absent ConfigMap = nil")
	}
	if err := c.ClearUncertainty(ctx, lease); err == nil {
		t.Fatal("ClearUncertainty on an absent ConfigMap = nil")
	}
	if err := c.(catalog.UncertaintyObserver).ObserveUncertainty(ctx, lease, "d"); err == nil {
		t.Fatal("ObserveUncertainty on an absent ConfigMap = nil")
	}
	if err := c.Release(ctx, lease); err != nil {
		t.Fatalf("Release without a ConfigMap = %v, want a no-op", err)
	}
	if err := c.(instances.Checker).Check(ctx); err != nil {
		t.Fatalf("Check without a ConfigMap = %v; the deployment manifest creates it", err)
	}
	if srv.ConfigMapData(namespace, name) != nil {
		t.Fatal("the catalog created the ConfigMap")
	}
}

// TestCheckSurfacesADeniedRead: RBAC that refuses the ConfigMap fails preflight
// with a permanent error naming the status.
func TestCheckSurfacesADeniedRead(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddConfigMap(namespace, name, map[string]string{})
	srv.Deny("configmaps/" + namespace + "/" + name)
	err := newCatalogIn(t, srv).(instances.Checker).Check(context.Background())
	if err == nil || !strings.Contains(err.Error(), "403") {
		t.Fatalf("Check = %v, want a 403", err)
	}
	if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassPermanent {
		t.Fatalf("class = %q, want permanent", class)
	}
}

// TestAClientThatCannotBeBuiltIsPermanent: every call reports the same
// construction failure, so a misconfigured deployment fails loudly instead of
// retrying a name that will never resolve.
func TestAClientThatCannotBeBuiltIsPermanent(t *testing.T) {
	const unsetHostEnv = "GCB_COVERAGE_UNSET_HOST"
	t.Setenv(unsetHostEnv, "")
	c := buildCatalog(t, leaseConfig(unsetHostEnv, testPortEnv, "/var/run/token", "/var/run/ca.crt"))
	ctx := context.Background()

	failures := map[string]func() error{
		"Check": func() error { return c.(instances.Checker).Check(ctx) },
		"Acquire": func() error {
			_, err := c.Acquire(ctx, catalog.LeaseRequest{Holder: "run"})
			return err
		},
		"MarkUncertainty":  func() error { return c.MarkUncertainty(ctx, catalog.Lease{Token: "t"}) },
		"ClearUncertainty": func() error { return c.ClearUncertainty(ctx, catalog.Lease{Token: "t"}) },
		"ObserveUncertainty": func() error {
			return c.(catalog.UncertaintyObserver).ObserveUncertainty(ctx, catalog.Lease{Token: "t"}, "d")
		},
		"Release": func() error { return c.Release(ctx, catalog.Lease{Token: "t"}) },
		"HolderState": func() error {
			_, _, err := c.(catalog.Liveness).HolderState(ctx, catalog.Lease{
				HolderNamespace: namespace, HolderPod: "backup-a", HolderUID: "uid-a"})
			return err
		},
	}
	for name, call := range failures {
		t.Run(name, func(t *testing.T) {
			err := call()
			if err == nil {
				t.Fatal("a client that cannot be built was not reported")
			}
			if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassPermanent {
				t.Fatalf("class = %q, want permanent: %v", class, err)
			}
		})
	}
}

func TestConfigValidation(t *testing.T) {
	base := func(mutate func(*configFixture)) jsontext.Value {
		cfg := configFixture{
			APIHostEnv: testHostEnv, APIPortEnv: testPortEnv,
			TokenFile: "/var/run/token", CAFile: "/var/run/ca.crt",
			RequestTimeout: "10s", Namespace: namespace, Name: name, LeaseKey: leaseKey,
		}
		mutate(&cfg)
		raw, err := json.Marshal(cfg)
		if err != nil {
			t.Fatal(err)
		}
		return jsontext.Value(raw)
	}
	if err := registry(t).Check("catalog", "kubernetes_configmap", base(func(*configFixture) {})); err != nil {
		t.Fatalf("the valid config = %v", err)
	}
	cases := map[string]struct {
		raw  jsontext.Value
		want string
	}{
		"empty host env":      {base(func(c *configFixture) { c.APIHostEnv = "" }), "/api_host_env is empty"},
		"empty port env":      {base(func(c *configFixture) { c.APIPortEnv = "" }), "/api_port_env is empty"},
		"relative token file": {base(func(c *configFixture) { c.TokenFile = "token" }), "/token_file"},
		"relative ca file":    {base(func(c *configFixture) { c.CAFile = "ca.crt" }), "/ca_file"},
		"zero timeout":        {base(func(c *configFixture) { c.RequestTimeout = "0s" }), "/request_timeout"},
		"empty namespace":     {base(func(c *configFixture) { c.Namespace = "" }), "/namespace is empty"},
		"empty name":          {base(func(c *configFixture) { c.Name = "" }), "/name is empty"},
		"empty lease key":     {base(func(c *configFixture) { c.LeaseKey = "" }), "/lease_key is empty"},
	}
	for testName, tc := range cases {
		t.Run(testName, func(t *testing.T) {
			err := registry(t).Check("catalog", "kubernetes_configmap", tc.raw)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Check = %v, want a message containing %q", err, tc.want)
			}
		})
	}
}

// TestNewRefusesACancelledContext: a cancelled context stops the constructor
// before it builds anything.
func TestNewRefusesACancelledContext(t *testing.T) {
	cfg := kubernetesconfigmap.Config{
		APIHostEnv: "GCB_HOST", APIPortEnv: "GCB_PORT", TokenFile: "/var/run/token", CAFile: "/var/run/ca.crt",
		RequestTimeout: model.Duration(10 * time.Second), Namespace: namespace, Name: name, LeaseKey: leaseKey,
	}
	c, err := kubernetesconfigmap.New(context.Background(), cfg, instances.Deps{})
	if err != nil || c == nil {
		t.Fatalf("New = %v, %v", c, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := kubernetesconfigmap.New(ctx, cfg, instances.Deps{}); err == nil {
		t.Fatal("New built a catalog on a cancelled context")
	}
}

// TestConfigMapWithoutAVersionIsNotOneToWriteTo: a read answered without a
// resourceVersion is not an object to take, mark, clear or release a claim
// against, because an unconditional patch is not a compare-and-swap.
func TestConfigMapWithoutAVersionIsNotOneToWriteTo(t *testing.T) {
	c := emptyVersionCatalog(t)
	ctx := context.Background()
	lease := catalog.Lease{Token: "t"}

	_, err := c.Acquire(ctx, catalog.LeaseRequest{Holder: "run", Kind: model.RunKindManual})
	if err == nil || !strings.Contains(err.Error(), "is not a claim to write to") {
		t.Fatalf("Acquire = %v, want the unversioned-ConfigMap refusal", err)
	}
	if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassNotFound {
		t.Fatalf("class = %q, want not_found", class)
	}
	if err := c.MarkUncertainty(ctx, lease); err == nil {
		t.Fatal("MarkUncertainty on an unversioned ConfigMap = nil")
	}
	if err := c.ClearUncertainty(ctx, lease); err == nil {
		t.Fatal("ClearUncertainty on an unversioned ConfigMap = nil")
	}
	if err := c.(catalog.UncertaintyObserver).ObserveUncertainty(ctx, lease, "d"); err == nil {
		t.Fatal("ObserveUncertainty on an unversioned ConfigMap = nil")
	}
	if err := c.Release(ctx, lease); err != nil {
		t.Fatalf("Release = %v, want a no-op", err)
	}
	if err := c.(instances.Checker).Check(ctx); err != nil {
		t.Fatalf("Check = %v", err)
	}
}

// emptyVersionCatalog points the plugin at an API server that answers a
// ConfigMap read with an object carrying no resourceVersion — the one shape the
// real API server never produces, and the reason an empty version reads as "not
// there".
func emptyVersionCatalog(t *testing.T) catalog.Catalog {
	t.Helper()
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if _, err := io.WriteString(w, `{"metadata":{"name":"`+name+`","namespace":"`+namespace+`"},"data":{}}`); err != nil {
			t.Errorf("stub: %v", err)
		}
	}))
	t.Cleanup(srv.Close)
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.crt")
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	if err := os.WriteFile(caPath, ca, 0o600); err != nil {
		t.Fatal(err)
	}
	tokenPath := filepath.Join(dir, "token")
	if err := os.WriteFile(tokenPath, []byte("token-1"), 0o600); err != nil {
		t.Fatal(err)
	}
	const hostEnv, portEnv = "GCB_STUB_HOST", "GCB_STUB_PORT"
	t.Setenv(hostEnv, u.Hostname())
	t.Setenv(portEnv, u.Port())
	return buildCatalog(t, leaseConfig(hostEnv, portEnv, tokenPath, caPath))
}

// TestUnencodableLeaseIsPermanent: an identity string the JSON encoder cannot
// render is refused as permanent, not silently dropped from the claim.
func TestUnencodableLeaseIsPermanent(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddConfigMap(namespace, name, map[string]string{})
	c := newCatalogIn(t, srv)

	_, err := c.Acquire(context.Background(), catalog.LeaseRequest{Holder: string([]byte{0xff})})
	if err == nil || !strings.Contains(err.Error(), "encode lease") {
		t.Fatalf("Acquire = %v, want the encode refusal", err)
	}
	if class, _ := pluginerrors.ClassOf(err); class != pluginerrors.ClassPermanent {
		t.Fatalf("class = %q, want permanent", class)
	}
	if data := srv.ConfigMapData(namespace, name); len(data) != 0 {
		t.Fatalf("the unencodable claim was written: %v", data)
	}
}

// TestHolderStateProvesTermination: only a pod found with the recorded UID in
// phase Succeeded/Failed is DEFINITIVE termination. An absent pod or a different
// UID is proof only of the object being gone — a forced deletion can produce it
// while the process still runs — so it is reported non-definitively and the
// the protection lease applies its hard bound. A running or pending pod, a partial identity
// and a failed read are all "not terminated".
func TestHolderStateProvesTermination(t *testing.T) {
	cases := []struct {
		name       string
		pod        string
		namespace  string
		uid        string
		terminated bool
		definitive bool
	}{
		{"absent pod", "", "infrastructure", "uid-a", true, false},
		{"recreated pod", `{"metadata":{"name":"backup-a","namespace":"infrastructure","uid":"uid-new"},"status":{"phase":"Running"}}`, "infrastructure", "uid-a", true, false},
		{"succeeded pod", `{"metadata":{"name":"backup-a","namespace":"infrastructure","uid":"uid-a"},"status":{"phase":"Succeeded"}}`, "infrastructure", "uid-a", true, true},
		{"failed pod", `{"metadata":{"name":"backup-a","namespace":"infrastructure","uid":"uid-a"},"status":{"phase":"Failed"}}`, "infrastructure", "uid-a", true, true},
		{"running pod with a terminated container status", `{"metadata":{"name":"backup-a","namespace":"infrastructure","uid":"uid-a"},"status":{"phase":"Running","containerStatuses":[{"state":{"terminated":{}}}]}}`, "infrastructure", "uid-a", false, false},
		{"pending pod", `{"metadata":{"name":"backup-a","namespace":"infrastructure","uid":"uid-a"},"status":{"phase":"Pending"}}`, "infrastructure", "uid-a", false, false},
		{"a different namespace finds no pod", `{"metadata":{"name":"backup-a","namespace":"infrastructure","uid":"uid-a"},"status":{"phase":"Succeeded"}}`, "other", "uid-a", true, false},
		{"partial identity", "", "infrastructure", "", false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := kubeapitest.New(t)
			srv.AddConfigMap(namespace, name, map[string]string{})
			if tc.pod != "" {
				srv.AddPod(tc.pod)
			}
			l := newCatalogIn(t, srv).(catalog.Liveness)
			terminated, definitive, err := l.HolderState(context.Background(), catalog.Lease{
				Holder: "h", HolderNamespace: tc.namespace, HolderPod: "backup-a", HolderUID: tc.uid})
			if err != nil {
				t.Fatalf("HolderState: %v", err)
			}
			if terminated != tc.terminated || definitive != tc.definitive {
				t.Fatalf("HolderState = (%v, %v), want (%v, %v)", terminated, definitive, tc.terminated, tc.definitive)
			}
		})
	}
	// A read that fails is not a verdict.
	srv := kubeapitest.New(t)
	srv.AddConfigMap(namespace, name, map[string]string{})
	srv.Fail("pods", 500)
	l := newCatalogIn(t, srv).(catalog.Liveness)
	if terminated, _, err := l.HolderState(context.Background(), catalog.Lease{
		Holder: "h", HolderNamespace: "infrastructure", HolderPod: "backup-a", HolderUID: "uid-a"}); terminated || err == nil {
		t.Fatalf("a failed liveness read = %v, %v; want not terminated with an error", terminated, err)
	}
}

// TestTheObjectHoldsOnlyTheLease: the ConfigMap carries the lease record under
// the configured key and nothing else — there is no run history to keep.
func TestTheObjectHoldsOnlyTheLease(t *testing.T) {
	srv := kubeapitest.New(t)
	srv.AddConfigMap(namespace, name, map[string]string{})
	c := newCatalogIn(t, srv)
	ctx := context.Background()
	if err := c.(instances.Checker).Check(ctx); err != nil {
		t.Fatalf("Check on a readable ConfigMap = %v", err)
	}

	held, err := c.Acquire(ctx, catalog.LeaseRequest{Holder: "run-a", Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.MarkUncertainty(ctx, markedLease(held, "run-a")); err != nil {
		t.Fatal(err)
	}
	if err := c.Release(ctx, held); err != nil {
		t.Fatal(err)
	}
	data := srv.ConfigMapData(namespace, name)
	if len(data) != 1 {
		t.Fatalf("stored keys = %v, want the lease key alone", data)
	}
	// The released record carries no holder fields and keeps the uncertainty.
	second, err := c.Acquire(ctx, catalog.LeaseRequest{Holder: "run-b", Kind: model.RunKindScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if second.Uncertainty == nil || !slices.Equal(second.Uncertainty.RunIDs, []string{"run-a"}) {
		t.Fatalf("the released record's uncertainty was not inherited: %+v", second.Uncertainty)
	}
	record := readRecord(t, srv)
	if record.Holder != "run-b" || record.Kind != model.RunKindScheduled {
		t.Fatalf("stored claim = %+v, want run-b", record)
	}
}

// readRecord decodes the lease record the fake API server stores.
func readRecord(t *testing.T, srv *kubeapitest.Server) catalog.Lease {
	t.Helper()
	var lease catalog.Lease
	if err := json.Unmarshal([]byte(srv.ConfigMapData(namespace, name)[leaseKey]), &lease); err != nil {
		t.Fatalf("the stored lease record: %v", err)
	}
	return lease
}
